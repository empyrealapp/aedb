use super::{
    AedbInstance, CommitFinality, CommitTelemetryEvent, LifecycleEvent, LifecycleHook,
    QueryBatchItem, QueryCommitTelemetryHook, QueryTelemetryEvent, REACTIVE_ACK_CACHE_MAX_ENTRIES,
    RECOVERY_CACHE_TTL, ReactiveCheckpointAckCacheKey, ReactiveCheckpointAckState,
    ReactiveProcessorOptions, ReadOnlySqlAdapter, RecoveryCache, RemoteBackupAdapter,
    SYSTEM_CALLER_ID,
};
use crate::PredicateEvaluationPath;
use crate::catalog::schema::{ColumnDef, IndexType};
use crate::catalog::types::{ColumnType, Row, Value};
use crate::catalog::{DdlOperation, ResourceType};
use crate::commit::action::{ActionCommitOutcome, ActionEnvelopeRequest};
use crate::commit::tx::{
    IdempotencyKey, ReadAssertion, TransactionEnvelope, WriteClass, WriteIntent,
};
use crate::commit::validation::{
    KvIntegerAmount, KvIntegerMissingPolicy, KvIntegerUnderflowPolicy, KvU64MissingPolicy,
    KvU64OverflowPolicy, KvU64UnderflowPolicy, KvU256MissingPolicy, KvU256UnderflowPolicy,
    MAX_COUNTER_SHARDS, Mutation,
};
use crate::config::{AedbConfig, DurabilityMode, RecoveryMode, StorageMode};
use crate::error::{AedbError, AedbErrorCode, ResourceType as ErrorResourceType};
use crate::open_support::reclaim_eligible_wal_segments;
use crate::permission::{CallerContext, Permission};
use crate::query::error::QueryError;
use crate::query::plan::{ConsistencyMode, Expr, Query, QueryOptions};
use crate::query::planner::ExecutionStage;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn mk_recovery_view(seq: u64) -> crate::snapshot::reader::SnapshotReadView {
    crate::snapshot::reader::SnapshotReadView {
        keyspace: Arc::new(crate::storage::keyspace::Keyspace::default().snapshot()),
        catalog: Arc::new(crate::catalog::Catalog::default()),
        seq,
    }
}

struct RecordingLifecycleHook {
    events: Arc<std::sync::Mutex<Vec<LifecycleEvent>>>,
}

impl LifecycleHook for RecordingLifecycleHook {
    fn on_event(&self, event: &LifecycleEvent) {
        self.events
            .lock()
            .expect("recording hook mutex poisoned")
            .push(event.clone());
    }
}

fn u256_be_test(v: u64) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[24..].copy_from_slice(&v.to_be_bytes());
    out
}

fn u64_be_test(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<(), AedbError> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let entry_path = entry.path();
        let out_path = dst.join(entry.file_name());
        let ty = entry.file_type()?;
        if ty.is_dir() {
            copy_dir_recursive(&entry_path, &out_path)?;
        } else if ty.is_file() {
            fs::copy(&entry_path, &out_path)?;
        }
    }
    Ok(())
}

async fn create_table(
    db: &AedbInstance,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    columns: Vec<ColumnDef>,
    primary_key: Vec<&str>,
) {
    db.commit_ddl(DdlOperation::CreateTable {
        owner_id: None,
        project_id: project_id.to_string(),
        scope_id: scope_id.to_string(),
        table_name: table_name.to_string(),
        if_not_exists: false,
        columns,
        primary_key: primary_key.into_iter().map(|v| v.to_string()).collect(),
    })
    .await
    .expect("create table");
}

struct RecordingTelemetryHook {
    queries: Arc<std::sync::Mutex<Vec<QueryTelemetryEvent>>>,
    commits: Arc<std::sync::Mutex<Vec<CommitTelemetryEvent>>>,
}

impl QueryCommitTelemetryHook for RecordingTelemetryHook {
    fn on_query(&self, event: &QueryTelemetryEvent) {
        self.queries
            .lock()
            .expect("query telemetry mutex poisoned")
            .push(event.clone());
    }

    fn on_commit(&self, event: &CommitTelemetryEvent) {
        self.commits
            .lock()
            .expect("commit telemetry mutex poisoned")
            .push(event.clone());
    }
}

struct EchoSqlAdapter;

impl ReadOnlySqlAdapter for EchoSqlAdapter {
    fn execute_read_only(
        &self,
        _project_id: &str,
        _scope_id: &str,
        sql: &str,
    ) -> Result<(Query, QueryOptions), QueryError> {
        if sql.trim() == "select id from items" {
            Ok((
                Query::select(&["id"]).from("items"),
                QueryOptions {
                    allow_full_scan: true,
                    ..QueryOptions::default()
                },
            ))
        } else {
            Err(QueryError::InvalidQuery {
                reason: "unsupported sql".into(),
            })
        }
    }
}

struct RestrictiveSqlAdapter;

impl ReadOnlySqlAdapter for RestrictiveSqlAdapter {
    fn execute_read_only(
        &self,
        _project_id: &str,
        _scope_id: &str,
        sql: &str,
    ) -> Result<(Query, QueryOptions), QueryError> {
        if sql.trim() == "select * from items" {
            Ok((Query::select(&["*"]).from("items"), QueryOptions::default()))
        } else {
            Err(QueryError::InvalidQuery {
                reason: "unsupported sql".into(),
            })
        }
    }
}

struct LocalRemoteAdapter;

impl RemoteBackupAdapter for LocalRemoteAdapter {
    fn store_backup_dir(&self, uri: &str, backup_dir: &Path) -> Result<(), AedbError> {
        let target = PathBuf::from(uri);
        if target.exists() {
            fs::remove_dir_all(&target)?;
        }
        copy_dir_recursive(backup_dir, &target)
    }

    fn materialize_backup_chain(
        &self,
        uri: &str,
        scratch_dir: &Path,
    ) -> Result<Vec<PathBuf>, AedbError> {
        let source = PathBuf::from(uri);
        let target = scratch_dir.join("remote_chain_0");
        copy_dir_recursive(&source, &target)?;
        Ok(vec![target])
    }
}

mod auth;
mod backup_restore;
mod commit_ops;
mod kv_api;
mod migrations;
mod operations;
mod order_book_api;
mod query_api;
mod reactive_processors;
