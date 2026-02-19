pub mod backup;
pub mod catalog;
pub mod checkpoint;
pub mod commit;
pub mod config;
pub mod declarative;
pub mod error;
mod lib_helpers;
#[cfg(test)]
mod lib_tests;
pub mod manifest;
pub mod migration;
pub mod offline;
pub mod permission;
pub mod preflight;
pub mod query;
pub mod recovery;
pub mod repository;
pub mod snapshot;
pub mod storage;
pub mod sync_bridge;
pub mod version_store;
pub mod wal;

use crate::backup::{
    BackupManifest, extract_backup_archive, load_backup_manifest, resolve_backup_path,
    sha256_file_hex, verify_backup_files, write_backup_archive, write_backup_manifest,
};
use crate::catalog::namespace_key;
use crate::catalog::schema::{AsyncIndexDef, IndexDef, TableSchema};
use crate::catalog::types::{Row, Value};
use crate::catalog::{DdlOperation, ResourceType};
use crate::checkpoint::loader::load_checkpoint_with_key;
use crate::checkpoint::writer::write_checkpoint_with_key;
use crate::commit::executor::{CommitExecutor, CommitResult, ExecutorMetrics};
use crate::commit::tx::{ReadSet, TransactionEnvelope, WriteClass, WriteIntent};
use crate::commit::validation::{Mutation, TableUpdateExpr, validate_permissions};
use crate::config::{AedbConfig, DurabilityMode, RecoveryMode};
use crate::error::AedbError;
use crate::error::ResourceType as ErrorResourceType;
use crate::lib_helpers::*;
use crate::manifest::atomic::write_manifest_atomic_signed;
use crate::manifest::schema::{Manifest, SegmentMeta};
use crate::migration::{
    Migration, MigrationRecord, checksum_hex, decode_record, encode_record, migration_key,
};
use crate::permission::{CallerContext, Permission};
use crate::preflight::{PreflightResult, preflight, preflight_plan};
use crate::query::error::QueryError;
use crate::query::executor::{QueryResult, execute_query_with_options};
use crate::query::plan::{ConsistencyMode, Expr, Order, Query, QueryOptions};
use crate::query::planner::{ExecutionStage, build_physical_plan};
use crate::query::{KvCursor, KvScanResult, ScopedKvEntry};
use crate::recovery::replay::replay_segments;
use crate::recovery::{recover_at_seq_with_config, recover_with_config};
use crate::snapshot::gc::{SnapshotHandle, SnapshotManager};
use crate::snapshot::reader::SnapshotReadView;
use crate::storage::keyspace::{KvEntry, NamespaceId};
use crate::wal::frame::{FrameError, FrameReader};
use crate::wal::segment::{SEGMENT_HEADER_SIZE, SegmentHeader};
use parking_lot::Mutex;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fs;
use std::fs::File;
use std::io::{BufReader, Read};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tracing::{info, warn};

/// Creates a directory with restrictive permissions (0o700 on Unix) to prevent
/// unauthorized access to database files on multi-user systems.
fn create_private_dir_all(path: &Path) -> Result<(), AedbError> {
    #[cfg(unix)]
    {
        use std::fs::DirBuilder;
        use std::os::unix::fs::DirBuilderExt;
        use std::os::unix::fs::PermissionsExt;

        DirBuilder::new()
            .recursive(true)
            .mode(0o700) // Owner read/write/execute only
            .create(path)?;
        let metadata = fs::metadata(path)?;
        if !metadata.is_dir() {
            return Err(AedbError::Validation(format!(
                "path is not a directory: {}",
                path.display()
            )));
        }
        let mut perms = metadata.permissions();
        if perms.mode() != 0o700 {
            perms.set_mode(0o700);
            fs::set_permissions(path, perms)?;
        }
    }
    #[cfg(not(unix))]
    {
        fs::create_dir_all(path)?;
    }
    Ok(())
}

pub struct AedbInstance {
    _config: AedbConfig,
    require_authenticated_calls: bool,
    dir: PathBuf,
    executor: CommitExecutor,
    /// Fast-path shutdown check using atomic flag to avoid RwLock contention.
    /// Set to true when checkpoint or shutdown is in progress.
    checkpoint_in_progress: Arc<AtomicBool>,
    /// Semaphore to control checkpoint exclusivity. Normal operations acquire
    /// a permit in shared mode; checkpoint acquires all permits for exclusivity.
    checkpoint_gate: Arc<Semaphore>,
    snapshot_manager: Arc<Mutex<SnapshotManager>>,
    recovery_cache: Arc<Mutex<RecoveryCache>>,
    lifecycle_hooks: Arc<Mutex<Vec<Arc<dyn LifecycleHook>>>>,
    telemetry_hooks: Arc<Mutex<Vec<Arc<dyn QueryCommitTelemetryHook>>>>,
    startup_recovery_micros: u64,
    startup_recovered_seq: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitFinality {
    /// Return as soon as commit is visible at the snapshot head.
    Visible,
    /// Return only after commit sequence is durable in WAL.
    Durable,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OperationalMetrics {
    pub commits_total: u64,
    pub commit_errors: u64,
    pub conflict_rejections: u64,
    pub read_set_conflicts: u64,
    pub conflict_rate: f64,
    pub avg_commit_latency_micros: u64,
    pub coordinator_apply_attempts: u64,
    pub avg_coordinator_apply_micros: u64,
    pub inflight_commits: usize,
    pub queue_depth: usize,
    pub durable_head_lag: u64,
    pub visible_head_seq: u64,
    pub durable_head_seq: u64,
    pub current_seq: u64,
    pub snapshot_age_micros: u64,
    pub startup_recovery_micros: u64,
    pub startup_recovered_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectInfo {
    pub project_id: String,
    pub scope_count: u32,
    pub created_at_micros: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeInfo {
    pub scope_id: String,
    pub table_count: u32,
    pub kv_key_count: u64,
    pub created_at_micros: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    pub table_name: String,
    pub column_count: u32,
    pub index_count: u32,
    pub row_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DdlResult {
    pub applied: bool,
    pub seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DdlBatchResult {
    pub seq: u64,
    pub results: Vec<DdlResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DdlBatchOrderMode {
    AsProvided,
    DependencyAware,
}

#[derive(Debug, Clone)]
pub struct MutateWhereReturningResult {
    pub commit: CommitResult,
    pub primary_key: Vec<Value>,
    pub before: Row,
    pub after: Row,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationReport {
    pub applied: Vec<(u64, String, Duration)>,
    pub skipped: Vec<u64>,
    pub current_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LifecycleEvent {
    ProjectCreated {
        project_id: String,
        seq: u64,
    },
    ProjectDropped {
        project_id: String,
        seq: u64,
    },
    ScopeCreated {
        project_id: String,
        scope_id: String,
        seq: u64,
    },
    ScopeDropped {
        project_id: String,
        scope_id: String,
        seq: u64,
    },
    TableCreated {
        project_id: String,
        scope_id: String,
        table_name: String,
        seq: u64,
    },
    TableDropped {
        project_id: String,
        scope_id: String,
        table_name: String,
        seq: u64,
    },
    TableAltered {
        project_id: String,
        scope_id: String,
        table_name: String,
        seq: u64,
    },
}

pub trait LifecycleHook: Send + Sync {
    fn on_event(&self, event: &LifecycleEvent);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPageResult {
    pub rows: Vec<Row>,
    pub total_count: usize,
    pub next_cursor: Option<String>,
    pub snapshot_seq: u64,
    pub rows_examined: usize,
}

#[derive(Debug, Clone)]
pub struct ListWithTotalRequest {
    pub project_id: String,
    pub scope_id: String,
    pub query: Query,
    pub cursor: Option<String>,
    pub offset: Option<usize>,
    pub page_size: usize,
    pub consistency: ConsistencyMode,
}

#[derive(Debug, Clone)]
pub struct LookupThenHydrateRequest {
    pub project_id: String,
    pub scope_id: String,
    pub source_query: Query,
    pub source_key_index: usize,
    pub hydrate_query: Query,
    pub hydrate_key_column: String,
    pub consistency: ConsistencyMode,
}

#[derive(Debug, Clone)]
pub struct UpdateWhereRequest {
    pub caller: CallerContext,
    pub project_id: String,
    pub scope_id: String,
    pub table_name: String,
    pub predicate: Expr,
    pub updates: Vec<(String, Value)>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct UpdateWhereExprRequest {
    pub caller: CallerContext,
    pub project_id: String,
    pub scope_id: String,
    pub table_name: String,
    pub predicate: Expr,
    pub updates: Vec<(String, TableUpdateExpr)>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct TableU256MutationRequest {
    pub caller: CallerContext,
    pub project_id: String,
    pub scope_id: String,
    pub table_name: String,
    pub primary_key: Vec<Value>,
    pub column: String,
    pub amount_be: [u8; 32],
}

#[derive(Debug, Clone)]
pub struct CompareAndSwapRequest {
    pub caller: CallerContext,
    pub project_id: String,
    pub scope_id: String,
    pub table_name: String,
    pub primary_key: Vec<Value>,
    pub row: Row,
    pub expected_seq: u64,
}

#[derive(Debug, Clone)]
pub struct QueryBatchItem {
    pub query: Query,
    pub options: QueryOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryDiagnostics {
    pub snapshot_seq: u64,
    pub estimated_scan_rows: u64,
    pub max_scan_rows: u64,
    pub index_used: Option<String>,
    pub stages: Vec<ExecutionStage>,
    pub bounded_by_limit_or_cursor: bool,
    pub has_joins: bool,
}

#[derive(Debug, Clone)]
pub struct QueryWithDiagnosticsResult {
    pub result: QueryResult,
    pub diagnostics: QueryDiagnostics,
}

#[derive(Debug, Clone)]
pub struct SqlTransactionPlan {
    pub base_seq: u64,
    pub caller: Option<CallerContext>,
    pub mutations: Vec<Mutation>,
}

pub trait QueryCommitTelemetryHook: Send + Sync {
    fn on_query(&self, _event: &QueryTelemetryEvent) {}
    fn on_commit(&self, _event: &CommitTelemetryEvent) {}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryTelemetryEvent {
    pub project_id: String,
    pub scope_id: String,
    pub table: String,
    pub snapshot_seq: u64,
    pub rows_examined: usize,
    pub latency_micros: u64,
    pub ok: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitTelemetryEvent {
    pub op: &'static str,
    pub commit_seq: Option<u64>,
    pub durable_head_seq: Option<u64>,
    pub latency_micros: u64,
    pub ok: bool,
    pub error: Option<String>,
}

pub trait ReadOnlySqlAdapter: Send + Sync {
    fn execute_read_only(
        &self,
        project_id: &str,
        scope_id: &str,
        sql: &str,
    ) -> Result<(Query, QueryOptions), QueryError>;
}

pub trait RemoteBackupAdapter: Send + Sync {
    fn store_backup_dir(&self, uri: &str, backup_dir: &Path) -> Result<(), AedbError>;
    fn materialize_backup_chain(
        &self,
        uri: &str,
        scratch_dir: &Path,
    ) -> Result<Vec<PathBuf>, AedbError>;
}

struct SnapshotLease {
    manager: Arc<Mutex<SnapshotManager>>,
    handle: SnapshotHandle,
    view: SnapshotReadView,
}

pub struct ReadTx<'a> {
    db: &'a AedbInstance,
    lease: SnapshotLease,
    caller: Option<CallerContext>,
}

const RECOVERY_CACHE_CAPACITY: usize = 16;
const RECOVERY_CACHE_TTL: Duration = Duration::from_secs(60);
const SYSTEM_CALLER_ID: &str = "system";
const MAX_MUTATE_WHERE_RETURNING_RETRIES: usize = 16;

#[derive(Debug, Default)]
struct RecoveryCache {
    order: VecDeque<(u64, u64)>,
    entries: HashMap<u64, RecoveryCacheEntry>,
    next_generation: u64,
}

#[derive(Debug)]
struct RecoveryCacheEntry {
    view: SnapshotReadView,
    created: Instant,
    generation: u64,
}

impl RecoveryCache {
    fn get(&mut self, seq: u64) -> Option<SnapshotReadView> {
        self.prune_expired();
        let generation = self.bump_generation();
        let view = {
            let entry = self.entries.get_mut(&seq)?;
            entry.created = Instant::now();
            entry.generation = generation;
            entry.view.clone()
        };
        self.order.push_back((seq, generation));
        self.compact_order_if_needed();
        Some(view)
    }

    fn put(&mut self, seq: u64, view: SnapshotReadView) {
        self.prune_expired();
        let generation = self.bump_generation();
        self.entries.insert(
            seq,
            RecoveryCacheEntry {
                view,
                created: Instant::now(),
                generation,
            },
        );
        self.order.push_back((seq, generation));
        self.evict_to_capacity();
        self.compact_order_if_needed();
    }

    fn prune_expired(&mut self) {
        let now = Instant::now();
        let expired: Vec<u64> = self
            .entries
            .iter()
            .filter_map(|(seq, entry)| {
                if now.duration_since(entry.created) > RECOVERY_CACHE_TTL {
                    Some(*seq)
                } else {
                    None
                }
            })
            .collect();
        for seq in expired {
            self.entries.remove(&seq);
        }
        self.compact_order_if_needed();
    }

    fn bump_generation(&mut self) -> u64 {
        let generation = self.next_generation;
        self.next_generation = self.next_generation.wrapping_add(1);
        generation
    }

    fn evict_to_capacity(&mut self) {
        while self.entries.len() > RECOVERY_CACHE_CAPACITY {
            let Some((seq, generation)) = self.order.pop_front() else {
                break;
            };
            let should_remove = self
                .entries
                .get(&seq)
                .map(|entry| entry.generation == generation)
                .unwrap_or(false);
            if should_remove {
                self.entries.remove(&seq);
            }
        }
    }

    fn compact_order_if_needed(&mut self) {
        let max_order_len = RECOVERY_CACHE_CAPACITY.saturating_mul(8);
        if self.order.len() <= max_order_len {
            return;
        }
        let mut live: Vec<(u64, u64)> = self
            .entries
            .iter()
            .map(|(seq, entry)| (*seq, entry.generation))
            .collect();
        live.sort_by_key(|(_, generation)| *generation);
        self.order = live.into_iter().collect();
    }
}

impl Drop for SnapshotLease {
    fn drop(&mut self) {
        let mut mgr = self.manager.lock();
        mgr.release(self.handle);
        let _ = mgr.gc();
    }
}

impl AedbInstance {
    pub fn open_production(config: AedbConfig, dir: &Path) -> Result<Self, AedbError> {
        validate_arcana_config(&config)?;
        Self::open_internal(config, dir, true)
    }

    pub fn open_secure(config: AedbConfig, dir: &Path) -> Result<Self, AedbError> {
        validate_secure_config(&config)?;
        Self::open_internal(config, dir, true)
    }

    pub fn open(config: AedbConfig, dir: &Path) -> Result<Self, AedbError> {
        Self::open_internal(config, dir, false)
    }

    fn open_internal(
        config: AedbConfig,
        dir: &Path,
        require_authenticated_calls: bool,
    ) -> Result<Self, AedbError> {
        validate_config(&config)?;
        info!(
            max_segment_bytes = config.max_segment_bytes,
            max_segment_age_secs = config.max_segment_age_secs,
            durability_mode = ?config.durability_mode,
            batch_interval_ms = config.batch_interval_ms,
            batch_max_bytes = config.batch_max_bytes,
            idempotency_window_seconds = config.idempotency_window_seconds,
            max_inflight_commits = config.max_inflight_commits,
            max_commit_queue_bytes = config.max_commit_queue_bytes,
            max_transaction_bytes = config.max_transaction_bytes,
            commit_timeout_ms = config.commit_timeout_ms,
            max_snapshot_age_ms = config.max_snapshot_age_ms,
            max_concurrent_snapshots = config.max_concurrent_snapshots,
            max_scan_rows = config.max_scan_rows,
            max_kv_key_bytes = config.max_kv_key_bytes,
            max_kv_value_bytes = config.max_kv_value_bytes,
            max_memory_estimate_bytes = config.max_memory_estimate_bytes,
            epoch_max_wait_us = config.epoch_max_wait_us,
            epoch_min_commits = config.epoch_min_commits,
            epoch_max_commits = config.epoch_max_commits,
            adaptive_epoch_enabled = config.adaptive_epoch_enabled,
            adaptive_epoch_min_commits_floor = config.adaptive_epoch_min_commits_floor,
            adaptive_epoch_min_commits_ceiling = config.adaptive_epoch_min_commits_ceiling,
            adaptive_epoch_wait_us_floor = config.adaptive_epoch_wait_us_floor,
            adaptive_epoch_wait_us_ceiling = config.adaptive_epoch_wait_us_ceiling,
            adaptive_epoch_target_latency_us = config.adaptive_epoch_target_latency_us,
            parallel_apply_enabled = config.parallel_apply_enabled,
            parallel_worker_threads = config.parallel_worker_threads,
            coordinator_locking_enabled = config.coordinator_locking_enabled,
            global_unique_index_enabled = config.global_unique_index_enabled,
            partition_lock_timeout_ms = config.partition_lock_timeout_ms,
            epoch_apply_timeout_ms = config.epoch_apply_timeout_ms,
            checkpoint_encryption_enabled = config.checkpoint_encryption_key.is_some(),
            checkpoint_key_id = config.checkpoint_key_id.as_deref().unwrap_or(""),
            manifest_hmac_enabled = config.manifest_hmac_key.is_some(),
            recovery_mode = ?config.recovery_mode,
            hash_chain_required = config.hash_chain_required,
            primary_index_backend = ?config.primary_index_backend,
            "aedb config"
        );
        create_private_dir_all(dir)?;
        let has_existing = fs::read_dir(dir)?
            .filter_map(|e| e.ok())
            .any(|e| e.file_name().to_string_lossy().contains(".aedb"));

        let recovery_start = std::time::SystemTime::now();
        let (executor, startup_recovered_seq) = if has_existing {
            let mut recovered = recover_with_config(dir, &config)?;
            recovered.keyspace.set_backend(config.primary_index_backend);
            if require_authenticated_calls {
                seed_system_global_admin(&mut recovered.catalog);
            }
            (
                CommitExecutor::with_state(
                    dir,
                    recovered.keyspace,
                    recovered.catalog,
                    recovered.current_seq,
                    recovered.current_seq + 1,
                    config.clone(),
                    recovered.idempotency,
                )?,
                recovered.current_seq,
            )
        } else {
            let mut catalog = crate::catalog::Catalog::default();
            if require_authenticated_calls {
                seed_system_global_admin(&mut catalog);
            }
            (
                CommitExecutor::with_state(
                    dir,
                    crate::storage::keyspace::Keyspace::with_backend(config.primary_index_backend),
                    catalog,
                    0,
                    1,
                    config.clone(),
                    std::collections::HashMap::new(),
                )?,
                0,
            )
        };
        let startup_recovery_micros =
            recovery_start.elapsed().unwrap_or_default().as_micros() as u64;

        Ok(Self {
            _config: config,
            require_authenticated_calls,
            dir: dir.to_path_buf(),
            executor,
            checkpoint_in_progress: Arc::new(AtomicBool::new(false)),
            checkpoint_gate: Arc::new(Semaphore::new(1000)),
            snapshot_manager: Arc::new(Mutex::new(SnapshotManager::default())),
            recovery_cache: Arc::new(Mutex::new(RecoveryCache::default())),
            lifecycle_hooks: Arc::new(Mutex::new(Vec::new())),
            telemetry_hooks: Arc::new(Mutex::new(Vec::new())),
            startup_recovery_micros,
            startup_recovered_seq,
        })
    }

    pub async fn commit(&self, mutation: Mutation) -> Result<CommitResult, AedbError> {
        let started = Instant::now();
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required; use commit_as in secure mode".into(),
            ));
        }
        // Early size validation to prevent DoS via oversized keys/values
        crate::commit::validation::validate_kv_sizes_early(&mutation, &self._config)?;

        // Fast-path atomic check avoids semaphore contention during normal operation
        if self.checkpoint_in_progress.load(Ordering::Acquire) {
            return Err(AedbError::CheckpointInProgress);
        }
        let _permit = self
            .checkpoint_gate
            .acquire()
            .await
            .expect("checkpoint gate semaphore should never be closed");
        let lifecycle_events = self
            .plan_lifecycle_events(std::slice::from_ref(&mutation))
            .await?;
        let result = self.executor.submit(mutation).await;
        self.emit_commit_telemetry("commit", started, &result);
        let result = result?;
        self.dispatch_lifecycle_events(lifecycle_events, result.commit_seq);
        Ok(result)
    }

    pub async fn commit_with_finality(
        &self,
        mutation: Mutation,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        let mut result = self.commit(mutation).await?;
        self.enforce_finality(&mut result, finality).await?;
        Ok(result)
    }

    pub async fn commit_with_preflight(
        &self,
        mutation: Mutation,
    ) -> Result<CommitResult, AedbError> {
        let started = Instant::now();
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required; use commit_as_with_preflight in secure mode".into(),
            ));
        }
        crate::commit::validation::validate_kv_sizes_early(&mutation, &self._config)?;
        let plan = self.preflight_plan(mutation).await;
        let result = commit_from_preflight_plan(self, None, plan).await;
        self.emit_commit_telemetry("commit_with_preflight", started, &result);
        result
    }

    pub async fn upsert_batch(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        rows: Vec<crate::catalog::types::Row>,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::UpsertBatch {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            table_name: table_name.to_string(),
            rows,
        })
        .await
    }

    pub async fn insert(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        primary_key: Vec<crate::catalog::types::Value>,
        row: crate::catalog::types::Row,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::Insert {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            table_name: table_name.to_string(),
            primary_key,
            row,
        })
        .await
    }

    pub async fn commit_as(
        &self,
        caller: CallerContext,
        mutation: Mutation,
    ) -> Result<CommitResult, AedbError> {
        let started = Instant::now();
        ensure_external_caller_allowed(&caller)?;
        // Early size validation to prevent DoS via oversized keys/values
        crate::commit::validation::validate_kv_sizes_early(&mutation, &self._config)?;

        if self.checkpoint_in_progress.load(Ordering::Acquire) {
            return Err(AedbError::CheckpointInProgress);
        }
        let _permit = self
            .checkpoint_gate
            .acquire()
            .await
            .expect("checkpoint gate semaphore should never be closed");
        let lifecycle_events = self
            .plan_lifecycle_events(std::slice::from_ref(&mutation))
            .await?;
        let result = self.executor.submit_as(Some(caller), mutation).await;
        self.emit_commit_telemetry("commit_as", started, &result);
        let result = result?;
        self.dispatch_lifecycle_events(lifecycle_events, result.commit_seq);
        Ok(result)
    }

    pub async fn commit_as_with_finality(
        &self,
        caller: CallerContext,
        mutation: Mutation,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        let mut result = self.commit_as(caller, mutation).await?;
        self.enforce_finality(&mut result, finality).await?;
        Ok(result)
    }

    pub async fn commit_as_with_preflight(
        &self,
        caller: CallerContext,
        mutation: Mutation,
    ) -> Result<CommitResult, AedbError> {
        let started = Instant::now();
        ensure_external_caller_allowed(&caller)?;
        crate::commit::validation::validate_kv_sizes_early(&mutation, &self._config)?;
        let plan = self.preflight_plan_as(&caller, mutation).await?;
        let result = commit_from_preflight_plan(self, Some(caller), plan).await;
        self.emit_commit_telemetry("commit_as_with_preflight", started, &result);
        result
    }

    pub async fn commit_envelope(
        &self,
        envelope: TransactionEnvelope,
    ) -> Result<CommitResult, AedbError> {
        let started = Instant::now();
        if self.require_authenticated_calls && envelope.caller.is_none() {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required; provide envelope.caller in secure mode".into(),
            ));
        }
        if let Some(caller) = envelope.caller.as_ref() {
            if caller.is_internal_system() {
                return Err(AedbError::PermissionDenied(
                    "internal system caller requires trusted commit path".into(),
                ));
            }
            ensure_external_caller_allowed(caller)?;
        }
        if self.checkpoint_in_progress.load(Ordering::Acquire) {
            return Err(AedbError::CheckpointInProgress);
        }
        let _permit = self
            .checkpoint_gate
            .acquire()
            .await
            .expect("checkpoint gate semaphore should never be closed");
        let lifecycle_events = self
            .plan_lifecycle_events(&envelope.write_intent.mutations)
            .await?;
        let result = self.executor.submit_envelope(envelope).await;
        self.emit_commit_telemetry("commit_envelope", started, &result);
        let result = result?;
        self.dispatch_lifecycle_events(lifecycle_events, result.commit_seq);
        Ok(result)
    }

    pub async fn commit_envelope_with_finality(
        &self,
        envelope: TransactionEnvelope,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        let mut result = self.commit_envelope(envelope).await?;
        self.enforce_finality(&mut result, finality).await?;
        Ok(result)
    }

    async fn enforce_finality(
        &self,
        result: &mut CommitResult,
        finality: CommitFinality,
    ) -> Result<(), AedbError> {
        if matches!(finality, CommitFinality::Durable) && result.durable_head_seq < result.commit_seq
        {
            self.wait_for_durable(result.commit_seq).await?;
            result.durable_head_seq = self.executor.durable_head_seq().await;
        }
        Ok(())
    }

    async fn plan_lifecycle_events(
        &self,
        mutations: &[Mutation],
    ) -> Result<Vec<LifecycleEventTemplate>, AedbError> {
        if !mutations.iter().any(|m| matches!(m, Mutation::Ddl(_))) {
            return Ok(Vec::new());
        }
        let (_, catalog, _) = self.executor.snapshot_state().await;
        let mut planned_catalog = catalog;
        let mut events = Vec::new();
        for mutation in mutations {
            let Mutation::Ddl(op) = mutation else {
                continue;
            };
            let applied = ddl_would_apply(&planned_catalog, op);
            planned_catalog.apply_ddl(op.clone())?;
            if applied && let Some(event) = lifecycle_template_for_ddl(op) {
                events.push(event);
            }
        }
        Ok(events)
    }

    fn dispatch_lifecycle_events(&self, events: Vec<LifecycleEventTemplate>, seq: u64) {
        if events.is_empty() {
            return;
        }
        let hooks = self.lifecycle_hooks.lock().clone();
        if hooks.is_empty() {
            return;
        }
        tokio::spawn(async move {
            for template in events {
                let event = template.with_seq(seq);
                for hook in &hooks {
                    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        hook.on_event(&event)
                    }))
                    .is_err()
                    {
                        warn!("lifecycle hook panicked while handling event");
                    }
                }
            }
        });
    }

    fn emit_query_telemetry(
        &self,
        started: Instant,
        project_id: &str,
        scope_id: &str,
        table: &str,
        snapshot_seq: u64,
        result: &Result<QueryResult, QueryError>,
    ) {
        let hooks = self.telemetry_hooks.lock().clone();
        if hooks.is_empty() {
            return;
        }
        let (rows_examined, ok, error) = match result {
            Ok(res) => (res.rows_examined, true, None),
            Err(err) => (0usize, false, Some(err.to_string())),
        };
        let event = QueryTelemetryEvent {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            table: table.to_string(),
            snapshot_seq,
            rows_examined,
            latency_micros: started.elapsed().as_micros() as u64,
            ok,
            error,
        };
        for hook in hooks {
            hook.on_query(&event);
        }
    }

    fn emit_commit_telemetry(
        &self,
        op: &'static str,
        started: Instant,
        result: &Result<CommitResult, AedbError>,
    ) {
        let hooks = self.telemetry_hooks.lock().clone();
        if hooks.is_empty() {
            return;
        }
        let (commit_seq, durable_head_seq, ok, error) = match result {
            Ok(res) => (Some(res.commit_seq), Some(res.durable_head_seq), true, None),
            Err(err) => (None, None, false, Some(err.to_string())),
        };
        let event = CommitTelemetryEvent {
            op,
            commit_seq,
            durable_head_seq,
            latency_micros: started.elapsed().as_micros() as u64,
            ok,
            error,
        };
        for hook in hooks {
            hook.on_commit(&event);
        }
    }

    pub async fn query(
        &self,
        project_id: &str,
        scope_id: &str,
        query: Query,
    ) -> Result<QueryResult, QueryError> {
        if self.require_authenticated_calls {
            return Err(QueryError::PermissionDenied {
                permission: "authenticated caller required in secure mode".into(),
                scope: "anonymous".into(),
            });
        }
        self.query_with_options_as(None, project_id, scope_id, query, QueryOptions::default())
            .await
    }

    pub(crate) async fn query_unchecked(
        &self,
        project_id: &str,
        scope_id: &str,
        query: Query,
        options: QueryOptions,
    ) -> Result<QueryResult, QueryError> {
        self.query_with_options_as(None, project_id, scope_id, query, options)
            .await
    }

    pub async fn query_no_auth(
        &self,
        project_id: &str,
        scope_id: &str,
        query: Query,
        options: QueryOptions,
    ) -> Result<QueryResult, QueryError> {
        assert!(
            !self.require_authenticated_calls,
            "query_no_auth called in secure/authenticated mode"
        );
        self.query_unchecked(project_id, scope_id, query, options)
            .await
    }

    pub async fn query_with_options(
        &self,
        project_id: &str,
        scope_id: &str,
        query: Query,
        options: QueryOptions,
    ) -> Result<QueryResult, QueryError> {
        if self.require_authenticated_calls {
            return Err(QueryError::PermissionDenied {
                permission: "authenticated caller required in secure mode".into(),
                scope: "anonymous".into(),
            });
        }
        self.query_with_options_as(None, project_id, scope_id, query, options)
            .await
    }

    pub async fn query_with_options_as(
        &self,
        caller: Option<&CallerContext>,
        project_id: &str,
        scope_id: &str,
        query: Query,
        mut options: QueryOptions,
    ) -> Result<QueryResult, QueryError> {
        if self.require_authenticated_calls && caller.is_none() {
            return Err(QueryError::PermissionDenied {
                permission: "authenticated caller required in secure mode".into(),
                scope: "anonymous".into(),
            });
        }

        if options.async_index.is_none() {
            options.async_index = query.use_index.clone();
        }

        if let Some(cursor) = &options.cursor {
            let token = parse_cursor_seq(cursor).map_err(QueryError::from)?;
            options.consistency = ConsistencyMode::AtSeq(token);
        }

        let lease = self
            .acquire_snapshot(options.consistency)
            .await
            .map_err(QueryError::from)?;
        let started = Instant::now();
        let table = query.table.clone();
        let result = execute_query_against_view(
            &lease.view,
            project_id,
            scope_id,
            query,
            &options,
            caller,
            self._config.max_scan_rows,
        );
        self.emit_query_telemetry(
            started,
            project_id,
            scope_id,
            &table,
            lease.view.seq,
            &result,
        );
        result
    }

    pub async fn begin_read_tx(
        &self,
        consistency: ConsistencyMode,
    ) -> Result<ReadTx<'_>, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode; use begin_read_tx_as".into(),
            ));
        }
        let lease = self.acquire_snapshot(consistency).await?;
        Ok(ReadTx {
            db: self,
            lease,
            caller: None,
        })
    }

    pub async fn begin_read_tx_as(
        &self,
        caller: CallerContext,
        consistency: ConsistencyMode,
    ) -> Result<ReadTx<'_>, AedbError> {
        ensure_query_caller_allowed(&caller).map_err(|err| match err {
            QueryError::PermissionDenied { permission, .. } => {
                AedbError::PermissionDenied(permission)
            }
            other => AedbError::Validation(other.to_string()),
        })?;
        let lease = self.acquire_snapshot(consistency).await?;
        Ok(ReadTx {
            db: self,
            lease,
            caller: Some(caller),
        })
    }

    pub async fn query_page_stable(
        &self,
        project_id: &str,
        scope_id: &str,
        mut query: Query,
        cursor: Option<String>,
        page_size: usize,
        consistency: ConsistencyMode,
    ) -> Result<QueryResult, QueryError> {
        if query.joins.is_empty() && query.order_by.is_empty() {
            let (_, catalog, _) = self.executor.snapshot_state().await;
            let (q_project, q_scope, q_table) =
                resolve_query_table_ref(project_id, scope_id, &query.table);
            if let Some(schema) = catalog
                .tables
                .get(&(namespace_key(&q_project, &q_scope), q_table))
            {
                for pk in &schema.primary_key {
                    query = query.order_by(pk, Order::Asc);
                }
            }
        }
        query.limit = Some(page_size.max(1));
        self.query_with_options(
            project_id,
            scope_id,
            query,
            QueryOptions {
                consistency,
                cursor,
                ..QueryOptions::default()
            },
        )
        .await
    }

    pub async fn query_batch(
        &self,
        project_id: &str,
        scope_id: &str,
        items: Vec<QueryBatchItem>,
        consistency: ConsistencyMode,
    ) -> Result<Vec<QueryResult>, QueryError> {
        let tx = self
            .begin_read_tx(consistency)
            .await
            .map_err(QueryError::from)?;
        tx.query_batch(project_id, scope_id, items).await
    }

    pub async fn query_batch_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        items: Vec<QueryBatchItem>,
        consistency: ConsistencyMode,
    ) -> Result<Vec<QueryResult>, QueryError> {
        let tx = self
            .begin_read_tx_as(caller, consistency)
            .await
            .map_err(QueryError::from)?;
        tx.query_batch(project_id, scope_id, items).await
    }

    pub async fn exists(
        &self,
        project_id: &str,
        scope_id: &str,
        query: Query,
        consistency: ConsistencyMode,
    ) -> Result<bool, QueryError> {
        let tx = self
            .begin_read_tx(consistency)
            .await
            .map_err(QueryError::from)?;
        tx.exists(project_id, scope_id, query).await
    }

    pub async fn exists_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        query: Query,
        consistency: ConsistencyMode,
    ) -> Result<bool, QueryError> {
        let tx = self
            .begin_read_tx_as(caller, consistency)
            .await
            .map_err(QueryError::from)?;
        tx.exists(project_id, scope_id, query).await
    }

    pub async fn explain_query(
        &self,
        project_id: &str,
        scope_id: &str,
        query: Query,
        options: QueryOptions,
    ) -> Result<QueryDiagnostics, QueryError> {
        self.explain_query_as(None, project_id, scope_id, query, options)
            .await
    }

    pub async fn explain_query_as(
        &self,
        caller: Option<&CallerContext>,
        project_id: &str,
        scope_id: &str,
        query: Query,
        options: QueryOptions,
    ) -> Result<QueryDiagnostics, QueryError> {
        if self.require_authenticated_calls && caller.is_none() {
            return Err(QueryError::PermissionDenied {
                permission: "authenticated caller required in secure mode".into(),
                scope: "anonymous".into(),
            });
        }

        let lease = self
            .acquire_snapshot(options.consistency)
            .await
            .map_err(QueryError::from)?;
        explain_query_against_view(
            &lease.view,
            project_id,
            scope_id,
            query,
            &options,
            caller,
            self._config.max_scan_rows,
        )
    }

    pub async fn query_with_diagnostics(
        &self,
        project_id: &str,
        scope_id: &str,
        query: Query,
        options: QueryOptions,
    ) -> Result<QueryWithDiagnosticsResult, QueryError> {
        let diagnostics = self
            .explain_query(project_id, scope_id, query.clone(), options.clone())
            .await?;
        let result = self
            .query_with_options(project_id, scope_id, query, options)
            .await?;
        Ok(QueryWithDiagnosticsResult {
            result,
            diagnostics,
        })
    }

    pub async fn query_with_diagnostics_as(
        &self,
        caller: &CallerContext,
        project_id: &str,
        scope_id: &str,
        query: Query,
        options: QueryOptions,
    ) -> Result<QueryWithDiagnosticsResult, QueryError> {
        let diagnostics = self
            .explain_query_as(
                Some(caller),
                project_id,
                scope_id,
                query.clone(),
                options.clone(),
            )
            .await?;
        let result = self
            .query_with_options_as(Some(caller), project_id, scope_id, query, options)
            .await?;
        Ok(QueryWithDiagnosticsResult {
            result,
            diagnostics,
        })
    }

    pub async fn list_with_total_with(
        &self,
        request: ListWithTotalRequest,
    ) -> Result<ListPageResult, QueryError> {
        let tx = self
            .begin_read_tx(request.consistency)
            .await
            .map_err(QueryError::from)?;
        tx.list_with_total(
            &request.project_id,
            &request.scope_id,
            request.query,
            request.cursor,
            request.offset,
            request.page_size,
        )
        .await
    }

    pub async fn list_with_total_as_with(
        &self,
        caller: CallerContext,
        request: ListWithTotalRequest,
    ) -> Result<ListPageResult, QueryError> {
        let tx = self
            .begin_read_tx_as(caller, request.consistency)
            .await
            .map_err(QueryError::from)?;
        tx.list_with_total(
            &request.project_id,
            &request.scope_id,
            request.query,
            request.cursor,
            request.offset,
            request.page_size,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn list_with_total(
        &self,
        project_id: &str,
        scope_id: &str,
        query: Query,
        cursor: Option<String>,
        offset: Option<usize>,
        page_size: usize,
        consistency: ConsistencyMode,
    ) -> Result<ListPageResult, QueryError> {
        self.list_with_total_with(ListWithTotalRequest {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            query,
            cursor,
            offset,
            page_size,
            consistency,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn list_with_total_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        query: Query,
        cursor: Option<String>,
        offset: Option<usize>,
        page_size: usize,
        consistency: ConsistencyMode,
    ) -> Result<ListPageResult, QueryError> {
        self.list_with_total_as_with(
            caller,
            ListWithTotalRequest {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                query,
                cursor,
                offset,
                page_size,
                consistency,
            },
        )
        .await
    }

    pub async fn lookup_then_hydrate_with(
        &self,
        request: LookupThenHydrateRequest,
    ) -> Result<(QueryResult, QueryResult), QueryError> {
        let tx = self
            .begin_read_tx(request.consistency)
            .await
            .map_err(QueryError::from)?;
        tx.lookup_then_hydrate(
            &request.project_id,
            &request.scope_id,
            request.source_query,
            request.source_key_index,
            request.hydrate_query,
            &request.hydrate_key_column,
        )
        .await
    }

    pub async fn lookup_then_hydrate_as_with(
        &self,
        caller: CallerContext,
        request: LookupThenHydrateRequest,
    ) -> Result<(QueryResult, QueryResult), QueryError> {
        let tx = self
            .begin_read_tx_as(caller, request.consistency)
            .await
            .map_err(QueryError::from)?;
        tx.lookup_then_hydrate(
            &request.project_id,
            &request.scope_id,
            request.source_query,
            request.source_key_index,
            request.hydrate_query,
            &request.hydrate_key_column,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn lookup_then_hydrate(
        &self,
        project_id: &str,
        scope_id: &str,
        source_query: Query,
        source_key_index: usize,
        hydrate_query: Query,
        hydrate_key_column: &str,
        consistency: ConsistencyMode,
    ) -> Result<(QueryResult, QueryResult), QueryError> {
        self.lookup_then_hydrate_with(LookupThenHydrateRequest {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            source_query,
            source_key_index,
            hydrate_query,
            hydrate_key_column: hydrate_key_column.to_string(),
            consistency,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn lookup_then_hydrate_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        source_query: Query,
        source_key_index: usize,
        hydrate_query: Query,
        hydrate_key_column: &str,
        consistency: ConsistencyMode,
    ) -> Result<(QueryResult, QueryResult), QueryError> {
        self.lookup_then_hydrate_as_with(
            caller,
            LookupThenHydrateRequest {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                source_query,
                source_key_index,
                hydrate_query,
                hydrate_key_column: hydrate_key_column.to_string(),
                consistency,
            },
        )
        .await
    }

    pub async fn query_sql_read_only(
        &self,
        adapter: &dyn ReadOnlySqlAdapter,
        project_id: &str,
        scope_id: &str,
        sql: &str,
        consistency: ConsistencyMode,
    ) -> Result<QueryResult, QueryError> {
        let (query, mut options) = adapter.execute_read_only(project_id, scope_id, sql)?;
        options.consistency = consistency;
        options.allow_full_scan = true;
        self.query_with_options(project_id, scope_id, query, options)
            .await
    }

    pub async fn query_sql_read_only_as(
        &self,
        adapter: &dyn ReadOnlySqlAdapter,
        caller: &CallerContext,
        project_id: &str,
        scope_id: &str,
        sql: &str,
        consistency: ConsistencyMode,
    ) -> Result<QueryResult, QueryError> {
        let (query, mut options) = adapter.execute_read_only(project_id, scope_id, sql)?;
        options.consistency = consistency;
        options.allow_full_scan = true;
        self.query_with_options_as(Some(caller), project_id, scope_id, query, options)
            .await
    }

    pub async fn plan_sql_transaction(
        &self,
        consistency: ConsistencyMode,
        mutations: Vec<Mutation>,
    ) -> Result<SqlTransactionPlan, AedbError> {
        if mutations.is_empty() {
            return Err(AedbError::Validation(
                "sql transaction plan requires at least one mutation".into(),
            ));
        }
        Ok(SqlTransactionPlan {
            base_seq: self.snapshot_probe(consistency).await?,
            caller: None,
            mutations,
        })
    }

    pub async fn plan_sql_transaction_as(
        &self,
        caller: CallerContext,
        consistency: ConsistencyMode,
        mutations: Vec<Mutation>,
    ) -> Result<SqlTransactionPlan, AedbError> {
        if mutations.is_empty() {
            return Err(AedbError::Validation(
                "sql transaction plan requires at least one mutation".into(),
            ));
        }
        ensure_external_caller_allowed(&caller)?;
        Ok(SqlTransactionPlan {
            base_seq: self.snapshot_probe(consistency).await?,
            caller: Some(caller),
            mutations,
        })
    }

    pub async fn commit_sql_transaction_plan(
        &self,
        plan: SqlTransactionPlan,
    ) -> Result<CommitResult, AedbError> {
        self.commit_envelope(TransactionEnvelope {
            caller: plan.caller,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: plan.mutations,
            },
            base_seq: plan.base_seq,
        })
        .await
    }

    pub async fn commit_sql_transaction(
        &self,
        consistency: ConsistencyMode,
        mutations: Vec<Mutation>,
    ) -> Result<CommitResult, AedbError> {
        let plan = self.plan_sql_transaction(consistency, mutations).await?;
        self.commit_sql_transaction_plan(plan).await
    }

    pub async fn commit_sql_transaction_as(
        &self,
        caller: CallerContext,
        consistency: ConsistencyMode,
        mutations: Vec<Mutation>,
    ) -> Result<CommitResult, AedbError> {
        let plan = self
            .plan_sql_transaction_as(caller, consistency, mutations)
            .await?;
        self.commit_sql_transaction_plan(plan).await
    }

    pub async fn commit_many_atomic(
        &self,
        mutations: Vec<Mutation>,
    ) -> Result<CommitResult, AedbError> {
        if mutations.is_empty() {
            return Err(AedbError::Validation(
                "transaction envelope has no mutations".into(),
            ));
        }
        self.commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: ReadSet::default(),
            write_intent: WriteIntent { mutations },
            base_seq: self.snapshot_probe(ConsistencyMode::AtLatest).await?,
        })
        .await
    }

    pub async fn commit_many_atomic_as(
        &self,
        caller: CallerContext,
        mutations: Vec<Mutation>,
    ) -> Result<CommitResult, AedbError> {
        if mutations.is_empty() {
            return Err(AedbError::Validation(
                "transaction envelope has no mutations".into(),
            ));
        }
        self.commit_envelope(TransactionEnvelope {
            caller: Some(caller),
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: ReadSet::default(),
            write_intent: WriteIntent { mutations },
            base_seq: self.snapshot_probe(ConsistencyMode::AtLatest).await?,
        })
        .await
    }

    pub async fn delete_where(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        predicate: Expr,
        limit: Option<usize>,
    ) -> Result<Option<CommitResult>, AedbError> {
        let result = self
            .commit(Mutation::DeleteWhere {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: table_name.to_string(),
                predicate,
                limit,
            })
            .await?;
        Ok(Some(result))
    }

    pub async fn delete_where_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        predicate: Expr,
        limit: Option<usize>,
    ) -> Result<Option<CommitResult>, AedbError> {
        let result = self
            .commit_as(
                caller,
                Mutation::DeleteWhere {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    table_name: table_name.to_string(),
                    predicate,
                    limit,
                },
            )
            .await?;
        Ok(Some(result))
    }

    pub async fn update_where(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        predicate: Expr,
        updates: Vec<(String, Value)>,
        limit: Option<usize>,
    ) -> Result<Option<CommitResult>, AedbError> {
        let result = self
            .commit(Mutation::UpdateWhere {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: table_name.to_string(),
                predicate,
                updates,
                limit,
            })
            .await?;
        Ok(Some(result))
    }

    pub async fn update_where_as_with(
        &self,
        request: UpdateWhereRequest,
    ) -> Result<Option<CommitResult>, AedbError> {
        let result = self
            .commit_as(
                request.caller,
                Mutation::UpdateWhere {
                    project_id: request.project_id,
                    scope_id: request.scope_id,
                    table_name: request.table_name,
                    predicate: request.predicate,
                    updates: request.updates,
                    limit: request.limit,
                },
            )
            .await?;
        Ok(Some(result))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_where_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        predicate: Expr,
        updates: Vec<(String, Value)>,
        limit: Option<usize>,
    ) -> Result<Option<CommitResult>, AedbError> {
        self.update_where_as_with(UpdateWhereRequest {
            caller,
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            table_name: table_name.to_string(),
            predicate,
            updates,
            limit,
        })
        .await
    }

    pub async fn update_where_expr(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        predicate: Expr,
        updates: Vec<(String, TableUpdateExpr)>,
        limit: Option<usize>,
    ) -> Result<Option<CommitResult>, AedbError> {
        let result = self
            .commit(Mutation::UpdateWhereExpr {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: table_name.to_string(),
                predicate,
                updates,
                limit,
            })
            .await?;
        Ok(Some(result))
    }

    pub async fn update_where_expr_as_with(
        &self,
        request: UpdateWhereExprRequest,
    ) -> Result<Option<CommitResult>, AedbError> {
        let result = self
            .commit_as(
                request.caller,
                Mutation::UpdateWhereExpr {
                    project_id: request.project_id,
                    scope_id: request.scope_id,
                    table_name: request.table_name,
                    predicate: request.predicate,
                    updates: request.updates,
                    limit: request.limit,
                },
            )
            .await?;
        Ok(Some(result))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_where_expr_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        predicate: Expr,
        updates: Vec<(String, TableUpdateExpr)>,
        limit: Option<usize>,
    ) -> Result<Option<CommitResult>, AedbError> {
        self.update_where_expr_as_with(UpdateWhereExprRequest {
            caller,
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            table_name: table_name.to_string(),
            predicate,
            updates,
            limit,
        })
        .await
    }

    pub async fn mutate_where_returning(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        predicate: Expr,
        updates: Vec<(String, TableUpdateExpr)>,
    ) -> Result<Option<MutateWhereReturningResult>, AedbError> {
        self.mutate_where_returning_inner(
            None, project_id, scope_id, table_name, predicate, updates,
        )
        .await
    }

    pub async fn mutate_where_returning_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        predicate: Expr,
        updates: Vec<(String, TableUpdateExpr)>,
    ) -> Result<Option<MutateWhereReturningResult>, AedbError> {
        self.mutate_where_returning_inner(
            Some(caller),
            project_id,
            scope_id,
            table_name,
            predicate,
            updates,
        )
        .await
    }

    pub async fn claim_one(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        predicate: Expr,
        updates: Vec<(String, TableUpdateExpr)>,
    ) -> Result<Option<MutateWhereReturningResult>, AedbError> {
        self.mutate_where_returning(project_id, scope_id, table_name, predicate, updates)
            .await
    }

    pub async fn claim_one_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        predicate: Expr,
        updates: Vec<(String, TableUpdateExpr)>,
    ) -> Result<Option<MutateWhereReturningResult>, AedbError> {
        self.mutate_where_returning_as(caller, project_id, scope_id, table_name, predicate, updates)
            .await
    }

    pub async fn kv_get(
        &self,
        project_id: &str,
        scope_id: &str,
        key: &[u8],
        consistency: ConsistencyMode,
        caller: &CallerContext,
    ) -> Result<Option<KvEntry>, QueryError> {
        ensure_query_caller_allowed(caller)?;
        let lease = self
            .acquire_snapshot(consistency)
            .await
            .map_err(QueryError::from)?;
        let snapshot = &lease.view.keyspace;
        let catalog = &lease.view.catalog;
        if !catalog.has_kv_read_permission(&caller.caller_id, project_id, scope_id, key) {
            return Err(QueryError::PermissionDenied {
                permission: format!("KvRead({project_id}.{scope_id})"),
                scope: caller.caller_id.clone(),
            });
        }
        Ok(snapshot.kv_get(project_id, scope_id, key).cloned())
    }

    pub async fn kv_get_default_scope(
        &self,
        project_id: &str,
        key: &[u8],
        consistency: ConsistencyMode,
        caller: &CallerContext,
    ) -> Result<Option<KvEntry>, QueryError> {
        self.kv_get(
            project_id,
            crate::catalog::DEFAULT_SCOPE_ID,
            key,
            consistency,
            caller,
        )
        .await
    }

    pub(crate) async fn kv_get_unchecked(
        &self,
        project_id: &str,
        scope_id: &str,
        key: &[u8],
        consistency: ConsistencyMode,
    ) -> Result<Option<KvEntry>, QueryError> {
        let lease = self
            .acquire_snapshot(consistency)
            .await
            .map_err(QueryError::from)?;
        Ok(lease
            .view
            .keyspace
            .kv_get(project_id, scope_id, key)
            .cloned())
    }

    pub async fn kv_get_no_auth(
        &self,
        project_id: &str,
        scope_id: &str,
        key: &[u8],
        consistency: ConsistencyMode,
    ) -> Result<Option<KvEntry>, QueryError> {
        assert!(
            !self.require_authenticated_calls,
            "kv_get_no_auth called in secure/authenticated mode"
        );
        self.kv_get_unchecked(project_id, scope_id, key, consistency)
            .await
    }

    pub(crate) async fn kv_scan_prefix_unchecked(
        &self,
        project_id: &str,
        scope_id: &str,
        prefix: &[u8],
        limit: u64,
        consistency: ConsistencyMode,
    ) -> Result<Vec<(Vec<u8>, KvEntry)>, QueryError> {
        let lease = self
            .acquire_snapshot(consistency)
            .await
            .map_err(QueryError::from)?;
        let page_size = limit.min(self._config.max_scan_rows as u64) as usize;
        let start_bound = Bound::Included(prefix.to_vec());
        let end_bound = next_prefix_bytes(prefix).map_or(Bound::Unbounded, Bound::Excluded);
        let ns = NamespaceId::project_scope(project_id, scope_id);
        let mut entries = Vec::new();
        if let Some(kv) = lease.view.keyspace.namespaces.get(&ns).map(|n| &n.kv) {
            for (k, v) in kv.entries.range((start_bound, end_bound)) {
                if !k.starts_with(prefix) {
                    break;
                }
                entries.push((k.clone(), v.clone()));
                if entries.len() == page_size {
                    break;
                }
            }
        }
        Ok(entries)
    }

    pub async fn kv_scan_prefix_no_auth(
        &self,
        project_id: &str,
        scope_id: &str,
        prefix: &[u8],
        limit: u64,
        consistency: ConsistencyMode,
    ) -> Result<Vec<(Vec<u8>, KvEntry)>, QueryError> {
        assert!(
            !self.require_authenticated_calls,
            "kv_scan_prefix_no_auth called in secure/authenticated mode"
        );
        self.kv_scan_prefix_unchecked(project_id, scope_id, prefix, limit, consistency)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn kv_scan_prefix(
        &self,
        project_id: &str,
        scope_id: &str,
        prefix: &[u8],
        limit: u64,
        cursor: Option<KvCursor>,
        consistency: ConsistencyMode,
        caller: &CallerContext,
    ) -> Result<KvScanResult, QueryError> {
        ensure_query_caller_allowed(caller)?;
        let effective_consistency = if let Some(c) = &cursor {
            ConsistencyMode::AtSeq(c.snapshot_seq)
        } else {
            consistency
        };
        let lease = self
            .acquire_snapshot(effective_consistency)
            .await
            .map_err(QueryError::from)?;
        let snapshot = &lease.view.keyspace;
        let catalog = &lease.view.catalog;
        let snapshot_seq = lease.view.seq;
        let allowed_prefixes =
            match catalog.kv_read_prefixes_for_caller(&caller.caller_id, project_id, scope_id) {
                Some(prefixes) => prefixes,
                None => {
                    return Err(QueryError::PermissionDenied {
                        permission: format!("KvRead({project_id}.{scope_id})"),
                        scope: caller.caller_id.clone(),
                    });
                }
            };
        if let Some(c) = &cursor
            && c.snapshot_seq != snapshot_seq
        {
            return Err(QueryError::InvalidQuery {
                reason: "cursor snapshot_seq mismatch".into(),
            });
        }
        let page_size = limit.min(self._config.max_scan_rows as u64) as usize;
        let start_bound = cursor
            .as_ref()
            .map_or(Bound::Included(prefix.to_vec()), |c| {
                Bound::Excluded(c.last_key.clone())
            });
        let end_bound = next_prefix_bytes(prefix).map_or(Bound::Unbounded, Bound::Excluded);

        let ns = NamespaceId::project_scope(project_id, scope_id);
        let mut entries = Vec::new();
        if let Some(kv) = snapshot.namespaces.get(&ns).map(|n| &n.kv) {
            for (k, v) in kv.entries.range((start_bound, end_bound)) {
                if !k.starts_with(prefix) {
                    break;
                }
                if !allowed_prefixes.is_empty()
                    && !allowed_prefixes
                        .iter()
                        .any(|allowed| k.starts_with(allowed))
                {
                    continue;
                }
                entries.push((k.clone(), v.clone()));
                if entries.len() > page_size {
                    break;
                }
            }
        }

        let truncated = entries.len() > page_size;
        if truncated {
            entries.truncate(page_size);
        }
        let next_cursor = if truncated {
            entries.last().map(|(k, _)| KvCursor {
                snapshot_seq,
                last_key: k.clone(),
                page_size: page_size as u64,
            })
        } else {
            None
        };
        Ok(KvScanResult {
            entries,
            cursor: next_cursor,
            snapshot_seq,
            truncated,
        })
    }

    pub async fn kv_scan_prefix_default_scope(
        &self,
        project_id: &str,
        prefix: &[u8],
        limit: u64,
        cursor: Option<KvCursor>,
        consistency: ConsistencyMode,
        caller: &CallerContext,
    ) -> Result<KvScanResult, QueryError> {
        self.kv_scan_prefix(
            project_id,
            crate::catalog::DEFAULT_SCOPE_ID,
            prefix,
            limit,
            cursor,
            consistency,
            caller,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn kv_scan_range(
        &self,
        project_id: &str,
        scope_id: &str,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
        limit: u64,
        cursor: Option<KvCursor>,
        consistency: ConsistencyMode,
        caller: &CallerContext,
    ) -> Result<KvScanResult, QueryError> {
        ensure_query_caller_allowed(caller)?;
        let effective_consistency = if let Some(c) = &cursor {
            ConsistencyMode::AtSeq(c.snapshot_seq)
        } else {
            consistency
        };
        let lease = self
            .acquire_snapshot(effective_consistency)
            .await
            .map_err(QueryError::from)?;
        let snapshot = &lease.view.keyspace;
        let catalog = &lease.view.catalog;
        let snapshot_seq = lease.view.seq;
        let allowed_prefixes =
            match catalog.kv_read_prefixes_for_caller(&caller.caller_id, project_id, scope_id) {
                Some(prefixes) => prefixes,
                None => {
                    return Err(QueryError::PermissionDenied {
                        permission: format!("KvRead({project_id}.{scope_id})"),
                        scope: caller.caller_id.clone(),
                    });
                }
            };
        if let Some(c) = &cursor
            && c.snapshot_seq != snapshot_seq
        {
            return Err(QueryError::InvalidQuery {
                reason: "cursor snapshot_seq mismatch".into(),
            });
        }
        let page_size = limit.min(self._config.max_scan_rows as u64) as usize;
        let adjusted_start = match (&cursor, start) {
            (Some(c), _) => Bound::Excluded(c.last_key.clone()),
            (None, b) => b,
        };

        let ns = NamespaceId::project_scope(project_id, scope_id);
        let mut entries = Vec::new();
        if let Some(kv) = snapshot.namespaces.get(&ns).map(|n| &n.kv) {
            for (k, v) in kv.entries.range((adjusted_start, end)) {
                if !allowed_prefixes.is_empty()
                    && !allowed_prefixes
                        .iter()
                        .any(|allowed| k.starts_with(allowed))
                {
                    continue;
                }
                entries.push((k.clone(), v.clone()));
                if entries.len() > page_size {
                    break;
                }
            }
        }

        let truncated = entries.len() > page_size;
        if truncated {
            entries.truncate(page_size);
        }
        let next_cursor = if truncated {
            entries.last().map(|(k, _)| KvCursor {
                snapshot_seq,
                last_key: k.clone(),
                page_size: page_size as u64,
            })
        } else {
            None
        };
        Ok(KvScanResult {
            entries,
            cursor: next_cursor,
            snapshot_seq,
            truncated,
        })
    }

    pub async fn kv_scan_all_scopes(
        &self,
        project_id: &str,
        prefix: &[u8],
        limit: u64,
        consistency: ConsistencyMode,
        caller: &CallerContext,
    ) -> Result<Vec<ScopedKvEntry>, QueryError> {
        ensure_query_caller_allowed(caller)?;
        let (_, catalog, _) = self.executor.snapshot_state().await;
        let project_read = catalog.has_permission(
            &caller.caller_id,
            &Permission::KvRead {
                project_id: project_id.to_string(),
                scope_id: None,
                prefix: None,
            },
        ) || catalog.has_permission(
            &caller.caller_id,
            &Permission::ProjectAdmin {
                project_id: project_id.to_string(),
            },
        );
        if !project_read {
            return Err(QueryError::PermissionDenied {
                permission: format!("KvRead({project_id}.*)"),
                scope: caller.caller_id.clone(),
            });
        }
        let lease = self
            .acquire_snapshot(consistency)
            .await
            .map_err(QueryError::from)?;
        let snapshot = &lease.view.keyspace;
        let mut out = Vec::new();
        for (ns_id, ns) in snapshot.namespaces.iter() {
            let Some(ns_key) = ns_id.as_project_scope_key() else {
                continue;
            };
            let Some((p, scope)) = ns_key.split_once("::") else {
                continue;
            };
            if p != project_id {
                continue;
            }
            for (k, v) in ns.kv.entries.iter() {
                if !k.starts_with(prefix) {
                    continue;
                }
                out.push(ScopedKvEntry {
                    scope_id: scope.to_string(),
                    key: k.clone(),
                    value: v.value.clone(),
                    version: v.version,
                });
                if out.len() >= limit as usize {
                    return Ok(out);
                }
            }
        }
        Ok(out)
    }

    pub async fn kv_set(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvSet {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            value,
        })
        .await
    }

    pub async fn kv_set_default_scope(
        &self,
        project_id: &str,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<CommitResult, AedbError> {
        self.kv_set(project_id, crate::catalog::DEFAULT_SCOPE_ID, key, value)
            .await
    }

    pub async fn kv_set_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::KvSet {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key,
                value,
            },
        )
        .await
    }

    pub async fn kv_del(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvDel {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
        })
        .await
    }

    pub async fn kv_del_default_scope(
        &self,
        project_id: &str,
        key: Vec<u8>,
    ) -> Result<CommitResult, AedbError> {
        self.kv_del(project_id, crate::catalog::DEFAULT_SCOPE_ID, key)
            .await
    }

    pub async fn kv_del_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::KvDel {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key,
            },
        )
        .await
    }

    pub async fn kv_inc_u256(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 32],
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvIncU256 {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            amount_be,
        })
        .await
    }

    pub async fn kv_inc_u256_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 32],
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::KvIncU256 {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key,
                amount_be,
            },
        )
        .await
    }

    pub async fn kv_dec_u256(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 32],
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvDecU256 {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            amount_be,
        })
        .await
    }

    pub async fn kv_dec_u256_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 32],
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::KvDecU256 {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key,
                amount_be,
            },
        )
        .await
    }

    pub async fn kv_compare_and_swap(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        value: Vec<u8>,
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: crate::commit::tx::WriteClass::Standard,
            assertions: vec![crate::commit::tx::ReadAssertion::KeyVersion {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key: key.clone(),
                expected_seq,
            }],
            read_set: crate::commit::tx::ReadSet::default(),
            write_intent: crate::commit::tx::WriteIntent {
                mutations: vec![Mutation::KvSet {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    key,
                    value,
                }],
            },
            base_seq: self.snapshot_probe(ConsistencyMode::AtLatest).await?,
        })
        .await
    }

    pub async fn kv_compare_and_swap_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        value: Vec<u8>,
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit_envelope(TransactionEnvelope {
            caller: Some(caller),
            idempotency_key: None,
            write_class: crate::commit::tx::WriteClass::Standard,
            assertions: vec![crate::commit::tx::ReadAssertion::KeyVersion {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key: key.clone(),
                expected_seq,
            }],
            read_set: crate::commit::tx::ReadSet::default(),
            write_intent: crate::commit::tx::WriteIntent {
                mutations: vec![Mutation::KvSet {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    key,
                    value,
                }],
            },
            base_seq: self.snapshot_probe(ConsistencyMode::AtLatest).await?,
        })
        .await
    }

    pub async fn kv_compare_and_inc_u256(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 32],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: crate::commit::tx::WriteClass::Standard,
            assertions: vec![crate::commit::tx::ReadAssertion::KeyVersion {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key: key.clone(),
                expected_seq,
            }],
            read_set: crate::commit::tx::ReadSet::default(),
            write_intent: crate::commit::tx::WriteIntent {
                mutations: vec![Mutation::KvIncU256 {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    key,
                    amount_be,
                }],
            },
            base_seq: self.snapshot_probe(ConsistencyMode::AtLatest).await?,
        })
        .await
    }

    pub async fn kv_compare_and_inc_u256_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 32],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit_envelope(TransactionEnvelope {
            caller: Some(caller),
            idempotency_key: None,
            write_class: crate::commit::tx::WriteClass::Standard,
            assertions: vec![crate::commit::tx::ReadAssertion::KeyVersion {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key: key.clone(),
                expected_seq,
            }],
            read_set: crate::commit::tx::ReadSet::default(),
            write_intent: crate::commit::tx::WriteIntent {
                mutations: vec![Mutation::KvIncU256 {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    key,
                    amount_be,
                }],
            },
            base_seq: self.snapshot_probe(ConsistencyMode::AtLatest).await?,
        })
        .await
    }

    pub async fn kv_compare_and_dec_u256(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 32],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: crate::commit::tx::WriteClass::Standard,
            assertions: vec![crate::commit::tx::ReadAssertion::KeyVersion {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key: key.clone(),
                expected_seq,
            }],
            read_set: crate::commit::tx::ReadSet::default(),
            write_intent: crate::commit::tx::WriteIntent {
                mutations: vec![Mutation::KvDecU256 {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    key,
                    amount_be,
                }],
            },
            base_seq: self.snapshot_probe(ConsistencyMode::AtLatest).await?,
        })
        .await
    }

    pub async fn kv_compare_and_dec_u256_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 32],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit_envelope(TransactionEnvelope {
            caller: Some(caller),
            idempotency_key: None,
            write_class: crate::commit::tx::WriteClass::Standard,
            assertions: vec![crate::commit::tx::ReadAssertion::KeyVersion {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key: key.clone(),
                expected_seq,
            }],
            read_set: crate::commit::tx::ReadSet::default(),
            write_intent: crate::commit::tx::WriteIntent {
                mutations: vec![Mutation::KvDecU256 {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    key,
                    amount_be,
                }],
            },
            base_seq: self.snapshot_probe(ConsistencyMode::AtLatest).await?,
        })
        .await
    }

    pub async fn table_inc_u256(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        primary_key: Vec<Value>,
        column: &str,
        amount_be: [u8; 32],
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::TableIncU256 {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            table_name: table_name.to_string(),
            primary_key,
            column: column.to_string(),
            amount_be,
        })
        .await
    }

    pub async fn table_inc_u256_as_with(
        &self,
        request: TableU256MutationRequest,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            request.caller,
            Mutation::TableIncU256 {
                project_id: request.project_id,
                scope_id: request.scope_id,
                table_name: request.table_name,
                primary_key: request.primary_key,
                column: request.column,
                amount_be: request.amount_be,
            },
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn table_inc_u256_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        primary_key: Vec<Value>,
        column: &str,
        amount_be: [u8; 32],
    ) -> Result<CommitResult, AedbError> {
        self.table_inc_u256_as_with(TableU256MutationRequest {
            caller,
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            table_name: table_name.to_string(),
            primary_key,
            column: column.to_string(),
            amount_be,
        })
        .await
    }

    pub async fn table_dec_u256(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        primary_key: Vec<Value>,
        column: &str,
        amount_be: [u8; 32],
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::TableDecU256 {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            table_name: table_name.to_string(),
            primary_key,
            column: column.to_string(),
            amount_be,
        })
        .await
    }

    pub async fn table_dec_u256_as_with(
        &self,
        request: TableU256MutationRequest,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            request.caller,
            Mutation::TableDecU256 {
                project_id: request.project_id,
                scope_id: request.scope_id,
                table_name: request.table_name,
                primary_key: request.primary_key,
                column: request.column,
                amount_be: request.amount_be,
            },
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn table_dec_u256_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        primary_key: Vec<Value>,
        column: &str,
        amount_be: [u8; 32],
    ) -> Result<CommitResult, AedbError> {
        self.table_dec_u256_as_with(TableU256MutationRequest {
            caller,
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            table_name: table_name.to_string(),
            primary_key,
            column: column.to_string(),
            amount_be,
        })
        .await
    }

    pub async fn compare_and_swap(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        primary_key: Vec<Value>,
        row: Row,
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: crate::commit::tx::WriteClass::Standard,
            assertions: vec![crate::commit::tx::ReadAssertion::RowVersion {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: table_name.to_string(),
                primary_key: primary_key.clone(),
                expected_seq,
            }],
            read_set: crate::commit::tx::ReadSet::default(),
            write_intent: crate::commit::tx::WriteIntent {
                mutations: vec![Mutation::Upsert {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    table_name: table_name.to_string(),
                    primary_key,
                    row,
                }],
            },
            base_seq: self.snapshot_probe(ConsistencyMode::AtLatest).await?,
        })
        .await
    }

    pub async fn compare_and_swap_as_with(
        &self,
        request: CompareAndSwapRequest,
    ) -> Result<CommitResult, AedbError> {
        self.commit_envelope(TransactionEnvelope {
            caller: Some(request.caller),
            idempotency_key: None,
            write_class: crate::commit::tx::WriteClass::Standard,
            assertions: vec![crate::commit::tx::ReadAssertion::RowVersion {
                project_id: request.project_id.clone(),
                scope_id: request.scope_id.clone(),
                table_name: request.table_name.clone(),
                primary_key: request.primary_key.clone(),
                expected_seq: request.expected_seq,
            }],
            read_set: crate::commit::tx::ReadSet::default(),
            write_intent: crate::commit::tx::WriteIntent {
                mutations: vec![Mutation::Upsert {
                    project_id: request.project_id,
                    scope_id: request.scope_id,
                    table_name: request.table_name,
                    primary_key: request.primary_key,
                    row: request.row,
                }],
            },
            base_seq: self.snapshot_probe(ConsistencyMode::AtLatest).await?,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn compare_and_swap_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        primary_key: Vec<Value>,
        row: Row,
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.compare_and_swap_as_with(CompareAndSwapRequest {
            caller,
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            table_name: table_name.to_string(),
            primary_key,
            row,
            expected_seq,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn compare_and_inc_u256(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        primary_key: Vec<Value>,
        column: &str,
        amount_be: [u8; 32],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: crate::commit::tx::WriteClass::Standard,
            assertions: vec![crate::commit::tx::ReadAssertion::RowVersion {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: table_name.to_string(),
                primary_key: primary_key.clone(),
                expected_seq,
            }],
            read_set: crate::commit::tx::ReadSet::default(),
            write_intent: crate::commit::tx::WriteIntent {
                mutations: vec![Mutation::TableIncU256 {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    table_name: table_name.to_string(),
                    primary_key,
                    column: column.to_string(),
                    amount_be,
                }],
            },
            base_seq: self.snapshot_probe(ConsistencyMode::AtLatest).await?,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn compare_and_inc_u256_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        primary_key: Vec<Value>,
        column: &str,
        amount_be: [u8; 32],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit_envelope(TransactionEnvelope {
            caller: Some(caller),
            idempotency_key: None,
            write_class: crate::commit::tx::WriteClass::Standard,
            assertions: vec![crate::commit::tx::ReadAssertion::RowVersion {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: table_name.to_string(),
                primary_key: primary_key.clone(),
                expected_seq,
            }],
            read_set: crate::commit::tx::ReadSet::default(),
            write_intent: crate::commit::tx::WriteIntent {
                mutations: vec![Mutation::TableIncU256 {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    table_name: table_name.to_string(),
                    primary_key,
                    column: column.to_string(),
                    amount_be,
                }],
            },
            base_seq: self.snapshot_probe(ConsistencyMode::AtLatest).await?,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn compare_and_dec_u256(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        primary_key: Vec<Value>,
        column: &str,
        amount_be: [u8; 32],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: crate::commit::tx::WriteClass::Standard,
            assertions: vec![crate::commit::tx::ReadAssertion::RowVersion {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: table_name.to_string(),
                primary_key: primary_key.clone(),
                expected_seq,
            }],
            read_set: crate::commit::tx::ReadSet::default(),
            write_intent: crate::commit::tx::WriteIntent {
                mutations: vec![Mutation::TableDecU256 {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    table_name: table_name.to_string(),
                    primary_key,
                    column: column.to_string(),
                    amount_be,
                }],
            },
            base_seq: self.snapshot_probe(ConsistencyMode::AtLatest).await?,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn compare_and_dec_u256_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        primary_key: Vec<Value>,
        column: &str,
        amount_be: [u8; 32],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit_envelope(TransactionEnvelope {
            caller: Some(caller),
            idempotency_key: None,
            write_class: crate::commit::tx::WriteClass::Standard,
            assertions: vec![crate::commit::tx::ReadAssertion::RowVersion {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: table_name.to_string(),
                primary_key: primary_key.clone(),
                expected_seq,
            }],
            read_set: crate::commit::tx::ReadSet::default(),
            write_intent: crate::commit::tx::WriteIntent {
                mutations: vec![Mutation::TableDecU256 {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    table_name: table_name.to_string(),
                    primary_key,
                    column: column.to_string(),
                    amount_be,
                }],
            },
            base_seq: self.snapshot_probe(ConsistencyMode::AtLatest).await?,
        })
        .await
    }

    async fn mutate_where_returning_inner(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        predicate: Expr,
        updates: Vec<(String, TableUpdateExpr)>,
    ) -> Result<Option<MutateWhereReturningResult>, AedbError> {
        for _ in 0..MAX_MUTATE_WHERE_RETURNING_RETRIES {
            let (_, catalog_snapshot, _) = self.executor.snapshot_state().await;
            let schema = catalog_snapshot
                .tables
                .get(&(namespace_key(project_id, scope_id), table_name.to_string()))
                .cloned()
                .ok_or_else(|| AedbError::NotFound {
                    resource_type: ErrorResourceType::Table,
                    resource_id: format!("{project_id}.{scope_id}.{table_name}"),
                })?;

            let mut query = Query::select(&["*"])
                .from(table_name)
                .where_(predicate.clone())
                .limit(1);
            for pk in &schema.primary_key {
                query = query.order_by(pk, Order::Asc);
            }
            let query_result = if let Some(caller_ref) = caller.as_ref() {
                self.query_with_options_as(
                    Some(caller_ref),
                    project_id,
                    scope_id,
                    query,
                    QueryOptions {
                        consistency: ConsistencyMode::AtLatest,
                        allow_full_scan: true,
                        ..QueryOptions::default()
                    },
                )
                .await
                .map_err(query_error_to_aedb)?
            } else {
                self.query_with_options(
                    project_id,
                    scope_id,
                    query,
                    QueryOptions {
                        consistency: ConsistencyMode::AtLatest,
                        allow_full_scan: true,
                        ..QueryOptions::default()
                    },
                )
                .await
                .map_err(query_error_to_aedb)?
            };

            let Some(before) = query_result.rows.first().cloned() else {
                return Ok(None);
            };
            let primary_key = extract_primary_key_values(&schema, &before)?;
            let snapshot_seq = query_result.snapshot_seq;
            let lease = self
                .acquire_snapshot(ConsistencyMode::AtSeq(snapshot_seq))
                .await?;
            let expected_seq = lease
                .view
                .keyspace
                .table(project_id, scope_id, table_name)
                .and_then(|table| {
                    table
                        .row_versions
                        .get(&crate::storage::encoded_key::EncodedKey::from_values(
                            &primary_key,
                        ))
                        .copied()
                })
                .unwrap_or(0);
            if expected_seq == 0 {
                continue;
            }
            let after = apply_table_update_exprs(&schema, &before, &updates)?;

            let commit = self
                .commit_envelope(TransactionEnvelope {
                    caller: caller.clone(),
                    idempotency_key: None,
                    write_class: WriteClass::Standard,
                    assertions: vec![crate::commit::tx::ReadAssertion::RowVersion {
                        project_id: project_id.to_string(),
                        scope_id: scope_id.to_string(),
                        table_name: table_name.to_string(),
                        primary_key: primary_key.clone(),
                        expected_seq,
                    }],
                    read_set: ReadSet::default(),
                    write_intent: WriteIntent {
                        mutations: vec![Mutation::Upsert {
                            project_id: project_id.to_string(),
                            scope_id: scope_id.to_string(),
                            table_name: table_name.to_string(),
                            primary_key: primary_key.clone(),
                            row: after.clone(),
                        }],
                    },
                    base_seq: self.snapshot_probe(ConsistencyMode::AtLatest).await?,
                })
                .await;

            match commit {
                Ok(commit) => {
                    return Ok(Some(MutateWhereReturningResult {
                        commit,
                        primary_key,
                        before,
                        after,
                    }));
                }
                Err(AedbError::AssertionFailed { .. }) => continue,
                Err(err) => return Err(err),
            }
        }
        Err(AedbError::Conflict(
            "mutate_where_returning exceeded retry budget".into(),
        ))
    }

    async fn snapshot_for_consistency(
        &self,
        consistency: ConsistencyMode,
    ) -> Result<SnapshotReadView, AedbError> {
        match consistency {
            ConsistencyMode::AtLatest => {
                let (keyspace, catalog, seq) = self.executor.snapshot_state().await;
                Ok(SnapshotReadView {
                    keyspace: Arc::new(keyspace),
                    catalog: Arc::new(catalog),
                    seq,
                })
            }
            ConsistencyMode::AtSeq(requested) => {
                match self.executor.snapshot_at_seq(requested).await {
                    Ok(view) => Ok(view),
                    Err(e) => {
                        if let Some(view) = self.recovery_cache.lock().get(requested) {
                            return Ok(view);
                        }
                        if !should_fallback_to_recovery(&e) {
                            return Err(e);
                        }
                        warn!(
                            seq = requested,
                            "AtSeq falling back to disk recovery; version not in ring buffer"
                        );
                        let recovered =
                            recover_at_seq_with_config(&self.dir, requested, &self._config)?;
                        let view = SnapshotReadView {
                            keyspace: Arc::new(recovered.keyspace.snapshot()),
                            catalog: Arc::new(recovered.catalog.snapshot()),
                            seq: recovered.current_seq,
                        };
                        self.recovery_cache.lock().put(requested, view.clone());
                        Ok(view)
                    }
                }
            }
            ConsistencyMode::AtCheckpoint => {
                let manifest = crate::manifest::atomic::load_manifest_signed(
                    &self.dir,
                    self._config.hmac_key(),
                )?;
                let Some(cp) = manifest.checkpoints.last() else {
                    return Err(AedbError::Unavailable {
                        message: "no checkpoint available".into(),
                    });
                };
                match self.executor.snapshot_at_seq(cp.seq).await {
                    Ok(view) => Ok(view),
                    Err(e) => {
                        if let Some(view) = self.recovery_cache.lock().get(cp.seq) {
                            return Ok(view);
                        }
                        if !should_fallback_to_recovery(&e) {
                            return Err(e);
                        }
                        let recovered =
                            recover_at_seq_with_config(&self.dir, cp.seq, &self._config)?;
                        let view = SnapshotReadView {
                            keyspace: Arc::new(recovered.keyspace.snapshot()),
                            catalog: Arc::new(recovered.catalog.snapshot()),
                            seq: recovered.current_seq,
                        };
                        self.recovery_cache.lock().put(cp.seq, view.clone());
                        Ok(view)
                    }
                }
            }
        }
    }

    async fn acquire_snapshot(
        &self,
        consistency: ConsistencyMode,
    ) -> Result<SnapshotLease, AedbError> {
        let view = self.snapshot_for_consistency(consistency).await?;
        let mut mgr = self.snapshot_manager.lock();
        let handle = mgr.acquire_bounded(view.clone(), self._config.max_concurrent_snapshots)?;
        let checked = mgr
            .get_checked(handle, self._config.max_snapshot_age_ms)?
            .clone();
        Ok(SnapshotLease {
            manager: Arc::clone(&self.snapshot_manager),
            handle,
            view: checked,
        })
    }

    pub async fn preflight(&self, mutation: Mutation) -> PreflightResult {
        let (snapshot, catalog, _) = self.executor.snapshot_state().await;
        preflight(&snapshot, &catalog, &mutation)
    }

    pub async fn preflight_plan(&self, mutation: Mutation) -> crate::commit::tx::PreflightPlan {
        let (snapshot, catalog, base_seq) = self.executor.snapshot_state().await;
        preflight_plan(&snapshot, &catalog, &mutation, base_seq)
    }

    pub async fn preflight_as(
        &self,
        caller: &CallerContext,
        mutation: Mutation,
    ) -> Result<PreflightResult, AedbError> {
        ensure_external_caller_allowed(caller)?;
        let (snapshot, catalog, _) = self.executor.snapshot_state().await;
        validate_permissions(&catalog, Some(caller), &mutation)?;
        Ok(preflight(&snapshot, &catalog, &mutation))
    }

    pub async fn preflight_plan_as(
        &self,
        caller: &CallerContext,
        mutation: Mutation,
    ) -> Result<crate::commit::tx::PreflightPlan, AedbError> {
        ensure_external_caller_allowed(caller)?;
        let (snapshot, catalog, base_seq) = self.executor.snapshot_state().await;
        validate_permissions(&catalog, Some(caller), &mutation)?;
        Ok(preflight_plan(&snapshot, &catalog, &mutation, base_seq))
    }

    pub async fn commit_ddl(&self, op: DdlOperation) -> Result<DdlResult, AedbError> {
        let (_, catalog, _) = self.executor.snapshot_state().await;
        let applied = ddl_would_apply(&catalog, &op);
        let result = self.commit(Mutation::Ddl(op)).await?;
        Ok(DdlResult {
            applied,
            seq: result.commit_seq,
        })
    }

    pub async fn commit_ddl_batch(
        &self,
        ddl_ops: Vec<DdlOperation>,
    ) -> Result<DdlBatchResult, AedbError> {
        self.commit_ddl_batch_with_mode(ddl_ops, DdlBatchOrderMode::AsProvided)
            .await
    }

    pub async fn commit_ddl_batch_dependency_aware(
        &self,
        ddl_ops: Vec<DdlOperation>,
    ) -> Result<DdlBatchResult, AedbError> {
        self.commit_ddl_batch_with_mode(ddl_ops, DdlBatchOrderMode::DependencyAware)
            .await
    }

    pub async fn commit_ddl_batch_with_mode(
        &self,
        ddl_ops: Vec<DdlOperation>,
        mode: DdlBatchOrderMode,
    ) -> Result<DdlBatchResult, AedbError> {
        if ddl_ops.is_empty() {
            return Err(AedbError::InvalidConfig {
                message: "ddl batch cannot be empty".into(),
            });
        }
        let ddl_ops = match mode {
            DdlBatchOrderMode::AsProvided => ddl_ops,
            DdlBatchOrderMode::DependencyAware => order_ddl_ops_for_batch(ddl_ops)?,
        };
        let (_, catalog, _) = self.executor.snapshot_state().await;
        let mut planned_catalog = catalog;
        let mut results = Vec::with_capacity(ddl_ops.len());
        for op in &ddl_ops {
            let applied = ddl_would_apply(&planned_catalog, op);
            planned_catalog.apply_ddl(op.clone())?;
            results.push(DdlResult { applied, seq: 0 });
        }
        let mutations = ddl_ops.into_iter().map(Mutation::Ddl).collect();
        let base_seq = self.executor.current_seq().await;
        let committed = self
            .commit_envelope(TransactionEnvelope {
                caller: None,
                idempotency_key: None,
                write_class: crate::commit::tx::WriteClass::Standard,
                assertions: Vec::new(),
                read_set: crate::commit::tx::ReadSet::default(),
                write_intent: crate::commit::tx::WriteIntent { mutations },
                base_seq,
            })
            .await?;
        for result in &mut results {
            result.seq = committed.commit_seq;
        }
        Ok(DdlBatchResult {
            seq: committed.commit_seq,
            results,
        })
    }

    pub fn add_lifecycle_hook(&self, hook: Arc<dyn LifecycleHook>) {
        self.lifecycle_hooks.lock().push(hook);
    }

    pub fn remove_lifecycle_hook(&self, hook: &Arc<dyn LifecycleHook>) {
        let mut hooks = self.lifecycle_hooks.lock();
        hooks.retain(|existing| !Arc::ptr_eq(existing, hook));
    }

    pub fn add_telemetry_hook(&self, hook: Arc<dyn QueryCommitTelemetryHook>) {
        self.telemetry_hooks.lock().push(hook);
    }

    pub fn remove_telemetry_hook(&self, hook: &Arc<dyn QueryCommitTelemetryHook>) {
        let mut hooks = self.telemetry_hooks.lock();
        hooks.retain(|existing| !Arc::ptr_eq(existing, hook));
    }

    pub async fn backup_full_to_remote(
        &self,
        adapter: &dyn RemoteBackupAdapter,
        uri: &str,
    ) -> Result<BackupManifest, AedbError> {
        let temp = tempfile::tempdir()?;
        let manifest = self.backup_full(temp.path()).await?;
        adapter.store_backup_dir(uri, temp.path())?;
        Ok(manifest)
    }

    pub fn restore_from_remote(
        adapter: &dyn RemoteBackupAdapter,
        uri: &str,
        data_dir: &Path,
        config: &AedbConfig,
    ) -> Result<u64, AedbError> {
        let temp = tempfile::tempdir()?;
        let chain = adapter.materialize_backup_chain(uri, temp.path())?;
        Self::restore_from_backup_chain(&chain, data_dir, config, None)
    }

    pub async fn create_project(&self, project_id: &str) -> Result<(), AedbError> {
        self.commit_ddl(DdlOperation::CreateProject {
            owner_id: None,
            project_id: project_id.to_string(),
            if_not_exists: true,
        })
        .await?;
        Ok(())
    }

    pub async fn drop_project(&self, project_id: &str) -> Result<(), AedbError> {
        self.commit_ddl(DdlOperation::DropProject {
            project_id: project_id.to_string(),
            if_exists: true,
        })
        .await?;
        Ok(())
    }

    pub async fn create_scope(&self, project_id: &str, scope_id: &str) -> Result<(), AedbError> {
        self.commit_ddl(DdlOperation::CreateScope {
            owner_id: None,
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            if_not_exists: true,
        })
        .await?;
        Ok(())
    }

    pub async fn drop_scope(&self, project_id: &str, scope_id: &str) -> Result<(), AedbError> {
        self.commit_ddl(DdlOperation::DropScope {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            if_exists: true,
        })
        .await?;
        Ok(())
    }

    pub async fn enable_kv_projection(
        &self,
        project_id: &str,
        scope_id: &str,
    ) -> Result<(), AedbError> {
        self.commit(Mutation::Ddl(DdlOperation::EnableKvProjection {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
        }))
        .await?;
        Ok(())
    }

    pub async fn disable_kv_projection(
        &self,
        project_id: &str,
        scope_id: &str,
    ) -> Result<(), AedbError> {
        self.commit(Mutation::Ddl(DdlOperation::DisableKvProjection {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
        }))
        .await?;
        Ok(())
    }

    pub async fn list_scopes(&self, project_id: &str) -> Result<Vec<String>, AedbError> {
        let (_, catalog, _) = self.executor.snapshot_state().await;
        Ok(catalog.list_scopes(project_id))
    }

    pub async fn project_exists(&self, project_id: &str) -> Result<bool, AedbError> {
        let (_, catalog, _) = self.executor.snapshot_state().await;
        Ok(catalog.projects.contains_key(project_id))
    }

    pub async fn scope_exists(&self, project_id: &str, scope_id: &str) -> Result<bool, AedbError> {
        let (_, catalog, _) = self.executor.snapshot_state().await;
        Ok(catalog
            .scopes
            .contains_key(&(project_id.to_string(), scope_id.to_string())))
    }

    pub async fn table_exists(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
    ) -> Result<bool, AedbError> {
        let (_, catalog, _) = self.executor.snapshot_state().await;
        Ok(catalog
            .tables
            .contains_key(&(namespace_key(project_id, scope_id), table_name.to_string())))
    }

    pub async fn index_exists(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        index_name: &str,
    ) -> Result<bool, AedbError> {
        let (_, catalog, _) = self.executor.snapshot_state().await;
        Ok(catalog.indexes.contains_key(&(
            namespace_key(project_id, scope_id),
            table_name.to_string(),
            index_name.to_string(),
        )))
    }

    pub async fn list_projects(&self) -> Result<Vec<ProjectInfo>, AedbError> {
        let (_, catalog, _) = self.executor.snapshot_state().await;
        let mut infos: Vec<ProjectInfo> = catalog
            .projects
            .values()
            .map(|project| {
                let scope_count = catalog
                    .scopes
                    .values()
                    .filter(|scope| scope.project_id == project.project_id)
                    .count() as u32;
                ProjectInfo {
                    project_id: project.project_id.clone(),
                    scope_count,
                    created_at_micros: project.created_at_micros,
                }
            })
            .collect();
        infos.sort_by(|a, b| a.project_id.cmp(&b.project_id));
        Ok(infos)
    }

    pub async fn list_scopes_info(&self, project_id: &str) -> Result<Vec<ScopeInfo>, AedbError> {
        let (snapshot, catalog, _) = self.executor.snapshot_state().await;
        let mut infos: Vec<ScopeInfo> = catalog
            .scopes
            .values()
            .filter(|scope| scope.project_id == project_id)
            .map(|scope| {
                let table_count = catalog
                    .tables
                    .values()
                    .filter(|table| {
                        table.project_id == scope.project_id && table.scope_id == scope.scope_id
                    })
                    .count() as u32;
                let kv_key_count = snapshot
                    .namespaces
                    .get(&NamespaceId::project_scope(project_id, &scope.scope_id))
                    .map_or(0, |ns| ns.kv.entries.len() as u64);
                ScopeInfo {
                    scope_id: scope.scope_id.clone(),
                    table_count,
                    kv_key_count,
                    created_at_micros: scope.created_at_micros,
                }
            })
            .collect();
        infos.sort_by(|a, b| a.scope_id.cmp(&b.scope_id));
        Ok(infos)
    }

    pub async fn list_tables_info(
        &self,
        project_id: &str,
        scope_id: &str,
    ) -> Result<Vec<TableInfo>, AedbError> {
        let (snapshot, catalog, _) = self.executor.snapshot_state().await;
        let ns_key = namespace_key(project_id, scope_id);
        let ns_id = NamespaceId::project_scope(project_id, scope_id);
        let mut infos: Vec<TableInfo> = catalog
            .tables
            .values()
            .filter(|table| table.project_id == project_id && table.scope_id == scope_id)
            .map(|table| {
                let index_count = catalog
                    .indexes
                    .keys()
                    .filter(|(ns, name, _)| ns == &ns_key && name == &table.table_name)
                    .count() as u32;
                let row_count = snapshot
                    .namespaces
                    .get(&ns_id)
                    .and_then(|ns| ns.tables.get(&table.table_name))
                    .map_or(0, |t| t.rows.len() as u64);
                TableInfo {
                    table_name: table.table_name.clone(),
                    column_count: table.columns.len() as u32,
                    index_count,
                    row_count,
                }
            })
            .collect();
        infos.sort_by(|a, b| a.table_name.cmp(&b.table_name));
        Ok(infos)
    }

    pub async fn set_read_policy(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        predicate: Expr,
    ) -> Result<(), AedbError> {
        self.commit(Mutation::Ddl(DdlOperation::SetReadPolicy {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            table_name: table_name.to_string(),
            predicate,
            actor_id: None,
        }))
        .await?;
        Ok(())
    }

    pub async fn clear_read_policy(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
    ) -> Result<(), AedbError> {
        self.commit(Mutation::Ddl(DdlOperation::ClearReadPolicy {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            table_name: table_name.to_string(),
            actor_id: None,
        }))
        .await?;
        Ok(())
    }

    pub async fn transfer_ownership(
        &self,
        resource_type: ResourceType,
        project_id: &str,
        scope_id: Option<&str>,
        table_name: Option<&str>,
        new_owner_id: &str,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::Ddl(DdlOperation::TransferOwnership {
            resource_type,
            project_id: project_id.to_string(),
            scope_id: scope_id.map(ToString::to_string),
            table_name: table_name.map(ToString::to_string),
            new_owner_id: new_owner_id.to_string(),
            actor_id: None,
        }))
        .await
    }

    pub async fn list_tables(
        &self,
        project_id: &str,
        scope_id: &str,
    ) -> Result<Vec<TableSchema>, AedbError> {
        let (_, catalog, _) = self.executor.snapshot_state().await;
        Ok(catalog
            .tables
            .values()
            .filter(|t| t.project_id == project_id && t.scope_id == scope_id)
            .cloned()
            .collect())
    }

    pub async fn describe_table(
        &self,
        project_id: &str,
        scope_id: &str,
        table: &str,
    ) -> Result<TableSchema, AedbError> {
        let (_, catalog, _) = self.executor.snapshot_state().await;
        catalog
            .tables
            .get(&(namespace_key(project_id, scope_id), table.to_string()))
            .cloned()
            .ok_or_else(|| AedbError::NotFound {
                resource_type: ErrorResourceType::Table,
                resource_id: format!("{project_id}.{scope_id}.{table}"),
            })
    }

    pub async fn list_indexes(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
    ) -> Result<Vec<IndexDef>, AedbError> {
        let (_, catalog, _) = self.executor.snapshot_state().await;
        let mut out: Vec<IndexDef> = catalog
            .indexes
            .values()
            .filter(|idx| {
                idx.project_id == project_id
                    && idx.scope_id == scope_id
                    && idx.table_name == table_name
            })
            .cloned()
            .collect();
        out.sort_by(|a, b| a.index_name.cmp(&b.index_name));
        Ok(out)
    }

    pub async fn describe_index(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        index_name: &str,
    ) -> Result<IndexDef, AedbError> {
        let (_, catalog, _) = self.executor.snapshot_state().await;
        catalog
            .indexes
            .get(&(
                namespace_key(project_id, scope_id),
                table_name.to_string(),
                index_name.to_string(),
            ))
            .cloned()
            .ok_or_else(|| AedbError::NotFound {
                resource_type: ErrorResourceType::Index,
                resource_id: format!("{project_id}.{scope_id}.{table_name}.{index_name}"),
            })
    }

    pub async fn list_async_indexes(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
    ) -> Result<Vec<AsyncIndexDef>, AedbError> {
        let (_, catalog, _) = self.executor.snapshot_state().await;
        let mut out: Vec<AsyncIndexDef> = catalog
            .async_indexes
            .values()
            .filter(|idx| {
                idx.project_id == project_id
                    && idx.scope_id == scope_id
                    && idx.table_name == table_name
            })
            .cloned()
            .collect();
        out.sort_by(|a, b| a.index_name.cmp(&b.index_name));
        Ok(out)
    }

    pub async fn describe_async_index(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        index_name: &str,
    ) -> Result<AsyncIndexDef, AedbError> {
        let (_, catalog, _) = self.executor.snapshot_state().await;
        catalog
            .async_indexes
            .get(&(
                namespace_key(project_id, scope_id),
                table_name.to_string(),
                index_name.to_string(),
            ))
            .cloned()
            .ok_or_else(|| AedbError::NotFound {
                resource_type: ErrorResourceType::Index,
                resource_id: format!("{project_id}.{scope_id}.{table_name}.{index_name}"),
            })
    }

    pub async fn shutdown(&self) -> Result<(), AedbError> {
        let _ = self.checkpoint_now().await?;
        Ok(())
    }

    pub async fn checkpoint_now(&self) -> Result<u64, AedbError> {
        // Set checkpoint flag to fast-fail new operations, then acquire all permits
        // to ensure exclusivity (wait for in-flight operations to complete)
        self.checkpoint_in_progress.store(true, Ordering::Release);
        let _all_permits = self.checkpoint_gate.acquire_many(1000).await.unwrap();

        // Ensure flag is reset even if we return early with error
        struct ResetGuard<'a>(&'a AtomicBool);
        impl Drop for ResetGuard<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }
        let _reset_guard = ResetGuard(&self.checkpoint_in_progress);

        let _ = self.executor.force_fsync().await?;
        let (snapshot, catalog, seq) = self.executor.snapshot_state().await;
        let idempotency = self.executor.idempotency_snapshot().await;
        let heads = self.executor.head_state().await;
        let checkpoint = write_checkpoint_with_key(
            &snapshot,
            &catalog,
            seq,
            &self.dir,
            self._config.checkpoint_key(),
            self._config.checkpoint_key_id.clone(),
            idempotency,
        )?;

        let segments = read_segments(&self.dir)?;
        let active_segment_seq = segments
            .last()
            .map(|segment| segment.segment_seq)
            .unwrap_or(0);
        let manifest = Manifest {
            durable_seq: heads.durable_head_seq,
            visible_seq: heads.visible_head_seq,
            active_segment_seq,
            checkpoints: vec![checkpoint],
            segments,
        };
        write_manifest_atomic_signed(&manifest, &self.dir, self._config.hmac_key())?;
        Ok(seq)
    }

    pub async fn backup_full(&self, backup_dir: &Path) -> Result<BackupManifest, AedbError> {
        create_private_dir_all(backup_dir)?;
        create_private_dir_all(&backup_dir.join("wal_tail"))?;

        // Hot backup: pin a consistent read view without blocking the write path.
        let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
        let snapshot_seq = lease.view.seq;
        let idempotency = self.executor.idempotency_snapshot().await;
        let checkpoint = write_checkpoint_with_key(
            lease.view.keyspace.as_ref(),
            lease.view.catalog.as_ref(),
            snapshot_seq,
            backup_dir,
            self._config.checkpoint_key(),
            self._config.checkpoint_key_id.clone(),
            idempotency,
        )?;

        let mut wal_segments = Vec::new();
        let mut file_sha256 = HashMap::new();
        file_sha256.insert(
            checkpoint.filename.clone(),
            sha256_file_hex(&backup_dir.join(&checkpoint.filename))?,
        );

        for segment in read_segments(&self.dir)? {
            let src = self.dir.join(&segment.filename);
            let rel = format!("wal_tail/{}", segment.filename);
            let dst = backup_dir.join(&rel);
            fs::copy(src, &dst)?;
            wal_segments.push(segment.filename);
            file_sha256.insert(rel, sha256_file_hex(&dst)?);
        }

        wal_segments.sort_by_key(|name| segment_seq_from_name(name).unwrap_or(0));
        let created_at_micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        let manifest = BackupManifest {
            backup_id: format!("bk_{}_{}", created_at_micros, snapshot_seq),
            backup_type: "full".into(),
            parent_backup_id: None,
            from_seq: None,
            created_at_micros,
            aedb_version: env!("CARGO_PKG_VERSION").to_string(),
            checkpoint_seq: snapshot_seq,
            wal_head_seq: snapshot_seq,
            checkpoint_file: checkpoint.filename,
            wal_segments,
            file_sha256,
        };
        write_backup_manifest(backup_dir, &manifest, self._config.hmac_key())?;
        Ok(manifest)
    }

    /// Creates a full backup and packs it into a single archive file.
    ///
    /// The archive is encrypted when `checkpoint_encryption_key` is configured.
    pub async fn backup_full_to_file(
        &self,
        backup_file: &Path,
    ) -> Result<BackupManifest, AedbError> {
        let temp = tempfile::tempdir()?;
        let manifest = self.backup_full(temp.path()).await?;
        write_backup_archive(temp.path(), backup_file, self._config.checkpoint_key())?;
        Ok(manifest)
    }

    pub async fn backup_incremental(
        &self,
        backup_dir: &Path,
        parent_backup_dir: &Path,
    ) -> Result<BackupManifest, AedbError> {
        create_private_dir_all(backup_dir)?;
        create_private_dir_all(&backup_dir.join("wal_tail"))?;
        let parent = load_backup_manifest(parent_backup_dir, self._config.hmac_key())?;
        verify_backup_files(parent_backup_dir, &parent)?;
        let from_seq = parent.wal_head_seq.saturating_add(1);

        let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
        let to_seq = lease.view.seq;
        let created_at_micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        let mut wal_segments = Vec::new();
        let mut file_sha256 = HashMap::new();

        for segment in read_segments(&self.dir)? {
            let src = self.dir.join(&segment.filename);
            let Some((min_seq, max_seq)) = scan_segment_seq_range(&src)? else {
                continue;
            };
            if max_seq < from_seq || min_seq > to_seq {
                continue;
            }
            let rel = format!("wal_tail/{}", segment.filename);
            let dst = backup_dir.join(&rel);
            fs::copy(src, &dst)?;
            wal_segments.push(segment.filename);
            file_sha256.insert(rel, sha256_file_hex(&dst)?);
        }

        wal_segments.sort_by_key(|name| segment_seq_from_name(name).unwrap_or(0));
        let manifest = BackupManifest {
            backup_id: format!("bk_{}_{}", created_at_micros, to_seq),
            backup_type: "incremental".into(),
            parent_backup_id: Some(parent.backup_id),
            from_seq: Some(from_seq),
            created_at_micros,
            aedb_version: env!("CARGO_PKG_VERSION").to_string(),
            checkpoint_seq: parent.checkpoint_seq,
            wal_head_seq: to_seq,
            checkpoint_file: parent.checkpoint_file,
            wal_segments,
            file_sha256,
        };
        write_backup_manifest(backup_dir, &manifest, self._config.hmac_key())?;
        Ok(manifest)
    }

    pub fn restore_from_backup(
        backup_dir: &Path,
        data_dir: &Path,
        config: &AedbConfig,
    ) -> Result<u64, AedbError> {
        Self::restore_from_backup_chain(&[backup_dir.to_path_buf()], data_dir, config, None)
    }

    /// Restores from a single-file full backup archive created by `backup_full_to_file`.
    pub fn restore_from_backup_file(
        backup_file: &Path,
        data_dir: &Path,
        config: &AedbConfig,
    ) -> Result<u64, AedbError> {
        let temp = tempfile::tempdir()?;
        extract_backup_archive(backup_file, temp.path(), config.checkpoint_key())?;
        Self::restore_from_backup(temp.path(), data_dir, config)
    }

    pub fn restore_from_backup_chain(
        backup_dirs: &[PathBuf],
        data_dir: &Path,
        config: &AedbConfig,
        target_seq: Option<u64>,
    ) -> Result<u64, AedbError> {
        let chain = load_verified_backup_chain(backup_dirs, config)?;
        if data_dir.exists() && fs::read_dir(data_dir)?.next().is_some() {
            return Err(AedbError::Validation(
                "restore target directory must be empty".into(),
            ));
        }
        create_private_dir_all(data_dir)?;
        let effective_target =
            target_seq.unwrap_or(chain.last().expect("non-empty").1.wal_head_seq);
        let full = &chain[0].1;
        if effective_target < full.checkpoint_seq {
            return Err(AedbError::Validation(
                "target_seq is older than full checkpoint_seq".into(),
            ));
        }

        let checkpoint_path = resolve_backup_path(&chain[0].0, &full.checkpoint_file)?;
        let (mut keyspace, mut catalog, checkpoint_seq, mut idempotency) =
            load_checkpoint_with_key(&checkpoint_path, config.checkpoint_key())?;
        let mut current_seq = checkpoint_seq;
        let mut last_verified_chain: Option<(u64, [u8; 32])> = None;

        for (dir, manifest) in &chain {
            let replay_to = effective_target.min(manifest.wal_head_seq);
            if replay_to <= current_seq {
                continue;
            }
            let mut wal_paths = Vec::new();
            for seg in &manifest.wal_segments {
                wal_paths.push(resolve_backup_path(dir, &format!("wal_tail/{seg}"))?);
            }
            wal_paths.sort_by_key(|p| {
                p.file_name()
                    .and_then(|n| segment_seq_from_name(&n.to_string_lossy()))
                    .unwrap_or(0)
            });
            if config.hash_chain_required && !wal_paths.is_empty() {
                let first_seq = wal_paths
                    .first()
                    .and_then(|p| p.file_name())
                    .and_then(|n| segment_seq_from_name(&n.to_string_lossy()));
                let chain_anchor = match (last_verified_chain, first_seq) {
                    (Some((last_seq, last_hash)), Some(first_seq)) if first_seq > last_seq => {
                        Some(last_hash)
                    }
                    _ => None,
                };
                if let Some(last) = verify_hash_chain_batch(&wal_paths, chain_anchor)? {
                    last_verified_chain = Some(last);
                }
            }
            current_seq = replay_segments(
                &wal_paths,
                current_seq,
                Some(replay_to),
                None,
                false,
                config.strict_recovery(),
                &mut keyspace,
                &mut catalog,
                &mut idempotency,
            )?;
            if current_seq >= effective_target {
                break;
            }
        }
        let restored_seq = current_seq;
        let cp = write_checkpoint_with_key(
            &keyspace.snapshot(),
            &catalog,
            restored_seq,
            data_dir,
            config.checkpoint_key(),
            config.checkpoint_key_id.clone(),
            idempotency,
        )?;
        let restored_manifest = Manifest {
            durable_seq: restored_seq,
            visible_seq: restored_seq,
            active_segment_seq: restored_seq + 1,
            checkpoints: vec![cp],
            segments: Vec::new(),
        };
        write_manifest_atomic_signed(&restored_manifest, data_dir, config.hmac_key())?;
        Ok(restored_seq)
    }

    pub fn restore_from_backup_chain_at_time(
        backup_dirs: &[PathBuf],
        data_dir: &Path,
        config: &AedbConfig,
        target_time_micros: u64,
    ) -> Result<u64, AedbError> {
        let chain = load_verified_backup_chain(backup_dirs, config)?;
        let target_seq = resolve_target_seq_for_time(&chain, target_time_micros)?;
        Self::restore_from_backup_chain(backup_dirs, data_dir, config, Some(target_seq))
    }

    pub fn restore_namespace_from_backup_chain(
        backup_dirs: &[PathBuf],
        data_dir: &Path,
        config: &AedbConfig,
        project_id: &str,
        scope_id: &str,
        target_seq: Option<u64>,
    ) -> Result<u64, AedbError> {
        let chain = load_verified_backup_chain(backup_dirs, config)?;
        let effective_target =
            target_seq.unwrap_or(chain.last().expect("non-empty").1.wal_head_seq);
        let live = recover_with_config(data_dir, config)?;

        let temp = tempfile::tempdir()?;
        let _ = Self::restore_from_backup_chain(
            backup_dirs,
            temp.path(),
            config,
            Some(effective_target),
        )?;
        let restored = recover_with_config(temp.path(), config)?;

        let ns_key = namespace_key(project_id, scope_id);
        let ns_id = NamespaceId::project_scope(project_id, scope_id);

        let mut merged_keyspace = live.keyspace.clone();
        match restored.keyspace.namespaces.get(&ns_id) {
            Some(namespace) => {
                Arc::make_mut(&mut merged_keyspace.namespaces)
                    .insert(ns_id.clone(), namespace.clone());
            }
            None => {
                Arc::make_mut(&mut merged_keyspace.namespaces).remove(&ns_id);
            }
        }
        let async_keys: Vec<(NamespaceId, String, String)> = merged_keyspace
            .async_indexes
            .keys()
            .filter(|(ns, _, _)| *ns == ns_id)
            .cloned()
            .collect();
        for key in async_keys {
            Arc::make_mut(&mut merged_keyspace.async_indexes).remove(&key);
        }
        for (key, value) in restored.keyspace.async_indexes.iter() {
            if key.0 == ns_id {
                Arc::make_mut(&mut merged_keyspace.async_indexes)
                    .insert(key.clone(), value.clone());
            }
        }

        let mut merged_catalog = live.catalog.clone();
        if let Some(project) = restored.catalog.projects.get(project_id) {
            merged_catalog
                .projects
                .insert(project_id.to_string(), project.clone());
        }
        let scope_key = (project_id.to_string(), scope_id.to_string());
        match restored.catalog.scopes.get(&scope_key) {
            Some(scope) => {
                merged_catalog
                    .scopes
                    .insert(scope_key.clone(), scope.clone());
            }
            None => {
                merged_catalog.scopes.remove(&scope_key);
            }
        }
        let table_keys: Vec<(String, String)> = merged_catalog
            .tables
            .keys()
            .filter(|(ns, _)| ns == &ns_key)
            .cloned()
            .collect();
        for key in table_keys {
            merged_catalog.tables.remove(&key);
        }
        for (key, table) in restored.catalog.tables.iter() {
            if key.0 == ns_key {
                merged_catalog.tables.insert(key.clone(), table.clone());
            }
        }

        let index_keys: Vec<(String, String, String)> = merged_catalog
            .indexes
            .keys()
            .filter(|(ns, _, _)| ns == &ns_key)
            .cloned()
            .collect();
        for key in index_keys {
            merged_catalog.indexes.remove(&key);
        }
        for (key, index) in restored.catalog.indexes.iter() {
            if key.0 == ns_key {
                merged_catalog.indexes.insert(key.clone(), index.clone());
            }
        }

        let async_index_keys: Vec<(String, String, String)> = merged_catalog
            .async_indexes
            .keys()
            .filter(|(ns, _, _)| ns == &ns_key)
            .cloned()
            .collect();
        for key in async_index_keys {
            merged_catalog.async_indexes.remove(&key);
        }
        for (key, index) in restored.catalog.async_indexes.iter() {
            if key.0 == ns_key {
                merged_catalog
                    .async_indexes
                    .insert(key.clone(), index.clone());
            }
        }

        let merged_seq = live.current_seq.max(effective_target);
        let cp = write_checkpoint_with_key(
            &merged_keyspace.snapshot(),
            &merged_catalog,
            merged_seq,
            data_dir,
            config.checkpoint_key(),
            config.checkpoint_key_id.clone(),
            live.idempotency,
        )?;
        let manifest = Manifest {
            durable_seq: merged_seq,
            visible_seq: merged_seq,
            active_segment_seq: merged_seq + 1,
            checkpoints: vec![cp],
            segments: Vec::new(),
        };
        write_manifest_atomic_signed(&manifest, data_dir, config.hmac_key())?;
        Ok(merged_seq)
    }

    pub async fn snapshot_probe(&self, consistency: ConsistencyMode) -> Result<u64, AedbError> {
        let lease = self.acquire_snapshot(consistency).await?;
        Ok(lease.view.seq)
    }

    pub fn metrics(&self) -> ExecutorMetrics {
        self.executor.metrics()
    }

    pub async fn operational_metrics(&self) -> OperationalMetrics {
        let core = self.executor.metrics();
        let runtime = self.executor.runtime_state_metrics().await;
        let now_micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        let snapshot_age_micros = now_micros.saturating_sub(runtime.last_full_snapshot_micros);
        let conflict_rate = if core.commits_total == 0 {
            0.0
        } else {
            core.conflict_rejections as f64 / core.commits_total as f64
        };
        OperationalMetrics {
            commits_total: core.commits_total,
            commit_errors: core.commit_errors,
            conflict_rejections: core.conflict_rejections,
            read_set_conflicts: core.read_set_conflicts,
            conflict_rate,
            avg_commit_latency_micros: core.avg_commit_latency_micros,
            coordinator_apply_attempts: core.coordinator_apply_attempts,
            avg_coordinator_apply_micros: core.avg_coordinator_apply_micros,
            inflight_commits: core.inflight_commits,
            queue_depth: core.inflight_commits,
            durable_head_lag: runtime
                .visible_head_seq
                .saturating_sub(runtime.durable_head_seq),
            visible_head_seq: runtime.visible_head_seq,
            durable_head_seq: runtime.durable_head_seq,
            current_seq: runtime.current_seq,
            snapshot_age_micros,
            startup_recovery_micros: self.startup_recovery_micros,
            startup_recovered_seq: self.startup_recovered_seq,
        }
    }

    pub async fn wait_for_durable(&self, seq: u64) -> Result<(), AedbError> {
        self.executor.wait_for_durable(seq).await
    }

    pub async fn force_fsync(&self) -> Result<u64, AedbError> {
        self.executor.force_fsync().await
    }

    pub async fn apply_migrations(&self, mut migrations: Vec<Migration>) -> Result<(), AedbError> {
        migrations.sort_by_key(|m| m.version);
        for migration in migrations {
            self.apply_migration(migration).await?;
        }
        Ok(())
    }

    pub async fn run_migrations(
        &self,
        mut migrations: Vec<Migration>,
    ) -> Result<MigrationReport, AedbError> {
        migrations.sort_by_key(|m| m.version);
        if migrations.is_empty() {
            return Ok(MigrationReport {
                applied: Vec::new(),
                skipped: Vec::new(),
                current_version: 0,
            });
        }
        let project_id = migrations[0].project_id.clone();
        let scope_id = migrations[0].scope_id.clone();
        if migrations
            .iter()
            .any(|m| m.project_id != project_id || m.scope_id != scope_id)
        {
            return Err(AedbError::InvalidConfig {
                message: "all migrations in run_migrations must target the same project and scope"
                    .into(),
            });
        }
        let mut applied = Vec::new();
        let mut skipped = Vec::new();
        let existing = self
            .list_applied_migrations(&project_id, &scope_id)
            .await?
            .into_iter()
            .map(|record| (record.version, record))
            .collect::<HashMap<_, _>>();
        for migration in migrations {
            if let Some(record) = existing.get(&migration.version) {
                let checksum = checksum_hex(&migration)?;
                if checksum != record.checksum_hex {
                    return Err(AedbError::IntegrityError {
                        message: format!(
                            "migration checksum mismatch for version {}",
                            migration.version
                        ),
                    });
                }
                skipped.push(migration.version);
                continue;
            }
            let started = Instant::now();
            let version = migration.version;
            let name = migration.name.clone();
            self.apply_migration(migration).await?;
            applied.push((version, name, started.elapsed()));
        }
        let current_version = self.current_version(&project_id, &scope_id).await?;
        Ok(MigrationReport {
            applied,
            skipped,
            current_version,
        })
    }

    pub async fn apply_migration(&self, migration: Migration) -> Result<(), AedbError> {
        const MAX_RETRIES_ON_CONFLICT: usize = 8;

        let checksum = checksum_hex(&migration)?;
        let key = migration_key(migration.version);
        for attempt in 0..=MAX_RETRIES_ON_CONFLICT {
            let (snapshot, _, _) = self.executor.snapshot_state().await;
            if let Some(existing) =
                snapshot.kv_get(&migration.project_id, &migration.scope_id, &key)
            {
                let record = decode_record(&existing.value)?;
                if record.checksum_hex != checksum {
                    return Err(AedbError::IntegrityError {
                        message: format!(
                            "migration checksum mismatch for version {}",
                            migration.version
                        ),
                    });
                }
                return Ok(());
            }

            let mut mutations = migration.mutations.clone();
            let predicted_applied_seq = self.executor.current_seq().await + 1;
            let record = MigrationRecord {
                version: migration.version,
                name: migration.name.clone(),
                project_id: migration.project_id.clone(),
                scope_id: migration.scope_id.clone(),
                applied_at_micros: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_micros() as u64,
                applied_seq: predicted_applied_seq,
                checksum_hex: checksum.clone(),
            };
            mutations.push(Mutation::KvSet {
                project_id: migration.project_id.clone(),
                scope_id: migration.scope_id.clone(),
                key: key.clone(),
                value: encode_record(&record)?,
            });
            let base_seq = predicted_applied_seq.saturating_sub(1);
            match self
                .commit_envelope(TransactionEnvelope {
                    caller: None,
                    idempotency_key: None,
                    write_class: crate::commit::tx::WriteClass::Standard,
                    assertions: Vec::new(),
                    read_set: crate::commit::tx::ReadSet::default(),
                    write_intent: crate::commit::tx::WriteIntent { mutations },
                    base_seq,
                })
                .await
            {
                Ok(_) => return Ok(()),
                Err(AedbError::Conflict(_)) if attempt < MAX_RETRIES_ON_CONFLICT => continue,
                Err(err) => {
                    let (snapshot, _, _) = self.executor.snapshot_state().await;
                    if let Some(existing) =
                        snapshot.kv_get(&migration.project_id, &migration.scope_id, &key)
                    {
                        let record = decode_record(&existing.value)?;
                        if record.checksum_hex != checksum {
                            return Err(AedbError::IntegrityError {
                                message: format!(
                                    "migration checksum mismatch for version {}",
                                    migration.version
                                ),
                            });
                        }
                        return Ok(());
                    }
                    return Err(err);
                }
            }
        }

        Err(AedbError::Conflict(format!(
            "migration {} could not be applied due to repeated conflicts",
            migration.version
        )))
    }

    pub async fn list_applied_migrations(
        &self,
        project_id: &str,
        scope_id: &str,
    ) -> Result<Vec<MigrationRecord>, AedbError> {
        let (snapshot, _, _) = self.executor.snapshot_state().await;
        let ns = NamespaceId::project_scope(project_id, scope_id);
        let mut out = Vec::new();
        if let Some(namespace) = snapshot.namespaces.get(&ns) {
            for (k, v) in &namespace.kv.entries {
                if k.starts_with(b"__migrations/") {
                    out.push(decode_record(&v.value)?);
                }
            }
        }
        out.sort_by_key(|r| r.version);
        Ok(out)
    }

    pub async fn applied_migrations(
        &self,
        project_id: &str,
        scope_id: &str,
    ) -> Result<Vec<MigrationRecord>, AedbError> {
        self.list_applied_migrations(project_id, scope_id).await
    }

    pub async fn current_version(
        &self,
        project_id: &str,
        scope_id: &str,
    ) -> Result<u64, AedbError> {
        Ok(self
            .list_applied_migrations(project_id, scope_id)
            .await?
            .last()
            .map_or(0, |record| record.version))
    }

    pub async fn rollback_to_migration(
        &self,
        project_id: &str,
        scope_id: &str,
        target_version_inclusive: u64,
        mut available_migrations: Vec<Migration>,
    ) -> Result<(), AedbError> {
        let applied = self.list_applied_migrations(project_id, scope_id).await?;
        available_migrations.sort_by_key(|m| m.version);
        for record in applied.into_iter().rev() {
            if record.version <= target_version_inclusive {
                break;
            }
            let migration = available_migrations
                .iter()
                .find(|m| m.version == record.version)
                .ok_or_else(|| {
                    AedbError::Validation(format!(
                        "rollback migration definition missing for version {}",
                        record.version
                    ))
                })?;
            let Some(down) = &migration.down_mutations else {
                return Err(AedbError::Validation(format!(
                    "migration {} has no down_mutations",
                    record.version
                )));
            };
            let mut mutations = down.clone();
            mutations.push(Mutation::KvDel {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key: migration_key(record.version),
            });
            let base_seq = self.executor.current_seq().await;
            self.commit_envelope(TransactionEnvelope {
                caller: None,
                idempotency_key: None,
                write_class: crate::commit::tx::WriteClass::Standard,
                assertions: Vec::new(),
                read_set: crate::commit::tx::ReadSet::default(),
                write_intent: crate::commit::tx::WriteIntent { mutations },
                base_seq,
            })
            .await?;
        }
        Ok(())
    }

    pub async fn backfill_table(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        update: fn(&crate::catalog::types::Row) -> Option<crate::catalog::types::Row>,
    ) -> Result<u64, AedbError> {
        self.backfill_table_batched(project_id, scope_id, table_name, usize::MAX, update)
            .await
    }

    pub async fn backfill_table_batched(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        batch_size: usize,
        update: fn(&crate::catalog::types::Row) -> Option<crate::catalog::types::Row>,
    ) -> Result<u64, AedbError> {
        let batch_size = batch_size.max(1);
        let (snapshot, catalog, _) = self.executor.snapshot_state().await;
        let schema = catalog
            .tables
            .get(&(namespace_key(project_id, scope_id), table_name.to_string()))
            .ok_or_else(|| AedbError::Validation("table not found".into()))?
            .clone();
        let table = snapshot
            .table(project_id, scope_id, table_name)
            .ok_or_else(|| AedbError::Validation("table not found".into()))?;
        let rows: Vec<crate::catalog::types::Row> = table.rows.values().cloned().collect();
        let mut updated = 0u64;
        for chunk in rows.chunks(batch_size) {
            for row in chunk {
                if let Some(new_row) = update(row) {
                    let primary_key = schema
                        .primary_key
                        .iter()
                        .map(|pk_name| {
                            let idx = schema
                                .columns
                                .iter()
                                .position(|c| c.name == *pk_name)
                                .ok_or_else(|| {
                                    AedbError::Validation(format!(
                                        "primary key column missing: {pk_name}"
                                    ))
                                })?;
                            Ok(new_row.values[idx].clone())
                        })
                        .collect::<Result<Vec<_>, AedbError>>()?;
                    self.commit(Mutation::Upsert {
                        project_id: project_id.to_string(),
                        scope_id: scope_id.to_string(),
                        table_name: table_name.to_string(),
                        primary_key,
                        row: new_row,
                    })
                    .await?;
                    updated += 1;
                }
            }
        }
        Ok(updated)
    }

    pub async fn backfill_table_batched_resumable(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        batch_size: usize,
        progress_key: &[u8],
        update: fn(&crate::catalog::types::Row) -> Option<crate::catalog::types::Row>,
    ) -> Result<u64, AedbError> {
        let batch_size = batch_size.max(1);
        let (snapshot, catalog, _) = self.executor.snapshot_state().await;
        let schema = catalog
            .tables
            .get(&(namespace_key(project_id, scope_id), table_name.to_string()))
            .ok_or_else(|| AedbError::Validation("table not found".into()))?
            .clone();
        let table = snapshot
            .table(project_id, scope_id, table_name)
            .ok_or_else(|| AedbError::Validation("table not found".into()))?;
        let rows: Vec<crate::catalog::types::Row> = table.rows.values().cloned().collect();
        let start_offset = snapshot
            .kv_get(project_id, scope_id, progress_key)
            .and_then(|e| std::str::from_utf8(&e.value).ok()?.parse::<usize>().ok())
            .unwrap_or(0);
        let mut updated = 0u64;
        let start_offset = start_offset.min(rows.len());
        for (idx, chunk) in rows[start_offset..].chunks(batch_size).enumerate() {
            for row in chunk {
                if let Some(new_row) = update(row) {
                    let primary_key = schema
                        .primary_key
                        .iter()
                        .map(|pk_name| {
                            let idx = schema
                                .columns
                                .iter()
                                .position(|c| c.name == *pk_name)
                                .ok_or_else(|| {
                                    AedbError::Validation(format!(
                                        "primary key column missing: {pk_name}"
                                    ))
                                })?;
                            Ok(new_row.values[idx].clone())
                        })
                        .collect::<Result<Vec<_>, AedbError>>()?;
                    self.commit(Mutation::Upsert {
                        project_id: project_id.to_string(),
                        scope_id: scope_id.to_string(),
                        table_name: table_name.to_string(),
                        primary_key,
                        row: new_row,
                    })
                    .await?;
                    updated += 1;
                }
            }
            let progressed = start_offset + ((idx + 1) * batch_size).min(rows.len() - start_offset);
            self.commit(Mutation::KvSet {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key: progress_key.to_vec(),
                value: progressed.to_string().into_bytes(),
            })
            .await?;
        }
        Ok(updated)
    }

    pub async fn head_state(&self) -> crate::commit::executor::HeadState {
        self.executor.head_state().await
    }
}

impl ReadTx<'_> {
    pub fn snapshot_seq(&self) -> u64 {
        self.lease.view.seq
    }

    pub async fn query(
        &self,
        project_id: &str,
        scope_id: &str,
        query: Query,
    ) -> Result<QueryResult, QueryError> {
        self.query_with_options(project_id, scope_id, query, QueryOptions::default())
            .await
    }

    pub async fn query_with_options(
        &self,
        project_id: &str,
        scope_id: &str,
        query: Query,
        mut options: QueryOptions,
    ) -> Result<QueryResult, QueryError> {
        options.consistency = ConsistencyMode::AtSeq(self.lease.view.seq);
        let started = Instant::now();
        let table = query.table.clone();
        let result = execute_query_against_view(
            &self.lease.view,
            project_id,
            scope_id,
            query,
            &options,
            self.caller.as_ref(),
            self.db._config.max_scan_rows,
        );
        self.db.emit_query_telemetry(
            started,
            project_id,
            scope_id,
            &table,
            self.lease.view.seq,
            &result,
        );
        result
    }

    pub async fn query_batch(
        &self,
        project_id: &str,
        scope_id: &str,
        items: Vec<QueryBatchItem>,
    ) -> Result<Vec<QueryResult>, QueryError> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            out.push(
                self.query_with_options(project_id, scope_id, item.query, item.options)
                    .await?,
            );
        }
        Ok(out)
    }

    pub async fn exists(
        &self,
        project_id: &str,
        scope_id: &str,
        mut query: Query,
    ) -> Result<bool, QueryError> {
        query.limit = Some(1);
        let result = self.query(project_id, scope_id, query).await?;
        Ok(!result.rows.is_empty())
    }

    pub fn explain(
        &self,
        project_id: &str,
        scope_id: &str,
        query: Query,
        mut options: QueryOptions,
    ) -> Result<QueryDiagnostics, QueryError> {
        options.consistency = ConsistencyMode::AtSeq(self.lease.view.seq);
        explain_query_against_view(
            &self.lease.view,
            project_id,
            scope_id,
            query,
            &options,
            self.caller.as_ref(),
            self.db._config.max_scan_rows,
        )
    }

    pub async fn query_with_diagnostics(
        &self,
        project_id: &str,
        scope_id: &str,
        query: Query,
        options: QueryOptions,
    ) -> Result<QueryWithDiagnosticsResult, QueryError> {
        let diagnostics = self.explain(project_id, scope_id, query.clone(), options.clone())?;
        let result = self
            .query_with_options(project_id, scope_id, query, options)
            .await?;
        Ok(QueryWithDiagnosticsResult {
            result,
            diagnostics,
        })
    }

    pub async fn query_page_stable(
        &self,
        project_id: &str,
        scope_id: &str,
        query: Query,
        cursor: Option<String>,
        page_size: usize,
    ) -> Result<QueryResult, QueryError> {
        let query =
            ensure_stable_order_from_catalog(project_id, scope_id, &self.lease.view.catalog, query);
        self.query_with_options(
            project_id,
            scope_id,
            query.limit(page_size.max(1)),
            QueryOptions {
                consistency: ConsistencyMode::AtSeq(self.lease.view.seq),
                cursor,
                ..QueryOptions::default()
            },
        )
        .await
    }

    pub async fn list_with_total(
        &self,
        project_id: &str,
        scope_id: &str,
        mut query: Query,
        cursor: Option<String>,
        offset: Option<usize>,
        page_size: usize,
    ) -> Result<ListPageResult, QueryError> {
        if !query.aggregates.is_empty() || !query.group_by.is_empty() || query.having.is_some() {
            return Err(QueryError::InvalidQuery {
                reason: "list_with_total expects non-aggregate query".into(),
            });
        }
        query =
            ensure_stable_order_from_catalog(project_id, scope_id, &self.lease.view.catalog, query);
        let count_query = Query {
            select: vec!["count_star".into()],
            table: query.table.clone(),
            table_alias: query.table_alias.clone(),
            joins: query.joins.clone(),
            predicate: query.predicate.clone(),
            order_by: Vec::new(),
            limit: None,
            group_by: Vec::new(),
            aggregates: vec![crate::query::plan::Aggregate::Count],
            having: None,
            use_index: query.use_index.clone(),
        };
        let count_result = self
            .query_with_options(
                project_id,
                scope_id,
                count_query,
                QueryOptions {
                    allow_full_scan: true,
                    ..QueryOptions::default()
                },
            )
            .await?;
        let total_count = count_result
            .rows
            .first()
            .and_then(|row| row.values.first())
            .and_then(|value| match value {
                Value::Integer(v) => usize::try_from(*v).ok(),
                _ => None,
            })
            .unwrap_or(0);

        if let Some(offset) = offset {
            let mut full_query = query.clone();
            full_query.limit = None;
            let full = self
                .query_with_options(
                    project_id,
                    scope_id,
                    full_query,
                    QueryOptions {
                        allow_full_scan: true,
                        ..QueryOptions::default()
                    },
                )
                .await?;
            let rows = full
                .rows
                .into_iter()
                .skip(offset)
                .take(page_size.max(1))
                .collect::<Vec<_>>();
            return Ok(ListPageResult {
                rows,
                total_count,
                next_cursor: None,
                snapshot_seq: full.snapshot_seq,
                rows_examined: full.rows_examined,
            });
        }

        let page = self
            .query_page_stable(project_id, scope_id, query, cursor, page_size)
            .await?;
        Ok(ListPageResult {
            rows: page.rows,
            total_count,
            next_cursor: page.cursor,
            snapshot_seq: page.snapshot_seq,
            rows_examined: page.rows_examined,
        })
    }

    pub async fn lookup_then_hydrate(
        &self,
        project_id: &str,
        scope_id: &str,
        source_query: Query,
        source_key_index: usize,
        hydrate_query: Query,
        hydrate_key_column: &str,
    ) -> Result<(QueryResult, QueryResult), QueryError> {
        let source = self.query(project_id, scope_id, source_query).await?;
        let keys = source
            .rows
            .iter()
            .filter_map(|row| row.values.get(source_key_index).cloned())
            .collect::<Vec<_>>();
        if keys.is_empty() {
            return Ok((
                source,
                QueryResult {
                    rows: Vec::new(),
                    rows_examined: 0,
                    cursor: None,
                    snapshot_seq: self.lease.view.seq,
                    materialized_seq: None,
                },
            ));
        }
        let predicate = Expr::In(hydrate_key_column.to_string(), keys);
        let hydrate_query = Query {
            predicate: Some(match hydrate_query.predicate {
                Some(existing) => Expr::And(Box::new(existing), Box::new(predicate)),
                None => predicate,
            }),
            ..hydrate_query
        };
        let hydrated = self.query(project_id, scope_id, hydrate_query).await?;
        Ok((source, hydrated))
    }
}
