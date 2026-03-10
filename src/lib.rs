pub mod backup;
pub mod catalog;
pub mod checkpoint;
pub mod commit;
pub mod config;
pub mod declarative;
pub mod engine_interface;
pub mod error;
mod lib_helpers;
#[cfg(test)]
mod lib_tests;
pub mod manifest;
pub mod migration;
pub mod offline;
pub mod order_book;
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
use crate::catalog::schema::{AccumulatorValueType, AsyncIndexDef, IndexDef, TableSchema};
use crate::catalog::types::{Row, Value};
use crate::catalog::{DdlOperation, ResourceType};
use crate::checkpoint::loader::load_checkpoint_with_key;
use crate::checkpoint::writer::write_checkpoint_with_key;
use crate::commit::action::{ActionCommitOutcome, ActionCommitResult, ActionEnvelopeRequest};
use crate::commit::executor::{CommitExecutor, CommitResult, ExecutorMetrics, IdempotencyOutcome};
use crate::commit::tx::{
    ReadKey, ReadSet, ReadSetEntry, TransactionEnvelope, WriteClass, WriteIntent,
};
use crate::commit::validation::{
    KvU64MissingPolicy, KvU64MutatorOp, KvU64OverflowPolicy, KvU64UnderflowPolicy, KvU256MutatorOp,
    MAX_COUNTER_SHARDS, Mutation, TableUpdateExpr, validate_mutation_with_config,
    validate_permissions,
};
use crate::config::{AedbConfig, DurabilityMode, RecoveryMode};
use crate::error::AedbError;
use crate::error::ResourceType as ErrorResourceType;
use crate::lib_helpers::*;
use crate::manifest::atomic::write_manifest_atomic_signed;
use crate::manifest::schema::{Manifest, SegmentMeta};
use crate::migration::{
    Migration, MigrationRecord, checksum_hex, decode_record, encode_record, migration_key,
};
use crate::order_book::{
    ExecInstruction, FillSpec, InstrumentConfig, OrderBookDepth, OrderBookTableMode, OrderRecord,
    OrderRequest, OrderSide, OrderType, Spread, TimeInForce, key_client_id, key_order,
    read_last_execution_report, read_open_orders, read_order_status, read_recent_trades,
    read_spread, read_top_n, scoped_instrument, u256_from_be,
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
use crate::storage::encoded_key::EncodedKey;
use crate::storage::keyspace::{Keyspace, KvEntry, NamespaceId};
use crate::wal::frame::{FrameError, FrameReader};
use crate::wal::segment::{SEGMENT_HEADER_SIZE, SegmentHeader};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::fs;
use std::fs::File;
use std::future::Future;
use std::io::{BufReader, Read};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;
use tracing::{info, warn};
use uuid::Uuid;

const TRUST_MODE_MARKER_FILE: &str = "trust_mode.json";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct TrustModeMarker {
    #[serde(default)]
    ever_non_strict_recovery: bool,
    #[serde(default)]
    ever_hash_chain_disabled: bool,
}

fn trust_mode_marker_path(dir: &Path) -> PathBuf {
    dir.join(TRUST_MODE_MARKER_FILE)
}

fn load_trust_mode_marker(dir: &Path) -> Result<Option<TrustModeMarker>, AedbError> {
    let path = trust_mode_marker_path(dir);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(&path)?;
    let marker: TrustModeMarker =
        serde_json::from_slice(&bytes).map_err(|e| AedbError::Validation(e.to_string()))?;
    Ok(Some(marker))
}

fn persist_trust_mode_marker(dir: &Path, marker: &TrustModeMarker) -> Result<(), AedbError> {
    let bytes = serde_json::to_vec(marker).map_err(|e| AedbError::Encode(e.to_string()))?;
    fs::write(trust_mode_marker_path(dir), bytes)?;
    Ok(())
}

fn enforce_and_record_trust_mode(dir: &Path, config: &AedbConfig) -> Result<(), AedbError> {
    let mut marker = load_trust_mode_marker(dir)?.unwrap_or_default();
    if config.strict_recovery()
        && (marker.ever_non_strict_recovery || marker.ever_hash_chain_disabled)
    {
        return Err(AedbError::Validation(
            "strict open denied: data directory was previously opened with non-strict recovery or hash-chain disabled"
                .into(),
        ));
    }

    let mut changed = false;
    if !config.strict_recovery() && !marker.ever_non_strict_recovery {
        marker.ever_non_strict_recovery = true;
        changed = true;
    }
    if !config.hash_chain_required && !marker.ever_hash_chain_disabled {
        marker.ever_hash_chain_disabled = true;
        changed = true;
    }
    if changed {
        persist_trust_mode_marker(dir, &marker)?;
    }
    Ok(())
}

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

fn derive_cursor_signing_key(config: &AedbConfig, dir: &Path) -> [u8; 32] {
    let mut material = Vec::new();
    if let Some(key) = config.hmac_key() {
        material.extend_from_slice(key);
        material.extend_from_slice(dir.to_string_lossy().as_bytes());
    } else {
        material.extend_from_slice(Uuid::new_v4().as_bytes());
        material.extend_from_slice(
            &SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
                .to_le_bytes(),
        );
        material.extend_from_slice(dir.to_string_lossy().as_bytes());
    }
    blake3::derive_key("aedb-query-cursor-v1", &material)
}

pub struct AedbInstance {
    _config: AedbConfig,
    require_authenticated_calls: bool,
    dir: PathBuf,
    cursor_signing_key: [u8; 32],
    executor: CommitExecutor,
    /// Serializes checkpoint writers without blocking commit/query traffic.
    checkpoint_lock: Arc<AsyncMutex<()>>,
    snapshot_manager: Arc<Mutex<SnapshotManager>>,
    recovery_cache: Arc<Mutex<RecoveryCache>>,
    lifecycle_hooks: Arc<Mutex<Vec<Arc<dyn LifecycleHook>>>>,
    lifecycle_hooks_present: Arc<AtomicBool>,
    telemetry_hooks: Arc<Mutex<Vec<Arc<dyn QueryCommitTelemetryHook>>>>,
    telemetry_hooks_present: Arc<AtomicBool>,
    upstream_validation_rejections: Arc<AtomicU64>,
    durable_wait_ops: Arc<AtomicU64>,
    durable_wait_micros: Arc<AtomicU64>,
    durable_ack_fsync_leader: Arc<AtomicBool>,
    reactive_processor_ack_watermarks:
        Arc<Mutex<HashMap<ReactiveCheckpointAckCacheKey, ReactiveCheckpointAckState>>>,
    reactive_processor_handlers: Arc<Mutex<HashMap<String, ReactiveProcessorHandler>>>,
    reactive_processor_runtimes: Arc<AsyncMutex<HashMap<String, ReactiveProcessorRuntime>>>,
    startup_recovery_micros: u64,
    startup_recovered_seq: u64,
}

const SYSTEM_SCOPE_ID: &str = "app";
const LIFECYCLE_OUTBOX_TABLE: &str = "lifecycle_outbox";
const EVENT_OUTBOX_TABLE: &str = "event_outbox";
const REACTIVE_PROCESSOR_CHECKPOINTS_TABLE: &str = "reactive_processor_checkpoints";
const REACTIVE_PROCESSOR_REGISTRY_TABLE: &str = "reactive_processor_registry";
const REACTIVE_PROCESSOR_DLQ_TABLE: &str = "reactive_processor_dead_letters";

struct ReadPhaseLogSettings {
    enabled: bool,
    threshold_ms: u64,
    sample_every: u64,
}

static READ_PHASE_LOG_SETTINGS: LazyLock<ReadPhaseLogSettings> = LazyLock::new(|| {
    let enabled = std::env::var("AEDB_READ_PHASE_LOG_ENABLED")
        .ok()
        .and_then(|v| {
            let n = v.trim().to_ascii_lowercase();
            match n.as_str() {
                "1" | "true" | "yes" | "on" => Some(true),
                "0" | "false" | "no" | "off" => Some(false),
                _ => None,
            }
        })
        .unwrap_or(false);
    let threshold_ms = std::env::var("AEDB_READ_PHASE_LOG_THRESHOLD_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(5);
    let sample_every = std::env::var("AEDB_READ_PHASE_LOG_SAMPLE_EVERY")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(1)
        .max(1);
    ReadPhaseLogSettings {
        enabled,
        threshold_ms,
        sample_every,
    }
});

static READ_PHASE_LOG_SAMPLE_COUNTER: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));

fn should_log_read_phase(total_micros: u64) -> bool {
    let settings = &*READ_PHASE_LOG_SETTINGS;
    if !settings.enabled || total_micros < settings.threshold_ms.saturating_mul(1_000) {
        return false;
    }
    let n = READ_PHASE_LOG_SAMPLE_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
    n % settings.sample_every == 0
}

fn system_now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

fn reactive_processor_checkpoint_mutation(processor_name: &str, checkpoint_seq: u64) -> Mutation {
    Mutation::Upsert {
        project_id: crate::catalog::SYSTEM_PROJECT_ID.to_string(),
        scope_id: SYSTEM_SCOPE_ID.to_string(),
        table_name: REACTIVE_PROCESSOR_CHECKPOINTS_TABLE.to_string(),
        primary_key: vec![Value::Text(processor_name.to_string().into())],
        row: Row::from_values(vec![
            Value::Text(processor_name.to_string().into()),
            Value::Integer(checkpoint_seq as i64),
            Value::Timestamp(system_now_micros() as i64),
        ]),
    }
}

fn reactive_processor_registry_mutation(
    processor_name: &str,
    options: &ReactiveProcessorOptions,
    enabled: bool,
) -> Result<Mutation, AedbError> {
    let options_json =
        serde_json::to_string(options).map_err(|e| AedbError::Encode(e.to_string()))?;
    Ok(Mutation::Upsert {
        project_id: crate::catalog::SYSTEM_PROJECT_ID.to_string(),
        scope_id: SYSTEM_SCOPE_ID.to_string(),
        table_name: REACTIVE_PROCESSOR_REGISTRY_TABLE.to_string(),
        primary_key: vec![Value::Text(processor_name.to_string().into())],
        row: Row::from_values(vec![
            Value::Text(processor_name.to_string().into()),
            Value::Json(options_json.into()),
            Value::Boolean(enabled),
            Value::Timestamp(system_now_micros() as i64),
        ]),
    })
}

fn reactive_processor_dlq_mutation(
    processor_name: &str,
    events: &[EventOutboxRecord],
    error: &str,
    attempts: u32,
) -> Mutation {
    let failed_at = system_now_micros() as i64;
    let rows = events
        .iter()
        .map(|event| {
            Row::from_values(vec![
                Value::Text(processor_name.to_string().into()),
                Value::Integer(event.commit_seq as i64),
                Value::Text(event.topic.clone().into()),
                Value::Text(event.event_key.clone().into()),
                Value::Json(event.payload_json.clone().into()),
                Value::Text(error.to_string().into()),
                Value::Integer(attempts as i64),
                Value::Timestamp(failed_at),
            ])
        })
        .collect();
    Mutation::UpsertBatch {
        project_id: crate::catalog::SYSTEM_PROJECT_ID.to_string(),
        scope_id: SYSTEM_SCOPE_ID.to_string(),
        table_name: REACTIVE_PROCESSOR_DLQ_TABLE.to_string(),
        rows,
    }
}

#[derive(Debug, Clone, Default)]
struct ReactiveCheckpointAckState {
    last_persisted_seq: u64,
    last_touch_micros: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ReactiveCheckpointAckCacheKey {
    processor_name: String,
    caller_id: Option<String>,
}

const REACTIVE_ACK_CACHE_MAX_ENTRIES: usize = 1_024;
const REACTIVE_ACK_CACHE_EVICT_BATCH: usize = 128;

fn prune_reactive_ack_cache(
    cache: &mut HashMap<ReactiveCheckpointAckCacheKey, ReactiveCheckpointAckState>,
) {
    if cache.len() <= REACTIVE_ACK_CACHE_MAX_ENTRIES {
        return;
    }
    let excess = cache.len() - REACTIVE_ACK_CACHE_MAX_ENTRIES;
    let prune_count = excess
        .saturating_add(REACTIVE_ACK_CACHE_EVICT_BATCH)
        .min(cache.len());
    let mut oldest: Vec<(ReactiveCheckpointAckCacheKey, u64)> = cache
        .iter()
        .map(|(k, v)| (k.clone(), v.last_touch_micros))
        .collect();
    oldest.sort_by_key(|(_, ts)| *ts);
    for (key, _) in oldest.into_iter().take(prune_count) {
        cache.remove(&key);
    }
}

pub type ReactiveProcessorHandlerFuture =
    Pin<Box<dyn Future<Output = Result<(), AedbError>> + Send>>;
pub type ReactiveProcessorHandler = Arc<
    dyn Fn(Arc<AedbInstance>, Vec<EventOutboxRecord>) -> ReactiveProcessorHandlerFuture
        + Send
        + Sync,
>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ReactiveProcessorOptions {
    pub caller_id: Option<String>,
    pub topic_filter: Option<String>,
    pub run_on_interval: bool,
    pub max_allowed_lag_commits: Option<u64>,
    pub max_allowed_stall_ms: Option<u64>,
    pub max_events_per_run: usize,
    pub max_bytes_per_run: usize,
    pub max_run_duration_ms: u64,
    pub run_interval_ms: u64,
    pub idle_backoff_ms: u64,
    pub checkpoint_watermark_commits: u64,
    pub max_retries: u32,
    pub retry_backoff_ms: u64,
}

impl Default for ReactiveProcessorOptions {
    fn default() -> Self {
        Self {
            caller_id: None,
            topic_filter: None,
            run_on_interval: false,
            max_allowed_lag_commits: None,
            max_allowed_stall_ms: None,
            max_events_per_run: 256,
            max_bytes_per_run: 2 * 1024 * 1024,
            max_run_duration_ms: 250,
            run_interval_ms: 20,
            idle_backoff_ms: 50,
            checkpoint_watermark_commits: 64,
            max_retries: 3,
            retry_backoff_ms: 100,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReactiveProcessorRuntimeStatus {
    pub processor_name: String,
    pub running: bool,
    pub runs_total: u64,
    pub processed_events_total: u64,
    pub failures_total: u64,
    pub retries_total: u64,
    pub dead_lettered_total: u64,
    pub last_processed_seq: u64,
    pub last_error: Option<String>,
    pub last_run_started_micros: Option<u64>,
    pub last_run_completed_micros: Option<u64>,
    pub last_success_micros: Option<u64>,
    pub last_failure_micros: Option<u64>,
    pub last_retry_micros: Option<u64>,
    pub last_sleep_ms: u64,
    pub last_batch_events: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReactiveProcessorInfo {
    pub processor_name: String,
    pub options: ReactiveProcessorOptions,
    pub enabled: bool,
    pub running: bool,
    pub updated_at_micros: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReactiveProcessorHealth {
    pub processor_name: String,
    pub enabled: bool,
    pub running: bool,
    pub checkpoint_seq: u64,
    pub head_seq: u64,
    pub lag_commits: u64,
    pub runs_total: u64,
    pub processed_events_total: u64,
    pub failures_total: u64,
    pub retries_total: u64,
    pub dead_lettered_total: u64,
    pub last_processed_seq: u64,
    pub last_error: Option<String>,
    pub last_run_started_micros: Option<u64>,
    pub last_run_completed_micros: Option<u64>,
    pub last_success_micros: Option<u64>,
    pub last_failure_micros: Option<u64>,
    pub last_retry_micros: Option<u64>,
    pub last_sleep_ms: u64,
    pub last_batch_events: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReactiveProcessorSloStatus {
    pub processor_name: String,
    pub breached: bool,
    pub enabled: bool,
    pub running: bool,
    pub lag_commits: u64,
    pub max_allowed_lag_commits: Option<u64>,
    pub stall_ms: Option<u64>,
    pub max_allowed_stall_ms: Option<u64>,
    pub reasons: Vec<String>,
}

#[derive(Debug)]
struct ReactiveProcessorRuntime {
    stop: Arc<AtomicBool>,
    join: JoinHandle<()>,
    stats: Arc<AsyncMutex<ReactiveProcessorRuntimeStatus>>,
}

#[derive(Debug, Clone)]
struct ReactiveProcessorRetryState {
    events: Vec<EventOutboxRecord>,
    last_seq: u64,
    attempts: u32,
    next_retry_at: Instant,
    last_error: String,
}

#[derive(Debug, Clone)]
struct ReactiveProcessorRegistration {
    processor_name: String,
    options: ReactiveProcessorOptions,
    enabled: bool,
    updated_at_micros: u64,
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
    pub permission_rejections: u64,
    pub validation_rejections: u64,
    pub queue_full_rejections: u64,
    pub timeout_rejections: u64,
    pub conflict_rejections: u64,
    pub read_set_conflicts: u64,
    pub conflict_rate: f64,
    pub avg_commit_latency_micros: u64,
    pub coordinator_apply_attempts: u64,
    pub avg_coordinator_apply_micros: u64,
    pub wal_append_ops: u64,
    pub wal_append_bytes: u64,
    pub avg_wal_append_micros: u64,
    pub wal_sync_ops: u64,
    pub avg_wal_sync_micros: u64,
    pub prestage_validate_ops: u64,
    pub avg_prestage_validate_micros: u64,
    pub epoch_process_ops: u64,
    pub avg_epoch_process_micros: u64,
    pub group_commit_filling_epochs: u64,
    pub group_commit_flushing_epochs: u64,
    pub group_commit_complete_epochs: u64,
    pub group_commit_flush_reason_max_group_size: u64,
    pub group_commit_flush_reason_max_group_delay: u64,
    pub group_commit_flush_reason_ingress_drained: u64,
    pub group_commit_flush_reason_structural_barrier: u64,
    pub durable_wait_ops: u64,
    pub avg_durable_wait_micros: u64,
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
pub struct AccumulatorLag {
    pub latest_order_key: u64,
    pub last_applied_order_key: u64,
    pub lag_orders: u64,
    pub latest_seq: u64,
    pub materialized_seq: u64,
    pub lag_commits: u64,
    pub unapplied_deltas: usize,
    pub projector_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccumulatorExposureMetrics {
    pub total_exposure: i64,
    pub available: i64,
    pub rejection_count: u64,
    pub open_exposure_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventOutboxRecord {
    pub commit_seq: u64,
    pub ts_micros: u64,
    pub project_id: String,
    pub scope_id: String,
    pub topic: String,
    pub event_key: String,
    pub payload_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventStreamPage {
    pub events: Vec<EventOutboxRecord>,
    pub next_commit_seq: Option<u64>,
    pub snapshot_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReactiveProcessorLag {
    pub processor_name: String,
    pub checkpoint_seq: u64,
    pub head_seq: u64,
    pub lag_commits: u64,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    AccumulatorDeltaAppended {
        project_id: String,
        scope_id: String,
        accumulator_name: String,
        delta: i64,
        dedupe_key: String,
        order_key: u64,
        release_exposure_id: Option<String>,
        seq: u64,
    },
    AccumulatorExposureReserved {
        project_id: String,
        scope_id: String,
        accumulator_name: String,
        amount: i64,
        exposure_id: String,
        seq: u64,
    },
    AppEventEmitted {
        project_id: String,
        scope_id: String,
        topic: String,
        event_key: String,
        payload_json: String,
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
    pub selected_indexes: Vec<String>,
    pub predicate_evaluation_path: PredicateEvaluationPath,
    pub plan_trace: Vec<String>,
    pub stages: Vec<ExecutionStage>,
    pub bounded_by_limit_or_cursor: bool,
    pub has_joins: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PredicateEvaluationPath {
    None,
    PrimaryKeyEqLookup,
    SecondaryIndexLookup,
    AsyncIndexProjection,
    FullScanFilter,
    JoinExecution,
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
    manager: Option<Arc<Mutex<SnapshotManager>>>,
    handle: Option<SnapshotHandle>,
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
        let (Some(manager), Some(handle)) = (&self.manager, self.handle) else {
            return;
        };
        let mut mgr = manager.lock();
        mgr.release(handle);
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
            idempotency_window_commits = config.idempotency_window_commits,
            max_inflight_commits = config.max_inflight_commits,
            max_commit_queue_bytes = config.max_commit_queue_bytes,
            max_transaction_bytes = config.max_transaction_bytes,
            commit_timeout_ms = config.commit_timeout_ms,
            durable_ack_coalescing_enabled = config.durable_ack_coalescing_enabled,
            durable_ack_coalesce_window_us = config.durable_ack_coalesce_window_us,
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
            checkpoint_compression_level = config.checkpoint_compression_level,
            manifest_hmac_enabled = config.manifest_hmac_key.is_some(),
            recovery_mode = ?config.recovery_mode,
            hash_chain_required = config.hash_chain_required,
            primary_index_backend = ?config.primary_index_backend,
            "aedb config"
        );
        create_private_dir_all(dir)?;
        enforce_and_record_trust_mode(dir, &config)?;
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
        let cursor_signing_key = derive_cursor_signing_key(&config, dir);

        Ok(Self {
            _config: config,
            require_authenticated_calls,
            dir: dir.to_path_buf(),
            cursor_signing_key,
            executor,
            checkpoint_lock: Arc::new(AsyncMutex::new(())),
            snapshot_manager: Arc::new(Mutex::new(SnapshotManager::default())),
            recovery_cache: Arc::new(Mutex::new(RecoveryCache::default())),
            lifecycle_hooks: Arc::new(Mutex::new(Vec::new())),
            lifecycle_hooks_present: Arc::new(AtomicBool::new(false)),
            telemetry_hooks: Arc::new(Mutex::new(Vec::new())),
            telemetry_hooks_present: Arc::new(AtomicBool::new(false)),
            upstream_validation_rejections: Arc::new(AtomicU64::new(0)),
            durable_wait_ops: Arc::new(AtomicU64::new(0)),
            durable_wait_micros: Arc::new(AtomicU64::new(0)),
            durable_ack_fsync_leader: Arc::new(AtomicBool::new(false)),
            reactive_processor_ack_watermarks: Arc::new(Mutex::new(HashMap::new())),
            reactive_processor_handlers: Arc::new(Mutex::new(HashMap::new())),
            reactive_processor_runtimes: Arc::new(AsyncMutex::new(HashMap::new())),
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

        let result = self.executor.submit(mutation).await;
        self.emit_commit_telemetry("commit", started, &result);
        let result = result?;
        self.dispatch_lifecycle_events_for_commit(result.commit_seq)
            .await;
        Ok(result)
    }

    async fn commit_prevalidated_internal(
        &self,
        op_name: &'static str,
        mutation: Mutation,
    ) -> Result<CommitResult, AedbError> {
        let started = Instant::now();
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required; use commit_as in secure mode".into(),
            ));
        }
        crate::commit::validation::validate_kv_sizes_early(&mutation, &self._config)?;
        let result = self.executor.submit_prevalidated(mutation).await;
        self.emit_commit_telemetry(op_name, started, &result);
        result
    }

    async fn commit_prevalidated_system_internal(
        &self,
        op_name: &'static str,
        mutation: Mutation,
    ) -> Result<CommitResult, AedbError> {
        let started = Instant::now();
        crate::commit::validation::validate_kv_sizes_early(&mutation, &self._config)?;
        let result = self.executor.submit_prevalidated(mutation).await;
        self.emit_commit_telemetry(op_name, started, &result);
        result
    }

    async fn commit_envelope_prevalidated_internal(
        &self,
        op_name: &'static str,
        envelope: TransactionEnvelope,
    ) -> Result<CommitResult, AedbError> {
        let started = Instant::now();
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required; use commit_as in secure mode".into(),
            ));
        }
        let result = self.executor.submit_envelope_prevalidated(envelope).await;
        self.emit_commit_telemetry(op_name, started, &result);
        result
    }

    async fn commit_prevalidated_internal_with_finality(
        &self,
        op_name: &'static str,
        mutation: Mutation,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        let mut result = self.commit_prevalidated_internal(op_name, mutation).await?;
        self.enforce_finality(&mut result, finality).await?;
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

    pub async fn insert_batch(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        rows: Vec<crate::catalog::types::Row>,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::InsertBatch {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            table_name: table_name.to_string(),
            rows,
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

        let result = self.executor.submit_as(Some(caller), mutation).await;
        self.emit_commit_telemetry("commit_as", started, &result);
        let result = result?;
        self.dispatch_lifecycle_events_for_commit(result.commit_seq)
            .await;
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
        let result = self.executor.submit_envelope(envelope).await;
        self.emit_commit_telemetry("commit_envelope", started, &result);
        let result = result?;
        self.dispatch_lifecycle_events_for_commit(result.commit_seq)
            .await;
        Ok(result)
    }

    pub async fn commit_action_envelope(
        &self,
        req: ActionEnvelopeRequest,
    ) -> Result<ActionCommitResult, AedbError> {
        let result = self
            .commit_envelope(TransactionEnvelope {
                caller: req.caller,
                idempotency_key: Some(req.idempotency_key),
                write_class: req.write_class,
                assertions: req.assertions,
                read_set: Default::default(),
                write_intent: WriteIntent {
                    mutations: req.mutations,
                },
                base_seq: req.base_seq,
            })
            .await?;
        let outcome = match result.idempotency {
            IdempotencyOutcome::Applied => ActionCommitOutcome::Applied,
            IdempotencyOutcome::Duplicate => ActionCommitOutcome::Duplicate,
        };
        Ok(ActionCommitResult {
            commit_seq: result.commit_seq,
            durable_head_seq: result.durable_head_seq,
            outcome,
        })
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
        if matches!(finality, CommitFinality::Durable)
            && result.durable_head_seq < result.commit_seq
        {
            let wait_started = Instant::now();
            if self._config.durable_ack_coalescing_enabled
                && matches!(self._config.durability_mode, DurabilityMode::Batch)
            {
                let window_us = self._config.durable_ack_coalesce_window_us;
                if window_us > 0 {
                    tokio::time::sleep(Duration::from_micros(window_us)).await;
                }
                if self.executor.durable_head_seq_now() < result.commit_seq {
                    // Give the periodic batch flusher a chance to satisfy durable
                    // finality first; only force fsync if it misses this window.
                    let grace_wait_us = window_us.max(
                        self._config
                            .batch_interval_ms
                            .saturating_mul(1000)
                            .saturating_mul(2),
                    );
                    if grace_wait_us > 0 {
                        let _ = tokio::time::timeout(
                            Duration::from_micros(grace_wait_us),
                            self.wait_for_durable(result.commit_seq),
                        )
                        .await;
                    }

                    if self.executor.durable_head_seq_now() < result.commit_seq {
                        let recently_synced = grace_wait_us > 0
                            && self
                                .executor
                                .last_wal_sync_age_us()
                                .is_some_and(|age| age < grace_wait_us);
                        if recently_synced {
                            let _ = tokio::time::timeout(
                                Duration::from_micros(grace_wait_us),
                                self.wait_for_durable(result.commit_seq),
                            )
                            .await;
                        }
                    }

                    if self.executor.durable_head_seq_now() < result.commit_seq {
                        if self
                            .durable_ack_fsync_leader
                            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                            .is_ok()
                        {
                            struct LeaderGuard<'a>(&'a AtomicBool);
                            impl Drop for LeaderGuard<'_> {
                                fn drop(&mut self) {
                                    self.0.store(false, Ordering::Release);
                                }
                            }
                            let _guard = LeaderGuard(&self.durable_ack_fsync_leader);
                            let _ = self.force_fsync().await?;
                        } else {
                            self.wait_for_durable(result.commit_seq).await?;
                        }
                    }
                }
            } else {
                self.wait_for_durable(result.commit_seq).await?;
            }
            self.durable_wait_ops.fetch_add(1, Ordering::Relaxed);
            self.durable_wait_micros
                .fetch_add(wait_started.elapsed().as_micros() as u64, Ordering::Relaxed);
            result.durable_head_seq = self.executor.durable_head_seq_now();
        }
        Ok(())
    }

    async fn dispatch_lifecycle_events_for_commit(&self, commit_seq: u64) {
        if !self.lifecycle_hooks_present.load(Ordering::Acquire) {
            return;
        }
        let events = match self.read_lifecycle_events_for_commit(commit_seq).await {
            Ok(events) => events,
            Err(err) => {
                warn!(commit_seq, error = ?err, "failed to read lifecycle outbox");
                return;
            }
        };
        self.dispatch_lifecycle_events(events);
    }

    async fn read_lifecycle_events_for_commit(
        &self,
        commit_seq: u64,
    ) -> Result<Vec<LifecycleEvent>, AedbError> {
        let ns = namespace_key(crate::catalog::SYSTEM_PROJECT_ID, SYSTEM_SCOPE_ID);
        let table_key = (ns.clone(), LIFECYCLE_OUTBOX_TABLE.to_string());
        let (snapshot, catalog, _) = self.executor.snapshot_state().await;
        let Some(schema) = catalog.tables.get(&table_key) else {
            return Ok(Vec::new());
        };
        let Some(events_idx) = schema.columns.iter().position(|c| c.name == "events") else {
            return Ok(Vec::new());
        };
        let Some(table) = snapshot.table(
            crate::catalog::SYSTEM_PROJECT_ID,
            SYSTEM_SCOPE_ID,
            LIFECYCLE_OUTBOX_TABLE,
        ) else {
            return Ok(Vec::new());
        };
        let encoded_pk = EncodedKey::from_values(&[Value::Integer(commit_seq as i64)]);
        let Some(row) = table.rows.get(&encoded_pk) else {
            return Ok(Vec::new());
        };
        let Some(Value::Json(events_json)) = row.values.get(events_idx) else {
            return Ok(Vec::new());
        };
        serde_json::from_str(events_json.as_str()).map_err(|e| AedbError::Decode(e.to_string()))
    }

    fn dispatch_lifecycle_events(&self, events: Vec<LifecycleEvent>) {
        if events.is_empty() {
            return;
        }
        if !self.lifecycle_hooks_present.load(Ordering::Acquire) {
            return;
        }
        let hooks = self.lifecycle_hooks.lock().clone();
        if hooks.is_empty() {
            return;
        }
        tokio::spawn(async move {
            for event in events {
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
        if !self.telemetry_hooks_present.load(Ordering::Acquire) {
            return;
        }
        let hooks = self.telemetry_hooks.lock().clone();
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
        if !self.telemetry_hooks_present.load(Ordering::Acquire) {
            return;
        }
        let hooks = self.telemetry_hooks.lock().clone();
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

    fn maybe_log_read_phase(
        &self,
        op: &'static str,
        consistency: ConsistencyMode,
        snapshot_micros: u64,
        execute_micros: u64,
        units: usize,
        ok: bool,
    ) {
        let total_micros = snapshot_micros.saturating_add(execute_micros);
        if !should_log_read_phase(total_micros) {
            return;
        }
        info!(
            target: "aedb.read_phase",
            operation = op,
            total_us = total_micros,
            total_ms = total_micros / 1_000,
            snapshot_us = snapshot_micros,
            snapshot_ms = snapshot_micros / 1_000,
            execute_us = execute_micros,
            execute_ms = execute_micros / 1_000,
            units,
            consistency = ?consistency,
            ok,
            "aedb read phase timing"
        );
    }

    fn normalize_query_cursor_options(&self, options: &mut QueryOptions) -> Result<(), QueryError> {
        let Some(cursor) = options.cursor.take() else {
            return Ok(());
        };
        let (raw_cursor, snapshot_seq) =
            verify_signed_query_cursor(&cursor, &self.cursor_signing_key)
                .map_err(QueryError::from)?;
        options.consistency = ConsistencyMode::AtSeq(snapshot_seq);
        options.cursor = Some(raw_cursor);
        Ok(())
    }

    fn sign_query_result_cursor(&self, result: &mut QueryResult) -> Result<(), QueryError> {
        let Some(raw_cursor) = result.cursor.take() else {
            return Ok(());
        };
        let signed =
            sign_query_cursor(&raw_cursor, &self.cursor_signing_key).map_err(QueryError::from)?;
        result.cursor = Some(signed);
        Ok(())
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
        if self.require_authenticated_calls {
            return Err(QueryError::PermissionDenied {
                permission: "query_no_auth is unavailable in secure mode".into(),
                scope: "anonymous".into(),
            });
        }
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
        if let Some(caller) = caller {
            ensure_query_caller_allowed(caller)?;
        }

        if options.async_index.is_none() {
            options.async_index = query.use_index.clone();
        }

        self.normalize_query_cursor_options(&mut options)?;

        let snapshot_started = Instant::now();
        let view = self
            .snapshot_for_consistency(options.consistency)
            .await
            .map_err(QueryError::from)?;
        let snapshot_micros = snapshot_started.elapsed().as_micros() as u64;
        let started = Instant::now();
        let execute_started = Instant::now();
        let table = query.table.clone();
        let result = execute_query_against_view(
            &view,
            project_id,
            scope_id,
            query,
            &options,
            caller,
            self._config.max_scan_rows,
        );
        let execute_micros = execute_started.elapsed().as_micros() as u64;
        let units = result
            .as_ref()
            .map(|res| res.rows_examined)
            .unwrap_or_default();
        self.maybe_log_read_phase(
            "query_with_options",
            options.consistency,
            snapshot_micros,
            execute_micros,
            units,
            result.is_ok(),
        );
        let mut result = result?;
        self.sign_query_result_cursor(&mut result)?;
        let result = Ok(result);
        self.emit_query_telemetry(
            started,
            project_id,
            scope_id,
            &table,
            view.seq,
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

        let mut options = options;
        self.normalize_query_cursor_options(&mut options)?;

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
            // No read set/assertions in this helper path.
            // Keep hot-path parity with submit/submit_as and avoid snapshot acquisition.
            base_seq: 0,
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
            // No read set/assertions in this helper path.
            // Keep hot-path parity with submit/submit_as and avoid snapshot acquisition.
            base_seq: 0,
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
        ensure_external_caller_allowed(caller)?;
        let snapshot_started = Instant::now();
        let lease = self
            .acquire_snapshot(consistency)
            .await
            .map_err(QueryError::from)?;
        let snapshot_micros = snapshot_started.elapsed().as_micros() as u64;
        let snapshot = &lease.view.keyspace;
        let catalog = &lease.view.catalog;
        if !catalog.has_kv_read_permission(&caller.caller_id, project_id, scope_id, key) {
            return Err(QueryError::PermissionDenied {
                permission: format!("KvRead({project_id}.{scope_id})"),
                scope: caller.caller_id.clone(),
            });
        }
        let execute_started = Instant::now();
        let result = snapshot.kv_get(project_id, scope_id, key).cloned();
        let execute_micros = execute_started.elapsed().as_micros() as u64;
        self.maybe_log_read_phase(
            "kv_get",
            consistency,
            snapshot_micros,
            execute_micros,
            usize::from(result.is_some()),
            true,
        );
        Ok(result)
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
        let snapshot_started = Instant::now();
        let lease = self
            .acquire_snapshot(consistency)
            .await
            .map_err(QueryError::from)?;
        let snapshot_micros = snapshot_started.elapsed().as_micros() as u64;
        let execute_started = Instant::now();
        let result = lease
            .view
            .keyspace
            .kv_get(project_id, scope_id, key)
            .cloned();
        let execute_micros = execute_started.elapsed().as_micros() as u64;
        self.maybe_log_read_phase(
            "kv_get",
            consistency,
            snapshot_micros,
            execute_micros,
            usize::from(result.is_some()),
            true,
        );
        Ok(result)
    }

    pub async fn kv_get_no_auth(
        &self,
        project_id: &str,
        scope_id: &str,
        key: &[u8],
        consistency: ConsistencyMode,
    ) -> Result<Option<KvEntry>, QueryError> {
        if self.require_authenticated_calls {
            return Err(QueryError::PermissionDenied {
                permission: "kv_get_no_auth is unavailable in secure mode".into(),
                scope: "anonymous".into(),
            });
        }
        self.kv_get_unchecked(project_id, scope_id, key, consistency)
            .await
    }

    pub async fn kv_get_many_no_auth(
        &self,
        project_id: &str,
        scope_id: &str,
        keys: &[Vec<u8>],
        consistency: ConsistencyMode,
    ) -> Result<Vec<Option<KvEntry>>, QueryError> {
        if self.require_authenticated_calls {
            return Err(QueryError::PermissionDenied {
                permission: "kv_get_many_no_auth is unavailable in secure mode".into(),
                scope: "anonymous".into(),
            });
        }

        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let snapshot_started = Instant::now();
        let lease = self
            .acquire_snapshot(consistency)
            .await
            .map_err(QueryError::from)?;
        let snapshot_micros = snapshot_started.elapsed().as_micros() as u64;
        let snapshot = &lease.view.keyspace;
        let execute_started = Instant::now();
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            out.push(snapshot.kv_get(project_id, scope_id, key).cloned());
        }
        let execute_micros = execute_started.elapsed().as_micros() as u64;
        self.maybe_log_read_phase(
            "kv_get_many",
            consistency,
            snapshot_micros,
            execute_micros,
            keys.len(),
            true,
        );
        Ok(out)
    }

    pub(crate) async fn kv_scan_prefix_unchecked(
        &self,
        project_id: &str,
        scope_id: &str,
        prefix: &[u8],
        limit: u64,
        consistency: ConsistencyMode,
    ) -> Result<Vec<(Vec<u8>, KvEntry)>, QueryError> {
        let snapshot_started = Instant::now();
        let lease = self
            .acquire_snapshot(consistency)
            .await
            .map_err(QueryError::from)?;
        let snapshot_micros = snapshot_started.elapsed().as_micros() as u64;
        let page_size = limit.min(self._config.max_scan_rows as u64) as usize;
        let start_bound = Bound::Included(prefix.to_vec());
        let end_bound = next_prefix_bytes(prefix).map_or(Bound::Unbounded, Bound::Excluded);
        let ns = NamespaceId::project_scope(project_id, scope_id);
        let execute_started = Instant::now();
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
        let execute_micros = execute_started.elapsed().as_micros() as u64;
        self.maybe_log_read_phase(
            "kv_scan_prefix",
            consistency,
            snapshot_micros,
            execute_micros,
            entries.len(),
            true,
        );
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
        if self.require_authenticated_calls {
            return Err(QueryError::PermissionDenied {
                permission: "kv_scan_prefix_no_auth is unavailable in secure mode".into(),
                scope: "anonymous".into(),
            });
        }
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
        ensure_external_caller_allowed(caller)?;
        let effective_consistency = if let Some(c) = &cursor {
            ConsistencyMode::AtSeq(c.snapshot_seq)
        } else {
            consistency
        };
        let snapshot_started = Instant::now();
        let lease = self
            .acquire_snapshot(effective_consistency)
            .await
            .map_err(QueryError::from)?;
        let snapshot_micros = snapshot_started.elapsed().as_micros() as u64;
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
        let execute_started = Instant::now();
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
        let execute_micros = execute_started.elapsed().as_micros() as u64;
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
        .inspect(|res| {
            self.maybe_log_read_phase(
                "kv_scan_prefix",
                effective_consistency,
                snapshot_micros,
                execute_micros,
                res.entries.len(),
                true,
            );
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
        ensure_external_caller_allowed(caller)?;
        let effective_consistency = if let Some(c) = &cursor {
            ConsistencyMode::AtSeq(c.snapshot_seq)
        } else {
            consistency
        };
        let snapshot_started = Instant::now();
        let lease = self
            .acquire_snapshot(effective_consistency)
            .await
            .map_err(QueryError::from)?;
        let snapshot_micros = snapshot_started.elapsed().as_micros() as u64;
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
        let execute_started = Instant::now();
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
        let execute_micros = execute_started.elapsed().as_micros() as u64;
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
        .inspect(|res| {
            self.maybe_log_read_phase(
                "kv_scan_range",
                effective_consistency,
                snapshot_micros,
                execute_micros,
                res.entries.len(),
                true,
            );
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
        ensure_external_caller_allowed(caller)?;
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

    pub async fn kv_add_u64_ex(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 8],
        on_missing: KvU64MissingPolicy,
        on_overflow: KvU64OverflowPolicy,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvAddU64Ex {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            amount_be,
            on_missing,
            on_overflow,
        })
        .await
    }

    pub async fn kv_sub_u64_ex(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 8],
        on_missing: KvU64MissingPolicy,
        on_underflow: KvU64UnderflowPolicy,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvSubU64Ex {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            amount_be,
            on_missing,
            on_underflow,
        })
        .await
    }

    pub async fn kv_max_u64(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        candidate_be: [u8; 8],
        on_missing: KvU64MissingPolicy,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvMaxU64 {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            candidate_be,
            on_missing,
        })
        .await
    }

    pub async fn kv_min_u64(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        candidate_be: [u8; 8],
        on_missing: KvU64MissingPolicy,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvMinU64 {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            candidate_be,
            on_missing,
        })
        .await
    }

    pub async fn kv_mutate_u64(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        op: KvU64MutatorOp,
        operand_be: [u8; 8],
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvMutateU64 {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            op,
            operand_be,
            expected_seq: None,
        })
        .await
    }

    pub async fn kv_mutate_u64_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        op: KvU64MutatorOp,
        operand_be: [u8; 8],
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::KvMutateU64 {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key,
                op,
                operand_be,
                expected_seq: None,
            },
        )
        .await
    }

    pub async fn counter_add_sharded(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 8],
        shard_count: u16,
        shard_hint: u32,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::CounterAdd {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            amount_be,
            shard_count,
            shard_hint,
        })
        .await
    }

    pub async fn counter_add_sharded_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 8],
        shard_count: u16,
        shard_hint: u32,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::CounterAdd {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key,
                amount_be,
                shard_count,
                shard_hint,
            },
        )
        .await
    }

    pub async fn counter_read_sharded(
        &self,
        project_id: &str,
        scope_id: &str,
        key: &[u8],
        shard_count: u16,
        consistency: ConsistencyMode,
    ) -> Result<u64, AedbError> {
        if shard_count == 0 {
            return Err(AedbError::Validation(
                "counter shard_count must be > 0".into(),
            ));
        }
        if shard_count > MAX_COUNTER_SHARDS {
            return Err(AedbError::Validation(format!(
                "counter shard_count exceeds maximum {}",
                MAX_COUNTER_SHARDS
            )));
        }
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        let lease = self.acquire_snapshot(consistency).await?;
        lease
            .view
            .keyspace
            .counter_read_sharded(project_id, scope_id, key, shard_count)
    }

    pub async fn counter_read_sharded_as(
        &self,
        caller: &CallerContext,
        project_id: &str,
        scope_id: &str,
        key: &[u8],
        shard_count: u16,
        consistency: ConsistencyMode,
    ) -> Result<u64, AedbError> {
        if shard_count == 0 {
            return Err(AedbError::Validation(
                "counter shard_count must be > 0".into(),
            ));
        }
        if shard_count > MAX_COUNTER_SHARDS {
            return Err(AedbError::Validation(format!(
                "counter shard_count exceeds maximum {}",
                MAX_COUNTER_SHARDS
            )));
        }
        ensure_external_caller_allowed(caller)?;
        let lease = self.acquire_snapshot(consistency).await?;
        if !lease
            .view
            .catalog
            .has_kv_read_permission(&caller.caller_id, project_id, scope_id, key)
        {
            return Err(AedbError::PermissionDenied(format!(
                "caller={} missing kv read permission for counter",
                caller.caller_id
            )));
        }
        lease
            .view
            .keyspace
            .counter_read_sharded(project_id, scope_id, key, shard_count)
    }

    pub async fn create_accumulator(
        &self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        dedupe_retain_commits: Option<u64>,
        snapshot_every: u64,
    ) -> Result<CommitResult, AedbError> {
        self.create_accumulator_with_options(
            project_id,
            scope_id,
            accumulator_name,
            dedupe_retain_commits,
            snapshot_every,
            1_000,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_accumulator_with_options(
        &self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        dedupe_retain_commits: Option<u64>,
        snapshot_every: u64,
        exposure_margin_bps: u32,
        exposure_ttl_commits: Option<u64>,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::Ddl(DdlOperation::CreateAccumulator {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            accumulator_name: accumulator_name.to_string(),
            if_not_exists: false,
            value_type: AccumulatorValueType::BigInt,
            dedupe_retain_commits,
            snapshot_every,
            exposure_margin_bps,
            exposure_ttl_commits,
        }))
        .await
    }

    pub async fn create_accumulator_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        dedupe_retain_commits: Option<u64>,
        snapshot_every: u64,
    ) -> Result<CommitResult, AedbError> {
        self.create_accumulator_with_options_as(
            caller,
            project_id,
            scope_id,
            accumulator_name,
            dedupe_retain_commits,
            snapshot_every,
            1_000,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_accumulator_with_options_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        dedupe_retain_commits: Option<u64>,
        snapshot_every: u64,
        exposure_margin_bps: u32,
        exposure_ttl_commits: Option<u64>,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::Ddl(DdlOperation::CreateAccumulator {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                accumulator_name: accumulator_name.to_string(),
                if_not_exists: false,
                value_type: AccumulatorValueType::BigInt,
                dedupe_retain_commits,
                snapshot_every,
                exposure_margin_bps,
                exposure_ttl_commits,
            }),
        )
        .await
    }

    pub async fn drop_accumulator(
        &self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::Ddl(DdlOperation::DropAccumulator {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            accumulator_name: accumulator_name.to_string(),
            if_exists: true,
        }))
        .await
    }

    pub async fn drop_accumulator_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::Ddl(DdlOperation::DropAccumulator {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                accumulator_name: accumulator_name.to_string(),
                if_exists: true,
            }),
        )
        .await
    }

    pub async fn accumulate(
        &self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        delta: i64,
        dedupe_key: String,
        order_key: u64,
    ) -> Result<CommitResult, AedbError> {
        self.accumulate_with_release(
            project_id,
            scope_id,
            accumulator_name,
            delta,
            dedupe_key,
            order_key,
            None,
        )
        .await
    }

    pub async fn accumulate_with_release(
        &self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        delta: i64,
        dedupe_key: String,
        order_key: u64,
        release_exposure_id: Option<String>,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::Accumulate {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            accumulator_name: accumulator_name.to_string(),
            delta,
            dedupe_key,
            order_key,
            release_exposure_id,
        })
        .await
    }

    pub async fn accumulate_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        delta: i64,
        dedupe_key: String,
        order_key: u64,
    ) -> Result<CommitResult, AedbError> {
        self.accumulate_with_release_as(
            caller,
            project_id,
            scope_id,
            accumulator_name,
            delta,
            dedupe_key,
            order_key,
            None,
        )
        .await
    }

    pub async fn accumulate_with_release_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        delta: i64,
        dedupe_key: String,
        order_key: u64,
        release_exposure_id: Option<String>,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::Accumulate {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                accumulator_name: accumulator_name.to_string(),
                delta,
                dedupe_key,
                order_key,
                release_exposure_id,
            },
        )
        .await
    }

    pub async fn expose_accumulator(
        &self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        amount: i64,
        exposure_id: String,
    ) -> Result<CommitResult, AedbError> {
        if amount <= 0 {
            return Err(AedbError::Validation("exposure amount must be > 0".into()));
        }
        if exposure_id.trim().is_empty() {
            return Err(AedbError::Validation("exposure_id cannot be empty".into()));
        }
        // EXPOSE is a hot-path primitive; prevalidated submit skips prestage mutation checks.
        self.commit_prevalidated_internal(
            "expose_accumulator",
            Mutation::ExposeAccumulator {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                accumulator_name: accumulator_name.to_string(),
                amount,
                exposure_id,
            },
        )
        .await
    }

    pub async fn expose_accumulator_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        amount: i64,
        exposure_id: String,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::ExposeAccumulator {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                accumulator_name: accumulator_name.to_string(),
                amount,
                exposure_id,
            },
        )
        .await
    }

    pub async fn expose_accumulator_with_preflight(
        &self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        amount: i64,
        exposure_id: String,
    ) -> Result<CommitResult, AedbError> {
        self.commit_with_preflight(Mutation::ExposeAccumulator {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            accumulator_name: accumulator_name.to_string(),
            amount,
            exposure_id,
        })
        .await
    }

    pub async fn expose_accumulator_many_atomic(
        &self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        exposures: Vec<(i64, String)>,
    ) -> Result<CommitResult, AedbError> {
        if exposures.is_empty() {
            return Err(AedbError::Validation(
                "expose_accumulator_many_atomic requires at least one exposure".into(),
            ));
        }
        for (amount, exposure_id) in &exposures {
            if *amount <= 0 {
                return Err(AedbError::Validation("exposure amount must be > 0".into()));
            }
            if exposure_id.trim().is_empty() {
                return Err(AedbError::Validation("exposure_id cannot be empty".into()));
            }
        }
        self.commit_prevalidated_internal(
            "expose_accumulator_many_atomic",
            Mutation::ExposeAccumulatorBatch {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                accumulator_name: accumulator_name.to_string(),
                exposures,
            },
        )
        .await
    }

    pub async fn expose_accumulator_many_atomic_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        exposures: Vec<(i64, String)>,
    ) -> Result<CommitResult, AedbError> {
        if exposures.is_empty() {
            return Err(AedbError::Validation(
                "expose_accumulator_many_atomic_as requires at least one exposure".into(),
            ));
        }
        for (amount, exposure_id) in &exposures {
            if *amount <= 0 {
                return Err(AedbError::Validation("exposure amount must be > 0".into()));
            }
            if exposure_id.trim().is_empty() {
                return Err(AedbError::Validation("exposure_id cannot be empty".into()));
            }
        }
        self.commit_as(
            caller,
            Mutation::ExposeAccumulatorBatch {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                accumulator_name: accumulator_name.to_string(),
                exposures,
            },
        )
        .await
    }

    pub async fn release_accumulator_exposure(
        &self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        exposure_id: String,
    ) -> Result<CommitResult, AedbError> {
        if exposure_id.trim().is_empty() {
            return Err(AedbError::Validation("exposure_id cannot be empty".into()));
        }
        self.commit(Mutation::ReleaseAccumulatorExposure {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            accumulator_name: accumulator_name.to_string(),
            exposure_id,
        })
        .await
    }

    pub async fn release_accumulator_exposure_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        exposure_id: String,
    ) -> Result<CommitResult, AedbError> {
        if exposure_id.trim().is_empty() {
            return Err(AedbError::Validation("exposure_id cannot be empty".into()));
        }
        self.commit_as(
            caller,
            Mutation::ReleaseAccumulatorExposure {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                accumulator_name: accumulator_name.to_string(),
                exposure_id,
            },
        )
        .await
    }

    pub async fn emit_event(
        &self,
        project_id: &str,
        scope_id: &str,
        topic: &str,
        event_key: String,
        payload_json: String,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::EmitEvent {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            topic: topic.to_string(),
            event_key,
            payload_json,
        })
        .await
    }

    pub async fn emit_event_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        topic: &str,
        event_key: String,
        payload_json: String,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::EmitEvent {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                topic: topic.to_string(),
                event_key,
                payload_json,
            },
        )
        .await
    }

    pub async fn accumulator_value(
        &self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        consistency: ConsistencyMode,
    ) -> Result<i64, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        let lease = self.acquire_snapshot(consistency).await?;
        let Some(acc) = lease
            .view
            .keyspace
            .accumulator(project_id, scope_id, accumulator_name)
        else {
            return Err(AedbError::Validation(format!(
                "accumulator does not exist: {project_id}.{scope_id}.{accumulator_name}"
            )));
        };
        Ok(acc.value)
    }

    pub async fn accumulator_value_as(
        &self,
        caller: &CallerContext,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        consistency: ConsistencyMode,
    ) -> Result<i64, AedbError> {
        ensure_external_caller_allowed(caller)?;
        let lease = self.acquire_snapshot(consistency).await?;
        if !lease.view.catalog.has_kv_read_permission(
            &caller.caller_id,
            project_id,
            scope_id,
            accumulator_name.as_bytes(),
        ) {
            return Err(AedbError::PermissionDenied(format!(
                "caller={} missing kv read permission for accumulator",
                caller.caller_id
            )));
        }
        let Some(acc) = lease
            .view
            .keyspace
            .accumulator(project_id, scope_id, accumulator_name)
        else {
            return Err(AedbError::Validation(format!(
                "accumulator does not exist: {project_id}.{scope_id}.{accumulator_name}"
            )));
        };
        Ok(acc.value)
    }

    pub async fn accumulator_value_strong(
        &self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        consistency: ConsistencyMode,
    ) -> Result<i64, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        let lease = self.acquire_snapshot(consistency).await?;
        lease
            .view
            .keyspace
            .accumulator_effective_value(project_id, scope_id, accumulator_name)?
            .ok_or_else(|| {
                AedbError::Validation(format!(
                    "accumulator does not exist: {project_id}.{scope_id}.{accumulator_name}"
                ))
            })
    }

    pub async fn accumulator_value_strong_as(
        &self,
        caller: &CallerContext,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        consistency: ConsistencyMode,
    ) -> Result<i64, AedbError> {
        ensure_external_caller_allowed(caller)?;
        let lease = self.acquire_snapshot(consistency).await?;
        if !lease.view.catalog.has_kv_read_permission(
            &caller.caller_id,
            project_id,
            scope_id,
            accumulator_name.as_bytes(),
        ) {
            return Err(AedbError::PermissionDenied(format!(
                "caller={} missing kv read permission for accumulator",
                caller.caller_id
            )));
        }
        lease
            .view
            .keyspace
            .accumulator_effective_value(project_id, scope_id, accumulator_name)?
            .ok_or_else(|| {
                AedbError::Validation(format!(
                    "accumulator does not exist: {project_id}.{scope_id}.{accumulator_name}"
                ))
            })
    }

    pub async fn accumulator_lag(
        &self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        consistency: ConsistencyMode,
    ) -> Result<AccumulatorLag, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        let lease = self.acquire_snapshot(consistency).await?;
        let Some(acc) = lease
            .view
            .keyspace
            .accumulator(project_id, scope_id, accumulator_name)
        else {
            return Err(AedbError::Validation(format!(
                "accumulator does not exist: {project_id}.{scope_id}.{accumulator_name}"
            )));
        };
        let lag_orders = acc
            .latest_order_key
            .saturating_sub(acc.last_applied_order_key);
        let lag_commits = acc.latest_seq.saturating_sub(acc.materialized_seq);
        let unapplied_deltas = acc
            .deltas
            .range((
                Bound::Excluded(acc.last_applied_order_key),
                Bound::Unbounded,
            ))
            .count();
        Ok(AccumulatorLag {
            latest_order_key: acc.latest_order_key,
            last_applied_order_key: acc.last_applied_order_key,
            lag_orders,
            latest_seq: acc.latest_seq,
            materialized_seq: acc.materialized_seq,
            lag_commits,
            unapplied_deltas,
            projector_error: acc.projector_error.clone(),
        })
    }

    pub async fn accumulator_lag_as(
        &self,
        caller: &CallerContext,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        consistency: ConsistencyMode,
    ) -> Result<AccumulatorLag, AedbError> {
        ensure_external_caller_allowed(caller)?;
        let lease = self.acquire_snapshot(consistency).await?;
        if !lease.view.catalog.has_kv_read_permission(
            &caller.caller_id,
            project_id,
            scope_id,
            accumulator_name.as_bytes(),
        ) {
            return Err(AedbError::PermissionDenied(format!(
                "caller={} missing kv read permission for accumulator",
                caller.caller_id
            )));
        }
        let Some(acc) = lease
            .view
            .keyspace
            .accumulator(project_id, scope_id, accumulator_name)
        else {
            return Err(AedbError::Validation(format!(
                "accumulator does not exist: {project_id}.{scope_id}.{accumulator_name}"
            )));
        };
        let lag_orders = acc
            .latest_order_key
            .saturating_sub(acc.last_applied_order_key);
        let lag_commits = acc.latest_seq.saturating_sub(acc.materialized_seq);
        let unapplied_deltas = acc
            .deltas
            .range((
                Bound::Excluded(acc.last_applied_order_key),
                Bound::Unbounded,
            ))
            .count();
        Ok(AccumulatorLag {
            latest_order_key: acc.latest_order_key,
            last_applied_order_key: acc.last_applied_order_key,
            lag_orders,
            latest_seq: acc.latest_seq,
            materialized_seq: acc.materialized_seq,
            lag_commits,
            unapplied_deltas,
            projector_error: acc.projector_error.clone(),
        })
    }

    pub async fn accumulator_exposure(
        &self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        consistency: ConsistencyMode,
    ) -> Result<i64, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        let lease = self.acquire_snapshot(consistency).await?;
        let Some(acc) = lease
            .view
            .keyspace
            .accumulator(project_id, scope_id, accumulator_name)
        else {
            return Err(AedbError::Validation(format!(
                "accumulator does not exist: {project_id}.{scope_id}.{accumulator_name}"
            )));
        };
        Ok(acc.total_exposure)
    }

    pub async fn accumulator_exposure_as(
        &self,
        caller: &CallerContext,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        consistency: ConsistencyMode,
    ) -> Result<i64, AedbError> {
        ensure_external_caller_allowed(caller)?;
        let lease = self.acquire_snapshot(consistency).await?;
        if !lease.view.catalog.has_kv_read_permission(
            &caller.caller_id,
            project_id,
            scope_id,
            accumulator_name.as_bytes(),
        ) {
            return Err(AedbError::PermissionDenied(format!(
                "caller={} missing kv read permission for accumulator",
                caller.caller_id
            )));
        }
        let Some(acc) = lease
            .view
            .keyspace
            .accumulator(project_id, scope_id, accumulator_name)
        else {
            return Err(AedbError::Validation(format!(
                "accumulator does not exist: {project_id}.{scope_id}.{accumulator_name}"
            )));
        };
        Ok(acc.total_exposure)
    }

    pub async fn accumulator_available(
        &self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        consistency: ConsistencyMode,
    ) -> Result<i64, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        let lease = self.acquire_snapshot(consistency).await?;
        let Some(acc) = lease
            .view
            .keyspace
            .accumulator(project_id, scope_id, accumulator_name)
        else {
            return Err(AedbError::Validation(format!(
                "accumulator does not exist: {project_id}.{scope_id}.{accumulator_name}"
            )));
        };
        let effective = lease
            .view
            .keyspace
            .accumulator_effective_value(project_id, scope_id, accumulator_name)?
            .ok_or_else(|| {
                AedbError::Validation(format!(
                    "accumulator does not exist: {project_id}.{scope_id}.{accumulator_name}"
                ))
            })?;
        effective
            .checked_sub(acc.total_exposure)
            .ok_or(AedbError::Overflow)
    }

    pub async fn accumulator_available_as(
        &self,
        caller: &CallerContext,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        consistency: ConsistencyMode,
    ) -> Result<i64, AedbError> {
        ensure_external_caller_allowed(caller)?;
        let lease = self.acquire_snapshot(consistency).await?;
        if !lease.view.catalog.has_kv_read_permission(
            &caller.caller_id,
            project_id,
            scope_id,
            accumulator_name.as_bytes(),
        ) {
            return Err(AedbError::PermissionDenied(format!(
                "caller={} missing kv read permission for accumulator",
                caller.caller_id
            )));
        }
        let Some(acc) = lease
            .view
            .keyspace
            .accumulator(project_id, scope_id, accumulator_name)
        else {
            return Err(AedbError::Validation(format!(
                "accumulator does not exist: {project_id}.{scope_id}.{accumulator_name}"
            )));
        };
        let effective = lease
            .view
            .keyspace
            .accumulator_effective_value(project_id, scope_id, accumulator_name)?
            .ok_or_else(|| {
                AedbError::Validation(format!(
                    "accumulator does not exist: {project_id}.{scope_id}.{accumulator_name}"
                ))
            })?;
        effective
            .checked_sub(acc.total_exposure)
            .ok_or(AedbError::Overflow)
    }

    pub async fn accumulator_exposure_metrics(
        &self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        consistency: ConsistencyMode,
    ) -> Result<AccumulatorExposureMetrics, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        let lease = self.acquire_snapshot(consistency).await?;
        let Some(acc) = lease
            .view
            .keyspace
            .accumulator(project_id, scope_id, accumulator_name)
        else {
            return Err(AedbError::Validation(format!(
                "accumulator does not exist: {project_id}.{scope_id}.{accumulator_name}"
            )));
        };
        let effective = lease
            .view
            .keyspace
            .accumulator_effective_value(project_id, scope_id, accumulator_name)?
            .ok_or_else(|| {
                AedbError::Validation(format!(
                    "accumulator does not exist: {project_id}.{scope_id}.{accumulator_name}"
                ))
            })?;
        let available = effective
            .checked_sub(acc.total_exposure)
            .ok_or(AedbError::Overflow)?;
        Ok(AccumulatorExposureMetrics {
            total_exposure: acc.total_exposure,
            available,
            rejection_count: acc.exposure_rejections,
            open_exposure_count: acc.open_exposures.len(),
        })
    }

    pub async fn accumulator_exposure_metrics_as(
        &self,
        caller: &CallerContext,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        consistency: ConsistencyMode,
    ) -> Result<AccumulatorExposureMetrics, AedbError> {
        ensure_external_caller_allowed(caller)?;
        let lease = self.acquire_snapshot(consistency).await?;
        if !lease.view.catalog.has_kv_read_permission(
            &caller.caller_id,
            project_id,
            scope_id,
            accumulator_name.as_bytes(),
        ) {
            return Err(AedbError::PermissionDenied(format!(
                "caller={} missing kv read permission for accumulator",
                caller.caller_id
            )));
        }
        let Some(acc) = lease
            .view
            .keyspace
            .accumulator(project_id, scope_id, accumulator_name)
        else {
            return Err(AedbError::Validation(format!(
                "accumulator does not exist: {project_id}.{scope_id}.{accumulator_name}"
            )));
        };
        let effective = lease
            .view
            .keyspace
            .accumulator_effective_value(project_id, scope_id, accumulator_name)?
            .ok_or_else(|| {
                AedbError::Validation(format!(
                    "accumulator does not exist: {project_id}.{scope_id}.{accumulator_name}"
                ))
            })?;
        let available = effective
            .checked_sub(acc.total_exposure)
            .ok_or(AedbError::Overflow)?;
        Ok(AccumulatorExposureMetrics {
            total_exposure: acc.total_exposure,
            available,
            rejection_count: acc.exposure_rejections,
            open_exposure_count: acc.open_exposures.len(),
        })
    }

    pub async fn read_event_stream(
        &self,
        topic_filter: Option<&str>,
        from_commit_seq_exclusive: u64,
        limit: usize,
        consistency: ConsistencyMode,
    ) -> Result<EventStreamPage, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        self.read_event_stream_internal(
            topic_filter,
            from_commit_seq_exclusive,
            limit,
            consistency,
            None,
        )
        .await
    }

    pub async fn read_event_stream_as(
        &self,
        caller: &CallerContext,
        topic_filter: Option<&str>,
        from_commit_seq_exclusive: u64,
        limit: usize,
        consistency: ConsistencyMode,
    ) -> Result<EventStreamPage, AedbError> {
        ensure_external_caller_allowed(caller)?;
        self.read_event_stream_internal(
            topic_filter,
            from_commit_seq_exclusive,
            limit,
            consistency,
            Some(caller),
        )
        .await
    }

    async fn read_event_stream_internal(
        &self,
        topic_filter: Option<&str>,
        from_commit_seq_exclusive: u64,
        limit: usize,
        consistency: ConsistencyMode,
        caller: Option<&CallerContext>,
    ) -> Result<EventStreamPage, AedbError> {
        let lease = self.acquire_snapshot(consistency).await?;
        let snapshot_seq = lease.view.seq;
        if limit == 0 {
            return Ok(EventStreamPage {
                events: Vec::new(),
                next_commit_seq: None,
                snapshot_seq,
            });
        }
        if let Some(caller) = caller {
            let required = Permission::TableRead {
                project_id: crate::catalog::SYSTEM_PROJECT_ID.to_string(),
                scope_id: SYSTEM_SCOPE_ID.to_string(),
                table_name: EVENT_OUTBOX_TABLE.to_string(),
            };
            if !lease
                .view
                .catalog
                .has_permission(&caller.caller_id, &required)
            {
                return Err(AedbError::PermissionDenied(format!(
                    "caller={} missing table read permission for system event stream",
                    caller.caller_id
                )));
            }
        }
        let Some(table) = lease.view.keyspace.table(
            crate::catalog::SYSTEM_PROJECT_ID,
            SYSTEM_SCOPE_ID,
            EVENT_OUTBOX_TABLE,
        ) else {
            return Ok(EventStreamPage {
                events: Vec::new(),
                next_commit_seq: None,
                snapshot_seq,
            });
        };
        let mut events = Vec::new();
        let start_seq = from_commit_seq_exclusive.saturating_add(1);
        let Ok(start_seq_i64) = i64::try_from(start_seq) else {
            return Ok(EventStreamPage {
                events: Vec::new(),
                next_commit_seq: None,
                snapshot_seq,
            });
        };
        let start_key = EncodedKey::from_values(&[
            Value::Integer(start_seq_i64),
            Value::Text("".into()),
            Value::Text("".into()),
        ]);
        for row in table
            .rows
            .range((Bound::Included(start_key), Bound::Unbounded))
            .map(|(_, row)| row)
        {
            if events.len() >= limit {
                break;
            }
            let (
                Some(Value::Integer(commit_seq_i64)),
                Some(Value::Timestamp(ts_i64)),
                Some(Value::Text(project_id)),
                Some(Value::Text(scope_id)),
                Some(Value::Text(topic)),
                Some(Value::Text(event_key)),
                Some(Value::Json(payload)),
            ) = (
                row.values.first(),
                row.values.get(1),
                row.values.get(2),
                row.values.get(3),
                row.values.get(4),
                row.values.get(5),
                row.values.get(6),
            )
            else {
                continue;
            };
            let Ok(commit_seq) = u64::try_from(*commit_seq_i64) else {
                continue;
            };
            if let Some(filter_topic) = topic_filter
                && topic.as_str() != filter_topic
            {
                continue;
            }
            let Ok(ts_micros) = u64::try_from(*ts_i64) else {
                continue;
            };
            events.push(EventOutboxRecord {
                commit_seq,
                ts_micros,
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                topic: topic.to_string(),
                event_key: event_key.to_string(),
                payload_json: payload.to_string(),
            });
        }
        let next_commit_seq = events.last().map(|e| e.commit_seq);
        Ok(EventStreamPage {
            events,
            next_commit_seq,
            snapshot_seq,
        })
    }

    pub async fn ack_reactive_processor_checkpoint(
        &self,
        processor_name: &str,
        checkpoint_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        if processor_name.trim().is_empty() {
            return Err(AedbError::Validation(
                "processor_name cannot be empty".into(),
            ));
        }
        let envelope = self
            .build_reactive_processor_checkpoint_envelope(processor_name, checkpoint_seq)
            .await?;
        self.commit_envelope_prevalidated_internal("ack_reactive_processor_checkpoint", envelope)
            .await
    }

    pub async fn ack_reactive_processor_checkpoint_as(
        &self,
        caller: CallerContext,
        processor_name: &str,
        checkpoint_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        ensure_external_caller_allowed(&caller)?;
        if processor_name.trim().is_empty() {
            return Err(AedbError::Validation(
                "processor_name cannot be empty".into(),
            ));
        }
        let mut envelope = self
            .build_reactive_processor_checkpoint_envelope(processor_name, checkpoint_seq)
            .await?;
        envelope.caller = Some(caller);
        self.commit_envelope(envelope).await
    }

    pub async fn ack_reactive_processor_checkpoint_batched(
        &self,
        processor_name: &str,
        checkpoint_seq: u64,
        watermark_commits: u64,
    ) -> Result<Option<CommitResult>, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        if processor_name.trim().is_empty() {
            return Err(AedbError::Validation(
                "processor_name cannot be empty".into(),
            ));
        }
        if watermark_commits == 0 {
            return Err(AedbError::Validation(
                "watermark_commits must be > 0".into(),
            ));
        }
        let cache_key = ReactiveCheckpointAckCacheKey {
            processor_name: processor_name.to_string(),
            caller_id: None,
        };
        let should_persist = {
            let cache = self.reactive_processor_ack_watermarks.lock();
            let last_persisted = cache
                .get(&cache_key)
                .map(|s| s.last_persisted_seq)
                .unwrap_or(0);
            checkpoint_seq > last_persisted
                && (last_persisted == 0
                    || checkpoint_seq.saturating_sub(last_persisted) >= watermark_commits)
        };
        if !should_persist {
            return Ok(None);
        }
        let envelope = self
            .build_reactive_processor_checkpoint_envelope(processor_name, checkpoint_seq)
            .await?;
        let committed = self
            .commit_envelope_prevalidated_internal(
                "ack_reactive_processor_checkpoint_batched",
                envelope,
            )
            .await?;
        {
            let mut cache = self.reactive_processor_ack_watermarks.lock();
            let state = cache.entry(cache_key).or_default();
            state.last_persisted_seq = state.last_persisted_seq.max(checkpoint_seq);
            state.last_touch_micros = system_now_micros();
            prune_reactive_ack_cache(&mut cache);
        }
        Ok(Some(committed))
    }

    pub async fn ack_reactive_processor_checkpoint_batched_as(
        &self,
        caller: CallerContext,
        processor_name: &str,
        checkpoint_seq: u64,
        watermark_commits: u64,
    ) -> Result<Option<CommitResult>, AedbError> {
        ensure_external_caller_allowed(&caller)?;
        if processor_name.trim().is_empty() {
            return Err(AedbError::Validation(
                "processor_name cannot be empty".into(),
            ));
        }
        if watermark_commits == 0 {
            return Err(AedbError::Validation(
                "watermark_commits must be > 0".into(),
            ));
        }
        let cache_key = ReactiveCheckpointAckCacheKey {
            processor_name: processor_name.to_string(),
            caller_id: Some(caller.caller_id.clone()),
        };
        let should_persist = {
            let cache = self.reactive_processor_ack_watermarks.lock();
            let last_persisted = cache
                .get(&cache_key)
                .map(|s| s.last_persisted_seq)
                .unwrap_or(0);
            checkpoint_seq > last_persisted
                && (last_persisted == 0
                    || checkpoint_seq.saturating_sub(last_persisted) >= watermark_commits)
        };
        if !should_persist {
            return Ok(None);
        }
        let mut envelope = self
            .build_reactive_processor_checkpoint_envelope(processor_name, checkpoint_seq)
            .await?;
        envelope.caller = Some(caller);
        let committed = self
            .commit_envelope(envelope)
            .await?;
        {
            let mut cache = self.reactive_processor_ack_watermarks.lock();
            let state = cache.entry(cache_key).or_default();
            state.last_persisted_seq = state.last_persisted_seq.max(checkpoint_seq);
            state.last_touch_micros = system_now_micros();
            prune_reactive_ack_cache(&mut cache);
        }
        Ok(Some(committed))
    }

    async fn build_reactive_processor_checkpoint_envelope(
        &self,
        processor_name: &str,
        checkpoint_seq: u64,
    ) -> Result<TransactionEnvelope, AedbError> {
        let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
        let checkpoint_key = Value::Text(processor_name.to_string().into());
        let checkpoint_pk = EncodedKey::from_values(std::slice::from_ref(&checkpoint_key));
        let checkpoint_table = lease.view.keyspace.table(
            crate::catalog::SYSTEM_PROJECT_ID,
            SYSTEM_SCOPE_ID,
            REACTIVE_PROCESSOR_CHECKPOINTS_TABLE,
        );
        let current_checkpoint = checkpoint_table
            .and_then(|table| table.rows.get(&checkpoint_pk))
            .and_then(|row| row.values.get(1))
            .and_then(|v| match v {
                Value::Integer(i) => u64::try_from(*i).ok(),
                _ => None,
            })
            .unwrap_or(0);
        if checkpoint_seq < current_checkpoint {
            return Err(AedbError::Validation(format!(
                "checkpoint_seq {checkpoint_seq} regresses current checkpoint {current_checkpoint}"
            )));
        }
        let checkpoint_version = checkpoint_table
            .and_then(|table| table.row_versions.get(&checkpoint_pk))
            .copied()
            .unwrap_or(0);
        let primary_key = vec![checkpoint_key.clone()];
        let read_set = ReadSet {
            points: vec![ReadSetEntry {
                key: ReadKey::TableRow {
                    project_id: crate::catalog::SYSTEM_PROJECT_ID.to_string(),
                    scope_id: SYSTEM_SCOPE_ID.to_string(),
                    table_name: REACTIVE_PROCESSOR_CHECKPOINTS_TABLE.to_string(),
                    primary_key,
                },
                version_at_read: checkpoint_version,
            }],
            ranges: Vec::new(),
        };

        Ok(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set,
            write_intent: WriteIntent {
                mutations: vec![reactive_processor_checkpoint_mutation(
                    processor_name,
                    checkpoint_seq,
                )],
            },
            base_seq: lease.view.seq,
        })
    }

    pub async fn reactive_processor_lag(
        &self,
        processor_name: &str,
        consistency: ConsistencyMode,
    ) -> Result<ReactiveProcessorLag, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        if processor_name.trim().is_empty() {
            return Err(AedbError::Validation(
                "processor_name cannot be empty".into(),
            ));
        }
        self.reactive_processor_lag_internal(processor_name, consistency, None)
            .await
    }

    async fn reactive_processor_lag_internal(
        &self,
        processor_name: &str,
        consistency: ConsistencyMode,
        caller: Option<&CallerContext>,
    ) -> Result<ReactiveProcessorLag, AedbError> {
        let lease = self.acquire_snapshot(consistency).await?;
        if let Some(caller) = caller {
            let required = Permission::TableRead {
                project_id: crate::catalog::SYSTEM_PROJECT_ID.to_string(),
                scope_id: SYSTEM_SCOPE_ID.to_string(),
                table_name: REACTIVE_PROCESSOR_CHECKPOINTS_TABLE.to_string(),
            };
            if !lease
                .view
                .catalog
                .has_permission(&caller.caller_id, &required)
            {
                return Err(AedbError::PermissionDenied(format!(
                    "caller={} missing table read permission for reactive processor checkpoints",
                    caller.caller_id
                )));
            }
        }
        let mut checkpoint_seq = 0u64;
        if let Some(table) = lease.view.keyspace.table(
            crate::catalog::SYSTEM_PROJECT_ID,
            SYSTEM_SCOPE_ID,
            REACTIVE_PROCESSOR_CHECKPOINTS_TABLE,
        ) {
            let pk = EncodedKey::from_values(&[Value::Text(processor_name.to_string().into())]);
            if let Some(row) = table.rows.get(&pk)
                && let Some(Value::Integer(v)) = row.values.get(1)
            {
                checkpoint_seq = u64::try_from(*v).unwrap_or(0);
            }
        }
        let head_seq = lease.view.seq;
        Ok(ReactiveProcessorLag {
            processor_name: processor_name.to_string(),
            checkpoint_seq,
            head_seq,
            lag_commits: head_seq.saturating_sub(checkpoint_seq),
        })
    }

    pub async fn reactive_processor_lag_as(
        &self,
        caller: &CallerContext,
        processor_name: &str,
        consistency: ConsistencyMode,
    ) -> Result<ReactiveProcessorLag, AedbError> {
        ensure_external_caller_allowed(caller)?;
        if processor_name.trim().is_empty() {
            return Err(AedbError::Validation(
                "processor_name cannot be empty".into(),
            ));
        }
        self.reactive_processor_lag_internal(processor_name, consistency, Some(caller))
            .await
    }

    pub async fn reactive_processor_runtime_status(
        &self,
        processor_name: &str,
    ) -> Option<ReactiveProcessorRuntimeStatus> {
        let (running, stats) = {
            let runtimes = self.reactive_processor_runtimes.lock().await;
            let runtime = runtimes.get(processor_name)?;
            (true, Arc::clone(&runtime.stats))
        };
        let mut status = stats.lock().await.clone();
        status.running = running;
        Some(status)
    }

    pub async fn stop_reactive_processor(&self, processor_name: &str) -> Result<(), AedbError> {
        self.pause_reactive_processor(processor_name).await
    }

    pub async fn pause_reactive_processor(&self, processor_name: &str) -> Result<(), AedbError> {
        if processor_name.trim().is_empty() {
            return Err(AedbError::Validation(
                "processor_name cannot be empty".into(),
            ));
        }
        if let Some((options, _)) = self
            .load_reactive_processor_registration(processor_name)
            .await?
        {
            self.persist_reactive_processor_registration(processor_name, &options, false)
                .await?;
        }
        let runtime = {
            let mut runtimes = self.reactive_processor_runtimes.lock().await;
            runtimes.remove(processor_name)
        };
        if let Some(runtime) = runtime {
            runtime.stop.store(true, Ordering::Release);
            runtime.join.abort();
            let _ = runtime.join.await;
        }
        Ok(())
    }

    pub async fn resume_reactive_processor(
        self: &Arc<Self>,
        processor_name: &str,
    ) -> Result<(), AedbError> {
        if processor_name.trim().is_empty() {
            return Err(AedbError::Validation(
                "processor_name cannot be empty".into(),
            ));
        }
        {
            let runtimes = self.reactive_processor_runtimes.lock().await;
            if runtimes.contains_key(processor_name) {
                return Ok(());
            }
        }
        let registration = self
            .load_reactive_processor_registration(processor_name)
            .await?
            .ok_or_else(|| AedbError::NotFound {
                resource_type: ErrorResourceType::Table,
                resource_id: format!(
                    "{}.{}.{}:{}",
                    crate::catalog::SYSTEM_PROJECT_ID,
                    SYSTEM_SCOPE_ID,
                    REACTIVE_PROCESSOR_REGISTRY_TABLE,
                    processor_name
                ),
            })?;
        let handler = {
            let handlers = self.reactive_processor_handlers.lock();
            handlers.get(processor_name).cloned().ok_or_else(|| {
                AedbError::Validation(format!(
                    "reactive processor handler not registered: {processor_name}"
                ))
            })?
        };
        let (options, _) = registration;
        self.start_reactive_processor_with_handler(processor_name, options, handler, true)
            .await
    }

    pub async fn list_reactive_processors(
        &self,
        consistency: ConsistencyMode,
    ) -> Result<Vec<ReactiveProcessorInfo>, AedbError> {
        let mut registrations = self
            .load_reactive_processor_registrations(consistency)
            .await?;
        registrations.sort_by(|a, b| a.processor_name.cmp(&b.processor_name));
        let running_names: HashSet<String> = {
            let runtimes = self.reactive_processor_runtimes.lock().await;
            runtimes.keys().cloned().collect()
        };
        Ok(registrations
            .into_iter()
            .map(|r| ReactiveProcessorInfo {
                running: running_names.contains(&r.processor_name),
                processor_name: r.processor_name,
                options: r.options,
                enabled: r.enabled,
                updated_at_micros: r.updated_at_micros,
            })
            .collect())
    }

    pub async fn reactive_processor_health(
        &self,
        processor_name: &str,
        consistency: ConsistencyMode,
    ) -> Result<ReactiveProcessorHealth, AedbError> {
        if processor_name.trim().is_empty() {
            return Err(AedbError::Validation(
                "processor_name cannot be empty".into(),
            ));
        }
        let registration = self
            .load_reactive_processor_registration_record(processor_name, consistency)
            .await?;
        let runtime_status = self.reactive_processor_runtime_status(processor_name).await;
        let lag = self
            .reactive_processor_lag_internal(processor_name, consistency, None)
            .await?;
        let running = runtime_status.as_ref().map(|s| s.running).unwrap_or(false);
        let enabled = registration.as_ref().map(|r| r.enabled).unwrap_or(running);
        let status = runtime_status.unwrap_or_else(|| ReactiveProcessorRuntimeStatus {
            processor_name: processor_name.to_string(),
            running,
            ..ReactiveProcessorRuntimeStatus::default()
        });
        Ok(ReactiveProcessorHealth {
            processor_name: processor_name.to_string(),
            enabled,
            running,
            checkpoint_seq: lag.checkpoint_seq,
            head_seq: lag.head_seq,
            lag_commits: lag.lag_commits,
            runs_total: status.runs_total,
            processed_events_total: status.processed_events_total,
            failures_total: status.failures_total,
            retries_total: status.retries_total,
            dead_lettered_total: status.dead_lettered_total,
            last_processed_seq: status.last_processed_seq,
            last_error: status.last_error,
            last_run_started_micros: status.last_run_started_micros,
            last_run_completed_micros: status.last_run_completed_micros,
            last_success_micros: status.last_success_micros,
            last_failure_micros: status.last_failure_micros,
            last_retry_micros: status.last_retry_micros,
            last_sleep_ms: status.last_sleep_ms,
            last_batch_events: status.last_batch_events,
        })
    }

    pub async fn reactive_processor_slo_status(
        &self,
        processor_name: &str,
        consistency: ConsistencyMode,
    ) -> Result<ReactiveProcessorSloStatus, AedbError> {
        if processor_name.trim().is_empty() {
            return Err(AedbError::Validation(
                "processor_name cannot be empty".into(),
            ));
        }
        let registration = self
            .load_reactive_processor_registration_record(processor_name, consistency)
            .await?
            .ok_or_else(|| AedbError::NotFound {
                resource_type: ErrorResourceType::Table,
                resource_id: format!(
                    "{}.{}.{}:{}",
                    crate::catalog::SYSTEM_PROJECT_ID,
                    SYSTEM_SCOPE_ID,
                    REACTIVE_PROCESSOR_REGISTRY_TABLE,
                    processor_name
                ),
            })?;
        let health = self
            .reactive_processor_health(processor_name, consistency)
            .await?;
        Ok(Self::build_reactive_processor_slo_status(
            &registration.options,
            health,
        ))
    }

    pub async fn list_reactive_processor_slo_statuses(
        &self,
        consistency: ConsistencyMode,
    ) -> Result<Vec<ReactiveProcessorSloStatus>, AedbError> {
        let infos = self.list_reactive_processors(consistency).await?;
        let mut out = Vec::with_capacity(infos.len());
        for info in infos {
            let health = self
                .reactive_processor_health(&info.processor_name, consistency)
                .await?;
            out.push(Self::build_reactive_processor_slo_status(
                &info.options,
                health,
            ));
        }
        out.sort_by(|a, b| a.processor_name.cmp(&b.processor_name));
        Ok(out)
    }

    pub async fn enforce_reactive_processor_slos(
        &self,
        consistency: ConsistencyMode,
    ) -> Result<(), AedbError> {
        let statuses = self
            .list_reactive_processor_slo_statuses(consistency)
            .await?;
        let breaches: Vec<ReactiveProcessorSloStatus> =
            statuses.into_iter().filter(|s| s.breached).collect();
        if breaches.is_empty() {
            return Ok(());
        }
        let details = breaches
            .iter()
            .map(|s| {
                format!(
                    "{}: {}",
                    s.processor_name,
                    if s.reasons.is_empty() {
                        "unknown breach".to_string()
                    } else {
                        s.reasons.join("; ")
                    }
                )
            })
            .collect::<Vec<_>>()
            .join(" | ");
        Err(AedbError::Unavailable {
            message: format!("reactive processor SLO breach: {details}"),
        })
    }

    pub async fn start_reactive_processor<F, Fut>(
        self: &Arc<Self>,
        processor_name: &str,
        options: ReactiveProcessorOptions,
        handler: F,
    ) -> Result<(), AedbError>
    where
        F: Fn(Arc<AedbInstance>, Vec<EventOutboxRecord>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), AedbError>> + Send + 'static,
    {
        let handler: ReactiveProcessorHandler =
            Arc::new(move |db, events| Box::pin(handler(db, events)));
        self.start_reactive_processor_with_handler(processor_name, options, handler, true)
            .await
    }

    pub async fn register_reactive_processor_handler<F, Fut>(
        self: &Arc<Self>,
        processor_name: &str,
        handler: F,
    ) -> Result<bool, AedbError>
    where
        F: Fn(Arc<AedbInstance>, Vec<EventOutboxRecord>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), AedbError>> + Send + 'static,
    {
        if processor_name.trim().is_empty() {
            return Err(AedbError::Validation(
                "processor_name cannot be empty".into(),
            ));
        }
        let handler: ReactiveProcessorHandler =
            Arc::new(move |db, events| Box::pin(handler(db, events)));
        self.reactive_processor_handlers
            .lock()
            .insert(processor_name.to_string(), Arc::clone(&handler));
        let Some((options, enabled)) = self
            .load_reactive_processor_registration(processor_name)
            .await?
        else {
            return Ok(false);
        };
        if !enabled {
            return Ok(false);
        }
        {
            let runtimes = self.reactive_processor_runtimes.lock().await;
            if runtimes.contains_key(processor_name) {
                return Ok(false);
            }
        }
        self.start_reactive_processor_with_handler(processor_name, options, handler, false)
            .await?;
        Ok(true)
    }

    async fn start_reactive_processor_with_handler(
        self: &Arc<Self>,
        processor_name: &str,
        options: ReactiveProcessorOptions,
        handler: ReactiveProcessorHandler,
        persist_registry: bool,
    ) -> Result<(), AedbError> {
        if processor_name.trim().is_empty() {
            return Err(AedbError::Validation(
                "processor_name cannot be empty".into(),
            ));
        }
        if options.max_events_per_run == 0
            || options.max_bytes_per_run == 0
            || options.max_run_duration_ms == 0
            || options.run_interval_ms == 0
            || options.idle_backoff_ms == 0
            || options.checkpoint_watermark_commits == 0
            || (options.max_retries > 0 && options.retry_backoff_ms == 0)
        {
            return Err(AedbError::Validation(
                "reactive processor options must be > 0".into(),
            ));
        }
        let caller = if let Some(raw) = options.caller_id.as_ref() {
            let caller = CallerContext::new(raw.clone());
            ensure_external_caller_allowed(&caller)?;
            Some(caller)
        } else if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "reactive processor caller_id required in secure mode".into(),
            ));
        } else {
            None
        };
        if persist_registry {
            self.persist_reactive_processor_registration(processor_name, &options, true)
                .await?;
        }
        // Seed checkpoint state so caller-scoped ACKs never race a missing internal table.
        let _ = self
            .commit_prevalidated_system_internal(
                "init_reactive_processor_checkpoint",
                reactive_processor_checkpoint_mutation(processor_name, 0),
            )
            .await?;
        let stats = Arc::new(AsyncMutex::new(ReactiveProcessorRuntimeStatus {
            processor_name: processor_name.to_string(),
            running: true,
            ..ReactiveProcessorRuntimeStatus::default()
        }));
        {
            let runtimes = self.reactive_processor_runtimes.lock().await;
            if runtimes.contains_key(processor_name) {
                return Err(AedbError::Validation(format!(
                    "reactive processor already running: {processor_name}"
                )));
            }
        }
        let weak = Arc::downgrade(self);
        let stop = Arc::new(AtomicBool::new(false));
        let stop_loop = Arc::clone(&stop);
        let processor_name_owned = processor_name.to_string();
        let options_owned = options.clone();
        let caller_owned = caller.clone();
        let stats_loop = Arc::clone(&stats);
        let handler_loop = Arc::clone(&handler);
        let join = tokio::spawn(async move {
            let mut retry: Option<ReactiveProcessorRetryState> = None;
            loop {
                if stop_loop.load(Ordering::Acquire) {
                    break;
                }
                let loop_started_micros = system_now_micros();
                let Some(db) = weak.upgrade() else {
                    break;
                };
                let mut sleep_ms = options_owned.idle_backoff_ms;
                let mut add_processed = 0u64;
                let mut add_failures = 0u64;
                let mut add_retries = 0u64;
                let mut add_dead_lettered = 0u64;
                let mut last_processed_seq = 0u64;
                let mut last_error: Option<String> = None;

                if let Some(pending) = retry.as_mut() {
                    if Instant::now() < pending.next_retry_at {
                        let wait_ms = (pending.next_retry_at - Instant::now()).as_millis() as u64;
                        sleep_ms = wait_ms.max(1).min(options_owned.idle_backoff_ms.max(1));
                    } else {
                        match AedbInstance::process_reactive_processor_batch(
                            Arc::clone(&db),
                            &processor_name_owned,
                            caller_owned.as_ref(),
                            &options_owned,
                            &handler_loop,
                            pending.events.clone(),
                            pending.last_seq,
                        )
                        .await
                        {
                            Ok((processed, last_seq)) => {
                                add_processed = processed;
                                last_processed_seq = last_seq;
                                last_error = None;
                                retry = None;
                                sleep_ms = options_owned.run_interval_ms;
                            }
                            Err(err) => {
                                add_failures = 1;
                                last_error = Some(err.to_string());
                                if pending.attempts >= options_owned.max_retries {
                                    let exhausted = pending.clone();
                                    let _ = db
                                        .dead_letter_reactive_processor_batch(
                                            &processor_name_owned,
                                            caller_owned.as_ref(),
                                            &exhausted.events,
                                            &err.to_string(),
                                            exhausted.attempts.saturating_add(1),
                                        )
                                        .await;
                                    let _ = if let Some(caller) = caller_owned.as_ref() {
                                        db.ack_reactive_processor_checkpoint_batched_as(
                                            caller.clone(),
                                            &processor_name_owned,
                                            exhausted.last_seq,
                                            options_owned.checkpoint_watermark_commits,
                                        )
                                        .await
                                    } else {
                                        db.ack_reactive_processor_checkpoint_batched(
                                            &processor_name_owned,
                                            exhausted.last_seq,
                                            options_owned.checkpoint_watermark_commits,
                                        )
                                        .await
                                    };
                                    add_dead_lettered = exhausted.events.len() as u64;
                                    last_processed_seq = exhausted.last_seq;
                                    retry = None;
                                    sleep_ms = options_owned.run_interval_ms;
                                } else {
                                    pending.attempts = pending.attempts.saturating_add(1);
                                    let exp = pending.attempts.saturating_sub(1).min(8);
                                    let backoff =
                                        options_owned.retry_backoff_ms.saturating_mul(1u64 << exp);
                                    pending.last_error = err.to_string();
                                    pending.next_retry_at =
                                        Instant::now() + Duration::from_millis(backoff.max(1));
                                    add_retries = 1;
                                    sleep_ms = backoff.max(1);
                                }
                            }
                        }
                    }
                } else {
                    match AedbInstance::fetch_reactive_processor_batch(
                        Arc::clone(&db),
                        &processor_name_owned,
                        caller_owned.as_ref(),
                        &options_owned,
                    )
                    .await
                    {
                        Ok(events) if events.is_empty() => {
                            if options_owned.run_on_interval {
                                match AedbInstance::process_reactive_processor_batch(
                                    Arc::clone(&db),
                                    &processor_name_owned,
                                    caller_owned.as_ref(),
                                    &options_owned,
                                    &handler_loop,
                                    Vec::new(),
                                    0,
                                )
                                .await
                                {
                                    Ok((_, _)) => {
                                        last_error = None;
                                        sleep_ms = options_owned.run_interval_ms;
                                    }
                                    Err(err) => {
                                        add_failures = 1;
                                        last_error = Some(err.to_string());
                                        if options_owned.max_retries > 0 {
                                            add_retries = 1;
                                            let backoff = options_owned.retry_backoff_ms.max(1);
                                            retry = Some(ReactiveProcessorRetryState {
                                                events: Vec::new(),
                                                last_seq: 0,
                                                attempts: 1,
                                                next_retry_at: Instant::now()
                                                    + Duration::from_millis(backoff),
                                                last_error: err.to_string(),
                                            });
                                            sleep_ms = backoff;
                                        }
                                    }
                                }
                            }
                        }
                        Ok(events) => {
                            let last_seq = events.last().map(|e| e.commit_seq).unwrap_or(0);
                            match AedbInstance::process_reactive_processor_batch(
                                Arc::clone(&db),
                                &processor_name_owned,
                                caller_owned.as_ref(),
                                &options_owned,
                                &handler_loop,
                                events.clone(),
                                last_seq,
                            )
                            .await
                            {
                                Ok((processed, seq)) => {
                                    add_processed = processed;
                                    last_processed_seq = seq;
                                    sleep_ms = options_owned.run_interval_ms;
                                }
                                Err(err) => {
                                    add_failures = 1;
                                    last_error = Some(err.to_string());
                                    if options_owned.max_retries == 0 {
                                        let _ = db
                                            .dead_letter_reactive_processor_batch(
                                                &processor_name_owned,
                                                caller_owned.as_ref(),
                                                &events,
                                                &err.to_string(),
                                                1,
                                            )
                                            .await;
                                        let _ = if let Some(caller) = caller_owned.as_ref() {
                                            db.ack_reactive_processor_checkpoint_batched_as(
                                                caller.clone(),
                                                &processor_name_owned,
                                                last_seq,
                                                options_owned.checkpoint_watermark_commits,
                                            )
                                            .await
                                        } else {
                                            db.ack_reactive_processor_checkpoint_batched(
                                                &processor_name_owned,
                                                last_seq,
                                                options_owned.checkpoint_watermark_commits,
                                            )
                                            .await
                                        };
                                        add_dead_lettered = events.len() as u64;
                                        last_processed_seq = last_seq;
                                        sleep_ms = options_owned.run_interval_ms;
                                    } else {
                                        add_retries = 1;
                                        let backoff = options_owned.retry_backoff_ms.max(1);
                                        retry = Some(ReactiveProcessorRetryState {
                                            events,
                                            last_seq,
                                            attempts: 1,
                                            next_retry_at: Instant::now()
                                                + Duration::from_millis(backoff),
                                            last_error: err.to_string(),
                                        });
                                        sleep_ms = backoff;
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            add_failures = 1;
                            last_error = Some(err.to_string());
                        }
                    }
                }

                {
                    let mut s = stats_loop.lock().await;
                    s.last_run_started_micros = Some(loop_started_micros);
                    s.runs_total = s.runs_total.saturating_add(1);
                    s.processed_events_total =
                        s.processed_events_total.saturating_add(add_processed);
                    s.failures_total = s.failures_total.saturating_add(add_failures);
                    s.retries_total = s.retries_total.saturating_add(add_retries);
                    s.dead_lettered_total = s.dead_lettered_total.saturating_add(add_dead_lettered);
                    s.last_sleep_ms = sleep_ms;
                    s.last_batch_events = add_processed.saturating_add(add_dead_lettered);
                    if last_processed_seq > 0 {
                        s.last_processed_seq = s.last_processed_seq.max(last_processed_seq);
                    }
                    if let Some(err) = last_error {
                        s.last_error = Some(err);
                    } else if add_processed > 0 || add_dead_lettered > 0 {
                        s.last_error = None;
                    }
                    let now = system_now_micros();
                    s.last_run_completed_micros = Some(now);
                    if add_processed > 0 || add_dead_lettered > 0 {
                        s.last_success_micros = Some(now);
                    }
                    if add_failures > 0 {
                        s.last_failure_micros = Some(now);
                    }
                    if add_retries > 0 {
                        s.last_retry_micros = Some(now);
                    }
                }
                tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
            }
            let Some(db) = weak.upgrade() else {
                return;
            };
            if let Some(runtime) =
                db.reactive_processor_runtimes
                    .try_lock()
                    .ok()
                    .and_then(|runtimes| {
                        runtimes
                            .get(&processor_name_owned)
                            .map(|r| Arc::clone(&r.stats))
                    })
            {
                let mut s = runtime.lock().await;
                s.running = false;
            }
        });

        let mut runtimes = self.reactive_processor_runtimes.lock().await;
        if runtimes.contains_key(processor_name) {
            stop.store(true, Ordering::Release);
            join.abort();
            return Err(AedbError::Validation(format!(
                "reactive processor already running: {processor_name}"
            )));
        }
        self.reactive_processor_handlers
            .lock()
            .insert(processor_name.to_string(), handler);
        runtimes.insert(
            processor_name.to_string(),
            ReactiveProcessorRuntime { stop, join, stats },
        );
        Ok(())
    }

    async fn persist_reactive_processor_registration(
        &self,
        processor_name: &str,
        options: &ReactiveProcessorOptions,
        enabled: bool,
    ) -> Result<CommitResult, AedbError> {
        let mutation = reactive_processor_registry_mutation(processor_name, options, enabled)?;
        self.commit_prevalidated_system_internal(
            "persist_reactive_processor_registration",
            mutation,
        )
        .await
    }

    async fn load_reactive_processor_registration(
        &self,
        processor_name: &str,
    ) -> Result<Option<(ReactiveProcessorOptions, bool)>, AedbError> {
        self.load_reactive_processor_registration_record(processor_name, ConsistencyMode::AtLatest)
            .await
            .map(|opt| opt.map(|r| (r.options, r.enabled)))
    }

    async fn load_reactive_processor_registration_record(
        &self,
        processor_name: &str,
        consistency: ConsistencyMode,
    ) -> Result<Option<ReactiveProcessorRegistration>, AedbError> {
        let lease = self.acquire_snapshot(consistency).await?;
        let Some(table) = lease.view.keyspace.table(
            crate::catalog::SYSTEM_PROJECT_ID,
            SYSTEM_SCOPE_ID,
            REACTIVE_PROCESSOR_REGISTRY_TABLE,
        ) else {
            return Ok(None);
        };
        let pk = EncodedKey::from_values(&[Value::Text(processor_name.to_string().into())]);
        let Some(row) = table.rows.get(&pk) else {
            return Ok(None);
        };
        Self::decode_reactive_processor_registration_row(row)
    }

    async fn load_reactive_processor_registrations(
        &self,
        consistency: ConsistencyMode,
    ) -> Result<Vec<ReactiveProcessorRegistration>, AedbError> {
        let lease = self.acquire_snapshot(consistency).await?;
        let Some(table) = lease.view.keyspace.table(
            crate::catalog::SYSTEM_PROJECT_ID,
            SYSTEM_SCOPE_ID,
            REACTIVE_PROCESSOR_REGISTRY_TABLE,
        ) else {
            return Ok(Vec::new());
        };
        let mut out = Vec::with_capacity(table.rows.len());
        for row in table.rows.values() {
            out.push(
                Self::decode_reactive_processor_registration_row(row)?.ok_or_else(|| {
                    AedbError::Validation(
                        "reactive processor registry row missing processor_name".into(),
                    )
                })?,
            );
        }
        Ok(out)
    }

    fn decode_reactive_processor_registration_row(
        row: &Row,
    ) -> Result<Option<ReactiveProcessorRegistration>, AedbError> {
        let Some(Value::Text(processor_name)) = row.values.first() else {
            return Ok(None);
        };
        let Some(Value::Json(options_json)) = row.values.get(1) else {
            return Err(AedbError::Validation(
                "reactive processor registry options missing".into(),
            ));
        };
        let Some(Value::Boolean(enabled)) = row.values.get(2) else {
            return Err(AedbError::Validation(
                "reactive processor registry enabled flag missing".into(),
            ));
        };
        let updated_at_micros = match row.values.get(3) {
            Some(Value::Timestamp(v)) => u64::try_from(*v).unwrap_or(0),
            _ => 0,
        };
        let options: ReactiveProcessorOptions =
            serde_json::from_str(options_json).map_err(|e| AedbError::Validation(e.to_string()))?;
        Ok(Some(ReactiveProcessorRegistration {
            processor_name: processor_name.to_string(),
            options,
            enabled: *enabled,
            updated_at_micros,
        }))
    }

    fn build_reactive_processor_slo_status(
        options: &ReactiveProcessorOptions,
        health: ReactiveProcessorHealth,
    ) -> ReactiveProcessorSloStatus {
        let now = system_now_micros();
        let reference_ts = health
            .last_success_micros
            .or(health.last_run_completed_micros);
        let stall_ms = reference_ts.map(|ts| now.saturating_sub(ts) / 1_000);
        let mut reasons = Vec::new();
        let mut breached = false;
        if health.enabled {
            if let Some(max_lag) = options.max_allowed_lag_commits
                && health.lag_commits > max_lag
            {
                breached = true;
                reasons.push(format!(
                    "lag_commits={} exceeds max_allowed_lag_commits={}",
                    health.lag_commits, max_lag
                ));
            }
            if let Some(max_stall) = options.max_allowed_stall_ms {
                match stall_ms {
                    Some(stall) if stall > max_stall => {
                        breached = true;
                        reasons.push(format!(
                            "stall_ms={} exceeds max_allowed_stall_ms={}",
                            stall, max_stall
                        ));
                    }
                    None => {
                        breached = true;
                        reasons.push(
                            "stall_ms unavailable; processor has not produced a completed run"
                                .to_string(),
                        );
                    }
                    _ => {}
                }
            }
        }
        ReactiveProcessorSloStatus {
            processor_name: health.processor_name.clone(),
            breached,
            enabled: health.enabled,
            running: health.running,
            lag_commits: health.lag_commits,
            max_allowed_lag_commits: options.max_allowed_lag_commits,
            stall_ms,
            max_allowed_stall_ms: options.max_allowed_stall_ms,
            reasons,
        }
    }

    async fn fetch_reactive_processor_batch(
        db: Arc<Self>,
        processor_name: &str,
        caller: Option<&CallerContext>,
        options: &ReactiveProcessorOptions,
    ) -> Result<Vec<EventOutboxRecord>, AedbError> {
        let lag = if let Some(caller) = caller {
            db.reactive_processor_lag_as(caller, processor_name, ConsistencyMode::AtLatest)
                .await?
        } else {
            db.reactive_processor_lag(processor_name, ConsistencyMode::AtLatest)
                .await?
        };
        let mut from_seq = lag.checkpoint_seq;
        let deadline = Instant::now() + Duration::from_millis(options.max_run_duration_ms);
        let mut events = Vec::new();
        let mut bytes = 0usize;
        'read: while events.len() < options.max_events_per_run
            && bytes < options.max_bytes_per_run
            && Instant::now() < deadline
        {
            let limit = (options.max_events_per_run - events.len()).min(128).max(1);
            let page = if let Some(caller) = caller {
                db.read_event_stream_as(
                    caller,
                    options.topic_filter.as_deref(),
                    from_seq,
                    limit,
                    ConsistencyMode::AtLatest,
                )
                .await?
            } else {
                db.read_event_stream(
                    options.topic_filter.as_deref(),
                    from_seq,
                    limit,
                    ConsistencyMode::AtLatest,
                )
                .await?
            };
            if page.events.is_empty() {
                break;
            }
            for event in page.events {
                let approx_bytes =
                    event.payload_json.len() + event.topic.len() + event.event_key.len() + 64;
                if !events.is_empty()
                    && bytes.saturating_add(approx_bytes) > options.max_bytes_per_run
                {
                    break 'read;
                }
                bytes = bytes.saturating_add(approx_bytes);
                from_seq = event.commit_seq;
                events.push(event);
                if events.len() >= options.max_events_per_run
                    || bytes >= options.max_bytes_per_run
                    || Instant::now() >= deadline
                {
                    break 'read;
                }
            }
            if page.next_commit_seq.is_none() {
                break;
            }
        }
        Ok(events)
    }

    async fn process_reactive_processor_batch(
        db: Arc<Self>,
        processor_name: &str,
        caller: Option<&CallerContext>,
        options: &ReactiveProcessorOptions,
        handler: &ReactiveProcessorHandler,
        events: Vec<EventOutboxRecord>,
        last_seq: u64,
    ) -> Result<(u64, u64), AedbError> {
        if events.is_empty() {
            handler(Arc::clone(&db), events).await?;
            return Ok((0, last_seq));
        }
        let processed = events.len() as u64;
        handler(Arc::clone(&db), events).await?;
        let _ = if let Some(caller) = caller {
            db.ack_reactive_processor_checkpoint_batched_as(
                caller.clone(),
                processor_name,
                last_seq,
                options.checkpoint_watermark_commits,
            )
            .await?
        } else {
            db.ack_reactive_processor_checkpoint_batched(
                processor_name,
                last_seq,
                options.checkpoint_watermark_commits,
            )
            .await?
        };
        Ok((processed, last_seq))
    }

    async fn dead_letter_reactive_processor_batch(
        &self,
        processor_name: &str,
        caller: Option<&CallerContext>,
        events: &[EventOutboxRecord],
        error: &str,
        attempts: u32,
    ) -> Result<CommitResult, AedbError> {
        let mutation = reactive_processor_dlq_mutation(processor_name, events, error, attempts);
        if let Some(caller) = caller {
            self.commit_as(caller.clone(), mutation).await
        } else {
            self.commit_prevalidated_system_internal("reactive_processor_dead_letter", mutation)
                .await
        }
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
        self.commit(Mutation::KvMutateU256 {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            op: KvU256MutatorOp::Add,
            operand_be: amount_be,
            expected_seq: Some(expected_seq),
        })
        .await
    }

    pub async fn kv_compare_and_add_u64(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 8],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvMutateU64 {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            op: KvU64MutatorOp::Add,
            operand_be: amount_be,
            expected_seq: Some(expected_seq),
        })
        .await
    }

    pub async fn kv_compare_and_add_u64_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 8],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::KvMutateU64 {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key,
                op: KvU64MutatorOp::Add,
                operand_be: amount_be,
                expected_seq: Some(expected_seq),
            },
        )
        .await
    }

    pub async fn kv_compare_and_set_u64(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        value_be: [u8; 8],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvMutateU64 {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            op: KvU64MutatorOp::Set,
            operand_be: value_be,
            expected_seq: Some(expected_seq),
        })
        .await
    }

    pub async fn kv_compare_and_set_u64_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        value_be: [u8; 8],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::KvMutateU64 {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key,
                op: KvU64MutatorOp::Set,
                operand_be: value_be,
                expected_seq: Some(expected_seq),
            },
        )
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
        self.commit_as(
            caller,
            Mutation::KvMutateU256 {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key,
                op: KvU256MutatorOp::Add,
                operand_be: amount_be,
                expected_seq: Some(expected_seq),
            },
        )
        .await
    }

    pub async fn kv_compare_and_set_u256(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        value_be: [u8; 32],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvMutateU256 {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            op: KvU256MutatorOp::Set,
            operand_be: value_be,
            expected_seq: Some(expected_seq),
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
        self.commit(Mutation::KvMutateU256 {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            op: KvU256MutatorOp::Sub,
            operand_be: amount_be,
            expected_seq: Some(expected_seq),
        })
        .await
    }

    pub async fn kv_compare_and_sub_u64(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 8],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvMutateU64 {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            op: KvU64MutatorOp::Sub,
            operand_be: amount_be,
            expected_seq: Some(expected_seq),
        })
        .await
    }

    pub async fn kv_compare_and_sub_u64_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 8],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::KvMutateU64 {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key,
                op: KvU64MutatorOp::Sub,
                operand_be: amount_be,
                expected_seq: Some(expected_seq),
            },
        )
        .await
    }

    pub async fn kv_compare_and_set_u256_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        value_be: [u8; 32],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::KvMutateU256 {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key,
                op: KvU256MutatorOp::Set,
                operand_be: value_be,
                expected_seq: Some(expected_seq),
            },
        )
        .await
    }

    pub async fn kv_mutate_u256(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        op: KvU256MutatorOp,
        operand_be: [u8; 32],
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvMutateU256 {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            op,
            operand_be,
            expected_seq: None,
        })
        .await
    }

    pub async fn kv_mutate_u256_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        op: KvU256MutatorOp,
        operand_be: [u8; 32],
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::KvMutateU256 {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key,
                op,
                operand_be,
                expected_seq: None,
            },
        )
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
        self.commit_as(
            caller,
            Mutation::KvMutateU256 {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key,
                op: KvU256MutatorOp::Sub,
                operand_be: amount_be,
                expected_seq: Some(expected_seq),
            },
        )
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

    pub async fn order_book_new(
        &self,
        project_id: &str,
        scope_id: &str,
        request: OrderRequest,
    ) -> Result<CommitResult, AedbError> {
        self.preflight_order_book_new_if_high_reject_risk(None, project_id, scope_id, &request)
            .await?;
        let mutation = Mutation::OrderBookNew {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            request,
        };
        let (_, catalog, _) = self.executor.snapshot_state().await;
        validate_mutation_with_config(&catalog, &mutation, &self._config)?;
        self.commit_prevalidated_internal("order_book_new", mutation)
            .await
    }

    pub async fn order_book_new_with_finality(
        &self,
        project_id: &str,
        scope_id: &str,
        request: OrderRequest,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        let mut result = self.order_book_new(project_id, scope_id, request).await?;
        self.enforce_finality(&mut result, finality).await?;
        Ok(result)
    }

    pub async fn order_book_define_table(
        &self,
        project_id: &str,
        scope_id: &str,
        table_id: &str,
        mode: OrderBookTableMode,
    ) -> Result<CommitResult, AedbError> {
        self.commit_prevalidated_internal(
            "order_book_define_table",
            Mutation::OrderBookDefineTable {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_id: table_id.to_string(),
                mode,
            },
        )
        .await
    }

    pub async fn order_book_define_table_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        table_id: &str,
        mode: OrderBookTableMode,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::OrderBookDefineTable {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_id: table_id.to_string(),
                mode,
            },
        )
        .await
    }

    pub async fn order_book_drop_table(
        &self,
        project_id: &str,
        scope_id: &str,
        table_id: &str,
    ) -> Result<CommitResult, AedbError> {
        self.commit_prevalidated_internal(
            "order_book_drop_table",
            Mutation::OrderBookDropTable {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_id: table_id.to_string(),
            },
        )
        .await
    }

    pub async fn order_book_drop_table_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        table_id: &str,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::OrderBookDropTable {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_id: table_id.to_string(),
            },
        )
        .await
    }

    pub async fn order_book_set_instrument_config(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        config: InstrumentConfig,
    ) -> Result<CommitResult, AedbError> {
        self.commit_prevalidated_internal(
            "order_book_set_instrument_config",
            Mutation::OrderBookSetInstrumentConfig {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                config,
            },
        )
        .await
    }

    pub async fn order_book_set_instrument_config_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        config: InstrumentConfig,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::OrderBookSetInstrumentConfig {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                config,
            },
        )
        .await
    }

    pub async fn order_book_set_instrument_halted(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        halted: bool,
    ) -> Result<CommitResult, AedbError> {
        self.commit_prevalidated_internal(
            "order_book_set_instrument_halted",
            Mutation::OrderBookSetInstrumentHalted {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                halted,
            },
        )
        .await
    }

    pub async fn order_book_set_instrument_halted_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        halted: bool,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::OrderBookSetInstrumentHalted {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                halted,
            },
        )
        .await
    }

    pub async fn order_book_new_in_table(
        &self,
        project_id: &str,
        scope_id: &str,
        table_id: &str,
        asset_id: &str,
        mut request: OrderRequest,
    ) -> Result<CommitResult, AedbError> {
        request.instrument = scoped_instrument(table_id, asset_id);
        self.order_book_new(project_id, scope_id, request).await
    }

    pub async fn order_book_new_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        request: OrderRequest,
    ) -> Result<CommitResult, AedbError> {
        self.preflight_order_book_new_if_high_reject_risk(
            Some(&caller),
            project_id,
            scope_id,
            &request,
        )
        .await?;
        self.commit_as(
            caller,
            Mutation::OrderBookNew {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                request,
            },
        )
        .await
    }

    pub async fn order_book_new_as_with_finality(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        request: OrderRequest,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        let mut result = self
            .order_book_new_as(caller, project_id, scope_id, request)
            .await?;
        self.enforce_finality(&mut result, finality).await?;
        Ok(result)
    }

    fn should_preflight_order_book_new(request: &OrderRequest) -> bool {
        request.exec_instructions.post_only()
            || matches!(request.time_in_force, TimeInForce::Fok)
            || matches!(request.order_type, OrderType::Market)
    }

    async fn preflight_order_book_new_if_high_reject_risk(
        &self,
        caller: Option<&CallerContext>,
        project_id: &str,
        scope_id: &str,
        request: &OrderRequest,
    ) -> Result<(), AedbError> {
        if !Self::should_preflight_order_book_new(request) {
            return Ok(());
        }
        let mutation = Mutation::OrderBookNew {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            request: request.clone(),
        };
        let preflight_result = if let Some(caller) = caller {
            self.preflight_as(caller, mutation).await?
        } else {
            self.preflight(mutation).await
        };
        if let PreflightResult::Err { reason } = preflight_result {
            self.upstream_validation_rejections
                .fetch_add(1, Ordering::Relaxed);
            return Err(AedbError::Validation(reason));
        }
        Ok(())
    }

    pub async fn order_book_cancel(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
    ) -> Result<CommitResult, AedbError> {
        self.commit_prevalidated_internal(
            "order_book_cancel",
            Mutation::OrderBookCancel {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                order_id,
                client_order_id: None,
                owner: owner.to_string(),
            },
        )
        .await
    }

    pub async fn order_book_cancel_with_finality(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        self.commit_prevalidated_internal_with_finality(
            "order_book_cancel",
            Mutation::OrderBookCancel {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                order_id,
                client_order_id: None,
                owner: owner.to_string(),
            },
            finality,
        )
        .await
    }

    pub async fn order_book_cancel_strict(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        self.order_book_cancel_strict_as_internal(
            None, project_id, scope_id, instrument, order_id, owner, finality,
        )
        .await
    }

    pub async fn order_book_cancel_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::OrderBookCancel {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                order_id,
                client_order_id: None,
                owner: owner.to_string(),
            },
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn order_book_cancel_as_with_finality(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        let mut result = self
            .order_book_cancel_as(caller, project_id, scope_id, instrument, order_id, owner)
            .await?;
        self.enforce_finality(&mut result, finality).await?;
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn order_book_cancel_strict_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        self.order_book_cancel_strict_as_internal(
            Some(caller),
            project_id,
            scope_id,
            instrument,
            order_id,
            owner,
            finality,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn order_book_cancel_strict_as_internal(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
        let order_key = key_order(instrument, order_id);
        let Some(entry) = lease.view.keyspace.kv_get(project_id, scope_id, &order_key) else {
            return Err(AedbError::Validation(format!(
                "strict cancel target not found: order_id={order_id}"
            )));
        };
        let order: OrderRecord =
            rmp_serde::from_slice(&entry.value).map_err(|e| AedbError::Decode(e.to_string()))?;
        if order.owner != owner {
            return Err(AedbError::PermissionDenied(
                "order ownership mismatch".into(),
            ));
        }
        if !matches!(
            order.status,
            crate::order_book::OrderStatus::Open | crate::order_book::OrderStatus::PartiallyFilled
        ) || u256_from_be(order.remaining_qty_be).is_zero()
        {
            return Err(AedbError::Validation(format!(
                "order not cancellable in current status: {:?}",
                order.status
            )));
        }
        let envelope = TransactionEnvelope {
            caller,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: ReadSet {
                points: vec![ReadSetEntry {
                    key: ReadKey::KvKey {
                        project_id: project_id.to_string(),
                        scope_id: scope_id.to_string(),
                        key: order_key,
                    },
                    version_at_read: entry.version,
                }],
                ranges: Vec::new(),
            },
            write_intent: WriteIntent {
                mutations: vec![Mutation::OrderBookCancel {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    instrument: instrument.to_string(),
                    order_id,
                    client_order_id: None,
                    owner: owner.to_string(),
                }],
            },
            base_seq: lease.view.seq,
        };
        self.commit_envelope_with_finality(envelope, finality).await
    }

    pub async fn order_book_cancel_by_client_id(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        client_order_id: &str,
        owner: &str,
    ) -> Result<CommitResult, AedbError> {
        self.commit_prevalidated_internal(
            "order_book_cancel_by_client_id",
            Mutation::OrderBookCancel {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                order_id: 0,
                client_order_id: Some(client_order_id.to_string()),
                owner: owner.to_string(),
            },
        )
        .await
    }

    pub async fn order_book_cancel_by_client_id_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        client_order_id: &str,
        owner: &str,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::OrderBookCancel {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                order_id: 0,
                client_order_id: Some(client_order_id.to_string()),
                owner: owner.to_string(),
            },
        )
        .await
    }

    pub async fn order_book_cancel_by_client_id_strict(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        client_order_id: &str,
        owner: &str,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        self.order_book_cancel_by_client_id_strict_as_internal(
            None,
            project_id,
            scope_id,
            instrument,
            client_order_id,
            owner,
            finality,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn order_book_cancel_by_client_id_strict_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        client_order_id: &str,
        owner: &str,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        self.order_book_cancel_by_client_id_strict_as_internal(
            Some(caller),
            project_id,
            scope_id,
            instrument,
            client_order_id,
            owner,
            finality,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn order_book_cancel_by_client_id_strict_as_internal(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        client_order_id: &str,
        owner: &str,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
        let cid_key = key_client_id(instrument, owner, client_order_id);
        let Some(cid_entry) = lease.view.keyspace.kv_get(project_id, scope_id, &cid_key) else {
            return Err(AedbError::Validation(format!(
                "strict cancel target not found: client_order_id={client_order_id}"
            )));
        };
        if cid_entry.value.len() != 8 {
            return Err(AedbError::Validation(
                "invalid client-order mapping encoding".into(),
            ));
        }
        let mut id_bytes = [0u8; 8];
        id_bytes.copy_from_slice(&cid_entry.value);
        let order_id = u64::from_be_bytes(id_bytes);
        let order_key = key_order(instrument, order_id);
        let Some(order_entry) = lease.view.keyspace.kv_get(project_id, scope_id, &order_key) else {
            return Err(AedbError::Validation(format!(
                "strict cancel target not found: order_id={order_id}"
            )));
        };
        let order: OrderRecord = rmp_serde::from_slice(&order_entry.value)
            .map_err(|e| AedbError::Decode(e.to_string()))?;
        if order.owner != owner {
            return Err(AedbError::PermissionDenied(
                "order ownership mismatch".into(),
            ));
        }
        if !matches!(
            order.status,
            crate::order_book::OrderStatus::Open | crate::order_book::OrderStatus::PartiallyFilled
        ) || u256_from_be(order.remaining_qty_be).is_zero()
        {
            return Err(AedbError::Validation(format!(
                "order not cancellable in current status: {:?}",
                order.status
            )));
        }
        let envelope = TransactionEnvelope {
            caller,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: ReadSet {
                points: vec![
                    ReadSetEntry {
                        key: ReadKey::KvKey {
                            project_id: project_id.to_string(),
                            scope_id: scope_id.to_string(),
                            key: cid_key,
                        },
                        version_at_read: cid_entry.version,
                    },
                    ReadSetEntry {
                        key: ReadKey::KvKey {
                            project_id: project_id.to_string(),
                            scope_id: scope_id.to_string(),
                            key: order_key,
                        },
                        version_at_read: order_entry.version,
                    },
                ],
                ranges: Vec::new(),
            },
            write_intent: WriteIntent {
                mutations: vec![Mutation::OrderBookCancel {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    instrument: instrument.to_string(),
                    order_id: 0,
                    client_order_id: Some(client_order_id.to_string()),
                    owner: owner.to_string(),
                }],
            },
            base_seq: lease.view.seq,
        };
        self.commit_envelope_with_finality(envelope, finality).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn order_book_cancel_replace_strict(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        new_price_ticks: Option<i64>,
        new_qty_be: Option<[u8; 32]>,
        new_time_in_force: Option<TimeInForce>,
        new_exec_instructions: Option<ExecInstruction>,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        self.order_book_cancel_replace_strict_as_internal(
            None,
            project_id,
            scope_id,
            instrument,
            order_id,
            owner,
            new_price_ticks,
            new_qty_be,
            new_time_in_force,
            new_exec_instructions,
            finality,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn order_book_cancel_replace_strict_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        new_price_ticks: Option<i64>,
        new_qty_be: Option<[u8; 32]>,
        new_time_in_force: Option<TimeInForce>,
        new_exec_instructions: Option<ExecInstruction>,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        self.order_book_cancel_replace_strict_as_internal(
            Some(caller),
            project_id,
            scope_id,
            instrument,
            order_id,
            owner,
            new_price_ticks,
            new_qty_be,
            new_time_in_force,
            new_exec_instructions,
            finality,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn order_book_cancel_replace_strict_as_internal(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        new_price_ticks: Option<i64>,
        new_qty_be: Option<[u8; 32]>,
        new_time_in_force: Option<TimeInForce>,
        new_exec_instructions: Option<ExecInstruction>,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        let (order_key, version, base_seq) = self
            .order_book_strict_cancellable_version(
                project_id, scope_id, instrument, order_id, owner,
            )
            .await?;
        let envelope = TransactionEnvelope {
            caller,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: ReadSet {
                points: vec![ReadSetEntry {
                    key: ReadKey::KvKey {
                        project_id: project_id.to_string(),
                        scope_id: scope_id.to_string(),
                        key: order_key,
                    },
                    version_at_read: version,
                }],
                ranges: Vec::new(),
            },
            write_intent: WriteIntent {
                mutations: vec![Mutation::OrderBookCancelReplace {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    instrument: instrument.to_string(),
                    order_id,
                    owner: owner.to_string(),
                    new_price_ticks,
                    new_qty_be,
                    new_time_in_force,
                    new_exec_instructions,
                }],
            },
            base_seq,
        };
        self.commit_envelope_with_finality(envelope, finality).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn order_book_reduce_strict(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        reduce_by_be: [u8; 32],
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        self.order_book_reduce_strict_as_internal(
            None,
            project_id,
            scope_id,
            instrument,
            order_id,
            owner,
            reduce_by_be,
            finality,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn order_book_reduce_strict_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        reduce_by_be: [u8; 32],
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        self.order_book_reduce_strict_as_internal(
            Some(caller),
            project_id,
            scope_id,
            instrument,
            order_id,
            owner,
            reduce_by_be,
            finality,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn order_book_reduce_strict_as_internal(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        reduce_by_be: [u8; 32],
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        let reduce_by = u256_from_be(reduce_by_be);
        if reduce_by.is_zero() {
            return Err(AedbError::Validation(
                "strict reduce requires reduce_by > 0".into(),
            ));
        }
        let (order_key, version, base_seq) = self
            .order_book_strict_cancellable_version(
                project_id, scope_id, instrument, order_id, owner,
            )
            .await?;
        let envelope = TransactionEnvelope {
            caller,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: ReadSet {
                points: vec![ReadSetEntry {
                    key: ReadKey::KvKey {
                        project_id: project_id.to_string(),
                        scope_id: scope_id.to_string(),
                        key: order_key,
                    },
                    version_at_read: version,
                }],
                ranges: Vec::new(),
            },
            write_intent: WriteIntent {
                mutations: vec![Mutation::OrderBookReduce {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    instrument: instrument.to_string(),
                    order_id,
                    owner: owner.to_string(),
                    reduce_by_be,
                }],
            },
            base_seq,
        };
        self.commit_envelope_with_finality(envelope, finality).await
    }

    async fn order_book_strict_cancellable_version(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
    ) -> Result<(Vec<u8>, u64, u64), AedbError> {
        let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
        let order_key = key_order(instrument, order_id);
        let Some(entry) = lease.view.keyspace.kv_get(project_id, scope_id, &order_key) else {
            return Err(AedbError::Validation(format!(
                "strict target not found: order_id={order_id}"
            )));
        };
        let order: OrderRecord =
            rmp_serde::from_slice(&entry.value).map_err(|e| AedbError::Decode(e.to_string()))?;
        if order.owner != owner {
            return Err(AedbError::PermissionDenied(
                "order ownership mismatch".into(),
            ));
        }
        if !matches!(
            order.status,
            crate::order_book::OrderStatus::Open | crate::order_book::OrderStatus::PartiallyFilled
        ) || u256_from_be(order.remaining_qty_be).is_zero()
        {
            return Err(AedbError::Validation(format!(
                "order not mutable in current status: {:?}",
                order.status
            )));
        }
        Ok((order_key, entry.version, lease.view.seq))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn order_book_cancel_replace(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        new_price_ticks: Option<i64>,
        new_qty_be: Option<[u8; 32]>,
        new_time_in_force: Option<TimeInForce>,
        new_exec_instructions: Option<ExecInstruction>,
    ) -> Result<CommitResult, AedbError> {
        self.commit_prevalidated_internal(
            "order_book_cancel_replace",
            Mutation::OrderBookCancelReplace {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                order_id,
                owner: owner.to_string(),
                new_price_ticks,
                new_qty_be,
                new_time_in_force,
                new_exec_instructions,
            },
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn order_book_cancel_replace_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        new_price_ticks: Option<i64>,
        new_qty_be: Option<[u8; 32]>,
        new_time_in_force: Option<TimeInForce>,
        new_exec_instructions: Option<ExecInstruction>,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::OrderBookCancelReplace {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                order_id,
                owner: owner.to_string(),
                new_price_ticks,
                new_qty_be,
                new_time_in_force,
                new_exec_instructions,
            },
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn order_book_mass_cancel(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        owner: &str,
        side: Option<OrderSide>,
        owner_filter: Option<String>,
        price_range_ticks: Option<(i64, i64)>,
    ) -> Result<CommitResult, AedbError> {
        self.commit_prevalidated_internal(
            "order_book_mass_cancel",
            Mutation::OrderBookMassCancel {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                owner: owner.to_string(),
                side,
                owner_filter,
                price_range_ticks,
            },
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn order_book_mass_cancel_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        owner: &str,
        side: Option<OrderSide>,
        owner_filter: Option<String>,
        price_range_ticks: Option<(i64, i64)>,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::OrderBookMassCancel {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                owner: owner.to_string(),
                side,
                owner_filter,
                price_range_ticks,
            },
        )
        .await
    }

    pub async fn order_book_reduce(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        reduce_by_be: [u8; 32],
    ) -> Result<CommitResult, AedbError> {
        self.commit_prevalidated_internal(
            "order_book_reduce",
            Mutation::OrderBookReduce {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                order_id,
                owner: owner.to_string(),
                reduce_by_be,
            },
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn order_book_reduce_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        reduce_by_be: [u8; 32],
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::OrderBookReduce {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                order_id,
                owner: owner.to_string(),
                reduce_by_be,
            },
        )
        .await
    }

    pub async fn order_book_match_internal(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        fills: Vec<FillSpec>,
    ) -> Result<CommitResult, AedbError> {
        self.commit_prevalidated_internal(
            "order_book_match_internal",
            Mutation::OrderBookMatch {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                fills,
            },
        )
        .await
    }

    pub async fn order_book_match_internal_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        fills: Vec<FillSpec>,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::OrderBookMatch {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                fills,
            },
        )
        .await
    }

    pub async fn order_book_top_n(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        depth: u32,
        consistency: ConsistencyMode,
        caller: &CallerContext,
    ) -> Result<OrderBookDepth, QueryError> {
        ensure_query_caller_allowed(caller)?;
        let lease = self
            .acquire_snapshot(consistency)
            .await
            .map_err(QueryError::from)?;
        let prefix = format!("ob:{instrument}:");
        if !lease.view.catalog.has_kv_read_permission(
            &caller.caller_id,
            project_id,
            scope_id,
            prefix.as_bytes(),
        ) {
            return Err(QueryError::PermissionDenied {
                permission: format!("KvRead({project_id}.{scope_id})"),
                scope: caller.caller_id.clone(),
            });
        }
        read_top_n(
            &lease.view.keyspace,
            project_id,
            scope_id,
            instrument,
            depth as usize,
            lease.view.seq,
        )
        .map_err(QueryError::from)
    }

    pub async fn order_status(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        consistency: ConsistencyMode,
        caller: &CallerContext,
    ) -> Result<Option<OrderRecord>, QueryError> {
        ensure_query_caller_allowed(caller)?;
        let lease = self
            .acquire_snapshot(consistency)
            .await
            .map_err(QueryError::from)?;
        let prefix = format!("ob:{instrument}:");
        if !lease.view.catalog.has_kv_read_permission(
            &caller.caller_id,
            project_id,
            scope_id,
            prefix.as_bytes(),
        ) {
            return Err(QueryError::PermissionDenied {
                permission: format!("KvRead({project_id}.{scope_id})"),
                scope: caller.caller_id.clone(),
            });
        }
        let order = read_order_status(
            &lease.view.keyspace,
            project_id,
            scope_id,
            instrument,
            order_id,
        )
        .map_err(QueryError::from)?;
        if let Some(order) = &order {
            let admin = lease
                .view
                .catalog
                .has_permission(&caller.caller_id, &Permission::GlobalAdmin);
            if !admin && order.owner != caller.caller_id {
                return Err(QueryError::PermissionDenied {
                    permission: "order_status(owner match)".into(),
                    scope: caller.caller_id.clone(),
                });
            }
        }
        Ok(order)
    }

    pub async fn open_orders(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        owner: &str,
        consistency: ConsistencyMode,
        caller: &CallerContext,
    ) -> Result<Vec<OrderRecord>, QueryError> {
        ensure_query_caller_allowed(caller)?;
        let lease = self
            .acquire_snapshot(consistency)
            .await
            .map_err(QueryError::from)?;
        let prefix = format!("ob:{instrument}:");
        if !lease.view.catalog.has_kv_read_permission(
            &caller.caller_id,
            project_id,
            scope_id,
            prefix.as_bytes(),
        ) {
            return Err(QueryError::PermissionDenied {
                permission: format!("KvRead({project_id}.{scope_id})"),
                scope: caller.caller_id.clone(),
            });
        }
        let admin = lease
            .view
            .catalog
            .has_permission(&caller.caller_id, &Permission::GlobalAdmin);
        if !admin && owner != caller.caller_id {
            return Err(QueryError::PermissionDenied {
                permission: "open_orders(owner match)".into(),
                scope: caller.caller_id.clone(),
            });
        }
        read_open_orders(
            &lease.view.keyspace,
            project_id,
            scope_id,
            instrument,
            owner,
        )
        .map_err(QueryError::from)
    }

    pub async fn recent_trades(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        limit: u32,
        consistency: ConsistencyMode,
        caller: &CallerContext,
    ) -> Result<Vec<crate::order_book::FillRecord>, QueryError> {
        ensure_query_caller_allowed(caller)?;
        let lease = self
            .acquire_snapshot(consistency)
            .await
            .map_err(QueryError::from)?;
        let prefix = format!("ob:{instrument}:");
        if !lease.view.catalog.has_kv_read_permission(
            &caller.caller_id,
            project_id,
            scope_id,
            prefix.as_bytes(),
        ) {
            return Err(QueryError::PermissionDenied {
                permission: format!("KvRead({project_id}.{scope_id})"),
                scope: caller.caller_id.clone(),
            });
        }
        read_recent_trades(
            &lease.view.keyspace,
            project_id,
            scope_id,
            instrument,
            limit as usize,
        )
        .map_err(QueryError::from)
    }

    pub async fn spread(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        consistency: ConsistencyMode,
        caller: &CallerContext,
    ) -> Result<Spread, QueryError> {
        ensure_query_caller_allowed(caller)?;
        let lease = self
            .acquire_snapshot(consistency)
            .await
            .map_err(QueryError::from)?;
        let prefix = format!("ob:{instrument}:");
        if !lease.view.catalog.has_kv_read_permission(
            &caller.caller_id,
            project_id,
            scope_id,
            prefix.as_bytes(),
        ) {
            return Err(QueryError::PermissionDenied {
                permission: format!("KvRead({project_id}.{scope_id})"),
                scope: caller.caller_id.clone(),
            });
        }
        read_spread(
            &lease.view.keyspace,
            project_id,
            scope_id,
            instrument,
            lease.view.seq,
        )
        .map_err(QueryError::from)
    }

    pub async fn order_book_last_execution_report(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        consistency: ConsistencyMode,
        caller: &CallerContext,
    ) -> Result<Option<crate::order_book::ExecutionReport>, QueryError> {
        ensure_query_caller_allowed(caller)?;
        let lease = self
            .acquire_snapshot(consistency)
            .await
            .map_err(QueryError::from)?;
        let prefix = format!("ob:{instrument}:");
        if !lease.view.catalog.has_kv_read_permission(
            &caller.caller_id,
            project_id,
            scope_id,
            prefix.as_bytes(),
        ) {
            return Err(QueryError::PermissionDenied {
                permission: format!("KvRead({project_id}.{scope_id})"),
                scope: caller.caller_id.clone(),
            });
        }
        read_last_execution_report(&lease.view.keyspace, project_id, scope_id, instrument)
            .map_err(QueryError::from)
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
            let query_result = self
                .query_with_options_as(
                    caller.as_ref(),
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
                .map_err(query_error_to_aedb)?;

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
            ConsistencyMode::AtLatest => Ok(self.executor.snapshot_latest_view().await),
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
            manager: Some(Arc::clone(&self.snapshot_manager)),
            handle: Some(handle),
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
        self.lifecycle_hooks_present.store(true, Ordering::Release);
    }

    pub fn remove_lifecycle_hook(&self, hook: &Arc<dyn LifecycleHook>) {
        let mut hooks = self.lifecycle_hooks.lock();
        hooks.retain(|existing| !Arc::ptr_eq(existing, hook));
        self.lifecycle_hooks_present
            .store(!hooks.is_empty(), Ordering::Release);
    }

    pub fn add_telemetry_hook(&self, hook: Arc<dyn QueryCommitTelemetryHook>) {
        self.telemetry_hooks.lock().push(hook);
        self.telemetry_hooks_present.store(true, Ordering::Release);
    }

    pub fn remove_telemetry_hook(&self, hook: &Arc<dyn QueryCommitTelemetryHook>) {
        let mut hooks = self.telemetry_hooks.lock();
        hooks.retain(|existing| !Arc::ptr_eq(existing, hook));
        self.telemetry_hooks_present
            .store(!hooks.is_empty(), Ordering::Release);
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
        // Ensure batch durability tails are flushed before checkpointing shutdown state.
        let _ = self.force_fsync().await?;
        let _ = self.checkpoint_now().await?;
        Ok(())
    }

    pub async fn checkpoint_now(&self) -> Result<u64, AedbError> {
        // Serialize checkpoints while allowing normal commits/queries to continue.
        let _checkpoint_guard = self.checkpoint_lock.lock().await;

        // In batch durability mode, flush un-synced WAL tail so checkpoint captures a
        // stable durable horizon and recovery does not lose committed tail entries.
        let _ = self.force_fsync().await?;

        // Anchor checkpoint to a stable committed horizon.
        let seq = self.executor.durable_head_seq_now();
        let lease = self.acquire_snapshot(ConsistencyMode::AtSeq(seq)).await?;
        let snapshot = Arc::clone(&lease.view.keyspace);
        let catalog = Arc::clone(&lease.view.catalog);
        let mut idempotency = self.executor.idempotency_snapshot().await;
        idempotency.retain(|_, record| record.commit_seq <= seq);
        let dir = self.dir.clone();
        let checkpoint_key = self._config.checkpoint_key().copied();
        let checkpoint_key_id = self._config.checkpoint_key_id.clone();
        let compression_level = self._config.checkpoint_compression_level;
        let manifest_hmac_key = self._config.hmac_key().map(|key| key.to_vec());
        tokio::task::spawn_blocking(move || -> Result<(), AedbError> {
            let checkpoint = write_checkpoint_with_key(
                snapshot.as_ref(),
                catalog.as_ref(),
                seq,
                &dir,
                checkpoint_key.as_ref(),
                checkpoint_key_id,
                idempotency,
                compression_level,
            )?;

            let segments = read_segments_for_checkpoint(&dir, seq)?;
            let active_segment_seq = segments
                .last()
                .map(|segment| segment.segment_seq)
                .unwrap_or(seq.saturating_add(1));
            let manifest = Manifest {
                durable_seq: seq,
                visible_seq: seq,
                active_segment_seq,
                checkpoints: vec![checkpoint],
                segments,
            };
            write_manifest_atomic_signed(&manifest, &dir, manifest_hmac_key.as_deref())?;
            Ok(())
        })
        .await
        .map_err(|e| AedbError::Io(std::io::Error::other(e.to_string())))??;
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
            self._config.checkpoint_compression_level,
        )?;

        let mut wal_segments = Vec::new();
        let mut file_sha256 = HashMap::new();
        file_sha256.insert(
            checkpoint.filename.clone(),
            sha256_file_hex(&backup_dir.join(&checkpoint.filename))?,
        );

        for segment in read_segments_for_checkpoint(&self.dir, snapshot_seq)? {
            let src = self.dir.join(&segment.filename);
            let rel = format!("wal_tail/{}", segment.filename);
            let dst = backup_dir.join(&rel);
            copy_file_prefix(&src, &dst, segment.size_bytes)?;
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
            copy_file_prefix(&src, &dst, segment.size_bytes)?;
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
        if current_seq != effective_target {
            return Err(AedbError::Validation(format!(
                "backup chain replay incomplete: restored seq {current_seq}, expected {effective_target}"
            )));
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
            config.checkpoint_compression_level,
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

        let mut merged_namespaces = live
            .keyspace
            .namespaces
            .iter()
            .filter(|(ns, _)| **ns != ns_id)
            .map(|(ns, namespace)| (ns.clone(), namespace.clone()))
            .collect::<HashMap<_, _>>();
        if let Some(namespace) = restored.keyspace.namespaces.get(&ns_id) {
            merged_namespaces.insert(ns_id.clone(), namespace.clone());
        }
        let mut merged_async_indexes = live
            .keyspace
            .async_indexes
            .iter()
            .filter(|(key, _)| key.0 != ns_id)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<HashMap<_, _>>();
        for (key, value) in restored.keyspace.async_indexes.iter() {
            if key.0 == ns_id {
                merged_async_indexes.insert(key.clone(), value.clone());
            }
        }
        let merged_keyspace = Keyspace {
            primary_index_backend: live.keyspace.primary_index_backend,
            namespaces: Arc::new(merged_namespaces.into()),
            async_indexes: Arc::new(merged_async_indexes.into()),
        };

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
            config.checkpoint_compression_level,
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
        Ok(self.snapshot_for_consistency(consistency).await?.seq)
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
        let durable_wait_ops = self.durable_wait_ops.load(Ordering::Relaxed);
        let durable_wait_micros = self.durable_wait_micros.load(Ordering::Relaxed);
        let avg_durable_wait_micros = if durable_wait_ops == 0 {
            0
        } else {
            durable_wait_micros / durable_wait_ops
        };
        let upstream_validation_rejections =
            self.upstream_validation_rejections.load(Ordering::Relaxed);
        OperationalMetrics {
            commits_total: core.commits_total,
            commit_errors: core.commit_errors,
            permission_rejections: core.permission_rejections,
            validation_rejections: core
                .validation_rejections
                .saturating_add(upstream_validation_rejections),
            queue_full_rejections: core.queue_full_rejections,
            timeout_rejections: core.timeout_rejections,
            conflict_rejections: core.conflict_rejections,
            read_set_conflicts: core.read_set_conflicts,
            conflict_rate,
            avg_commit_latency_micros: core.avg_commit_latency_micros,
            coordinator_apply_attempts: core.coordinator_apply_attempts,
            avg_coordinator_apply_micros: core.avg_coordinator_apply_micros,
            wal_append_ops: core.wal_append_ops,
            wal_append_bytes: core.wal_append_bytes,
            avg_wal_append_micros: core.avg_wal_append_micros,
            wal_sync_ops: core.wal_sync_ops,
            avg_wal_sync_micros: core.avg_wal_sync_micros,
            prestage_validate_ops: core.prestage_validate_ops,
            avg_prestage_validate_micros: core.avg_prestage_validate_micros,
            epoch_process_ops: core.epoch_process_ops,
            avg_epoch_process_micros: core.avg_epoch_process_micros,
            group_commit_filling_epochs: core.group_commit_filling_epochs,
            group_commit_flushing_epochs: core.group_commit_flushing_epochs,
            group_commit_complete_epochs: core.group_commit_complete_epochs,
            group_commit_flush_reason_max_group_size: core.group_commit_flush_reason_max_group_size,
            group_commit_flush_reason_max_group_delay: core
                .group_commit_flush_reason_max_group_delay,
            group_commit_flush_reason_ingress_drained: core
                .group_commit_flush_reason_ingress_drained,
            group_commit_flush_reason_structural_barrier: core
                .group_commit_flush_reason_structural_barrier,
            durable_wait_ops,
            avg_durable_wait_micros,
            inflight_commits: core.inflight_commits,
            queue_depth: core.queued_commits,
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

    /// Returns an in-memory footprint estimate (bytes) for current keyspace state.
    ///
    /// This is an estimate based on row/KV/index payload sizes and is intended for
    /// policy and safety heuristics (for example, retention memory-pressure offload).
    pub async fn estimated_memory_bytes(&self) -> usize {
        let (snapshot, _, _) = self.executor.snapshot_state().await;
        snapshot.estimate_memory_bytes()
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
        let target_versions = migrations.iter().map(|m| m.version).collect::<HashSet<_>>();
        let existing_by_version = self
            .list_applied_migrations_for_versions(&project_id, &scope_id, &target_versions)
            .await?;
        let mut current_version = self.current_version(&project_id, &scope_id).await?;
        for migration in migrations {
            if let Some(record) = existing_by_version.get(&migration.version) {
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
            current_version = current_version.max(version);
        }
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
            let (snapshot, _, base_seq) = self.executor.snapshot_state().await;
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
            let predicted_applied_seq = base_seq.saturating_add(1);
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
            let prefix = b"__migrations/";
            let start = Bound::Included(prefix.to_vec());
            let end = next_prefix_bytes(prefix).map_or(Bound::Unbounded, Bound::Excluded);
            for (k, v) in namespace.kv.entries.range((start, end)) {
                if !k.starts_with(prefix) {
                    break;
                }
                out.push(decode_record(&v.value)?);
            }
        }
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
        let (snapshot, _, _) = self.executor.snapshot_state().await;
        let ns = NamespaceId::project_scope(project_id, scope_id);
        if let Some(namespace) = snapshot.namespaces.get(&ns) {
            let prefix = b"__migrations/";
            let start = Bound::Included(prefix.to_vec());
            let end = next_prefix_bytes(prefix).map_or(Bound::Unbounded, Bound::Excluded);
            if let Some((key, value)) = namespace.kv.entries.range((start, end)).rev().next() {
                if let Some(version) = parse_migration_version_from_key(key) {
                    return Ok(version);
                }
                return Ok(decode_record(&value.value)?.version);
            }
        }
        Ok(0)
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
                            let column_index = schema
                                .columns
                                .iter()
                                .position(|c| c.name == *pk_name)
                                .ok_or_else(|| {
                                    AedbError::Validation(format!(
                                        "primary key column missing: {pk_name}"
                                    ))
                                })?;
                            Ok(new_row.values[column_index].clone())
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
        for (chunk_index, chunk) in rows[start_offset..].chunks(batch_size).enumerate() {
            for row in chunk {
                if let Some(new_row) = update(row) {
                    let primary_key = schema
                        .primary_key
                        .iter()
                        .map(|pk_name| {
                            let column_index = schema
                                .columns
                                .iter()
                                .position(|c| c.name == *pk_name)
                                .ok_or_else(|| {
                                    AedbError::Validation(format!(
                                        "primary key column missing: {pk_name}"
                                    ))
                                })?;
                            Ok(new_row.values[column_index].clone())
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
            let progressed =
                start_offset + ((chunk_index + 1) * batch_size).min(rows.len() - start_offset);
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

fn parse_migration_version_from_key(key: &[u8]) -> Option<u64> {
    const PREFIX: &[u8] = b"__migrations/";
    let suffix = key.strip_prefix(PREFIX)?;
    if suffix.len() != 20 || !suffix.iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    std::str::from_utf8(suffix).ok()?.parse::<u64>().ok()
}

impl AedbInstance {
    async fn list_applied_migrations_for_versions(
        &self,
        project_id: &str,
        scope_id: &str,
        versions: &HashSet<u64>,
    ) -> Result<HashMap<u64, MigrationRecord>, AedbError> {
        if versions.is_empty() {
            return Ok(HashMap::new());
        }
        let (snapshot, _, _) = self.executor.snapshot_state().await;
        let ns = NamespaceId::project_scope(project_id, scope_id);
        let mut out = HashMap::with_capacity(versions.len());
        if let Some(namespace) = snapshot.namespaces.get(&ns) {
            let prefix = b"__migrations/";
            let start = Bound::Included(prefix.to_vec());
            let end = next_prefix_bytes(prefix).map_or(Bound::Unbounded, Bound::Excluded);
            for (k, v) in namespace.kv.entries.range((start, end)) {
                if !k.starts_with(prefix) {
                    break;
                }
                if let Some(version) = parse_migration_version_from_key(k) {
                    if !versions.contains(&version) {
                        continue;
                    }
                    let record = decode_record(&v.value)?;
                    out.insert(version, record);
                    continue;
                }
                let record = decode_record(&v.value)?;
                if versions.contains(&record.version) {
                    out.insert(record.version, record);
                }
            }
        }
        Ok(out)
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
        self.db.normalize_query_cursor_options(&mut options)?;
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
        let mut result = result?;
        self.db.sign_query_result_cursor(&mut result)?;
        let result = Ok(result);
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
        self.db.normalize_query_cursor_options(&mut options)?;
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
        let mut source = self.query(project_id, scope_id, source_query).await?;
        // This helper treats the source query's limit as the caller's complete key set.
        // Do not surface pagination state from the underlying query engine here.
        source.cursor = None;
        source.truncated = false;
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
                    truncated: false,
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
        let page_size = self.db._config.max_scan_rows.min(100).max(1);
        let mut hydrate_query = ensure_stable_order_from_catalog(
            project_id,
            scope_id,
            &self.lease.view.catalog,
            hydrate_query,
        );
        hydrate_query.limit = Some(page_size);

        let mut all_rows = Vec::new();
        let mut total_rows_examined = 0usize;
        let mut next_cursor: Option<String> = None;
        let mut materialized_seq = None;
        loop {
            let page = self
                .query_with_options(
                    project_id,
                    scope_id,
                    hydrate_query.clone(),
                    QueryOptions {
                        consistency: ConsistencyMode::AtSeq(self.lease.view.seq),
                        cursor: next_cursor.clone(),
                        ..QueryOptions::default()
                    },
                )
                .await?;
            total_rows_examined = total_rows_examined.saturating_add(page.rows_examined);
            if materialized_seq.is_none() {
                materialized_seq = page.materialized_seq;
            }
            all_rows.extend(page.rows);
            if let Some(cursor) = page.cursor {
                next_cursor = Some(cursor);
                continue;
            }
            break;
        }
        let hydrated = QueryResult {
            rows: all_rows,
            rows_examined: total_rows_examined,
            cursor: None,
            truncated: false,
            snapshot_seq: self.lease.view.seq,
            materialized_seq,
        };
        Ok((source, hydrated))
    }
}
