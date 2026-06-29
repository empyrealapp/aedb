pub mod backup;
mod backup_chain;
pub mod catalog;
pub mod checkpoint;
pub mod commit;
pub mod config;
mod config_validation;
mod ddl_lifecycle;
pub mod declarative;
pub mod engine_interface;
pub mod error;
pub mod faults;
mod lib_helpers;
#[cfg(test)]
#[allow(deprecated)]
mod lib_tests;
pub mod locks;
pub mod manifest;
pub mod migration;
pub mod offline;
mod open_support;
pub mod order_book;
pub mod permission;
pub mod preflight;
mod preflight_commit;
pub mod query;
mod query_authorization;
mod query_cursor;
mod query_runtime;
pub mod recovery;
pub mod repository;
pub mod snapshot;
pub mod storage;
pub mod sync_bridge;
mod table_mutation;
mod trust_mode;
pub mod version_store;
pub mod wal;

mod api;

// Public API types defined alongside their primitives to keep this module lean.
pub use crate::api::lease::LeaseRecord;
pub use crate::api::queue::{
    ClaimedTask, EnqueueOptions, EnqueueOutcome, EnqueueSpec, TaskRecord, TaskState,
};

use crate::backup_chain::read_segments_for_checkpoint;
use crate::catalog::namespace_key;
use crate::catalog::schema::{AsyncIndexDef, IndexDef, TableSchema};
use crate::catalog::types::{Row, Value};
use crate::catalog::{DdlOperation, ResourceType};
use crate::checkpoint::retention::{merge_retained_checkpoints, prune_superseded_checkpoint_files};
use crate::checkpoint::writer::write_checkpoint_with_key;
use crate::commit::action::{ActionCommitOutcome, ActionCommitResult, ActionEnvelopeRequest};
use crate::commit::executor::{
    CommitExecutor, CommitResult, ENVELOPE_OVERHEAD_UPPER_BOUND, ExecutorMetrics,
    IdempotencyOutcome, MUTATION_OVERHEAD_UPPER_BOUND,
};
use crate::commit::tx::{ReadSet, TransactionEnvelope, WriteClass, WriteIntent};
use crate::commit::validation::{Mutation, TableUpdateExpr, validate_permissions};
use crate::config::{AedbConfig, DurabilityMode, StorageMode};
use crate::config_validation::{validate_arcana_config, validate_config, validate_secure_config};
use crate::ddl_lifecycle::{ddl_would_apply, order_ddl_ops_for_batch};
use crate::error::AedbError;
use crate::error::ResourceType as ErrorResourceType;
use crate::lib_helpers::{seed_system_global_admin, should_fallback_to_recovery};
use crate::manifest::atomic::write_manifest_atomic_signed;
use crate::manifest::schema::Manifest;
use crate::migration::{
    Migration, MigrationRecord, checksum_hex, decode_record, encode_record, migration_key,
};
use crate::permission::CallerContext;
use crate::preflight::{PreflightResult, preflight_plan_with_config, preflight_with_config};
use crate::preflight_commit::commit_from_preflight_plan;
use crate::query::error::QueryError;
use crate::query::executor::QueryResult;
use crate::query::plan::{ConsistencyMode, Expr, Order, Query, QueryOptions};
use crate::query::planner::ExecutionStage;
use crate::query_authorization::ensure_external_caller_allowed;
use crate::query_cursor::{sign_query_cursor, verify_signed_query_cursor};
use crate::query_runtime::{
    QueryExecutionContext, ensure_stable_order_from_catalog, execute_query_against_view,
    explain_query_against_view, query_error_to_aedb,
};
use crate::recovery::{recover_at_seq_with_config, recover_with_config};
use crate::snapshot::gc::{SnapshotHandle, SnapshotManager};
use crate::snapshot::reader::SnapshotReadView;
use crate::storage::encoded_key::EncodedKey;
use crate::storage::keyspace::NamespaceId;
use crate::table_mutation::{apply_table_update_exprs, extract_primary_key_values};
use fs2::FileExt;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;
use tracing::{info, warn};

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
    wal_gc_shutdown: Arc<AtomicBool>,
    wal_gc_thread: Option<std::thread::JoinHandle<()>>,
    /// In-memory, transaction-scoped object lock registry. Holds no durable
    /// state: it starts empty after every restart and is never written to the
    /// WAL or checkpoints. See [`locks`] for the serialization primitives the
    /// runtime layers on top of MVCC.
    locks: Arc<crate::locks::LockManager>,
    /// Exclusive advisory lock on the data directory, held for the lifetime of
    /// the instance. Prevents a second `AedbInstance` from opening the same
    /// directory concurrently, which would race two writers (and two WAL-GC
    /// threads) against the same segments/manifest and corrupt the store. The
    /// OS releases the lock when this file handle is dropped (i.e. when the
    /// instance is dropped, after its GC thread is joined).
    _dir_lock: fs::File,
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
    n.is_multiple_of(settings.sample_every)
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
    pub persistent_value_store_bytes: u64,
    pub persistent_value_hot_cache_bytes: usize,
    pub persistent_value_hot_cache_capacity_bytes: usize,
    pub persistent_value_hot_cache_hits: u64,
    pub persistent_value_hot_cache_misses: u64,
    pub kv_segment_block_cache_bytes: usize,
    pub kv_segment_block_cache_capacity_bytes: usize,
    pub kv_segment_block_cache_hits: u64,
    pub kv_segment_block_cache_misses: u64,
    /// Live (acquired, not-yet-released) read snapshots — reader pressure.
    pub active_snapshots: usize,
    /// Estimated resident memory of the in-memory keyspace skeleton (keys,
    /// index entries, row metadata; spilled payloads excluded). This is the
    /// footprint that scales with row/key count, so it is the figure to watch
    /// as a dataset grows.
    pub keyspace_resident_bytes: u64,
    /// Configured `max_memory_estimate_bytes` soft cap that drives spill-to-disk.
    pub keyspace_memory_budget_bytes: u64,
    /// `keyspace_resident_bytes / keyspace_memory_budget_bytes`, clamped to
    /// `[0, 1+]`. Approaching `1.0` means spill pressure / headroom exhaustion.
    pub keyspace_memory_used_fraction: f64,
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

/// Scan direction for [`AedbInstance::query_events`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EventOrder {
    /// Oldest first, ordered by ascending commit sequence (default).
    #[default]
    Ascending,
    /// Newest first, ordered by descending commit sequence. Use for
    /// "latest N events by type" style queries.
    Descending,
}

/// Equality filter against a top-level field of an event's JSON payload.
///
/// Lets callers query by `recipient`, `instance_id`, `block`, or any other
/// attribute carried inside the opaque payload without changing the event
/// schema. Filters are evaluated at scan time (not via a secondary index),
/// so always pair them with a `topic` and/or sequence/time bound plus a
/// sensible `limit` to keep scans bounded (see `max_scan_rows`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventFieldFilter {
    /// Top-level JSON key to match, e.g. `"recipient"` or `"instance_id"`.
    pub field: String,
    /// Expected value, compared against the payload value rendered as a
    /// string: JSON strings compare by content; numbers and booleans by
    /// their canonical textual form.
    pub equals: String,
}

/// Ergonomic query over the canonical event log (the `event_outbox` system
/// table). Supports filtering by event type (`topic`), project/scope,
/// commit-sequence range (cursor pagination), time range, and arbitrary
/// payload fields (recipient, instance_id, block, ...), in either direction.
///
/// Construct with [`EventQuery::new`] and the builder methods, then pass to
/// [`AedbInstance::query_events`]. Pagination is commit-atomic: a page never
/// splits the events emitted by a single commit, and the returned
/// [`EventStreamPage::next_commit_seq`] is the cursor for the next page
/// (feed it back as `from_commit_seq_exclusive` for ascending queries, or
/// `to_commit_seq_exclusive` for descending queries).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EventQuery {
    /// Event type filter (matches the emit `topic`). `None` matches all.
    pub topic: Option<String>,
    /// Restrict to events emitted under this project. `None` matches all.
    pub project_id: Option<String>,
    /// Restrict to events emitted under this scope. `None` matches all.
    pub scope_id: Option<String>,
    /// Exclusive lower bound on commit sequence (ascending cursor).
    pub from_commit_seq_exclusive: Option<u64>,
    /// Exclusive upper bound on commit sequence (descending cursor).
    pub to_commit_seq_exclusive: Option<u64>,
    /// Inclusive lower bound on event timestamp (micros since epoch).
    pub from_ts_micros: Option<u64>,
    /// Inclusive upper bound on event timestamp (micros since epoch).
    pub to_ts_micros: Option<u64>,
    /// Payload field equality filters (ANDed together).
    pub field_filters: Vec<EventFieldFilter>,
    /// Scan direction.
    pub order: EventOrder,
    /// Maximum number of events to return in this page.
    pub limit: usize,
}

/// Lifecycle state of an exactly-once external-effect checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectStatus {
    /// The effect has been claimed but not yet completed (may be mid-flight).
    Pending,
    /// The effect completed exactly once; its internal mutations were applied.
    Committed,
}

/// Outcome of [`AedbInstance::begin_effect`] — claiming a dedupe key before
/// performing a side effect (e.g. an external submission or a credit).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EffectClaim {
    /// No prior attempt existed; the caller now owns this key and should
    /// perform the effect, then call [`AedbInstance::complete_effect`].
    Fresh,
    /// A prior attempt claimed this key but never completed. `attempts` is the
    /// total number of claims so far (including this one). The caller must
    /// decide whether the external effect is safe to repeat — for a
    /// non-idempotent external API this signals "reconcile before resubmitting".
    InProgress { attempts: u64 },
    /// The effect already completed exactly once. The caller must NOT repeat it.
    /// `result_json` is whatever was recorded at completion (e.g. a tx hash).
    AlreadyCommitted { result_json: String },
}

/// A durable record of an external-effect checkpoint. Stored in KV under an
/// engine-reserved key, so it survives restart, checkpoint, and restore.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectCheckpointRecord {
    pub project_id: String,
    pub scope_id: String,
    pub dedupe_key: String,
    pub status: EffectStatus,
    pub attempts: u64,
    pub created_at_micros: u64,
    pub updated_at_micros: u64,
    pub result_json: String,
}

/// What a single commit produced — the events it emitted. Answers "what
/// effects/events did this action commit?" for debugging.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitTrace {
    pub commit_seq: u64,
    pub events: Vec<EventOutboxRecord>,
}

/// Summary of the canonical event log (the `event_outbox` system table).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventLogSummary {
    pub count: u64,
    pub oldest_seq: Option<u64>,
    pub newest_seq: Option<u64>,
}

/// Aggregated inspection of one project/scope: its monitors and the state of
/// its external-effect checkpoints. A one-call dashboard for "which monitor
/// checkpoint advanced" and "which effects committed".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceInspection {
    pub project_id: String,
    pub scope_id: String,
    pub monitors: Vec<MonitorStatus>,
    pub effect_pending: u64,
    pub effect_committed: u64,
}

/// Retention policy for compacting the canonical event log. Events are pruned
/// only when **every** active constraint permits it (retain if any says keep).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EventRetentionPolicy {
    /// Prune events older than this many microseconds. `None` = no age bound.
    pub max_age_micros: Option<u64>,
    /// Always keep at least this many most-recent events. `None` = no count bound.
    pub keep_last: Option<u64>,
    /// When true (recommended), never prune events that a reactive processor has
    /// not yet consumed (i.e. never prune above the slowest processor
    /// checkpoint). Manual stream consumers must track their own cursors.
    pub respect_processor_checkpoints: bool,
    /// Cap the number of events pruned in a single call. `None` defers to
    /// `max_scan_rows`. If the eligible set is larger, the call reports
    /// `more_remaining` so the caller can loop.
    pub max_prune_per_run: Option<usize>,
}

/// Result of an [`AedbInstance::compact_events`] run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventCompactionReport {
    /// Number of events deleted.
    pub pruned_count: u64,
    /// Smallest commit sequence pruned (the start of the archived range).
    pub archived_min_seq: Option<u64>,
    /// Largest commit sequence pruned (the end of the archived range).
    pub archived_max_seq: Option<u64>,
    /// The reactive-processor checkpoint floor that capped pruning, if any.
    pub processor_floor_seq: Option<u64>,
    /// True if the eligible set was larger than the per-run cap; call again to
    /// continue.
    pub more_remaining: bool,
}

/// A held monitor lease: a single-owner claim with a fencing token and expiry.
/// The `fencing_token` strictly increases each time ownership changes, so a
/// checkpoint advance can be rejected if a newer owner has taken over (fencing
/// out a zombie monitor).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonitorLease {
    pub owner_id: String,
    pub fencing_token: u64,
    pub lease_until_micros: u64,
}

/// Result of attempting to acquire a monitor lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseOutcome {
    /// The caller now holds the lease.
    Acquired(MonitorLease),
    /// Another owner holds a live lease; retry after `lease_until_micros`.
    Held {
        owner_id: String,
        lease_until_micros: u64,
    },
}

/// Result of renewing a monitor lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenewOutcome {
    Renewed(MonitorLease),
    /// The fencing token is stale — another owner has taken over. Re-acquire.
    LeaseLost,
}

/// Result of a lease-fenced checkpoint advance.
#[derive(Debug, Clone)]
pub enum FencedCommit {
    /// The checkpoint update and caller mutations were applied atomically.
    Applied(CommitResult),
    /// The fencing token is no longer current (lease lost or expired). Nothing
    /// was applied; the caller must re-acquire the lease.
    LeaseLost,
}

/// Partial update to a monitor checkpoint. `None` fields are left unchanged.
/// Used to record scan progress (last scanned block, cursor) and retry state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MonitorCheckpointUpdate {
    /// The block the monitor started scanning from (typically set once).
    pub start_block: Option<u64>,
    /// Highest block fully scanned so far.
    pub last_scanned_block: Option<u64>,
    /// Opaque last-processed cursor (e.g. a Nado page cursor).
    pub last_processed_cursor: Option<String>,
    /// Retry counter for the current backoff cycle.
    pub retry_count: Option<u32>,
    /// Last error string; `Some("")` clears it.
    pub last_error: Option<String>,
}

/// Full inspection view of a monitor: scan checkpoint, retry state, and lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonitorStatus {
    pub project_id: String,
    pub scope_id: String,
    pub monitor: String,
    pub start_block: Option<u64>,
    pub last_scanned_block: Option<u64>,
    pub last_processed_cursor: Option<String>,
    pub last_update_micros: u64,
    pub retry_count: u32,
    pub last_error: Option<String>,
    pub owner_id: Option<String>,
    pub fencing_token: u64,
    pub lease_until_micros: u64,
}

/// A single entry from a latest-per-key projection view (see the projection
/// API: latest protocol status, account position summaries, pending intents).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionEntry {
    pub entity_key: String,
    pub value_json: String,
    pub updated_at_micros: u64,
}

/// Outcome of [`AedbInstance::complete_effect`].
#[derive(Debug, Clone)]
pub enum CompleteEffect {
    /// The internal mutations and the "committed" marker were applied together
    /// in a single atomic commit (exactly-once).
    Applied(CommitResult),
    /// The effect had already been completed by an earlier call; the internal
    /// mutations were NOT applied again. `result_json` is the recorded result.
    AlreadyCommitted { result_json: String },
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

    fn kv_segment_filenames(&mut self) -> HashSet<String> {
        self.prune_expired();
        let mut filenames = HashSet::new();
        for entry in self.entries.values() {
            filenames.extend(entry.view.keyspace.kv_segment_filenames());
        }
        filenames
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

impl Drop for AedbInstance {
    fn drop(&mut self) {
        self.wal_gc_shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.wal_gc_thread.take() {
            let _ = handle.join();
        }
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

    /// Open an instance that requires an authenticated caller for every
    /// caller-facing operation (the secure default).
    ///
    /// Anonymous `commit`/`kv_set`/`query` and the `*_no_auth` helpers are
    /// rejected; use the `*_as` APIs with a [`CallerContext`]. Unlike
    /// [`Self::open_secure`], this does not additionally enforce the full
    /// secure-config validation (e.g. a configured manifest HMAC key) — it only
    /// flips the authenticated-caller guard on.
    ///
    /// To intentionally allow anonymous access, use [`Self::open_anonymous`].
    pub fn open(config: AedbConfig, dir: &Path) -> Result<Self, AedbError> {
        Self::open_internal(config, dir, true)
    }

    /// Open an instance that permits anonymous (unauthenticated) operations.
    ///
    /// This is the explicit opt-out from the authenticated-caller default of
    /// [`Self::open`]. Anonymous `commit`/`kv_set`/`query` and the `*_no_auth`
    /// helpers are allowed. Prefer [`Self::open`] (or [`Self::open_secure`]) for
    /// any deployment exposed to untrusted callers.
    pub fn open_anonymous(config: AedbConfig, dir: &Path) -> Result<Self, AedbError> {
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
            max_batch_rows = config.max_batch_rows,
            max_kv_key_bytes = config.max_kv_key_bytes,
            max_kv_value_bytes = config.max_kv_value_bytes,
            max_table_value_bytes = config.max_table_value_bytes,
            max_event_payload_bytes = config.max_event_payload_bytes,
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
            max_versions = config.max_versions,
            version_store_full_snapshot_interval_deltas =
                config.version_store_full_snapshot_interval_deltas,
            min_version_age_ms = config.min_version_age_ms,
            version_gc_interval_ms = config.version_gc_interval_ms,
            checkpoint_encryption_enabled = config.checkpoint_encryption_key.is_some(),
            checkpoint_key_id = config.checkpoint_key_id.as_deref().unwrap_or(""),
            checkpoint_compression_level = config.checkpoint_compression_level,
            manifest_hmac_enabled = config.manifest_hmac_key.is_some(),
            recovery_mode = ?config.recovery_mode,
            hash_chain_required = config.hash_chain_required,
            primary_index_backend = ?config.primary_index_backend,
            storage_mode = ?config.storage_mode,
            persistent_value_inline_threshold_bytes = config.persistent_value_inline_threshold_bytes,
            persistent_value_hot_cache_bytes = config.persistent_value_hot_cache_bytes,
            kv_segment_block_cache_bytes = config.kv_segment_block_cache_bytes,
            "aedb config"
        );
        open_support::create_private_dir_all(dir)?;

        // Acquire an exclusive advisory lock on the data directory before
        // touching any state. This is the single guard that makes it safe to
        // assume there is exactly one writer per directory: a second open() on
        // the same dir (e.g. a stray reopen while the first instance is still
        // alive) fails fast here instead of silently corrupting WAL segments or
        // having two GC threads delete each other's live segments. The lock
        // file name deliberately avoids the ".aedb" substring so it is not
        // mistaken for existing state by the recovery probe below.
        let dir_lock = {
            let lock_path = dir.join(".dirlock");
            let file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&lock_path)?;
            match FileExt::try_lock_exclusive(&file) {
                Ok(()) => file,
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    return Err(AedbError::Unavailable {
                        message: format!(
                            "another AedbInstance already holds an exclusive lock on {dir:?}; \
                             refusing to open a second instance on the same data directory \
                             (concurrent writers would corrupt WAL segments and the manifest)"
                        ),
                    });
                }
                Err(err) => return Err(AedbError::Io(err)),
            }
        };

        trust_mode::enforce_and_record_trust_mode(dir, &config)?;
        let has_existing = fs::read_dir(dir)?
            .filter_map(|e| e.ok())
            .any(|e| e.file_name().to_string_lossy().contains(".aedb"));

        let recovery_start = std::time::SystemTime::now();
        let (executor, startup_recovered_seq) = if has_existing {
            let mut recovered = recover_with_config(dir, &config)?;
            recovered.keyspace.set_backend(config.primary_index_backend);
            open_support::attach_configured_value_store(&mut recovered.keyspace, &config, dir)?;
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
            let mut keyspace =
                crate::storage::keyspace::Keyspace::with_backend(config.primary_index_backend);
            open_support::attach_configured_value_store(&mut keyspace, &config, dir)?;
            (
                CommitExecutor::with_state(
                    dir,
                    keyspace,
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
        let cursor_signing_key = open_support::derive_cursor_signing_key(&config, dir);
        let snapshot_manager = Arc::new(Mutex::new(SnapshotManager::default()));
        let wal_gc_shutdown = Arc::new(AtomicBool::new(false));
        let wal_gc_dir = dir.to_path_buf();
        let wal_gc_hmac_key = config.hmac_key().map(|key| key.to_vec());
        let wal_gc_snapshot_manager = Arc::clone(&snapshot_manager);
        let wal_gc_shutdown_thread = Arc::clone(&wal_gc_shutdown);
        let wal_gc_interval_ms = config.version_gc_interval_ms.max(1);
        let wal_gc_thread = std::thread::spawn(move || {
            while !wal_gc_shutdown_thread.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(wal_gc_interval_ms));
                {
                    let mut mgr = wal_gc_snapshot_manager.lock();
                    let _ = mgr.gc();
                }
                if let Err(err) = open_support::reclaim_eligible_wal_segments(
                    &wal_gc_dir,
                    &wal_gc_snapshot_manager,
                    wal_gc_hmac_key.as_deref(),
                ) {
                    warn!(error = ?err, "wal segment gc failed");
                }
            }
        });

        Ok(Self {
            _config: config,
            require_authenticated_calls,
            dir: dir.to_path_buf(),
            cursor_signing_key,
            executor,
            checkpoint_lock: Arc::new(AsyncMutex::new(())),
            snapshot_manager,
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
            wal_gc_shutdown,
            wal_gc_thread: Some(wal_gc_thread),
            locks: Arc::new(crate::locks::LockManager::default()),
            _dir_lock: dir_lock,
        })
    }

    pub async fn commit(&self, mutation: Mutation) -> Result<CommitResult, AedbError> {
        let started = self
            .telemetry_hooks_present
            .load(Ordering::Acquire)
            .then(Instant::now);
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required; use commit_as in secure mode".into(),
            ));
        }
        // Early size validation to prevent DoS via oversized keys/values
        crate::commit::validation::validate_kv_sizes_early(&mutation, &self._config)?;

        let result = self.executor.submit_kv_sizes_prechecked(mutation).await;
        if let Some(started) = started {
            self.emit_commit_telemetry("commit", started, &result);
        }
        let result = result?;
        self.dispatch_lifecycle_events_for_commit(result.commit_seq)
            .await;
        Ok(result)
    }

    /// Commit a mutation without a caller identity.
    ///
    /// This is the explicit anonymous write path, mirroring
    /// [`Self::query_no_auth`] on the read side. It is unavailable for instances
    /// opened with [`Self::open`], [`Self::open_secure`], or
    /// [`Self::open_production`]; prefer [`Self::commit_as`] in services. Open
    /// the instance with [`Self::open_anonymous`] to use this path.
    pub async fn commit_no_auth(&self, mutation: Mutation) -> Result<CommitResult, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "commit_no_auth is unavailable in secure mode; use commit_as".into(),
            ));
        }
        let started = self
            .telemetry_hooks_present
            .load(Ordering::Acquire)
            .then(Instant::now);
        // Early size validation to prevent DoS via oversized keys/values
        crate::commit::validation::validate_kv_sizes_early(&mutation, &self._config)?;

        let result = self.executor.submit_kv_sizes_prechecked(mutation).await;
        if let Some(started) = started {
            self.emit_commit_telemetry("commit_no_auth", started, &result);
        }
        let result = result?;
        self.dispatch_lifecycle_events_for_commit(result.commit_seq)
            .await;
        Ok(result)
    }

    /// Subscribe to the public commit-delta broadcast stream.
    ///
    /// Each successful commit produces one `Arc<CommitDelta>` on the channel
    /// after the commit's deltas have been applied and the durable / visible
    /// head has advanced. Lagged subscribers receive
    /// `tokio::sync::broadcast::error::RecvError::Lagged(n)` and may resume.
    /// The channel capacity is `AedbConfig::commit_broadcast_capacity`.
    pub fn subscribe_commits(
        &self,
    ) -> tokio::sync::broadcast::Receiver<Arc<crate::version_store::CommitDelta>> {
        self.executor.subscribe_commits()
    }

    /// Begin a transaction-scoped lock set for serializing modifications to
    /// logical objects (see the [`locks`] module for full semantics and
    /// examples).
    ///
    /// Acquire one or more [`locks::LockKey`]s on the returned
    /// [`locks::TxLockSet`], perform the guarded reads/writes with the normal
    /// commit and query APIs, then call `commit()` / `rollback()` (or drop the
    /// set) to release them. Locks are exclusive while held, never block MVCC
    /// snapshot readers, and are discarded on restart.
    pub fn lock_scope(&self) -> crate::locks::TxLockSet {
        self.locks.begin()
    }

    /// Direct handle to the in-memory object lock registry. Most callers should
    /// use [`lock_scope`](Self::lock_scope); this is exposed for inspection and
    /// for sharing the registry with a co-located runtime.
    pub fn lock_manager(&self) -> &Arc<crate::locks::LockManager> {
        &self.locks
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

    async fn commit_envelope_prevalidated_system_internal(
        &self,
        op_name: &'static str,
        envelope: TransactionEnvelope,
    ) -> Result<CommitResult, AedbError> {
        let started = Instant::now();
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
        self.commit_envelope_with_size_hint(envelope, None).await
    }

    async fn commit_envelope_with_size_hint(
        &self,
        envelope: TransactionEnvelope,
        encoded_size_hint: Option<usize>,
    ) -> Result<CommitResult, AedbError> {
        let started = self
            .telemetry_hooks_present
            .load(Ordering::Acquire)
            .then(Instant::now);
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
        let result = match encoded_size_hint {
            Some(hint) => {
                self.executor
                    .submit_envelope_with_size_hint(envelope, Some(hint))
                    .await
            }
            None => self.executor.submit_envelope(envelope).await,
        };
        if let Some(started) = started {
            self.emit_commit_telemetry("commit_envelope", started, &result);
        }
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
        let Some(stored) = table.rows.get(&encoded_pk) else {
            return Ok(Vec::new());
        };
        let row = snapshot.materialize_row(stored)?;
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
                    table.version_of(&crate::storage::encoded_key::EncodedKey::from_values(
                        &primary_key,
                    ))
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
        preflight_with_config(&snapshot, &catalog, &mutation, &self._config)
    }

    pub async fn preflight_plan(&self, mutation: Mutation) -> crate::commit::tx::PreflightPlan {
        let (snapshot, catalog, base_seq) = self.executor.snapshot_state().await;
        preflight_plan_with_config(&snapshot, &catalog, &mutation, base_seq, &self._config)
    }

    pub async fn preflight_as(
        &self,
        caller: &CallerContext,
        mutation: Mutation,
    ) -> Result<PreflightResult, AedbError> {
        ensure_external_caller_allowed(caller)?;
        let (snapshot, catalog, _) = self.executor.snapshot_state().await;
        validate_permissions(&catalog, Some(caller), &mutation)?;
        Ok(preflight_with_config(
            &snapshot,
            &catalog,
            &mutation,
            &self._config,
        ))
    }

    pub async fn preflight_plan_as(
        &self,
        caller: &CallerContext,
        mutation: Mutation,
    ) -> Result<crate::commit::tx::PreflightPlan, AedbError> {
        ensure_external_caller_allowed(caller)?;
        let (snapshot, catalog, base_seq) = self.executor.snapshot_state().await;
        validate_permissions(&catalog, Some(caller), &mutation)?;
        Ok(preflight_plan_with_config(
            &snapshot,
            &catalog,
            &mutation,
            base_seq,
            &self._config,
        ))
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
                    .map_or(0, |ns| {
                        ns.kv
                            .entries
                            .len()
                            .saturating_add(ns.kv.small_entries.len())
                            as u64
                    });
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
        // Quiesce background reactive-processor loops first. They hold a
        // Weak<self> and upgrade it per iteration; stopping them ensures no loop
        // is holding a strong ref when the caller drops its Arc, so the instance
        // can fully drop and release the exclusive data-directory lock. Registry
        // state is left untouched so processors auto-resume on the next start.
        self.stop_all_reactive_runtimes_for_shutdown().await;
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
        let retention_count = self._config.checkpoint_retention_count();
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

            // Retain the previous `retention_count` checkpoints so a corrupt
            // newest checkpoint can fall back to an older one; WAL is anchored to
            // the OLDEST retained checkpoint so fallback can still replay forward.
            // If the prior manifest cannot be loaded (first checkpoint, or
            // corrupt/untrusted under an HMAC key where reconstruction is
            // disabled), use an empty prior list but DO NOT prune: collapsing the
            // window to the new seq would delete the older checkpoint files that
            // are the whole point of retention, right when the manifest is suspect.
            let prior_manifest =
                crate::manifest::atomic::load_manifest_signed(&dir, manifest_hmac_key.as_deref());
            let prune_allowed = prior_manifest.is_ok();
            let prior = prior_manifest
                .map(|manifest| manifest.checkpoints)
                .unwrap_or_default();
            let checkpoints = merge_retained_checkpoints(prior, checkpoint, retention_count);
            let oldest_retained_seq = checkpoints.first().map(|cp| cp.seq).unwrap_or(seq);

            let segments = read_segments_for_checkpoint(&dir, oldest_retained_seq)?;
            let active_segment_seq = segments
                .last()
                .map(|segment| segment.segment_seq)
                .unwrap_or(seq.saturating_add(1));
            let manifest = Manifest {
                durable_seq: seq,
                visible_seq: seq,
                active_segment_seq,
                checkpoints,
                segments,
                ..Default::default()
            };
            write_manifest_atomic_signed(&manifest, &dir, manifest_hmac_key.as_deref())?;
            // Manifest is durable (including a dir fsync) before we remove the
            // now-unreferenced older checkpoint files.
            if prune_allowed {
                prune_superseded_checkpoint_files(&dir, oldest_retained_seq);
            }
            Ok(())
        })
        .await
        .map_err(|e| AedbError::Io(std::io::Error::other(e.to_string())))??;
        Ok(seq)
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
        let avg_durable_wait_micros = durable_wait_micros
            .checked_div(durable_wait_ops)
            .unwrap_or(0);
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
            persistent_value_store_bytes: runtime.persistent_value_store_bytes,
            persistent_value_hot_cache_bytes: runtime.persistent_value_hot_cache_bytes,
            persistent_value_hot_cache_capacity_bytes: runtime
                .persistent_value_hot_cache_capacity_bytes,
            persistent_value_hot_cache_hits: runtime.persistent_value_hot_cache_hits,
            persistent_value_hot_cache_misses: runtime.persistent_value_hot_cache_misses,
            kv_segment_block_cache_bytes: runtime.kv_segment_block_cache_bytes,
            kv_segment_block_cache_capacity_bytes: runtime.kv_segment_block_cache_capacity_bytes,
            kv_segment_block_cache_hits: runtime.kv_segment_block_cache_hits,
            kv_segment_block_cache_misses: runtime.kv_segment_block_cache_misses,
            active_snapshots: self.snapshot_manager.lock().live_count(),
            keyspace_resident_bytes: runtime.keyspace_resident_bytes,
            keyspace_memory_budget_bytes: runtime.keyspace_memory_budget_bytes,
            keyspace_memory_used_fraction: if runtime.keyspace_memory_budget_bytes == 0 {
                0.0
            } else {
                runtime.keyspace_resident_bytes as f64 / runtime.keyspace_memory_budget_bytes as f64
            },
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

    pub async fn reclaim_unused_kv_segments(&self) -> Result<usize, AedbError> {
        if !matches!(self._config.storage_mode, StorageMode::DiskBacked) {
            return Ok(0);
        }
        let mut referenced_filenames = HashSet::new();
        {
            let manager = self.snapshot_manager.lock();
            referenced_filenames.extend(manager.active_kv_segment_filenames());
        }
        {
            let mut recovery_cache = self.recovery_cache.lock();
            referenced_filenames.extend(recovery_cache.kv_segment_filenames());
        }
        self.executor
            .reclaim_unused_kv_segments(referenced_filenames)
            .await
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
                snapshot.try_kv_get(&migration.project_id, &migration.scope_id, &key)?
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
                        snapshot.try_kv_get(&migration.project_id, &migration.scope_id, &key)?
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
        let mut out = Vec::new();
        let prefix = b"__migrations/";
        for (_, v) in snapshot.try_kv_scan_prefix(project_id, scope_id, prefix, usize::MAX)? {
            out.push(decode_record(&v.value)?);
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
        let prefix = b"__migrations/";
        let entries = snapshot.try_kv_scan_prefix(project_id, scope_id, prefix, usize::MAX)?;
        if let Some((key, value)) = entries.last() {
            if let Some(version) = parse_migration_version_from_key(key) {
                return Ok(version);
            }
            return Ok(decode_record(&value.value)?.version);
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
        let rows: Vec<crate::catalog::types::Row> = {
            let mut materialized = Vec::with_capacity(table.rows.len());
            for stored in table.rows.values() {
                materialized.push(snapshot.materialize_row(stored)?.into_owned());
            }
            materialized
        };
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
        let rows: Vec<crate::catalog::types::Row> = {
            let mut materialized = Vec::with_capacity(table.rows.len());
            for stored in table.rows.values() {
                materialized.push(snapshot.materialize_row(stored)?.into_owned());
            }
            materialized
        };
        let start_offset = snapshot
            .try_kv_get(project_id, scope_id, progress_key)?
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
        let mut out = HashMap::with_capacity(versions.len());
        let prefix = b"__migrations/";
        for (k, v) in snapshot.try_kv_scan_prefix(project_id, scope_id, prefix, usize::MAX)? {
            if let Some(version) = parse_migration_version_from_key(&k) {
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
            QueryExecutionContext {
                view: &self.lease.view,
                project_id,
                scope_id,
                options: &options,
                caller: self.caller.as_ref(),
                max_scan_rows: self.db._config.max_scan_rows,
                cursor_signing_key: self.db._config.cursor_signing_key(),
            },
            query,
        );
        tokio::task::yield_now().await;
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
            offset: None,
            group_by: Vec::new(),
            aggregates: vec![crate::query::plan::Aggregate::Count],
            having: None,
            use_index: query.use_index.clone(),
            distinct: false,
            computed: Vec::new(),
        };
        let count_result = self
            .query_with_options(project_id, scope_id, count_query, QueryOptions::default())
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
            let requested_rows = offset.saturating_add(page_size.max(1));
            if requested_rows > self.db._config.max_scan_rows {
                return Err(QueryError::ScanBoundExceeded {
                    estimated_rows: requested_rows as u64,
                    max_scan_rows: self.db._config.max_scan_rows as u64,
                });
            }
            let mut full_query = query.clone();
            full_query.limit = Some(page_size.max(1));
            full_query.offset = Some(offset);
            let full = self
                .query_with_options(project_id, scope_id, full_query, QueryOptions::default())
                .await?;
            return Ok(ListPageResult {
                rows: full.rows,
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
                    split_recommended: false,
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
        let page_size = self.db._config.max_scan_rows.clamp(1, 100);
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
            split_recommended: false,
        };
        Ok((source, hydrated))
    }
}
