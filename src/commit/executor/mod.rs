use crate::catalog::Catalog;
use crate::catalog::namespace_key;
use crate::catalog::schema::TableSchema;
use crate::catalog::types::{Row, Value};
use crate::commit::apply::{apply_mutation, apply_mutation_trusted_if_eligible};
use crate::commit::tx::{
    IdempotencyKey, IdempotencyRecord, ReadBound, ReadKey, ReadRange, TransactionEnvelope,
    WriteClass, WriteIntent,
};
use crate::commit::validation::{Mutation, validate_permissions};
use crate::config::{AedbConfig, DurabilityMode};
use crate::error::AedbError;
use crate::permission::CallerContext;
use crate::snapshot::reader::SnapshotReadView;
use crate::storage::encoded_key::EncodedKey;
use crate::storage::index::extract_index_key_encoded;
use crate::storage::keyspace::{Keyspace, KeyspaceSnapshot, KvData, Namespace, NamespaceId};
use crate::version_store::{CommitDelta, VersionStore};
use crate::wal::segment::{SegmentConfig, SegmentManager};
use adaptive::AdaptiveEpochState;
use coordinator::CoordinatorLockManager;
use global_index::GlobalUniqueIndexState;
use group_commit::{
    GroupCommitFlushReason, GroupCommitPhase, GroupCommitPolicy, GroupCommitStateMachine,
};
use parallel_runtime::ParallelApplyRuntime;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, Notify, mpsc as tokio_mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct CommitResult {
    pub commit_seq: u64,
    pub durable_head_seq: u64,
    pub idempotency: IdempotencyOutcome,
    pub canonical_commit_seq: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdempotencyOutcome {
    Applied,
    Duplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeadState {
    pub visible_head_seq: u64,
    pub durable_head_seq: u64,
}

struct CommitRequest {
    envelope: TransactionEnvelope,
    mutation_count: usize,
    encoded_len: usize,
    enqueue_micros: u64,
    prevalidated: bool,
    assertions_engine_verified: bool,
    write_partitions: HashSet<String>,
    read_partitions: HashSet<String>,
    defer_count: u8,
    result_tx: oneshot::Sender<Result<CommitResult, AedbError>>,
}

struct PostApplyTask {
    delta: Arc<CommitDelta>,
}

const GLOBAL_PARTITION_TOKEN: &str = "__global__";
const MAX_EPOCH_DEFER: u8 = 3;

#[cfg(test)]
static COORDINATOR_TEST_DELAY_MS: AtomicU64 = AtomicU64::new(0);

struct SequencedCommit {
    request: CommitRequest,
    seq: u64,
    commit_ts_micros: u64,
    payload_type: u8,
    payload: Vec<u8>,
    delta: Arc<CommitDelta>,
}

#[derive(Default)]
struct CountingWriter {
    len: usize,
}

impl std::io::Write for CountingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.len = self.len.saturating_add(buf.len());
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn estimate_prevalidated_single_mutation_size_upper_bound(mutation: &Mutation) -> Option<usize> {
    const ENVELOPE_OVERHEAD_UPPER_BOUND: usize = 256;
    match mutation {
        Mutation::ExposeAccumulator {
            project_id,
            scope_id,
            accumulator_name,
            exposure_id,
            ..
        } => Some(
            ENVELOPE_OVERHEAD_UPPER_BOUND
                + project_id.len()
                + scope_id.len()
                + accumulator_name.len()
                + exposure_id.len(),
        ),
        Mutation::Accumulate {
            project_id,
            scope_id,
            accumulator_name,
            dedupe_key,
            release_exposure_id,
            ..
        } => Some(
            ENVELOPE_OVERHEAD_UPPER_BOUND
                + project_id.len()
                + scope_id.len()
                + accumulator_name.len()
                + dedupe_key.len()
                + release_exposure_id.as_ref().map(|id| id.len()).unwrap_or(0),
        ),
        Mutation::ExposeAccumulatorBatch {
            project_id,
            scope_id,
            accumulator_name,
            exposures,
        } => Some(
            ENVELOPE_OVERHEAD_UPPER_BOUND
                + project_id.len()
                + scope_id.len()
                + accumulator_name.len()
                + exposures.iter().map(|(_, id)| id.len() + 16).sum::<usize>(),
        ),
        _ => None,
    }
}

fn estimate_value_size_upper_bound(value: &Value) -> usize {
    match value {
        Value::Text(v) | Value::Json(v) => 16 + v.len(),
        Value::Blob(v) => 16 + v.len(),
        Value::U256(_) | Value::I256(_) => 48,
        Value::Float(_) | Value::U64(_) | Value::Integer(_) | Value::Timestamp(_) => 16,
        Value::Boolean(_) | Value::U8(_) | Value::Null => 8,
    }
}

fn estimate_values_size_upper_bound(values: &[Value]) -> usize {
    16 + values
        .iter()
        .map(estimate_value_size_upper_bound)
        .sum::<usize>()
}

fn estimate_row_size_upper_bound(row: &Row) -> usize {
    estimate_values_size_upper_bound(&row.values)
}

const ENVELOPE_OVERHEAD_UPPER_BOUND: usize = 384;
const MUTATION_OVERHEAD_UPPER_BOUND: usize = 192;

fn estimate_mutation_size_upper_bound(mutation: &Mutation) -> Option<usize> {
    match mutation {
        Mutation::KvSet {
            project_id,
            scope_id,
            key,
            value,
        } => Some(
            MUTATION_OVERHEAD_UPPER_BOUND
                + project_id.len()
                + scope_id.len()
                + key.len()
                + value.len(),
        ),
        Mutation::KvDel {
            project_id,
            scope_id,
            key,
        } => Some(MUTATION_OVERHEAD_UPPER_BOUND + project_id.len() + scope_id.len() + key.len()),
        Mutation::KvIncU256 {
            project_id,
            scope_id,
            key,
            ..
        }
        | Mutation::KvDecU256 {
            project_id,
            scope_id,
            key,
            ..
        }
        | Mutation::KvMaxU256 {
            project_id,
            scope_id,
            key,
            ..
        }
        | Mutation::KvMinU256 {
            project_id,
            scope_id,
            key,
            ..
        } => {
            Some(MUTATION_OVERHEAD_UPPER_BOUND + project_id.len() + scope_id.len() + key.len() + 48)
        }
        Mutation::KvAddU64Ex {
            project_id,
            scope_id,
            key,
            ..
        }
        | Mutation::KvSubU64Ex {
            project_id,
            scope_id,
            key,
            ..
        }
        | Mutation::KvMaxU64 {
            project_id,
            scope_id,
            key,
            ..
        }
        | Mutation::KvMinU64 {
            project_id,
            scope_id,
            key,
            ..
        } => {
            Some(MUTATION_OVERHEAD_UPPER_BOUND + project_id.len() + scope_id.len() + key.len() + 24)
        }
        Mutation::Upsert {
            project_id,
            scope_id,
            table_name,
            primary_key,
            row,
        }
        | Mutation::Insert {
            project_id,
            scope_id,
            table_name,
            primary_key,
            row,
        } => Some(
            MUTATION_OVERHEAD_UPPER_BOUND
                + project_id.len()
                + scope_id.len()
                + table_name.len()
                + estimate_values_size_upper_bound(primary_key)
                + estimate_row_size_upper_bound(row),
        ),
        Mutation::Delete {
            project_id,
            scope_id,
            table_name,
            primary_key,
        } => Some(
            MUTATION_OVERHEAD_UPPER_BOUND
                + project_id.len()
                + scope_id.len()
                + table_name.len()
                + estimate_values_size_upper_bound(primary_key),
        ),
        _ => estimate_prevalidated_single_mutation_size_upper_bound(mutation),
    }
}

fn estimate_single_mutation_size_upper_bound(mutation: &Mutation) -> Option<usize> {
    estimate_mutation_size_upper_bound(mutation)?.checked_add(ENVELOPE_OVERHEAD_UPPER_BOUND)
}

fn estimate_transaction_envelope_size_upper_bound(envelope: &TransactionEnvelope) -> Option<usize> {
    if envelope.caller.is_some()
        || envelope.idempotency_key.is_some()
        || !envelope.assertions.is_empty()
        || !envelope.read_set.points.is_empty()
        || !envelope.read_set.ranges.is_empty()
    {
        return None;
    }
    envelope
        .write_intent
        .mutations
        .iter()
        .try_fold(ENVELOPE_OVERHEAD_UPPER_BOUND, |acc, mutation| {
            acc.checked_add(estimate_mutation_size_upper_bound(mutation)?)
        })
}

struct InternalSequencedCommit {
    seq: u64,
    commit_ts_micros: u64,
    payload_type: u8,
    payload: Vec<u8>,
    delta: Arc<CommitDelta>,
}

struct EpochOutcome {
    request: CommitRequest,
    result: Result<CommitResult, AedbError>,
    post_apply_delta: Option<Arc<CommitDelta>>,
}

#[derive(Default)]
struct EpochProcessResult {
    outcomes: Vec<EpochOutcome>,
    coordinator_apply_attempts: u64,
    coordinator_apply_micros: u64,
    parallel_apply_micros: u64,
    pre_wal_micros: u64,
    finalize_micros: u64,
    read_set_conflicts: u64,
    wal_append_ops: u64,
    wal_append_bytes: u64,
    wal_append_micros: u64,
    wal_sync_ops: u64,
    wal_sync_micros: u64,
    sync_executed: bool,
    catalog_changed: bool,
}

struct ExecutorState {
    keyspace: Keyspace,
    catalog: Catalog,
    wal: SegmentManager,
    current_seq: u64,
    visible_head_seq: u64,
    durable_head_seq: u64,
    pending_batch_bytes: usize,
    pending_batch_max_seq: u64,
    config: Arc<AedbConfig>,
    coordinator_locks: Arc<CoordinatorLockManager>,
    parallel_runtime: Arc<ParallelApplyRuntime>,
    adaptive_epoch: AdaptiveEpochState,
    global_unique_index: GlobalUniqueIndexState,
    idempotency: HashMap<IdempotencyKey, IdempotencyRecord>,
    version_store: VersionStore,
    last_full_snapshot_micros: u64,
    last_memory_estimate_micros: u64,
}

struct CommitPhaseLogSettings {
    enabled: bool,
    threshold_ms: u64,
    sample_every: u64,
}

static COMMIT_PHASE_LOG_SETTINGS: LazyLock<CommitPhaseLogSettings> = LazyLock::new(|| {
    let enabled = std::env::var("AEDB_COMMIT_PHASE_LOG_ENABLED")
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
    let threshold_ms = std::env::var("AEDB_COMMIT_PHASE_LOG_THRESHOLD_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(5);
    let sample_every = std::env::var("AEDB_COMMIT_PHASE_LOG_SAMPLE_EVERY")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(1)
        .max(1);
    CommitPhaseLogSettings {
        enabled,
        threshold_ms,
        sample_every,
    }
});

static COMMIT_PHASE_LOG_SAMPLE_COUNTER: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));

fn should_log_commit_phase(total_elapsed_ms: u64) -> bool {
    let settings = &*COMMIT_PHASE_LOG_SETTINGS;
    if !settings.enabled || total_elapsed_ms < settings.threshold_ms {
        return false;
    }
    let n = COMMIT_PHASE_LOG_SAMPLE_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
    n.is_multiple_of(settings.sample_every)
}

#[derive(Clone)]
pub struct CommitExecutor {
    ingress_txs: Vec<tokio_mpsc::Sender<CommitRequest>>,
    config: Arc<AedbConfig>,
    state: Arc<Mutex<ExecutorState>>,
    latest_snapshot_view: Arc<RwLock<SnapshotReadView>>,
    latest_snapshot_generation: Arc<AtomicU64>,
    cached_snapshot_generation: Arc<AtomicU64>,
    durable_notify: Arc<Notify>,
    current_seq: Arc<AtomicU64>,
    visible_head_seq: Arc<AtomicU64>,
    durable_head_seq: Arc<AtomicU64>,
    start_instant: Instant,
    last_wal_sync_elapsed_us: Arc<AtomicU64>,
    last_full_snapshot_micros: Arc<AtomicU64>,
    queued_bytes: Arc<AtomicUsize>,
    telemetry: Arc<ExecutorTelemetry>,
    background_tasks: Arc<StdMutex<Vec<JoinHandle<()>>>>,
}

#[derive(Debug, Default)]
struct ExecutorTelemetry {
    inflight_commits: AtomicUsize,
    queued_commits: AtomicUsize,
    commits_total: AtomicU64,
    commit_errors: AtomicU64,
    permission_rejections: AtomicU64,
    validation_rejections: AtomicU64,
    queue_full_rejections: AtomicU64,
    timeout_rejections: AtomicU64,
    conflict_rejections: AtomicU64,
    total_latency_micros: AtomicU64,
    epochs_total: AtomicU64,
    epoch_failures: AtomicU64,
    coordinator_lock_timeouts: AtomicU64,
    parallel_apply_timeouts: AtomicU64,
    read_set_conflicts: AtomicU64,
    coordinator_apply_attempts: AtomicU64,
    coordinator_apply_micros: AtomicU64,
    wal_append_ops: AtomicU64,
    wal_append_bytes: AtomicU64,
    wal_append_micros: AtomicU64,
    wal_sync_ops: AtomicU64,
    wal_sync_micros: AtomicU64,
    prestage_validate_ops: AtomicU64,
    prestage_validate_micros: AtomicU64,
    epoch_process_ops: AtomicU64,
    epoch_process_micros: AtomicU64,
    parallel_runtime_queue_depth: AtomicUsize,
    adaptive_epoch_min_commits: AtomicUsize,
    adaptive_epoch_max_wait_us: AtomicU64,
    global_unique_index_entries: AtomicUsize,
    group_commit_filling_epochs: AtomicU64,
    group_commit_flushing_epochs: AtomicU64,
    group_commit_complete_epochs: AtomicU64,
    group_commit_flush_reason_max_group_size: AtomicU64,
    group_commit_flush_reason_max_group_delay: AtomicU64,
    group_commit_flush_reason_ingress_drained: AtomicU64,
    group_commit_flush_reason_structural_barrier: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutorMetrics {
    pub inflight_commits: usize,
    pub queued_commits: usize,
    pub queued_bytes: usize,
    pub commits_total: u64,
    pub commit_errors: u64,
    pub permission_rejections: u64,
    pub validation_rejections: u64,
    pub queue_full_rejections: u64,
    pub timeout_rejections: u64,
    pub conflict_rejections: u64,
    pub avg_commit_latency_micros: u64,
    pub epochs_total: u64,
    pub epoch_failures: u64,
    pub coordinator_lock_timeouts: u64,
    pub parallel_apply_timeouts: u64,
    pub read_set_conflicts: u64,
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
    pub parallel_runtime_queue_depth: usize,
    pub adaptive_epoch_min_commits: usize,
    pub adaptive_epoch_max_wait_us: u64,
    pub global_unique_index_entries: usize,
    pub group_commit_filling_epochs: u64,
    pub group_commit_flushing_epochs: u64,
    pub group_commit_complete_epochs: u64,
    pub group_commit_flush_reason_max_group_size: u64,
    pub group_commit_flush_reason_max_group_delay: u64,
    pub group_commit_flush_reason_ingress_drained: u64,
    pub group_commit_flush_reason_structural_barrier: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutorRuntimeState {
    pub current_seq: u64,
    pub visible_head_seq: u64,
    pub durable_head_seq: u64,
    pub last_full_snapshot_micros: u64,
}

impl CommitExecutor {
    fn snapshot_view_from_state(state: &ExecutorState) -> SnapshotReadView {
        SnapshotReadView {
            keyspace: Arc::new(state.keyspace.snapshot()),
            catalog: Arc::new(state.catalog.snapshot()),
            seq: state.visible_head_seq,
        }
    }

    fn catalog_requires_post_apply_refresh(catalog: &Catalog) -> bool {
        !catalog.async_indexes.is_empty()
            || !catalog.kv_projections.is_empty()
            || !catalog.accumulators.is_empty()
    }

    pub fn new(wal_dir: &Path) -> Result<Self, AedbError> {
        let config = AedbConfig::default();
        Self::with_state(
            wal_dir,
            Keyspace::with_backend(config.primary_index_backend),
            Catalog::default(),
            0,
            1,
            config,
            HashMap::new(),
        )
    }

    pub fn with_state(
        wal_dir: &Path,
        keyspace: Keyspace,
        catalog: Catalog,
        current_seq: u64,
        next_segment_seq: u64,
        config: AedbConfig,
        idempotency: HashMap<IdempotencyKey, IdempotencyRecord>,
    ) -> Result<Self, AedbError> {
        let max_inflight_commits = config.max_inflight_commits;
        let prestage_shards = config.prestage_shards.max(1);
        let validation_catalog = Arc::new(RwLock::new(catalog.clone()));
        let coordinator_locks = Arc::new(CoordinatorLockManager::default());
        let parallel_runtime = Arc::new(ParallelApplyRuntime::new(config.parallel_worker_threads));
        let adaptive_epoch = AdaptiveEpochState::from_config(&config);
        let initial_epoch_min_commits = adaptive_epoch.current_min_commits();
        let initial_epoch_max_wait_us = adaptive_epoch.current_max_wait_us();
        let global_unique_index = GlobalUniqueIndexState::from_snapshot(&catalog, &keyspace)?;
        let initial_post_apply_refresh_needed = Self::catalog_requires_post_apply_refresh(&catalog);
        let mut version_store = VersionStore::new(
            config.max_versions,
            config.version_store_full_snapshot_interval_deltas,
            config.min_version_age_ms,
        );
        version_store.bootstrap(current_seq, keyspace.snapshot(), catalog.snapshot());
        let initial_latest_snapshot_view = version_store.acquire_latest()?.into_view();
        let mut wal = SegmentManager::new(
            wal_dir,
            SegmentConfig {
                max_segment_bytes: config.max_segment_bytes,
                max_segment_age: std::time::Duration::from_secs(config.max_segment_age_secs),
            },
            1,
        );
        wal.open_active(next_segment_seq)
            .map_err(|e| AedbError::Io(std::io::Error::other(e.to_string())))?;

        let config = Arc::new(config);
        let initial_last_full_snapshot_micros = now_micros();
        let state = Arc::new(Mutex::new(ExecutorState {
            keyspace,
            catalog,
            wal,
            current_seq,
            visible_head_seq: current_seq,
            durable_head_seq: current_seq,
            pending_batch_bytes: 0,
            pending_batch_max_seq: current_seq,
            config: Arc::clone(&config),
            coordinator_locks,
            parallel_runtime,
            adaptive_epoch,
            global_unique_index,
            idempotency,
            version_store,
            last_full_snapshot_micros: initial_last_full_snapshot_micros,
            last_memory_estimate_micros: 0,
        }));
        let latest_snapshot_view = Arc::new(RwLock::new(initial_latest_snapshot_view));
        let (apply_tx, mut rx) = tokio_mpsc::channel::<CommitRequest>(max_inflight_commits);
        let queued_bytes = Arc::new(AtomicUsize::new(0));
        let telemetry = Arc::new(ExecutorTelemetry::default());
        let durable_notify = Arc::new(Notify::new());
        let current_seq_atomic = Arc::new(AtomicU64::new(current_seq));
        let visible_head_seq = Arc::new(AtomicU64::new(current_seq));
        let durable_head_seq = Arc::new(AtomicU64::new(current_seq));
        let start_instant = Instant::now();
        let last_wal_sync_elapsed_us = Arc::new(AtomicU64::new(0));
        let last_full_snapshot_micros = Arc::new(AtomicU64::new(initial_last_full_snapshot_micros));
        let latest_snapshot_generation = Arc::new(AtomicU64::new(0));
        let cached_snapshot_generation = Arc::new(AtomicU64::new(0));
        let post_apply_refresh_needed =
            Arc::new(AtomicBool::new(initial_post_apply_refresh_needed));
        let background_tasks = Arc::new(StdMutex::new(Vec::new()));
        telemetry
            .adaptive_epoch_min_commits
            .store(initial_epoch_min_commits, Ordering::Relaxed);
        telemetry
            .adaptive_epoch_max_wait_us
            .store(initial_epoch_max_wait_us, Ordering::Relaxed);

        let mut post_apply_txs = Vec::with_capacity(prestage_shards);
        for _ in 0..prestage_shards {
            let (post_tx, mut post_rx) =
                tokio_mpsc::channel::<PostApplyTask>(max_inflight_commits * 2);
            post_apply_txs.push(post_tx);
            let post_state = Arc::clone(&state);
            let post_latest_snapshot_generation = Arc::clone(&latest_snapshot_generation);
            let handle = tokio::spawn(async move {
                while let Some(task) = post_rx.recv().await {
                    let mut s = post_state.lock().await;
                    if let Ok(changed) = refresh_async_indexes(&mut s, Some(task.delta.as_ref()))
                        && changed
                    {
                        let seq = s.visible_head_seq.max(task.delta.seq);
                        let keyspace_snapshot = s.keyspace.snapshot();
                        let catalog_snapshot = s.catalog.snapshot();
                        s.version_store
                            .publish_full(seq, keyspace_snapshot, catalog_snapshot);
                        post_latest_snapshot_generation.fetch_add(1, Ordering::Release);
                    }
                }
            });
            background_tasks
                .lock()
                .expect("background task list poisoned")
                .push(handle);
        }

        let loop_state = Arc::clone(&state);
        let loop_config = Arc::clone(&config);
        let loop_validation_catalog = Arc::clone(&validation_catalog);
        let loop_post_txs = post_apply_txs.clone();
        let queue_counter = Arc::clone(&queued_bytes);
        let loop_telemetry = Arc::clone(&telemetry);
        let loop_durable_notify = Arc::clone(&durable_notify);
        let loop_current_seq = Arc::clone(&current_seq_atomic);
        let loop_visible_head = Arc::clone(&visible_head_seq);
        let loop_durable_head = Arc::clone(&durable_head_seq);
        let loop_last_wal_sync_elapsed_us = Arc::clone(&last_wal_sync_elapsed_us);
        let loop_start_instant = start_instant;
        let loop_last_full_snapshot_micros = Arc::clone(&last_full_snapshot_micros);
        let loop_latest_snapshot_generation = Arc::clone(&latest_snapshot_generation);
        let loop_post_apply_refresh_needed = Arc::clone(&post_apply_refresh_needed);
        let apply_handle = tokio::spawn(async move {
            let mut pending = VecDeque::new();
            let mut ingress_closed = false;
            let mut group_commit = GroupCommitStateMachine::new(GroupCommitPolicy {
                max_group_size: 1,
                max_group_delay: Duration::from_micros(1),
            });

            loop {
                if pending.is_empty() && !ingress_closed {
                    match rx.recv().await {
                        Some(req) => pending.push_back(req),
                        None => ingress_closed = true,
                    }
                }
                while let Ok(req) = rx.try_recv() {
                    pending.push_back(req);
                }
                if pending.is_empty() {
                    break;
                }

                let (max_wait_us, min_commits, max_commits) = {
                    let s = loop_state.lock().await;
                    s.adaptive_epoch.epoch_params(&loop_config, pending.len())
                };
                group_commit.begin_filling(
                    Instant::now(),
                    GroupCommitPolicy {
                        max_group_size: max_commits.max(1),
                        max_group_delay: Duration::from_micros(max_wait_us.max(1)),
                    },
                );
                loop_telemetry
                    .group_commit_filling_epochs
                    .fetch_add(1, Ordering::Relaxed);
                let deadline = Instant::now() + Duration::from_micros(max_wait_us);
                let epoch_requests = build_epoch_requests(
                    &mut pending,
                    min_commits,
                    max_commits,
                    deadline,
                    &mut rx,
                    ingress_closed,
                )
                .await;
                if epoch_requests.is_empty() {
                    continue;
                }
                group_commit.record_pending_commits(epoch_requests.len());
                let flush_reason = if group_commit.reached_size_limit() {
                    GroupCommitFlushReason::MaxGroupSize
                } else if group_commit.reached_delay_limit(Instant::now()) {
                    GroupCommitFlushReason::MaxGroupDelay
                } else if pending.is_empty() || ingress_closed {
                    GroupCommitFlushReason::IngressDrained
                } else {
                    GroupCommitFlushReason::StructuralBarrier
                };
                group_commit.begin_flushing(flush_reason);
                loop_telemetry
                    .group_commit_flushing_epochs
                    .fetch_add(1, Ordering::Relaxed);
                match flush_reason {
                    GroupCommitFlushReason::MaxGroupSize => {
                        loop_telemetry
                            .group_commit_flush_reason_max_group_size
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    GroupCommitFlushReason::MaxGroupDelay => {
                        loop_telemetry
                            .group_commit_flush_reason_max_group_delay
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    GroupCommitFlushReason::IngressDrained => {
                        loop_telemetry
                            .group_commit_flush_reason_ingress_drained
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    GroupCommitFlushReason::StructuralBarrier => {
                        loop_telemetry
                            .group_commit_flush_reason_structural_barrier
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                debug_assert!(matches!(
                    group_commit.snapshot().phase,
                    GroupCommitPhase::Flushing
                ));

                let mut s = loop_state.lock().await;
                let durable_before = s.durable_head_seq;
                let epoch_started = Instant::now();
                let epoch_result = process_commit_epoch(&mut s, epoch_requests);
                group_commit.complete_flush();
                loop_telemetry
                    .group_commit_complete_epochs
                    .fetch_add(1, Ordering::Relaxed);
                debug_assert!(matches!(
                    group_commit.snapshot().phase,
                    GroupCommitPhase::Complete
                ));
                loop_telemetry
                    .epoch_process_ops
                    .fetch_add(1, Ordering::Relaxed);
                loop_telemetry.epoch_process_micros.fetch_add(
                    epoch_started.elapsed().as_micros() as u64,
                    Ordering::Relaxed,
                );
                let catalog_changed = epoch_result.catalog_changed;
                let outcomes = epoch_result.outcomes;
                let epoch_commit_count = outcomes.len();
                let had_error = outcomes.iter().any(|o| o.result.is_err());
                let epoch_elapsed_micros = epoch_started.elapsed().as_micros() as u64;
                s.adaptive_epoch.observe_epoch(
                    &loop_config,
                    outcomes.len(),
                    epoch_started.elapsed(),
                    had_error,
                    pending.len(),
                );
                loop_telemetry
                    .adaptive_epoch_min_commits
                    .store(s.adaptive_epoch.current_min_commits(), Ordering::Relaxed);
                loop_telemetry
                    .adaptive_epoch_max_wait_us
                    .store(s.adaptive_epoch.current_max_wait_us(), Ordering::Relaxed);
                loop_telemetry
                    .parallel_runtime_queue_depth
                    .store(s.parallel_runtime.queued_tasks(), Ordering::Relaxed);
                loop_telemetry
                    .global_unique_index_entries
                    .store(s.global_unique_index.total_entries(), Ordering::Relaxed);
                loop_telemetry.epochs_total.fetch_add(1, Ordering::Relaxed);
                loop_telemetry
                    .read_set_conflicts
                    .fetch_add(epoch_result.read_set_conflicts, Ordering::Relaxed);
                loop_telemetry
                    .coordinator_apply_attempts
                    .fetch_add(epoch_result.coordinator_apply_attempts, Ordering::Relaxed);
                loop_telemetry
                    .coordinator_apply_micros
                    .fetch_add(epoch_result.coordinator_apply_micros, Ordering::Relaxed);
                loop_telemetry
                    .wal_append_ops
                    .fetch_add(epoch_result.wal_append_ops, Ordering::Relaxed);
                loop_telemetry
                    .wal_append_bytes
                    .fetch_add(epoch_result.wal_append_bytes, Ordering::Relaxed);
                loop_telemetry
                    .wal_append_micros
                    .fetch_add(epoch_result.wal_append_micros, Ordering::Relaxed);
                loop_telemetry
                    .wal_sync_ops
                    .fetch_add(epoch_result.wal_sync_ops, Ordering::Relaxed);
                loop_telemetry
                    .wal_sync_micros
                    .fetch_add(epoch_result.wal_sync_micros, Ordering::Relaxed);
                if epoch_result.sync_executed {
                    let elapsed = loop_start_instant.elapsed().as_micros() as u64;
                    loop_last_wal_sync_elapsed_us
                        .store(elapsed.saturating_add(1), Ordering::Relaxed);
                }
                if had_error {
                    loop_telemetry
                        .epoch_failures
                        .fetch_add(1, Ordering::Relaxed);
                }
                if catalog_changed {
                    *loop_validation_catalog.write() = s.catalog.clone();
                    loop_post_apply_refresh_needed.store(
                        CommitExecutor::catalog_requires_post_apply_refresh(&s.catalog),
                        Ordering::Release,
                    );
                }
                loop_current_seq.store(s.current_seq, Ordering::Release);
                loop_visible_head.store(s.visible_head_seq, Ordering::Release);
                loop_durable_head.store(s.durable_head_seq, Ordering::Release);
                loop_last_full_snapshot_micros
                    .store(s.last_full_snapshot_micros, Ordering::Release);
                loop_latest_snapshot_generation.fetch_add(1, Ordering::Release);
                let durable_advanced = s.durable_head_seq > durable_before;
                drop(s);
                if durable_advanced {
                    loop_durable_notify.notify_waiters();
                }

                for outcome in outcomes {
                    if loop_post_apply_refresh_needed.load(Ordering::Acquire)
                        && let Some(delta) = outcome.post_apply_delta
                    {
                        let post_shard =
                            shard_for_envelope(&outcome.request.envelope, loop_post_txs.len());
                        if let Some(tx) = loop_post_txs.get(post_shard) {
                            let _ = tx.try_send(PostApplyTask { delta });
                        }
                    }
                    let elapsed_micros =
                        now_micros().saturating_sub(outcome.request.enqueue_micros);
                    let elapsed_ms = elapsed_micros / 1_000;
                    if should_log_commit_phase(elapsed_ms) {
                        let queue_wait_micros = elapsed_micros.saturating_sub(epoch_elapsed_micros);
                        info!(
                            target: "aedb.commit_phase",
                            total_us = elapsed_micros,
                            total_ms = elapsed_ms,
                            queue_wait_us = queue_wait_micros,
                            queue_wait_ms = queue_wait_micros / 1_000,
                            epoch_us = epoch_elapsed_micros,
                            epoch_ms = epoch_elapsed_micros / 1_000,
                            pre_wal_us = epoch_result.pre_wal_micros,
                            pre_wal_ms = epoch_result.pre_wal_micros / 1_000,
                            coordinator_apply_us = epoch_result.coordinator_apply_micros,
                            coordinator_apply_ms = epoch_result.coordinator_apply_micros / 1_000,
                            parallel_apply_us = epoch_result.parallel_apply_micros,
                            parallel_apply_ms = epoch_result.parallel_apply_micros / 1_000,
                            wal_append_us = epoch_result.wal_append_micros,
                            wal_append_ms = epoch_result.wal_append_micros / 1_000,
                            wal_sync_us = epoch_result.wal_sync_micros,
                            wal_sync_ms = epoch_result.wal_sync_micros / 1_000,
                            finalize_us = epoch_result.finalize_micros,
                            finalize_ms = epoch_result.finalize_micros / 1_000,
                            epoch_commit_count,
                            epoch_wal_append_ops = epoch_result.wal_append_ops,
                            epoch_wal_sync_ops = epoch_result.wal_sync_ops,
                            mutation_count = outcome.request.mutation_count,
                            write_class = ?outcome.request.envelope.write_class,
                            prevalidated = outcome.request.prevalidated,
                            has_error = outcome.result.is_err(),
                            "aedb commit phase timing"
                        );
                    }
                    loop_telemetry
                        .total_latency_micros
                        .fetch_add(elapsed_micros, Ordering::Relaxed);
                    loop_telemetry.commits_total.fetch_add(1, Ordering::Relaxed);
                    if outcome.result.is_err() {
                        loop_telemetry.commit_errors.fetch_add(1, Ordering::Relaxed);
                        if let Err(err) = &outcome.result {
                            if is_permission_rejection_error(err) {
                                loop_telemetry
                                    .permission_rejections
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                            if is_validation_rejection_error(err) {
                                loop_telemetry
                                    .validation_rejections
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                            if is_conflict_rejection_error(err) {
                                loop_telemetry
                                    .conflict_rejections
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                            if is_coordinator_timeout_error(err) {
                                loop_telemetry
                                    .coordinator_lock_timeouts
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                            if is_parallel_apply_timeout_error(err) {
                                loop_telemetry
                                    .parallel_apply_timeouts
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    queue_counter.fetch_sub(outcome.request.encoded_len, Ordering::Relaxed);
                    loop_telemetry
                        .queued_commits
                        .fetch_sub(1, Ordering::Relaxed);
                    loop_telemetry
                        .inflight_commits
                        .fetch_sub(1, Ordering::Relaxed);
                    let _ = outcome.request.result_tx.send(outcome.result);
                }
            }

            for req in pending {
                queue_counter.fetch_sub(req.encoded_len, Ordering::Relaxed);
                loop_telemetry
                    .queued_commits
                    .fetch_sub(1, Ordering::Relaxed);
                loop_telemetry
                    .inflight_commits
                    .fetch_sub(1, Ordering::Relaxed);
                loop_telemetry.commits_total.fetch_add(1, Ordering::Relaxed);
                loop_telemetry.commit_errors.fetch_add(1, Ordering::Relaxed);
                let _ = req.result_tx.send(Err(AedbError::Validation(
                    "commit apply queue closed".into(),
                )));
            }
        });
        background_tasks
            .lock()
            .expect("background task list poisoned")
            .push(apply_handle);

        let mut ingress_txs = Vec::with_capacity(prestage_shards);
        for _ in 0..prestage_shards {
            let (ingress_tx, mut ingress_rx) =
                tokio_mpsc::channel::<CommitRequest>(max_inflight_commits);
            ingress_txs.push(ingress_tx);
            let pre_validation_catalog = Arc::clone(&validation_catalog);
            let pre_apply_tx = apply_tx.clone();
            let pre_queue_counter = Arc::clone(&queued_bytes);
            let pre_telemetry = Arc::clone(&telemetry);
            let pre_config = Arc::clone(&config);
            let handle = tokio::spawn(async move {
                while let Some(mut req) = ingress_rx.recv().await {
                    let mut prevalidate_elapsed_us = None;
                    let (write_partitions, read_partitions) = if req.prevalidated {
                        let write_partitions = if req
                            .envelope
                            .write_intent
                            .mutations
                            .iter()
                            .any(mutation_requires_fk_expansion)
                        {
                            let catalog = pre_validation_catalog.read();
                            derive_write_partitions_for_envelope(
                                &catalog,
                                &req.envelope.write_intent.mutations,
                            )
                        } else if let [mutation] = req.envelope.write_intent.mutations.as_slice() {
                            if let Some(token) = single_write_partition_token(mutation) {
                                let mut out = HashSet::with_capacity(1);
                                out.insert(token);
                                out
                            } else {
                                derive_write_partitions(&req.envelope.write_intent.mutations)
                            }
                        } else {
                            derive_write_partitions(&req.envelope.write_intent.mutations)
                        };
                        (write_partitions, derive_read_partitions(&req.envelope))
                    } else {
                        let prevalidate_started = Instant::now();
                        let result = pre_stage_validate(
                            &pre_validation_catalog,
                            &req.envelope,
                            pre_config.as_ref(),
                        );
                        prevalidate_elapsed_us =
                            Some(prevalidate_started.elapsed().as_micros() as u64);
                        match result {
                            Ok(partitions) => partitions,
                            Err(e) => {
                                pre_telemetry.commit_errors.fetch_add(1, Ordering::Relaxed);
                                if is_permission_rejection_error(&e) {
                                    pre_telemetry
                                        .permission_rejections
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                                if is_validation_rejection_error(&e) {
                                    pre_telemetry
                                        .validation_rejections
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                                pre_queue_counter.fetch_sub(req.encoded_len, Ordering::Relaxed);
                                pre_telemetry.queued_commits.fetch_sub(1, Ordering::Relaxed);
                                let _ = req.result_tx.send(Err(e));
                                continue;
                            }
                        }
                    };
                    if let Some(elapsed_us) = prevalidate_elapsed_us {
                        pre_telemetry
                            .prestage_validate_ops
                            .fetch_add(1, Ordering::Relaxed);
                        pre_telemetry
                            .prestage_validate_micros
                            .fetch_add(elapsed_us, Ordering::Relaxed);
                    }
                    if write_partitions.is_empty() {
                        pre_telemetry.commit_errors.fetch_add(1, Ordering::Relaxed);
                        pre_queue_counter.fetch_sub(req.encoded_len, Ordering::Relaxed);
                        pre_telemetry.queued_commits.fetch_sub(1, Ordering::Relaxed);
                        let _ = req.result_tx.send(Err(AedbError::Validation(
                            "transaction envelope has no mutations".into(),
                        )));
                        continue;
                    }
                    req.write_partitions = write_partitions;
                    req.read_partitions = read_partitions;
                    pre_telemetry
                        .inflight_commits
                        .fetch_add(1, Ordering::Relaxed);
                    if let Err(send_err) = pre_apply_tx.send(req).await {
                        let req = send_err.0;
                        pre_telemetry
                            .inflight_commits
                            .fetch_sub(1, Ordering::Relaxed);
                        pre_queue_counter.fetch_sub(req.encoded_len, Ordering::Relaxed);
                        pre_telemetry.queued_commits.fetch_sub(1, Ordering::Relaxed);
                        let _ = req.result_tx.send(Err(AedbError::Validation(
                            "commit apply queue closed".into(),
                        )));
                    }
                }
            });
            background_tasks
                .lock()
                .expect("background task list poisoned")
                .push(handle);
        }

        let flush_state = Arc::clone(&state);
        let flush_config = Arc::clone(&config);
        let flush_telemetry = Arc::clone(&telemetry);
        let flush_durable_notify = Arc::clone(&durable_notify);
        let flush_durable_head = Arc::clone(&durable_head_seq);
        let flush_last_wal_sync_elapsed_us = Arc::clone(&last_wal_sync_elapsed_us);
        let flush_start_instant = start_instant;
        let flush_handle = tokio::spawn(async move {
            loop {
                let sleep_ms = flush_config.batch_interval_ms;
                tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                let mut s = flush_state.lock().await;
                if flush_config.durability_mode != DurabilityMode::Batch {
                    continue;
                }
                if s.pending_batch_bytes > 0 {
                    let sync_started = Instant::now();
                    if s.wal.sync_active().is_ok() {
                        let sync_us = sync_started.elapsed().as_micros() as u64;
                        s.durable_head_seq = s.pending_batch_max_seq.max(s.durable_head_seq);
                        s.pending_batch_bytes = 0;
                        s.pending_batch_max_seq = s.durable_head_seq;
                        flush_durable_head.store(s.durable_head_seq, Ordering::Release);
                        flush_telemetry.wal_sync_ops.fetch_add(1, Ordering::Relaxed);
                        flush_telemetry
                            .wal_sync_micros
                            .fetch_add(sync_us, Ordering::Relaxed);
                        let elapsed = flush_start_instant.elapsed().as_micros() as u64;
                        flush_last_wal_sync_elapsed_us
                            .store(elapsed.saturating_add(1), Ordering::Relaxed);
                        flush_durable_notify.notify_waiters();
                    }
                }
                prune_idempotency(&mut s);
            }
        });
        background_tasks
            .lock()
            .expect("background task list poisoned")
            .push(flush_handle);

        let projection_state = Arc::clone(&state);
        let gc_config = Arc::clone(&config);
        let gc_handle = tokio::spawn(async move {
            loop {
                let sleep_ms = gc_config.version_gc_interval_ms.max(1);
                tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                let mut s = projection_state.lock().await;
                s.version_store.gc();
            }
        });
        background_tasks
            .lock()
            .expect("background task list poisoned")
            .push(gc_handle);

        Ok(Self {
            ingress_txs,
            config,
            state,
            latest_snapshot_view,
            latest_snapshot_generation,
            cached_snapshot_generation,
            durable_notify,
            current_seq: current_seq_atomic,
            visible_head_seq,
            durable_head_seq,
            start_instant,
            last_wal_sync_elapsed_us,
            last_full_snapshot_micros,
            queued_bytes,
            telemetry,
            background_tasks,
        })
    }

    pub async fn submit(&self, mutation: Mutation) -> Result<CommitResult, AedbError> {
        let fast_size_hint = estimate_single_mutation_size_upper_bound(&mutation);
        self.submit_envelope_with_mode_size_hint(
            TransactionEnvelope {
                caller: None,
                idempotency_key: None,
                write_class: WriteClass::Standard,
                assertions: Vec::new(),
                read_set: Default::default(),
                write_intent: WriteIntent {
                    mutations: vec![mutation],
                },
                base_seq: 0,
            },
            false,
            false,
            fast_size_hint,
        )
        .await
    }

    pub(crate) async fn submit_prevalidated(
        &self,
        mutation: Mutation,
    ) -> Result<CommitResult, AedbError> {
        let fast_size_hint = estimate_prevalidated_single_mutation_size_upper_bound(&mutation);
        self.submit_envelope_with_mode_size_hint(
            TransactionEnvelope {
                caller: None,
                idempotency_key: None,
                write_class: WriteClass::Standard,
                assertions: Vec::new(),
                read_set: Default::default(),
                write_intent: WriteIntent {
                    mutations: vec![mutation],
                },
                base_seq: 0,
            },
            true,
            false,
            fast_size_hint,
        )
        .await
    }

    pub async fn submit_as(
        &self,
        caller: Option<CallerContext>,
        mutation: Mutation,
    ) -> Result<CommitResult, AedbError> {
        let fast_size_hint = caller
            .is_none()
            .then(|| estimate_single_mutation_size_upper_bound(&mutation))
            .flatten();
        self.submit_envelope_with_mode_size_hint(
            TransactionEnvelope {
                caller,
                idempotency_key: None,
                write_class: WriteClass::Standard,
                assertions: Vec::new(),
                read_set: Default::default(),
                write_intent: WriteIntent {
                    mutations: vec![mutation],
                },
                // No read set/assertions in single-mutation submit path.
                // A fixed base_seq avoids an extra state lock on the hot write path.
                base_seq: 0,
            },
            false,
            false,
            fast_size_hint,
        )
        .await
    }

    pub async fn submit_envelope(
        &self,
        envelope: TransactionEnvelope,
    ) -> Result<CommitResult, AedbError> {
        self.submit_envelope_with_mode(envelope, false, false).await
    }

    pub(crate) async fn submit_envelope_prevalidated(
        &self,
        envelope: TransactionEnvelope,
    ) -> Result<CommitResult, AedbError> {
        self.submit_envelope_with_mode(envelope, true, false).await
    }

    #[cfg(test)]
    pub(crate) async fn submit_envelope_trusted(
        &self,
        envelope: TransactionEnvelope,
    ) -> Result<CommitResult, AedbError> {
        if !envelope
            .caller
            .as_ref()
            .is_some_and(CallerContext::is_internal_system)
        {
            return Err(AedbError::PermissionDenied(
                "trusted commit requires caller_id=system".into(),
            ));
        }
        self.submit_envelope_with_mode(envelope, true, false).await
    }

    #[cfg(test)]
    pub(crate) async fn submit_envelope_trusted_engine_verified(
        &self,
        envelope: TransactionEnvelope,
    ) -> Result<CommitResult, AedbError> {
        if !envelope
            .caller
            .as_ref()
            .is_some_and(CallerContext::is_internal_system)
        {
            return Err(AedbError::PermissionDenied(
                "trusted commit requires caller_id=system".into(),
            ));
        }
        self.submit_envelope_with_mode(envelope, true, true).await
    }

    async fn submit_envelope_with_mode(
        &self,
        envelope: TransactionEnvelope,
        prevalidated: bool,
        assertions_engine_verified: bool,
    ) -> Result<CommitResult, AedbError> {
        let encoded_size_hint = estimate_transaction_envelope_size_upper_bound(&envelope);
        self.submit_envelope_with_mode_size_hint(
            envelope,
            prevalidated,
            assertions_engine_verified,
            encoded_size_hint,
        )
        .await
    }

    async fn submit_envelope_with_mode_size_hint(
        &self,
        envelope: TransactionEnvelope,
        prevalidated: bool,
        assertions_engine_verified: bool,
        encoded_size_hint: Option<usize>,
    ) -> Result<CommitResult, AedbError> {
        let config = &self.config;
        let encoded_size_bytes = if let Some(upper_bound) = encoded_size_hint {
            if upper_bound <= config.max_transaction_bytes {
                upper_bound
            } else {
                // Hint exceeded max bytes; compute exact size to avoid false rejections.
                let mut counter = CountingWriter::default();
                rmp_serde::encode::write(&mut counter, &envelope)
                    .map_err(|e| AedbError::Encode(e.to_string()))?;
                counter.len
            }
        } else {
            // Compute exact encoded byte length without allocating a transient Vec<u8>.
            let mut counter = CountingWriter::default();
            rmp_serde::encode::write(&mut counter, &envelope)
                .map_err(|e| AedbError::Encode(e.to_string()))?;
            counter.len
        };
        if encoded_size_bytes > config.max_transaction_bytes {
            return Err(AedbError::Validation(
                "transaction exceeds max_transaction_bytes".into(),
            ));
        }
        let mut current = self.queued_bytes.load(Ordering::Relaxed);
        loop {
            let next_queued_size_bytes = current.saturating_add(encoded_size_bytes);
            if next_queued_size_bytes > config.max_commit_queue_bytes {
                self.telemetry
                    .queue_full_rejections
                    .fetch_add(1, Ordering::Relaxed);
                return Err(AedbError::QueueFull);
            }
            match self.queued_bytes.compare_exchange_weak(
                current,
                next_queued_size_bytes,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
        self.telemetry
            .queued_commits
            .fetch_add(1, Ordering::Relaxed);
        let (result_tx, result_rx) = oneshot::channel();
        let shard = shard_for_envelope(&envelope, self.ingress_txs.len());
        let ingress_tx = self
            .ingress_txs
            .get(shard)
            .ok_or_else(|| AedbError::Validation("no ingress shard available".into()))?
            .clone();
        let request = CommitRequest {
            mutation_count: envelope.write_intent.mutations.len(),
            envelope,
            encoded_len: encoded_size_bytes,
            enqueue_micros: now_micros(),
            prevalidated,
            assertions_engine_verified,
            write_partitions: HashSet::new(),
            read_partitions: HashSet::new(),
            defer_count: 0,
            result_tx,
        };
        match ingress_tx.try_send(request) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(request)) => {
                let send_result = tokio::time::timeout(
                    std::time::Duration::from_millis(config.commit_timeout_ms),
                    ingress_tx.send(request),
                )
                .await;
                match send_result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        self.queued_bytes
                            .fetch_sub(encoded_size_bytes, Ordering::Relaxed);
                        self.telemetry
                            .queued_commits
                            .fetch_sub(1, Ordering::Relaxed);
                        return Err(AedbError::Validation(format!("commit queue closed: {e}")));
                    }
                    Err(_) => {
                        self.queued_bytes
                            .fetch_sub(encoded_size_bytes, Ordering::Relaxed);
                        self.telemetry
                            .queued_commits
                            .fetch_sub(1, Ordering::Relaxed);
                        self.telemetry
                            .timeout_rejections
                            .fetch_add(1, Ordering::Relaxed);
                        return Err(AedbError::Timeout);
                    }
                }
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_request)) => {
                self.queued_bytes
                    .fetch_sub(encoded_size_bytes, Ordering::Relaxed);
                self.telemetry
                    .queued_commits
                    .fetch_sub(1, Ordering::Relaxed);
                return Err(AedbError::Validation("commit queue closed".into()));
            }
        }
        let mut result_rx = result_rx;
        match result_rx.try_recv() {
            Ok(result) => result,
            Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                match tokio::time::timeout(
                    std::time::Duration::from_millis(config.commit_timeout_ms),
                    result_rx,
                )
                .await
                {
                    Ok(Ok(result)) => result,
                    Ok(Err(e)) => Err(AedbError::Validation(format!(
                        "commit result channel closed: {e}"
                    ))),
                    Err(_) => {
                        self.telemetry
                            .timeout_rejections
                            .fetch_add(1, Ordering::Relaxed);
                        Err(AedbError::Timeout)
                    }
                }
            }
            Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                Err(AedbError::Validation("commit result channel closed".into()))
            }
        }
    }

    pub async fn current_seq(&self) -> u64 {
        self.current_seq.load(Ordering::Acquire)
    }

    pub async fn visible_head_seq(&self) -> u64 {
        self.visible_head_seq_now()
    }

    #[inline]
    pub fn visible_head_seq_now(&self) -> u64 {
        self.visible_head_seq.load(Ordering::Acquire)
    }

    pub async fn durable_head_seq(&self) -> u64 {
        self.durable_head_seq_now()
    }

    #[inline]
    pub fn durable_head_seq_now(&self) -> u64 {
        self.durable_head_seq.load(Ordering::Acquire)
    }

    pub async fn head_state(&self) -> HeadState {
        HeadState {
            visible_head_seq: self.visible_head_seq.load(Ordering::Acquire),
            durable_head_seq: self.durable_head_seq.load(Ordering::Acquire),
        }
    }

    pub async fn snapshot_state(&self) -> (KeyspaceSnapshot, Catalog, u64) {
        let state = self.state.lock().await;
        (
            state.keyspace.snapshot(),
            state.catalog.snapshot(),
            state.visible_head_seq,
        )
    }

    pub async fn snapshot_latest_view(&self) -> SnapshotReadView {
        let current_generation = self.latest_snapshot_generation.load(Ordering::Acquire);
        if self.cached_snapshot_generation.load(Ordering::Acquire) == current_generation {
            return self.latest_snapshot_view.read().clone();
        }

        let state = self.state.lock().await;
        let current_generation = self.latest_snapshot_generation.load(Ordering::Acquire);
        let view = Self::snapshot_view_from_state(&state);
        *self.latest_snapshot_view.write() = view.clone();
        self.cached_snapshot_generation
            .store(current_generation, Ordering::Release);
        view
    }

    pub async fn snapshot_at_seq(&self, seq: u64) -> Result<SnapshotReadView, AedbError> {
        let mut state = self.state.lock().await;
        let view = state.version_store.acquire_at_seq(seq)?.into_view();
        Ok(view)
    }

    pub async fn wait_for_durable(&self, seq: u64) -> Result<(), AedbError> {
        loop {
            let notified = self.durable_notify.notified();
            if self.durable_head_seq_now() >= seq {
                return Ok(());
            }
            notified.await;
        }
    }

    pub async fn force_fsync(&self) -> Result<u64, AedbError> {
        let mut s = self.state.lock().await;
        if s.pending_batch_bytes == 0 && s.durable_head_seq >= s.visible_head_seq {
            return Ok(s.durable_head_seq);
        }
        let durable_before = s.durable_head_seq;
        let sync_started = Instant::now();
        s.wal
            .sync_active()
            .map_err(|e| AedbError::Io(std::io::Error::other(e.to_string())))?;
        let sync_us = sync_started.elapsed().as_micros() as u64;
        s.durable_head_seq = s.visible_head_seq;
        s.pending_batch_bytes = 0;
        s.pending_batch_max_seq = s.durable_head_seq;
        self.durable_head_seq
            .store(s.durable_head_seq, Ordering::Release);
        self.telemetry.wal_sync_ops.fetch_add(1, Ordering::Relaxed);
        self.telemetry
            .wal_sync_micros
            .fetch_add(sync_us, Ordering::Relaxed);
        let elapsed = self.start_instant.elapsed().as_micros() as u64;
        self.last_wal_sync_elapsed_us
            .store(elapsed.saturating_add(1), Ordering::Relaxed);
        let durable = s.durable_head_seq;
        let durable_advanced = durable > durable_before;
        drop(s);
        if durable_advanced {
            self.durable_notify.notify_waiters();
        }
        Ok(durable)
    }

    #[inline]
    pub fn last_wal_sync_age_us(&self) -> Option<u64> {
        let last_plus_one = self.last_wal_sync_elapsed_us.load(Ordering::Relaxed);
        if last_plus_one == 0 {
            return None;
        }
        let elapsed = self.start_instant.elapsed().as_micros() as u64;
        Some(elapsed.saturating_sub(last_plus_one - 1))
    }

    pub async fn idempotency_snapshot(&self) -> HashMap<IdempotencyKey, IdempotencyRecord> {
        let s = self.state.lock().await;
        s.idempotency.clone()
    }

    pub fn metrics(&self) -> ExecutorMetrics {
        let commits_total = self.telemetry.commits_total.load(Ordering::Relaxed);
        let total_latency = self.telemetry.total_latency_micros.load(Ordering::Relaxed);
        let avg_commit_latency_micros = if commits_total == 0 {
            0
        } else {
            total_latency / commits_total
        };
        let coordinator_apply_attempts = self
            .telemetry
            .coordinator_apply_attempts
            .load(Ordering::Relaxed);
        let coordinator_apply_micros = self
            .telemetry
            .coordinator_apply_micros
            .load(Ordering::Relaxed);
        let avg_coordinator_apply_micros = if coordinator_apply_attempts == 0 {
            0
        } else {
            coordinator_apply_micros / coordinator_apply_attempts
        };
        let wal_sync_ops = self.telemetry.wal_sync_ops.load(Ordering::Relaxed);
        let wal_sync_micros = self.telemetry.wal_sync_micros.load(Ordering::Relaxed);
        let wal_append_ops = self.telemetry.wal_append_ops.load(Ordering::Relaxed);
        let wal_append_bytes = self.telemetry.wal_append_bytes.load(Ordering::Relaxed);
        let wal_append_micros = self.telemetry.wal_append_micros.load(Ordering::Relaxed);
        let avg_wal_append_micros = if wal_append_ops == 0 {
            0
        } else {
            wal_append_micros / wal_append_ops
        };
        let avg_wal_sync_micros = if wal_sync_ops == 0 {
            0
        } else {
            wal_sync_micros / wal_sync_ops
        };
        let prestage_validate_ops = self.telemetry.prestage_validate_ops.load(Ordering::Relaxed);
        let prestage_validate_micros = self
            .telemetry
            .prestage_validate_micros
            .load(Ordering::Relaxed);
        let avg_prestage_validate_micros = if prestage_validate_ops == 0 {
            0
        } else {
            prestage_validate_micros / prestage_validate_ops
        };
        let epoch_process_ops = self.telemetry.epoch_process_ops.load(Ordering::Relaxed);
        let epoch_process_micros = self.telemetry.epoch_process_micros.load(Ordering::Relaxed);
        let avg_epoch_process_micros = if epoch_process_ops == 0 {
            0
        } else {
            epoch_process_micros / epoch_process_ops
        };
        ExecutorMetrics {
            inflight_commits: self.telemetry.inflight_commits.load(Ordering::Relaxed),
            queued_commits: self.telemetry.queued_commits.load(Ordering::Relaxed),
            queued_bytes: self.queued_bytes.load(Ordering::Relaxed),
            commits_total,
            commit_errors: self.telemetry.commit_errors.load(Ordering::Relaxed),
            permission_rejections: self.telemetry.permission_rejections.load(Ordering::Relaxed),
            validation_rejections: self.telemetry.validation_rejections.load(Ordering::Relaxed),
            queue_full_rejections: self.telemetry.queue_full_rejections.load(Ordering::Relaxed),
            timeout_rejections: self.telemetry.timeout_rejections.load(Ordering::Relaxed),
            conflict_rejections: self.telemetry.conflict_rejections.load(Ordering::Relaxed),
            avg_commit_latency_micros,
            epochs_total: self.telemetry.epochs_total.load(Ordering::Relaxed),
            epoch_failures: self.telemetry.epoch_failures.load(Ordering::Relaxed),
            coordinator_lock_timeouts: self
                .telemetry
                .coordinator_lock_timeouts
                .load(Ordering::Relaxed),
            parallel_apply_timeouts: self
                .telemetry
                .parallel_apply_timeouts
                .load(Ordering::Relaxed),
            read_set_conflicts: self.telemetry.read_set_conflicts.load(Ordering::Relaxed),
            coordinator_apply_attempts,
            avg_coordinator_apply_micros,
            wal_append_ops,
            wal_append_bytes,
            avg_wal_append_micros,
            wal_sync_ops,
            avg_wal_sync_micros,
            prestage_validate_ops,
            avg_prestage_validate_micros,
            epoch_process_ops,
            avg_epoch_process_micros,
            parallel_runtime_queue_depth: self
                .telemetry
                .parallel_runtime_queue_depth
                .load(Ordering::Relaxed),
            adaptive_epoch_min_commits: self
                .telemetry
                .adaptive_epoch_min_commits
                .load(Ordering::Relaxed),
            adaptive_epoch_max_wait_us: self
                .telemetry
                .adaptive_epoch_max_wait_us
                .load(Ordering::Relaxed),
            global_unique_index_entries: self
                .telemetry
                .global_unique_index_entries
                .load(Ordering::Relaxed),
            group_commit_filling_epochs: self
                .telemetry
                .group_commit_filling_epochs
                .load(Ordering::Relaxed),
            group_commit_flushing_epochs: self
                .telemetry
                .group_commit_flushing_epochs
                .load(Ordering::Relaxed),
            group_commit_complete_epochs: self
                .telemetry
                .group_commit_complete_epochs
                .load(Ordering::Relaxed),
            group_commit_flush_reason_max_group_size: self
                .telemetry
                .group_commit_flush_reason_max_group_size
                .load(Ordering::Relaxed),
            group_commit_flush_reason_max_group_delay: self
                .telemetry
                .group_commit_flush_reason_max_group_delay
                .load(Ordering::Relaxed),
            group_commit_flush_reason_ingress_drained: self
                .telemetry
                .group_commit_flush_reason_ingress_drained
                .load(Ordering::Relaxed),
            group_commit_flush_reason_structural_barrier: self
                .telemetry
                .group_commit_flush_reason_structural_barrier
                .load(Ordering::Relaxed),
        }
    }

    pub async fn runtime_state_metrics(&self) -> ExecutorRuntimeState {
        ExecutorRuntimeState {
            current_seq: self.current_seq.load(Ordering::Acquire),
            visible_head_seq: self.visible_head_seq.load(Ordering::Acquire),
            durable_head_seq: self.durable_head_seq.load(Ordering::Acquire),
            last_full_snapshot_micros: self.last_full_snapshot_micros.load(Ordering::Acquire),
        }
    }
}

impl Drop for CommitExecutor {
    fn drop(&mut self) {
        if Arc::strong_count(&self.background_tasks) != 1 {
            return;
        }
        let mut handles = self
            .background_tasks
            .lock()
            .expect("background task list poisoned");
        for handle in handles.drain(..) {
            handle.abort();
        }
    }
}

fn is_conflict_rejection_error(err: &AedbError) -> bool {
    matches!(err, AedbError::Conflict(_))
        || matches!(err, AedbError::Validation(msg) if msg.contains("conflict"))
}

fn is_permission_rejection_error(err: &AedbError) -> bool {
    matches!(err, AedbError::PermissionDenied(_))
}

fn is_validation_rejection_error(err: &AedbError) -> bool {
    matches!(err, AedbError::Validation(msg) if !msg.contains("conflict"))
}

fn is_coordinator_timeout_error(err: &AedbError) -> bool {
    matches!(err, AedbError::PartitionLockTimeout)
}

fn is_parallel_apply_timeout_error(err: &AedbError) -> bool {
    matches!(
        err,
        AedbError::EpochApplyTimeout | AedbError::ParallelApplyCancelled
    )
}

mod adaptive;
mod coordinator;
mod global_index;
mod group_commit;
mod internals;
mod parallel_runtime;
use internals::*;

#[cfg(test)]
mod tests;
