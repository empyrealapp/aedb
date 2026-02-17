use crate::catalog::Catalog;
use crate::catalog::namespace_key;
use crate::catalog::schema::TableSchema;
use crate::catalog::types::{Row, Value};
use crate::commit::apply::{apply_mutation, apply_mutation_trusted_if_eligible};
use crate::commit::tx::{
    IdempotencyKey, IdempotencyRecord, ReadBound, ReadKey, ReadRange, TransactionEnvelope,
    WalCommitPayload, WriteClass, WriteIntent,
};
use crate::commit::validation::{Mutation, validate_mutation, validate_permissions};
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
use parallel_runtime::ParallelApplyRuntime;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, mpsc as tokio_mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::warn;

#[derive(Debug, Clone)]
pub struct CommitResult {
    pub commit_seq: u64,
    pub durable_head_seq: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeadState {
    pub visible_head_seq: u64,
    pub durable_head_seq: u64,
}

struct CommitRequest {
    envelope: TransactionEnvelope,
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
    delta: CommitDelta,
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
    delta: CommitDelta,
}

struct InternalSequencedCommit {
    seq: u64,
    commit_ts_micros: u64,
    payload_type: u8,
    payload: Vec<u8>,
    delta: CommitDelta,
}

struct EpochOutcome {
    request: CommitRequest,
    result: Result<CommitResult, AedbError>,
    post_apply_delta: Option<CommitDelta>,
}

#[derive(Default)]
struct EpochProcessResult {
    outcomes: Vec<EpochOutcome>,
    coordinator_apply_attempts: u64,
    coordinator_apply_micros: u64,
    read_set_conflicts: u64,
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
}

#[derive(Clone)]
pub struct CommitExecutor {
    ingress_txs: Vec<tokio_mpsc::Sender<CommitRequest>>,
    config: Arc<AedbConfig>,
    state: Arc<Mutex<ExecutorState>>,
    queued_bytes: Arc<AtomicUsize>,
    telemetry: Arc<ExecutorTelemetry>,
    background_tasks: Arc<StdMutex<Vec<JoinHandle<()>>>>,
}

#[derive(Debug, Default)]
struct ExecutorTelemetry {
    inflight_commits: AtomicUsize,
    commits_total: AtomicU64,
    commit_errors: AtomicU64,
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
    parallel_runtime_queue_depth: AtomicUsize,
    adaptive_epoch_min_commits: AtomicUsize,
    adaptive_epoch_max_wait_us: AtomicU64,
    global_unique_index_entries: AtomicUsize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutorMetrics {
    pub inflight_commits: usize,
    pub queued_bytes: usize,
    pub commits_total: u64,
    pub commit_errors: u64,
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
    pub parallel_runtime_queue_depth: usize,
    pub adaptive_epoch_min_commits: usize,
    pub adaptive_epoch_max_wait_us: u64,
    pub global_unique_index_entries: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutorRuntimeState {
    pub current_seq: u64,
    pub visible_head_seq: u64,
    pub durable_head_seq: u64,
    pub last_full_snapshot_micros: u64,
}

impl CommitExecutor {
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
        let mut version_store = VersionStore::new(config.max_versions, config.min_version_age_ms);
        version_store.bootstrap(current_seq, keyspace.snapshot(), catalog.snapshot());
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
            last_full_snapshot_micros: now_micros(),
        }));
        let (apply_tx, mut rx) = tokio_mpsc::channel::<CommitRequest>(max_inflight_commits);
        let queued_bytes = Arc::new(AtomicUsize::new(0));
        let telemetry = Arc::new(ExecutorTelemetry::default());
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
            let handle = tokio::spawn(async move {
                while let Some(task) = post_rx.recv().await {
                    let mut s = post_state.lock().await;
                    if let Ok(changed) = refresh_async_indexes(&mut s, Some(&task.delta))
                        && changed
                    {
                        let seq = s.visible_head_seq.max(task.delta.seq);
                        let keyspace_snapshot = s.keyspace.snapshot();
                        let catalog_snapshot = s.catalog.snapshot();
                        s.version_store
                            .publish_full(seq, keyspace_snapshot, catalog_snapshot);
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
        let apply_handle = tokio::spawn(async move {
            let mut pending = VecDeque::new();
            let mut ingress_closed = false;

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
                let mut s = loop_state.lock().await;
                let epoch_started = Instant::now();
                let epoch_result = process_commit_epoch(&mut s, epoch_requests);
                let outcomes = epoch_result.outcomes;
                let had_error = outcomes.iter().any(|o| o.result.is_err());
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
                if had_error {
                    loop_telemetry
                        .epoch_failures
                        .fetch_add(1, Ordering::Relaxed);
                }
                *loop_validation_catalog.write() = s.catalog.clone();
                drop(s);

                for outcome in outcomes {
                    if let Some(delta) = outcome.post_apply_delta {
                        let post_shard =
                            shard_for_envelope(&outcome.request.envelope, loop_post_txs.len());
                        if let Some(tx) = loop_post_txs.get(post_shard) {
                            let _ = tx.try_send(PostApplyTask { delta });
                        }
                    }
                    let elapsed_micros =
                        now_micros().saturating_sub(outcome.request.enqueue_micros);
                    loop_telemetry
                        .total_latency_micros
                        .fetch_add(elapsed_micros, Ordering::Relaxed);
                    loop_telemetry.commits_total.fetch_add(1, Ordering::Relaxed);
                    if outcome.result.is_err() {
                        loop_telemetry.commit_errors.fetch_add(1, Ordering::Relaxed);
                        if let Err(err) = &outcome.result {
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
                        .inflight_commits
                        .fetch_sub(1, Ordering::Relaxed);
                    let _ = outcome.request.result_tx.send(outcome.result);
                }
            }

            for req in pending {
                queue_counter.fetch_sub(req.encoded_len, Ordering::Relaxed);
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
            let handle = tokio::spawn(async move {
                while let Some(mut req) = ingress_rx.recv().await {
                    let (write_partitions, read_partitions) = if req.prevalidated {
                        let catalog = pre_validation_catalog.read().snapshot();
                        (
                            derive_write_partitions_with_fk_expansion(
                                &catalog,
                                &req.envelope.write_intent.mutations,
                            ),
                            derive_read_partitions(&req.envelope),
                        )
                    } else {
                        match pre_stage_validate(&pre_validation_catalog, &req.envelope).await {
                            Ok(partitions) => partitions,
                            Err(e) => {
                                pre_telemetry.commit_errors.fetch_add(1, Ordering::Relaxed);
                                pre_queue_counter.fetch_sub(req.encoded_len, Ordering::Relaxed);
                                let _ = req.result_tx.send(Err(e));
                                continue;
                            }
                        }
                    };
                    if write_partitions.is_empty() {
                        pre_telemetry.commit_errors.fetch_add(1, Ordering::Relaxed);
                        pre_queue_counter.fetch_sub(req.encoded_len, Ordering::Relaxed);
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
        let flush_handle = tokio::spawn(async move {
            loop {
                let sleep_ms = flush_config.batch_interval_ms;
                tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                let mut s = flush_state.lock().await;
                if flush_config.durability_mode != DurabilityMode::Batch {
                    continue;
                }
                if s.pending_batch_bytes > 0 && s.wal.sync_active().is_ok() {
                    s.durable_head_seq = s.pending_batch_max_seq.max(s.durable_head_seq);
                    s.pending_batch_bytes = 0;
                    s.pending_batch_max_seq = s.durable_head_seq;
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
            queued_bytes,
            telemetry,
            background_tasks,
        })
    }

    pub async fn submit(&self, mutation: Mutation) -> Result<CommitResult, AedbError> {
        self.submit_as(None, mutation).await
    }

    pub async fn submit_as(
        &self,
        caller: Option<CallerContext>,
        mutation: Mutation,
    ) -> Result<CommitResult, AedbError> {
        let base_seq = self.current_seq().await;
        self.submit_envelope(TransactionEnvelope {
            caller,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: Default::default(),
            write_intent: WriteIntent {
                mutations: vec![mutation],
            },
            base_seq,
        })
        .await
    }

    pub async fn submit_envelope(
        &self,
        envelope: TransactionEnvelope,
    ) -> Result<CommitResult, AedbError> {
        self.submit_envelope_with_mode(envelope, false, false).await
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
        let config = &self.config;
        let encoded = rmp_serde::to_vec(&envelope).map_err(|e| AedbError::Encode(e.to_string()))?;
        if encoded.len() > config.max_transaction_bytes {
            return Err(AedbError::Validation(
                "transaction exceeds max_transaction_bytes".into(),
            ));
        }
        let len = encoded.len();
        let mut current = self.queued_bytes.load(Ordering::Relaxed);
        loop {
            let next = current.saturating_add(len);
            if next > config.max_commit_queue_bytes {
                self.telemetry
                    .queue_full_rejections
                    .fetch_add(1, Ordering::Relaxed);
                return Err(AedbError::QueueFull);
            }
            match self.queued_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
        let (result_tx, result_rx) = oneshot::channel();
        let shard = shard_for_envelope(&envelope, self.ingress_txs.len());
        let ingress_tx = self
            .ingress_txs
            .get(shard)
            .ok_or_else(|| AedbError::Validation("no ingress shard available".into()))?
            .clone();
        let send_result = tokio::time::timeout(
            std::time::Duration::from_millis(config.commit_timeout_ms),
            ingress_tx.send(CommitRequest {
                envelope,
                encoded_len: len,
                enqueue_micros: now_micros(),
                prevalidated,
                assertions_engine_verified,
                write_partitions: HashSet::new(),
                read_partitions: HashSet::new(),
                defer_count: 0,
                result_tx,
            }),
        )
        .await;
        match send_result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                self.queued_bytes.fetch_sub(len, Ordering::Relaxed);
                return Err(AedbError::Validation(format!("commit queue closed: {e}")));
            }
            Err(_) => {
                self.queued_bytes.fetch_sub(len, Ordering::Relaxed);
                self.telemetry
                    .timeout_rejections
                    .fetch_add(1, Ordering::Relaxed);
                return Err(AedbError::Timeout);
            }
        }
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

    pub async fn current_seq(&self) -> u64 {
        self.state.lock().await.current_seq
    }

    pub async fn visible_head_seq(&self) -> u64 {
        self.state.lock().await.visible_head_seq
    }

    pub async fn durable_head_seq(&self) -> u64 {
        self.state.lock().await.durable_head_seq
    }

    pub async fn head_state(&self) -> HeadState {
        let s = self.state.lock().await;
        HeadState {
            visible_head_seq: s.visible_head_seq,
            durable_head_seq: s.durable_head_seq,
        }
    }

    pub async fn snapshot_state(&self) -> (KeyspaceSnapshot, Catalog, u64) {
        let mut state = self.state.lock().await;
        let view = state
            .version_store
            .acquire_latest()
            .expect("version store should always have a latest view")
            .into_view();
        ((*view.keyspace).clone(), (*view.catalog).clone(), view.seq)
    }

    pub async fn snapshot_at_seq(&self, seq: u64) -> Result<SnapshotReadView, AedbError> {
        let mut state = self.state.lock().await;
        let view = state.version_store.acquire_at_seq(seq)?.into_view();
        Ok(view)
    }

    pub async fn wait_for_durable(&self, seq: u64) -> Result<(), AedbError> {
        loop {
            if self.durable_head_seq().await >= seq {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }

    pub async fn force_fsync(&self) -> Result<u64, AedbError> {
        let mut s = self.state.lock().await;
        s.wal
            .sync_active()
            .map_err(|e| AedbError::Io(std::io::Error::other(e.to_string())))?;
        s.durable_head_seq = s.visible_head_seq;
        s.pending_batch_bytes = 0;
        s.pending_batch_max_seq = s.durable_head_seq;
        Ok(s.durable_head_seq)
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
        ExecutorMetrics {
            inflight_commits: self.telemetry.inflight_commits.load(Ordering::Relaxed),
            queued_bytes: self.queued_bytes.load(Ordering::Relaxed),
            commits_total,
            commit_errors: self.telemetry.commit_errors.load(Ordering::Relaxed),
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
        }
    }

    pub async fn runtime_state_metrics(&self) -> ExecutorRuntimeState {
        let s = self.state.lock().await;
        ExecutorRuntimeState {
            current_seq: s.current_seq,
            visible_head_seq: s.visible_head_seq,
            durable_head_seq: s.durable_head_seq,
            last_full_snapshot_micros: s.last_full_snapshot_micros,
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
mod internals;
mod parallel_runtime;
use internals::*;

#[cfg(test)]
mod tests;
