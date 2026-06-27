//! Durable-before-visible epoch finalization.
//!
//! The apply loop appends an epoch's WAL frames under the executor state lock,
//! then releases the lock to run the durability fsync (see
//! [`super::internals::prepare_commit_epoch`]). This module holds the types that
//! carry the in-progress epoch across that lock-release boundary and the
//! functions that finalize — apply in-memory and publish — once the data is
//! durable, or abort without exposing the epoch if the fsync fails.

use super::global_index::GlobalUniqueIndexState;
use super::internals::{
    MEMORY_ESTIMATE_INTERVAL_MICROS, inline_kv_set_epoch_namespace, now_micros,
    overwrite_assertion_failures_with_wal_error, prune_idempotency,
};
use super::{
    CommitResult, EpochOutcome, EpochProcessResult, ExecutorState, IdempotencyOutcome,
    InternalSequencedCommit, SequencedCommit,
};
use crate::catalog::Catalog;
use crate::commit::tx::{IdempotencyKey, IdempotencyRecord};
use crate::commit::validation::Mutation;
use crate::config::DurabilityMode;
use crate::error::AedbError;
use crate::storage::keyspace::Keyspace;
use crate::storage::value_store::PersistentValueStore;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::warn;

/// Durability fsyncs that must run with the executor state lock released.
///
/// Holds cloned handles to the same backing files the in-lock path would sync,
/// so the (potentially slow) fsync can proceed while other threads acquire the
/// executor state lock. Nothing is made visible until `run()` succeeds — the
/// caller re-acquires the lock and finalizes only on success, preserving the
/// durable-before-visible invariant.
pub(super) struct PreparedSync {
    pub(super) value_store: Option<Arc<PersistentValueStore>>,
    pub(super) wal_file: std::fs::File,
}

/// Which stage of [`PreparedSync::run`] failed, so the epoch can be aborted with
/// the same operator-facing message the in-lock path produced.
pub(super) enum DurabilitySyncError {
    ValueStore(AedbError),
    Wal(AedbError),
}

impl DurabilitySyncError {
    /// The (error, context) pair used to build abort outcomes, matching the
    /// messages emitted by the original in-lock sync path.
    fn into_parts(self) -> (AedbError, &'static str) {
        match self {
            DurabilitySyncError::ValueStore(err) => {
                (err, "epoch aborted during persistent value sync")
            }
            DurabilitySyncError::Wal(err) => (err, "epoch aborted during WAL sync"),
        }
    }
}

impl PreparedSync {
    /// Flush the persistent value store (if any) followed by the active WAL
    /// segment, matching the in-lock sync order. Safe to call without holding
    /// the executor state lock: the value store syncs through a shared `Arc`
    /// and the WAL handle is a clone of the active segment's file.
    pub(super) fn run(&self) -> Result<(), DurabilitySyncError> {
        if let Some(store) = &self.value_store {
            store.sync_all().map_err(DurabilitySyncError::ValueStore)?;
        }
        self.wal_file
            .sync_data()
            .map_err(|err| DurabilitySyncError::Wal(AedbError::Io(err)))?;
        Ok(())
    }
}

/// Result of preparing an epoch under the executor state lock: either the epoch
/// finished entirely under the lock (no out-of-lock durability sync was needed,
/// or it aborted before the sync point), or the WAL has been appended and a
/// durability fsync must run with the lock released before the epoch is
/// finalized and published.
pub(super) enum EpochCommitStep {
    Done(EpochProcessResult),
    NeedsSync {
        sync: PreparedSync,
        finalize: PendingFinalize,
    },
}

/// Carries the work needed to finalize an epoch after its out-of-lock
/// durability sync, dispatched by epoch kind.
pub(super) enum PendingFinalize {
    General(Box<PreparedCommitEpoch>),
    InlineKv(Box<PreparedInlineKvEpoch>),
}

/// In-progress general epoch state captured at the WAL-appended-but-not-yet-
/// durable boundary, so the apply loop can drop the lock for the fsync and then
/// finalize the in-memory apply + publish on re-acquire.
pub(super) struct PreparedCommitEpoch {
    pub(super) outcomes: Vec<EpochOutcome>,
    pub(super) sequenced: Vec<SequencedCommit>,
    pub(super) internal_sequenced: Vec<InternalSequencedCommit>,
    pub(super) working_keyspace: Keyspace,
    pub(super) working_catalog: Catalog,
    pub(super) working_idempotency: Option<HashMap<IdempotencyKey, IdempotencyRecord>>,
    pub(super) working_global_unique_index: GlobalUniqueIndexState,
    pub(super) requires_sync: bool,
    pub(super) wal_payload_size_bytes: usize,
    pub(super) coordinator_apply_attempts: u64,
    pub(super) coordinator_apply_micros: u64,
    pub(super) parallel_apply_micros: u64,
    pub(super) pre_wal_micros: u64,
    pub(super) read_set_conflicts: u64,
    pub(super) wal_append_ops: u64,
    pub(super) wal_append_bytes: u64,
    pub(super) wal_append_micros: u64,
    pub(super) wal_sync_ops: u64,
    pub(super) wal_sync_micros: u64,
    pub(super) sync_executed: bool,
    pub(super) catalog_changed: bool,
}

/// In-progress inline-KV fast-path epoch state captured at the WAL-appended
/// boundary, mirroring [`PreparedCommitEpoch`] for the anonymous KvSet hot path.
pub(super) struct PreparedInlineKvEpoch {
    pub(super) outcomes: Vec<EpochOutcome>,
    pub(super) sequenced: Vec<SequencedCommit>,
    pub(super) wal_payload_size_bytes: usize,
    pub(super) pre_wal_micros: u64,
    pub(super) wal_append_ops: u64,
    pub(super) wal_append_bytes: u64,
    pub(super) wal_append_micros: u64,
    pub(super) wal_sync_ops: u64,
    pub(super) wal_sync_micros: u64,
    pub(super) sync_executed: bool,
}

/// Abort a prepared general epoch whose out-of-lock durability sync failed:
/// overwrite any assertion-failure outcomes with the durability error, fail
/// every sequenced commit, and publish nothing. The working state is never
/// swapped in, so the epoch stays invisible and the failure is not exposed.
pub(super) fn abort_general_epoch_after_durability_failure(
    mut prepared: PreparedCommitEpoch,
    err: &AedbError,
    context: &str,
) -> EpochProcessResult {
    overwrite_assertion_failures_with_wal_error(&mut prepared.outcomes, err, context);
    let PreparedCommitEpoch {
        mut outcomes,
        sequenced,
        coordinator_apply_attempts,
        coordinator_apply_micros,
        parallel_apply_micros,
        pre_wal_micros,
        read_set_conflicts,
        wal_append_ops,
        wal_append_bytes,
        wal_append_micros,
        wal_sync_ops,
        wal_sync_micros,
        sync_executed,
        catalog_changed,
        ..
    } = prepared;
    for failed in sequenced {
        outcomes.push(EpochOutcome {
            request: failed.request,
            result: Err(AedbError::Validation(format!("{context}: {err}"))),
            post_apply_delta: None,
        });
    }
    EpochProcessResult {
        outcomes,
        coordinator_apply_attempts,
        coordinator_apply_micros,
        parallel_apply_micros,
        pre_wal_micros,
        finalize_micros: 0,
        read_set_conflicts,
        wal_append_ops,
        wal_append_bytes,
        wal_append_micros,
        wal_sync_ops,
        wal_sync_micros,
        sync_executed,
        catalog_changed,
    }
}

/// Finalize a prepared general epoch under the executor state lock: swap the
/// working state in, advance the sequence/durability heads, publish deltas, and
/// build success outcomes.
///
/// `durability_synced` is true when the out-of-lock fsync already completed for
/// this epoch (its telemetry must be credited and `sync_executed` set);
/// `sync_micros` is how long that sync took. When false no durability sync ran
/// before this call, though an opportunistic batch flush may still run below.
pub(super) fn finalize_committed_epoch(
    state: &mut ExecutorState,
    prepared: PreparedCommitEpoch,
    sync_micros: u64,
    durability_synced: bool,
) -> EpochProcessResult {
    let PreparedCommitEpoch {
        mut outcomes,
        sequenced,
        internal_sequenced,
        working_keyspace,
        working_catalog,
        working_idempotency,
        working_global_unique_index,
        requires_sync,
        wal_payload_size_bytes,
        coordinator_apply_attempts,
        coordinator_apply_micros,
        parallel_apply_micros,
        pre_wal_micros,
        read_set_conflicts,
        wal_append_ops,
        wal_append_bytes,
        wal_append_micros,
        mut wal_sync_ops,
        mut wal_sync_micros,
        mut sync_executed,
        catalog_changed,
    } = prepared;

    if durability_synced {
        wal_sync_ops = wal_sync_ops.saturating_add(1);
        wal_sync_micros = wal_sync_micros.saturating_add(sync_micros);
        sync_executed = true;
    }

    let last_user_seq = sequenced.last().map(|c| c.seq).unwrap_or(state.current_seq);
    let last_internal_seq = internal_sequenced
        .last()
        .map(|c| c.seq)
        .unwrap_or(state.current_seq);
    let last_seq = last_user_seq.max(last_internal_seq);
    debug_assert!(
        last_seq >= state.current_seq,
        "commit seq must be monotonic across epochs"
    );
    state.keyspace = working_keyspace;
    state.catalog = working_catalog;
    state.global_unique_index = working_global_unique_index;
    if let Some(updated) = working_idempotency {
        state.idempotency = updated;
    }
    state.current_seq = last_seq;
    state.visible_head_seq = last_seq;
    match state.config.durability_mode {
        DurabilityMode::Full => {
            state.durable_head_seq = last_seq;
            state.pending_batch_bytes = 0;
            state.pending_batch_max_seq = state.durable_head_seq;
        }
        DurabilityMode::Batch => {
            if requires_sync {
                state.durable_head_seq = last_seq;
                state.pending_batch_bytes = 0;
                state.pending_batch_max_seq = state.durable_head_seq;
            } else {
                state.pending_batch_bytes = state
                    .pending_batch_bytes
                    .saturating_add(wal_payload_size_bytes);
                state.pending_batch_max_seq = last_seq;
                if state.pending_batch_bytes >= state.config.batch_max_bytes {
                    let sync_started = Instant::now();
                    if state.keyspace.sync_persistent_value_store().is_ok()
                        && state.wal.sync_active().is_ok()
                    {
                        wal_sync_ops = wal_sync_ops.saturating_add(1);
                        wal_sync_micros = wal_sync_micros
                            .saturating_add(sync_started.elapsed().as_micros() as u64);
                        sync_executed = true;
                        state.durable_head_seq = state.pending_batch_max_seq;
                        state.pending_batch_bytes = 0;
                        state.pending_batch_max_seq = state.durable_head_seq;
                    }
                }
            }
        }
        DurabilityMode::OsBuffered => {}
    }
    debug_assert!(
        state.durable_head_seq <= state.visible_head_seq,
        "durable head cannot exceed visible head"
    );
    prune_idempotency(state);

    let forced_full_snapshot = state.version_store.publish_deltas(
        sequenced
            .iter()
            .map(|commit| (commit.seq, Arc::clone(&commit.delta)))
            .chain(
                internal_sequenced
                    .iter()
                    .map(|commit| (commit.seq, Arc::clone(&commit.delta))),
            ),
    );
    let snapshot_due = now_micros().saturating_sub(state.last_full_snapshot_micros)
        >= state.config.max_snapshot_age_ms.saturating_mul(1000);
    if snapshot_due || forced_full_snapshot {
        state.version_store.publish_full(
            state.visible_head_seq,
            state.keyspace.snapshot(),
            state.catalog.snapshot(),
        );
        state.last_full_snapshot_micros = now_micros();
    }

    let now_micros = now_micros();
    if now_micros.saturating_sub(state.last_memory_estimate_micros)
        >= MEMORY_ESTIMATE_INTERVAL_MICROS
    {
        state.last_memory_estimate_micros = now_micros;
        let mem_estimate = state.keyspace.estimate_memory_bytes();
        if mem_estimate > state.config.max_memory_estimate_bytes {
            warn!(
                mem_estimate,
                max_memory_estimate_bytes = state.config.max_memory_estimate_bytes,
                "aedb memory estimate exceeded threshold"
            );
        }
    }
    if let Some(reason) = state.wal.should_rotate()
        && let Err(err) = state.wal.rotate()
    {
        // The epoch is already durable at this point, so a rotation failure does
        // not invalidate the committed data. It must not be silent, though: a
        // persistent failure lets the active segment grow unbounded and degrades
        // recovery, so surface it for operators rather than dropping it.
        warn!(
            reason = ?reason,
            error = %err,
            "aedb WAL segment rotation failed; active segment will continue to grow"
        );
    }

    let durable_head = state.durable_head_seq;
    let finalize_started = Instant::now();
    for commit in sequenced {
        outcomes.push(EpochOutcome {
            request: commit.request,
            result: Ok(CommitResult {
                commit_seq: commit.seq,
                durable_head_seq: durable_head,
                idempotency: IdempotencyOutcome::Applied,
                canonical_commit_seq: commit.seq,
            }),
            post_apply_delta: Some(commit.delta),
        });
    }
    EpochProcessResult {
        outcomes,
        coordinator_apply_attempts,
        coordinator_apply_micros,
        parallel_apply_micros,
        pre_wal_micros,
        finalize_micros: finalize_started.elapsed().as_micros() as u64,
        read_set_conflicts,
        wal_append_ops,
        wal_append_bytes,
        wal_append_micros,
        wal_sync_ops,
        wal_sync_micros,
        sync_executed,
        catalog_changed,
    }
}

/// Abort a prepared inline-KV epoch whose out-of-lock WAL sync failed: fail
/// every sequenced commit and apply nothing, leaving the epoch invisible.
pub(super) fn abort_inline_kv_epoch_after_durability_failure(
    prepared: PreparedInlineKvEpoch,
    err: &AedbError,
    context: &str,
) -> EpochProcessResult {
    let PreparedInlineKvEpoch {
        mut outcomes,
        sequenced,
        pre_wal_micros,
        wal_append_ops,
        wal_append_bytes,
        wal_append_micros,
        wal_sync_ops,
        wal_sync_micros,
        ..
    } = prepared;
    for failed in sequenced {
        outcomes.push(EpochOutcome {
            request: failed.request,
            result: Err(AedbError::Validation(format!("{context}: {err}"))),
            post_apply_delta: None,
        });
    }
    EpochProcessResult {
        outcomes,
        pre_wal_micros,
        wal_append_ops,
        wal_append_bytes,
        wal_append_micros,
        wal_sync_ops,
        wal_sync_micros,
        ..EpochProcessResult::default()
    }
}

/// Finalize a prepared inline-KV epoch under the executor state lock: apply the
/// KV writes in-memory, advance the sequence/durability heads, publish, and
/// build success outcomes. `durability_synced`/`sync_micros` carry the
/// out-of-lock fsync result for the `DurabilityMode::Full` path.
pub(super) fn finalize_inline_kv_committed_epoch(
    state: &mut ExecutorState,
    prepared: PreparedInlineKvEpoch,
    sync_micros: u64,
    durability_synced: bool,
) -> EpochProcessResult {
    let PreparedInlineKvEpoch {
        mut outcomes,
        sequenced,
        wal_payload_size_bytes,
        pre_wal_micros,
        wal_append_ops,
        wal_append_bytes,
        wal_append_micros,
        mut wal_sync_ops,
        mut wal_sync_micros,
        mut sync_executed,
    } = prepared;

    if durability_synced {
        wal_sync_ops = wal_sync_ops.saturating_add(1);
        wal_sync_micros = wal_sync_micros.saturating_add(sync_micros);
        sync_executed = true;
    }

    if let Some((project_id, scope_id)) = inline_kv_set_epoch_namespace(&sequenced) {
        state.keyspace.kv_set_many_inline_same_namespace_with_seq(
            project_id,
            scope_id,
            sequenced.iter().flat_map(|commit| {
                commit.delta.mutations.iter().map(move |mutation| {
                    let Mutation::KvSet { key, value, .. } = mutation else {
                        unreachable!("inline KV fast path only sequences KvSet commits");
                    };
                    (key, value, commit.seq)
                })
            }),
        );
    } else {
        for commit in &sequenced {
            for mutation in &commit.delta.mutations {
                let Mutation::KvSet {
                    project_id,
                    scope_id,
                    key,
                    value,
                } = mutation
                else {
                    unreachable!("inline KV fast path only sequences KvSet commits");
                };
                state.keyspace.kv_set_inline(
                    project_id,
                    scope_id,
                    key.clone(),
                    value.clone(),
                    commit.seq,
                );
            }
        }
    }

    let last_seq = sequenced
        .last()
        .map(|commit| commit.seq)
        .unwrap_or(state.current_seq);
    state.current_seq = last_seq;
    state.visible_head_seq = last_seq;
    match state.config.durability_mode {
        DurabilityMode::Full => {
            state.durable_head_seq = last_seq;
            state.pending_batch_bytes = 0;
            state.pending_batch_max_seq = state.durable_head_seq;
        }
        DurabilityMode::Batch => {
            state.pending_batch_bytes = state
                .pending_batch_bytes
                .saturating_add(wal_payload_size_bytes);
            state.pending_batch_max_seq = last_seq;
            if state.pending_batch_bytes >= state.config.batch_max_bytes {
                let sync_started = Instant::now();
                if state.keyspace.sync_persistent_value_store().is_ok()
                    && state.wal.sync_active().is_ok()
                {
                    wal_sync_ops = wal_sync_ops.saturating_add(1);
                    wal_sync_micros =
                        wal_sync_micros.saturating_add(sync_started.elapsed().as_micros() as u64);
                    sync_executed = true;
                    state.durable_head_seq = state.pending_batch_max_seq;
                    state.pending_batch_bytes = 0;
                    state.pending_batch_max_seq = state.durable_head_seq;
                }
            }
        }
        DurabilityMode::OsBuffered => {}
    }
    debug_assert!(
        state.durable_head_seq <= state.visible_head_seq,
        "durable head cannot exceed visible head"
    );
    prune_idempotency(state);

    let forced_full_snapshot = state.version_store.publish_deltas(
        sequenced
            .iter()
            .map(|commit| (commit.seq, Arc::clone(&commit.delta))),
    );
    let snapshot_due = now_micros().saturating_sub(state.last_full_snapshot_micros)
        >= state.config.max_snapshot_age_ms.saturating_mul(1000);
    if snapshot_due || forced_full_snapshot {
        state.version_store.publish_full(
            state.visible_head_seq,
            state.keyspace.snapshot(),
            state.catalog.snapshot(),
        );
        state.last_full_snapshot_micros = now_micros();
    }

    let now_micros = now_micros();
    if now_micros.saturating_sub(state.last_memory_estimate_micros)
        >= MEMORY_ESTIMATE_INTERVAL_MICROS
    {
        state.last_memory_estimate_micros = now_micros;
        let mem_estimate = state.keyspace.estimate_memory_bytes();
        if mem_estimate > state.config.max_memory_estimate_bytes {
            warn!(
                mem_estimate,
                max_memory_estimate_bytes = state.config.max_memory_estimate_bytes,
                "aedb memory estimate exceeded threshold"
            );
        }
    }
    if let Some(reason) = state.wal.should_rotate()
        && let Err(err) = state.wal.rotate()
    {
        // The epoch is already durable at this point, so a rotation failure does
        // not invalidate the committed data. It must not be silent, though: a
        // persistent failure lets the active segment grow unbounded and degrades
        // recovery, so surface it for operators rather than dropping it.
        warn!(
            reason = ?reason,
            error = %err,
            "aedb WAL segment rotation failed; active segment will continue to grow"
        );
    }

    let durable_head = state.durable_head_seq;
    let finalize_started = Instant::now();
    for commit in sequenced {
        outcomes.push(EpochOutcome {
            request: commit.request,
            result: Ok(CommitResult {
                commit_seq: commit.seq,
                durable_head_seq: durable_head,
                idempotency: IdempotencyOutcome::Applied,
                canonical_commit_seq: commit.seq,
            }),
            post_apply_delta: Some(commit.delta),
        });
    }

    EpochProcessResult {
        outcomes,
        pre_wal_micros,
        finalize_micros: finalize_started.elapsed().as_micros() as u64,
        wal_append_ops,
        wal_append_bytes,
        wal_append_micros,
        wal_sync_ops,
        wal_sync_micros,
        sync_executed,
        ..EpochProcessResult::default()
    }
}

/// Finalize an epoch after its out-of-lock durability sync, with the executor
/// state lock re-acquired. On a sync failure the epoch is aborted and nothing is
/// published, preserving the durable-before-visible invariant; on success the
/// in-memory apply and publish proceed. `sync_micros` is the time the
/// out-of-lock fsync took, credited to the epoch's sync telemetry.
pub(super) fn finalize_after_durability_sync(
    state: &mut ExecutorState,
    finalize: PendingFinalize,
    sync_result: Result<(), DurabilitySyncError>,
    sync_micros: u64,
) -> EpochProcessResult {
    match finalize {
        PendingFinalize::General(prepared) => match sync_result {
            Ok(()) => finalize_committed_epoch(state, *prepared, sync_micros, true),
            Err(sync_err) => {
                let (err, context) = sync_err.into_parts();
                abort_general_epoch_after_durability_failure(*prepared, &err, context)
            }
        },
        PendingFinalize::InlineKv(prepared) => match sync_result {
            Ok(()) => finalize_inline_kv_committed_epoch(state, *prepared, sync_micros, true),
            Err(sync_err) => {
                let (err, context) = sync_err.into_parts();
                abort_inline_kv_epoch_after_durability_failure(*prepared, &err, context)
            }
        },
    }
}
