use super::parallel_runtime::ParallelTask;
use super::*;
use crate::catalog::SYSTEM_PROJECT_ID;
use crate::commit::assertions::{evaluate_assertions, validate_assertions};
use crate::commit::tx::ReadAssertion;
use primitive_types::U256;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc as std_mpsc;

pub(super) fn pre_stage_validate(
    validation_catalog: &Arc<RwLock<Catalog>>,
    envelope: &TransactionEnvelope,
) -> impl std::future::Future<Output = Result<(HashSet<String>, HashSet<String>), AedbError>>
+ Send
+ 'static {
    let validation_catalog = Arc::clone(validation_catalog);
    let envelope = envelope.clone();
    async move {
        let catalog = validation_catalog.read().snapshot();
        validate_assertions(&catalog, &envelope.assertions)?;
        let mut staged_catalog = catalog.clone();
        for mutation in &envelope.write_intent.mutations {
            validate_permissions(&staged_catalog, envelope.caller.as_ref(), mutation)?;
            validate_mutation(&staged_catalog, mutation)?;
            if let Mutation::Ddl(ddl) = mutation {
                staged_catalog.apply_ddl(ddl.clone())?;
            }
        }
        let write_partitions = derive_write_partitions_with_fk_expansion(
            &staged_catalog,
            &envelope.write_intent.mutations,
        );
        let read_partitions = derive_read_partitions(&envelope);
        Ok((write_partitions, read_partitions))
    }
}

pub(super) fn shard_for_envelope(envelope: &TransactionEnvelope, shard_count: usize) -> usize {
    if shard_count <= 1 {
        return 0;
    }
    let key = scope_shard_key(&envelope.write_intent.mutations);
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % shard_count
}

pub(super) fn scope_shard_key(mutations: &[Mutation]) -> String {
    let Some(mutation) = mutations.first() else {
        return "empty".into();
    };
    match mutation {
        Mutation::Insert {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::InsertBatch {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::Upsert {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::UpsertBatch {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::UpsertOnConflict {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::UpsertBatchOnConflict {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::Delete {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::DeleteWhere {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::UpdateWhere {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::UpdateWhereExpr {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::TableIncU256 {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::TableDecU256 {
            project_id,
            scope_id,
            table_name,
            ..
        } => format!("t:{project_id}:{scope_id}:{table_name}"),
        Mutation::KvSet {
            project_id,
            scope_id,
            ..
        }
        | Mutation::KvDel {
            project_id,
            scope_id,
            ..
        }
        | Mutation::KvIncU256 {
            project_id,
            scope_id,
            ..
        }
        | Mutation::KvDecU256 {
            project_id,
            scope_id,
            ..
        } => format!("k:{project_id}:{scope_id}"),
        Mutation::Ddl(ddl) => format!("ddl:{ddl:?}"),
    }
}

pub(super) async fn build_epoch_requests(
    pending: &mut VecDeque<CommitRequest>,
    min_commits: usize,
    max_commits: usize,
    deadline: Instant,
    rx: &mut tokio_mpsc::Receiver<CommitRequest>,
    ingress_closed: bool,
) -> Vec<CommitRequest> {
    let mut selected = Vec::new();
    let mut epoch_writes = HashSet::new();
    let mut epoch_reads = HashSet::new();
    let mut has_economic = false;
    let mut has_cross_partition = false;

    while selected.len() < max_commits {
        if pending.is_empty() {
            if !selected.is_empty() && selected.len() >= min_commits {
                break;
            }
            if ingress_closed {
                break;
            }
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            match tokio::time::timeout(deadline - now, rx.recv()).await {
                Ok(Some(req)) => pending.push_back(req),
                _ => break,
            }
            while let Ok(req) = rx.try_recv() {
                pending.push_back(req);
            }
            continue;
        }

        let Some(candidate_idx) = find_compatible_candidate_index(
            pending,
            &epoch_writes,
            &epoch_reads,
            has_cross_partition,
            has_economic,
        ) else {
            break;
        };

        if candidate_idx > 0
            && let Some(front) = pending.front_mut()
        {
            front.defer_count = front.defer_count.saturating_add(1);
            if front.defer_count >= MAX_EPOCH_DEFER {
                break;
            }
        }

        let req = pending
            .remove(candidate_idx)
            .expect("compatible pending entry must exist");
        let write_set = req.write_partitions.clone();
        let read_set = req.read_partitions.clone();
        let candidate_cross_partition = is_cross_partition_write_set(&write_set);
        let candidate_economic = matches!(req.envelope.write_class, WriteClass::Economic);

        has_cross_partition |= candidate_cross_partition;
        has_economic |= candidate_economic;
        epoch_writes.extend(write_set);
        epoch_reads.extend(read_set);
        selected.push(req);

        if has_cross_partition || has_economic {
            break;
        }

        while let Ok(req) = rx.try_recv() {
            pending.push_back(req);
        }
        if selected.len() >= min_commits && pending.is_empty() {
            break;
        }
        if Instant::now() >= deadline {
            break;
        }
    }
    selected
}

pub(super) fn find_compatible_candidate_index(
    pending: &VecDeque<CommitRequest>,
    epoch_writes: &HashSet<String>,
    epoch_reads: &HashSet<String>,
    has_cross_partition: bool,
    has_economic: bool,
) -> Option<usize> {
    for (idx, candidate) in pending.iter().enumerate() {
        let write_set = &candidate.write_partitions;
        let read_set = &candidate.read_partitions;
        let candidate_cross_partition = is_cross_partition_write_set(write_set);
        let candidate_economic = matches!(candidate.envelope.write_class, WriteClass::Economic);
        let reads_conflict = read_set.iter().any(|p| epoch_writes.contains(p));
        let anti_dependency = write_set.iter().any(|p| epoch_reads.contains(p));
        let structural_conflict =
            has_cross_partition || candidate_cross_partition || has_economic || candidate_economic;
        if epoch_writes.is_empty() || !(reads_conflict || anti_dependency || structural_conflict) {
            return Some(idx);
        }
    }
    None
}

pub(super) fn process_commit_epoch(
    state: &mut ExecutorState,
    requests: Vec<CommitRequest>,
) -> EpochProcessResult {
    if requests.is_empty() {
        return EpochProcessResult::default();
    }

    let mut outcomes = Vec::with_capacity(requests.len());
    let mut coordinator_apply_attempts = 0u64;
    let mut coordinator_apply_micros = 0u64;
    let mut read_set_conflicts = 0u64;
    let mut working_keyspace = state.keyspace.clone();
    let mut working_catalog = state.catalog.clone();
    let mut working_idempotency = state.idempotency.clone();
    let mut working_global_unique_index = state.global_unique_index.clone();
    let mut sequenced = Vec::new();
    let mut internal_sequenced = Vec::new();
    let mut deferred_parallel_commits = Vec::new();
    let mut deferred_parallel_namespaces = HashSet::new();
    let mut next_seq = state.current_seq;

    for request in requests {
        if request.envelope.write_intent.mutations.is_empty() {
            outcomes.push(EpochOutcome {
                request,
                result: Err(AedbError::Validation(
                    "transaction envelope has no mutations".into(),
                )),
                post_apply_delta: None,
            });
            continue;
        }

        if let Some(key) = request.envelope.idempotency_key.clone()
            && let Some(record) = working_idempotency.get(&key)
        {
            outcomes.push(EpochOutcome {
                request,
                result: Ok(CommitResult {
                    commit_seq: record.commit_seq,
                    durable_head_seq: state.durable_head_seq.max(record.commit_seq),
                }),
                post_apply_delta: None,
            });
            continue;
        }

        if let Err(err) = revalidate_read_set_for_keyspace(&working_keyspace, &request.envelope) {
            if is_read_set_conflict_error(&err) {
                read_set_conflicts = read_set_conflicts.saturating_add(1);
            }
            outcomes.push(EpochOutcome {
                request,
                result: Err(err),
                post_apply_delta: None,
            });
            continue;
        }

        let skip_assertions = request.prevalidated && request.assertions_engine_verified;
        if !skip_assertions
            && let Err(err) = evaluate_assertions(
                &working_catalog,
                &working_keyspace,
                &request.envelope.assertions,
            )
        {
            if let Some(internal) = build_assertion_audit_commit(
                &request.envelope,
                &err,
                &mut working_catalog,
                &mut working_keyspace,
                &mut next_seq,
            ) {
                internal_sequenced.push(internal);
            }
            outcomes.push(EpochOutcome {
                request,
                result: Err(err),
                post_apply_delta: None,
            });
            continue;
        }

        let mut mutations = request.envelope.write_intent.mutations.clone();
        augment_mutations_with_caller(&mut mutations, request.envelope.caller.as_ref());
        if !request.prevalidated {
            let mut permission_error = None;
            for mutation in &mutations {
                if let Err(err) = validate_permissions(
                    &working_catalog,
                    request.envelope.caller.as_ref(),
                    mutation,
                ) {
                    permission_error = Some(err);
                    break;
                }
            }
            if let Some(err) = permission_error {
                outcomes.push(EpochOutcome {
                    request,
                    result: Err(err),
                    post_apply_delta: None,
                });
                continue;
            }
        }

        let snapshot_seq_before_commit = next_seq;
        next_seq = next_seq.saturating_add(1);
        let commit_seq = next_seq;
        let requires_coordinator =
            request_requires_coordinator(&working_catalog, &request, &mutations);
        let is_cross_partition = is_cross_partition_request(&request);
        let parallel_namespace = namespace_id_for_parallel_mutations(&mutations);
        let can_defer_parallel_apply = state.config.parallel_apply_enabled
            && matches!(request.envelope.write_class, WriteClass::Standard)
            && is_parallel_single_partition_apply_candidate(&request, &mutations, &working_catalog)
            && parallel_namespace
                .as_ref()
                .map(|ns| !deferred_parallel_namespaces.contains(ns))
                .unwrap_or(false);
        if is_cross_partition || requires_coordinator {
            coordinator_apply_attempts = coordinator_apply_attempts.saturating_add(1);
            let mut trial_keyspace = working_keyspace.clone();
            let mut trial_catalog = working_catalog.clone();
            let mut trial_global_unique_index = working_global_unique_index.clone();
            let partition_order = canonical_partition_order(&request.write_partitions);
            let coordinator_started = Instant::now();
            let apply_result = apply_via_coordinator(
                &mut trial_catalog,
                &mut trial_keyspace,
                &mut trial_global_unique_index,
                &mutations,
                commit_seq,
                &partition_order,
                CoordinatorApplyOptions {
                    coordinator_locking_enabled: state.config.coordinator_locking_enabled,
                    lock_manager: &state.coordinator_locks,
                    global_unique_index_enabled: state.config.global_unique_index_enabled,
                    partition_lock_timeout_ms: state.config.partition_lock_timeout_ms,
                },
            );
            coordinator_apply_micros = coordinator_apply_micros
                .saturating_add(coordinator_started.elapsed().as_micros() as u64);
            if let Err(err) = apply_result {
                outcomes.push(EpochOutcome {
                    request,
                    result: Err(err),
                    post_apply_delta: None,
                });
                next_seq = next_seq.saturating_sub(1);
                continue;
            }
            working_keyspace = trial_keyspace;
            working_catalog = trial_catalog;
            working_global_unique_index = trial_global_unique_index;
        } else if !can_defer_parallel_apply {
            let mut trial_keyspace = working_keyspace.clone();
            let mut trial_catalog = working_catalog.clone();
            let mut apply_error = None;
            for mutation in &mutations {
                let applied = if request.prevalidated {
                    apply_mutation_trusted_if_eligible(
                        &mut trial_catalog,
                        &mut trial_keyspace,
                        mutation.clone(),
                        commit_seq,
                        request.envelope.base_seq,
                        snapshot_seq_before_commit,
                    )
                } else {
                    None
                };
                match applied {
                    Some(Ok(())) => {}
                    Some(Err(err)) => {
                        apply_error = Some(err);
                        break;
                    }
                    None => {
                        if let Err(err) = apply_mutation(
                            &mut trial_catalog,
                            &mut trial_keyspace,
                            mutation.clone(),
                            commit_seq,
                        ) {
                            apply_error = Some(err);
                            break;
                        }
                    }
                }
            }
            if let Some(err) = apply_error {
                outcomes.push(EpochOutcome {
                    request,
                    result: Err(err),
                    post_apply_delta: None,
                });
                next_seq = next_seq.saturating_sub(1);
                continue;
            }
            working_keyspace = trial_keyspace;
            working_catalog = trial_catalog;
        }

        let payload_type = payload_type_for_mutations(&mutations);
        let payload = match encode_wal_payload(&WalCommitPayload {
            mutations: mutations.clone(),
            assertions: request.envelope.assertions.clone(),
            idempotency_key: request.envelope.idempotency_key.clone(),
        }) {
            Ok(payload) => payload,
            Err(err) => {
                outcomes.push(EpochOutcome {
                    request,
                    result: Err(err),
                    post_apply_delta: None,
                });
                next_seq = next_seq.saturating_sub(1);
                continue;
            }
        };
        let commit_ts_micros = now_micros();

        if let Some(key) = request.envelope.idempotency_key.clone() {
            working_idempotency.insert(
                key,
                IdempotencyRecord {
                    commit_seq,
                    recorded_at_micros: commit_ts_micros,
                },
            );
        }

        let delta = CommitDelta {
            seq: commit_seq,
            mutations: mutations.clone(),
        };
        sequenced.push(SequencedCommit {
            request,
            seq: commit_seq,
            commit_ts_micros,
            payload_type,
            payload,
            delta,
        });
        if can_defer_parallel_apply {
            if let Some(ns) = parallel_namespace {
                deferred_parallel_namespaces.insert(ns);
            }
            deferred_parallel_commits.push(sequenced.len() - 1);
        }
    }

    if sequenced.is_empty() && internal_sequenced.is_empty() {
        return EpochProcessResult {
            outcomes,
            coordinator_apply_attempts,
            coordinator_apply_micros,
            read_set_conflicts,
        };
    }

    if !deferred_parallel_commits.is_empty()
        && let Err(err) = apply_deferred_parallel_single_partition_commits(
            &working_catalog,
            &mut working_keyspace,
            &state.parallel_runtime,
            &sequenced,
            &deferred_parallel_commits,
            state.config.epoch_apply_timeout_ms,
        )
    {
        let wrapped = format!("epoch aborted during parallel apply: {err}");
        for failed in sequenced {
            let surfaced = match &err {
                AedbError::EpochApplyTimeout => AedbError::EpochApplyTimeout,
                AedbError::ParallelApplyCancelled => AedbError::ParallelApplyCancelled,
                AedbError::ParallelApplyWorkerPanicked => AedbError::ParallelApplyWorkerPanicked,
                _ => AedbError::Validation(wrapped.clone()),
            };
            outcomes.push(EpochOutcome {
                request: failed.request,
                result: Err(surfaced),
                post_apply_delta: None,
            });
        }
        return EpochProcessResult {
            outcomes,
            coordinator_apply_attempts,
            coordinator_apply_micros,
            read_set_conflicts,
        };
    }

    let mut wal_bytes = 0usize;
    let requires_sync = sequenced
        .iter()
        .any(|c| matches!(c.request.envelope.write_class, WriteClass::Economic))
        || matches!(state.config.durability_mode, DurabilityMode::Full);
    let mut user_idx = 0usize;
    let mut internal_idx = 0usize;
    while user_idx < sequenced.len() || internal_idx < internal_sequenced.len() {
        let next_is_user = match (
            sequenced.get(user_idx).map(|c| c.seq),
            internal_sequenced.get(internal_idx).map(|c| c.seq),
        ) {
            (Some(user_seq), Some(internal_seq)) => user_seq <= internal_seq,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => false,
        };
        let (seq, ts, payload_type, payload) = if next_is_user {
            let c = &sequenced[user_idx];
            user_idx += 1;
            (c.seq, c.commit_ts_micros, c.payload_type, &c.payload)
        } else {
            let c = &internal_sequenced[internal_idx];
            internal_idx += 1;
            (c.seq, c.commit_ts_micros, c.payload_type, &c.payload)
        };
        wal_bytes = wal_bytes.saturating_add(payload.len());
        if let Err(err) = state
            .wal
            .append_frame_with_sync(seq, ts, payload_type, payload, false)
        {
            let err = AedbError::Io(std::io::Error::other(err.to_string()));
            overwrite_assertion_failures_with_wal_error(
                &mut outcomes,
                &err,
                "epoch aborted before WAL commit",
            );
            for failed in sequenced {
                outcomes.push(EpochOutcome {
                    request: failed.request,
                    result: Err(AedbError::Validation(format!(
                        "epoch aborted before WAL commit: {err}"
                    ))),
                    post_apply_delta: None,
                });
            }
            return EpochProcessResult {
                outcomes,
                coordinator_apply_attempts,
                coordinator_apply_micros,
                read_set_conflicts,
            };
        }
    }
    if requires_sync && let Err(err) = state.wal.sync_active() {
        let err = AedbError::Io(std::io::Error::other(err.to_string()));
        overwrite_assertion_failures_with_wal_error(
            &mut outcomes,
            &err,
            "epoch aborted during WAL sync",
        );
        for failed in sequenced {
            outcomes.push(EpochOutcome {
                request: failed.request,
                result: Err(AedbError::Validation(format!(
                    "epoch aborted during WAL sync: {err}"
                ))),
                post_apply_delta: None,
            });
        }
        return EpochProcessResult {
            outcomes,
            coordinator_apply_attempts,
            coordinator_apply_micros,
            read_set_conflicts,
        };
    }

    let last_user_seq = sequenced.last().map(|c| c.seq).unwrap_or(state.current_seq);
    let last_internal_seq = internal_sequenced
        .last()
        .map(|c| c.seq)
        .unwrap_or(state.current_seq);
    let last_seq = last_user_seq.max(last_internal_seq);
    state.keyspace = working_keyspace;
    state.catalog = working_catalog;
    state.global_unique_index = working_global_unique_index;
    state.idempotency = working_idempotency;
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
                state.pending_batch_bytes = state.pending_batch_bytes.saturating_add(wal_bytes);
                state.pending_batch_max_seq = last_seq;
                if state.pending_batch_bytes >= state.config.batch_max_bytes
                    && state.wal.sync_active().is_ok()
                {
                    state.durable_head_seq = state.pending_batch_max_seq;
                    state.pending_batch_bytes = 0;
                    state.pending_batch_max_seq = state.durable_head_seq;
                }
            }
        }
        DurabilityMode::OsBuffered => {}
    }
    prune_idempotency(state);

    for commit in &sequenced {
        state
            .version_store
            .publish_delta(commit.seq, commit.delta.clone());
    }
    for commit in &internal_sequenced {
        state
            .version_store
            .publish_delta(commit.seq, commit.delta.clone());
    }
    let snapshot_due = now_micros().saturating_sub(state.last_full_snapshot_micros)
        >= state.config.max_snapshot_age_ms.saturating_mul(1000);
    if snapshot_due {
        state.version_store.publish_full(
            state.visible_head_seq,
            state.keyspace.snapshot(),
            state.catalog.snapshot(),
        );
        state.last_full_snapshot_micros = now_micros();
    }

    let mem_estimate = state.keyspace.estimate_memory_bytes();
    if mem_estimate > state.config.max_memory_estimate_bytes {
        warn!(
            mem_estimate,
            max_memory_estimate_bytes = state.config.max_memory_estimate_bytes,
            "aedb memory estimate exceeded threshold"
        );
    }
    if state.wal.should_rotate().is_some() {
        let _ = state
            .wal
            .rotate()
            .map_err(|e| AedbError::Io(std::io::Error::other(e.to_string())));
    }

    let durable_head = state.durable_head_seq;
    for commit in sequenced {
        outcomes.push(EpochOutcome {
            request: commit.request,
            result: Ok(CommitResult {
                commit_seq: commit.seq,
                durable_head_seq: durable_head,
            }),
            post_apply_delta: Some(commit.delta),
        });
    }
    EpochProcessResult {
        outcomes,
        coordinator_apply_attempts,
        coordinator_apply_micros,
        read_set_conflicts,
    }
}

fn overwrite_assertion_failures_with_wal_error(
    outcomes: &mut [EpochOutcome],
    wal_error: &AedbError,
    context: &str,
) {
    for outcome in outcomes {
        if matches!(outcome.result, Err(AedbError::AssertionFailed { .. })) {
            outcome.result = Err(AedbError::Validation(format!("{context}: {wal_error}")));
        }
    }
}

fn is_read_set_conflict_error(err: &AedbError) -> bool {
    match err {
        AedbError::Conflict(msg) => {
            msg.starts_with("read set conflict")
                || msg.starts_with("range read set conflict")
                || msg.starts_with("range structural conflict")
        }
        AedbError::Validation(msg) => {
            msg.starts_with("read set conflict")
                || msg.starts_with("range read set conflict")
                || msg.starts_with("range structural conflict")
        }
        _ => false,
    }
}

const ASSERTION_AUDIT_TABLE: &str = "assertion_audit";
const ASSERTION_AUDIT_SCOPE_ID: &str = "app";

fn build_assertion_audit_commit(
    envelope: &TransactionEnvelope,
    err: &AedbError,
    catalog: &mut Catalog,
    keyspace: &mut Keyspace,
    next_seq: &mut u64,
) -> Option<InternalSequencedCommit> {
    let AedbError::AssertionFailed {
        index,
        assertion,
        actual,
    } = err
    else {
        return None;
    };

    let assertion_json =
        serde_json::to_string(assertion).unwrap_or_else(|_| format!("{assertion:?}"));
    let actual_json = serde_json::to_string(actual).unwrap_or_else(|_| format!("{actual:?}"));
    let caller_id = envelope
        .caller
        .as_ref()
        .map(|c| Value::Text(c.caller_id.clone().into()))
        .unwrap_or(Value::Null);
    let ts_micros = now_micros();

    *next_seq = next_seq.saturating_add(1);
    let commit_seq = *next_seq;
    let mutations = vec![Mutation::Upsert {
        project_id: SYSTEM_PROJECT_ID.to_string(),
        scope_id: ASSERTION_AUDIT_SCOPE_ID.to_string(),
        table_name: ASSERTION_AUDIT_TABLE.to_string(),
        primary_key: vec![Value::Integer(commit_seq as i64)],
        row: Row::from_values(vec![
            Value::Integer(commit_seq as i64),
            Value::Timestamp(ts_micros as i64),
            caller_id,
            Value::Integer(*index as i64),
            Value::Json(assertion_json.into()),
            Value::Json(actual_json.into()),
            Value::Text("assertion_failed".into()),
        ]),
    }];

    for mutation in &mutations {
        if let Err(apply_err) = apply_mutation(catalog, keyspace, mutation.clone(), commit_seq) {
            warn!(error = ?apply_err, "failed to apply assertion audit mutation");
            *next_seq = next_seq.saturating_sub(1);
            return None;
        }
    }

    let payload = match encode_wal_payload(&WalCommitPayload {
        mutations: mutations.clone(),
        assertions: Vec::new(),
        idempotency_key: None,
    }) {
        Ok(payload) => payload,
        Err(encode_err) => {
            warn!(error = ?encode_err, "failed to encode assertion audit payload");
            *next_seq = next_seq.saturating_sub(1);
            return None;
        }
    };
    Some(InternalSequencedCommit {
        seq: commit_seq,
        commit_ts_micros: ts_micros,
        payload_type: payload_type_for_mutations(&mutations),
        payload,
        delta: CommitDelta {
            seq: commit_seq,
            mutations,
        },
    })
}

pub(super) fn is_parallel_single_partition_apply_candidate(
    request: &CommitRequest,
    mutations: &[Mutation],
    catalog: &Catalog,
) -> bool {
    if !request.read_partitions.is_empty() {
        return false;
    }
    if is_cross_partition_write_set(&request.write_partitions) {
        return false;
    }
    let mut ns: Option<NamespaceId> = None;
    for mutation in mutations {
        let Some(current_ns) = namespace_id_for_parallel_mutation(mutation) else {
            return false;
        };
        if let Some(existing) = &ns {
            if existing != &current_ns {
                return false;
            }
        } else {
            ns = Some(current_ns);
        }
        if !is_parallel_mutation_safe(catalog, mutation) {
            return false;
        }
    }
    if request_requires_coordinator(catalog, request, mutations) {
        return false;
    }
    true
}

pub(super) fn request_requires_coordinator(
    catalog: &Catalog,
    request: &CommitRequest,
    mutations: &[Mutation],
) -> bool {
    request.write_partitions.contains(GLOBAL_PARTITION_TOKEN)
        || mutations
            .iter()
            .any(|m| mutation_requires_coordinator(catalog, m))
}

pub(super) fn apply_deferred_parallel_single_partition_commits(
    catalog: &Catalog,
    keyspace: &mut Keyspace,
    runtime: &Arc<ParallelApplyRuntime>,
    sequenced: &[SequencedCommit],
    deferred_indexes: &[usize],
    epoch_apply_timeout_ms: u64,
) -> Result<(), AedbError> {
    let started = Instant::now();
    let mut receivers = Vec::with_capacity(deferred_indexes.len());
    let mut cancellations = Vec::with_capacity(deferred_indexes.len());
    let backend = keyspace.primary_index_backend;
    for idx in deferred_indexes {
        let commit = sequenced
            .get(*idx)
            .expect("deferred index must reference sequenced commit");
        let seq = commit.seq;
        let mutations = commit.delta.mutations.clone();
        let Some(ns_id) = namespace_id_for_parallel_mutations(&mutations) else {
            return Err(AedbError::Validation(
                "parallel apply requires a single namespace".into(),
            ));
        };
        let base_namespace =
            keyspace
                .namespaces
                .get(&ns_id)
                .cloned()
                .unwrap_or_else(|| Namespace {
                    id: ns_id.clone(),
                    tables: Default::default(),
                    kv: KvData::default(),
                });
        let cancel = Arc::new(AtomicBool::new(false));
        let (tx, rx) = std_mpsc::channel::<Result<(NamespaceId, Namespace), AedbError>>();
        receivers.push(rx);
        runtime.submit(ParallelTask {
            namespace_id: ns_id,
            base_namespace,
            mutations,
            commit_seq: seq,
            backend,
            catalog: catalog.clone(),
            cancel: Arc::clone(&cancel),
            response_tx: tx,
        })?;
        cancellations.push(cancel);
    }

    for rx in receivers {
        let elapsed = started.elapsed();
        if elapsed > Duration::from_millis(epoch_apply_timeout_ms) {
            for c in &cancellations {
                c.store(true, Ordering::Relaxed);
            }
            return Err(AedbError::EpochApplyTimeout);
        }
        let remaining = Duration::from_millis(epoch_apply_timeout_ms).saturating_sub(elapsed);
        let result = match rx.recv_timeout(remaining) {
            Ok(result) => result,
            Err(std_mpsc::RecvTimeoutError::Timeout) => {
                for c in &cancellations {
                    c.store(true, Ordering::Relaxed);
                }
                return Err(AedbError::EpochApplyTimeout);
            }
            Err(std_mpsc::RecvTimeoutError::Disconnected) => {
                for c in &cancellations {
                    c.store(true, Ordering::Relaxed);
                }
                return Err(AedbError::ParallelApplyWorkerPanicked);
            }
        };
        let (ns_id, namespace) = result?;
        keyspace.insert_namespace(ns_id, namespace);
    }
    if started.elapsed() > Duration::from_millis(epoch_apply_timeout_ms) {
        for c in &cancellations {
            c.store(true, Ordering::Relaxed);
        }
        return Err(AedbError::EpochApplyTimeout);
    }
    Ok(())
}

pub(super) fn namespace_id_for_parallel_mutations(mutations: &[Mutation]) -> Option<NamespaceId> {
    let mut ns: Option<NamespaceId> = None;
    for mutation in mutations {
        let current = namespace_id_for_parallel_mutation(mutation)?;
        match &ns {
            Some(existing) if existing != &current => return None,
            None => ns = Some(current),
            _ => {}
        }
    }
    ns
}

pub(super) fn namespace_id_for_parallel_mutation(mutation: &Mutation) -> Option<NamespaceId> {
    match mutation {
        Mutation::KvSet {
            project_id,
            scope_id,
            ..
        }
        | Mutation::KvDel {
            project_id,
            scope_id,
            ..
        }
        | Mutation::KvIncU256 {
            project_id,
            scope_id,
            ..
        }
        | Mutation::KvDecU256 {
            project_id,
            scope_id,
            ..
        }
        | Mutation::Insert {
            project_id,
            scope_id,
            ..
        }
        | Mutation::InsertBatch {
            project_id,
            scope_id,
            ..
        }
        | Mutation::Upsert {
            project_id,
            scope_id,
            ..
        }
        | Mutation::UpsertBatch {
            project_id,
            scope_id,
            ..
        }
        | Mutation::Delete {
            project_id,
            scope_id,
            ..
        }
        | Mutation::TableIncU256 {
            project_id,
            scope_id,
            ..
        }
        | Mutation::TableDecU256 {
            project_id,
            scope_id,
            ..
        } => Some(NamespaceId::project_scope(project_id, scope_id)),
        _ => None,
    }
}

pub(super) fn is_parallel_mutation_safe(catalog: &Catalog, mutation: &Mutation) -> bool {
    match mutation {
        Mutation::KvSet { .. }
        | Mutation::KvDel { .. }
        | Mutation::KvIncU256 { .. }
        | Mutation::KvDecU256 { .. } => true,
        Mutation::Insert {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::InsertBatch {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::Upsert {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::UpsertBatch {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::Delete {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::DeleteWhere {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::UpdateWhere {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::UpdateWhereExpr {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::TableIncU256 {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::TableDecU256 {
            project_id,
            scope_id,
            table_name,
            ..
        } => is_parallel_table_safe(catalog, project_id, scope_id, table_name),
        _ => false,
    }
}

pub(super) fn is_parallel_table_safe(
    catalog: &Catalog,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
) -> bool {
    let ns = namespace_key(project_id, scope_id);
    let Some(schema) = catalog.tables.get(&(ns.clone(), table_name.to_string())) else {
        return false;
    };
    if !schema.foreign_keys.is_empty() {
        return false;
    }
    for ((dep_ns, _), dep_schema) in &catalog.tables {
        for fk in &dep_schema.foreign_keys {
            if namespace_key(&fk.references_project_id, &fk.references_scope_id) == ns
                && fk.references_table == table_name
            {
                let _ = dep_ns;
                return false;
            }
        }
    }
    true
}

pub(super) fn mutation_requires_coordinator(catalog: &Catalog, mutation: &Mutation) -> bool {
    match mutation {
        Mutation::Ddl(_) => true,
        Mutation::UpsertOnConflict { .. } | Mutation::UpsertBatchOnConflict { .. } => true,
        Mutation::Insert {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::InsertBatch {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::Upsert {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::UpsertBatch {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::Delete {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::TableIncU256 {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::TableDecU256 {
            project_id,
            scope_id,
            table_name,
            ..
        } => table_has_global_unique_index(catalog, project_id, scope_id, table_name),
        _ => false,
    }
}

pub(super) fn table_has_global_unique_index(
    catalog: &Catalog,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
) -> bool {
    if project_id != "_global" {
        return false;
    }
    let ns = namespace_key(project_id, scope_id);
    catalog.indexes.iter().any(|((idx_ns, idx_table, _), def)| {
        (idx_ns == &ns || idx_ns.starts_with("_global::"))
            && idx_table == table_name
            && matches!(
                def.index_type,
                crate::catalog::schema::IndexType::UniqueHash
            )
    })
}

pub(super) fn enforce_global_unique_scope_invariants(
    catalog: &Catalog,
    keyspace: &Keyspace,
    mutation: &Mutation,
) -> Result<(), AedbError> {
    match mutation {
        Mutation::Insert {
            project_id,
            scope_id,
            table_name,
            primary_key,
            row,
        } => enforce_global_unique_for_row(
            catalog,
            keyspace,
            project_id,
            scope_id,
            table_name,
            primary_key,
            row,
        ),
        Mutation::Upsert {
            project_id,
            scope_id,
            table_name,
            primary_key,
            row,
        } => enforce_global_unique_for_row(
            catalog,
            keyspace,
            project_id,
            scope_id,
            table_name,
            primary_key,
            row,
        ),
        Mutation::UpsertOnConflict {
            project_id,
            scope_id,
            table_name,
            row,
            ..
        } => {
            let schema = table_schema_for(catalog, project_id, scope_id, table_name)?;
            let pk = extract_pk_from_row(&schema, row)?;
            enforce_global_unique_for_row(
                catalog, keyspace, project_id, scope_id, table_name, &pk, row,
            )
        }
        Mutation::UpsertBatch {
            project_id,
            scope_id,
            table_name,
            rows,
        } => {
            let schema = table_schema_for(catalog, project_id, scope_id, table_name)?;
            for row in rows {
                let pk = extract_pk_from_row(&schema, row)?;
                enforce_global_unique_for_row(
                    catalog, keyspace, project_id, scope_id, table_name, &pk, row,
                )?;
            }
            Ok(())
        }
        Mutation::InsertBatch {
            project_id,
            scope_id,
            table_name,
            rows,
        } => {
            let schema = table_schema_for(catalog, project_id, scope_id, table_name)?;
            for row in rows {
                let pk = extract_pk_from_row(&schema, row)?;
                enforce_global_unique_for_row(
                    catalog, keyspace, project_id, scope_id, table_name, &pk, row,
                )?;
            }
            Ok(())
        }
        Mutation::UpsertBatchOnConflict {
            project_id,
            scope_id,
            table_name,
            rows,
            ..
        } => {
            let schema = table_schema_for(catalog, project_id, scope_id, table_name)?;
            for row in rows {
                let pk = extract_pk_from_row(&schema, row)?;
                enforce_global_unique_for_row(
                    catalog, keyspace, project_id, scope_id, table_name, &pk, row,
                )?;
            }
            Ok(())
        }
        Mutation::TableIncU256 {
            project_id,
            scope_id,
            table_name,
            primary_key,
            column,
            amount_be,
        }
        | Mutation::TableDecU256 {
            project_id,
            scope_id,
            table_name,
            primary_key,
            column,
            amount_be,
        } => {
            let schema = table_schema_for(catalog, project_id, scope_id, table_name)?;
            let row_key = EncodedKey::from_values(primary_key);
            let existing = keyspace
                .table_by_namespace_key(&namespace_key(project_id, scope_id), table_name)
                .and_then(|t| t.rows.get(&row_key))
                .ok_or_else(|| AedbError::Validation("row not found".into()))?;
            let Some(col_idx) = schema.columns.iter().position(|c| c.name == *column) else {
                return Err(AedbError::Validation(format!("column not found: {column}")));
            };
            let current = match existing.values.get(col_idx) {
                Some(Value::U256(bytes)) => U256::from_big_endian(bytes.as_slice()),
                _ => {
                    return Err(AedbError::Validation(format!(
                        "column {column} must be U256"
                    )));
                }
            };
            let amount = U256::from_big_endian(amount_be);
            let next = if matches!(mutation, Mutation::TableIncU256 { .. }) {
                current.saturating_add(amount)
            } else if current < amount {
                return Err(AedbError::Underflow);
            } else {
                current - amount
            };
            let mut next_be = [0u8; 32];
            next.to_big_endian(&mut next_be);
            let mut next_row = existing.clone();
            next_row.values[col_idx] = Value::U256(next_be);
            enforce_global_unique_for_row(
                catalog,
                keyspace,
                project_id,
                scope_id,
                table_name,
                primary_key,
                &next_row,
            )
        }
        Mutation::DeleteWhere {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::UpdateWhere {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::UpdateWhereExpr {
            project_id,
            scope_id,
            table_name,
            ..
        } => {
            if table_has_global_unique_index(catalog, project_id, scope_id, table_name) {
                return Err(AedbError::Validation(
                    "predicate mutations are not supported on tables with global unique indexes"
                        .into(),
                ));
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

pub(super) fn enforce_global_unique_for_row(
    catalog: &Catalog,
    keyspace: &Keyspace,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    incoming_pk: &[Value],
    incoming_row: &Row,
) -> Result<(), AedbError> {
    if project_id != "_global" {
        return Ok(());
    }
    let defs = global_unique_defs_for_project_table(catalog, table_name);
    if defs.is_empty() {
        return Ok(());
    }
    let current_ns = namespace_key(project_id, scope_id);
    let incoming_pk_encoded = EncodedKey::from_values(incoming_pk);
    let current_schema = table_schema_for(catalog, project_id, scope_id, table_name)?;

    for def in defs {
        let incoming_index_key =
            extract_index_key_encoded(incoming_row, &current_schema, &def.columns)?;
        for (ns_id, ns_data) in keyspace.namespaces.iter() {
            let NamespaceId::Project(ns_key) = ns_id else {
                continue;
            };
            if !ns_key.starts_with("_global::") {
                continue;
            }
            let Some(table) = ns_data.tables.get(table_name) else {
                continue;
            };
            let Some(schema) = catalog
                .tables
                .get(&(ns_key.clone(), table_name.to_string()))
            else {
                continue;
            };
            if def
                .columns
                .iter()
                .any(|col| !schema.columns.iter().any(|c| c.name == *col))
            {
                continue;
            }
            for (pk, row) in &table.rows {
                if ns_key.as_str() == current_ns && pk == &incoming_pk_encoded {
                    continue;
                }
                let existing_key = extract_index_key_encoded(row, schema, &def.columns)?;
                if existing_key == incoming_index_key {
                    return Err(AedbError::Validation(format!(
                        "global unique constraint violation on {} ({})",
                        table_name, def.index_name
                    )));
                }
            }
        }
    }
    Ok(())
}

pub(super) fn table_schema_for(
    catalog: &Catalog,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
) -> Result<TableSchema, AedbError> {
    let ns = namespace_key(project_id, scope_id);
    catalog
        .tables
        .get(&(ns, table_name.to_string()))
        .cloned()
        .ok_or_else(|| AedbError::Validation("table missing".into()))
}

pub(super) fn extract_pk_from_row(
    schema: &TableSchema,
    row: &Row,
) -> Result<Vec<Value>, AedbError> {
    let mut pk = Vec::with_capacity(schema.primary_key.len());
    for col in &schema.primary_key {
        let idx = schema
            .columns
            .iter()
            .position(|c| c.name == *col)
            .ok_or_else(|| AedbError::Validation(format!("primary key column missing: {col}")))?;
        pk.push(row.values[idx].clone());
    }
    Ok(pk)
}

#[derive(Clone)]
pub(super) struct GlobalUniqueDef {
    index_name: String,
    columns: Vec<String>,
}

pub(super) fn global_unique_defs_for_project_table(
    catalog: &Catalog,
    table_name: &str,
) -> Vec<GlobalUniqueDef> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for ((ns, t, idx_name), def) in &catalog.indexes {
        if !ns.starts_with("_global::") || t != table_name {
            continue;
        }
        if !matches!(
            def.index_type,
            crate::catalog::schema::IndexType::UniqueHash
        ) {
            continue;
        }
        let dedupe = format!("{idx_name}:{}", def.columns.join(","));
        if seen.insert(dedupe) {
            out.push(GlobalUniqueDef {
                index_name: idx_name.clone(),
                columns: def.columns.clone(),
            });
        }
    }
    out
}

pub(super) fn is_cross_partition_request(request: &CommitRequest) -> bool {
    is_cross_partition_write_set(&request.write_partitions)
}

pub(super) fn canonical_partition_order(write_partitions: &HashSet<String>) -> Vec<String> {
    let mut ordered: Vec<String> = write_partitions.iter().cloned().collect();
    ordered.sort();
    ordered
}

pub(super) struct CoordinatorApplyOptions<'a> {
    pub coordinator_locking_enabled: bool,
    pub lock_manager: &'a Arc<CoordinatorLockManager>,
    pub global_unique_index_enabled: bool,
    pub partition_lock_timeout_ms: u64,
}

pub(super) fn apply_via_coordinator(
    catalog: &mut Catalog,
    keyspace: &mut Keyspace,
    global_unique_index: &mut GlobalUniqueIndexState,
    mutations: &[Mutation],
    commit_seq: u64,
    ordered_partitions: &[String],
    options: CoordinatorApplyOptions<'_>,
) -> Result<(), AedbError> {
    let started = Instant::now();
    let _lock_guard = if options.coordinator_locking_enabled {
        Some(options.lock_manager.acquire_all(
            ordered_partitions,
            Duration::from_millis(options.partition_lock_timeout_ms),
        )?)
    } else {
        None
    };
    for mutation in mutations {
        if started.elapsed() > Duration::from_millis(options.partition_lock_timeout_ms) {
            return Err(AedbError::PartitionLockTimeout);
        }
        coordinator_test_delay();
        if started.elapsed() > Duration::from_millis(options.partition_lock_timeout_ms) {
            return Err(AedbError::PartitionLockTimeout);
        }
        if options.global_unique_index_enabled {
            global_unique_index.enforce_and_apply(catalog, keyspace, mutation)?;
        } else {
            enforce_global_unique_scope_invariants(catalog, keyspace, mutation)?;
        }
        apply_mutation(catalog, keyspace, mutation.clone(), commit_seq)?;
        if matches!(mutation, Mutation::Ddl(_)) {
            *global_unique_index = GlobalUniqueIndexState::from_snapshot(catalog, keyspace)?;
        }
    }
    Ok(())
}

#[inline]
pub(super) fn coordinator_test_delay() {
    #[cfg(test)]
    {
        let delay = COORDINATOR_TEST_DELAY_MS.load(Ordering::Relaxed);
        if delay > 0 {
            std::thread::sleep(Duration::from_millis(delay));
        }
    }
}

#[cfg(test)]
#[inline]
pub(super) fn parallel_worker_test_hook_for_mutation(mutation: &Mutation) {
    const PANIC_KEY: &[u8] = b"__panic_parallel_worker__";
    const SLOW_KEY: &[u8] = b"__slow_parallel_worker__";
    match mutation {
        Mutation::KvSet { key, .. } | Mutation::KvDel { key, .. } => {
            if key.as_slice() == PANIC_KEY {
                panic!("parallel apply worker injected panic");
            }
            if key.as_slice() == SLOW_KEY {
                std::thread::sleep(Duration::from_millis(25));
            }
        }
        _ => {}
    }
}

#[cfg(not(test))]
#[inline]
pub(super) fn parallel_worker_test_hook_for_mutation(_mutation: &Mutation) {}

pub(super) fn encode_wal_payload(payload: &WalCommitPayload) -> Result<Vec<u8>, AedbError> {
    rmp_serde::to_vec(payload).map_err(|e| AedbError::Encode(e.to_string()))
}

pub(super) fn payload_type_for_mutations(mutations: &[Mutation]) -> u8 {
    let all_ddl = mutations.iter().all(|m| matches!(m, Mutation::Ddl(_)));
    if all_ddl {
        return 0x02;
    }
    let all_kv = mutations.iter().all(|m| {
        matches!(
            m,
            Mutation::KvSet { .. }
                | Mutation::KvDel { .. }
                | Mutation::KvIncU256 { .. }
                | Mutation::KvDecU256 { .. }
        )
    });
    if all_kv { 0x04 } else { 0x01 }
}

pub(super) fn derive_write_partitions_with_fk_expansion(
    catalog: &Catalog,
    mutations: &[Mutation],
) -> HashSet<String> {
    let mut out = HashSet::new();
    let mut touched_tables = HashSet::new();
    for mutation in mutations {
        match mutation {
            Mutation::Insert {
                project_id,
                scope_id,
                table_name,
                ..
            }
            | Mutation::InsertBatch {
                project_id,
                scope_id,
                table_name,
                ..
            }
            | Mutation::Upsert {
                project_id,
                scope_id,
                table_name,
                ..
            }
            | Mutation::UpsertBatch {
                project_id,
                scope_id,
                table_name,
                ..
            }
            | Mutation::UpsertOnConflict {
                project_id,
                scope_id,
                table_name,
                ..
            }
            | Mutation::UpsertBatchOnConflict {
                project_id,
                scope_id,
                table_name,
                ..
            }
            | Mutation::Delete {
                project_id,
                scope_id,
                table_name,
                ..
            }
            | Mutation::DeleteWhere {
                project_id,
                scope_id,
                table_name,
                ..
            }
            | Mutation::UpdateWhere {
                project_id,
                scope_id,
                table_name,
                ..
            }
            | Mutation::UpdateWhereExpr {
                project_id,
                scope_id,
                table_name,
                ..
            }
            | Mutation::TableIncU256 {
                project_id,
                scope_id,
                table_name,
                ..
            }
            | Mutation::TableDecU256 {
                project_id,
                scope_id,
                table_name,
                ..
            } => {
                let ns = namespace_key(project_id, scope_id);
                out.insert(table_partition_token(&ns, table_name));
                touched_tables.insert((ns, table_name.clone()));
            }
            Mutation::KvSet {
                project_id,
                scope_id,
                key,
                ..
            }
            | Mutation::KvDel {
                project_id,
                scope_id,
                key,
                ..
            }
            | Mutation::KvIncU256 {
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
            } => {
                let ns = namespace_key(project_id, scope_id);
                out.insert(kv_key_partition_token(&ns, key));
            }
            Mutation::Ddl(_) => {
                out.insert(GLOBAL_PARTITION_TOKEN.to_string());
            }
        }
    }

    for (target_ns, target_table) in touched_tables {
        for ((dep_ns, dep_table), dep_schema) in &catalog.tables {
            for fk in &dep_schema.foreign_keys {
                if namespace_key(&fk.references_project_id, &fk.references_scope_id) == target_ns
                    && fk.references_table == target_table
                {
                    out.insert(table_partition_token(dep_ns, dep_table));
                    break;
                }
            }
        }
    }

    out
}

#[cfg(test)]
pub(super) fn derive_write_partitions(mutations: &[Mutation]) -> HashSet<String> {
    derive_write_partitions_with_fk_expansion(&Catalog::default(), mutations)
}

pub(super) fn derive_read_partitions(envelope: &TransactionEnvelope) -> HashSet<String> {
    let mut out = HashSet::new();
    for entry in &envelope.read_set.points {
        match &entry.key {
            ReadKey::TableRow {
                project_id,
                scope_id,
                table_name,
                ..
            } => {
                let ns = namespace_key(project_id, scope_id);
                out.insert(table_partition_token(&ns, table_name));
            }
            ReadKey::KvKey {
                project_id,
                scope_id,
                key,
                ..
            } => {
                let ns = namespace_key(project_id, scope_id);
                out.insert(kv_key_partition_token(&ns, key));
            }
        }
    }
    for range in &envelope.read_set.ranges {
        match &range.range {
            ReadRange::TableRange {
                project_id,
                scope_id,
                table_name,
                ..
            } => {
                let ns = namespace_key(project_id, scope_id);
                out.insert(table_partition_token(&ns, table_name));
            }
            ReadRange::KvRange {
                project_id,
                scope_id,
                ..
            } => {
                let ns = namespace_key(project_id, scope_id);
                out.insert(kv_namespace_partition_token(&ns));
            }
        }
    }
    derive_tokens_for_assertions(&mut out, &envelope.assertions);
    out
}

fn derive_tokens_for_assertions(out: &mut HashSet<String>, assertions: &[ReadAssertion]) {
    for assertion in assertions {
        derive_tokens_for_assertion(out, assertion);
    }
}

fn derive_tokens_for_assertion(out: &mut HashSet<String>, assertion: &ReadAssertion) {
    match assertion {
        ReadAssertion::KeyEquals {
            project_id,
            scope_id,
            key,
            ..
        }
        | ReadAssertion::KeyCompare {
            project_id,
            scope_id,
            key,
            ..
        }
        | ReadAssertion::KeyExists {
            project_id,
            scope_id,
            key,
            ..
        }
        | ReadAssertion::KeyVersion {
            project_id,
            scope_id,
            key,
            ..
        } => {
            let ns = namespace_key(project_id, scope_id);
            out.insert(kv_key_partition_token(&ns, key));
        }
        ReadAssertion::RowVersion {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | ReadAssertion::RowExists {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | ReadAssertion::RowColumnCompare {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | ReadAssertion::CountCompare {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | ReadAssertion::SumCompare {
            project_id,
            scope_id,
            table_name,
            ..
        } => {
            let ns = namespace_key(project_id, scope_id);
            out.insert(table_partition_token(&ns, table_name));
        }
        ReadAssertion::All(inner) | ReadAssertion::Any(inner) => {
            derive_tokens_for_assertions(out, inner);
        }
        ReadAssertion::Not(inner) => derive_tokens_for_assertion(out, inner),
    }
}

fn table_partition_token(namespace: &str, table_name: &str) -> String {
    format!("t:{namespace}:{table_name}")
}

fn kv_key_partition_token(namespace: &str, key: &[u8]) -> String {
    let mut out = String::with_capacity(key.len().saturating_mul(2) + namespace.len() + 3);
    out.push_str("k:");
    out.push_str(namespace);
    out.push(':');
    for b in key {
        let hi = (b >> 4) & 0x0f;
        let lo = b & 0x0f;
        out.push(char::from(if hi < 10 {
            b'0' + hi
        } else {
            b'a' + (hi - 10)
        }));
        out.push(char::from(if lo < 10 {
            b'0' + lo
        } else {
            b'a' + (lo - 10)
        }));
    }
    out
}

fn kv_namespace_partition_token(namespace: &str) -> String {
    format!("kns:{namespace}")
}

fn is_cross_partition_write_set(write_set: &HashSet<String>) -> bool {
    if write_set.contains(GLOBAL_PARTITION_TOKEN) {
        return true;
    }
    let mut first_ns: Option<&str> = None;
    for token in write_set {
        let Some(ns) = token_namespace(token) else {
            continue;
        };
        if let Some(existing) = first_ns {
            if existing != ns {
                return true;
            }
        } else {
            first_ns = Some(ns);
        }
    }
    false
}

fn token_namespace(token: &str) -> Option<&str> {
    if let Some(rest) = token.strip_prefix("kns:") {
        return Some(rest);
    }
    if let Some(rest) = token.strip_prefix("t:")
        && let Some((ns, _table)) = rest.rsplit_once(':')
    {
        return Some(ns);
    }
    if let Some(rest) = token.strip_prefix("k:")
        && let Some((ns, _key)) = rest.rsplit_once(':')
    {
        return Some(ns);
    }
    None
}

pub(super) fn revalidate_read_set_for_keyspace(
    keyspace: &Keyspace,
    envelope: &TransactionEnvelope,
) -> Result<(), AedbError> {
    for entry in &envelope.read_set.points {
        let current_version = match &entry.key {
            ReadKey::TableRow {
                project_id,
                scope_id,
                table_name,
                primary_key,
            } => keyspace.get_row_version(project_id, scope_id, table_name, primary_key),
            ReadKey::KvKey {
                project_id,
                scope_id,
                key,
            } => keyspace.kv_version(project_id, scope_id, key),
        };
        if current_version > envelope.base_seq || current_version > entry.version_at_read {
            return Err(AedbError::Conflict(format!(
                "read set conflict at seq {current_version}"
            )));
        }
    }
    for range in &envelope.read_set.ranges {
        let (current_max_version, current_structural_version) = match &range.range {
            ReadRange::TableRange {
                project_id,
                scope_id,
                table_name,
                start,
                end,
            } => (
                keyspace.max_row_version_in_encoded_range(
                    project_id,
                    scope_id,
                    table_name,
                    value_bound_to_encoded(start),
                    value_bound_to_encoded(end),
                ),
                keyspace.table_structural_version(project_id, scope_id, table_name),
            ),
            ReadRange::KvRange {
                project_id,
                scope_id,
                start,
                end,
            } => (
                keyspace.max_kv_version_in_range(
                    project_id,
                    scope_id,
                    bytes_bound_to_vec(start),
                    bytes_bound_to_vec(end),
                ),
                keyspace.kv_structural_version(project_id, scope_id),
            ),
        };
        if current_max_version > envelope.base_seq
            || current_max_version > range.max_version_at_read
        {
            return Err(AedbError::Conflict(format!(
                "range read set conflict at seq {current_max_version}"
            )));
        }
        if current_structural_version > envelope.base_seq
            || current_structural_version > range.structural_version_at_read
        {
            return Err(AedbError::Conflict(format!(
                "range structural conflict at seq {current_structural_version}"
            )));
        }
    }
    Ok(())
}

pub(super) fn value_bound_to_encoded(
    bound: &ReadBound<Vec<crate::catalog::types::Value>>,
) -> Bound<EncodedKey> {
    match bound {
        ReadBound::Unbounded => Bound::Unbounded,
        ReadBound::Included(values) => Bound::Included(EncodedKey::from_values(values)),
        ReadBound::Excluded(values) => Bound::Excluded(EncodedKey::from_values(values)),
    }
}

pub(super) fn bytes_bound_to_vec(bound: &ReadBound<Vec<u8>>) -> Bound<Vec<u8>> {
    match bound {
        ReadBound::Unbounded => Bound::Unbounded,
        ReadBound::Included(value) => Bound::Included(value.clone()),
        ReadBound::Excluded(value) => Bound::Excluded(value.clone()),
    }
}

fn augment_mutations_with_caller(mutations: &mut [Mutation], caller: Option<&CallerContext>) {
    let Some(caller) = caller else {
        return;
    };
    for mutation in mutations {
        if let Mutation::Ddl(crate::catalog::DdlOperation::GrantPermission { actor_id, .. })
        | Mutation::Ddl(crate::catalog::DdlOperation::RevokePermission { actor_id, .. })
        | Mutation::Ddl(crate::catalog::DdlOperation::SetReadPolicy { actor_id, .. })
        | Mutation::Ddl(crate::catalog::DdlOperation::ClearReadPolicy { actor_id, .. })
        | Mutation::Ddl(crate::catalog::DdlOperation::TransferOwnership { actor_id, .. }) =
            mutation
        {
            *actor_id = Some(caller.caller_id.clone());
        }
    }
}

pub(super) fn prune_idempotency(state: &mut ExecutorState) {
    let now_micros = now_micros();
    let window = state.config.idempotency_window_seconds * 1_000_000;
    state
        .idempotency
        .retain(|_, rec| now_micros.saturating_sub(rec.recorded_at_micros) <= window);
}

pub(super) fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

pub(super) fn refresh_async_indexes(
    state: &mut ExecutorState,
    pending_delta: Option<&CommitDelta>,
) -> Result<bool, AedbError> {
    let target_seq = state.visible_head_seq;
    let mut changed = false;
    for ((ns, table_name, index_name), def) in &state.catalog.async_indexes {
        let key = (
            NamespaceId::Project(ns.clone()),
            table_name.clone(),
            index_name.clone(),
        );
        let current = state
            .keyspace
            .async_indexes
            .get(&key)
            .map(|v| v.materialized_seq)
            .unwrap_or(0);
        if current >= target_seq {
            continue;
        }
        let schema = state
            .catalog
            .tables
            .get(&(ns.clone(), table_name.clone()))
            .ok_or_else(|| AedbError::Validation("table missing".into()))?;
        let mut projection_rows = state
            .keyspace
            .async_indexes
            .get(&key)
            .map(|v| v.rows.clone())
            .unwrap_or_default();
        let mut materialized_seq = current;

        if current == 0
            && projection_rows.is_empty()
            && let Some(table) = state.keyspace.table_by_namespace_key(ns, table_name)
        {
            for (pk, row) in &table.rows {
                let projected = project_row(row, schema, &def.projected_columns)?;
                projection_rows.insert(pk.clone(), projected);
            }
            materialized_seq = target_seq;
        }

        if materialized_seq < target_seq {
            let Some(deltas) = state
                .version_store
                .deltas_since(materialized_seq, target_seq)
            else {
                warn!(
                    from_seq = materialized_seq,
                    to_seq = target_seq,
                    "async projection delta history evicted; rebuilding projection"
                );
                projection_rows.clear();
                if let Some(table) = state.keyspace.table_by_namespace_key(ns, table_name) {
                    for (pk, row) in &table.rows {
                        let projected = project_row(row, schema, &def.projected_columns)?;
                        projection_rows.insert(pk.clone(), projected);
                    }
                }
                materialized_seq = target_seq;
                state.keyspace.insert_async_projection(
                    key.0.clone(),
                    key.1.clone(),
                    key.2.clone(),
                    crate::storage::keyspace::AsyncProjectionData {
                        rows: projection_rows,
                        materialized_seq,
                    },
                );
                changed = true;
                continue;
            };

            let mut requires_rebuild = false;
            for delta in deltas {
                for mutation in &delta.mutations {
                    if matches!(
                        mutation,
                        Mutation::TableIncU256 {
                            project_id,
                            scope_id,
                            table_name: mutation_table,
                            ..
                        } | Mutation::TableDecU256 {
                            project_id,
                            scope_id,
                            table_name: mutation_table,
                            ..
                        } | Mutation::DeleteWhere {
                            project_id,
                            scope_id,
                            table_name: mutation_table,
                            ..
                        } | Mutation::UpdateWhere {
                            project_id,
                            scope_id,
                            table_name: mutation_table,
                            ..
                        } | Mutation::UpdateWhereExpr {
                            project_id,
                            scope_id,
                            table_name: mutation_table,
                            ..
                        } if namespace_key(project_id, scope_id) == *ns
                            && mutation_table == table_name
                    ) {
                        requires_rebuild = true;
                        break;
                    }
                    apply_projection_delta(
                        mutation,
                        ns,
                        table_name,
                        schema,
                        &def.projected_columns,
                        &mut projection_rows,
                    )?;
                }
                materialized_seq = delta.seq;
            }

            if let Some(delta) = pending_delta
                && delta.seq > materialized_seq
                && delta.seq <= target_seq
            {
                for mutation in &delta.mutations {
                    apply_projection_delta(
                        mutation,
                        ns,
                        table_name,
                        schema,
                        &def.projected_columns,
                        &mut projection_rows,
                    )?;
                }
                if requires_rebuild {
                    break;
                }
                materialized_seq = delta.seq;
            }

            if requires_rebuild {
                projection_rows.clear();
                if let Some(table) = state.keyspace.table_by_namespace_key(ns, table_name) {
                    for (pk, row) in &table.rows {
                        let projected = project_row(row, schema, &def.projected_columns)?;
                        projection_rows.insert(pk.clone(), projected);
                    }
                }
                materialized_seq = target_seq;
            }
        }

        state.keyspace.insert_async_projection(
            key.0,
            key.1,
            key.2,
            crate::storage::keyspace::AsyncProjectionData {
                rows: projection_rows,
                materialized_seq,
            },
        );
        changed = true;
    }
    if refresh_kv_projection_tables(state, pending_delta)? {
        changed = true;
    }
    Ok(changed)
}

fn refresh_kv_projection_tables(
    state: &mut ExecutorState,
    pending_delta: Option<&CommitDelta>,
) -> Result<bool, AedbError> {
    let target_seq = state.visible_head_seq;
    let mut changed = false;
    let projections: Vec<(String, String, String)> = state
        .catalog
        .kv_projections
        .iter()
        .map(|((project_id, scope_id), def)| {
            (project_id.clone(), scope_id.clone(), def.table_name.clone())
        })
        .collect();
    for (project_id, scope_id, table_name) in projections {
        let ns = namespace_key(&project_id, &scope_id);
        let current = state
            .keyspace
            .table_structural_version(&project_id, &scope_id, &table_name);
        if current >= target_seq {
            continue;
        }

        if current == 0 {
            rebuild_kv_projection_rows(state, &project_id, &scope_id, &table_name, target_seq)?;
            changed = true;
            continue;
        }

        let Some(deltas) = state.version_store.deltas_since(current, target_seq) else {
            rebuild_kv_projection_rows(state, &project_id, &scope_id, &table_name, target_seq)?;
            changed = true;
            continue;
        };
        for delta in deltas {
            for mutation in &delta.mutations {
                apply_kv_projection_delta(
                    state,
                    mutation,
                    &ns,
                    &project_id,
                    &scope_id,
                    &table_name,
                )?;
            }
            set_kv_projection_structural_version(
                state,
                &project_id,
                &scope_id,
                &table_name,
                delta.seq,
            );
        }
        if let Some(delta) = pending_delta
            && delta.seq > current
            && delta.seq <= target_seq
        {
            for mutation in &delta.mutations {
                apply_kv_projection_delta(
                    state,
                    mutation,
                    &ns,
                    &project_id,
                    &scope_id,
                    &table_name,
                )?;
            }
            set_kv_projection_structural_version(
                state,
                &project_id,
                &scope_id,
                &table_name,
                delta.seq,
            );
        }
        changed = true;
    }
    Ok(changed)
}

fn rebuild_kv_projection_rows(
    state: &mut ExecutorState,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    materialized_seq: u64,
) -> Result<(), AedbError> {
    let keys: Vec<Vec<u8>> = state
        .keyspace
        .kv_scan_prefix(project_id, scope_id, &[], usize::MAX)
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    let ns = namespace_key(project_id, scope_id);
    if let Some(table) = state.keyspace.table_by_namespace_key_mut(&ns, table_name) {
        table.rows.clear();
        table.row_versions.clear();
        table.pk_hash.clear();
        table.row_cache.clear();
        table.row_versions_cache.clear();
        table.structural_version = materialized_seq;
    } else {
        state
            .keyspace
            .table_mut(project_id, scope_id, table_name)
            .structural_version = materialized_seq;
    }
    for key in keys {
        if let Some(entry) = state.keyspace.kv_get(project_id, scope_id, &key).cloned() {
            upsert_kv_projection_row(
                state,
                project_id,
                scope_id,
                table_name,
                KvProjectionRow {
                    key,
                    value: entry.value,
                    commit_seq: entry.version,
                    updated_at: materialized_seq,
                },
            )?;
        }
    }
    Ok(())
}

fn apply_kv_projection_delta(
    state: &mut ExecutorState,
    mutation: &Mutation,
    namespace: &str,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
) -> Result<(), AedbError> {
    match mutation {
        Mutation::KvSet {
            project_id: p,
            scope_id: s,
            key,
            value,
        } if namespace_key(p, s) == namespace => {
            upsert_kv_projection_row(
                state,
                project_id,
                scope_id,
                table_name,
                KvProjectionRow {
                    key: key.clone(),
                    value: value.clone(),
                    commit_seq: state.keyspace.kv_version(project_id, scope_id, key),
                    updated_at: now_micros(),
                },
            )?;
        }
        Mutation::KvIncU256 {
            project_id: p,
            scope_id: s,
            key,
            ..
        }
        | Mutation::KvDecU256 {
            project_id: p,
            scope_id: s,
            key,
            ..
        } if namespace_key(p, s) == namespace => {
            if let Some(entry) = state.keyspace.kv_get(project_id, scope_id, key).cloned() {
                upsert_kv_projection_row(
                    state,
                    project_id,
                    scope_id,
                    table_name,
                    KvProjectionRow {
                        key: key.clone(),
                        value: entry.value,
                        commit_seq: entry.version,
                        updated_at: now_micros(),
                    },
                )?;
            }
        }
        Mutation::KvDel {
            project_id: p,
            scope_id: s,
            key,
        } if namespace_key(p, s) == namespace => {
            let _ = state.keyspace.delete_row(
                project_id,
                scope_id,
                table_name,
                &[crate::catalog::types::Value::Blob(key.clone())],
                now_micros(),
            );
        }
        Mutation::Ddl(crate::catalog::DdlOperation::DropScope {
            project_id: p,
            scope_id: s,
            if_exists: true,
            ..
        }) if namespace_key(p, s) == namespace => {
            state.keyspace.drop_table(project_id, scope_id, table_name);
        }
        Mutation::Ddl(crate::catalog::DdlOperation::DropProject { project_id: p, .. })
            if namespace.starts_with(&(p.to_string() + "::")) =>
        {
            state.keyspace.drop_table(project_id, scope_id, table_name);
        }
        Mutation::Ddl(crate::catalog::DdlOperation::DisableKvProjection {
            project_id: p,
            scope_id: s,
        }) if namespace_key(p, s) == namespace => {
            state.keyspace.drop_table(project_id, scope_id, table_name);
        }
        _ => {}
    }
    Ok(())
}

struct KvProjectionRow {
    key: Vec<u8>,
    value: Vec<u8>,
    commit_seq: u64,
    updated_at: u64,
}

fn upsert_kv_projection_row(
    state: &mut ExecutorState,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    row: KvProjectionRow,
) -> Result<(), AedbError> {
    let commit_seq_i64 = i64::try_from(row.commit_seq)
        .map_err(|_| AedbError::Validation("commit_seq overflow".into()))?;
    let updated_at = i64::try_from(row.updated_at)
        .map_err(|_| AedbError::Validation("updated_at overflow".into()))?;
    state.keyspace.upsert_row(
        project_id,
        scope_id,
        table_name,
        vec![crate::catalog::types::Value::Blob(row.key.clone())],
        crate::catalog::types::Row {
            values: vec![
                crate::catalog::types::Value::Text(project_id.into()),
                crate::catalog::types::Value::Text(scope_id.into()),
                crate::catalog::types::Value::Blob(row.key),
                crate::catalog::types::Value::Blob(row.value),
                crate::catalog::types::Value::Integer(commit_seq_i64),
                crate::catalog::types::Value::Timestamp(updated_at),
            ],
        },
        row.commit_seq,
    );
    Ok(())
}

fn set_kv_projection_structural_version(
    state: &mut ExecutorState,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    seq: u64,
) {
    let ns = namespace_key(project_id, scope_id);
    if let Some(table) = state.keyspace.table_by_namespace_key_mut(&ns, table_name) {
        table.structural_version = seq;
    }
}

pub(super) fn project_row(
    row: &crate::catalog::types::Row,
    schema: &TableSchema,
    projected_columns: &[String],
) -> Result<crate::catalog::types::Row, AedbError> {
    let mut values = Vec::with_capacity(projected_columns.len());
    for col in projected_columns {
        let idx = schema
            .columns
            .iter()
            .position(|c| c.name == *col)
            .ok_or_else(|| AedbError::Validation(format!("projection column missing: {col}")))?;
        values.push(row.values[idx].clone());
    }
    Ok(crate::catalog::types::Row { values })
}

pub(super) fn apply_projection_delta(
    mutation: &Mutation,
    ns: &str,
    table_name: &str,
    schema: &TableSchema,
    projected_columns: &[String],
    projection_rows: &mut im::OrdMap<EncodedKey, crate::catalog::types::Row>,
) -> Result<(), AedbError> {
    match mutation {
        Mutation::Insert {
            project_id,
            scope_id,
            table_name: mutation_table,
            primary_key,
            row,
        }
        | Mutation::Upsert {
            project_id,
            scope_id,
            table_name: mutation_table,
            primary_key,
            row,
        } => {
            if namespace_key(project_id, scope_id) == ns && mutation_table == table_name {
                let projected = project_row(row, schema, projected_columns)?;
                projection_rows.insert(EncodedKey::from_values(primary_key), projected);
            }
        }
        Mutation::InsertBatch {
            project_id,
            scope_id,
            table_name: mutation_table,
            rows,
        } => {
            if namespace_key(project_id, scope_id) == ns && mutation_table == table_name {
                for row in rows {
                    let pk = extract_pk_from_row(schema, row)?;
                    let projected = project_row(row, schema, projected_columns)?;
                    projection_rows.insert(EncodedKey::from_values(&pk), projected);
                }
            }
        }
        Mutation::Delete {
            project_id,
            scope_id,
            table_name: mutation_table,
            primary_key,
        } => {
            if namespace_key(project_id, scope_id) == ns && mutation_table == table_name {
                projection_rows.remove(&EncodedKey::from_values(primary_key));
            }
        }
        Mutation::Ddl(crate::catalog::DdlOperation::DropTable {
            project_id,
            scope_id,
            table_name: dropped_table,
            ..
        }) => {
            if namespace_key(project_id, scope_id) == ns && dropped_table == table_name {
                projection_rows.clear();
            }
        }
        Mutation::Ddl(crate::catalog::DdlOperation::DropScope {
            project_id,
            scope_id,
            ..
        }) => {
            if namespace_key(project_id, scope_id) == ns {
                projection_rows.clear();
            }
        }
        Mutation::Ddl(crate::catalog::DdlOperation::DropProject { project_id, .. }) => {
            if ns.starts_with(&(project_id.to_owned() + "::")) {
                projection_rows.clear();
            }
        }
        _ => {}
    }
    Ok(())
}
