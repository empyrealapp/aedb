# Commit Sequencing Architecture

This document describes the write path that turns a caller request into a visible and, depending on durability mode, durable commit. The implementation lives mainly in `src/commit/executor/` and is exercised by the crash/recovery and executor tests linked below.

## Sequence

1. API entry points build a `TransactionEnvelope`.
   - `commit`, `commit_as`, `commit_with_preflight`, and `commit_as_with_preflight` are in `src/lib.rs`.
   - Secure mode rejects unauthenticated write APIs before enqueueing.
   - Caller IDs are checked for reserved values before authorization.

2. Pre-stage validation prepares the request before epoch execution.
   - KV size limits are checked before queueing.
   - DDL that affects later mutations in the same envelope is staged before partition derivation.
   - Caller-scoped authorization is evaluated before apply, and preflight plans carry read sets into the commit envelope.
   - Tests:
     - `src/commit/executor/tests.rs::prestage_validate_applies_staged_ddl_before_partition_derivation`
     - `src/commit/executor/tests.rs::prestage_kv_fast_path_preserves_explicit_read_set_dependencies`
     - `tests/security_properties.rs::security_stale_table_preflight_plan_cannot_overwrite_row`

3. Epoch selection groups compatible requests.
   - The executor selects pending requests according to configured epoch wait/count limits.
   - Write and read partition tokens prevent conflicting requests from sharing an unsafe parallel window.
   - Assertion read dependencies participate in epoch selection.
   - Tests:
     - `src/commit/executor/tests.rs::epoch_selection_can_coalesce_same_partition_writes`
     - `src/commit/executor/tests.rs::epoch_selection_does_not_treat_same_namespace_multi_key_as_cross_partition`
     - `src/commit/executor/tests.rs::assertion_read_dependency_conflicts_with_write_token_in_epoch_selection`
     - `src/commit/executor/tests.rs::assertion_read_dependency_non_conflicting_key_can_coexist_in_epoch_selection`

4. Commit sequence numbers are assigned immediately before WAL encoding.
   - `commit_seq` is monotonically advanced from the executor state's `current_seq`.
   - Idempotency records are scoped by caller when a caller exists.
   - WAL payloads include the ordered mutations and idempotency metadata needed for replay.
   - Tests:
     - `src/commit/executor/tests.rs::serialization_concurrent_submissions_produce_ordered_wal`
     - `src/commit/executor/tests.rs::wal_payload_encoding_is_stable_with_capacity_hints`
     - `tests/security_properties.rs::security_idempotency_is_scoped_to_caller`
     - `tests/crash_matrix.rs::crash_matrix_idempotency_survives_restart`

5. WAL append happens before state publish.
   - The epoch appends frames for sequenced commits with `append_frames_with_sync`.
   - In `DurabilityMode::Full`, the WAL and persistent value store are synced before the epoch can publish.
   - In batch or OS-buffered modes, visible commits can lead durable head; callers that need durable acknowledgment should request durable finality.
   - Tests:
     - `tests/crash_matrix.rs::crash_matrix_full_durability_abrupt_restart_recovers_all_acknowledged_commits`
     - `tests/crash_matrix.rs::crash_matrix_mid_commit_batch_loses_unflushed_tail`
     - `src/commit/executor/tests.rs::batch_mode_heads_diverge_and_converge`
     - `src/commit/executor/tests.rs::batch_mode_groups_commits_without_per_commit_fsync`

6. Apply mutates working state, then publishes atomically.
   - General epochs apply to working keyspace/catalog copies before replacing executor state.
   - Cross-partition commits use the coordinator path.
   - Eligible same-namespace KV sets can use an inline fast path after WAL append.
   - Parallel apply failures reject the whole epoch without publishing partial state.
   - Tests:
     - `src/commit/executor/tests.rs::cross_partition_commit_uses_coordinator_and_applies_all_mutations`
     - `src/commit/executor/tests.rs::coordinator_timeout_rejects_cross_partition_without_side_effects`
     - `src/commit/executor/tests.rs::parallel_worker_timeout_rejects_entire_epoch_without_state_publish`
     - `src/commit/executor/tests.rs::parallel_worker_panic_rejects_entire_epoch_without_state_publish`
     - `tests/crash_matrix.rs::crash_matrix_coordinator_timeout_recovery_has_no_partial_writes`
     - `tests/crash_matrix.rs::crash_matrix_parallel_epoch_timeout_recovery_has_no_partial_writes`

7. Snapshot state and heads are advanced.
   - `current_seq` and `visible_head_seq` advance to the last sequenced commit that was published.
   - `durable_head_seq` advances immediately in full durability, and after configured sync points in batch mode.
   - Commit deltas are published to the version store; periodic or forced full snapshots are published for snapshot readers.
   - Tests:
     - `src/commit/executor/tests.rs::latest_snapshot_view_refreshes_lazily_after_commit`
     - `tests/security_properties.rs::security_replay_is_deterministic_via_snapshot_parity`

8. Recovery rebuilds from checkpoint plus WAL.
   - Startup loads the manifest, checkpoint, and WAL segments according to recovery mode.
   - Strict recovery fails closed for corruption or hash/HMAC mismatches.
   - Permissive recovery is intended for local/dev workflows and can stop replay at corrupted WAL tail boundaries.
   - Tests:
     - `tests/crash_matrix.rs::crash_matrix_baseline_graceful_shutdown_recovers_all_commits`
     - `tests/crash_matrix.rs::crash_matrix_mid_checkpoint_tmp_file_is_ignored`
     - `tests/crash_matrix.rs::crash_matrix_after_checkpoint_before_manifest_respects_manifest_lower_bound`
     - `tests/crash_matrix.rs::crash_matrix_corrupt_wal_frame_fails_closed`
     - `tests/crash_matrix.rs::crash_matrix_corrupt_manifest_hmac_fails_closed`
     - `tests/crash_matrix.rs::crash_matrix_segment_deletion_breaks_hash_chain`

## Failure Points

| Failure point | Expected behavior | Coverage |
| --- | --- | --- |
| API auth rejects request | No enqueue, no WAL frame, structured permission error | `tests/security_properties.rs::security_secure_mode_enforces_authenticated_commit_calls`, `src/lib_tests.rs::secure_mode_requires_authenticated_apis` |
| Pre-stage validation rejects request | No sequence assignment or state mutation for rejected request | `src/commit/executor/tests.rs::invalid_type_is_rejected_without_side_effects` |
| Read set or assertion conflict | Request rejected before WAL append for that request | `tests/security_properties.rs::security_stale_kv_read_set_cannot_overwrite_key`, `src/commit/executor/tests.rs::range_read_set_conflicts_on_kv_insert_in_range` |
| WAL append or sync fails | Epoch returns errors and does not publish sequenced state | Executor paths in `src/commit/executor/internals.rs`; crash coverage above |
| Coordinator or parallel apply times out | Whole affected epoch rejects without partial writes | `tests/crash_matrix.rs::crash_matrix_coordinator_timeout_recovery_has_no_partial_writes`, `tests/crash_matrix.rs::crash_matrix_parallel_epoch_timeout_recovery_has_no_partial_writes` |
| Process stops after visible publish but before OS flush in non-full durability | Recovery is bounded by durable data; unflushed visible tail can be lost | `tests/crash_matrix.rs::crash_matrix_mid_commit_batch_loses_unflushed_tail` |
| Checkpoint temp file exists after interruption | Temp checkpoint is ignored; manifest lower bound controls recovery | `tests/crash_matrix.rs::crash_matrix_mid_checkpoint_tmp_file_is_ignored`, `tests/crash_matrix.rs::crash_matrix_after_checkpoint_before_manifest_respects_manifest_lower_bound` |

## What The Tests Prove

The repo tests prove the behavior of the implemented code paths under deterministic unit/integration scenarios and selected crash simulations. They do not prove host filesystem semantics, deployment key custody, adversarial API boundary security, or operational restore readiness in a production environment. Those must be validated with the external requirements in `docs/PRODUCTION_READINESS.md`.
