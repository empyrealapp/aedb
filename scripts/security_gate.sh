#!/usr/bin/env bash
set -euo pipefail

echo "[security-gate] unit/library"
cargo test --lib

echo "[security-gate] security boundaries"
cargo test --test security_boundaries -- --test-threads=1
cargo test --test security_properties -- --test-threads=1
cargo test --test security_properties_proptest -- --test-threads=1

echo "[security-gate] strict backup/restore integrity"
cargo test --test backup_restore strict_backup_chain_restore_succeeds_with_hash_chain_enforcement -- --test-threads=1
cargo test --test backup_restore strict_backup_chain_restore_rejects_tampered_incremental_segment -- --test-threads=1

echo "[security-gate] idempotency + assertion behavior"
cargo test --test read_assertions integration_idempotent_retry_skips_assertion_re_evaluation -- --test-threads=1
cargo test --test read_assertions integration_failed_assertion_is_logged_to_system_audit_table -- --test-threads=1

echo "[security-gate] crash/recovery matrix"
cargo test --test crash_matrix -- --test-threads=1
cargo test --test crash_matrix crash_matrix_a17a_strict_restarts_fail_closed -- --ignored --test-threads=1
cargo test --test crash_matrix crash_matrix_a17b_thousand_crash_cycles_preserve_state -- --ignored --test-threads=1

echo "[security-gate] orderbook adversarial and chaos"
cargo test -p aedb-orderbook --test property_randomized_matrix -- --test-threads=1
cargo test -p aedb-orderbook --test adversarial_slo_sla -- --test-threads=1
cargo test -p aedb-orderbook --test chaos_ci_profile -- --test-threads=1
cargo test -p aedb-orderbook --test simulation_smoke simulation_soak_multi_asset_mixed -- --ignored --test-threads=1
cargo test -p aedb-orderbook --test simulation_smoke simulation_soak_single_asset_contention_limit -- --ignored --test-threads=1
cargo test --test order_book_simulation order_book_chaos_read_write_accuracy -- --test-threads=1

echo "[security-gate] complete"
