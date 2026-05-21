# Security Acceptance Criteria

This document defines repo-local acceptance gates for high-integrity workloads. Passing these gates is evidence about the tested code paths; it is not evidence of independent audit, penetration testing, key management quality, or restore readiness in a deployed environment.

## Mandatory CI Gates

The CI pipeline must pass all of the following on pull requests and protected branches:

- Crash/recovery:
  - `cargo test --test crash_matrix -- --test-threads=1`
  - `cargo test --test crash_matrix crash_matrix_a17a_strict_restarts_fail_closed -- --ignored --test-threads=1`
  - `cargo test --test crash_matrix crash_matrix_a17b_thousand_crash_cycles_preserve_state -- --ignored --test-threads=1`
- Strict security/authorization:
  - `cargo test --test security_boundaries -- --test-threads=1`
  - `cargo test --test security_properties -- --test-threads=1`
  - `cargo test --test security_properties_proptest -- --test-threads=1`
  - `cargo test --test read_assertions integration_idempotent_retry_skips_assertion_re_evaluation -- --test-threads=1`
  - `cargo test --test read_assertions integration_failed_assertion_is_logged_to_system_audit_table -- --test-threads=1`
- Backup/restore integrity:
  - `cargo test --test backup_restore strict_backup_chain_restore_succeeds_with_hash_chain_enforcement -- --test-threads=1`
  - `cargo test --test backup_restore strict_backup_chain_restore_rejects_tampered_incremental_segment -- --test-threads=1`
- Long chaos/adversarial orderbook profiles:
  - `cargo test -p aedb-orderbook --test property_randomized_matrix -- --test-threads=1`
  - `cargo test -p aedb-orderbook --test adversarial_slo_sla -- --test-threads=1`
  - `cargo test -p aedb-orderbook --test chaos_ci_profile -- --test-threads=1`
  - `cargo test -p aedb-orderbook --test simulation_smoke simulation_soak_multi_asset_mixed -- --ignored --test-threads=1`
  - `cargo test -p aedb-orderbook --test simulation_smoke simulation_soak_single_asset_contention_limit -- --ignored --test-threads=1`
  - `cargo test --test order_book_simulation order_book_chaos_read_write_accuracy -- --test-threads=1`

Use `scripts/security_gate.sh` to run this locally.

## SLO/SLA Thresholds

The adversarial SLO gate (`adversarial_slo_sla`) enforces the following defaults:

- `AEDB_ORDERBOOK_SLA_MIN_ATTEMPTED_TPS=600`
- `AEDB_ORDERBOOK_SLA_MAX_P99_US=1000000`
- `AEDB_ORDERBOOK_SLA_MAX_FINALITY_GAP=10000`
- `AEDB_ORDERBOOK_SLA_MAX_PRIMARY_REJECT_RATIO_PPM=900000`

These environment variables may be tightened in production CI.

## Invariant Requirements

All mandatory scenarios must satisfy:

- Zero dropped primary orders (`accepted + rejected == attempted`).
- Lifecycle accounting exactness (`lifecycle_accepted + lifecycle_rejected == lifecycle_attempted`).
- Durable and visible heads not behind accepted commit head.
- Deterministic replay/integrity checks pass in strict crash and strict restore suites.
- No authorization boundary bypass in secure mode test suites.

## Secure-Mode API Boundary

Secure mode is fail-closed at the public API boundary. Public methods that omit caller context must reject in secure mode unless they are pure local lifecycle/configuration operations that cannot read or mutate protected AEDB data.

Current unauthenticated read escape hatches are intentionally limited to non-secure deployments:

- `query_no_auth`
- `kv_get_no_auth`
- `kv_get_many_no_auth`
- `kv_scan_prefix_no_auth`

The synchronous bridge may expose wrappers with the same names; they inherit the same secure-mode denial from `AedbInstance`.

Public methods with anonymous read behavior but without a `_no_auth` suffix, including `query`, `query_with_options`, `query_with_read_set`, `read_event_stream`, `reactive_processor_lag`, migration runners, and backup writers, must also reject in secure mode until a caller-bearing/admin-authorized API exists.

Methods containing `unchecked` are crate-private implementation details only. They must not be exported as public API, and secure-mode coverage must fail if they become reachable without an authenticated caller.

Permission-denied diagnostics are part of the security boundary. They may identify that access was denied, but must not disclose protected project names, scope names, table names, keys, predicate details, row contents, or assertion actual values. Use generic `"permission denied"` messages for authorization failures; detailed resource identifiers belong in trusted audit logs, not user-facing errors.

## External Validation Required For Stronger Claims

The following are required before using security-audited, penetration-tested, financial-grade, or equivalent production-readiness claims:

- Independent code audit with focus on commit atomicity, authorization checks, and recovery path.
- Penetration testing of the embedding/API boundary in the host application.
- Key management and secret distribution review (HMAC/checkpoint keys, rotation, revocation).
- Incident response tabletop and restore drills with explicit RTO/RPO evidence.
