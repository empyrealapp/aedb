# Security Operations Runbook

## 1. Key Management

- Use `AedbConfig::production([u8; 32])` or `AedbConfig::low_latency([u8; 32])` for manifest HMAC key baseline.
- Set checkpoint encryption key with `with_checkpoint_key([u8; 32])` for encrypted checkpoint at-rest protection.
- Keep keys outside source control; inject via secret manager/environment at process boot.
- Rotate keys by controlled deployment with backup snapshots before and after rotation.

## 2. Authenticated Caller Model

- Use `AedbInstance::open_secure(...)` (or `open_production(...)`) in production paths.
- Require all caller-facing operations to use authenticated `*_as` APIs.
- Do not expose anonymous commit/query APIs in host application routes.
- Grant minimum permissions only (project/scope/table/KV-prefix scoped).

## 3. Audit Logging

- Keep system audit tables enabled:
  - `authz_audit` for grant/revoke/ownership events.
  - `assertion_audit` for assertion failures and policy enforcement failures.
- Periodically export audit table snapshots to immutable storage.
- Alert on:
  - repeated failed assertions,
  - unexpected GlobalAdmin usage,
  - unusual permission churn.

## 4. Backup/Restore Drills

- Daily full backup, periodic incremental backups (policy defined by RPO target).
- Weekly restore drill:
  - restore into clean directory,
  - verify strict hash-chain integrity,
  - run parity and invariant checks.
- Suggested commands:
  - `cargo test --test backup_restore strict_backup_chain_restore_succeeds_with_hash_chain_enforcement -- --test-threads=1`
  - `cargo test --test backup_restore strict_backup_chain_restore_rejects_tampered_incremental_segment -- --test-threads=1`
  - `cargo run --bin aedb -- check invariants --data-dir <restore_dir>`

## 5. Incident Response

- On integrity alert:
  - freeze writes at host application level,
  - capture WAL segments + manifest + checkpoint files,
  - run offline parity/invariant checks,
  - restore from latest validated backup chain if needed.
- On authorization alert:
  - revoke impacted permissions,
  - rotate application credentials,
  - inspect `authz_audit` and host API logs for blast radius.
- On repeated commit timeout/conflict spikes:
  - collect `operational_metrics()` snapshots,
  - reduce admission rate or shard workload by asset/project,
  - maintain durable-finality path for high-value transactions.
