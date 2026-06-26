# Security Operations Runbook

## 1. Key Management

- Use `AedbConfig::production([u8; 32])` or `AedbConfig::low_latency([u8; 32])` for manifest HMAC key baseline.
- Set checkpoint encryption key with `with_checkpoint_key([u8; 32])` for encrypted checkpoint at-rest protection.
- Keep keys outside source control; inject via secret manager/environment at process boot.
- Rotate keys by controlled deployment with backup snapshots before and after rotation.

## 2. Authenticated Caller Model

- `AedbInstance::open(...)` is authenticated-by-default: anonymous
  `commit`/`kv_set`/`query` and every `*_no_auth` helper are rejected. Use
  `open_secure(...)` (or `open_production(...)`) in production paths for the
  additional secure-config validation (e.g. a configured manifest HMAC key).
- `AedbInstance::open_anonymous(...)` is the explicit opt-out that permits
  unauthenticated operations. Use it only for trusted, non-exposed deployments.
- Require all caller-facing operations to use authenticated `*_as` APIs.
- The anonymous read/write surface is intentionally limited to the explicit
  `*_no_auth` twins (`commit_no_auth`, `kv_set_no_auth`, `kv_del_no_auth`,
  `query_no_auth`, `kv_get_no_auth`, ...); these all error under `open`/secure
  mode. Do not expose them in host application routes.
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

## 6. Data-at-Rest Integrity Threat Model

On-disk integrity is layered, and only the top layer is cryptographic. Know
which guarantee each layer provides before relying on it:

- **WAL frames** carry a per-frame CRC32C (`src/wal/frame.rs`). This detects
  bit-rot and torn writes. It is **not** a security check: CRC32C is unkeyed and
  trivially recomputable, so an attacker who can write the data directory can
  forge a frame with a valid CRC.
- **WAL segments** maintain a blake3 hash chain (`src/wal/segment.rs`). The chain
  detects truncation and out-of-order/missing frames, but the hasher is
  **unkeyed** — it is a strong corruption check, not a tamper-evidence check.
  Anyone who can rewrite a segment can recompute a consistent chain.
- **Checkpoints** are covered by unkeyed SHA-256 payload digests
  (`src/checkpoint/writer.rs`) — again corruption detection, recomputable by a
  writer.
- **The manifest is the only cryptographic root of trust.** It is signed with
  HMAC-SHA256, and the signature exists **only when an HMAC key is configured**
  (`write_manifest_atomic_signed`, `src/manifest/atomic.rs`). The manifest
  records the SHA-256 of every checkpoint/WAL file it references, so a valid
  manifest HMAC transitively authenticates those files' digests.

Consequence: **tamper-resistance of segments and checkpoints requires an HMAC
key.** Without one (e.g. plain `AedbConfig::new`/`open` rather than
`production`/`open_secure`), all on-disk checks degrade to corruption detection,
and an adversary with write access to the data directory can substitute files and
recompute every checksum undetectably. For any deployment with a meaningful
disk-tampering threat model:

- configure the manifest HMAC key (see §1) — this is what makes the strict
  hash-chain restore drill in §4 actually tamper-*evident* rather than merely
  corruption-detecting;
- treat the HMAC key as the asset whose compromise breaks at-rest integrity, and
  protect the data directory with OS-level permissions regardless.
