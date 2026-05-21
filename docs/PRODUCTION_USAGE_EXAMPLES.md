# Production Usage Examples

These examples show the recommended production API shape: secure open, explicit
caller identity, commit-time assertions, idempotency, and restore-drill checks.

## Secure Open

```rust
use std::path::Path;

use aedb::AedbInstance;
use aedb::config::AedbConfig;

fn open_store(
    data_dir: &Path,
    manifest_hmac_key: [u8; 32],
    checkpoint_key: [u8; 32],
) -> Result<AedbInstance, aedb::error::AedbError> {
    let config = AedbConfig::production(manifest_hmac_key)
        .with_checkpoint_key(checkpoint_key)
        .with_cursor_signing_key(manifest_hmac_key);

    AedbInstance::open_secure(config, data_dir)
}
```

`open_secure` rejects anonymous commit/query/KV APIs. Host applications should
load keys from their secret manager at boot and pass them into the config; do not
commit keys to source.

## Authenticated Commit, Query, And KV

```rust
use aedb::commit::validation::Mutation;
use aedb::permission::{CallerContext, Permission};
use aedb::query::plan::{ConsistencyMode, Query, QueryOptions};

# async fn example(db: &aedb::AedbInstance) -> Result<(), Box<dyn std::error::Error>> {
let admin = CallerContext::new("ops-api");
let worker = CallerContext::new("ledger-worker");

db.commit_as(
    admin,
    Mutation::Ddl(aedb::catalog::DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: worker.caller_id.clone(),
        permission: Permission::KvWrite {
            project_id: "prod".into(),
            scope_id: Some("ledger".into()),
            prefix: Some(b"balance/".to_vec()),
        },
    }),
)
.await?;

db.kv_set_as(
    worker.clone(),
    "prod",
    "ledger",
    b"balance/account-7".to_vec(),
    10_000u64.to_be_bytes().to_vec(),
)
.await?;

let balance = db
    .kv_get(
        "prod",
        "ledger",
        b"balance/account-7",
        ConsistencyMode::AtLatest,
        &worker,
    )
    .await?;

let _rows = db
    .query_with_options_as(
        Some(&worker),
        "prod",
        "ledger",
        Query::select(&["account_id", "balance"]).from("balances"),
        QueryOptions::default(),
    )
    .await?;
# Ok(())
# }
```

## Commit Assertions And Idempotency For Hot Keys

Use idempotency for retry-safe request handling and assertions or compare-and-set
helpers when a hot key must only be updated from the version that was read.

```rust
use aedb::commit::executor::IdempotencyOutcome;
use aedb::commit::tx::{IdempotencyKey, ReadAssertion, ReadSet, TransactionEnvelope, WriteClass, WriteIntent};
use aedb::commit::validation::Mutation;
use aedb::permission::CallerContext;
use aedb::query::plan::ConsistencyMode;

# async fn example(db: &aedb::AedbInstance) -> Result<(), Box<dyn std::error::Error>> {
let caller = CallerContext::new("ledger-worker");
let key = b"balance/account-7".to_vec();
let current = db
    .kv_get("prod", "ledger", &key, ConsistencyMode::AtLatest, &caller)
    .await?
    .expect("balance exists");

let result = db
    .commit_envelope(TransactionEnvelope {
        caller: Some(caller),
        idempotency_key: Some(IdempotencyKey([42; 16])),
        write_class: WriteClass::Standard,
        assertions: vec![ReadAssertion::KeyVersion {
            project_id: "prod".into(),
            scope_id: "ledger".into(),
            key: key.clone(),
            expected_seq: current.version,
        }],
        read_set: ReadSet::default(),
        write_intent: WriteIntent {
            mutations: vec![Mutation::KvSet {
                project_id: "prod".into(),
                scope_id: "ledger".into(),
                key,
                value: 10_250u64.to_be_bytes().to_vec(),
            }],
        },
        base_seq: current.version,
    })
    .await?;

match result.idempotency {
    IdempotencyOutcome::Applied => {}
    IdempotencyOutcome::Duplicate => {}
}
# Ok(())
# }
```

For counter-style hot keys, prefer the authenticated compare-and-update helpers
such as `kv_compare_and_add_u64_as` or `kv_compare_and_set_u256_as`.

## Backup, Restore, And Restore Drill Verification

```rust
use std::path::{Path, PathBuf};

use aedb::AedbInstance;
use aedb::config::AedbConfig;

# async fn example(db: &AedbInstance, backup_dir: &Path, drill_dir: &Path, manifest_hmac_key: [u8; 32], checkpoint_key: [u8; 32]) -> Result<(), Box<dyn std::error::Error>> {
let _full = db.backup_full(backup_dir).await?;
let drill_config = AedbConfig::production(manifest_hmac_key)
    .with_checkpoint_key(checkpoint_key)
    .with_cursor_signing_key(manifest_hmac_key);

let chain: Vec<PathBuf> = vec![backup_dir.to_path_buf()];
AedbInstance::restore_from_backup_chain(&chain, drill_dir, &drill_config, None)?;

let drill_db = AedbInstance::open_secure(drill_config, drill_dir)?;
let metrics = drill_db.operational_metrics().await;
assert!(metrics.visible_head_seq >= metrics.startup_recovered_seq);
# Ok(())
# }
```

Production drills should restore into a clean directory, open with the same
strict security profile, and run offline parity/invariant checks:

```bash
cargo run --bin aedb -- dump parity --dump /backups/latest.aedbdump --data-dir /srv/aedb-restore-drill
cargo run --bin aedb -- check invariants --data-dir /srv/aedb-restore-drill
```

## Error Codes And Caller Behavior

Use `AedbError::code()`, `AedbError::class()`, `QueryError::code()`, and
`QueryError::class()` for stable caller behavior. The `*_str()` helpers are
stable strings for logs, metrics, and API responses.

| Class | Caller behavior |
| --- | --- |
| `Retryable` | Retry with bounded backoff and preserve idempotency keys. |
| `Conflict` | Refresh the read state; retry only if the business operation is still valid. |
| `Permission` | Do not retry until authentication or grants change. |
| `Validation` | Fix the request, schema reference, cursor, or data shape. |
| `Integrity` | Stop the affected flow and page operators; inspect WAL/checkpoint/backup state. |
| `Unavailable` | Wait for readiness or competing maintenance to finish. |
