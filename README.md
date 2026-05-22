# AEDB

`aedb` is an embedded Rust storage engine for applications that need:

- transactional writes
- durable WAL + checkpoint recovery
- snapshot-consistent reads
- optional permission-aware APIs for multi-tenant workloads

Primary API entry point: `AedbInstance`.

## Why AEDB

AEDB is designed for local-first and service-side state where you want explicit durability and recovery controls without running an external database process.

Use AEDB when you want:

- in-process storage with explicit durability controls
- checkpoint + WAL replay with crash/recovery coverage in the repo test suite
- table + KV data models in one engine
- operational APIs for checkpoint, backup, restore, and diagnostics

## Installation

```toml
[dependencies]
aedb = "0.2.7"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Quick Start

```rust
use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::validation::Mutation;
use aedb::query::plan::{Expr, Query, QueryOptions};
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db = AedbInstance::open_no_auth(Default::default(), dir.path())?;

    db.create_project("demo").await?;
    db.create_scope("demo", "app").await?;

    db.commit_with_preflight(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "demo".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        owner_id: None,
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "username".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await?;

    db.commit_with_preflight(Mutation::Insert {
        project_id: "demo".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1), Value::Text("alice".into())]),
    })
    .await?;

    let query = Query::select(&["id", "username"])
        .from("users")
        .where_(Expr::Eq("id".into(), Value::Integer(1)));

    let result = db
        .query_no_auth("demo", "app", query, QueryOptions::default())
        .await?;
    assert_eq!(result.rows.len(), 1);

    Ok(())
}
```

## Core Concepts

### Data model

- Namespace hierarchy: `project -> scope -> table`
- Typed relational tables for structured data
- KV APIs for point lookups, prefix/range scans, and counters
- Atomic KV/table integer updates with explicit underflow/overflow behavior
- Event stream and processor checkpoint primitives for durable derived-state workflows

Use tables when you need schema, indexes, constraints, filters, ordering, or
joins. Use KV for point lookups, prefix/range scans, opaque payloads, and
counters. Keep one source of truth unless an explicit projection is part of the
design.

### Transaction model

Writes are appended to the WAL and applied as ordered commit envelopes. A commit
may include one mutation or a set of mutations that must publish atomically.

- `commit_with_preflight` / `commit_as_with_preflight` perform an advisory read
  and include the observed read set in the commit envelope.
- `ReadAssertion`s are commit-time checks evaluated against the working state
  before the write intent applies.
- `Mutation::PostflightCheck` evaluates assertions after earlier mutations in
  the same envelope have applied to the transaction-local trial state.
- Atomic numeric mutations encode their missing-key, underflow, and overflow
  policy in the mutation and return structured errors on rejection.
- `CommitFinality::Visible` waits for publication to readers; `CommitFinality::Durable`
  waits for the durable head.

### Operational Features

AEDB includes operational APIs for checkpointing, backup/restore, event streams,
processor checkpoints, diagnostics, and offline invariant checks. These APIs are
part of the storage engine surface, but application-specific processor logic
belongs outside the core write path.

Detailed examples live in:

- `docs/PRODUCTION_USAGE_EXAMPLES.md`
- `docs/COMMIT_SEQUENCING.md`
- `docs/AUTHORIZATION_MODEL.md`
- `docs/PERSISTENCE_COMPATIBILITY.md`

### Consistency modes

Reads are snapshot-based and configurable via `ConsistencyMode`:

- `AtLatest`
- `AtSeq`
- `AtCheckpoint`

```rust
use aedb::query::plan::{ConsistencyMode, Query, QueryOptions};

let result = db
    .query_with_options(
        "demo",
        "app",
        Query::select(&["*"]).from("users"),
        QueryOptions {
            consistency: ConsistencyMode::AtCheckpoint,
            ..QueryOptions::default()
        },
    )
    .await?;

println!("snapshot seq = {}", result.snapshot_seq);
```

### Preflight and commits

- `preflight` and `preflight_plan` are advisory; state may change before commit
- `commit_with_preflight` / `commit_as_with_preflight` include the preflight read set in the commit envelope, which catches stale reads before writes apply
- `ReadAssertion`s are commit-time checks for invariants that must hold under concurrency
- `Mutation::PostflightCheck` is a commit-time post-apply check for atomic updates; failed checks abort and roll back the whole envelope before publish
- atomic integer updates should encode their own underflow/overflow/missing-key policy and return structured errors rather than relying on fallbacks
- use `commit_with_finality(..., CommitFinality::Visible)` for low-latency user ack
- use `CommitFinality::Durable` for flows that must wait for WAL durability

Low-latency profile example:

```rust
use aedb::config::AedbConfig;

let config = AedbConfig::low_latency([7u8; 32]);
let db = aedb::AedbInstance::open_secure(config, dir.path())?;
```

## Security and Permissions

AEDB supports permission-aware APIs via `CallerContext` and `Permission`.

- `open_production` and `open_secure` require authenticated `*_as` calls
- `open_secure` enforces hardened durability/recovery settings (`DurabilityMode::Full`, strict recovery, hash chain, HMAC)
- table/KV/query access can be scoped per project/scope/resource
- `authz_audit` and `assertion_audit` system tables provide built-in audit trails

Security/operations docs:

- `docs/PRODUCTION_READINESS.md`
- `docs/PRODUCTION_USAGE_EXAMPLES.md`
- `docs/ARCHITECTURE_BOUNDARIES.md`
- `docs/SECURITY_ACCEPTANCE_CRITERIA.md`
- `docs/SECURITY_OPERATIONS_RUNBOOK.md`
- `docs/AUTHORIZATION_MODEL.md`
- `docs/COMMIT_SEQUENCING.md`
- `docs/PERSISTENCE_COMPATIBILITY.md`
- `docs/AEDB_SDK_PROCESSOR_MACRO_SPEC.md`
- `docs/AEDB_MIGRATION_SYSTEM.md`

Error handling:

- `AedbError::code()` / `QueryError::code()` are stable machine-readable codes
- `AedbError::class()` / `QueryError::class()` map errors to retryable, conflict, permission, validation, integrity, and unavailable caller behavior
- use `*_str()` helpers for log/API strings; do not parse display text

## Operational APIs

- `checkpoint_now()` to force a fuzzy checkpoint (does not block commit/query traffic)
- `backup_full(...)` / restore helpers for backup workflows
- `operational_metrics()` for commit latency, queue depth, durable head lag, and more

CLI helper (`src/bin/aedb.rs`) includes offline dump/parity/invariant tooling:

```bash
cargo run --bin aedb -- dump export --data-dir /tmp/aedb-data --out /tmp/aedb-dump.aedbdump
cargo run --bin aedb -- dump parity --dump /tmp/aedb-dump.aedbdump --data-dir /tmp/aedb-data
cargo run --bin aedb -- check invariants --data-dir /tmp/aedb-data
```

Explorer CLI crate (`crates/aedb-explorer`) provides read-only inspection of projects/scopes/tables, schema, and sample rows:

```bash
cargo run -p aedb-explorer -- projects --data-dir /tmp/aedb-data
cargo run -p aedb-explorer -- tables --data-dir /tmp/aedb-data --project demo --scope app
cargo run -p aedb-explorer -- scan-table --data-dir /tmp/aedb-data --project demo --scope app --table users --limit 25
```

## API Areas

- `aedb::commit`: mutations, envelopes, validation
- `aedb::query`: query planning and execution
- `aedb::catalog`: schema, types, and DDL
- `aedb::repository`: typed repository/pagination helpers
- `aedb::declarative`: declarative schema migration builders
- `aedb::backup`, `aedb::checkpoint`, `aedb::recovery`: durability and restore path

## Development

```bash
cargo build
cargo test
```

Focused suites:

```bash
cargo test --test query_integration
cargo test --test backup_restore
cargo test --test crash_matrix
cargo test --test stress
```

Security acceptance gate:

```bash
./scripts/security_gate.sh
```

Operational readiness gate:

```bash
./scripts/production_readiness_gate.sh
```

Operational rollout guidance:

- [docs/PRODUCTION_READINESS.md](docs/PRODUCTION_READINESS.md)

## License

Dual-licensed under:

- MIT (`LICENSE-MIT`)
- Apache-2.0 (`LICENSE-APACHE`)
