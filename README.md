# AEDB

`aedb` is an embedded Rust storage engine for applications that need:

- transactional writes
- durable WAL + checkpoint recovery
- snapshot-consistent reads
- optional permission-aware APIs for multi-tenant workloads

Primary API entry point: `AedbInstance`.

## Why AEDB

AEDB is designed for local-first and service-side state where you want predictable durability and recovery behavior without running an external database process.

Use AEDB when you want:

- in-process storage with explicit durability controls
- deterministic crash recovery from checkpoint + WAL replay
- table + KV data models in one engine
- operational APIs for checkpoint, backup, restore, and diagnostics

## Installation

```toml
[dependencies]
aedb = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Quick Start

```rust
use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::validation::Mutation;
use aedb::query::plan::{Expr, Query};
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db = AedbInstance::open(Default::default(), dir.path())?;

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

    let result = db.query("demo", "app", query).await?;
    assert_eq!(result.rows.len(), 1);

    Ok(())
}
```

## Core Concepts

### Data model

- Namespace hierarchy: `project -> scope -> table`
- Typed relational tables for structured data
- KV APIs for point lookups, prefix/range scans, and counters

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

- `preflight` and `preflight_plan` are advisory
- state may change before commit
- use `commit_with_preflight` / `commit_as_with_preflight` for lowest TOCTOU risk
- use `commit_with_finality(..., CommitFinality::Visible)` for low-latency user ack
- use `CommitFinality::Durable` for flows that must wait for WAL durability

Low-latency profile example:

```rust
use aedb::config::AedbConfig;

let config = AedbConfig::low_latency([7u8; 32]);
let db = aedb::AedbInstance::open(config, dir.path())?;
```

## Security and Permissions

AEDB supports permission-aware APIs via `CallerContext` and `Permission`.

- `open_production` and `open_secure` require authenticated `*_as` calls
- `open_secure` enforces hardened durability/recovery settings
- table/KV/query access can be scoped per project/scope/resource

## Operational APIs

- `checkpoint_now()` to force a checkpoint
- `backup_full(...)` / restore helpers for backup workflows
- `operational_metrics()` for commit latency, queue depth, durable head lag, and more

CLI helper (`src/bin/aedb.rs`) includes offline dump/parity/invariant tooling:

```bash
cargo run --bin aedb -- dump export --data-dir /tmp/aedb-data --out /tmp/aedb-dump.aedbdump
cargo run --bin aedb -- dump parity --dump /tmp/aedb-dump.aedbdump --data-dir /tmp/aedb-data
cargo run --bin aedb -- check invariants --data-dir /tmp/aedb-data
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

## License

Dual-licensed under:

- MIT (`LICENSE-MIT`)
- Apache-2.0 (`LICENSE-APACHE`)
