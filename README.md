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

### Action envelopes and typed KV numerics

For single hot-path action commits (effects + metadata in one atomic envelope), use
`AedbInstance::commit_action_envelope(...)` with `ActionEnvelopeRequest`.
Result semantics are explicit:

- `ActionCommitOutcome::Applied`
- `ActionCommitOutcome::Duplicate` (idempotent replay, no writes applied)

Native U256 KV mutation variants are available for strict/soft decrement and bounded updates:

- `Mutation::KvAddU256Ex`
- `Mutation::KvSubU256Ex`
- `Mutation::KvMaxU256`
- `Mutation::KvMinU256`

`CommitResult` also exposes idempotency metadata:

- `idempotency: IdempotencyOutcome`
- `canonical_commit_seq`

### Data model

- Namespace hierarchy: `project -> scope -> table`
- Typed relational tables for structured data
- KV APIs for point lookups, prefix/range scans, and counters
- Native accumulators for high-ingest, exactly-once additive state

Accumulator example:

```rust
db.create_accumulator("casino", "app", "house_balance", Some(86_400), 10_000)
    .await?;

db.accumulate(
    "casino",
    "app",
    "house_balance",
    -125,               // delta
    "settle_tx_123".into(), // dedupe key
    42,                 // order key
)
.await?;

let projected = db
    .accumulator_value("casino", "app", "house_balance", ConsistencyMode::AtLatest)
    .await?;
let strong = db
    .accumulator_value_strong("casino", "app", "house_balance", ConsistencyMode::AtLatest)
    .await?;
let lag = db
    .accumulator_lag("casino", "app", "house_balance", ConsistencyMode::AtLatest)
    .await?;

// Optional circuit-breaker controls (basis points + orphan TTL in commit units)
db.create_accumulator_with_options(
    "casino",
    "app",
    "house_balance_cb",
    Some(86_400),
    10_000,
    1_000, // 10% exposure margin
    Some(20_000),
)
.await?;

db.expose_accumulator("casino", "app", "house_balance_cb", 500, "hand-42".into())
    .await?;
db.accumulate_with_release(
    "casino",
    "app",
    "house_balance_cb",
    -120,
    "settle_hand_42".into(),
    43,
    Some("hand-42".into()),
)
.await?;
let exposure = db
    .accumulator_exposure("casino", "app", "house_balance_cb", ConsistencyMode::AtLatest)
    .await?;
let available = db
    .accumulator_available("casino", "app", "house_balance_cb", ConsistencyMode::AtLatest)
    .await?;
let exposure_metrics = db
    .accumulator_exposure_metrics("casino", "app", "house_balance_cb", ConsistencyMode::AtLatest)
    .await?;

// Tier-2 event stream + processor checkpoint primitives
db.emit_event(
    "casino",
    "app",
    "hand_settled",
    "hand_42".into(),
    r#"{"user_id":"u1","wager":100,"pnl":-120}"#.into(),
)
.await?;

let page = db
    .read_event_stream(Some("hand_settled"), 0, 100, ConsistencyMode::AtLatest)
    .await?;
db.ack_reactive_processor_checkpoint("points_processor", page.next_commit_seq.unwrap_or(0))
    .await?;
let processor_lag = db
    .reactive_processor_lag("points_processor", ConsistencyMode::AtLatest)
    .await?;
```

Recommended hand lifecycle (high-throughput):

```rust
// 1) Reserve max loss at deal time (fast circuit breaker)
db.expose_accumulator("casino", "app", "house_balance_cb", max_payout, hand_id.clone())
    .await?;

// 2) Apply actual outcome and release full reservation at settle time
db.accumulate_with_release(
    "casino",
    "app",
    "house_balance_cb",
    actual_result,
    hand_id.clone(), // dedupe key
    settle_seq,
    Some(hand_id),   // release exposure id
)
.await?;
```

For bursty deal traffic, use atomic batch reserve:

```rust
db.expose_accumulator_many_atomic(
    "casino",
    "app",
    "house_balance_cb",
    vec![
        (500, "hand-101".to_string()),
        (750, "hand-102".to_string()),
        (300, "hand-103".to_string()),
    ],
)
.await?;
```

For event processors, prefer watermark-batched checkpoint ACKs to reduce write load:

```rust
db.ack_reactive_processor_checkpoint_batched(
    "points_processor",
    processed_seq,
    100, // persist every 100 commits advanced
)
.await?;
```

`ack_reactive_processor_checkpoint_batched_as(...)` isolates watermark state per caller and
only updates cache state after successful commit.

Built-in processor scheduler (period + size limits):

```rust
use std::sync::Arc;

let db = Arc::new(db);
db.start_reactive_processor(
    "points_processor",
    aedb::ReactiveProcessorOptions {
        caller_id: Some("processor_points".into()),
        topic_filter: Some("hand_settled".into()),
        run_on_interval: false,
        max_allowed_lag_commits: Some(2_000),
        max_allowed_stall_ms: Some(30_000),
        max_events_per_run: 256,
        max_bytes_per_run: 2 * 1024 * 1024,
        max_run_duration_ms: 250,
        run_interval_ms: 20,
        idle_backoff_ms: 50,
        checkpoint_watermark_commits: 64,
        max_retries: 3,
        retry_backoff_ms: 100,
    },
    move |db, events| async move {
        // Your derived-state logic (idempotent)
        for event in events {
            let _ = db
                .emit_event(
                    "casino",
                    "app",
                    "points_applied",
                    format!("points:{}", event.event_key),
                    event.payload_json,
                )
                .await?;
        }
        Ok(())
    },
)
.await?;

let status = db
    .reactive_processor_runtime_status("points_processor")
    .await;

let health = db
    .reactive_processor_health("points_processor", ConsistencyMode::AtLatest)
    .await?;

let processors = db
    .list_reactive_processors(ConsistencyMode::AtLatest)
    .await?;

db.stop_reactive_processor("points_processor").await?;
```

`caller_id` defines the explicit auth context for that processor runtime:
- event stream reads run via `read_event_stream_as(...)`
- lag/checkpoint access runs via `reactive_processor_lag_as(...)` and
  `ack_reactive_processor_checkpoint_batched_as(...)`
- dead-letter writes run via `commit_as(...)`

In secure mode, `caller_id` is required for `start_reactive_processor(...)`.

For periodic refresh jobs (e.g. weekly/all-time leaderboard snapshots), set
`run_on_interval: true`. AEDB will invoke the processor handler on each
`run_interval_ms` tick even when no new events are present.

Lifecycle controls:
- `pause_reactive_processor(name)` disables in registry and stops runtime.
- `resume_reactive_processor(name)` re-enables and starts runtime using the registered handler.
- `list_reactive_processors(consistency)` returns durable config + running state.
- `reactive_processor_health(name, consistency)` returns lag + runtime counters/timestamps.
- `reactive_processor_slo_status(name, consistency)` returns threshold checks and breach reasons.
- `list_reactive_processor_slo_statuses(consistency)` returns all processor SLO states.
- `enforce_reactive_processor_slos(consistency)` returns `Unavailable` when any enabled processor breaches SLO.

Processor configs are durably persisted in a system registry table when started.
On process restart, register the handler again and AEDB auto-resumes enabled processors:

```rust
let resumed = db
    .register_reactive_processor_handler("points_processor", move |db, events| async move {
        // same handler logic
        Ok(())
    })
    .await?;
assert!(resumed); // true when durable registry had enabled processor config
```

When handler retries are exhausted, AEDB writes failed events to the durable
`_system.app.reactive_processor_dead_letters` table and advances checkpoint so
poison batches do not stall ingestion.

Secure mode/authenticated flows can use `create_accumulator_as`, `accumulate_as`,
`accumulator_value_as`, `accumulator_value_strong_as`, `accumulator_lag_as`,
`expose_accumulator_as`, `accumulate_with_release_as`, `accumulator_exposure_as`,
`accumulator_available_as`, `accumulator_exposure_metrics_as`,
`expose_accumulator_many_atomic_as`, and `ack_reactive_processor_checkpoint_batched_as`.

Arcana-oriented engine interface primitives (effect batches, keyed-state helpers,
processor pull/commit/context) are also exposed under `aedb::engine_interface`
and as `AedbInstance` methods:

- `commit_effect_batch(project_id, scope_id, EffectBatch)`
- `keyed_state_read` / `keyed_state_read_field` / `keyed_state_write` / `keyed_state_update` / `keyed_state_delete`
- `keyed_state_query_index` / `keyed_state_index_rank`
- `processor_pull(event_name, processor_id, max_count)`
- `processor_commit(processor_id, checkpoint_seq, mutations)` (atomic state + checkpoint commit)
- `processor_context(project_id, scope_id, processor_id, source_event)` with:
  - `pull(max_count)`
  - `read` / `query_index`
  - `write` / `update` / `delete`
  - `accumulate` / `value` / `expose` / `release_exposure`
  - `emit`
  - `commit`

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
- `open_secure` enforces hardened durability/recovery settings (`DurabilityMode::Full`, strict recovery, hash chain, HMAC)
- table/KV/query access can be scoped per project/scope/resource
- `authz_audit` and `assertion_audit` system tables provide built-in audit trails

Security/operations docs:

- `docs/SECURITY_ACCEPTANCE_CRITERIA.md`
- `docs/SECURITY_OPERATIONS_RUNBOOK.md`
- `docs/AEDB_SDK_PROCESSOR_MACRO_SPEC.md`
- `docs/AEDB_MIGRATION_SYSTEM.md`

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

Security acceptance gate (mandatory profile):

```bash
./scripts/security_gate.sh
```

## License

Dual-licensed under:

- MIT (`LICENSE-MIT`)
- Apache-2.0 (`LICENSE-APACHE`)
