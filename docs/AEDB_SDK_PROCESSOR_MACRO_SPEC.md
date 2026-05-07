# AEDB SDK & Processor Macro Specification

## Purpose
Define a type-safe developer SDK for AEDB application logic and processor authoring.

## Schema derivation
Application state/event types are plain Rust structs with derives:
- `#[derive(State)]` with `#[state(name = "...", key = "...")]`
- `#[derive(Event)]` with `#[event(name = "...")]`

State derive should generate:
- serialization/deserialization
- schema + index metadata
- type-safe state handle registration

Event derive should generate:
- serialization/deserialization
- event schema metadata
- typed `emit(...)` integration

## Type-safe handles
Reads and writes are typed by state/event type.

### StateHandle API
- `get(key) -> Option<T>`
- `get_field(key, field) -> Option<F>`
- `put(&T)`
- `modify(key, f)`
- `delete(key)`
- `query() -> IndexQueryBuilder<T>`

### IndexQueryBuilder API
- `index(name)`
- `prefix(value_or_tuple)`
- `limit(n)`
- `offset(n)`
- `after(key)`
- `exec() -> Vec<T>`
- `first() -> Option<T>`
- `rank(key) -> Option<usize>`

### Numeric state API
Numeric state is represented with normal KV values or typed table columns.
Generated SDK helpers should emit AEDB's numeric KV mutations instead of a
separate additive primitive:
- `add_u64(key, amount)`
- `sub_u64(key, amount)`
- `max_u64(key, value)`
- `min_u64(key, value)`
- `mutate_u64(key, op)`

## Effect batch builder
WASM game logic should return one `EffectBatch`:
- `put<T: State>(&T)`
- `delete<T: State>(key)`
- `emit<E: Event>(&E)`

## Numeric state boundary
AEDB should expose primitive building blocks only:
- typed KV integer mutations
- table column updates
- transaction envelopes for atomic state + metadata commits

AEDB should **not** encode app withdrawal policy semantics (for example `withdraw_delay` or `request_withdraw` workflow).  
Application logic (Arcana) should implement delayed withdrawals by scheduling a later effect batch or action envelope that applies the normal integer mutation.

## Processor macro
`#[processor(...)]` defines typed event consumers with generated registration + invocation wiring.

Expected options:
- `name`
- `source`
- `partition_by` (optional)
- `batch_size` (optional, defaulted)
- `owns` (writable state types)
- `reads` (read-only state types)

`ProcessorCtx<E>` should provide:
- `events()`
- `state<T>()` for owned types
- `read_state<T>()` for read-only types
- `numeric_kv()` for typed KV integer updates when generated SDK helpers need them
- `commit()` (atomic writes + checkpoint advance)

## Error model
- Invalid effect batches return structured typed errors.
- Processor panic/error does not advance checkpoint; events are retried.

## Startup flow
At startup, register:
- state schemas
- event schemas
- processors

Then start scheduler/ingestion; WAL replay + checkpoint recovery restore engine state before serving traffic.
