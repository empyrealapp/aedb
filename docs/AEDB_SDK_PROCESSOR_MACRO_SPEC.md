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

### AccumulatorHandle API
- `value()`
- `value_strong()`
- `available()`
- `exposure()`
- `lag()`

## Effect batch builder
WASM game logic should return one `EffectBatch`:
- `require_available(accumulator, min_amount)`
- `require_exposure_ok(accumulator, amount)`
- `accumulate(accumulator, delta).dedupe(id)`
- `expose(accumulator, amount, dedupe_id)`
- `release_exposure(accumulator, dedupe_id)`
- `put<T: State>(&T)`
- `delete<T: State>(key)`
- `emit<E: Event>(&E)`

## Accumulator boundary
AEDB should expose primitive building blocks only:
- positive/negative `accumulate`
- availability and exposure reads
- preconditions that reject insufficient balance/capacity

AEDB should **not** encode app withdrawal policy semantics (for example `withdraw_delay` or `request_withdraw` workflow).  
Application logic (Arcana) should implement delayed withdrawals by scheduling a later effect batch with `require_available + accumulate(negative_delta)`.

## Processor macro
`#[processor(...)]` defines typed event consumers with generated registration + invocation wiring.

Expected options:
- `name`
- `source`
- `partition_by` (optional)
- `batch_size` (optional, defaulted)
- `owns` (writable state types)
- `reads` (read-only state types)
- `reads_accumulators` (read-only accumulator names)

`ProcessorCtx<E>` should provide:
- `events()`
- `state<T>()` for owned types
- `read_state<T>()` for read-only types
- `accumulate(name, key, delta, dedupe_id)`
- `accumulator_value(name, key)`
- `commit()` (atomic writes + checkpoint advance)

## Error model
- Rejected effect batches return structured typed errors.
- Processor panic/error does not advance checkpoint; events are retried.

## Startup flow
At startup, register:
- accumulators
- state schemas
- event schemas
- processors

Then start scheduler/ingestion; WAL replay + checkpoint recovery restore engine state before serving traffic.
