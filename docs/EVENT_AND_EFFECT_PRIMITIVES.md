# Event & Effect Primitives

A set of application-facing primitives on `AedbInstance` for building rich,
event-sourced apps where **program state stays small and bounded** while history
and timeline data live in the canonical event log. They are generic engine
primitives (not app-specific tables): apps such as Arcana map them to domain
concepts (deposits, NAV, monitors).

All of these are built on existing durable primitives — the `event_outbox`
system table, KV with optimistic concurrency (`KeyVersion`/`KeyExists`
assertions), and the prevalidated commit path — so they persist through
restart, checkpoint, and restore with **no new on-disk format**. Read paths are
snapshot-consistent (`ConsistencyMode`) and bounded by `max_scan_rows`. Each
`_as` variant enforces caller permissions for secure mode.

> **Runnable example:** `cargo run --example event_effect_primitives`
> ([examples/event_effect_primitives.rs](../examples/event_effect_primitives.rs))
> walks the whole flow end to end.
>
> `ConsistencyMode` lives at `aedb::query::plan::ConsistencyMode`; the types
> below (`EventQuery`, `EffectClaim`, `MonitorCheckpointUpdate`,
> `EventRetentionPolicy`, …) are re-exported from the crate root (`aedb::`).

## 1. Event querying — `query_events`, `latest_events_by_topic`

Ergonomic queries over the canonical event log (`event_outbox`).

- Filter by `topic` (event type), project/scope, commit-sequence range, time
  range, and arbitrary payload fields (`recipient`, `instance_id`, `block`, …).
- Ascending or descending; `latest_events_by_topic(topic, n, …)` for "latest N".
- Commit-atomic cursor pagination: a page never splits the events of one commit,
  and `EventStreamPage::next_commit_seq` is the resume cursor.

```rust
let page = db.query_events(
    &EventQuery::new()
        .topic("deposit")
        .where_field("recipient", "0xabc")
        .after_commit_seq(cursor)
        .limit(50),
    ConsistencyMode::AtLatest,
).await?;
```

Payload-field filters are evaluated at scan time (not indexed); always pair them
with a topic and/or sequence bound. Indexed event attributes are a possible
future extension.

## 2. Exactly-once external-effect checkpoints — `begin_effect`, `complete_effect`, `effect_status`

The commit envelope already deduplicates *inbound* requests (`idempotency_key`).
These add the *outbound* half so monitors/tasks can replay without a duplicate
credit or duplicate external submission.

- `begin_effect(dedupe_key)` → `Fresh` (you own it; do the effect) /
  `InProgress { attempts }` (a prior attempt did not finish — reconcile before
  resubmitting a non-idempotent external call) / `AlreadyCommitted { result }`.
- `complete_effect(dedupe_key, result_json, mutations)` applies the internal
  mutations **and** marks the key committed in one atomic commit. Re-calling for
  the same key applies nothing and returns the recorded result. Under concurrency
  exactly one caller applies; the rest observe `AlreadyCommitted`.

The engine can make the *internal* state transition exactly-once; for a
non-idempotent external API the `InProgress { attempts }` signal is the hook to
build reconciliation on.

## 3. Bounded materialized projections — `projection_*`, `series_*`

Fast, queryable derived state that never grows unbounded.

- **Latest-per-key views**: `projection_put` / `projection_remove` /
  `projection_get` / `projection_list` — latest protocol status, per-account
  position summaries, the current set of pending intents.
- **Bounded series**: `series_append(series, point_json, max_points)` keeps only
  the last N points (NAV/price series, rolling decision logs); `series_read`
  returns them oldest- or newest-first.

## 4. Monitor checkpoint + lease + cursor — `monitor_*`

Durable progress state and a single-owner claim for chain monitors (ERC20, Nado).

- `monitor_acquire_lease(monitor, owner, ttl)` → `Acquired` / `Held`. Acquisition
  bumps a **fencing token** so a prior owner is fenced out.
- `monitor_renew_lease` / `monitor_release_lease`.
- `monitor_advance_checkpoint(monitor, fencing_token, update, mutations)` records
  scan progress (start/last-scanned block, last cursor, retry/error) **and**
  applies caller mutations atomically — but only while the lease is still held
  and live, otherwise `LeaseLost` (nothing applied).
- `monitor_status` exposes the full checkpoint + lease for inspection.

## 5. Event retention & compaction — `compact_events`

Prune the oldest events under an `EventRetentionPolicy` (`max_age_micros` and/or
`keep_last`) so hot state stays bounded. Safety:

- Events are pruned only as a contiguous prefix of the oldest sequences.
- With `respect_processor_checkpoints` (recommended), pruning never crosses the
  slowest reactive-processor checkpoint — unconsumed events are never deleted.
- The report names the archived sequence range and `more_remaining` for
  incremental runs. Requires `GlobalAdmin` in secure mode.

## 6. Debugging / inspection — `inspect_commit`, `find_events_by_key`, `event_log_summary`, `inspect_namespace`

Read-only operator tools that stitch the primitives together:

- `inspect_commit(seq)` — the events a commit emitted.
- `find_events_by_key(topic, key)` — which commit indexed a value (e.g. a user's
  share price).
- `event_log_summary()` — size and sequence range of the log.
- `inspect_namespace(project, scope)` — a one-call dashboard of monitors
  (checkpoint + lease) and external-effect checkpoint counts.

## Reserved KV key prefixes

Effect checkpoints, projections, and monitor state are stored in KV under
engine-reserved prefixes within the caller's own project/scope. Application KV
keys must not begin with these byte prefixes:

- `\x00aedb:effect:` — effect checkpoints
- `\x00aedb:proj:` — latest-per-key projection views
- `\x00aedb:series:` — bounded series
- `\x00aedb:monitor:` — monitor checkpoints/leases
