# Durable Queue & Generalized Leases

Two engine primitives that turn AEDB from a transactional store into the durable
substrate for asynchronous orchestration. The engine stays generic — it knows
queues, due times, leases, and transactions, not "keepers" or "workflows". The
application layer (Arcana) gives those meaning.

Both follow the established API-module conventions: state lives in KV under
engine-reserved key prefixes, mutated through the optimistic
`acquire_snapshot` → CAS `commit_envelope` retry loop, so it persists across
restart/checkpoint/restore with no new on-disk format. Application KV keys must
not begin with these prefixes.

| Prefix                 | Owner                    |
| ---------------------- | ------------------------ |
| `\x00aedb:lease:`      | `src/api/lease.rs`       |
| `\x00aedb:queue:`      | `src/api/queue.rs`       |

## Generalized lease (`src/api/lease.rs`)

A single-owner, time-bounded claim on a named resource with a monotonic
**fencing token**. The durable, wall-clock-expiring cousin of the in-memory
`src/locks.rs` primitive. Task ownership, keepers, monitors, and leader election
are all just `lease(<name>)`.

- `lease_acquire` / `lease_renew` / `lease_release` — acquire (or take over an
  expired/own lease, bumping the fencing token), heartbeat-extend, and release
  (idempotent).
- `lease_guarded_commit` — apply caller mutations **iff** the caller still holds
  the lease (token current and not expired), asserted against the lease key's
  version so a concurrent re-acquire fails the guarded write rather than racing
  it. Returns `FencedCommit::LeaseLost` to a fenced caller.
- `lease_status` — inspection.

Reuses `MonitorLease` / `LeaseOutcome` / `RenewOutcome` / `FencedCommit`;
`LeaseRecord` is the inspection view.

## Durable queue (`src/api/queue.rs`)

One time-ordered, priority-aware, lease-fenced queue is the substrate under task
queues, schedulers, timers, retries/backoff, dead-lettering, and workflow steps.

API: `queue_enqueue`, `queue_claim`, `queue_complete`, `queue_fail`,
`queue_heartbeat`, `queue_cancel`, `queue_get`, `queue_list` (plus `_as`
caller-scoped variants for the mutating calls).

### Internal key layout (per queue, within a project/scope)

`PREFIX || u16(qlen) || qname || kind || …`

| kind  | meaning                | ordering / key tail                         |
| ----- | ---------------------- | ------------------------------------------- |
| `p`   | primary record (by id) | `id`                                        |
| `s`   | scheduled index        | `BE(not_before) BE(seq) id` (due-time order) |
| `r`   | ready index            | `BE(MAX-priority) BE(seq) id` (priority order) |
| `f`   | in-flight index        | `BE(lease_deadline) BE(seq) id`             |
| `k`   | idempotency index      | `idempotency_key` → task id                 |
| `n`   | per-queue seq counter  | (single key)                                |

The **two-index "timer wheel"** is the key design choice: scheduled tasks wait
in due-time order (`s`); `queue_claim` first *promotes* matured scheduled tasks
into the priority-ordered ready index (`r`), then serves from `r`. A single
combined time+priority index would let maturation timestamps dominate priority
(immediately-enqueued tasks each read a slightly different `now`), so priority
would never matter for due work.

### How everything composes

- **delay / timer / scheduled execution** = `delay_micros` ⇒ `not_before = now + delay`.
- **retries / backoff** = `queue_fail` re-arms with exponential backoff into the
  scheduled index; exhausting `max_attempts` moves the task to the `Dead`
  (dead-letter) state, inspectable via `queue_list(.., Some(TaskState::Dead), ..)`.
- **transactional enqueue (outbox)** = `queue_enqueue` applies the caller's
  `extra_mutations` in the *same commit* — the task exists iff the surrounding
  state change commits.
- **scheduler re-arm / workflow next step / event fan-out** = `queue_complete`
  acks, applies caller mutations, and enqueues `follow_ups` (to any queue) in one
  atomic, lease-fenced commit.
- **workflow checkpoints** = `queue_heartbeat` durably records `progress_json`
  and extends the lease, so a resumed worker continues from the last step.

### Semantics worth knowing

- **Ownership fencing.** `queue_claim` bumps a per-task fencing token and sets a
  lease deadline. `complete`/`fail`/`heartbeat` require the current token, so a
  zombie owner is fenced out (`FencedCommit::LeaseLost`).
- **Lease expiry ≠ failure.** A crashed worker's expired lease is *reclaimed
  without backoff* — the work did not fail, the worker vanished — and the task
  goes straight back into the ready run-queue. Only `queue_fail` applies backoff.
- **Per-queue enqueue contention.** The `n` sequence counter is CAS-allocated, so
  concurrent enqueues to the *same* queue serialize and retry; distinct queues
  never contend. This is the one hot key per queue.
- **Reclaim/promote are best-effort and self-healing.** `queue_claim` runs a
  bounded promote + reclaim sweep first; conflicts mean another worker already
  did the work and are ignored. Dangling index entries (missing primary) are
  cleaned up opportunistically.

### What stays in Arcana / above the engine

Watch-push change feeds, secret storage, resource quotas, and service discovery
are deliberately *not* engine primitives. The event bus / outbox / exactly-once
half already exists (`event_outbox`, `effect_checkpoint`); a dispatcher that
drains them is an application loop composed from `queue_*` + `lease_*`.
