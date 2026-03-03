# Arcana-Oriented AEDB API Sketch (D9, D10, D13)

## Scope
This document defines concrete AEDB API additions for:
- `D9` Single-envelope action commit
- `D10` Native typed numeric KV mutations with fail semantics
- `D13` Envelope idempotency result semantics for exactly-once actions

The goal is to let Arcana submit one hot-path commit per action, with no second metadata commit and no userland numeric read/modify/write.

## Current AEDB baseline
Already present in AEDB today:
- `TransactionEnvelope` with `assertions`, `write_intent`, `idempotency_key`, `base_seq`
- `Mutation::KvIncU256` and `Mutation::KvDecU256`
- `ReadAssertion::KeyVersion`, `KeyExists`, `KeyCompare`

Main gap for Arcana:
- No dedicated single-call action helper with typed outcome for `applied / duplicate / rejected`
- Numeric KV ops do not express Arcana semantics (`soft fail`, `max/min`, `missing policy`) directly
- Idempotent replay does not return explicit duplicate-hit metadata usable by Arcana to skip processed markers

## D9: Single-envelope action commit API

### New public API
```rust
// aedb/src/lib.rs
pub async fn commit_action_envelope(
    &self,
    req: ActionEnvelopeRequest,
) -> Result<ActionCommitResult, AedbError>;
```

### Request type
```rust
// aedb/src/commit/action.rs
use crate::commit::tx::{IdempotencyKey, ReadAssertion, WriteClass};
use crate::commit::validation::Mutation;
use crate::permission::CallerContext;

#[derive(Debug, Clone)]
pub struct ActionEnvelopeRequest {
    pub caller: Option<CallerContext>,
    pub idempotency_key: IdempotencyKey,
    pub write_class: WriteClass,
    pub base_seq: u64,

    // All action-level guards (instance version, permissions, balance predicates, etc.)
    pub assertions: Vec<ReadAssertion>,

    // Full atomic write set:
    // - effect mutations
    // - instance state/version update mutation
    // - optional tx status mutation
    pub mutations: Vec<Mutation>,
}
```

### Result type
```rust
#[derive(Debug, Clone)]
pub struct ActionCommitResult {
    pub commit_seq: u64,
    pub durable_head_seq: u64,
    pub outcome: ActionCommitOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionCommitOutcome {
    Applied,
    Duplicate,
}
```

### Error shape expectations
- Assertion failure remains an error (`AedbError::AssertionFailed`) with structured failed assertion metadata.
- Validation failure remains `AedbError::Validation`.
- Duplicate idempotency key is **not** an error; returns `ActionCommitOutcome::Duplicate`.

### Why this helps Arcana
Arcana can stage in one envelope:
- capability + scope/key effect mutations
- instance state bytes + state version increment
- optional tx status mutation

That removes the second hot-path call currently made after effects (`commit_action_metadata_batch`).

## D10: Native typed numeric KV mutations

### New mutation family
```rust
// aedb/src/commit/validation.rs
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KvU256MissingPolicy {
    TreatAsZero,
    Reject,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KvU256UnderflowPolicy {
    Reject,
    NoOp,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KvU256OverflowPolicy {
    Reject,
    Saturate,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Mutation {
    // existing variants...

    KvAddU256Ex {
        project_id: String,
        scope_id: String,
        key: Vec<u8>,
        amount_be: [u8; 32],
        on_missing: KvU256MissingPolicy,
        on_overflow: KvU256OverflowPolicy,
    },

    KvSubU256Ex {
        project_id: String,
        scope_id: String,
        key: Vec<u8>,
        amount_be: [u8; 32],
        on_missing: KvU256MissingPolicy,
        on_underflow: KvU256UnderflowPolicy,
    },

    KvMaxU256 {
        project_id: String,
        scope_id: String,
        key: Vec<u8>,
        candidate_be: [u8; 32],
        on_missing: KvU256MissingPolicy,
    },

    KvMinU256 {
        project_id: String,
        scope_id: String,
        key: Vec<u8>,
        candidate_be: [u8; 32],
        on_missing: KvU256MissingPolicy,
    },
}
```

### Semantics required
- All arithmetic is unsigned 256-bit big-endian.
- `TreatAsZero` means missing key is interpreted as `0` before op.
- `NoOp` means mutation succeeds without changing state when underflow condition is hit.
- `Reject` propagates `AedbError::Validation` (or dedicated arithmetic error variant).
- Mutations remain fully atomic with all other mutations in the same envelope.

### Arcana mapping
- `SCOPE_DATA_INC` strict -> `KvAddU256Ex{TreatAsZero, Reject}`
- `SCOPE_DATA_DEC` strict -> `KvSubU256Ex{TreatAsZero, Reject}`
- `SCOPE_DATA_DEC` soft -> `KvSubU256Ex{TreatAsZero, NoOp}`
- `SCOPE_DATA_MAX/MIN` -> `KvMaxU256/KvMinU256`

This replaces in-process shadow-map arithmetic in `kv_store_effects` for the common path.

## D13: Envelope idempotency outcome contract

### API addition (commit result)
```rust
// aedb/src/commit/result.rs or existing result module
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum IdempotencyOutcome {
    Applied,
    Duplicate,
}

pub struct CommitResult {
    // existing fields
    pub idempotency: IdempotencyOutcome,
    pub canonical_commit_seq: u64, // for Duplicate, original seq; for Applied, current seq
}
```

### Behavior
- First envelope with key `K`: apply writes, record idempotency, return `Applied`.
- Later envelope with key `K` (within retention): skip writes, return `Duplicate` with `canonical_commit_seq`.
- No error for duplicate replay.

### Why this helps Arcana
Arcana can remove explicit "processed marker" writes for action idempotency and rely on engine-level exactly-once behavior.

## Arcana integration plan (call-site level)

## 1) Replace two-phase hot commit with one API call
Current flow in Arcana hot path:
1. `apply_commit_time_effects_with_kv_store(...)`
2. `commit_action_metadata_batch(...)`

Target flow:
1. Build one `ActionEnvelopeRequest` containing:
   - assertions: instance version + effect assertions
   - mutations: effect mutations + instance metadata/state mutation + optional tx status
   - idempotency key: action/transaction id
2. Call `commit_action_envelope(...)`
3. Branch by outcome:
   - `Applied` -> normal success
   - `Duplicate` -> return current state without replaying side-effects

## 2) Replace userland numeric RMW
In Arcana effect processor:
- stop pre-reading and calculating next numeric values in shadow maps for common u256 ops
- emit `KvAddU256Ex`/`KvSubU256Ex`/`KvMaxU256`/`KvMinU256` directly

## 3) Keep accumulators for hot shared keys
For house-balance style contention keys, keep accumulator mutations as primary path.
For isolated per-user keys, numeric KV mutations provide low-overhead direct updates.

## Acceptance tests (AEDB)
- `action_envelope_applied_once`: single envelope applies all writes atomically.
- `action_envelope_duplicate_returns_duplicate_outcome`: replay with same key returns `Duplicate` and no writes.
- `kv_sub_u256_soft_noop`: underflow with `NoOp` succeeds without mutation.
- `kv_sub_u256_strict_reject`: underflow with `Reject` fails transaction.
- `kv_max_min_u256`: correct max/min behavior for existing and missing keys.
- `single_envelope_atomicity`: mixed effect + instance metadata mutations all roll back on assertion failure.

## Rollout order
1. Implement D13 result semantics (low blast radius).
2. Implement D10 mutation variants and executor handling.
3. Implement D9 helper API as thin wrapper over `commit_envelope` with typed outcome.
4. Switch Arcana hot path to D9 + D10; remove second metadata commit path.
