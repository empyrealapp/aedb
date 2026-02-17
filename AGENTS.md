# AGENTS.md

Guidance for coding agents working in `aedb`.

## What AEDB Is For

- `aedb` is an embedded storage engine focused on durable transactional commits, predictable crash recovery, snapshot-consistent reads, and operational safety (checkpoint, backup, restore).
- Primary API surface: `AedbInstance` in `src/lib.rs`.

When editing, prioritize correctness of storage semantics over API convenience or micro-optimizations.

## Core Behavior Focus

- Write path correctness: `Mutation` validation, WAL append semantics, sequence (`seq`) monotonicity, idempotency behavior.
- Read path correctness: snapshot consistency (`AtLatest`, `AtSeq`, `AtCheckpoint`) and permission-aware query/KV access.
- Recovery correctness: checkpoint + WAL replay behavior and strict/permissive recovery guarantees.
- Operational correctness: backup manifest integrity, restore chain correctness, checkpoint safety.

## Decision Guide: KV vs Table

- Use KV (`Mutation::KvSet`, `KvDel`, `KvIncU256`, `KvDecU256`) when access is mostly point lookups, prefix/range scans, counters, or opaque payload storage.
- Use tables (`DdlOperation::CreateTable` + `Mutation::Upsert`/`Delete`) when you need typed schema, relational constraints, indexes, filtering, ordering, joins, or long-term query evolution.
- Prefer one source of truth. Do not dual-write KV + table unless explicitly required.
- If cross-mode lookup is needed, prefer explicit projection mechanisms (`EnableKvProjection`) over ad-hoc duplication.

## Preflight Expectations

- Preflight is advisory, not a commit guarantee. State can change between preflight and append.
- For lowest TOCTOU risk, prefer `commit_with_preflight` / `commit_as_with_preflight` over separate `preflight` then `commit`.
- `preflight_plan` is useful for UX/planning, but code must still handle commit-time validation and permission failures.
- Always return structured errors (`AedbError`/`QueryError`) on rejected writes, even when preflight passed.

## Creating Tables Safely

- Ensure namespace exists first: create project and scope before `CreateTable`.
- Define stable primary keys and explicit column types up front; avoid schema churn for hot paths.
- Keep DDL in narrowly scoped mutations and test both success and invalid-schema/error paths.
- If ownership or read-policy behavior matters, include it in the same change and tests.

## Tuning and Preflight Checklist

- Choose `AedbConfig` durability/recovery modes intentionally (`DurabilityMode::Full` + `RecoveryMode::Strict` for production safety-first defaults).
- Use `DurabilityMode::OsBuffered` + `RecoveryMode::Permissive` only for local/dev workflows.
- Size limits (`max_transaction_bytes`, `max_kv_key_bytes`, `max_kv_value_bytes`) should be validated in tests for expected rejection behavior.
- Throughput knobs (`max_inflight_commits`, queue/epoch settings, parallel apply settings) must be tuned with workload-representative benchmarks, not synthetic microcases alone.
- Before high-risk write-path changes, run preflight-style validation in tests plus crash/recovery suites (`crash_matrix`, `stress`).

## Performance Concerns

- Keep write-path correctness first: never trade away WAL ordering, seq monotonicity, or replay determinism for throughput.
- Avoid unbounded scans in latency-sensitive paths; respect `max_scan_rows` and favor indexed predicates.
- Prefer snapshot-aware reads (`AtLatest`, `AtSeq`, `AtCheckpoint`) based on correctness needs; do not assume they are equivalent for cost/latency.
- Use `operational_metrics()` to validate impact (commit latency, conflict rate, queue depth, durable head lag) before and after tuning changes.
- Any performance change touching storage/recovery must include regression coverage and a clear rollback path.

## Architecture Map

- `src/commit/`: validation, transaction envelopes, async executor.
- `src/query/`: planning and execution (`SELECT`, filters, joins, ordering).
- `src/storage/`: keyspace and index backends.
- `src/wal/`, `src/checkpoint/`, `src/recovery/`: durability and restart path.
- `src/catalog/`, `src/manifest/`, `src/migration/`: schema and metadata.
- `src/backup/`, `src/snapshot/`, `src/version_store.rs`: operational support.

## Change Strategy

- Keep changes scoped to the requested behavior.
- Preserve public API compatibility unless explicitly asked to break it.
- Prefer narrow, test-backed patches over broad refactors.
- Do not alter persistence formats, manifest fields, frame encoding, or replay ordering unless required by the task.
- If you must change on-disk behavior, document it in `docs/` and add migration/recovery tests.

## Validation Expectations

Run the smallest relevant command first, then broaden:

```bash
# Focused tests
cargo test --test query_integration
cargo test --test backup_restore

# Full suite
cargo test

# Benchmarks (manual/perf work)
cargo bench --bench perf
```

If a change touches WAL/checkpoint/recovery behavior, prioritize:

```bash
cargo test --test crash_matrix
cargo test --test stress
```

If a change touches planner/index behavior, prioritize:

```bash
cargo test --test query_integration
```

## Documentation Expectations

- Update `README.md` when API usage, setup, or module intent changes.
- Update docs in `docs/` when behavior diverges from audits/plans:
- `docs/SPEC_COMPLIANCE_AUDIT.md`
- `docs/PRODUCTION_READINESS_REPORT.md`
- `docs/KEYSPACE_V2_MIGRATION.md`

## Non-Goals For Routine Tasks

- Do not rewrite the architecture for style consistency alone.
- Do not swap core data structures/backends without explicit need.
- Do not introduce broad dependency churn for small fixes.

## Definition of Done

- Code compiles and relevant tests pass.
- New behavior is covered by at least one test (unit or integration).
- Error paths return structured `AedbError`/`QueryError` variants.
- User-facing docs stay in sync with the implementation.
