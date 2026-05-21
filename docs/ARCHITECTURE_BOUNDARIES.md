# Architecture Boundaries

AEDB is a storage engine first. Feature work should keep the core path easy to
review for ordering, durability, and recovery behavior.

## Core Modules

- `src/commit/`: validation, commit envelopes, WAL ordering, apply logic, and
  executor coordination.
- `src/storage/`: in-memory and disk-backed keyspace structures, indexes,
  value storage, and segment stores.
- `src/wal/`, `src/checkpoint/`, `src/recovery/`: durable frames, checkpoint
  materialization, manifest handling, and replay.
- `src/query/`: planning, execution, predicates, cursors, joins, ordering, and
  read-set capture.
- `src/api/`: public `AedbInstance` method groups. API modules should delegate
  storage behavior to the core modules rather than growing independent rules.

## Boundary Rules

- Do not add feature-specific business concepts to commit, storage, recovery, or
  query internals. Those modules should speak in database terms: rows, keys,
  indexes, commits, snapshots, checkpoints, and permissions.
- Keep application-oriented helpers behind explicit API modules or integration
  interfaces. They should not change WAL ordering, replay semantics, or
  snapshot visibility rules.
- Prefer small data structures that name the boundary being crossed over long
  argument lists. Keep explicit arguments only where they make storage
  correctness easier to audit.
- Large files are tolerated only when they are intentionally central and covered
  by a line-budget entry in `tests/source_shape.rs`.

## Large File Policy

The source-shape test is not a style contest. It is a tripwire for feature
pile-up. If a file exceeds the default line budget, either split it along a
stable domain boundary or add a narrow exception with a reason that explains why
keeping the code together improves correctness review.

Good split points:

- DDL/schema evolution separate from row mutation application.
- Foreign-key and constraint enforcement separate from basic row/KV writes.
- Query cursor/ordering helpers separate from scan and join execution.
- Test fixtures separate from scenario tests.

Avoid moving code simply to reduce a line count when it would obscure commit or
recovery ordering.
