# AEDB Migration System

## Purpose
Migrations are for structural engine changes that require AEDB action, not for every schema edit in application code.

## Core rule
With tagged-field serialization, adding/removing state/event fields is schema evolution handled by the format:
- unknown tags are ignored
- missing tags deserialize to defaults
- no startup rewrite required

Because of that, **field add/remove does not require a migration entry**.

## What requires a migration entry
- Create or drop state
- Create or drop event
- Create or drop processor
- Create or drop accumulator
- Alter accumulator runtime config
- Add or drop index
- Field type changes that require transform logic
- Field renames (tag/name alias mapping)

## Tagged-field rules
- Tags are never reused.
- Dropped fields retire their tag permanently.
- New fields must have a sensible default (or use `Option<T>`).
- Derive macros must reject duplicate tag definitions at compile time.

## Startup behavior
AEDB executes unapplied migrations in version order before serving traffic.
Applied metadata is tracked internally (`version`, `name`, timestamp/checkpoint metadata).

## Background migration tasks
Some migrations can complete asynchronously:
- Index backfill: index is marked building; writes maintain it immediately; queries can fail as not-ready until complete.
- Optional rewrite sweep for expensive type transforms on cold data.

## WAL replay compatibility
WAL replay must remain valid across migration versions:
- each WAL entry includes the schema version active at write time
- replay resolves the versioned schema chain and applies forward transforms when needed

If schema changes are additive-only (new tagged fields), replay works without special migration transforms.
