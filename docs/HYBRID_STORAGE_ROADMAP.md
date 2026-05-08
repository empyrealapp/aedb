# Hybrid Storage Roadmap

AEDB's production target is disk-authoritative storage with memory used as a
bounded acceleration layer. The database should be able to scale past RAM while
keeping hot reads and writes fast through page, row, and index caches.

## Current PR Scope

This PR adds two concrete pieces:

- Disk-backed KV payloads: production and secure profiles store every non-empty
  KV payload in `values.aedbdat`, with a bounded hot-value cache.
- `PagedStore`: an append-only, checksummed page file with stable `PageId`s and
  a bounded LRU page cache.

`PagedStore` is the substrate for disk-backed keyspace structures. It provides:

- fixed-size page frames
- deterministic page IDs
- batched page appends with a single file lock and flush
- per-page BLAKE3 integrity checks
- partial-frame rejection on open
- bounded in-memory page caching
- durable `sync_all`

The existing single-file backup archive path already includes all regular files
written under the backup directory, including future page files. Large files use
chunked compression and per-chunk encryption so backup and restore do not have
to materialize a whole page file in memory. The remaining manifest work is to
record page roots and page-file membership explicitly when rows, KV keys, and
indexes move onto `PagedStore`.

## What Is Still Memory-Resident

The current keyspace still keeps these structures in memory:

- KV keys and `KvEntry` metadata
- table primary rows and row versions
- secondary indexes
- async projections
- accumulators
- snapshot and version metadata

These are still protected by `max_memory_estimate_bytes`. If a commit still
exceeds that ceiling after KV payload spill, AEDB rejects it before WAL append.

## Next Migration Steps

1. Add a disk-backed primary row store using `PagedStore` pages and a bounded
   row cache.
2. Add a disk-backed KV key index whose leaves point to KV value refs.
3. Move secondary index leaves onto pages, keeping hot internal/root pages in
   memory.
4. Make snapshots reference immutable page roots instead of cloning whole maps.
5. Extend checkpoint/backup/restore manifests to include page roots and page
   file membership.
6. Add page-cache metrics and workload benchmarks for hot/cold mixes.

## Non-Goals For This PR

This PR does not claim general larger-than-memory table/index support. It lays
the first storage primitive required for that support and hardens production KV
payload behavior so payload data no longer has to live in memory.
