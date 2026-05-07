# aedb perf investigation — 2026-05-07

Single-session investigation prompted by "we want aedb to work more on disk, less in RAM."

## Starting premise vs reality

Premise: aedb is in-memory only; needs disk persistence.

Reality: aedb already saves everything durably to disk. WAL + checkpoint + manifest, all `sync_data`/`sync_all` per `DurabilityMode::Full`. Verified by file map:

- WAL frames synced per commit at `src/wal/segment.rs:235,245`
- Checkpoints at `src/checkpoint/writer.rs:27-60` (encrypted+zstd, fsync'd)
- Manifest HMAC + atomic write at `src/manifest/atomic.rs:15-59`
- Recovery replays WAL from checkpoint at `src/recovery/replay.rs`
- Reactive processor configs durable in `_system.app.reactive_processor_registry` table
- Only intentional RAM-only state: snapshot lease handles, processor handler closures (Rust closure constraint), in-flight commit batch state, runtime metrics

No gaps. The "more on disk" framing reduced to: **less data resident in RAM at runtime**.

## Tooling set up

- `flake.nix` — Rust nightly via rust-overlay + `cargo-flamegraph`, `samply`, `linuxPackages.perf`, `heaptrack`, `gnuplot`
- `.envrc` — direnv hook for `nix develop`
- `Cargo.toml` — added `dhat` and `pprof = { version = "0.13", features = ["flamegraph"] }` to dev-deps
- `benches/scale.rs` — table-size sweep, KV value-size sweep, index-count sweep (criterion)
- `examples/heap_profile.rs` — dhat heap profile with seed/measure phases
- `examples/cpu_profile.rs` — pprof in-process CPU flamegraph (no perf permissions needed; `kernel.perf_event_paranoid=2` blocks `cargo flamegraph`)
- `examples/ram_probe.rs` — RSS-vs-counter measurement at different shapes

## What we measured

### Throughput baseline (criterion, default config)

| Bench | Median | Notes |
|---|---|---|
| `hot_upsert_single_row` (Full durability) | 5.42 ms | fsync-bound |
| `hot_upsert_single_row_batch` | 401 µs | group commit kicks in |
| `hot_upsert_single_row_os_buffered` | 210 µs | no fsync |
| `insert_64_rows_as_64_commits` | 354 ms | 1 fsync per row |
| `insert_64_rows_as_1_batch_commit` | **5.69 ms** | 62× faster — batching is huge |
| Reads (point/index/KV) | 0.5–1.5 µs | not bottleneck |
| `e2e_bootstrap_commit_and_query` | exactly 1.001 s | suspicious round number, likely fixed sleep somewhere; not investigated |

### Scaling sweep (pre-fix)

| `upsert_at_table_size_N` | Median | vs 10k |
|---|---|---|
| 10k | 5.43 ms | 1× |
| 100k | 9.02 ms | 1.66× |
| 500k | **46.07 ms** | **8.5×** |

Worse than log N. From 100k to 500k it's basically linear — something amortized-O(N) on the commit path past ~100k.

KV value-size and index-count sweeps were within noise — fsync dominates everything there.

### Heap profile (dhat) at 500k seed + 2k measured commits

- 488 MB lifetime allocation across the 2k measured commits (~244 KB / commit)
- Peak gmax: 49.9 MB during measurement window
- 517 allocations per commit
- Top 8 allocators: all `im::OrdMap` / `im::HashMap` clone-and-mutate paths on `rows`, `row_versions`, `pk_hash`, `row_cache`, `row_versions_cache`
- Allocation count per commit barely grew from 100k seed to 500k seed (~14% growth) → **alloc churn is NOT the cause of the 5× latency growth**

### CPU flamegraph (pprof) at 500k seed

```
99.21%  process_commit_epoch
98.60%  Keyspace::estimate_memory_bytes  ← THE CULPRIT
 9.20%  im::nodes::btree::Iter::next     ← inside it
```

## Root cause

`Keyspace::estimate_memory_bytes()` at `src/storage/keyspace.rs:1614` walks every row, KV entry, projection, and accumulator entry — **O(N)**.

It was being called unconditionally on every commit at `src/commit/executor/internals.rs:1645` to enforce `max_memory_estimate_bytes` before WAL append. (A second post-apply check at line 1844 was already rate-limited to 250 ms; the pre-WAL one wasn't.)

Verified by math: 500k × ~37 ms commit, fsync = 5 ms, remaining ~32 ms = the O(N) walk.

## Fix

Replaced the O(N) walk with an O(1) running counter.

- Added `mem_bytes: usize` field to `Keyspace` and `KeyspaceSnapshot` (with `#[serde(default)]`)
- Cost helpers: `row_mem_cost`, `kv_entry_cost`, `accumulator_dedupe_cost`, `accumulator_exposure_cost`, `ACCUMULATOR_DELTA_COST`, `table_data_mem_cost`, `kv_data_mem_cost`, `projection_data_mem_cost`, `accumulator_data_mem_cost`, `namespace_mem_cost`
- `estimate_memory_bytes()` now returns `self.mem_bytes`
- `recompute_memory_bytes_full()` keeps the old O(N) logic for parity tests + post-load reseed
- `refresh_mem_bytes()` reseeds the counter from a full walk (used after constructing from external data)

Mutation sites updated to maintain delta:

- `upsert_row`, `upsert_row_by_encoded_pk` — replace path tracks old vs new cost
- `delete_row`, `delete_row_by_encoded`
- `kv_set`, `kv_del`
- `append_accumulator_delta`, `expose_accumulator`, `expose_accumulator_batch`, `release_accumulator_exposure`
- `insert_async_projection`, `remove_async_projection`, `take_async_projection`
- `drop_table`, `drop_scope`, `drop_project`, `drop_accumulator`
- `insert_namespace`
- `merge_parallel_namespace_result` in `src/commit/executor/internals.rs:2338` — tracks delta inline
- New `insert_namespace_unchecked` for the parallel runtime's throwaway local keyspace (avoids re-walking the whole base namespace per task)

External construction paths reseed via `refresh_mem_bytes`:

- `src/checkpoint/loader.rs:60` (after deserializing checkpoint)
- `src/lib.rs:9040` (after partial-restore merge)
- `src/version_store.rs:291` (`snapshot_to_keyspace` passes through)

Parity tests added at `src/storage/keyspace.rs`:

- `mem_bytes_running_counter_matches_full_walk` — exercises every mutation type, asserts `mem_bytes == recompute_memory_bytes_full()` after each
- `refresh_mem_bytes_recovers_from_external_construction` — confirms reseed path

## Results

### Bench (vs original walk-based baseline)

| Bench | Before | After | Δ |
|---|---|---|---|
| `upsert_at_table_size_10000` | 5.43 ms | 5.24 ms | -3.5% |
| `upsert_at_table_size_100000` | 9.02 ms | 5.21 ms | **-42.3%** |
| `upsert_at_table_size_500000` | 46.07 ms | **5.27 ms** | **-88.6%** |

**Latency is now flat in N.** 500k upsert equals 10k upsert — just the fsync floor.

### Tests

372 lib tests + 31 integration test binaries. All pass. Including the strict-budget test `lib_tests::memory_limit_is_enforced_before_wal_commit` which had been the canary for the broken rate-limit attempt.

## What we didn't fix — RAM still grows linearly

The latency fix changed nothing about resident memory. RAM probe results:

| Shape | Records | aedb counter | **Actual RSS** | Counter undercount |
|---|---|---|---|---|
| Rows (3-col) | 10k | 1.46 MiB | 50 MiB | **34×** |
| Rows (3-col) | 100k | 14.7 MiB | **614 MiB** | **42×** |
| Rows (3-col) | 500k | 73.8 MiB | **3.23 GiB** | **45×** |
| KV-64B | 10k | 957 KiB | 14.6 MiB | 16× |
| KV-16KB | 10k | 156 MiB | 236 MiB | **1.5×** |

Three findings:

1. **The counter is a 45× under-estimate for tables.** Default `max_memory_estimate_bytes: 2 GiB` allows ~13M rows by counter — that's ~90 GB resident reality. Capacity planning based on it puts users in OOM territory.
2. **Tables are the expensive shape; large-value KV is the cheap shape.** Per row: ~6.9 KB resident. Per KV-16KB record: 24.7 KB (only 1.5× overhead — the value itself dominates).
3. **OOM risk is real for any large-table workload.** 1M rows ≈ 6.5 GB, 10M rows ≈ 65 GB.

Top suspects for the 45× gap (from the dhat profile):

- Redundant `row_cache: HashMap` + `row_versions_cache: HashMap` + `pk_hash: HashMap` next to the `OrdMap` source-of-truth
- `im::OrdMap` HAMT/btree node Arc overhead
- `version_store` retaining historical snapshots (default `max_versions: 1024`, `min_version_age_ms: 5000`)
- Allocator fragmentation

## Recommendations, ranked

### Cheap (days)

1. **Drop redundant caches** in `TableData` (`row_cache`, `row_versions_cache`, `pk_hash`). 1–2 days. Probably halves table RAM, cuts ~3 of 4 HAMT clones per commit. Re-measure read latency to confirm acceptable regression. **The dhat profile is the evidence; the bench harness is ready to verify.**
2. **Make the counter honest.** Either multiply by an empirical structural factor or replace `max_memory_estimate_bytes` with an actual RSS-based check via `/proc/self/statm`. ~½ day. Otherwise users keep getting bitten by the 45× gap.
3. **Tune `version_store` retention.** Default `max_versions: 1024` is generous. 64–128 is plenty for most workloads. Hours of testing, no code change.

### Medium (weeks)

4. **Cold-table spill.** Annotate certain tables/projections as cold; they get checkpointed and unloaded from RAM, loaded on first read after restart. Hot tables stay full-RAM. 3–4 weeks.
5. **Async projection lazy materialization.** `AsyncProjectionData::rows` currently full-RAM. Could be lazily computed from base tables on read, or refuse materialization past N rows.
6. **Value-log spill.** Keep keys + small metadata in `OrdMap`, spill `KvEntry::value` to append-only file. Only useful if values dominate (rare given KV-16KB is already efficient).

### Big (months) — only if 1–5 don't suffice

7. **Paged backend (redb adapter).** Add `PrimaryIndexBackend::Paged` variant that stores rows in redb (single file, btree, mmap'd, MVCC). Phased: storage trait → KV-only path → tables → secondary indexes. ~3–4 months. Snapshot semantics close to existing `im` clone-as-snapshot model.

### Don't

- Custom pager + COW B-tree from scratch (6+ months, reinvents SQLite/LMDB edge cases).
- LSM-style memtable/SSTables/compaction. Wrong shape for aedb's existing snapshot-MVCC model.

## Questions for the coworker

The right next step depends on what they actually meant. High-signal questions:

1. **What's the symptom?** RAM use, slow opens, lost data, OOM, something else. *Most important.*
2. **Dataset size?** Rows × scopes × tables. Compared to machine RAM.
3. **Has it actually OOMed or is this preventive?** If preventive, the latency fix + retention tuning may already cover them.
4. **Is the data dir on tmpfs / `/dev/shm` / a container ephemeral mount?** Common gotcha — looks like "in-memory only" but is actually a config issue.
5. **Data shape: rows-with-many-cols or blobs-with-keys?** If blobs, KV-with-large-values is ~5× more efficient than tables.
6. **Where will it run?** Server (then redb works), mobile (constrained backend), WASM (no filesystem at all).
7. **Single-file format required?** Current is dir-of-segments. If they need one `.aedb` file → format change, not just RAM concern.

If they say "OOMs at 50 GB on a 32 GB box": #1, #2, #3, then probably #7.
If they say "I don't trust data is being saved": show them the WAL files, end conversation.
If they say "we run in WASM/sandbox": different problem entirely.

## How to reproduce

```bash
# Throughput baseline
cargo bench --bench perf -- --save-baseline main

# Scaling at table size (the smoking gun)
cargo bench --bench scale -- --save-baseline scale_main

# CPU flamegraph (no perf perms needed)
cargo run --release --example cpu_profile -- 500000 2000
# → profiles/flamegraph.svg

# Heap profile
cargo run --release --example heap_profile -- 500000 2000
# → profiles/dhat-heap.json
# View at https://nnethercote.github.io/dh_view/

# RAM shape probe at multiple scales
./target/release/examples/ram_probe 100000 rows
./target/release/examples/ram_probe 100000 kv
./target/release/examples/ram_probe 10000 kv-large

# Parity test
cargo test --release --lib mem_bytes_running_counter_matches_full_walk
```

## Files touched

- `src/storage/keyspace.rs` — `mem_bytes` field, cost helpers, mutation sites, parity tests
- `src/commit/executor/internals.rs` — `merge_parallel_namespace_result` tracks delta
- `src/commit/executor/parallel_runtime.rs` — uses `insert_namespace_unchecked`
- `src/checkpoint/loader.rs` — `refresh_mem_bytes` after build
- `src/lib.rs` — `refresh_mem_bytes` after partial-restore merge
- `src/version_store.rs` — `mem_bytes` passed through `snapshot_to_keyspace`
- `Cargo.toml` — `dhat`, `pprof`, `[[bench]] name = "scale"`

## Files added

- `flake.nix`, `.envrc` — dev shell
- `benches/scale.rs` — scaling sweeps
- `examples/heap_profile.rs` — dhat heap profile
- `examples/cpu_profile.rs` — pprof CPU flamegraph
- `examples/ram_probe.rs` — RSS measurement
- `profiles/dhat-heap.json`, `profiles/flamegraph.svg` — captured outputs

## TL;DR

- **Found and fixed a real bug**: a per-commit O(N) budget walk that capped write throughput on tables larger than ~100k rows. 88.6% reduction at 500k.
- **Confirmed durability is fine**: nothing important was missing from disk.
- **Quantified the actual RAM problem**: 45× gap between aedb's counter and resident memory for tables. ~6.9 KB per row of resident overhead. The OOM concern was real, just for different reasons than originally framed.
- **Diagnostic kit is now permanent**: criterion benches, dhat heap, pprof flamegraph, RAM probe. Future questions answered the same way.
