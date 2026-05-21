#!/usr/bin/env bash
set -euo pipefail

echo "[benchmark-gate] criterion perf bench"
cargo bench --bench perf

echo "[benchmark-gate] benchmark acceptance targets"
AEDB_ENFORCE_BENCH_GATES=1 cargo test --test benchmark_gate benchmark_gate_doc_matrix -- --ignored --test-threads=1
AEDB_ENFORCE_BENCH_GATES=1 cargo test --test benchmark_gate benchmark_coordinator_vs_parallel_lanes -- --ignored --test-threads=1

echo "[benchmark-gate] complete"
