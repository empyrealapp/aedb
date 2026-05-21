#!/usr/bin/env bash
set -euo pipefail

echo "[correctness-gate] formatting"
cargo fmt --check

echo "[correctness-gate] clippy"
cargo clippy --workspace --all-targets -- -D warnings

echo "[correctness-gate] library tests"
cargo test --lib

echo "[correctness-gate] workspace integration tests"
cargo test --workspace --tests --exclude aedb-orderbook --exclude aedb-explorer

echo "[correctness-gate] orderbook integration tests"
cargo test -p aedb-orderbook --tests

echo "[correctness-gate] complete"
