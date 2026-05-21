#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "[production-readiness] formatting"
cargo fmt --check

echo "[production-readiness] clippy"
cargo clippy --workspace --all-targets -- -D warnings

echo "[production-readiness] dependency and supply-chain review"
"${SCRIPT_DIR}/supply_chain_gate.sh"

echo "[production-readiness] integration correctness"
cargo test --test query_integration -- --test-threads=1

echo "[production-readiness] operational stress smoke"
cargo test --test stress arcana_l1_balance_conservation_under_load -- --test-threads=1

echo "[production-readiness] security and recovery acceptance"
"${SCRIPT_DIR}/security_gate.sh"

echo "[production-readiness] complete"
