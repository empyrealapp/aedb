#!/usr/bin/env bash
set -euo pipefail

echo "[production-readiness] integration correctness"
cargo test --test query_integration -- --test-threads=1

echo "[production-readiness] operational stress smoke"
cargo test --test stress arcana_l1_balance_conservation_under_load -- --test-threads=1

echo "[production-readiness] security and recovery acceptance"
"$(dirname "$0")/security_gate.sh"

echo "[production-readiness] complete"
