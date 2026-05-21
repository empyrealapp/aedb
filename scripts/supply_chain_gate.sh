#!/usr/bin/env bash
set -euo pipefail

if ! command -v cargo-deny >/dev/null 2>&1; then
    echo "cargo-deny is required for the supply-chain gate." >&2
    echo "Install it with: cargo install cargo-deny --locked" >&2
    exit 127
fi

echo "[supply-chain-gate] cargo deny"
cargo deny check

echo "[supply-chain-gate] complete"
