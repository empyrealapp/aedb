#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${IMAGE:-rust:latest}"
CPUS="${CPUS:-6}"
MEMORY="${MEMORY:-12g}"
RETRIES="${RETRIES:-3}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is not installed or not on PATH" >&2
  exit 1
fi

if ! docker version >/dev/null 2>&1; then
  echo "docker daemon is not running; start Docker and retry" >&2
  exit 1
fi

run_in_container() {
  local cmd="$1"
  docker run --rm \
    --cpus="${CPUS}" \
    --memory="${MEMORY}" \
    -e PATH=/usr/local/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    -v "${ROOT_DIR}:/work" \
    -v "${HOME}/.cargo/registry:/usr/local/cargo/registry" \
    -v "${HOME}/.cargo/git:/usr/local/cargo/git" \
    -w /work \
    "${IMAGE}" \
    bash -c "${cmd}"
}

retry() {
  local label="$1"
  local cmd="$2"
  local n=1
  while true; do
    echo "[${label}] attempt ${n}/${RETRIES}"
    if run_in_container "${cmd}"; then
      return 0
    fi
    if (( n >= RETRIES )); then
      echo "[${label}] failed after ${RETRIES} attempts" >&2
      return 1
    fi
    n=$((n + 1))
    sleep 2
  done
}

retry "chaos_ci_profile" "cargo test -q -p aedb-orderbook --test chaos_ci_profile"
retry "order_book_chaos_read_write_accuracy" "cargo test -q --test order_book_simulation order_book_chaos_read_write_accuracy"
retry "orderbook_perf" "cargo run --release -p aedb-orderbook --bin orderbook_perf"

echo "docker realism suite completed"
