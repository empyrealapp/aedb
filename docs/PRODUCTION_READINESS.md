# Production Readiness

This document is the repo-local deployment gate for AEDB.

Passing everything here is necessary before production rollout. It is not sufficient for financial-grade claims unless the external validation items at the end are also complete.

## Required Runtime Profile

- Use `AedbInstance::open_secure(...)` or `AedbInstance::open_production(...)`.
- Use `AedbConfig::production([u8; 32])` for durability-first workloads.
- Use `AedbConfig::low_latency([u8; 32])` only when visible-head latency matters and callers explicitly choose durable finality where required.
- Keep strict recovery enabled.
- Keep hash-chain enforcement enabled.
- Configure a manifest HMAC key.
- Prefer checkpoint encryption via `with_checkpoint_key([u8; 32])` for production deployments.

## Mandatory Repo Gates

Run the full repo-local production gate before production rollout:

```bash
./scripts/production_readiness_gate.sh
```

That gate currently enforces:

- `cargo fmt --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `./scripts/supply_chain_gate.sh`
- `cargo test --test query_integration -- --test-threads=1`
- `cargo test --test stress arcana_l1_balance_conservation_under_load -- --test-threads=1`
- `./scripts/security_gate.sh`

The security gate includes:

- strict authorization suites
- strict backup/restore integrity suites
- read assertion + idempotency audit suites
- crash matrix including ignored strict crash cases
- orderbook adversarial and chaos suites

See [SECURITY_ACCEPTANCE_CRITERIA.md](SECURITY_ACCEPTANCE_CRITERIA.md) for the detailed list.

## Local Quality Commands

Use the explicit gates below instead of `cargo test --workspace --all-targets`. The all-targets test command enters benchmark binaries, so it is not the default correctness gate.

Fast correctness gate:

```bash
./scripts/correctness_gate.sh
```

Ignored long-running correctness and recovery tests:

```bash
./scripts/security_gate.sh
```

Benchmarks and benchmark threshold targets:

```bash
./scripts/benchmark_gate.sh
```

Dependency and supply-chain review:

```bash
./scripts/supply_chain_gate.sh
```

The supply-chain gate uses `cargo deny check` to track advisories, yanked crates, licenses, duplicate versions, wildcard dependencies, banned crates, and unknown registries or git sources. Runtime dependency additions must be justified in PRs, including why the dependency belongs in the runtime graph instead of dev-only tooling.

Clippy treats warnings as errors. `clippy::too_many_arguments` is not globally allowed: production-facing boundaries should keep argument lists small when a coherent typed options struct exists. Local allowances are acceptable only with a rationale when the function is an internal storage/query boundary where explicit snapshot, caller, namespace, limit, cursor, or durability state makes correctness review clearer than hiding those values in an unrelated bag.

## Release Checklist

- Production config uses `open_secure(...)` or `open_production(...)`.
- Application routes use authenticated `*_as` APIs only.
- Secrets are injected at boot, not committed to source control.
- Backup chain restore checks pass on a clean directory.
- Offline invariant checks pass on a restored dataset.
- Operational metrics are captured for baseline commit latency, queue depth, and durable-head lag.
- Runtime dependency additions have PR rationale covering license, advisory status, duplicate versions, and why the crate is not dev-only.

## External Requirements

These are required before claiming full production readiness for high-integrity workloads and cannot be satisfied by source changes in this repo alone:

- Independent code audit focused on commit atomicity, authorization, and recovery.
- Penetration testing of the embedding and API boundary in the host application.
- Key management review for HMAC/checkpoint key storage, rotation, and revocation.
- Restore drills with explicit RTO/RPO evidence.
