# Operational Readiness

This document is the repo-local deployment checklist for AEDB. It is intentionally practical: every local claim should map to a command, configuration value, or operational artifact that can be checked before rollout.

Passing everything here is a baseline for operating AEDB in a serious deployment. It does not prove that a deployment is secure, audited, or operationally mature by itself. Those claims require the external validation items at the end of this document.

## Required Runtime Profile

- Use `AedbInstance::open_secure(...)` or `AedbInstance::open_production(...)`.
- Use `AedbConfig::production([u8; 32])` for durability-first workloads.
- Use `AedbConfig::low_latency([u8; 32])` only when visible-head latency matters and callers explicitly choose durable finality where required.
- Keep strict recovery enabled.
- Keep hash-chain enforcement enabled.
- Configure a manifest HMAC key.
- Prefer checkpoint encryption via `with_checkpoint_key([u8; 32])` for production deployments.

## Required Repo Gates

Run the full repo-local operational gate before rollout:

```bash
./scripts/production_readiness_gate.sh
```

That gate currently enforces repo-local behavior:

- `cargo fmt --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `./scripts/supply_chain_gate.sh`
- `cargo test --test query_integration -- --test-threads=1`
- `cargo test --test stress arcana_l1_balance_conservation_under_load -- --test-threads=1`
- `./scripts/security_gate.sh`

The security gate includes repo-local checks for:

- authorization boundaries and secure-mode API rejection
- backup/restore integrity under the checked fixtures
- read assertion and idempotency behavior
- crash/recovery behavior covered by the crash matrix, including ignored strict crash cases when explicitly run
- orderbook adversarial and chaos suites included in the gate

See [SECURITY_ACCEPTANCE_CRITERIA.md](SECURITY_ACCEPTANCE_CRITERIA.md) for the detailed list.

## What Repo Tests Prove

The repo gates can prove that the checked code paths pass deterministic unit tests, integration tests, property tests, and selected crash simulations in the test environment. In particular, they cover:

- commit atomicity, stale read-set rejection, idempotency replay, and no-partial-apply scenarios;
- strict authorization behavior for table/KV/index permissions, row policies, policy bypass, ownership, and secure-mode no-auth rejection;
- checkpoint/WAL replay, manifest HMAC rejection, hash-chain break detection, and selected crash windows;
- backup chain verification and restore consistency for the tested chain layouts.

The main references are:

- [COMMIT_SEQUENCING.md](COMMIT_SEQUENCING.md)
- [AUTHORIZATION_MODEL.md](AUTHORIZATION_MODEL.md)
- [PERSISTENCE_COMPATIBILITY.md](PERSISTENCE_COMPATIBILITY.md)
- [SECURITY_ACCEPTANCE_CRITERIA.md](SECURITY_ACCEPTANCE_CRITERIA.md)

## What Requires External Validation

Repo tests do not prove:

- that host authentication, API routing, or tenant binding cannot be bypassed;
- that HMAC/checkpoint keys are generated, stored, rotated, and revoked safely;
- that the production filesystem, disk, container, and backup storage meet durability assumptions;
- that restore procedures meet business RTO/RPO targets;
- that an adversary cannot exploit the embedding application around AEDB.

Do not describe a deployment as independently audited, penetration-tested, or suitable for regulated/high-risk workloads until the external requirements below have evidence.

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
- Any on-disk behavior change has a documented compatibility decision, migration/restore tests, and release notes; see [PERSISTENCE_COMPATIBILITY.md](PERSISTENCE_COMPATIBILITY.md).
- Runtime dependency additions have PR rationale covering license, advisory status, duplicate versions, and why the crate is not dev-only.

## External Requirements

These requirements cannot be satisfied by source changes in this repo alone:

- Independent code audit focused on commit atomicity, authorization, and recovery.
- Penetration testing of the embedding and API boundary in the host application.
- Key management review for HMAC/checkpoint key storage, rotation, and revocation.
- Restore drills with explicit RTO/RPO evidence.
