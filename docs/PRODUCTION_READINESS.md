# Production Readiness

This document is the repo-local deployment gate for AEDB. It is intentionally practical: every local claim should map to a command, configuration value, or operational artifact that can be checked before rollout.

Passing everything here is necessary before production rollout. It does not prove that a deployment is secure, financial-grade, or operationally ready by itself. Those claims require the external validation items at the end of this document.

## Required Runtime Profile

- Use `AedbInstance::open_secure(...)` or `AedbInstance::open_production(...)`.
- Use `AedbConfig::production([u8; 32])` for durability-first workloads.
- Use `AedbConfig::low_latency([u8; 32])` only when visible-head latency matters and callers explicitly choose durable finality where required.
- Keep strict recovery enabled.
- Keep hash-chain enforcement enabled.
- Configure a manifest HMAC key.
- Prefer checkpoint encryption via `with_checkpoint_key([u8; 32])` for production deployments.

## Mandatory Repo Gates

Run the full repo-local production gate:

```bash
./scripts/production_readiness_gate.sh
```

That gate currently enforces repo-local behavior:

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

Do not describe a deployment as security-audited, penetration-tested, financial-grade, or production-ready for high-integrity workloads until the external requirements below have evidence.

## Release Checklist

- Production config uses `open_secure(...)` or `open_production(...)`.
- Application routes use authenticated `*_as` APIs only.
- Secrets are injected at boot, not committed to source control.
- Backup chain restore checks pass on a clean directory.
- Offline invariant checks pass on a restored dataset.
- Operational metrics are captured for baseline commit latency, queue depth, and durable-head lag.
- Any on-disk behavior change has a documented compatibility decision, migration/restore tests, and release notes; see [PERSISTENCE_COMPATIBILITY.md](PERSISTENCE_COMPATIBILITY.md).

## External Requirements

These are required before claiming production readiness for high-integrity workloads and cannot be satisfied by source changes in this repo alone:

- Independent code audit focused on commit atomicity, authorization, and recovery.
- Penetration testing of the embedding and API boundary in the host application.
- Key management review for HMAC/checkpoint key storage, rotation, and revocation.
- Restore drills with explicit RTO/RPO evidence.
