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

Run the full repo-local production gate:

```bash
./scripts/production_readiness_gate.sh
```

That gate currently enforces:

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

## Release Checklist

- Production config uses `open_secure(...)` or `open_production(...)`.
- Application routes use authenticated `*_as` APIs only.
- Secrets are injected at boot, not committed to source control.
- Backup chain restore checks pass on a clean directory.
- Offline invariant checks pass on a restored dataset.
- Operational metrics are captured for baseline commit latency, queue depth, and durable-head lag.

## External Requirements

These are required before claiming full production readiness for high-integrity workloads and cannot be satisfied by source changes in this repo alone:

- Independent code audit focused on commit atomicity, authorization, and recovery.
- Penetration testing of the embedding and API boundary in the host application.
- Key management review for HMAC/checkpoint key storage, rotation, and revocation.
- Restore drills with explicit RTO/RPO evidence.
