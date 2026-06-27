//! Generalized distributed leases.
//!
//! A lease is a single-owner, time-bounded claim on a named resource with a
//! monotonic **fencing token**. It is the coordination primitive underneath
//! every long-running owner in the platform: task ownership, keepers, monitors,
//! and leader election are all just `lease(<name>)`. The engine never knows what
//! a "keeper" or "monitor" is — it only knows leases.
//!
//! This is the durable, time-based cousin of the in-memory [`crate::locks`]
//! primitive. Locks are transaction-scoped, non-persisted, and released on
//! restart; a lease persists across restart/checkpoint/restore and expires by
//! wall-clock TTL so a dead owner is automatically displaced.
//!
//! Lifecycle:
//! ```ignore
//! match db.lease_acquire(p, s, "keeper:cleanup", "worker-1", 30_000_000).await? {
//!     LeaseOutcome::Held { .. } => return,            // someone else owns it
//!     LeaseOutcome::Acquired(lease) => {
//!         // ... do work, periodically renewing ...
//!         db.lease_renew(p, s, "keeper:cleanup", lease.fencing_token, 30_000_000).await?;
//!         // apply state changes only while we still provably hold the lease:
//!         db.lease_guarded_commit(p, s, "keeper:cleanup", lease.fencing_token, muts).await?;
//!         db.lease_release(p, s, "keeper:cleanup", lease.fencing_token).await?;
//!     }
//! }
//! ```
//!
//! State is stored in KV under [`LEASE_KEY_PREFIX`] within the caller's
//! project/scope. Application KV keys must not begin with this prefix.

use serde::{Deserialize, Serialize};

use crate::commit::tx::{ReadAssertion, ReadSet, TransactionEnvelope, WriteClass, WriteIntent};
use crate::commit::validation::Mutation;
use crate::error::AedbError;
use crate::permission::{CallerContext, Permission};
use crate::query::plan::ConsistencyMode;
use crate::{AedbInstance, CommitResult, FencedCommit, LeaseOutcome, MonitorLease, RenewOutcome};

/// Reserved KV key prefix for generalized leases. Application keys must not
/// start with these bytes.
pub const LEASE_KEY_PREFIX: &[u8] = b"\x00aedb:lease:";

/// Full inspection view of a generalized lease (see this module's docs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseRecord {
    pub project_id: String,
    pub scope_id: String,
    /// The lease name (e.g. `keeper:cleanup`, `task:123`, `monitor:github`).
    pub name: String,
    /// Current owner, or `None` if free/released.
    pub owner_id: Option<String>,
    /// Monotonic token; strictly increases on each ownership change.
    pub fencing_token: u64,
    /// Wall-clock expiry; `<= now` means the lease is up for grabs.
    pub lease_until_micros: u64,
    /// When the current owner first acquired (most recent acquisition).
    pub acquired_at_micros: u64,
    /// Opaque caller metadata (heartbeat progress, host id, etc.).
    pub meta_json: String,
}

const MAX_CAS_RETRIES: usize = 16;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct StoredLease {
    #[serde(default)]
    owner_id: Option<String>,
    #[serde(default)]
    fencing_token: u64,
    #[serde(default)]
    lease_until_micros: u64,
    #[serde(default)]
    acquired_at_micros: u64,
    /// Opaque caller metadata (heartbeat progress, host id, etc.).
    #[serde(default)]
    meta_json: String,
}

fn lease_key(name: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(LEASE_KEY_PREFIX.len() + name.len());
    k.extend_from_slice(LEASE_KEY_PREFIX);
    k.extend_from_slice(name.as_bytes());
    k
}

fn is_retryable_conflict(err: &AedbError) -> bool {
    matches!(
        err,
        AedbError::Conflict(_) | AedbError::AssertionFailed { .. }
    )
}

/// Build the CAS assertion appropriate for the current key state.
fn cas_assertion(
    project_id: &str,
    scope_id: &str,
    key: &[u8],
    entry: Option<&crate::storage::keyspace::KvEntry>,
) -> ReadAssertion {
    match entry {
        Some(entry) => ReadAssertion::KeyVersion {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key: key.to_vec(),
            expected_seq: entry.version,
        },
        None => ReadAssertion::KeyExists {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key: key.to_vec(),
            expected: false,
        },
    }
}

fn decode(entry: Option<&crate::storage::keyspace::KvEntry>) -> StoredLease {
    entry
        .and_then(|e| serde_json::from_slice::<StoredLease>(&e.value).ok())
        .unwrap_or_default()
}

impl AedbInstance {
    /// Acquire or take over the lease named `name` for `ttl_micros`. Succeeds if
    /// the lease is free, expired, or already held by `owner_id`; otherwise
    /// returns [`LeaseOutcome::Held`]. Every (re)acquisition bumps the fencing
    /// token, fencing out any prior holder.
    pub async fn lease_acquire(
        &self,
        project_id: &str,
        scope_id: &str,
        name: &str,
        owner_id: &str,
        ttl_micros: u64,
    ) -> Result<LeaseOutcome, AedbError> {
        self.lease_acquire_inner(None, project_id, scope_id, name, owner_id, ttl_micros, None)
            .await
    }

    /// [`AedbInstance::lease_acquire`], optionally attaching opaque `meta_json`,
    /// on behalf of `caller` (KvWrite required in secure mode).
    #[allow(clippy::too_many_arguments)]
    pub async fn lease_acquire_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        name: &str,
        owner_id: &str,
        ttl_micros: u64,
        meta_json: Option<String>,
    ) -> Result<LeaseOutcome, AedbError> {
        self.lease_acquire_inner(
            Some(caller),
            project_id,
            scope_id,
            name,
            owner_id,
            ttl_micros,
            meta_json,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn lease_acquire_inner(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        name: &str,
        owner_id: &str,
        ttl_micros: u64,
        meta_json: Option<String>,
    ) -> Result<LeaseOutcome, AedbError> {
        if name.is_empty() || owner_id.is_empty() {
            return Err(AedbError::Validation(
                "lease name and owner_id cannot be empty".into(),
            ));
        }
        if let Some(meta) = &meta_json {
            ensure_valid_json(meta)?;
        }
        let key = lease_key(name);
        for _ in 0..MAX_CAS_RETRIES {
            let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
            let base_seq = lease.view.seq;
            let entry = lease.view.keyspace.kv_get(project_id, scope_id, &key);
            let mut stored = decode(entry.as_ref());
            let now = crate::system_now_micros();

            let held_by_other = stored.lease_until_micros > now
                && stored.owner_id.as_deref().is_some_and(|o| o != owner_id);
            if held_by_other {
                return Ok(LeaseOutcome::Held {
                    owner_id: stored.owner_id.clone().unwrap_or_default(),
                    lease_until_micros: stored.lease_until_micros,
                });
            }

            stored.fencing_token = stored.fencing_token.saturating_add(1);
            stored.owner_id = Some(owner_id.to_string());
            stored.lease_until_micros = now.saturating_add(ttl_micros);
            stored.acquired_at_micros = now;
            if let Some(meta) = &meta_json {
                stored.meta_json = meta.clone();
            }

            let outcome = LeaseOutcome::Acquired(MonitorLease {
                owner_id: owner_id.to_string(),
                fencing_token: stored.fencing_token,
                lease_until_micros: stored.lease_until_micros,
            });
            match self
                .write_lease(
                    caller.clone(),
                    project_id,
                    scope_id,
                    &key,
                    &stored,
                    cas_assertion(project_id, scope_id, &key, entry.as_ref()),
                    base_seq,
                    Vec::new(),
                )
                .await
            {
                Ok(_) => return Ok(outcome),
                Err(err) if is_retryable_conflict(&err) => continue,
                Err(err) => return Err(err),
            }
        }
        Err(AedbError::Conflict(
            "lease_acquire exhausted retries".into(),
        ))
    }

    /// Renew (heartbeat) a lease the caller still holds, extending it by
    /// `ttl_micros` and optionally updating `meta_json`. Returns
    /// [`RenewOutcome::LeaseLost`] if a newer owner has taken over.
    pub async fn lease_renew(
        &self,
        project_id: &str,
        scope_id: &str,
        name: &str,
        fencing_token: u64,
        ttl_micros: u64,
    ) -> Result<RenewOutcome, AedbError> {
        self.lease_renew_inner(
            None,
            project_id,
            scope_id,
            name,
            fencing_token,
            ttl_micros,
            None,
        )
        .await
    }

    /// [`AedbInstance::lease_renew`] with optional `meta_json` update, on behalf
    /// of `caller`.
    #[allow(clippy::too_many_arguments)]
    pub async fn lease_renew_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        name: &str,
        fencing_token: u64,
        ttl_micros: u64,
        meta_json: Option<String>,
    ) -> Result<RenewOutcome, AedbError> {
        self.lease_renew_inner(
            Some(caller),
            project_id,
            scope_id,
            name,
            fencing_token,
            ttl_micros,
            meta_json,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn lease_renew_inner(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        name: &str,
        fencing_token: u64,
        ttl_micros: u64,
        meta_json: Option<String>,
    ) -> Result<RenewOutcome, AedbError> {
        if let Some(meta) = &meta_json {
            ensure_valid_json(meta)?;
        }
        let key = lease_key(name);
        for _ in 0..MAX_CAS_RETRIES {
            let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
            let base_seq = lease.view.seq;
            let entry = lease.view.keyspace.kv_get(project_id, scope_id, &key);
            let mut stored = decode(entry.as_ref());
            if stored.fencing_token != fencing_token {
                return Ok(RenewOutcome::LeaseLost);
            }
            let now = crate::system_now_micros();
            stored.lease_until_micros = now.saturating_add(ttl_micros);
            if let Some(meta) = &meta_json {
                stored.meta_json = meta.clone();
            }
            let renewed = RenewOutcome::Renewed(MonitorLease {
                owner_id: stored.owner_id.clone().unwrap_or_default(),
                fencing_token,
                lease_until_micros: stored.lease_until_micros,
            });
            match self
                .write_lease(
                    caller.clone(),
                    project_id,
                    scope_id,
                    &key,
                    &stored,
                    cas_assertion(project_id, scope_id, &key, entry.as_ref()),
                    base_seq,
                    Vec::new(),
                )
                .await
            {
                Ok(_) => return Ok(renewed),
                Err(err) if is_retryable_conflict(&err) => continue,
                Err(err) => return Err(err),
            }
        }
        Err(AedbError::Conflict("lease_renew exhausted retries".into()))
    }

    /// Release a lease the caller holds. Idempotent: if the token is already
    /// stale (someone else owns it), this is a no-op.
    pub async fn lease_release(
        &self,
        project_id: &str,
        scope_id: &str,
        name: &str,
        fencing_token: u64,
    ) -> Result<(), AedbError> {
        self.lease_release_inner(None, project_id, scope_id, name, fencing_token)
            .await
    }

    /// [`AedbInstance::lease_release`] on behalf of `caller`.
    pub async fn lease_release_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        name: &str,
        fencing_token: u64,
    ) -> Result<(), AedbError> {
        self.lease_release_inner(Some(caller), project_id, scope_id, name, fencing_token)
            .await
    }

    async fn lease_release_inner(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        name: &str,
        fencing_token: u64,
    ) -> Result<(), AedbError> {
        let key = lease_key(name);
        for _ in 0..MAX_CAS_RETRIES {
            let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
            let base_seq = lease.view.seq;
            let entry = lease.view.keyspace.kv_get(project_id, scope_id, &key);
            let mut stored = decode(entry.as_ref());
            if stored.fencing_token != fencing_token {
                return Ok(()); // already taken over; nothing to release.
            }
            stored.owner_id = None;
            stored.lease_until_micros = 0;
            match self
                .write_lease(
                    caller.clone(),
                    project_id,
                    scope_id,
                    &key,
                    &stored,
                    cas_assertion(project_id, scope_id, &key, entry.as_ref()),
                    base_seq,
                    Vec::new(),
                )
                .await
            {
                Ok(_) => return Ok(()),
                Err(err) if is_retryable_conflict(&err) => continue,
                Err(err) => return Err(err),
            }
        }
        Err(AedbError::Conflict(
            "lease_release exhausted retries".into(),
        ))
    }

    /// Apply `mutations` atomically, but only while the caller still provably
    /// holds the lease identified by `fencing_token` and it has not expired. A
    /// fenced or expired caller gets [`FencedCommit::LeaseLost`] and applies
    /// nothing. The lease record itself is left unchanged (use
    /// [`AedbInstance::lease_renew`] to extend it), but the commit is asserted
    /// against the lease key's version so a concurrent re-acquire fails the
    /// guarded write rather than racing it.
    pub async fn lease_guarded_commit(
        &self,
        project_id: &str,
        scope_id: &str,
        name: &str,
        fencing_token: u64,
        mutations: Vec<Mutation>,
    ) -> Result<FencedCommit, AedbError> {
        self.lease_guarded_commit_inner(None, project_id, scope_id, name, fencing_token, mutations)
            .await
    }

    /// [`AedbInstance::lease_guarded_commit`] on behalf of `caller` (must hold
    /// the permissions for `mutations`).
    #[allow(clippy::too_many_arguments)]
    pub async fn lease_guarded_commit_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        name: &str,
        fencing_token: u64,
        mutations: Vec<Mutation>,
    ) -> Result<FencedCommit, AedbError> {
        self.lease_guarded_commit_inner(
            Some(caller),
            project_id,
            scope_id,
            name,
            fencing_token,
            mutations,
        )
        .await
    }

    async fn lease_guarded_commit_inner(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        name: &str,
        fencing_token: u64,
        mutations: Vec<Mutation>,
    ) -> Result<FencedCommit, AedbError> {
        let key = lease_key(name);
        for _ in 0..MAX_CAS_RETRIES {
            let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
            let base_seq = lease.view.seq;
            let entry = lease.view.keyspace.kv_get(project_id, scope_id, &key);
            let stored = decode(entry.as_ref());
            let now = crate::system_now_micros();
            if stored.fencing_token != fencing_token || stored.lease_until_micros <= now {
                return Ok(FencedCommit::LeaseLost);
            }
            let assertion = cas_assertion(project_id, scope_id, &key, entry.as_ref());
            let envelope = TransactionEnvelope {
                caller: caller.clone(),
                idempotency_key: None,
                write_class: WriteClass::Standard,
                assertions: vec![assertion],
                read_set: ReadSet::default(),
                write_intent: WriteIntent {
                    mutations: mutations.clone(),
                },
                base_seq,
            };
            match self.commit_envelope(envelope).await {
                Ok(commit) => return Ok(FencedCommit::Applied(commit)),
                Err(err) if is_retryable_conflict(&err) => continue,
                Err(err) => return Err(err),
            }
        }
        Err(AedbError::Conflict(
            "lease_guarded_commit exhausted retries".into(),
        ))
    }

    /// Inspect a lease's full state. `None` if the lease has never been touched.
    pub async fn lease_status(
        &self,
        project_id: &str,
        scope_id: &str,
        name: &str,
        consistency: ConsistencyMode,
    ) -> Result<Option<LeaseRecord>, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        self.lease_status_inner(None, project_id, scope_id, name, consistency)
            .await
    }

    /// [`AedbInstance::lease_status`] on behalf of `caller` (KvRead required).
    pub async fn lease_status_as(
        &self,
        caller: &CallerContext,
        project_id: &str,
        scope_id: &str,
        name: &str,
        consistency: ConsistencyMode,
    ) -> Result<Option<LeaseRecord>, AedbError> {
        self.lease_status_inner(Some(caller), project_id, scope_id, name, consistency)
            .await
    }

    async fn lease_status_inner(
        &self,
        caller: Option<&CallerContext>,
        project_id: &str,
        scope_id: &str,
        name: &str,
        consistency: ConsistencyMode,
    ) -> Result<Option<LeaseRecord>, AedbError> {
        let lease = self.acquire_snapshot(consistency).await?;
        if let Some(caller) = caller {
            let required = Permission::KvRead {
                project_id: project_id.to_string(),
                scope_id: Some(scope_id.to_string()),
                prefix: Some(LEASE_KEY_PREFIX.to_vec()),
            };
            if !lease
                .view
                .catalog
                .has_permission(&caller.caller_id, &required)
            {
                return Err(AedbError::PermissionDenied("permission denied".into()));
            }
        }
        let Some(entry) = lease
            .view
            .keyspace
            .kv_get(project_id, scope_id, &lease_key(name))
        else {
            return Ok(None);
        };
        let stored = serde_json::from_slice::<StoredLease>(&entry.value)
            .map_err(|_| AedbError::Decode("lease record is corrupt".into()))?;
        Ok(Some(LeaseRecord {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            name: name.to_string(),
            owner_id: stored.owner_id,
            fencing_token: stored.fencing_token,
            lease_until_micros: stored.lease_until_micros,
            acquired_at_micros: stored.acquired_at_micros,
            meta_json: stored.meta_json,
        }))
    }

    /// Commit a lease record (plus optional extra mutations) under a CAS
    /// assertion.
    #[allow(clippy::too_many_arguments)]
    async fn write_lease(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        key: &[u8],
        stored: &StoredLease,
        assertion: ReadAssertion,
        base_seq: u64,
        extra: Vec<Mutation>,
    ) -> Result<CommitResult, AedbError> {
        let value = serde_json::to_vec(stored)
            .map_err(|e| AedbError::Encode(format!("lease encode failed: {e}")))?;
        let mut mutations = extra;
        mutations.push(Mutation::KvSet {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key: key.to_vec(),
            value,
        });
        self.commit_envelope(TransactionEnvelope {
            caller,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: vec![assertion],
            read_set: ReadSet::default(),
            write_intent: WriteIntent { mutations },
            base_seq,
        })
        .await
    }
}

fn ensure_valid_json(s: &str) -> Result<(), AedbError> {
    serde_json::from_str::<serde_json::Value>(s)
        .map(|_| ())
        .map_err(|_| AedbError::Validation("meta_json must be valid JSON".into()))
}

#[cfg(test)]
mod tests;
