//! Monitor checkpoints with a single-owner lease, fencing token, and cursor.
//!
//! Chain monitors (ERC20 deposits, Nado updates) need durable progress state —
//! start block, last scanned block, last processed cursor, last update time,
//! retry/error state — plus an exclusive claim so two instances don't scan and
//! credit the same range. Reactive processors already provide a checkpoint
//! watermark and lag; this primitive adds the missing legs: a **lease** with a
//! monotonic **fencing token** that fences out a zombie owner, and a typed
//! checkpoint that a UI can inspect.
//!
//! Usage:
//! ```ignore
//! match db.monitor_acquire_lease(p, s, "erc20", "worker-1", 30_000_000).await? {
//!     LeaseOutcome::Held { .. } => return, // another worker owns it
//!     LeaseOutcome::Acquired(lease) => {
//!         // ... scan a block range ...
//!         match db.monitor_advance_checkpoint(p, s, "erc20", lease.fencing_token,
//!                 update, credit_mutations).await? {
//!             FencedCommit::Applied(_) => {}
//!             FencedCommit::LeaseLost => return, // we were fenced; stop
//!         }
//!     }
//! }
//! ```
//!
//! State is stored in KV under [`MONITOR_KEY_PREFIX`] within the caller's
//! project/scope (durable across restart/checkpoint/restore). Application KV
//! keys must not begin with this prefix.

use serde::{Deserialize, Serialize};

use crate::commit::tx::{ReadAssertion, ReadSet, TransactionEnvelope, WriteClass, WriteIntent};
use crate::commit::validation::Mutation;
use crate::error::AedbError;
use crate::permission::{CallerContext, Permission};
use crate::query::plan::ConsistencyMode;
use crate::{
    AedbInstance, FencedCommit, LeaseOutcome, MonitorCheckpointUpdate, MonitorLease, MonitorStatus,
    RenewOutcome,
};

/// Reserved KV key prefix for monitor state. Application keys must not start
/// with these bytes.
pub const MONITOR_KEY_PREFIX: &[u8] = b"\x00aedb:monitor:";

const MAX_CAS_RETRIES: usize = 16;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct StoredMonitor {
    #[serde(default)]
    start_block: Option<u64>,
    #[serde(default)]
    last_scanned_block: Option<u64>,
    #[serde(default)]
    last_processed_cursor: Option<String>,
    #[serde(default)]
    last_update_micros: u64,
    #[serde(default)]
    retry_count: u32,
    #[serde(default)]
    last_error: Option<String>,
    #[serde(default)]
    owner_id: Option<String>,
    #[serde(default)]
    fencing_token: u64,
    #[serde(default)]
    lease_until_micros: u64,
}

fn monitor_key(monitor: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(MONITOR_KEY_PREFIX.len() + monitor.len());
    k.extend_from_slice(MONITOR_KEY_PREFIX);
    k.extend_from_slice(monitor.as_bytes());
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

impl AedbInstance {
    /// Acquire or take over a monitor lease for `ttl_micros`. Succeeds if the
    /// lease is free, expired, or already held by `owner_id`; otherwise returns
    /// [`LeaseOutcome::Held`].
    pub async fn monitor_acquire_lease(
        &self,
        project_id: &str,
        scope_id: &str,
        monitor: &str,
        owner_id: &str,
        ttl_micros: u64,
    ) -> Result<LeaseOutcome, AedbError> {
        self.monitor_acquire_lease_inner(None, project_id, scope_id, monitor, owner_id, ttl_micros)
            .await
    }

    /// [`AedbInstance::monitor_acquire_lease`] on behalf of `caller`.
    pub async fn monitor_acquire_lease_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        monitor: &str,
        owner_id: &str,
        ttl_micros: u64,
    ) -> Result<LeaseOutcome, AedbError> {
        self.monitor_acquire_lease_inner(
            Some(caller),
            project_id,
            scope_id,
            monitor,
            owner_id,
            ttl_micros,
        )
        .await
    }

    async fn monitor_acquire_lease_inner(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        monitor: &str,
        owner_id: &str,
        ttl_micros: u64,
    ) -> Result<LeaseOutcome, AedbError> {
        if monitor.is_empty() || owner_id.is_empty() {
            return Err(AedbError::Validation(
                "monitor and owner_id cannot be empty".into(),
            ));
        }
        let key = monitor_key(monitor);
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

            // Bump the fencing token on every (re)acquisition so any prior
            // holder is fenced out, then take ownership.
            stored.fencing_token = stored.fencing_token.saturating_add(1);
            stored.owner_id = Some(owner_id.to_string());
            stored.lease_until_micros = now.saturating_add(ttl_micros);

            let outcome = LeaseOutcome::Acquired(MonitorLease {
                owner_id: owner_id.to_string(),
                fencing_token: stored.fencing_token,
                lease_until_micros: stored.lease_until_micros,
            });
            match self
                .write_monitor(
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
            "monitor_acquire_lease exhausted retries".into(),
        ))
    }

    /// Renew a lease the caller still holds (identified by `fencing_token`),
    /// extending it by `ttl_micros`. Returns [`RenewOutcome::LeaseLost`] if a
    /// newer owner has taken over.
    pub async fn monitor_renew_lease(
        &self,
        project_id: &str,
        scope_id: &str,
        monitor: &str,
        fencing_token: u64,
        ttl_micros: u64,
    ) -> Result<RenewOutcome, AedbError> {
        self.monitor_renew_lease_inner(
            None,
            project_id,
            scope_id,
            monitor,
            fencing_token,
            ttl_micros,
        )
        .await
    }

    /// [`AedbInstance::monitor_renew_lease`] on behalf of `caller`.
    pub async fn monitor_renew_lease_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        monitor: &str,
        fencing_token: u64,
        ttl_micros: u64,
    ) -> Result<RenewOutcome, AedbError> {
        self.monitor_renew_lease_inner(
            Some(caller),
            project_id,
            scope_id,
            monitor,
            fencing_token,
            ttl_micros,
        )
        .await
    }

    async fn monitor_renew_lease_inner(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        monitor: &str,
        fencing_token: u64,
        ttl_micros: u64,
    ) -> Result<RenewOutcome, AedbError> {
        let key = monitor_key(monitor);
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
            let renewed = RenewOutcome::Renewed(MonitorLease {
                owner_id: stored.owner_id.clone().unwrap_or_default(),
                fencing_token,
                lease_until_micros: stored.lease_until_micros,
            });
            match self
                .write_monitor(
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
        Err(AedbError::Conflict(
            "monitor_renew_lease exhausted retries".into(),
        ))
    }

    /// Release a lease the caller holds. Idempotent: if the token is already
    /// stale (someone else owns it), this is a no-op.
    pub async fn monitor_release_lease(
        &self,
        project_id: &str,
        scope_id: &str,
        monitor: &str,
        fencing_token: u64,
    ) -> Result<(), AedbError> {
        self.monitor_release_lease_inner(None, project_id, scope_id, monitor, fencing_token)
            .await
    }

    /// [`AedbInstance::monitor_release_lease`] on behalf of `caller`.
    pub async fn monitor_release_lease_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        monitor: &str,
        fencing_token: u64,
    ) -> Result<(), AedbError> {
        self.monitor_release_lease_inner(Some(caller), project_id, scope_id, monitor, fencing_token)
            .await
    }

    async fn monitor_release_lease_inner(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        monitor: &str,
        fencing_token: u64,
    ) -> Result<(), AedbError> {
        let key = monitor_key(monitor);
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
                .write_monitor(
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
            "monitor_release_lease exhausted retries".into(),
        ))
    }

    /// Advance the monitor checkpoint and apply `mutations` (credits, state
    /// changes) atomically — but only while the caller still holds the lease
    /// identified by `fencing_token` and it has not expired. A fenced or
    /// expired caller gets [`FencedCommit::LeaseLost`] and applies nothing.
    pub async fn monitor_advance_checkpoint(
        &self,
        project_id: &str,
        scope_id: &str,
        monitor: &str,
        fencing_token: u64,
        update: MonitorCheckpointUpdate,
        mutations: Vec<Mutation>,
    ) -> Result<FencedCommit, AedbError> {
        self.monitor_advance_inner(
            None,
            project_id,
            scope_id,
            monitor,
            fencing_token,
            update,
            mutations,
        )
        .await
    }

    /// [`AedbInstance::monitor_advance_checkpoint`] on behalf of `caller`.
    #[allow(clippy::too_many_arguments)]
    pub async fn monitor_advance_checkpoint_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        monitor: &str,
        fencing_token: u64,
        update: MonitorCheckpointUpdate,
        mutations: Vec<Mutation>,
    ) -> Result<FencedCommit, AedbError> {
        self.monitor_advance_inner(
            Some(caller),
            project_id,
            scope_id,
            monitor,
            fencing_token,
            update,
            mutations,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn monitor_advance_inner(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        monitor: &str,
        fencing_token: u64,
        update: MonitorCheckpointUpdate,
        mutations: Vec<Mutation>,
    ) -> Result<FencedCommit, AedbError> {
        let key = monitor_key(monitor);
        for _ in 0..MAX_CAS_RETRIES {
            let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
            let base_seq = lease.view.seq;
            let entry = lease.view.keyspace.kv_get(project_id, scope_id, &key);
            let mut stored = decode(entry.as_ref());
            let now = crate::system_now_micros();
            // Fence: token must be current and the lease still live.
            if stored.fencing_token != fencing_token || stored.lease_until_micros <= now {
                return Ok(FencedCommit::LeaseLost);
            }
            apply_update(&mut stored, &update, now);

            match self
                .write_monitor(
                    caller.clone(),
                    project_id,
                    scope_id,
                    &key,
                    &stored,
                    cas_assertion(project_id, scope_id, &key, entry.as_ref()),
                    base_seq,
                    mutations.clone(),
                )
                .await
            {
                Ok(commit) => return Ok(FencedCommit::Applied(commit)),
                Err(err) if is_retryable_conflict(&err) => continue,
                Err(err) => return Err(err),
            }
        }
        Err(AedbError::Conflict(
            "monitor_advance_checkpoint exhausted retries".into(),
        ))
    }

    /// Inspect a monitor's full state (checkpoint + retry + lease). `None` if
    /// the monitor has never been touched.
    pub async fn monitor_status(
        &self,
        project_id: &str,
        scope_id: &str,
        monitor: &str,
        consistency: ConsistencyMode,
    ) -> Result<Option<MonitorStatus>, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        self.monitor_status_inner(None, project_id, scope_id, monitor, consistency)
            .await
    }

    /// [`AedbInstance::monitor_status`] on behalf of `caller` (KvRead required).
    pub async fn monitor_status_as(
        &self,
        caller: &CallerContext,
        project_id: &str,
        scope_id: &str,
        monitor: &str,
        consistency: ConsistencyMode,
    ) -> Result<Option<MonitorStatus>, AedbError> {
        self.monitor_status_inner(Some(caller), project_id, scope_id, monitor, consistency)
            .await
    }

    async fn monitor_status_inner(
        &self,
        caller: Option<&CallerContext>,
        project_id: &str,
        scope_id: &str,
        monitor: &str,
        consistency: ConsistencyMode,
    ) -> Result<Option<MonitorStatus>, AedbError> {
        let lease = self.acquire_snapshot(consistency).await?;
        if let Some(caller) = caller {
            let required = Permission::KvRead {
                project_id: project_id.to_string(),
                scope_id: Some(scope_id.to_string()),
                prefix: Some(MONITOR_KEY_PREFIX.to_vec()),
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
            .kv_get(project_id, scope_id, &monitor_key(monitor))
        else {
            return Ok(None);
        };
        let stored = serde_json::from_slice::<StoredMonitor>(&entry.value)
            .map_err(|_| AedbError::Decode("monitor record is corrupt".into()))?;
        Ok(Some(MonitorStatus {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            monitor: monitor.to_string(),
            start_block: stored.start_block,
            last_scanned_block: stored.last_scanned_block,
            last_processed_cursor: stored.last_processed_cursor,
            last_update_micros: stored.last_update_micros,
            retry_count: stored.retry_count,
            last_error: stored.last_error.filter(|e| !e.is_empty()),
            owner_id: stored.owner_id,
            fencing_token: stored.fencing_token,
            lease_until_micros: stored.lease_until_micros,
        }))
    }

    /// Commit a monitor record (plus optional extra mutations) under a CAS
    /// assertion.
    #[allow(clippy::too_many_arguments)]
    async fn write_monitor(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        key: &[u8],
        stored: &StoredMonitor,
        assertion: ReadAssertion,
        base_seq: u64,
        extra: Vec<Mutation>,
    ) -> Result<CommitResultAlias, AedbError> {
        let value = serde_json::to_vec(stored)
            .map_err(|e| AedbError::Encode(format!("monitor encode failed: {e}")))?;
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

type CommitResultAlias = crate::CommitResult;

fn decode(entry: Option<&crate::storage::keyspace::KvEntry>) -> StoredMonitor {
    entry
        .and_then(|e| serde_json::from_slice::<StoredMonitor>(&e.value).ok())
        .unwrap_or_default()
}

fn apply_update(stored: &mut StoredMonitor, update: &MonitorCheckpointUpdate, now: u64) {
    if let Some(v) = update.start_block {
        stored.start_block = Some(v);
    }
    if let Some(v) = update.last_scanned_block {
        stored.last_scanned_block = Some(v);
    }
    if let Some(v) = &update.last_processed_cursor {
        stored.last_processed_cursor = Some(v.clone());
    }
    if let Some(v) = update.retry_count {
        stored.retry_count = v;
    }
    if let Some(v) = &update.last_error {
        stored.last_error = if v.is_empty() { None } else { Some(v.clone()) };
    }
    stored.last_update_micros = now;
}

#[cfg(test)]
mod tests;
