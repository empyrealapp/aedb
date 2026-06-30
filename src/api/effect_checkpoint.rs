//! Exactly-once external-effect checkpoints.
//!
//! The commit envelope already deduplicates *inbound* requests (see
//! `idempotency_key`). This module adds the missing half: a durable, named
//! checkpoint for *outbound* effects so a monitor or task can replay after a
//! crash without performing a side effect twice (a duplicate credit, or a
//! duplicate external submission to Nado / an order book / a chain).
//!
//! The pattern is a two-phase dedupe key:
//!
//! 1. [`AedbInstance::begin_effect`] atomically claims the key. `Fresh` means
//!    the caller now owns it and should perform the effect; `AlreadyCommitted`
//!    means a prior attempt already finished it (skip); `InProgress` means a
//!    prior attempt claimed but never completed (reconcile before resubmitting
//!    a non-idempotent external call).
//! 2. [`AedbInstance::complete_effect`] applies the caller's internal mutations
//!    (the credit, the state transition) **and** marks the key committed in a
//!    single atomic commit. If two workers race, exactly one applies the
//!    mutations; the loser observes `AlreadyCommitted` and applies nothing.
//!
//! Checkpoints are stored in KV under an engine-reserved key prefix
//! ([`EFFECT_CHECKPOINT_KEY_PREFIX`]) within the caller's own project/scope, so
//! they persist through restart, checkpoint, and restore with no new on-disk
//! format. Application KV keys must not begin with this prefix.

use serde::{Deserialize, Serialize};

use crate::commit::tx::{ReadAssertion, ReadSet, TransactionEnvelope, WriteClass, WriteIntent};
use crate::commit::validation::Mutation;
use crate::error::AedbError;
use crate::permission::{CallerContext, Permission};
use crate::query::plan::ConsistencyMode;
use crate::{AedbInstance, CompleteEffect, EffectCheckpointRecord, EffectClaim, EffectStatus};

/// Reserved KV key prefix for effect checkpoints. Application keys must not
/// start with these bytes.
pub const EFFECT_CHECKPOINT_KEY_PREFIX: &[u8] = b"\x00aedb:effect:";

/// How many times a claim/complete will re-read and retry on a write conflict
/// before surfacing the conflict to the caller.
const MAX_CAS_RETRIES: usize = 8;

const STATUS_PENDING: &str = "pending";
const STATUS_COMMITTED: &str = "committed";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredEffectCheckpoint {
    status: String,
    attempts: u64,
    created_at_micros: u64,
    updated_at_micros: u64,
    #[serde(default)]
    result_json: String,
}

fn checkpoint_key(dedupe_key: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(EFFECT_CHECKPOINT_KEY_PREFIX.len() + dedupe_key.len());
    k.extend_from_slice(EFFECT_CHECKPOINT_KEY_PREFIX);
    k.extend_from_slice(dedupe_key.as_bytes());
    k
}

fn is_retryable_conflict(err: &AedbError) -> bool {
    matches!(
        err,
        AedbError::Conflict(_) | AedbError::AssertionFailed { .. }
    )
}

impl AedbInstance {
    /// Claim a dedupe key before performing an external effect. See module docs.
    pub async fn begin_effect(
        &self,
        project_id: &str,
        scope_id: &str,
        dedupe_key: &str,
    ) -> Result<EffectClaim, AedbError> {
        self.begin_effect_inner(None, project_id, scope_id, dedupe_key)
            .await
    }

    /// [`AedbInstance::begin_effect`] on behalf of `caller` (required in secure
    /// mode); the caller must hold `KvWrite` on the project/scope.
    pub async fn begin_effect_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        dedupe_key: &str,
    ) -> Result<EffectClaim, AedbError> {
        self.begin_effect_inner(Some(caller), project_id, scope_id, dedupe_key)
            .await
    }

    async fn begin_effect_inner(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        dedupe_key: &str,
    ) -> Result<EffectClaim, AedbError> {
        if dedupe_key.is_empty() {
            return Err(AedbError::Validation("dedupe_key cannot be empty".into()));
        }
        let key = checkpoint_key(dedupe_key);

        for _ in 0..MAX_CAS_RETRIES {
            let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
            let base_seq = lease.view.seq;
            let existing = lease.view.keyspace.try_kv_get(project_id, scope_id, &key)?;

            let (assertion, next, claim) = match existing.as_ref().map(decode_stored) {
                Some(Ok(stored)) if stored.status == STATUS_COMMITTED => {
                    return Ok(EffectClaim::AlreadyCommitted {
                        result_json: stored.result_json,
                    });
                }
                Some(Ok(stored)) => {
                    // Pending: bump attempts under a version assertion.
                    let version = existing.as_ref().map(|e| e.version).unwrap_or(0);
                    let attempts = stored.attempts.saturating_add(1);
                    let next = StoredEffectCheckpoint {
                        status: STATUS_PENDING.into(),
                        attempts,
                        created_at_micros: stored.created_at_micros,
                        updated_at_micros: crate::system_now_micros(),
                        result_json: String::new(),
                    };
                    (
                        ReadAssertion::KeyVersion {
                            project_id: project_id.to_string(),
                            scope_id: scope_id.to_string(),
                            key: key.clone(),
                            expected_seq: version,
                        },
                        next,
                        EffectClaim::InProgress { attempts },
                    )
                }
                _ => {
                    // Absent (or unreadable legacy bytes): claim fresh.
                    let now = crate::system_now_micros();
                    let next = StoredEffectCheckpoint {
                        status: STATUS_PENDING.into(),
                        attempts: 1,
                        created_at_micros: now,
                        updated_at_micros: now,
                        result_json: String::new(),
                    };
                    (
                        ReadAssertion::KeyExists {
                            project_id: project_id.to_string(),
                            scope_id: scope_id.to_string(),
                            key: key.clone(),
                            expected: false,
                        },
                        next,
                        EffectClaim::Fresh,
                    )
                }
            };

            let envelope = TransactionEnvelope {
                caller: caller.clone(),
                idempotency_key: None,
                write_class: WriteClass::Standard,
                assertions: vec![assertion],
                read_set: ReadSet::default(),
                write_intent: WriteIntent {
                    mutations: vec![Mutation::KvSet {
                        project_id: project_id.to_string(),
                        scope_id: scope_id.to_string(),
                        key: key.clone(),
                        value: encode_stored(&next)?,
                    }],
                },
                base_seq,
            };
            match self.commit_envelope(envelope).await {
                Ok(_) => return Ok(claim),
                Err(err) if is_retryable_conflict(&err) => continue,
                Err(err) => return Err(err),
            }
        }
        Err(AedbError::Conflict(
            "begin_effect exhausted retries under contention".into(),
        ))
    }

    /// Complete an effect: apply `mutations` (the internal credit / state
    /// change) and mark the dedupe key committed in one atomic commit. Calling
    /// this again for the same key is a no-op that returns the recorded result.
    pub async fn complete_effect(
        &self,
        project_id: &str,
        scope_id: &str,
        dedupe_key: &str,
        result_json: String,
        mutations: Vec<Mutation>,
    ) -> Result<CompleteEffect, AedbError> {
        self.complete_effect_inner(
            None,
            project_id,
            scope_id,
            dedupe_key,
            result_json,
            mutations,
        )
        .await
    }

    /// [`AedbInstance::complete_effect`] on behalf of `caller` (required in
    /// secure mode); the caller must hold the permissions for `mutations` plus
    /// `KvWrite` on the project/scope.
    pub async fn complete_effect_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        dedupe_key: &str,
        result_json: String,
        mutations: Vec<Mutation>,
    ) -> Result<CompleteEffect, AedbError> {
        self.complete_effect_inner(
            Some(caller),
            project_id,
            scope_id,
            dedupe_key,
            result_json,
            mutations,
        )
        .await
    }

    async fn complete_effect_inner(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        dedupe_key: &str,
        result_json: String,
        mutations: Vec<Mutation>,
    ) -> Result<CompleteEffect, AedbError> {
        if dedupe_key.is_empty() {
            return Err(AedbError::Validation("dedupe_key cannot be empty".into()));
        }
        if !is_valid_json(&result_json) {
            return Err(AedbError::Validation(
                "complete_effect result_json must be valid JSON".into(),
            ));
        }
        let key = checkpoint_key(dedupe_key);

        for _ in 0..MAX_CAS_RETRIES {
            let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
            let base_seq = lease.view.seq;
            let existing = lease.view.keyspace.try_kv_get(project_id, scope_id, &key)?;

            let (created_at, attempts, assertion) = match existing.as_ref().map(decode_stored) {
                Some(Ok(stored)) if stored.status == STATUS_COMMITTED => {
                    // Already completed exactly once — do not re-apply.
                    return Ok(CompleteEffect::AlreadyCommitted {
                        result_json: stored.result_json,
                    });
                }
                Some(Ok(stored)) => {
                    let version = existing.as_ref().map(|e| e.version).unwrap_or(0);
                    (
                        stored.created_at_micros,
                        stored.attempts.max(1),
                        ReadAssertion::KeyVersion {
                            project_id: project_id.to_string(),
                            scope_id: scope_id.to_string(),
                            key: key.clone(),
                            expected_seq: version,
                        },
                    )
                }
                _ => (
                    crate::system_now_micros(),
                    1,
                    ReadAssertion::KeyExists {
                        project_id: project_id.to_string(),
                        scope_id: scope_id.to_string(),
                        key: key.clone(),
                        expected: false,
                    },
                ),
            };

            let marker = StoredEffectCheckpoint {
                status: STATUS_COMMITTED.into(),
                attempts,
                created_at_micros: created_at,
                updated_at_micros: crate::system_now_micros(),
                result_json: result_json.clone(),
            };
            let mut all = mutations.clone();
            all.push(Mutation::KvSet {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key: key.clone(),
                value: encode_stored(&marker)?,
            });

            let envelope = TransactionEnvelope {
                caller: caller.clone(),
                idempotency_key: None,
                write_class: WriteClass::Standard,
                assertions: vec![assertion],
                read_set: ReadSet::default(),
                write_intent: WriteIntent { mutations: all },
                base_seq,
            };
            match self.commit_envelope(envelope).await {
                Ok(commit) => return Ok(CompleteEffect::Applied(commit)),
                Err(err) if is_retryable_conflict(&err) => continue,
                Err(err) => return Err(err),
            }
        }
        Err(AedbError::Conflict(
            "complete_effect exhausted retries under contention".into(),
        ))
    }

    /// Inspect an effect checkpoint without mutating it. Returns `None` if the
    /// key was never claimed.
    pub async fn effect_status(
        &self,
        project_id: &str,
        scope_id: &str,
        dedupe_key: &str,
        consistency: ConsistencyMode,
    ) -> Result<Option<EffectCheckpointRecord>, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        self.effect_status_inner(None, project_id, scope_id, dedupe_key, consistency)
            .await
    }

    /// [`AedbInstance::effect_status`] on behalf of `caller`, enforcing
    /// `KvRead` on the project/scope.
    pub async fn effect_status_as(
        &self,
        caller: &CallerContext,
        project_id: &str,
        scope_id: &str,
        dedupe_key: &str,
        consistency: ConsistencyMode,
    ) -> Result<Option<EffectCheckpointRecord>, AedbError> {
        self.effect_status_inner(Some(caller), project_id, scope_id, dedupe_key, consistency)
            .await
    }

    async fn effect_status_inner(
        &self,
        caller: Option<&CallerContext>,
        project_id: &str,
        scope_id: &str,
        dedupe_key: &str,
        consistency: ConsistencyMode,
    ) -> Result<Option<EffectCheckpointRecord>, AedbError> {
        let lease = self.acquire_snapshot(consistency).await?;
        if let Some(caller) = caller {
            let required = Permission::KvRead {
                project_id: project_id.to_string(),
                scope_id: Some(scope_id.to_string()),
                prefix: Some(EFFECT_CHECKPOINT_KEY_PREFIX.to_vec()),
            };
            if !lease
                .view
                .catalog
                .has_permission(&caller.caller_id, &required)
            {
                return Err(AedbError::PermissionDenied("permission denied".into()));
            }
        }
        let key = checkpoint_key(dedupe_key);
        let Some(entry) = lease.view.keyspace.try_kv_get(project_id, scope_id, &key)? else {
            return Ok(None);
        };
        let stored = decode_stored(&entry)
            .map_err(|_| AedbError::Decode("effect checkpoint record is corrupt".into()))?;
        let status = if stored.status == STATUS_COMMITTED {
            EffectStatus::Committed
        } else {
            EffectStatus::Pending
        };
        Ok(Some(EffectCheckpointRecord {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            dedupe_key: dedupe_key.to_string(),
            status,
            attempts: stored.attempts,
            created_at_micros: stored.created_at_micros,
            updated_at_micros: stored.updated_at_micros,
            result_json: stored.result_json,
        }))
    }
}

fn encode_stored(stored: &StoredEffectCheckpoint) -> Result<Vec<u8>, AedbError> {
    serde_json::to_vec(stored)
        .map_err(|e| AedbError::Encode(format!("effect checkpoint encode failed: {e}")))
}

fn decode_stored(
    entry: &crate::storage::keyspace::KvEntry,
) -> Result<StoredEffectCheckpoint, serde_json::Error> {
    serde_json::from_slice(&entry.value)
}

fn is_valid_json(s: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(s).is_ok()
}

#[cfg(test)]
mod tests;
