//! Bounded materialized projections.
//!
//! Program (WASM) state should stay small and bounded; rich history belongs in
//! the event log. This module gives applications fast, queryable, **bounded**
//! derived state without pushing unbounded arrays into program state:
//!
//! * **Latest-per-key views** — a named collection of "latest value per entity"
//!   entries. Use for latest protocol status (single entity), per-account
//!   position summaries, or the current set of pending intents (insert on
//!   create, remove on resolve). Reads are point lookups or a bounded list.
//! * **Bounded series** — an append-only ring that keeps only the last `N`
//!   points. Use for a NAV series, a price series, or a rolling decision log.
//!
//! Both are stored in KV under engine-reserved key prefixes within the caller's
//! own project/scope, so they persist through restart/checkpoint/restore with
//! no new on-disk format. Application KV keys must not begin with
//! [`PROJECTION_VIEW_PREFIX`] or [`PROJECTION_SERIES_PREFIX`].

use serde::{Deserialize, Serialize};

use crate::commit::tx::{ReadAssertion, ReadSet, TransactionEnvelope, WriteClass, WriteIntent};
use crate::commit::validation::Mutation;
use crate::error::AedbError;
use crate::lib_helpers::next_prefix_bytes;
use crate::permission::{CallerContext, Permission};
use crate::query::plan::ConsistencyMode;
use crate::{AedbInstance, CommitResult, ProjectionEntry};

/// Reserved KV key prefix for latest-per-key projection views.
pub const PROJECTION_VIEW_PREFIX: &[u8] = b"\x00aedb:proj:";
/// Reserved KV key prefix for bounded series.
pub const PROJECTION_SERIES_PREFIX: &[u8] = b"\x00aedb:series:";

/// A bounded series is a single hot key: every append must win its CAS (unlike
/// a dedupe key, where late writers simply observe the committed value). Give
/// appends a larger budget and yield between attempts so in-flight commits land.
const SERIES_APPEND_RETRIES: usize = 64;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSeries {
    points: Vec<serde_json::Value>,
}

fn view_prefix(view: &str) -> Vec<u8> {
    let mut p = Vec::with_capacity(PROJECTION_VIEW_PREFIX.len() + view.len() + 1);
    p.extend_from_slice(PROJECTION_VIEW_PREFIX);
    p.extend_from_slice(view.as_bytes());
    p.push(0);
    p
}

fn view_key(view: &str, entity_key: &str) -> Vec<u8> {
    let mut k = view_prefix(view);
    k.extend_from_slice(entity_key.as_bytes());
    k
}

fn series_key(series: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(PROJECTION_SERIES_PREFIX.len() + series.len());
    k.extend_from_slice(PROJECTION_SERIES_PREFIX);
    k.extend_from_slice(series.as_bytes());
    k
}

fn validate_name(name: &str, what: &str) -> Result<(), AedbError> {
    if name.is_empty() {
        return Err(AedbError::Validation(format!("{what} cannot be empty")));
    }
    Ok(())
}

fn validate_json(value_json: &str) -> Result<serde_json::Value, AedbError> {
    serde_json::from_str::<serde_json::Value>(value_json)
        .map_err(|_| AedbError::Validation("projection value must be valid JSON".into()))
}

fn is_retryable_conflict(err: &AedbError) -> bool {
    matches!(
        err,
        AedbError::Conflict(_) | AedbError::AssertionFailed { .. }
    )
}

impl AedbInstance {
    // ---- latest-per-key views ------------------------------------------------

    /// Upsert the latest value for `entity_key` in a projection `view`.
    pub async fn projection_put(
        &self,
        project_id: &str,
        scope_id: &str,
        view: &str,
        entity_key: &str,
        value_json: &str,
    ) -> Result<CommitResult, AedbError> {
        self.projection_put_inner(None, project_id, scope_id, view, entity_key, value_json)
            .await
    }

    /// [`AedbInstance::projection_put`] on behalf of `caller` (KvWrite required).
    pub async fn projection_put_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        view: &str,
        entity_key: &str,
        value_json: &str,
    ) -> Result<CommitResult, AedbError> {
        self.projection_put_inner(
            Some(caller),
            project_id,
            scope_id,
            view,
            entity_key,
            value_json,
        )
        .await
    }

    async fn projection_put_inner(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        view: &str,
        entity_key: &str,
        value_json: &str,
    ) -> Result<CommitResult, AedbError> {
        validate_name(view, "view")?;
        validate_name(entity_key, "entity_key")?;
        validate_json(value_json)?;
        let mutation = Mutation::KvSet {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key: view_key(view, entity_key),
            value: value_json.as_bytes().to_vec(),
        };
        match caller {
            Some(caller) => self.commit_as(caller, mutation).await,
            None => self.commit(mutation).await,
        }
    }

    /// Remove an entity from a projection view (e.g. a pending intent resolved).
    pub async fn projection_remove(
        &self,
        project_id: &str,
        scope_id: &str,
        view: &str,
        entity_key: &str,
    ) -> Result<CommitResult, AedbError> {
        self.projection_remove_inner(None, project_id, scope_id, view, entity_key)
            .await
    }

    /// [`AedbInstance::projection_remove`] on behalf of `caller`.
    pub async fn projection_remove_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        view: &str,
        entity_key: &str,
    ) -> Result<CommitResult, AedbError> {
        self.projection_remove_inner(Some(caller), project_id, scope_id, view, entity_key)
            .await
    }

    async fn projection_remove_inner(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        view: &str,
        entity_key: &str,
    ) -> Result<CommitResult, AedbError> {
        validate_name(view, "view")?;
        validate_name(entity_key, "entity_key")?;
        let mutation = Mutation::KvDel {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key: view_key(view, entity_key),
        };
        match caller {
            Some(caller) => self.commit_as(caller, mutation).await,
            None => self.commit(mutation).await,
        }
    }

    /// Read the latest value for one entity in a view.
    pub async fn projection_get(
        &self,
        project_id: &str,
        scope_id: &str,
        view: &str,
        entity_key: &str,
        consistency: ConsistencyMode,
    ) -> Result<Option<String>, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        let lease = self.acquire_snapshot(consistency).await?;
        let entry = lease
            .view
            .keyspace
            .kv_get(project_id, scope_id, &view_key(view, entity_key));
        Ok(entry.map(|e| String::from_utf8_lossy(&e.value).into_owned()))
    }

    /// List all current entries in a projection view (newest writes are not
    /// ordered specially; entries come back in key order). Bounded by `limit`
    /// and `max_scan_rows`.
    pub async fn projection_list(
        &self,
        project_id: &str,
        scope_id: &str,
        view: &str,
        limit: usize,
        consistency: ConsistencyMode,
    ) -> Result<Vec<ProjectionEntry>, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        self.projection_list_inner(None, project_id, scope_id, view, limit, consistency)
            .await
    }

    /// [`AedbInstance::projection_list`] on behalf of `caller` (KvRead required).
    pub async fn projection_list_as(
        &self,
        caller: &CallerContext,
        project_id: &str,
        scope_id: &str,
        view: &str,
        limit: usize,
        consistency: ConsistencyMode,
    ) -> Result<Vec<ProjectionEntry>, AedbError> {
        self.projection_list_inner(Some(caller), project_id, scope_id, view, limit, consistency)
            .await
    }

    async fn projection_list_inner(
        &self,
        caller: Option<&CallerContext>,
        project_id: &str,
        scope_id: &str,
        view: &str,
        limit: usize,
        consistency: ConsistencyMode,
    ) -> Result<Vec<ProjectionEntry>, AedbError> {
        validate_name(view, "view")?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let lease = self.acquire_snapshot(consistency).await?;
        if let Some(caller) = caller {
            let required = Permission::KvRead {
                project_id: project_id.to_string(),
                scope_id: Some(scope_id.to_string()),
                prefix: Some(view_prefix(view)),
            };
            if !lease
                .view
                .catalog
                .has_permission(&caller.caller_id, &required)
            {
                return Err(AedbError::PermissionDenied("permission denied".into()));
            }
        }
        let prefix = view_prefix(view);
        let scan_limit = limit.min(self._config.max_scan_rows);
        let end = next_prefix_bytes(&prefix)
            .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
        let entries = lease.view.keyspace.try_kv_scan_range(
            project_id,
            scope_id,
            std::ops::Bound::Included(prefix.clone()),
            end,
            scan_limit,
        )?;
        Ok(entries
            .into_iter()
            .map(|(k, e)| ProjectionEntry {
                entity_key: String::from_utf8_lossy(&k[prefix.len().min(k.len())..]).into_owned(),
                value_json: String::from_utf8_lossy(&e.value).into_owned(),
                updated_at_micros: e.created_at,
            })
            .collect())
    }

    // ---- bounded series ------------------------------------------------------

    /// Append a point to a bounded series, retaining only the last `max_points`.
    /// `point_json` must be valid JSON. Concurrent appends are serialized via
    /// an optimistic version check and retried.
    pub async fn series_append(
        &self,
        project_id: &str,
        scope_id: &str,
        series: &str,
        point_json: &str,
        max_points: usize,
    ) -> Result<CommitResult, AedbError> {
        self.series_append_inner(None, project_id, scope_id, series, point_json, max_points)
            .await
    }

    /// [`AedbInstance::series_append`] on behalf of `caller` (KvWrite required).
    pub async fn series_append_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        series: &str,
        point_json: &str,
        max_points: usize,
    ) -> Result<CommitResult, AedbError> {
        self.series_append_inner(
            Some(caller),
            project_id,
            scope_id,
            series,
            point_json,
            max_points,
        )
        .await
    }

    async fn series_append_inner(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        series: &str,
        point_json: &str,
        max_points: usize,
    ) -> Result<CommitResult, AedbError> {
        validate_name(series, "series")?;
        if max_points == 0 {
            return Err(AedbError::Validation("max_points must be >= 1".into()));
        }
        let point = validate_json(point_json)?;
        let key = series_key(series);

        for attempt in 0..SERIES_APPEND_RETRIES {
            if attempt > 0 {
                // Let the commit that beat us land before re-reading the version.
                tokio::task::yield_now().await;
            }
            let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
            let base_seq = lease.view.seq;
            let existing = lease.view.keyspace.kv_get(project_id, scope_id, &key);

            let mut stored = match existing.as_ref() {
                Some(entry) => serde_json::from_slice::<StoredSeries>(&entry.value)
                    .unwrap_or(StoredSeries { points: Vec::new() }),
                None => StoredSeries { points: Vec::new() },
            };
            stored.points.push(point.clone());
            if stored.points.len() > max_points {
                let overflow = stored.points.len() - max_points;
                stored.points.drain(0..overflow);
            }
            let value = serde_json::to_vec(&stored)
                .map_err(|e| AedbError::Encode(format!("series encode failed: {e}")))?;

            let assertion = match existing.as_ref() {
                Some(entry) => ReadAssertion::KeyVersion {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    key: key.clone(),
                    expected_seq: entry.version,
                },
                None => ReadAssertion::KeyExists {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    key: key.clone(),
                    expected: false,
                },
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
                        value,
                    }],
                },
                base_seq,
            };
            match self.commit_envelope(envelope).await {
                Ok(commit) => return Ok(commit),
                Err(err) if is_retryable_conflict(&err) => continue,
                Err(err) => return Err(err),
            }
        }
        Err(AedbError::Conflict(
            "series_append exhausted retries under contention".into(),
        ))
    }

    /// Read up to `limit` points from a bounded series. With `newest_first`,
    /// the most recently appended points come first. Each point is returned as
    /// its JSON text.
    pub async fn series_read(
        &self,
        project_id: &str,
        scope_id: &str,
        series: &str,
        limit: usize,
        newest_first: bool,
        consistency: ConsistencyMode,
    ) -> Result<Vec<String>, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        validate_name(series, "series")?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let lease = self.acquire_snapshot(consistency).await?;
        let Some(entry) = lease
            .view
            .keyspace
            .kv_get(project_id, scope_id, &series_key(series))
        else {
            return Ok(Vec::new());
        };
        let stored = serde_json::from_slice::<StoredSeries>(&entry.value)
            .map_err(|_| AedbError::Decode("series record is corrupt".into()))?;
        let mut points: Vec<String> = stored.points.iter().map(|p| p.to_string()).collect();
        if newest_first {
            points.reverse();
        }
        points.truncate(limit);
        Ok(points)
    }
}

#[cfg(test)]
mod tests;
