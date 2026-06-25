//! Debugging and inspection: stitch the event log, external-effect checkpoints,
//! and monitor checkpoints into answers a developer actually asks.
//!
//! * [`AedbInstance::inspect_commit`] — what events a commit emitted.
//! * [`AedbInstance::find_events_by_key`] — which commit indexed a value (e.g.
//!   the event that recorded a user's share price).
//! * [`AedbInstance::event_log_summary`] — size and sequence range of the log.
//! * [`AedbInstance::inspect_namespace`] — a one-call dashboard of a project's
//!   monitors (checkpoint + lease state) and external-effect checkpoint counts.
//!
//! These are read-only operator tools; like the other read paths they require
//! a caller in secure mode.

use std::ops::Bound;

use crate::api::effect_checkpoint::EFFECT_CHECKPOINT_KEY_PREFIX;
use crate::api::monitor::MONITOR_KEY_PREFIX;
use crate::catalog::SYSTEM_PROJECT_ID;
use crate::catalog::types::Value;
use crate::error::AedbError;
use crate::lib_helpers::next_prefix_bytes;
use crate::query::plan::ConsistencyMode;
use crate::{
    AedbInstance, CommitTrace, EVENT_OUTBOX_TABLE, EventLogSummary, EventOutboxRecord, EventQuery,
    NamespaceInspection, SYSTEM_SCOPE_ID,
};

impl AedbInstance {
    /// The events emitted by a single commit (its observable effects on the log).
    pub async fn inspect_commit(
        &self,
        commit_seq: u64,
        consistency: ConsistencyMode,
    ) -> Result<CommitTrace, AedbError> {
        // Reuse the event query: exactly the events whose commit_seq == seq.
        let mut query = EventQuery::new().limit(self._config.max_scan_rows);
        if commit_seq > 0 {
            query = query.after_commit_seq(commit_seq - 1);
        }
        query = query.before_commit_seq(commit_seq.saturating_add(1));
        let page = self.query_events(&query, consistency).await?;
        Ok(CommitTrace {
            commit_seq,
            events: page.events,
        })
    }

    /// Locate the event(s) that recorded a given key under a topic — e.g. which
    /// commit indexed a user's share price. Returns matches newest-first.
    pub async fn find_events_by_key(
        &self,
        topic: &str,
        event_key: &str,
        limit: usize,
        consistency: ConsistencyMode,
    ) -> Result<Vec<EventOutboxRecord>, AedbError> {
        // event_key is part of the PK but not directly seekable without the
        // commit_seq, so scan by topic (bounded) and filter.
        let query = EventQuery::new()
            .topic(topic)
            .descending()
            .limit(self._config.max_scan_rows);
        let page = self.query_events(&query, consistency).await?;
        Ok(page
            .events
            .into_iter()
            .filter(|e| e.event_key == event_key)
            .take(limit)
            .collect())
    }

    /// Size and commit-sequence range of the canonical event log.
    pub async fn event_log_summary(
        &self,
        consistency: ConsistencyMode,
    ) -> Result<EventLogSummary, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        let lease = self.acquire_snapshot(consistency).await?;
        let Some(table) =
            lease
                .view
                .keyspace
                .table(SYSTEM_PROJECT_ID, SYSTEM_SCOPE_ID, EVENT_OUTBOX_TABLE)
        else {
            return Ok(EventLogSummary {
                count: 0,
                oldest_seq: None,
                newest_seq: None,
            });
        };
        let count = table.rows.len() as u64;
        let oldest = match table.rows.values().next() {
            Some(stored) => {
                let row = lease.view.keyspace.materialize_row(stored)?;
                row_seq(&row)
            }
            None => None,
        };
        let newest = match table.rows.values().next_back() {
            Some(stored) => {
                let row = lease.view.keyspace.materialize_row(stored)?;
                row_seq(&row)
            }
            None => None,
        };
        Ok(EventLogSummary {
            count,
            oldest_seq: oldest,
            newest_seq: newest,
        })
    }

    /// Aggregate the monitors and external-effect checkpoints of one namespace.
    pub async fn inspect_namespace(
        &self,
        project_id: &str,
        scope_id: &str,
        consistency: ConsistencyMode,
    ) -> Result<NamespaceInspection, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        let lease = self.acquire_snapshot(consistency).await?;
        let keyspace = &lease.view.keyspace;
        let cap = self._config.max_scan_rows;

        // Effect checkpoints: count by status.
        let mut effect_pending = 0u64;
        let mut effect_committed = 0u64;
        for (_, entry) in scan_prefix(
            keyspace,
            project_id,
            scope_id,
            EFFECT_CHECKPOINT_KEY_PREFIX,
            cap,
        )? {
            match serde_json::from_slice::<serde_json::Value>(&entry.value)
                .ok()
                .and_then(|v| v.get("status").and_then(|s| s.as_str()).map(str::to_owned))
                .as_deref()
            {
                Some("committed") => effect_committed += 1,
                Some(_) => effect_pending += 1,
                None => {}
            }
        }

        // Monitors: discover names by prefix, then read each status.
        let mut monitor_names = Vec::new();
        for (key, _) in scan_prefix(keyspace, project_id, scope_id, MONITOR_KEY_PREFIX, cap)? {
            let name = String::from_utf8_lossy(&key[MONITOR_KEY_PREFIX.len().min(key.len())..])
                .into_owned();
            monitor_names.push(name);
        }
        drop(lease);
        let mut monitors = Vec::with_capacity(monitor_names.len());
        for name in monitor_names {
            if let Some(status) = self
                .monitor_status(project_id, scope_id, &name, consistency)
                .await?
            {
                monitors.push(status);
            }
        }

        Ok(NamespaceInspection {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            monitors,
            effect_pending,
            effect_committed,
        })
    }
}

fn row_seq(row: &crate::catalog::types::Row) -> Option<u64> {
    match row.values.first() {
        Some(Value::Integer(i)) => u64::try_from(*i).ok(),
        _ => None,
    }
}

#[allow(clippy::type_complexity)]
fn scan_prefix(
    keyspace: &crate::storage::keyspace::KeyspaceSnapshot,
    project_id: &str,
    scope_id: &str,
    prefix: &[u8],
    limit: usize,
) -> Result<Vec<(Vec<u8>, crate::storage::keyspace::KvEntry)>, AedbError> {
    let end = next_prefix_bytes(prefix).map_or(Bound::Unbounded, Bound::Excluded);
    keyspace.try_kv_scan_range(
        project_id,
        scope_id,
        Bound::Included(prefix.to_vec()),
        end,
        limit,
    )
}

#[cfg(test)]
mod tests;
