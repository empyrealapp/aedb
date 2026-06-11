//! Ergonomic, first-class querying over the canonical event log.
//!
//! Events are appended to the `event_outbox` system table (primary key
//! `(commit_seq, topic, event_key)`) by [`crate::AedbInstance::emit_event`] and
//! by `commit_effect_batch`. This module turns that raw, append-only log into a
//! query surface suitable for building UIs and monitors: filter by event type,
//! project/scope, commit-sequence range, time range, and arbitrary payload
//! fields, in either direction, with commit-atomic cursor pagination.
//!
//! The query reads through a snapshot ([`ConsistencyMode`]) and is bounded by
//! `max_scan_rows` so a selective filter can never trigger an unbounded scan;
//! when the budget is hit mid-page the returned cursor lets the caller resume.

use std::ops::Bound;

use crate::catalog::SYSTEM_PROJECT_ID;
use crate::catalog::types::Value;
use crate::error::AedbError;
use crate::permission::{CallerContext, Permission};
use crate::query::plan::ConsistencyMode;
use crate::query_authorization::ensure_external_caller_allowed;
use crate::storage::encoded_key::EncodedKey;
use crate::{
    AedbInstance, EVENT_OUTBOX_TABLE, EventFieldFilter, EventOrder, EventOutboxRecord, EventQuery,
    EventStreamPage, SYSTEM_SCOPE_ID,
};

impl EventQuery {
    /// Start an empty query (ascending, no filters). Set `limit` before use;
    /// a `limit` of 0 yields an empty page.
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by event type (the emit `topic`).
    pub fn topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = Some(topic.into());
        self
    }

    /// Restrict to a single project / scope namespace.
    pub fn namespace(mut self, project_id: impl Into<String>, scope_id: impl Into<String>) -> Self {
        self.project_id = Some(project_id.into());
        self.scope_id = Some(scope_id.into());
        self
    }

    /// Resume an ascending scan after this commit sequence (exclusive).
    pub fn after_commit_seq(mut self, seq: u64) -> Self {
        self.from_commit_seq_exclusive = Some(seq);
        self
    }

    /// Resume a descending scan before this commit sequence (exclusive).
    pub fn before_commit_seq(mut self, seq: u64) -> Self {
        self.to_commit_seq_exclusive = Some(seq);
        self
    }

    /// Inclusive timestamp window (micros since epoch).
    pub fn time_range(mut self, from_micros: Option<u64>, to_micros: Option<u64>) -> Self {
        self.from_ts_micros = from_micros;
        self.to_ts_micros = to_micros;
        self
    }

    /// Add a payload-field equality filter (e.g. recipient, instance_id, block).
    pub fn where_field(mut self, field: impl Into<String>, equals: impl Into<String>) -> Self {
        self.field_filters.push(EventFieldFilter {
            field: field.into(),
            equals: equals.into(),
        });
        self
    }

    /// Newest-first ordering (for "latest N by type").
    pub fn descending(mut self) -> Self {
        self.order = EventOrder::Descending;
        self
    }

    /// Page size.
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }
}

impl AedbInstance {
    /// Query the canonical event log. See [`EventQuery`].
    ///
    /// In secure mode (authenticated calls required) this rejects anonymous
    /// access; use [`AedbInstance::query_events_as`] with a caller instead.
    pub async fn query_events(
        &self,
        query: &EventQuery,
        consistency: ConsistencyMode,
    ) -> Result<EventStreamPage, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        self.query_events_internal(query, consistency, None).await
    }

    /// Query the canonical event log on behalf of `caller`, enforcing
    /// `TableRead` permission on the event log.
    pub async fn query_events_as(
        &self,
        caller: &CallerContext,
        query: &EventQuery,
        consistency: ConsistencyMode,
    ) -> Result<EventStreamPage, AedbError> {
        ensure_external_caller_allowed(caller)?;
        self.query_events_internal(query, consistency, Some(caller))
            .await
    }

    /// Convenience: the most recent `n` events of a given type, newest first.
    pub async fn latest_events_by_topic(
        &self,
        topic: &str,
        n: usize,
        consistency: ConsistencyMode,
    ) -> Result<Vec<EventOutboxRecord>, AedbError> {
        let query = EventQuery::new().topic(topic).descending().limit(n);
        Ok(self.query_events(&query, consistency).await?.events)
    }

    async fn query_events_internal(
        &self,
        query: &EventQuery,
        consistency: ConsistencyMode,
        caller: Option<&CallerContext>,
    ) -> Result<EventStreamPage, AedbError> {
        let lease = self.acquire_snapshot(consistency).await?;
        let snapshot_seq = lease.view.seq;

        if query.limit == 0 {
            return Ok(EventStreamPage {
                events: Vec::new(),
                next_commit_seq: None,
                snapshot_seq,
            });
        }

        if let Some(caller) = caller {
            let required = Permission::TableRead {
                project_id: SYSTEM_PROJECT_ID.to_string(),
                scope_id: SYSTEM_SCOPE_ID.to_string(),
                table_name: EVENT_OUTBOX_TABLE.to_string(),
            };
            if !lease
                .view
                .catalog
                .has_permission(&caller.caller_id, &required)
            {
                return Err(AedbError::PermissionDenied("permission denied".into()));
            }
        }

        let Some(table) =
            lease
                .view
                .keyspace
                .table(SYSTEM_PROJECT_ID, SYSTEM_SCOPE_ID, EVENT_OUTBOX_TABLE)
        else {
            return Ok(EventStreamPage {
                events: Vec::new(),
                next_commit_seq: None,
                snapshot_seq,
            });
        };

        // Translate the commit-sequence cursor bounds into key bounds. The PK
        // is (commit_seq, topic, event_key); a key with an empty topic sorts
        // before every real row at the same commit_seq, so excluding it cleanly
        // drops an entire commit_seq from the range.
        let start_bound = match query.from_commit_seq_exclusive {
            Some(from) => match from.checked_add(1) {
                Some(start_seq) => match i64::try_from(start_seq) {
                    Ok(start_seq_i64) => Bound::Included(seq_key(start_seq_i64)),
                    // start beyond i64::MAX: nothing can match.
                    Err(_) => {
                        return Ok(EventStreamPage {
                            events: Vec::new(),
                            next_commit_seq: None,
                            snapshot_seq,
                        });
                    }
                },
                None => {
                    return Ok(EventStreamPage {
                        events: Vec::new(),
                        next_commit_seq: None,
                        snapshot_seq,
                    });
                }
            },
            None => Bound::Unbounded,
        };
        let end_bound = match query.to_commit_seq_exclusive {
            Some(to) => match i64::try_from(to) {
                Ok(to_i64) => Bound::Excluded(seq_key(to_i64)),
                // upper bound beyond i64::MAX: no effective cap.
                Err(_) => Bound::Unbounded,
            },
            None => Bound::Unbounded,
        };

        let max_scan_rows = self._config.max_scan_rows;
        let limit = query.limit;

        let mut events: Vec<EventOutboxRecord> = Vec::new();
        let mut examined: usize = 0;
        // Once set, we are finishing the events of the boundary commit and will
        // break as soon as we cross to a different commit_seq. This keeps a page
        // commit-atomic so cursor resumption never skips or duplicates events.
        let mut boundary_seq: Option<u64> = None;
        let mut stopped_early = false;

        let range = table.rows.range((start_bound, end_bound));
        match query.order {
            EventOrder::Ascending => {
                for row in range.map(|(_, row)| row) {
                    if !Self::consume_event_row(
                        row,
                        query,
                        limit,
                        max_scan_rows,
                        &mut events,
                        &mut examined,
                        &mut boundary_seq,
                        &mut stopped_early,
                    ) {
                        break;
                    }
                }
            }
            EventOrder::Descending => {
                for row in range.rev().map(|(_, row)| row) {
                    if !Self::consume_event_row(
                        row,
                        query,
                        limit,
                        max_scan_rows,
                        &mut events,
                        &mut examined,
                        &mut boundary_seq,
                        &mut stopped_early,
                    ) {
                        break;
                    }
                }
            }
        }

        // The cursor is the boundary commit_seq only when we stopped early
        // (there may be more); a naturally exhausted range reports no cursor.
        let next_commit_seq = if stopped_early { boundary_seq } else { None };

        Ok(EventStreamPage {
            events,
            next_commit_seq,
            snapshot_seq,
        })
    }

    /// Process one event-log row. Returns `false` when the scan should stop.
    /// The boundary logic is direction-agnostic: iteration order is already
    /// fixed by the caller, and a page boundary always ends at the trailing
    /// edge of the current commit_seq.
    #[allow(clippy::too_many_arguments)]
    fn consume_event_row(
        row: &crate::catalog::types::Row,
        query: &EventQuery,
        limit: usize,
        max_scan_rows: usize,
        events: &mut Vec<EventOutboxRecord>,
        examined: &mut usize,
        boundary_seq: &mut Option<u64>,
        stopped_early: &mut bool,
    ) -> bool {
        let Some(record) = decode_event_row(row) else {
            return true;
        };

        // If we are finishing a boundary commit, stop the moment we leave it.
        if let Some(b) = *boundary_seq
            && record.commit_seq != b
        {
            *stopped_early = true;
            return false;
        }

        *examined += 1;

        if event_matches(&record, query) {
            events.push(record.clone());
        }

        // After fully processing a row, decide whether this commit_seq becomes
        // the page boundary (limit reached, or scan budget exhausted). We finish
        // the rest of this commit's rows before breaking.
        if boundary_seq.is_none() && (events.len() >= limit || *examined >= max_scan_rows) {
            *boundary_seq = Some(record.commit_seq);
        }
        true
    }
}

/// Build a range key for the start of a commit_seq (empty topic / event_key
/// sort before every real row at that sequence).
fn seq_key(commit_seq_i64: i64) -> EncodedKey {
    EncodedKey::from_values(&[
        Value::Integer(commit_seq_i64),
        Value::Text("".into()),
        Value::Text("".into()),
    ])
}

/// Decode an `event_outbox` row into a public record, or `None` if the row
/// shape is unexpected (defensive; rows are written by a single code path).
fn decode_event_row(row: &crate::catalog::types::Row) -> Option<EventOutboxRecord> {
    let (
        Some(Value::Integer(commit_seq_i64)),
        Some(Value::Timestamp(ts_i64)),
        Some(Value::Text(project_id)),
        Some(Value::Text(scope_id)),
        Some(Value::Text(topic)),
        Some(Value::Text(event_key)),
        Some(Value::Json(payload)),
    ) = (
        row.values.first(),
        row.values.get(1),
        row.values.get(2),
        row.values.get(3),
        row.values.get(4),
        row.values.get(5),
        row.values.get(6),
    )
    else {
        return None;
    };
    let commit_seq = u64::try_from(*commit_seq_i64).ok()?;
    let ts_micros = u64::try_from(*ts_i64).ok()?;
    Some(EventOutboxRecord {
        commit_seq,
        ts_micros,
        project_id: project_id.to_string(),
        scope_id: scope_id.to_string(),
        topic: topic.to_string(),
        event_key: event_key.to_string(),
        payload_json: payload.to_string(),
    })
}

/// Apply the non-range filters (topic, namespace, time window, payload fields).
fn event_matches(record: &EventOutboxRecord, query: &EventQuery) -> bool {
    if let Some(topic) = &query.topic
        && &record.topic != topic
    {
        return false;
    }
    if let Some(project_id) = &query.project_id
        && &record.project_id != project_id
    {
        return false;
    }
    if let Some(scope_id) = &query.scope_id
        && &record.scope_id != scope_id
    {
        return false;
    }
    if let Some(from) = query.from_ts_micros
        && record.ts_micros < from
    {
        return false;
    }
    if let Some(to) = query.to_ts_micros
        && record.ts_micros > to
    {
        return false;
    }
    payload_fields_match(&record.payload_json, &query.field_filters)
}

/// Match top-level JSON payload fields by their string rendering. A payload
/// that is not a JSON object, or that lacks a filtered field, does not match.
fn payload_fields_match(payload_json: &str, filters: &[EventFieldFilter]) -> bool {
    if filters.is_empty() {
        return true;
    }
    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(payload_json) else {
        return false;
    };
    let Some(obj) = parsed.as_object() else {
        return false;
    };
    for filter in filters {
        let Some(value) = obj.get(&filter.field) else {
            return false;
        };
        let matches = match value {
            serde_json::Value::String(s) => s == &filter.equals,
            serde_json::Value::Number(n) => n.to_string() == filter.equals,
            serde_json::Value::Bool(b) => b.to_string() == filter.equals,
            _ => false,
        };
        if !matches {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests;
