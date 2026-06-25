//! Retention and compaction for the canonical event log.
//!
//! The `event_outbox` table is append-only and otherwise grows without bound.
//! [`AedbInstance::compact_events`] prunes the oldest events under a
//! [`EventRetentionPolicy`] (max age and/or keep-last-N) so hot state stays
//! bounded while recent history remains queryable.
//!
//! Safety: events are pruned only as a contiguous prefix of the oldest commit
//! sequences, and — when `respect_processor_checkpoints` is set — never above
//! the slowest reactive-processor checkpoint, so a monitor that has not yet
//! consumed a range can never have it deleted out from under it. The report
//! names the pruned (archived) sequence range so callers can ship those events
//! to cold storage first if they need durable archival beyond the hot log.
//!
//! `event_outbox` is a managed table, so pruning goes through the engine's
//! internal prevalidated commit path (the same one that maintains processor
//! checkpoints), not the public validated write path.

use crate::catalog::SYSTEM_PROJECT_ID;
use crate::catalog::types::Value;
use crate::commit::tx::{ReadSet, TransactionEnvelope, WriteClass, WriteIntent};
use crate::commit::validation::Mutation;
use crate::error::AedbError;
use crate::permission::{CallerContext, Permission};
use crate::query::plan::ConsistencyMode;
use crate::{
    AedbInstance, EVENT_OUTBOX_TABLE, EventCompactionReport, EventRetentionPolicy,
    REACTIVE_PROCESSOR_CHECKPOINTS_TABLE, SYSTEM_SCOPE_ID,
};

struct PrunePlan {
    deletes: Vec<Mutation>,
    report: EventCompactionReport,
    base_seq: u64,
}

impl AedbInstance {
    /// Compact the event log per `policy`. Available outside secure mode; use
    /// [`AedbInstance::compact_events_as`] with a `GlobalAdmin` caller otherwise.
    pub async fn compact_events(
        &self,
        policy: &EventRetentionPolicy,
    ) -> Result<EventCompactionReport, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated GlobalAdmin caller required in secure mode; use compact_events_as"
                    .into(),
            ));
        }
        let plan = self.plan_event_prune(policy).await?;
        if plan.deletes.is_empty() {
            return Ok(plan.report);
        }
        self.commit_envelope_prevalidated_internal(
            "compact_events",
            prune_envelope(plan.deletes, plan.base_seq),
        )
        .await?;
        Ok(plan.report)
    }

    /// Compact the event log on behalf of `caller`, who must hold `GlobalAdmin`.
    pub async fn compact_events_as(
        &self,
        caller: &CallerContext,
        policy: &EventRetentionPolicy,
    ) -> Result<EventCompactionReport, AedbError> {
        let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
        if !lease
            .view
            .catalog
            .has_permission(&caller.caller_id, &Permission::GlobalAdmin)
        {
            return Err(AedbError::PermissionDenied(
                "compact_events requires GlobalAdmin".into(),
            ));
        }
        let plan = self.plan_event_prune(policy).await?;
        if plan.deletes.is_empty() {
            return Ok(plan.report);
        }
        self.commit_envelope_prevalidated_system_internal(
            "compact_events",
            prune_envelope(plan.deletes, plan.base_seq),
        )
        .await?;
        Ok(plan.report)
    }

    /// Compute the contiguous prefix of oldest events eligible for pruning.
    async fn plan_event_prune(
        &self,
        policy: &EventRetentionPolicy,
    ) -> Result<PrunePlan, AedbError> {
        let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
        let base_seq = lease.view.seq;
        let empty = EventCompactionReport {
            pruned_count: 0,
            archived_min_seq: None,
            archived_max_seq: None,
            processor_floor_seq: None,
            more_remaining: false,
        };

        // With no retention constraint set, prune nothing. (The processor floor
        // is a guard, not a retention policy — it only restricts pruning.)
        if policy.max_age_micros.is_none() && policy.keep_last.is_none() {
            return Ok(PrunePlan {
                deletes: Vec::new(),
                report: empty,
                base_seq,
            });
        }

        let Some(table) =
            lease
                .view
                .keyspace
                .table(SYSTEM_PROJECT_ID, SYSTEM_SCOPE_ID, EVENT_OUTBOX_TABLE)
        else {
            return Ok(PrunePlan {
                deletes: Vec::new(),
                report: empty,
                base_seq,
            });
        };

        let total = table.rows.len() as u64;
        // Count policy: how many of the oldest events may be pruned.
        let count_prune_max = match policy.keep_last {
            Some(keep) => total.saturating_sub(keep),
            None => total, // no count constraint
        };
        // Age policy: events with ts strictly older than the cutoff may be pruned.
        let age_cutoff = policy
            .max_age_micros
            .map(|age| crate::system_now_micros().saturating_sub(age));
        // Processor floor: never prune above the slowest reactive checkpoint.
        let processor_floor = if policy.respect_processor_checkpoints {
            self.min_processor_checkpoint(&lease.view.keyspace)?
        } else {
            None
        };
        let cap = policy
            .max_prune_per_run
            .unwrap_or(self._config.max_scan_rows);

        let mut deletes = Vec::new();
        let mut min_seq: Option<u64> = None;
        let mut max_seq: Option<u64> = None;
        let mut index: u64 = 0;
        let mut more_remaining = false;

        for stored in table.rows.values() {
            let row = lease.view.keyspace.materialize_row(stored)?;
            let (
                Some(Value::Integer(seq_i64)),
                Some(Value::Timestamp(ts_i64)),
                Some(Value::Text(topic)),
                Some(Value::Text(event_key)),
            ) = (
                row.values.first(),
                row.values.get(1),
                row.values.get(4),
                row.values.get(5),
            )
            else {
                index += 1;
                continue;
            };
            let Ok(seq) = u64::try_from(*seq_i64) else {
                index += 1;
                continue;
            };
            let ts = u64::try_from(*ts_i64).unwrap_or(0);

            // Every active constraint must permit pruning. Each is monotonic in
            // ascending seq order, so the first event that fails ends the prefix.
            let count_ok = index < count_prune_max;
            let age_ok = age_cutoff.is_none_or(|cutoff| ts < cutoff);
            let floor_ok = processor_floor.is_none_or(|floor| seq <= floor);
            if !(count_ok && age_ok && floor_ok) {
                break;
            }

            if deletes.len() >= cap {
                more_remaining = true;
                break;
            }

            deletes.push(Mutation::Delete {
                project_id: SYSTEM_PROJECT_ID.to_string(),
                scope_id: SYSTEM_SCOPE_ID.to_string(),
                table_name: EVENT_OUTBOX_TABLE.to_string(),
                primary_key: vec![
                    Value::Integer(*seq_i64),
                    Value::Text(topic.clone()),
                    Value::Text(event_key.clone()),
                ],
            });
            min_seq.get_or_insert(seq);
            max_seq = Some(seq);
            index += 1;
        }

        Ok(PrunePlan {
            report: EventCompactionReport {
                pruned_count: deletes.len() as u64,
                archived_min_seq: min_seq,
                archived_max_seq: max_seq,
                processor_floor_seq: processor_floor,
                more_remaining,
            },
            deletes,
            base_seq,
        })
    }

    /// The minimum checkpoint sequence across all reactive processors, or `None`
    /// if no processor has registered a checkpoint (no constraint).
    fn min_processor_checkpoint(
        &self,
        keyspace: &crate::storage::keyspace::KeyspaceSnapshot,
    ) -> Result<Option<u64>, AedbError> {
        let Some(table) = keyspace.table(
            SYSTEM_PROJECT_ID,
            SYSTEM_SCOPE_ID,
            REACTIVE_PROCESSOR_CHECKPOINTS_TABLE,
        ) else {
            return Ok(None);
        };
        let mut min: Option<u64> = None;
        for stored in table.rows.values() {
            let row = keyspace.materialize_row(stored)?;
            if let Some(Value::Integer(seq)) = row.values.get(1)
                && let Ok(seq) = u64::try_from(*seq)
            {
                min = Some(min.map_or(seq, |m| m.min(seq)));
            }
        }
        Ok(min)
    }
}

fn prune_envelope(deletes: Vec<Mutation>, base_seq: u64) -> TransactionEnvelope {
    TransactionEnvelope {
        caller: None,
        idempotency_key: None,
        write_class: WriteClass::Standard,
        assertions: Vec::new(),
        read_set: ReadSet::default(),
        write_intent: WriteIntent { mutations: deletes },
        base_seq,
    }
}

#[cfg(test)]
mod tests;
