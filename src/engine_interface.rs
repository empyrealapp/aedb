use crate::catalog::types::{Row, Value};
use crate::catalog::{SYSTEM_PROJECT_ID, namespace_key};
use crate::commit::tx::{
    AssertionActual, ReadAssertion, ReadKey, ReadSet, ReadSetEntry, TransactionEnvelope,
    WriteClass, WriteIntent,
};
use crate::commit::validation::Mutation;
use crate::error::AedbError;
use crate::query::plan::ConsistencyMode;
use crate::storage::encoded_key::EncodedKey;
use crate::{AedbInstance, CommitResult, EventOutboxRecord};
use crate::{catalog::Catalog, catalog::schema::TableSchema};
use std::collections::HashMap;
use std::ops::Bound;

const SYSTEM_SCOPE_ID: &str = "app";
const EVENT_OUTBOX_TABLE: &str = "event_outbox";
const REACTIVE_PROCESSOR_CHECKPOINTS_TABLE: &str = "reactive_processor_checkpoints";

#[derive(Debug, Clone, PartialEq)]
pub struct EffectBatch {
    pub preconditions: Vec<EffectPrecondition>,
    pub effects: Vec<EffectOperation>,
    pub events: Vec<EffectEvent>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum EffectPrecondition {
    RequireAvailable {
        accumulator: String,
        min_amount: i64,
    },
    RequireExposureOk {
        accumulator: String,
        amount: i64,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum EffectOperation {
    Accumulate {
        accumulator: String,
        delta: i64,
        dedupe_id: String,
        order_key: u64,
    },
    Expose {
        accumulator: String,
        amount: i64,
        dedupe_id: String,
    },
    ReleaseExposure {
        accumulator: String,
        dedupe_id: String,
    },
    Write {
        keyed_state: String,
        key: Value,
        value: Row,
    },
    Delete {
        keyed_state: String,
        key: Value,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectEvent {
    pub event_name: String,
    pub event_key: String,
    pub data_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchRejected {
    pub error_code: &'static str,
    pub failed_precondition: String,
    pub actual_value: i64,
}

#[derive(Debug, Clone)]
pub enum EffectBatchCommitResult {
    Applied(CommitResult),
    Rejected(BatchRejected),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessorPullResult {
    pub events: Vec<EventOutboxRecord>,
    pub checkpoint_seq: u64,
    pub last_commit_seq: u64,
    pub head_seq: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KeyedStateQueryRequest {
    pub index_name: String,
    pub prefix: Vec<Value>,
    pub offset: usize,
    pub limit: usize,
}

pub struct ProcessorContext<'a> {
    db: &'a AedbInstance,
    project_id: String,
    scope_id: String,
    processor_id: String,
    source_event: String,
    pending: Vec<Mutation>,
    checkpoint_seq: u64,
}

struct KeyedStateSnapshot {
    row: Option<Row>,
    version: u64,
    snapshot_seq: u64,
}

impl AedbInstance {
    pub async fn commit_effect_batch(
        &self,
        project_id: &str,
        scope_id: &str,
        batch: EffectBatch,
    ) -> Result<EffectBatchCommitResult, AedbError> {
        let preflight_lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
        let assertions: Vec<ReadAssertion> = batch
            .preconditions
            .iter()
            .map(|precondition| match precondition {
                EffectPrecondition::RequireAvailable {
                    accumulator,
                    min_amount,
                } => ReadAssertion::AccumulatorAvailableAtLeast {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    accumulator_name: accumulator.clone(),
                    min_amount: *min_amount,
                },
                EffectPrecondition::RequireExposureOk {
                    accumulator,
                    amount,
                } => ReadAssertion::AccumulatorExposureWithinMargin {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    accumulator_name: accumulator.clone(),
                    additional_exposure: *amount,
                },
            })
            .collect();

        let mut mutations = Vec::with_capacity(batch.effects.len() + batch.events.len());
        let mut keyed_state_pk_index_cache: HashMap<String, usize> = HashMap::new();
        for effect in batch.effects {
            match effect {
                EffectOperation::Accumulate {
                    accumulator,
                    delta,
                    dedupe_id,
                    order_key,
                } => mutations.push(Mutation::Accumulate {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    accumulator_name: accumulator,
                    delta,
                    dedupe_key: dedupe_id,
                    order_key,
                    release_exposure_id: None,
                }),
                EffectOperation::Expose {
                    accumulator,
                    amount,
                    dedupe_id,
                } => mutations.push(Mutation::ExposeAccumulator {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    accumulator_name: accumulator,
                    amount,
                    exposure_id: dedupe_id,
                }),
                EffectOperation::ReleaseExposure {
                    accumulator,
                    dedupe_id,
                } => mutations.push(Mutation::ReleaseAccumulatorExposure {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    accumulator_name: accumulator,
                    exposure_id: dedupe_id,
                }),
                EffectOperation::Write {
                    keyed_state,
                    key,
                    value,
                } => {
                    let pk_index =
                        if let Some(pk_index) = keyed_state_pk_index_cache.get(&keyed_state) {
                            *pk_index
                        } else {
                            let schema = keyed_state_schema(
                                &preflight_lease.view.catalog,
                                project_id,
                                scope_id,
                                &keyed_state,
                                "write",
                            )?;
                            let pk_index = keyed_state_primary_key_index(
                                schema,
                                project_id,
                                scope_id,
                                &keyed_state,
                            )?;
                            keyed_state_pk_index_cache.insert(keyed_state.clone(), pk_index);
                            pk_index
                        };
                    validate_keyed_state_row_matches_key(
                        pk_index,
                        project_id,
                        scope_id,
                        &keyed_state,
                        &key,
                        &value,
                    )?;
                    mutations.push(Mutation::Upsert {
                        project_id: project_id.to_string(),
                        scope_id: scope_id.to_string(),
                        table_name: keyed_state,
                        primary_key: vec![key],
                        row: value,
                    });
                }
                EffectOperation::Delete { keyed_state, key } => {
                    if !keyed_state_pk_index_cache.contains_key(&keyed_state) {
                        let schema = keyed_state_schema(
                            &preflight_lease.view.catalog,
                            project_id,
                            scope_id,
                            &keyed_state,
                            "delete",
                        )?;
                        let pk_index = keyed_state_primary_key_index(
                            schema,
                            project_id,
                            scope_id,
                            &keyed_state,
                        )?;
                        keyed_state_pk_index_cache.insert(keyed_state.clone(), pk_index);
                    }
                    mutations.push(Mutation::Delete {
                        project_id: project_id.to_string(),
                        scope_id: scope_id.to_string(),
                        table_name: keyed_state,
                        primary_key: vec![key],
                    });
                }
            }
        }

        for event in batch.events {
            mutations.push(Mutation::EmitEvent {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                topic: event.event_name,
                event_key: event.event_key,
                payload_json: event.data_json,
            });
        }

        if mutations.is_empty() {
            return Err(AedbError::Validation(
                "effect batch must include at least one effect or event".into(),
            ));
        }

        let committed = self
            .commit_envelope(TransactionEnvelope {
                caller: None,
                idempotency_key: None,
                write_class: WriteClass::Standard,
                assertions,
                read_set: ReadSet::default(),
                write_intent: WriteIntent { mutations },
                base_seq: preflight_lease.view.seq,
            })
            .await;
        match committed {
            Ok(commit) => Ok(EffectBatchCommitResult::Applied(commit)),
            Err(AedbError::AssertionFailed {
                index,
                assertion,
                actual,
            }) => {
                if let Some(rejected) = batch_rejected_from_assertion(&assertion, &actual) {
                    Ok(EffectBatchCommitResult::Rejected(rejected))
                } else {
                    Err(AedbError::AssertionFailed {
                        index,
                        assertion,
                        actual,
                    })
                }
            }
            Err(err) => Err(err),
        }
    }

    pub async fn keyed_state_read(
        &self,
        project_id: &str,
        scope_id: &str,
        keyed_state: &str,
        key: Value,
        consistency: ConsistencyMode,
    ) -> Result<Option<Row>, AedbError> {
        let lease = self.acquire_snapshot(consistency).await?;
        let _ = keyed_state_schema(
            &lease.view.catalog,
            project_id,
            scope_id,
            keyed_state,
            "read",
        )?;
        let snapshot = keyed_state_snapshot_from_table(
            &lease.view.keyspace,
            lease.view.seq,
            project_id,
            scope_id,
            keyed_state,
            key,
        );
        Ok(snapshot.row)
    }

    pub async fn keyed_state_read_field(
        &self,
        project_id: &str,
        scope_id: &str,
        keyed_state: &str,
        key: Value,
        field: &str,
        consistency: ConsistencyMode,
    ) -> Result<Option<Value>, AedbError> {
        let lease = self.acquire_snapshot(consistency).await?;
        let schema = keyed_state_schema(
            &lease.view.catalog,
            project_id,
            scope_id,
            keyed_state,
            "read_field",
        )?;
        let Some(col_idx) = schema.columns.iter().position(|c| c.name == field) else {
            return Err(AedbError::Validation(format!(
                "unknown field in keyed_state: {project_id}.{scope_id}.{keyed_state}.{field}"
            )));
        };
        let snapshot = keyed_state_snapshot_from_table(
            &lease.view.keyspace,
            lease.view.seq,
            project_id,
            scope_id,
            keyed_state,
            key,
        );
        Ok(snapshot
            .row
            .and_then(|row| row.values.get(col_idx).cloned()))
    }

    pub async fn keyed_state_write(
        &self,
        project_id: &str,
        scope_id: &str,
        keyed_state: &str,
        key: Value,
        value: Row,
    ) -> Result<CommitResult, AedbError> {
        let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
        let schema = keyed_state_schema(
            &lease.view.catalog,
            project_id,
            scope_id,
            keyed_state,
            "write",
        )?;
        let pk_index = keyed_state_primary_key_index(schema, project_id, scope_id, keyed_state)?;
        validate_keyed_state_row_matches_key(
            pk_index,
            project_id,
            scope_id,
            keyed_state,
            &key,
            &value,
        )?;
        self.commit(Mutation::Upsert {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            table_name: keyed_state.to_string(),
            primary_key: vec![key],
            row: value,
        })
        .await
    }

    pub async fn keyed_state_delete(
        &self,
        project_id: &str,
        scope_id: &str,
        keyed_state: &str,
        key: Value,
    ) -> Result<CommitResult, AedbError> {
        let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
        let _ = keyed_state_schema(
            &lease.view.catalog,
            project_id,
            scope_id,
            keyed_state,
            "delete",
        )?;
        self.commit(Mutation::Delete {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            table_name: keyed_state.to_string(),
            primary_key: vec![key],
        })
        .await
    }

    pub async fn keyed_state_update<F>(
        &self,
        project_id: &str,
        scope_id: &str,
        keyed_state: &str,
        key: Value,
        update_fn: F,
    ) -> Result<CommitResult, AedbError>
    where
        F: FnOnce(Option<Row>) -> Result<Option<Row>, AedbError>,
    {
        let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
        let schema = keyed_state_schema(
            &lease.view.catalog,
            project_id,
            scope_id,
            keyed_state,
            "update",
        )?;
        let pk_index = keyed_state_primary_key_index(schema, project_id, scope_id, keyed_state)?;
        let snapshot = keyed_state_snapshot_from_table(
            &lease.view.keyspace,
            lease.view.seq,
            project_id,
            scope_id,
            keyed_state,
            key.clone(),
        );
        let assertion = keyed_state_assertion(project_id, scope_id, keyed_state, &key, &snapshot);
        let next = update_fn(snapshot.row)?;
        let mutation = match next {
            Some(row) => {
                validate_keyed_state_row_matches_key(
                    pk_index,
                    project_id,
                    scope_id,
                    keyed_state,
                    &key,
                    &row,
                )?;
                Mutation::Upsert {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    table_name: keyed_state.to_string(),
                    primary_key: vec![key],
                    row,
                }
            }
            None => Mutation::Delete {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: keyed_state.to_string(),
                primary_key: vec![key],
            },
        };
        self.commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: vec![assertion],
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![mutation],
            },
            base_seq: snapshot.snapshot_seq,
        })
        .await
    }

    pub async fn keyed_state_query_index(
        &self,
        project_id: &str,
        scope_id: &str,
        keyed_state: &str,
        req: KeyedStateQueryRequest,
        consistency: ConsistencyMode,
    ) -> Result<Vec<Row>, AedbError> {
        if req.limit == 0 {
            return Ok(Vec::new());
        }
        let lease = self.acquire_snapshot(consistency).await?;
        let _ = keyed_state_schema(
            &lease.view.catalog,
            project_id,
            scope_id,
            keyed_state,
            "query_index",
        )?;
        let Some(table) = lease.view.keyspace.table(project_id, scope_id, keyed_state) else {
            return Err(AedbError::Validation(format!(
                "table not found: {project_id}.{scope_id}.{keyed_state}"
            )));
        };
        let Some(index) = table.indexes.get(&req.index_name) else {
            return Err(AedbError::Validation(format!(
                "index not found: {project_id}.{scope_id}.{keyed_state}.{}",
                req.index_name
            )));
        };

        let pks = if req.prefix.is_empty() {
            index.scan_prefix_window(None, req.offset, req.limit)
        } else {
            let prefix = EncodedKey::from_values(&req.prefix);
            index.scan_prefix_window(Some(&prefix), req.offset, req.limit)
        };

        Ok(pks
            .into_iter()
            .filter_map(|pk| table.rows.get(&pk).cloned())
            .collect())
    }

    pub async fn keyed_state_index_rank(
        &self,
        project_id: &str,
        scope_id: &str,
        keyed_state: &str,
        index_name: &str,
        key: Value,
        consistency: ConsistencyMode,
    ) -> Result<Option<usize>, AedbError> {
        let lease = self.acquire_snapshot(consistency).await?;
        let _ = keyed_state_schema(
            &lease.view.catalog,
            project_id,
            scope_id,
            keyed_state,
            "rank",
        )?;
        let Some(table) = lease.view.keyspace.table(project_id, scope_id, keyed_state) else {
            return Err(AedbError::Validation(format!(
                "table not found: {project_id}.{scope_id}.{keyed_state}"
            )));
        };
        let Some(index) = table.indexes.get(index_name) else {
            return Err(AedbError::Validation(format!(
                "index not found: {project_id}.{scope_id}.{keyed_state}.{index_name}"
            )));
        };

        let target_pk = EncodedKey::from_values(&[key]);
        Ok(index.rank_of_pk(&target_pk))
    }

    pub async fn processor_pull(
        &self,
        event_name: &str,
        processor_id: &str,
        max_count: usize,
    ) -> Result<ProcessorPullResult, AedbError> {
        if max_count == 0 {
            return Err(AedbError::Validation("max_count must be > 0".into()));
        }
        let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
        let checkpoint_pk =
            EncodedKey::from_values(&[Value::Text(processor_id.to_string().into())]);
        let checkpoint_seq = lease
            .view
            .keyspace
            .table(
                SYSTEM_PROJECT_ID,
                SYSTEM_SCOPE_ID,
                REACTIVE_PROCESSOR_CHECKPOINTS_TABLE,
            )
            .and_then(|table| table.rows.get(&checkpoint_pk))
            .and_then(|row| row.values.get(1))
            .and_then(|v| match v {
                Value::Integer(i) => u64::try_from(*i).ok(),
                _ => None,
            })
            .unwrap_or(0);
        let head_seq = lease.view.seq;

        let mut events = Vec::with_capacity(max_count);
        let start_seq = checkpoint_seq.saturating_add(1);
        if let Some(table) =
            lease
                .view
                .keyspace
                .table(SYSTEM_PROJECT_ID, SYSTEM_SCOPE_ID, EVENT_OUTBOX_TABLE)
        {
            let start_key = EncodedKey::from_values(&[
                Value::Integer(start_seq as i64),
                Value::Text("".into()),
                Value::Text("".into()),
            ]);
            for row in table
                .rows
                .range((Bound::Included(start_key), Bound::Unbounded))
                .map(|(_, row)| row)
            {
                if events.len() >= max_count {
                    break;
                }
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
                    continue;
                };
                let Ok(commit_seq) = u64::try_from(*commit_seq_i64) else {
                    continue;
                };
                if topic.as_str() != event_name {
                    continue;
                }
                let Ok(ts_micros) = u64::try_from(*ts_i64) else {
                    continue;
                };
                events.push(EventOutboxRecord {
                    commit_seq,
                    ts_micros,
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    topic: topic.to_string(),
                    event_key: event_key.to_string(),
                    payload_json: payload.to_string(),
                });
            }
        }
        let last_commit_seq = events
            .last()
            .map(|event| event.commit_seq)
            .unwrap_or(checkpoint_seq);
        Ok(ProcessorPullResult {
            events,
            checkpoint_seq,
            last_commit_seq,
            head_seq,
        })
    }

    pub async fn processor_commit(
        &self,
        processor_id: &str,
        checkpoint_seq: u64,
        mutations: Vec<Mutation>,
    ) -> Result<CommitResult, AedbError> {
        let envelope = self
            .build_processor_commit_envelope(processor_id, checkpoint_seq, mutations)
            .await?;
        self.commit_envelope_prevalidated_internal("processor_commit", envelope)
            .await
    }

    pub fn processor_context<'a>(
        &'a self,
        project_id: &str,
        scope_id: &str,
        processor_id: &str,
        source_event: &str,
    ) -> ProcessorContext<'a> {
        ProcessorContext {
            db: self,
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            processor_id: processor_id.to_string(),
            source_event: source_event.to_string(),
            pending: Vec::new(),
            checkpoint_seq: 0,
        }
    }

    async fn build_processor_commit_envelope(
        &self,
        processor_id: &str,
        checkpoint_seq: u64,
        mut mutations: Vec<Mutation>,
    ) -> Result<TransactionEnvelope, AedbError> {
        if processor_id.trim().is_empty() {
            return Err(AedbError::Validation("processor_id cannot be empty".into()));
        }
        if mutations.is_empty() && checkpoint_seq == 0 {
            return Err(AedbError::Validation(
                "processor_commit requires mutations or a checkpoint".into(),
            ));
        }

        let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
        let checkpoint_key = Value::Text(processor_id.to_string().into());
        let checkpoint_pk = EncodedKey::from_values(std::slice::from_ref(&checkpoint_key));
        let checkpoint_table = lease.view.keyspace.table(
            SYSTEM_PROJECT_ID,
            SYSTEM_SCOPE_ID,
            REACTIVE_PROCESSOR_CHECKPOINTS_TABLE,
        );
        let current_checkpoint = checkpoint_table
            .and_then(|table| table.rows.get(&checkpoint_pk))
            .and_then(|row| row.values.get(1))
            .and_then(|v| match v {
                Value::Integer(i) => u64::try_from(*i).ok(),
                _ => None,
            })
            .unwrap_or(0);
        let checkpoint_version = checkpoint_table
            .and_then(|table| table.row_versions.get(&checkpoint_pk))
            .copied()
            .unwrap_or(0);
        let head_seq = lease.view.seq;

        let bounded_checkpoint = checkpoint_seq.max(current_checkpoint);
        if bounded_checkpoint > head_seq {
            return Err(AedbError::Validation(format!(
                "checkpoint_seq {} exceeds current WAL head {}",
                bounded_checkpoint, head_seq
            )));
        }
        if mutations.is_empty() && bounded_checkpoint == current_checkpoint {
            return Err(AedbError::Validation(
                "processor_commit has no mutations and does not advance checkpoint".into(),
            ));
        }
        if bounded_checkpoint > current_checkpoint {
            mutations.push(Mutation::Upsert {
                project_id: SYSTEM_PROJECT_ID.to_string(),
                scope_id: SYSTEM_SCOPE_ID.to_string(),
                table_name: REACTIVE_PROCESSOR_CHECKPOINTS_TABLE.to_string(),
                primary_key: vec![checkpoint_key.clone()],
                row: Row::from_values(vec![
                    checkpoint_key.clone(),
                    Value::Integer(bounded_checkpoint as i64),
                    Value::Timestamp(crate::system_now_micros() as i64),
                ]),
            });
        }

        let primary_key = vec![checkpoint_key];
        let read_set = ReadSet {
            points: vec![ReadSetEntry {
                key: ReadKey::TableRow {
                    project_id: SYSTEM_PROJECT_ID.to_string(),
                    scope_id: SYSTEM_SCOPE_ID.to_string(),
                    table_name: REACTIVE_PROCESSOR_CHECKPOINTS_TABLE.to_string(),
                    primary_key,
                },
                version_at_read: checkpoint_version,
            }],
            ranges: Vec::new(),
        };

        Ok(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set,
            write_intent: WriteIntent { mutations },
            base_seq: head_seq,
        })
    }
}

fn batch_rejected_from_assertion(
    assertion: &ReadAssertion,
    actual: &AssertionActual,
) -> Option<BatchRejected> {
    match assertion {
        ReadAssertion::AccumulatorAvailableAtLeast {
            accumulator_name,
            min_amount,
            ..
        } => Some(BatchRejected {
            error_code: "available_below_min",
            failed_precondition: format!(
                "RequireAvailable({}, min={})",
                accumulator_name, min_amount
            ),
            actual_value: assertion_actual_i64(actual),
        }),
        ReadAssertion::AccumulatorExposureWithinMargin {
            accumulator_name,
            additional_exposure,
            ..
        } => Some(BatchRejected {
            error_code: "exposure_margin_exceeded",
            failed_precondition: format!(
                "RequireExposureOk({}, amount={})",
                accumulator_name, additional_exposure
            ),
            actual_value: assertion_actual_i64(actual),
        }),
        _ => None,
    }
}

fn assertion_actual_i64(actual: &AssertionActual) -> i64 {
    match actual {
        AssertionActual::Value(Value::Integer(v)) => *v,
        AssertionActual::Value(Value::Timestamp(v)) => *v,
        AssertionActual::Value(Value::U64(v)) => (*v).min(i64::MAX as u64) as i64,
        AssertionActual::Version(v) | AssertionActual::Count(v) => (*v).min(i64::MAX as u64) as i64,
        AssertionActual::Bool(v) => i64::from(*v),
        AssertionActual::Missing | AssertionActual::Bytes(_) | AssertionActual::Value(_) => 0,
    }
}

impl<'a> ProcessorContext<'a> {
    pub async fn pull(&mut self, max_count: usize) -> Result<Vec<EventOutboxRecord>, AedbError> {
        let batch = self
            .db
            .processor_pull(&self.source_event, &self.processor_id, max_count)
            .await?;
        self.checkpoint_seq = self.checkpoint_seq.max(batch.last_commit_seq);
        Ok(batch.events)
    }

    pub fn write(&mut self, keyed_state: &str, key: Value, value: Row) {
        self.pending.push(Mutation::Upsert {
            project_id: self.project_id.clone(),
            scope_id: self.scope_id.clone(),
            table_name: keyed_state.to_string(),
            primary_key: vec![key],
            row: value,
        });
    }

    pub async fn update<F>(
        &mut self,
        keyed_state: &str,
        key: Value,
        update_fn: F,
    ) -> Result<(), AedbError>
    where
        F: FnOnce(Option<Row>) -> Result<Option<Row>, AedbError>,
    {
        let current = match pending_keyed_state_row(
            &self.pending,
            &self.project_id,
            &self.scope_id,
            keyed_state,
            &key,
        ) {
            Some(row) => row,
            None => {
                self.db
                    .keyed_state_read(
                        &self.project_id,
                        &self.scope_id,
                        keyed_state,
                        key.clone(),
                        ConsistencyMode::AtLatest,
                    )
                    .await?
            }
        };
        match update_fn(current)? {
            Some(next) => self.write(keyed_state, key, next),
            None => self.delete(keyed_state, key),
        }
        Ok(())
    }

    pub fn delete(&mut self, keyed_state: &str, key: Value) {
        self.pending.push(Mutation::Delete {
            project_id: self.project_id.clone(),
            scope_id: self.scope_id.clone(),
            table_name: keyed_state.to_string(),
            primary_key: vec![key],
        });
    }

    pub async fn read(&self, keyed_state: &str, key: Value) -> Result<Option<Row>, AedbError> {
        self.db
            .keyed_state_read(
                &self.project_id,
                &self.scope_id,
                keyed_state,
                key,
                ConsistencyMode::AtLatest,
            )
            .await
    }

    pub async fn query_index(
        &self,
        keyed_state: &str,
        req: KeyedStateQueryRequest,
    ) -> Result<Vec<Row>, AedbError> {
        self.db
            .keyed_state_query_index(
                &self.project_id,
                &self.scope_id,
                keyed_state,
                req,
                ConsistencyMode::AtLatest,
            )
            .await
    }

    pub fn accumulate(&mut self, accumulator: &str, delta: i64, dedupe_id: String, order_key: u64) {
        self.pending.push(Mutation::Accumulate {
            project_id: self.project_id.clone(),
            scope_id: self.scope_id.clone(),
            accumulator_name: accumulator.to_string(),
            delta,
            dedupe_key: dedupe_id,
            order_key,
            release_exposure_id: None,
        });
    }

    pub async fn value(&self, accumulator: &str) -> Result<i64, AedbError> {
        self.db
            .accumulator_value(
                &self.project_id,
                &self.scope_id,
                accumulator,
                ConsistencyMode::AtLatest,
            )
            .await
    }

    pub fn expose(&mut self, accumulator: &str, amount: i64, dedupe_id: String) {
        self.pending.push(Mutation::ExposeAccumulator {
            project_id: self.project_id.clone(),
            scope_id: self.scope_id.clone(),
            accumulator_name: accumulator.to_string(),
            amount,
            exposure_id: dedupe_id,
        });
    }

    pub fn release_exposure(&mut self, accumulator: &str, dedupe_id: String) {
        self.pending.push(Mutation::ReleaseAccumulatorExposure {
            project_id: self.project_id.clone(),
            scope_id: self.scope_id.clone(),
            accumulator_name: accumulator.to_string(),
            exposure_id: dedupe_id,
        });
    }

    pub fn emit(&mut self, event_name: &str, event_key: String, data_json: String) {
        self.pending.push(Mutation::EmitEvent {
            project_id: self.project_id.clone(),
            scope_id: self.scope_id.clone(),
            topic: event_name.to_string(),
            event_key,
            payload_json: data_json,
        });
    }

    pub async fn commit(self) -> Result<CommitResult, AedbError> {
        let lease = self.db.acquire_snapshot(ConsistencyMode::AtLatest).await?;
        let mut keyed_state_pk_index_cache: HashMap<String, usize> = HashMap::new();
        for mutation in &self.pending {
            match mutation {
                Mutation::Upsert {
                    project_id,
                    scope_id,
                    table_name,
                    primary_key,
                    row,
                } if project_id == &self.project_id && scope_id == &self.scope_id => {
                    let pk_index = if let Some(pk_index) = keyed_state_pk_index_cache.get(table_name)
                    {
                        *pk_index
                    } else {
                        let schema = keyed_state_schema(
                            &lease.view.catalog,
                            project_id,
                            scope_id,
                            table_name,
                            "processor_commit",
                        )?;
                        let pk_index =
                            keyed_state_primary_key_index(schema, project_id, scope_id, table_name)?;
                        keyed_state_pk_index_cache.insert(table_name.clone(), pk_index);
                        pk_index
                    };
                    if primary_key.len() != 1 {
                        return Err(AedbError::Validation(format!(
                            "keyed_state processor_commit requires single-column key: {project_id}.{scope_id}.{table_name}"
                        )));
                    }
                    validate_keyed_state_row_matches_key(
                        pk_index,
                        project_id,
                        scope_id,
                        table_name,
                        &primary_key[0],
                        row,
                    )?;
                }
                Mutation::Delete {
                    project_id,
                    scope_id,
                    table_name,
                    primary_key,
                } if project_id == &self.project_id && scope_id == &self.scope_id => {
                    if !keyed_state_pk_index_cache.contains_key(table_name) {
                        let schema = keyed_state_schema(
                            &lease.view.catalog,
                            project_id,
                            scope_id,
                            table_name,
                            "processor_commit",
                        )?;
                        let pk_index =
                            keyed_state_primary_key_index(schema, project_id, scope_id, table_name)?;
                        keyed_state_pk_index_cache.insert(table_name.clone(), pk_index);
                    }
                    if primary_key.len() != 1 {
                        return Err(AedbError::Validation(format!(
                            "keyed_state processor_commit requires single-column key: {project_id}.{scope_id}.{table_name}"
                        )));
                    }
                }
                _ => {}
            }
        }
        self.db
            .processor_commit(&self.processor_id, self.checkpoint_seq, self.pending)
            .await
    }
}

fn keyed_state_schema<'a>(
    catalog: &'a Catalog,
    project_id: &str,
    scope_id: &str,
    keyed_state: &str,
    operation: &str,
) -> Result<&'a TableSchema, AedbError> {
    let ns = namespace_key(project_id, scope_id);
    let schema = catalog
        .tables
        .get(&(ns, keyed_state.to_string()))
        .ok_or_else(|| {
            AedbError::Validation(format!(
                "table not found: {project_id}.{scope_id}.{keyed_state}"
            ))
        })?;
    if schema.primary_key.len() != 1 {
        return Err(AedbError::Validation(format!(
            "keyed_state {operation} requires single-column primary key: {project_id}.{scope_id}.{keyed_state}"
        )));
    }
    Ok(schema)
}

fn keyed_state_snapshot_from_table(
    keyspace: &crate::storage::keyspace::KeyspaceSnapshot,
    snapshot_seq: u64,
    project_id: &str,
    scope_id: &str,
    keyed_state: &str,
    key: Value,
) -> KeyedStateSnapshot {
    let encoded = EncodedKey::from_values(&[key.clone()]);
    let table = keyspace.table(project_id, scope_id, keyed_state);
    let row = table.and_then(|table| table.rows.get(&encoded).cloned());
    let version = table
        .and_then(|table| table.row_versions.get(&encoded).copied())
        .unwrap_or(0);
    KeyedStateSnapshot {
        row,
        version,
        snapshot_seq,
    }
}

fn keyed_state_assertion(
    project_id: &str,
    scope_id: &str,
    keyed_state: &str,
    key: &Value,
    snapshot: &KeyedStateSnapshot,
) -> ReadAssertion {
    let primary_key = vec![key.clone()];
    if snapshot.row.is_some() {
        ReadAssertion::RowVersion {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            table_name: keyed_state.to_string(),
            primary_key,
            expected_seq: snapshot.version,
        }
    } else {
        ReadAssertion::RowExists {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            table_name: keyed_state.to_string(),
            primary_key,
            expected: false,
        }
    }
}

fn keyed_state_primary_key_index(
    schema: &TableSchema,
    project_id: &str,
    scope_id: &str,
    keyed_state: &str,
) -> Result<usize, AedbError> {
    let pk_name = schema
        .primary_key
        .first()
        .ok_or_else(|| AedbError::Validation(format!(
            "keyed_state write requires single-column primary key: {project_id}.{scope_id}.{keyed_state}"
        )))?;
    schema
        .columns
        .iter()
        .position(|column| column.name == *pk_name)
        .ok_or_else(|| {
            AedbError::Validation(format!(
                "primary key column missing: {project_id}.{scope_id}.{keyed_state}.{pk_name}"
            ))
        })
}

fn validate_keyed_state_row_matches_key(
    pk_index: usize,
    project_id: &str,
    scope_id: &str,
    keyed_state: &str,
    key: &Value,
    row: &Row,
) -> Result<(), AedbError> {
    let row_key = row.values.get(pk_index).ok_or_else(|| {
        AedbError::Validation(format!(
            "row missing keyed_state primary key column: {project_id}.{scope_id}.{keyed_state}"
        ))
    })?;
    if row_key != key {
        return Err(AedbError::Validation(format!(
            "keyed_state key does not match row primary key column: {project_id}.{scope_id}.{keyed_state}"
        )));
    }
    Ok(())
}

fn pending_keyed_state_row(
    pending: &[Mutation],
    project_id: &str,
    scope_id: &str,
    keyed_state: &str,
    key: &Value,
) -> Option<Option<Row>> {
    pending.iter().rev().find_map(|mutation| match mutation {
        Mutation::Upsert {
            project_id: mutation_project,
            scope_id: mutation_scope,
            table_name,
            primary_key,
            row,
        } if mutation_project == project_id
            && mutation_scope == scope_id
            && table_name == keyed_state
            && primary_key.len() == 1
            && primary_key[0] == *key =>
        {
            Some(Some(row.clone()))
        }
        Mutation::Delete {
            project_id: mutation_project,
            scope_id: mutation_scope,
            table_name,
            primary_key,
        } if mutation_project == project_id
            && mutation_scope == scope_id
            && table_name == keyed_state
            && primary_key.len() == 1
            && primary_key[0] == *key =>
        {
            Some(None)
        }
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::DdlOperation;
    use crate::catalog::schema::ColumnDef;
    use crate::catalog::types::ColumnType;
    use tempfile::tempdir;

    fn user_state_columns() -> Vec<ColumnDef> {
        vec![
            ColumnDef {
                name: "user_id".to_string(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "points".to_string(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ]
    }

    #[tokio::test(flavor = "current_thread")]
    async fn processor_commit_rejects_stale_checkpoint_row_version() {
        let dir = tempdir().expect("tempdir");
        let db = AedbInstance::open(Default::default(), dir.path()).expect("open db");

        db.create_project("arcana").await.expect("project");
        db.create_scope("arcana", "game").await.expect("scope");
        db.commit_ddl(DdlOperation::CreateTable {
            project_id: "arcana".into(),
            scope_id: "game".into(),
            table_name: "user_state".into(),
            owner_id: None,
            columns: user_state_columns(),
            primary_key: vec!["user_id".into()],
            if_not_exists: false,
        })
        .await
        .expect("create table");
        db.commit_ddl(DdlOperation::CreateScope {
            project_id: crate::catalog::SYSTEM_PROJECT_ID.into(),
            scope_id: SYSTEM_SCOPE_ID.into(),
            owner_id: None,
            if_not_exists: true,
        })
        .await
        .expect("create system scope");
        db.commit_ddl(DdlOperation::CreateTable {
            project_id: crate::catalog::SYSTEM_PROJECT_ID.into(),
            scope_id: SYSTEM_SCOPE_ID.into(),
            table_name: REACTIVE_PROCESSOR_CHECKPOINTS_TABLE.into(),
            owner_id: None,
            columns: vec![
                ColumnDef {
                    name: "processor_name".to_string(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
                ColumnDef {
                    name: "checkpoint_seq".to_string(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "updated_at".to_string(),
                    col_type: ColumnType::Timestamp,
                    nullable: false,
                },
            ],
            primary_key: vec!["processor_name".into()],
            if_not_exists: true,
        })
        .await
        .expect("create checkpoint table");

        let stale = db
            .build_processor_commit_envelope(
                "points_processor",
                0,
                vec![Mutation::Upsert {
                    project_id: "arcana".into(),
                    scope_id: "game".into(),
                    table_name: "user_state".into(),
                    primary_key: vec![Value::Text("u1".into())],
                    row: Row::from_values(vec![Value::Text("u1".into()), Value::Integer(5)]),
                }],
            )
            .await
            .expect("stale envelope");

        db.processor_commit("points_processor", 1, Vec::new())
            .await
            .expect("advance checkpoint");

        let err = db
            .commit_envelope_prevalidated_internal("processor_commit_test", stale)
            .await
            .expect_err("stale processor commit must fail");
        assert!(matches!(err, AedbError::Conflict(_)));

        let lag = db
            .reactive_processor_lag("points_processor", ConsistencyMode::AtLatest)
            .await
            .expect("lag");
        assert_eq!(lag.checkpoint_seq, 1);

        let row = db
            .keyed_state_read(
                "arcana",
                "game",
                "user_state",
                Value::Text("u1".into()),
                ConsistencyMode::AtLatest,
            )
            .await
            .expect("read row");
        assert!(
            row.is_none(),
            "stale processor commit must not apply its user-state mutation"
        );
    }
}
