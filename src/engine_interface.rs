use crate::catalog::types::{Row, Value};
use crate::catalog::{SYSTEM_PROJECT_ID, namespace_key};
use crate::commit::tx::{
    AssertionActual, ReadAssertion, ReadSet, TransactionEnvelope, WriteClass, WriteIntent,
};
use crate::commit::validation::Mutation;
use crate::error::AedbError;
use crate::query::plan::ConsistencyMode;
use crate::storage::encoded_key::EncodedKey;
use crate::{AedbInstance, CommitResult, EventOutboxRecord};
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
        let mut keyed_state_pk_len_cache: HashMap<String, usize> = HashMap::new();
        let ns = namespace_key(project_id, scope_id);
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
                    let pk_len = if let Some(v) = keyed_state_pk_len_cache.get(&keyed_state) {
                        *v
                    } else {
                        let schema = preflight_lease
                            .view
                            .catalog
                            .tables
                            .get(&(ns.clone(), keyed_state.clone()))
                            .ok_or_else(|| {
                                AedbError::Validation(format!(
                                    "table not found: {project_id}.{scope_id}.{keyed_state}"
                                ))
                            })?;
                        let v = schema.primary_key.len();
                        keyed_state_pk_len_cache.insert(keyed_state.clone(), v);
                        v
                    };
                    if pk_len != 1 {
                        return Err(AedbError::Validation(format!(
                            "keyed_state write requires single-column primary key: {project_id}.{scope_id}.{keyed_state}"
                        )));
                    }
                    mutations.push(Mutation::Upsert {
                        project_id: project_id.to_string(),
                        scope_id: scope_id.to_string(),
                        table_name: keyed_state,
                        primary_key: vec![key],
                        row: value,
                    });
                }
                EffectOperation::Delete { keyed_state, key } => {
                    let pk_len = if let Some(v) = keyed_state_pk_len_cache.get(&keyed_state) {
                        *v
                    } else {
                        let schema = preflight_lease
                            .view
                            .catalog
                            .tables
                            .get(&(ns.clone(), keyed_state.clone()))
                            .ok_or_else(|| {
                                AedbError::Validation(format!(
                                    "table not found: {project_id}.{scope_id}.{keyed_state}"
                                ))
                            })?;
                        let v = schema.primary_key.len();
                        keyed_state_pk_len_cache.insert(keyed_state.clone(), v);
                        v
                    };
                    if pk_len != 1 {
                        return Err(AedbError::Validation(format!(
                            "keyed_state delete requires single-column primary key: {project_id}.{scope_id}.{keyed_state}"
                        )));
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
        let ns = namespace_key(project_id, scope_id);
        let schema = lease
            .view
            .catalog
            .tables
            .get(&(ns, keyed_state.to_string()))
            .ok_or_else(|| {
                AedbError::Validation(format!(
                    "table not found: {project_id}.{scope_id}.{keyed_state}"
                ))
            })?;
        if schema.primary_key.len() != 1 {
            return Err(AedbError::Validation(format!(
                "keyed_state read requires single-column primary key: {project_id}.{scope_id}.{keyed_state}"
            )));
        }
        let Some(table) = lease.view.keyspace.table(project_id, scope_id, keyed_state) else {
            return Ok(None);
        };
        let encoded = EncodedKey::from_values(&[key]);
        Ok(table.rows.get(&encoded).cloned())
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
        let ns = namespace_key(project_id, scope_id);
        let schema = lease
            .view
            .catalog
            .tables
            .get(&(ns, keyed_state.to_string()))
            .ok_or_else(|| {
                AedbError::Validation(format!(
                    "table not found: {project_id}.{scope_id}.{keyed_state}"
                ))
            })?;
        let Some(col_idx) = schema.columns.iter().position(|c| c.name == field) else {
            return Err(AedbError::Validation(format!(
                "unknown field in keyed_state: {project_id}.{scope_id}.{keyed_state}.{field}"
            )));
        };
        if schema.primary_key.len() != 1 {
            return Err(AedbError::Validation(format!(
                "keyed_state read_field requires single-column primary key: {project_id}.{scope_id}.{keyed_state}"
            )));
        }
        let Some(table) = lease.view.keyspace.table(project_id, scope_id, keyed_state) else {
            return Ok(None);
        };
        let encoded = EncodedKey::from_values(&[key]);
        let Some(row) = table.rows.get(&encoded) else {
            return Ok(None);
        };
        Ok(row.values.get(col_idx).cloned())
    }

    pub async fn keyed_state_write(
        &self,
        project_id: &str,
        scope_id: &str,
        keyed_state: &str,
        key: Value,
        value: Row,
    ) -> Result<CommitResult, AedbError> {
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
        let current = self
            .keyed_state_read(
                project_id,
                scope_id,
                keyed_state,
                key.clone(),
                ConsistencyMode::AtLatest,
            )
            .await?;
        match update_fn(current)? {
            Some(next) => {
                self.keyed_state_write(project_id, scope_id, keyed_state, key, next)
                    .await
            }
            None => {
                self.keyed_state_delete(project_id, scope_id, keyed_state, key)
                    .await
            }
        }
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
        let ns = namespace_key(project_id, scope_id);
        let schema = lease
            .view
            .catalog
            .tables
            .get(&(ns, keyed_state.to_string()))
            .ok_or_else(|| {
                AedbError::Validation(format!(
                    "table not found: {project_id}.{scope_id}.{keyed_state}"
                ))
            })?;
        if schema.primary_key.len() != 1 {
            return Err(AedbError::Validation(format!(
                "keyed_state rank requires single-column primary key: {project_id}.{scope_id}.{keyed_state}"
            )));
        }
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
        mut mutations: Vec<Mutation>,
    ) -> Result<CommitResult, AedbError> {
        if processor_id.trim().is_empty() {
            return Err(AedbError::Validation("processor_id cannot be empty".into()));
        }
        if mutations.is_empty() && checkpoint_seq == 0 {
            return Err(AedbError::Validation(
                "processor_commit requires mutations or a checkpoint".into(),
            ));
        }

        let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
        let checkpoint_pk =
            EncodedKey::from_values(&[Value::Text(processor_id.to_string().into())]);
        let current_checkpoint = lease
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
                primary_key: vec![Value::Text(processor_id.to_string().into())],
                row: Row::from_values(vec![
                    Value::Text(processor_id.to_string().into()),
                    Value::Integer(bounded_checkpoint as i64),
                    Value::Timestamp(crate::system_now_micros() as i64),
                ]),
            });
        }

        self.commit_envelope_prevalidated_internal(
            "processor_commit",
            TransactionEnvelope {
                caller: None,
                idempotency_key: None,
                write_class: WriteClass::Standard,
                assertions: Vec::new(),
                read_set: ReadSet::default(),
                write_intent: WriteIntent { mutations },
                base_seq: head_seq,
            },
        )
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
        let current = self
            .db
            .keyed_state_read(
                &self.project_id,
                &self.scope_id,
                keyed_state,
                key.clone(),
                ConsistencyMode::AtLatest,
            )
            .await?;
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
        self.db
            .processor_commit(&self.processor_id, self.checkpoint_seq, self.pending)
            .await
    }
}
