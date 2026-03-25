use crate::catalog::Catalog;
use crate::commit::tx::{AssertionActual, ReadAssertion};
use crate::commit::tx::{
    PreflightPlan, ReadBound, ReadKey, ReadRange, ReadRangeEntry, ReadSet, ReadSetEntry,
    WriteIntent,
};
use crate::commit::validation::{
    KvIntegerAmount, KvIntegerMissingPolicy, KvIntegerUnderflowPolicy, KvU64MissingPolicy,
    KvU64MutatorOp, KvU64OverflowPolicy, KvU64UnderflowPolicy, KvU256MissingPolicy,
    KvU256MutatorOp, KvU256OverflowPolicy, KvU256UnderflowPolicy, Mutation, counter_shard_index,
    counter_shard_storage_key, validate_mutation_with_config,
};
use crate::config::AedbConfig;
use crate::error::AedbError;
use crate::storage::encoded_key::EncodedKey;
use crate::storage::keyspace::KeyspaceSnapshot;
use primitive_types::U256;
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreflightResult {
    Ok { affected_rows: u64 },
    Err { reason: String },
}

pub fn preflight(
    snapshot: &KeyspaceSnapshot,
    catalog: &Catalog,
    mutation: &Mutation,
) -> PreflightResult {
    preflight_with_config(snapshot, catalog, mutation, &AedbConfig::default())
}

pub fn preflight_with_config(
    snapshot: &KeyspaceSnapshot,
    catalog: &Catalog,
    mutation: &Mutation,
    config: &AedbConfig,
) -> PreflightResult {
    if let Err(e) = validate_mutation_with_config(catalog, mutation, config) {
        return PreflightResult::Err {
            reason: e.to_string(),
        };
    }

    match mutation {
        Mutation::Insert {
            project_id,
            scope_id,
            table_name,
            primary_key,
            ..
        } => {
            let exists = snapshot
                .table(project_id, scope_id, table_name)
                .and_then(|t| t.rows.get(&EncodedKey::from_values(primary_key)))
                .is_some();
            if exists {
                return PreflightResult::Err {
                    reason: AedbError::DuplicatePK {
                        table: table_name.clone(),
                        key: format!("{primary_key:?}"),
                    }
                    .to_string(),
                };
            }
            PreflightResult::Ok { affected_rows: 1 }
        }
        Mutation::InsertBatch {
            project_id,
            scope_id,
            table_name,
            rows,
        } => {
            let ns = crate::catalog::namespace_key(project_id, scope_id);
            let Some(schema) = catalog.tables.get(&(ns, table_name.clone())) else {
                return PreflightResult::Err {
                    reason: AedbError::Validation("table missing".into()).to_string(),
                };
            };
            let mut seen = HashSet::with_capacity(rows.len());
            for row in rows {
                let mut primary_key = Vec::with_capacity(schema.primary_key.len());
                for pk_name in &schema.primary_key {
                    let Some(idx) = schema.columns.iter().position(|c| c.name == *pk_name) else {
                        return PreflightResult::Err {
                            reason: AedbError::Validation(format!(
                                "primary key column missing: {pk_name}"
                            ))
                            .to_string(),
                        };
                    };
                    let Some(value) = row.values.get(idx) else {
                        return PreflightResult::Err {
                            reason: AedbError::Validation(format!(
                                "primary key column value missing from row: {pk_name}"
                            ))
                            .to_string(),
                        };
                    };
                    primary_key.push(value.clone());
                }
                let encoded = EncodedKey::from_values(&primary_key);
                if !seen.insert(encoded.clone())
                    || snapshot
                        .table(project_id, scope_id, table_name)
                        .and_then(|t| t.rows.get(&encoded))
                        .is_some()
                {
                    return PreflightResult::Err {
                        reason: AedbError::DuplicatePK {
                            table: table_name.clone(),
                            key: format!("{primary_key:?}"),
                        }
                        .to_string(),
                    };
                }
            }
            PreflightResult::Ok {
                affected_rows: rows.len() as u64,
            }
        }
        Mutation::UpsertBatch { rows, .. } | Mutation::UpsertBatchOnConflict { rows, .. } => {
            PreflightResult::Ok {
                affected_rows: rows.len() as u64,
            }
        }
        Mutation::DeleteWhere { limit, .. }
        | Mutation::UpdateWhere { limit, .. }
        | Mutation::UpdateWhereExpr { limit, .. } => PreflightResult::Ok {
            affected_rows: limit.unwrap_or(1) as u64,
        },
        Mutation::KvDecU256 {
            project_id,
            scope_id,
            key,
            amount_be,
        } => {
            let current = match snapshot.kv_get(project_id, scope_id, key) {
                Some(entry) => match decode_u256(&entry.value) {
                    Ok(value) => value,
                    Err(reason) => return PreflightResult::Err { reason },
                },
                None => U256::zero(),
            };
            let amount = U256::from_big_endian(amount_be);
            if current < amount {
                return PreflightResult::Err {
                    reason: AedbError::Underflow.to_string(),
                };
            }
            PreflightResult::Ok { affected_rows: 1 }
        }
        Mutation::KvIncU256 {
            project_id,
            scope_id,
            key,
            amount_be,
        } => {
            let current = match snapshot.kv_get(project_id, scope_id, key) {
                Some(entry) => match decode_u256(&entry.value) {
                    Ok(value) => value,
                    Err(reason) => return PreflightResult::Err { reason },
                },
                None => U256::zero(),
            };
            let amount = U256::from_big_endian(amount_be);
            if current.checked_add(amount).is_none() {
                return PreflightResult::Err {
                    reason: AedbError::Overflow.to_string(),
                };
            }
            PreflightResult::Ok { affected_rows: 1 }
        }
        Mutation::KvAddU256Ex {
            project_id,
            scope_id,
            key,
            amount_be,
            on_missing,
            on_overflow,
        } => {
            let current = match snapshot.kv_get(project_id, scope_id, key) {
                Some(entry) => match decode_u256(&entry.value) {
                    Ok(value) => value,
                    Err(reason) => return PreflightResult::Err { reason },
                },
                None => match on_missing {
                    KvU256MissingPolicy::TreatAsZero => U256::zero(),
                    KvU256MissingPolicy::Reject => {
                        return PreflightResult::Err {
                            reason: AedbError::Validation(
                                "u256 key missing and policy is Reject".into(),
                            )
                            .to_string(),
                        };
                    }
                },
            };
            let amount = U256::from_big_endian(amount_be);
            if current.checked_add(amount).is_none()
                && matches!(on_overflow, KvU256OverflowPolicy::Reject)
            {
                return PreflightResult::Err {
                    reason: AedbError::Overflow.to_string(),
                };
            }
            PreflightResult::Ok { affected_rows: 1 }
        }
        Mutation::KvSubU256Ex {
            project_id,
            scope_id,
            key,
            amount_be,
            on_missing,
            on_underflow,
        } => {
            let current = match snapshot.kv_get(project_id, scope_id, key) {
                Some(entry) => match decode_u256(&entry.value) {
                    Ok(value) => value,
                    Err(reason) => return PreflightResult::Err { reason },
                },
                None => match on_missing {
                    KvU256MissingPolicy::TreatAsZero => U256::zero(),
                    KvU256MissingPolicy::Reject => {
                        return PreflightResult::Err {
                            reason: AedbError::Validation(
                                "u256 key missing and policy is Reject".into(),
                            )
                            .to_string(),
                        };
                    }
                },
            };
            let amount = U256::from_big_endian(amount_be);
            if current < amount && matches!(on_underflow, KvU256UnderflowPolicy::Reject) {
                return PreflightResult::Err {
                    reason: AedbError::Underflow.to_string(),
                };
            }
            PreflightResult::Ok { affected_rows: 1 }
        }
        Mutation::KvMaxU256 {
            project_id,
            scope_id,
            key,
            on_missing,
            ..
        }
        | Mutation::KvMinU256 {
            project_id,
            scope_id,
            key,
            on_missing,
            ..
        } => {
            if snapshot.kv_get(project_id, scope_id, key).is_none()
                && matches!(on_missing, KvU256MissingPolicy::Reject)
            {
                return PreflightResult::Err {
                    reason: AedbError::Validation("u256 key missing and policy is Reject".into())
                        .to_string(),
                };
            }
            PreflightResult::Ok { affected_rows: 1 }
        }
        Mutation::KvMutateU256 {
            project_id,
            scope_id,
            key,
            op,
            operand_be,
            expected_seq,
        } => {
            let current_entry = snapshot.kv_get(project_id, scope_id, key);
            let current_version = current_entry.map(|entry| entry.version).unwrap_or(0);
            if let Some(expected_seq) = expected_seq
                && current_version != *expected_seq
            {
                return PreflightResult::Err {
                    reason: AedbError::AssertionFailed {
                        index: 0,
                        assertion: Box::new(ReadAssertion::KeyVersion {
                            project_id: project_id.clone(),
                            scope_id: scope_id.clone(),
                            key: key.clone(),
                            expected_seq: *expected_seq,
                        }),
                        actual: Box::new(AssertionActual::Version(current_version)),
                    }
                    .to_string(),
                };
            }
            let current_value = match current_entry {
                Some(entry) => match decode_u256(&entry.value) {
                    Ok(value) => value,
                    Err(reason) => return PreflightResult::Err { reason },
                },
                None => U256::zero(),
            };
            let operand = U256::from_big_endian(operand_be);
            match op {
                KvU256MutatorOp::Set => {}
                KvU256MutatorOp::Add if current_value.checked_add(operand).is_none() => {
                    return PreflightResult::Err {
                        reason: AedbError::Overflow.to_string(),
                    };
                }
                KvU256MutatorOp::Sub if current_value < operand => {
                    return PreflightResult::Err {
                        reason: AedbError::Underflow.to_string(),
                    };
                }
                KvU256MutatorOp::Add | KvU256MutatorOp::Sub => {}
            }
            PreflightResult::Ok { affected_rows: 1 }
        }
        Mutation::KvAddU64Ex {
            project_id,
            scope_id,
            key,
            amount_be,
            on_missing,
            on_overflow,
        } => {
            let current = match snapshot.kv_get(project_id, scope_id, key) {
                Some(entry) => match decode_u64(&entry.value) {
                    Ok(value) => value,
                    Err(reason) => return PreflightResult::Err { reason },
                },
                None => match on_missing {
                    KvU64MissingPolicy::TreatAsZero => 0u64,
                    KvU64MissingPolicy::Reject => {
                        return PreflightResult::Err {
                            reason: AedbError::Validation(
                                "u64 key missing and policy is Reject".into(),
                            )
                            .to_string(),
                        };
                    }
                },
            };
            let amount = u64::from_be_bytes(*amount_be);
            if current.checked_add(amount).is_none()
                && matches!(on_overflow, KvU64OverflowPolicy::Reject)
            {
                return PreflightResult::Err {
                    reason: AedbError::Overflow.to_string(),
                };
            }
            PreflightResult::Ok { affected_rows: 1 }
        }
        Mutation::KvSubU64Ex {
            project_id,
            scope_id,
            key,
            amount_be,
            on_missing,
            on_underflow,
        } => {
            let current = match snapshot.kv_get(project_id, scope_id, key) {
                Some(entry) => match decode_u64(&entry.value) {
                    Ok(value) => value,
                    Err(reason) => return PreflightResult::Err { reason },
                },
                None => match on_missing {
                    KvU64MissingPolicy::TreatAsZero => 0u64,
                    KvU64MissingPolicy::Reject => {
                        return PreflightResult::Err {
                            reason: AedbError::Validation(
                                "u64 key missing and policy is Reject".into(),
                            )
                            .to_string(),
                        };
                    }
                },
            };
            let amount = u64::from_be_bytes(*amount_be);
            if current < amount && matches!(on_underflow, KvU64UnderflowPolicy::Reject) {
                return PreflightResult::Err {
                    reason: AedbError::Underflow.to_string(),
                };
            }
            PreflightResult::Ok { affected_rows: 1 }
        }
        Mutation::KvSubIntEx {
            project_id,
            scope_id,
            key,
            amount,
            on_missing,
            on_underflow,
        } => preflight_kv_sub_int_ex(
            snapshot,
            project_id,
            scope_id,
            key,
            amount,
            *on_missing,
            *on_underflow,
        ),
        Mutation::CounterAdd {
            project_id,
            scope_id,
            key,
            amount_be,
            shard_count,
            shard_hint,
        } => {
            let shard = counter_shard_index(*shard_hint, *shard_count);
            let shard_key = counter_shard_storage_key(key, shard);
            let current = match snapshot.kv_get(project_id, scope_id, &shard_key) {
                Some(entry) => match decode_u64(&entry.value) {
                    Ok(value) => value,
                    Err(reason) => return PreflightResult::Err { reason },
                },
                None => 0u64,
            };
            let amount = u64::from_be_bytes(*amount_be);
            if current.checked_add(amount).is_none() {
                return PreflightResult::Err {
                    reason: AedbError::Overflow.to_string(),
                };
            }
            PreflightResult::Ok { affected_rows: 1 }
        }
        Mutation::KvMaxU64 {
            project_id,
            scope_id,
            key,
            on_missing,
            ..
        }
        | Mutation::KvMinU64 {
            project_id,
            scope_id,
            key,
            on_missing,
            ..
        } => {
            if snapshot.kv_get(project_id, scope_id, key).is_none()
                && matches!(on_missing, KvU64MissingPolicy::Reject)
            {
                return PreflightResult::Err {
                    reason: AedbError::Validation("u64 key missing and policy is Reject".into())
                        .to_string(),
                };
            }
            PreflightResult::Ok { affected_rows: 1 }
        }
        Mutation::KvMutateU64 {
            project_id,
            scope_id,
            key,
            op,
            operand_be,
            expected_seq,
        } => {
            let current_entry = snapshot.kv_get(project_id, scope_id, key);
            let current_version = current_entry.map(|entry| entry.version).unwrap_or(0);
            if let Some(expected_seq) = expected_seq
                && current_version != *expected_seq
            {
                return PreflightResult::Err {
                    reason: AedbError::AssertionFailed {
                        index: 0,
                        assertion: Box::new(ReadAssertion::KeyVersion {
                            project_id: project_id.clone(),
                            scope_id: scope_id.clone(),
                            key: key.clone(),
                            expected_seq: *expected_seq,
                        }),
                        actual: Box::new(AssertionActual::Version(current_version)),
                    }
                    .to_string(),
                };
            }
            let current_value = match current_entry {
                Some(entry) => match decode_u64(&entry.value) {
                    Ok(value) => value,
                    Err(reason) => return PreflightResult::Err { reason },
                },
                None => 0u64,
            };
            let operand = u64::from_be_bytes(*operand_be);
            match op {
                KvU64MutatorOp::Set => {}
                KvU64MutatorOp::Add if current_value.checked_add(operand).is_none() => {
                    return PreflightResult::Err {
                        reason: AedbError::Overflow.to_string(),
                    };
                }
                KvU64MutatorOp::Sub if current_value < operand => {
                    return PreflightResult::Err {
                        reason: AedbError::Underflow.to_string(),
                    };
                }
                KvU64MutatorOp::Add | KvU64MutatorOp::Sub => {}
            }
            PreflightResult::Ok { affected_rows: 1 }
        }
        Mutation::TableIncU256 {
            project_id,
            scope_id,
            table_name,
            primary_key,
            column,
            amount_be,
        } => {
            let current = match load_table_u256_field(
                snapshot,
                catalog,
                project_id,
                scope_id,
                table_name,
                primary_key,
                column,
            ) {
                Ok(value) => value,
                Err(reason) => return PreflightResult::Err { reason },
            };
            let amount = U256::from_big_endian(amount_be);
            if current.checked_add(amount).is_none() {
                return PreflightResult::Err {
                    reason: AedbError::Overflow.to_string(),
                };
            }
            PreflightResult::Ok { affected_rows: 1 }
        }
        Mutation::TableDecU256 {
            project_id,
            scope_id,
            table_name,
            primary_key,
            column,
            amount_be,
        } => {
            let current = match load_table_u256_field(
                snapshot,
                catalog,
                project_id,
                scope_id,
                table_name,
                primary_key,
                column,
            ) {
                Ok(value) => value,
                Err(reason) => return PreflightResult::Err { reason },
            };
            let amount = U256::from_big_endian(amount_be);
            if current < amount {
                return PreflightResult::Err {
                    reason: AedbError::Underflow.to_string(),
                };
            }
            PreflightResult::Ok { affected_rows: 1 }
        }
        Mutation::Accumulate { .. } => PreflightResult::Ok { affected_rows: 1 },
        Mutation::ExposeAccumulator {
            project_id,
            scope_id,
            accumulator_name,
            amount,
            exposure_id,
        } => {
            let Some(acc) = snapshot.accumulator(project_id, scope_id, accumulator_name) else {
                return PreflightResult::Err {
                    reason: AedbError::Validation(format!(
                        "accumulator does not exist: {project_id}.{scope_id}.{accumulator_name}"
                    ))
                    .to_string(),
                };
            };
            if let Some(existing) = acc.open_exposures.get(exposure_id.as_str()) {
                if existing.amount == *amount {
                    return PreflightResult::Ok { affected_rows: 1 };
                }
                return PreflightResult::Err {
                    reason: AedbError::Validation(format!(
                        "exposure id already exists with different amount: {exposure_id}"
                    ))
                    .to_string(),
                };
            }
            let Some(new_total) = acc.total_exposure.checked_add(*amount) else {
                return PreflightResult::Err {
                    reason: AedbError::Overflow.to_string(),
                };
            };
            let effective_value = match snapshot.accumulator_effective_value(
                project_id,
                scope_id,
                accumulator_name,
            ) {
                Ok(Some(value)) => value,
                Ok(None) => {
                    return PreflightResult::Err {
                        reason: AedbError::Validation(format!(
                            "accumulator does not exist: {project_id}.{scope_id}.{accumulator_name}"
                        ))
                        .to_string(),
                    };
                }
                Err(err) => {
                    return PreflightResult::Err {
                        reason: err.to_string(),
                    };
                }
            };
            let max_exposure_allowed =
                (((effective_value as i128) * (10_000i128 - acc.exposure_margin_bps as i128))
                    / 10_000)
                    .clamp(i64::MIN as i128, i64::MAX as i128) as i64;
            if new_total > max_exposure_allowed {
                return PreflightResult::Err {
                    reason: AedbError::Validation(format!(
                        "exposure margin exceeded: requested_total={new_total}, allowed={max_exposure_allowed}"
                    ))
                    .to_string(),
                };
            }
            PreflightResult::Ok { affected_rows: 1 }
        }
        Mutation::ExposeAccumulatorBatch {
            project_id,
            scope_id,
            accumulator_name,
            exposures,
        } => {
            let Some(acc) = snapshot.accumulator(project_id, scope_id, accumulator_name) else {
                return PreflightResult::Err {
                    reason: AedbError::Validation(format!(
                        "accumulator does not exist: {project_id}.{scope_id}.{accumulator_name}"
                    ))
                    .to_string(),
                };
            };
            let mut running_total = acc.total_exposure;
            let effective_value = match snapshot.accumulator_effective_value(
                project_id,
                scope_id,
                accumulator_name,
            ) {
                Ok(Some(value)) => value,
                Ok(None) => {
                    return PreflightResult::Err {
                        reason: AedbError::Validation(format!(
                            "accumulator does not exist: {project_id}.{scope_id}.{accumulator_name}"
                        ))
                        .to_string(),
                    };
                }
                Err(err) => {
                    return PreflightResult::Err {
                        reason: err.to_string(),
                    };
                }
            };
            let max_exposure_allowed =
                (((effective_value as i128) * (10_000i128 - acc.exposure_margin_bps as i128))
                    / 10_000)
                    .clamp(i64::MIN as i128, i64::MAX as i128) as i64;
            let mut seen = std::collections::HashMap::new();
            for (amount, exposure_id) in exposures {
                if let Some(existing) = acc.open_exposures.get(exposure_id.as_str()) {
                    if existing.amount != *amount {
                        return PreflightResult::Err {
                            reason: AedbError::Validation(format!(
                                "exposure id already exists with different amount: {exposure_id}"
                            ))
                            .to_string(),
                        };
                    }
                    continue;
                }
                if let Some(seen_amount) = seen.get(exposure_id.as_str()) {
                    if *seen_amount != *amount {
                        return PreflightResult::Err {
                            reason: AedbError::Validation(format!(
                                "duplicate exposure id with different amount in batch: {exposure_id}"
                            ))
                            .to_string(),
                        };
                    }
                    continue;
                }
                let Some(next) = running_total.checked_add(*amount) else {
                    return PreflightResult::Err {
                        reason: AedbError::Overflow.to_string(),
                    };
                };
                running_total = next;
                seen.insert(exposure_id.as_str(), *amount);
                if running_total > max_exposure_allowed {
                    return PreflightResult::Err {
                        reason: AedbError::Validation(format!(
                            "exposure margin exceeded: requested_total={running_total}, allowed={max_exposure_allowed}"
                        ))
                        .to_string(),
                    };
                }
            }
            PreflightResult::Ok {
                affected_rows: exposures.len() as u64,
            }
        }
        Mutation::EmitEvent { .. } => PreflightResult::Ok { affected_rows: 1 },
        _ => PreflightResult::Ok { affected_rows: 1 },
    }
}

pub fn preflight_plan(
    snapshot: &KeyspaceSnapshot,
    catalog: &Catalog,
    mutation: &Mutation,
    base_seq: u64,
) -> PreflightPlan {
    preflight_plan_with_config(snapshot, catalog, mutation, base_seq, &AedbConfig::default())
}

pub fn preflight_plan_with_config(
    snapshot: &KeyspaceSnapshot,
    catalog: &Catalog,
    mutation: &Mutation,
    base_seq: u64,
    config: &AedbConfig,
) -> PreflightPlan {
    let mut errors = Vec::new();
    if let Err(e) = validate_mutation_with_config(catalog, mutation, config) {
        errors.push(e.to_string());
    }
    let estimated_affected_rows = match mutation {
        Mutation::InsertBatch { rows, .. }
        | Mutation::UpsertBatch { rows, .. }
        | Mutation::UpsertBatchOnConflict { rows, .. } => rows.len(),
        Mutation::ExposeAccumulatorBatch { exposures, .. } => exposures.len(),
        Mutation::DeleteWhere { limit, .. }
        | Mutation::UpdateWhere { limit, .. }
        | Mutation::UpdateWhereExpr { limit, .. } => limit.unwrap_or(1),
        _ => 1,
    };
    let mut read_set = ReadSet::default();
    match mutation {
        Mutation::Insert {
            project_id,
            scope_id,
            table_name,
            primary_key,
            ..
        }
        | Mutation::Upsert {
            project_id,
            scope_id,
            table_name,
            primary_key,
            ..
        }
        | Mutation::Delete {
            project_id,
            scope_id,
            table_name,
            primary_key,
        } => {
            let version = snapshot
                .table(project_id, scope_id, table_name)
                .and_then(|t| {
                    t.row_versions
                        .get(&EncodedKey::from_values(primary_key))
                        .copied()
                })
                .unwrap_or(0);
            read_set.points.push(ReadSetEntry {
                key: ReadKey::TableRow {
                    project_id: project_id.clone(),
                    scope_id: scope_id.clone(),
                    table_name: table_name.clone(),
                    primary_key: primary_key.clone(),
                },
                version_at_read: version,
            });
        }
        Mutation::InsertBatch {
            project_id,
            scope_id,
            table_name,
            rows,
        }
        | Mutation::UpsertBatch {
            project_id,
            scope_id,
            table_name,
            rows,
        } => {
            let ns = crate::catalog::namespace_key(project_id, scope_id);
            if let Some(schema) = catalog.tables.get(&(ns, table_name.clone())) {
                for row in rows {
                    let mut primary_key = Vec::with_capacity(schema.primary_key.len());
                    for pk_name in &schema.primary_key {
                        if let Some(idx) = schema.columns.iter().position(|c| c.name == *pk_name)
                            && let Some(value) = row.values.get(idx)
                        {
                            primary_key.push(value.clone());
                        }
                    }
                    if primary_key.len() != schema.primary_key.len() {
                        continue;
                    }
                    let version = snapshot
                        .table(project_id, scope_id, table_name)
                        .and_then(|t| {
                            t.row_versions
                                .get(&EncodedKey::from_values(&primary_key))
                                .copied()
                        })
                        .unwrap_or(0);
                    read_set.points.push(ReadSetEntry {
                        key: ReadKey::TableRow {
                            project_id: project_id.clone(),
                            scope_id: scope_id.clone(),
                            table_name: table_name.clone(),
                            primary_key,
                        },
                        version_at_read: version,
                    });
                }
            }
        }
        Mutation::UpsertOnConflict { .. } => {
            // Conflict checks for upsert-on-conflict are performed in apply.
        }
        Mutation::UpsertBatchOnConflict { .. } => {
            // Conflict checks for batched upsert-on-conflict are performed in apply.
        }
        Mutation::DeleteWhere {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::UpdateWhere {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::UpdateWhereExpr {
            project_id,
            scope_id,
            table_name,
            ..
        } => {
            let (max_version, structural_version) = snapshot
                .table(project_id, scope_id, table_name)
                .map(|t| {
                    (
                        t.row_versions.values().copied().max().unwrap_or(0),
                        t.structural_version,
                    )
                })
                .unwrap_or((0, 0));
            read_set.ranges.push(ReadRangeEntry {
                range: ReadRange::TableRange {
                    project_id: project_id.clone(),
                    scope_id: scope_id.clone(),
                    table_name: table_name.clone(),
                    start: ReadBound::Unbounded,
                    end: ReadBound::Unbounded,
                },
                max_version_at_read: max_version,
                structural_version_at_read: structural_version,
            });
        }
        Mutation::KvSet {
            project_id,
            scope_id,
            key,
            ..
        }
        | Mutation::KvDel {
            project_id,
            scope_id,
            key,
        } => {
            let version = snapshot
                .kv_get(project_id, scope_id, key)
                .map(|e| e.version)
                .unwrap_or(0);
            read_set.points.push(ReadSetEntry {
                key: ReadKey::KvKey {
                    project_id: project_id.clone(),
                    scope_id: scope_id.clone(),
                    key: key.clone(),
                },
                version_at_read: version,
            });
        }
        Mutation::KvIncU256 { .. }
        | Mutation::KvAddU256Ex { .. }
        | Mutation::KvAddU64Ex { .. }
        | Mutation::CounterAdd { .. } => {
            // Commutative increment executes against current apply-time value.
        }
        Mutation::KvMutateU256 {
            project_id,
            scope_id,
            key,
            op,
            expected_seq,
            ..
        } => {
            if expected_seq.is_some() || matches!(op, KvU256MutatorOp::Sub) {
                let version = snapshot
                    .kv_get(project_id, scope_id, key)
                    .map(|e| e.version)
                    .unwrap_or(0);
                read_set.points.push(ReadSetEntry {
                    key: ReadKey::KvKey {
                        project_id: project_id.clone(),
                        scope_id: scope_id.clone(),
                        key: key.clone(),
                    },
                    version_at_read: version,
                });
            }
        }
        Mutation::KvDecU256 {
            project_id,
            scope_id,
            key,
            ..
        }
        | Mutation::KvSubU256Ex {
            project_id,
            scope_id,
            key,
            on_underflow: KvU256UnderflowPolicy::Reject,
            ..
        } => {
            let version = snapshot
                .kv_get(project_id, scope_id, key)
                .map(|e| e.version)
                .unwrap_or(0);
            read_set.points.push(ReadSetEntry {
                key: ReadKey::KvKey {
                    project_id: project_id.clone(),
                    scope_id: scope_id.clone(),
                    key: key.clone(),
                },
                version_at_read: version,
            });
        }
        Mutation::KvSubU256Ex { .. } => {}
        Mutation::KvMaxU256 { .. } => {}
        Mutation::KvMinU256 { .. } => {}
        Mutation::KvSubU64Ex {
            project_id,
            scope_id,
            key,
            on_underflow: KvU64UnderflowPolicy::Reject,
            ..
        } => {
            let version = snapshot
                .kv_get(project_id, scope_id, key)
                .map(|e| e.version)
                .unwrap_or(0);
            read_set.points.push(ReadSetEntry {
                key: ReadKey::KvKey {
                    project_id: project_id.clone(),
                    scope_id: scope_id.clone(),
                    key: key.clone(),
                },
                version_at_read: version,
            });
        }
        Mutation::KvSubU64Ex { .. } => {}
        Mutation::KvMutateU64 {
            project_id,
            scope_id,
            key,
            op,
            expected_seq,
            ..
        } => {
            if expected_seq.is_some() || matches!(op, KvU64MutatorOp::Sub) {
                let version = snapshot
                    .kv_get(project_id, scope_id, key)
                    .map(|e| e.version)
                    .unwrap_or(0);
                read_set.points.push(ReadSetEntry {
                    key: ReadKey::KvKey {
                        project_id: project_id.clone(),
                        scope_id: scope_id.clone(),
                        key: key.clone(),
                    },
                    version_at_read: version,
                });
            }
        }
        Mutation::KvSubIntEx {
            project_id,
            scope_id,
            key,
            on_underflow: KvIntegerUnderflowPolicy::Reject,
            ..
        } => {
            let version = snapshot
                .kv_get(project_id, scope_id, key)
                .map(|e| e.version)
                .unwrap_or(0);
            read_set.points.push(ReadSetEntry {
                key: ReadKey::KvKey {
                    project_id: project_id.clone(),
                    scope_id: scope_id.clone(),
                    key: key.clone(),
                },
                version_at_read: version,
            });
        }
        Mutation::KvSubIntEx { .. } => {}
        Mutation::KvMaxU64 { .. } => {}
        Mutation::KvMinU64 { .. } => {}
        Mutation::TableIncU256 {
            project_id,
            scope_id,
            table_name,
            primary_key,
            ..
        }
        | Mutation::TableDecU256 {
            project_id,
            scope_id,
            table_name,
            primary_key,
            ..
        } => {
            let version = snapshot
                .table(project_id, scope_id, table_name)
                .and_then(|t| {
                    t.row_versions
                        .get(&EncodedKey::from_values(primary_key))
                        .copied()
                })
                .unwrap_or(0);
            read_set.points.push(ReadSetEntry {
                key: ReadKey::TableRow {
                    project_id: project_id.clone(),
                    scope_id: scope_id.clone(),
                    table_name: table_name.clone(),
                    primary_key: primary_key.clone(),
                },
                version_at_read: version,
            });
        }
        Mutation::Accumulate { .. } => {}
        Mutation::ExposeAccumulator { .. } => {}
        Mutation::ExposeAccumulatorBatch { .. } => {}
        Mutation::ReleaseAccumulatorExposure { .. } => {}
        Mutation::EmitEvent { .. } => {}
        Mutation::OrderBookNew {
            project_id,
            scope_id,
            request,
        } => {
            let version = snapshot
                .kv_get(project_id, scope_id, request.instrument.as_bytes())
                .map(|e| e.version)
                .unwrap_or(0);
            read_set.points.push(ReadSetEntry {
                key: ReadKey::KvKey {
                    project_id: project_id.clone(),
                    scope_id: scope_id.clone(),
                    key: request.instrument.clone().into_bytes(),
                },
                version_at_read: version,
            });
        }
        Mutation::OrderBookCancel {
            project_id,
            scope_id,
            instrument,
            order_id,
            ..
        }
        | Mutation::OrderBookCancelReplace {
            project_id,
            scope_id,
            instrument,
            order_id,
            ..
        }
        | Mutation::OrderBookReduce {
            project_id,
            scope_id,
            instrument,
            order_id,
            ..
        } => {
            let mut key = format!("ob:{instrument}:ord:").into_bytes();
            key.extend_from_slice(&order_id.to_be_bytes());
            let version = snapshot
                .kv_get(project_id, scope_id, &key)
                .map(|e| e.version)
                .unwrap_or(0);
            read_set.points.push(ReadSetEntry {
                key: ReadKey::KvKey {
                    project_id: project_id.clone(),
                    scope_id: scope_id.clone(),
                    key,
                },
                version_at_read: version,
            });
        }
        Mutation::OrderBookMassCancel {
            project_id,
            scope_id,
            instrument,
            ..
        }
        | Mutation::OrderBookMatch {
            project_id,
            scope_id,
            instrument,
            ..
        } => {
            let prefix = format!("ob:{instrument}:").into_bytes();
            let start = ReadBound::Included(prefix.clone());
            let mut end = prefix.clone();
            end.push(0xff);
            let max_version =
                snapshot_max_kv_version_for_prefix(snapshot, project_id, scope_id, &prefix);
            let structural_version = snapshot_kv_structural_version(snapshot, project_id, scope_id);
            read_set.ranges.push(ReadRangeEntry {
                range: ReadRange::KvRange {
                    project_id: project_id.clone(),
                    scope_id: scope_id.clone(),
                    start,
                    end: ReadBound::Excluded(end),
                },
                max_version_at_read: max_version,
                structural_version_at_read: structural_version,
            });
        }
        Mutation::OrderBookDefineTable {
            project_id,
            scope_id,
            table_id,
            ..
        }
        | Mutation::OrderBookDropTable {
            project_id,
            scope_id,
            table_id,
        } => {
            let key = crate::order_book::key_order_book_table_spec(table_id);
            let version = snapshot
                .kv_get(project_id, scope_id, &key)
                .map(|e| e.version)
                .unwrap_or(0);
            read_set.points.push(ReadSetEntry {
                key: ReadKey::KvKey {
                    project_id: project_id.clone(),
                    scope_id: scope_id.clone(),
                    key,
                },
                version_at_read: version,
            });
        }
        Mutation::OrderBookSetInstrumentConfig {
            project_id,
            scope_id,
            instrument,
            ..
        }
        | Mutation::OrderBookSetInstrumentHalted {
            project_id,
            scope_id,
            instrument,
            ..
        } => {
            let key = crate::order_book::key_instrument_config(instrument);
            let version = snapshot
                .kv_get(project_id, scope_id, &key)
                .map(|e| e.version)
                .unwrap_or(0);
            read_set.points.push(ReadSetEntry {
                key: ReadKey::KvKey {
                    project_id: project_id.clone(),
                    scope_id: scope_id.clone(),
                    key,
                },
                version_at_read: version,
            });
        }
        Mutation::Ddl(_) => {}
    }

    PreflightPlan {
        valid: errors.is_empty(),
        read_set,
        write_intent: WriteIntent {
            mutations: vec![mutation.clone()],
        },
        base_seq,
        estimated_affected_rows,
        errors,
    }
}

fn decode_u256(bytes: &[u8]) -> Result<U256, String> {
    if bytes.len() != 32 {
        return Err(AedbError::Validation("invalid u256 bytes length".into()).to_string());
    }
    Ok(U256::from_big_endian(bytes))
}

fn preflight_kv_sub_int_ex(
    snapshot: &KeyspaceSnapshot,
    project_id: &str,
    scope_id: &str,
    key: &[u8],
    amount: &KvIntegerAmount,
    on_missing: KvIntegerMissingPolicy,
    on_underflow: KvIntegerUnderflowPolicy,
) -> PreflightResult {
    match amount {
        KvIntegerAmount::U64(amount_be) => {
            let current = match snapshot.kv_get(project_id, scope_id, key) {
                Some(entry) => match decode_u64(&entry.value) {
                    Ok(value) => value,
                    Err(reason) => return PreflightResult::Err { reason },
                },
                None => match on_missing {
                    KvIntegerMissingPolicy::TreatAsZero => 0u64,
                    KvIntegerMissingPolicy::Reject => {
                        return PreflightResult::Err {
                            reason: AedbError::Validation(
                                "u64 key missing and policy is Reject".into(),
                            )
                            .to_string(),
                        };
                    }
                },
            };
            let amount = u64::from_be_bytes(*amount_be);
            if current < amount && matches!(on_underflow, KvIntegerUnderflowPolicy::Reject) {
                return PreflightResult::Err {
                    reason: AedbError::Underflow.to_string(),
                };
            }
            PreflightResult::Ok { affected_rows: 1 }
        }
        KvIntegerAmount::U256(amount_be) => {
            let current = match snapshot.kv_get(project_id, scope_id, key) {
                Some(entry) => match decode_u256(&entry.value) {
                    Ok(value) => value,
                    Err(reason) => return PreflightResult::Err { reason },
                },
                None => match on_missing {
                    KvIntegerMissingPolicy::TreatAsZero => U256::zero(),
                    KvIntegerMissingPolicy::Reject => {
                        return PreflightResult::Err {
                            reason: AedbError::Validation(
                                "u256 key missing and policy is Reject".into(),
                            )
                            .to_string(),
                        };
                    }
                },
            };
            let amount = U256::from_big_endian(amount_be);
            if current < amount && matches!(on_underflow, KvIntegerUnderflowPolicy::Reject) {
                return PreflightResult::Err {
                    reason: AedbError::Underflow.to_string(),
                };
            }
            PreflightResult::Ok { affected_rows: 1 }
        }
    }
}

fn decode_u64(bytes: &[u8]) -> Result<u64, String> {
    if bytes.len() != 8 {
        return Err(AedbError::Validation("invalid u64 bytes length".into()).to_string());
    }
    let mut out = [0u8; 8];
    out.copy_from_slice(bytes);
    Ok(u64::from_be_bytes(out))
}

fn load_table_u256_field(
    snapshot: &KeyspaceSnapshot,
    catalog: &Catalog,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    primary_key: &[crate::catalog::types::Value],
    column: &str,
) -> Result<U256, String> {
    let ns = crate::catalog::namespace_key(project_id, scope_id);
    let schema = catalog
        .tables
        .get(&(ns, table_name.to_string()))
        .ok_or_else(|| {
            AedbError::Validation(format!(
                "table does not exist: {project_id}.{scope_id}.{table_name}"
            ))
            .to_string()
        })?;
    let col_idx = schema
        .columns
        .iter()
        .position(|c| c.name == column)
        .ok_or_else(|| AedbError::Validation(format!("column not found: {column}")).to_string())?;
    let row_key = EncodedKey::from_values(primary_key);
    let row = snapshot
        .table(project_id, scope_id, table_name)
        .and_then(|t| t.rows.get(&row_key))
        .ok_or_else(|| AedbError::Validation("row not found".into()).to_string())?;
    let value = row
        .values
        .get(col_idx)
        .ok_or_else(|| AedbError::Validation("column value missing".into()).to_string())?;
    match value {
        crate::catalog::types::Value::U256(bytes) => Ok(U256::from_big_endian(bytes)),
        _ => Err(AedbError::Validation(format!("column {column} is not U256")).to_string()),
    }
}

fn snapshot_max_kv_version_for_prefix(
    snapshot: &KeyspaceSnapshot,
    project_id: &str,
    scope_id: &str,
    prefix: &[u8],
) -> u64 {
    let ns = crate::storage::keyspace::NamespaceId::project_scope(project_id, scope_id);
    let Some(namespace) = snapshot.namespaces.get(&ns) else {
        return 0;
    };
    namespace
        .kv
        .entries
        .iter()
        .filter(|(k, _)| k.starts_with(prefix))
        .map(|(_, v)| v.version)
        .max()
        .unwrap_or(0)
}

fn snapshot_kv_structural_version(
    snapshot: &KeyspaceSnapshot,
    project_id: &str,
    scope_id: &str,
) -> u64 {
    let ns = crate::storage::keyspace::NamespaceId::project_scope(project_id, scope_id);
    snapshot
        .namespaces
        .get(&ns)
        .map(|n| n.kv.structural_version)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{PreflightResult, preflight};
    use crate::catalog::DdlOperation;
    use crate::catalog::schema::ColumnDef;
    use crate::catalog::types::{ColumnType, Row, Value};
    use crate::commit::validation::Mutation;
    use crate::storage::keyspace::Keyspace;
    fn u256_be(value: u64) -> [u8; 32] {
        let mut bytes = [0u8; 32];
        bytes[24..].copy_from_slice(&value.to_be_bytes());
        bytes
    }

    #[test]
    fn preflight_valid_and_invalid_paths() {
        let mut catalog = crate::catalog::Catalog::default();
        let snapshot = Keyspace::default().snapshot();

        let missing = preflight(
            &snapshot,
            &catalog,
            &Mutation::Upsert {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                primary_key: vec![Value::Integer(1)],
                row: Row {
                    values: vec![Value::Integer(1)],
                },
            },
        );
        assert!(matches!(missing, PreflightResult::Err { .. }));

        catalog.create_project("p").expect("project");
        catalog
            .create_table(
                "p",
                "app",
                "users",
                vec![
                    ColumnDef {
                        name: "id".into(),
                        col_type: ColumnType::Integer,
                        nullable: false,
                    },
                    ColumnDef {
                        name: "name".into(),
                        col_type: ColumnType::Text,
                        nullable: false,
                    },
                ],
                vec!["id".into()],
            )
            .expect("table");

        let ok = preflight(
            &snapshot,
            &catalog,
            &Mutation::Upsert {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                primary_key: vec![Value::Integer(1)],
                row: Row {
                    values: vec![Value::Integer(1), Value::Text("ok".into())],
                },
            },
        );
        assert_eq!(ok, PreflightResult::Ok { affected_rows: 1 });

        let mismatch = preflight(
            &snapshot,
            &catalog,
            &Mutation::Upsert {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                primary_key: vec![Value::Integer(1)],
                row: Row {
                    values: vec![Value::Text("bad".into()), Value::Text("ok".into())],
                },
            },
        );
        assert!(matches!(mismatch, PreflightResult::Err { .. }));

        let ddl_existing = preflight(
            &snapshot,
            &catalog,
            &Mutation::Ddl(DdlOperation::CreateTable {
                owner_id: None,
                if_not_exists: false,
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                columns: vec![],
                primary_key: vec![],
            }),
        );
        assert!(matches!(ddl_existing, PreflightResult::Err { .. }));
    }

    #[test]
    fn preflight_is_advisory_not_authoritative() {
        let mut catalog = crate::catalog::Catalog::default();
        let snapshot = Keyspace::default().snapshot();
        catalog.create_project("p").expect("project");
        catalog
            .create_table(
                "p",
                "app",
                "users",
                vec![
                    ColumnDef {
                        name: "id".into(),
                        col_type: ColumnType::Integer,
                        nullable: false,
                    },
                    ColumnDef {
                        name: "name".into(),
                        col_type: ColumnType::Text,
                        nullable: false,
                    },
                ],
                vec!["id".into()],
            )
            .expect("table");
        let mutation = Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(1)],
            row: Row {
                values: vec![Value::Integer(1), Value::Text("ok".into())],
            },
        };
        let pre = preflight(&snapshot, &catalog, &mutation);
        assert!(matches!(pre, PreflightResult::Ok { .. }));

        catalog
            .alter_table(
                "p",
                "app",
                "users",
                crate::catalog::schema::TableAlteration::DropColumn {
                    name: "name".into(),
                },
            )
            .expect("alter");
        let after = preflight(&snapshot, &catalog, &mutation);
        assert!(matches!(after, PreflightResult::Err { .. }));
    }

    #[test]
    fn preflight_checks_kv_dec_balance_and_existing_u256_encoding() {
        let mut catalog = crate::catalog::Catalog::default();
        catalog.create_project("p").expect("project");

        let mut keyspace = Keyspace::default();
        keyspace.kv_set("p", "app", b"balance".to_vec(), u256_be(125).to_vec(), 1);
        let snapshot = keyspace.snapshot();

        let ok = preflight(
            &snapshot,
            &catalog,
            &Mutation::KvDecU256 {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"balance".to_vec(),
                amount_be: u256_be(100),
            },
        );
        assert_eq!(ok, PreflightResult::Ok { affected_rows: 1 });

        let underflow = preflight(
            &snapshot,
            &catalog,
            &Mutation::KvDecU256 {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"balance".to_vec(),
                amount_be: u256_be(130),
            },
        );
        assert!(matches!(
            underflow,
            PreflightResult::Err { ref reason } if reason == "underflow"
        ));

        let mut bad_keyspace = Keyspace::default();
        bad_keyspace.kv_set("p", "app", b"balance".to_vec(), vec![1, 2, 3], 1);
        let bad_snapshot = bad_keyspace.snapshot();
        let bad = preflight(
            &bad_snapshot,
            &catalog,
            &Mutation::KvIncU256 {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"balance".to_vec(),
                amount_be: u256_be(1),
            },
        );
        assert!(matches!(
            bad,
            PreflightResult::Err { ref reason } if reason.contains("invalid u256 bytes length")
        ));
    }

    #[test]
    fn preflight_insert_reports_duplicate_primary_key() {
        let mut catalog = crate::catalog::Catalog::default();
        catalog.create_project("p").expect("project");
        catalog
            .create_table(
                "p",
                "app",
                "users",
                vec![
                    ColumnDef {
                        name: "id".into(),
                        col_type: ColumnType::Integer,
                        nullable: false,
                    },
                    ColumnDef {
                        name: "name".into(),
                        col_type: ColumnType::Text,
                        nullable: false,
                    },
                ],
                vec!["id".into()],
            )
            .expect("table");

        let mut keyspace = Keyspace::default();
        keyspace.upsert_row(
            "p",
            "app",
            "users",
            vec![Value::Integer(1)],
            Row {
                values: vec![Value::Integer(1), Value::Text("alice".into())],
            },
            1,
        );
        let snapshot = keyspace.snapshot();

        let duplicate = preflight(
            &snapshot,
            &catalog,
            &Mutation::Insert {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                primary_key: vec![Value::Integer(1)],
                row: Row {
                    values: vec![Value::Integer(1), Value::Text("second".into())],
                },
            },
        );
        assert!(matches!(duplicate, PreflightResult::Err { .. }));
    }

    #[test]
    fn preflight_insert_batch_reports_duplicate_primary_key() {
        let mut catalog = crate::catalog::Catalog::default();
        catalog.create_project("p").expect("project");
        catalog
            .create_table(
                "p",
                "app",
                "users",
                vec![
                    ColumnDef {
                        name: "id".into(),
                        col_type: ColumnType::Integer,
                        nullable: false,
                    },
                    ColumnDef {
                        name: "name".into(),
                        col_type: ColumnType::Text,
                        nullable: false,
                    },
                ],
                vec!["id".into()],
            )
            .expect("table");

        let snapshot = Keyspace::default().snapshot();
        let duplicate = preflight(
            &snapshot,
            &catalog,
            &Mutation::InsertBatch {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                rows: vec![
                    Row {
                        values: vec![Value::Integer(1), Value::Text("alice".into())],
                    },
                    Row {
                        values: vec![Value::Integer(1), Value::Text("alice-dup".into())],
                    },
                ],
            },
        );
        assert!(matches!(duplicate, PreflightResult::Err { .. }));
    }
}
