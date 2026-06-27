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
    macro_rules! kv_get {
        ($project_id:expr, $scope_id:expr, $key:expr) => {
            match snapshot.try_kv_get($project_id, $scope_id, $key) {
                Ok(entry) => entry,
                Err(e) => {
                    return PreflightResult::Err {
                        reason: e.to_string(),
                    };
                }
            }
        };
    }

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
            let current = match kv_get!(project_id, scope_id, key) {
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
            let current = match kv_get!(project_id, scope_id, key) {
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
            let current = match kv_get!(project_id, scope_id, key) {
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
            let current = match kv_get!(project_id, scope_id, key) {
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
            if kv_get!(project_id, scope_id, key).is_none()
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
            let current_entry = kv_get!(project_id, scope_id, key);
            let current_version = current_entry
                .as_ref()
                .map(|entry| entry.version)
                .unwrap_or(0);
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
            let current = match kv_get!(project_id, scope_id, key) {
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
            let current = match kv_get!(project_id, scope_id, key) {
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
        Mutation::KvAddI64Bounded { .. } => PreflightResult::Ok { affected_rows: 1 },
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
            let current = match kv_get!(project_id, scope_id, &shard_key) {
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
            if kv_get!(project_id, scope_id, key).is_none()
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
            let current_entry = kv_get!(project_id, scope_id, key);
            let current_version = current_entry
                .as_ref()
                .map(|entry| entry.version)
                .unwrap_or(0);
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
    preflight_plan_with_config(
        snapshot,
        catalog,
        mutation,
        base_seq,
        &AedbConfig::default(),
    )
}

pub fn preflight_plan_with_config(
    snapshot: &KeyspaceSnapshot,
    catalog: &Catalog,
    mutation: &Mutation,
    base_seq: u64,
    config: &AedbConfig,
) -> PreflightPlan {
    let mut errors = Vec::new();
    macro_rules! kv_version {
        ($project_id:expr, $scope_id:expr, $key:expr) => {
            snapshot_kv_version_for_key(snapshot, $project_id, $scope_id, $key, &mut errors)
        };
    }
    if let Err(e) = validate_mutation_with_config(catalog, mutation, config) {
        errors.push(e.to_string());
    }
    let estimated_affected_rows = match mutation {
        Mutation::InsertBatch { rows, .. }
        | Mutation::UpsertBatch { rows, .. }
        | Mutation::UpsertBatchOnConflict { rows, .. } => rows.len(),
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
                .and_then(|t| t.version_of(&EncodedKey::from_values(primary_key)))
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
                        .and_then(|t| t.version_of(&EncodedKey::from_values(&primary_key)))
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
                    (t.max_version(), t.structural_version)
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
            let version = kv_version!(project_id, scope_id, key);
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
                let version = kv_version!(project_id, scope_id, key);
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
            let version = kv_version!(project_id, scope_id, key);
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
            let version = kv_version!(project_id, scope_id, key);
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
                let version = kv_version!(project_id, scope_id, key);
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
            let version = kv_version!(project_id, scope_id, key);
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
        Mutation::KvAddI64Bounded { .. } => {}
        Mutation::KvMaxU64 { .. } => {}
        Mutation::KvMinU64 { .. } => {}
        Mutation::PostflightCheck { .. } => {
            // Postflight checks intentionally do not enter the advisory read set:
            // they are evaluated after prior atomic updates against the
            // transaction-local trial state, so they avoid turning a hot-key
            // atomic update into a pre-apply read/write conflict.
        }
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
                .and_then(|t| t.version_of(&EncodedKey::from_values(primary_key)))
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
        Mutation::EmitEvent { .. } => {}
        Mutation::OrderBookNew {
            project_id,
            scope_id,
            request,
        } => {
            let version = kv_version!(project_id, scope_id, request.instrument.as_bytes());
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
            let version = kv_version!(project_id, scope_id, &key);
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
            let max_version = snapshot_max_kv_version_for_prefix(
                snapshot,
                project_id,
                scope_id,
                &prefix,
                &mut errors,
            );
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
            let version = kv_version!(project_id, scope_id, &key);
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
            let version = kv_version!(project_id, scope_id, &key);
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
            let current = match match snapshot.try_kv_get(project_id, scope_id, key) {
                Ok(entry) => entry,
                Err(e) => {
                    return PreflightResult::Err {
                        reason: e.to_string(),
                    };
                }
            } {
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
            let current = match match snapshot.try_kv_get(project_id, scope_id, key) {
                Ok(entry) => entry,
                Err(e) => {
                    return PreflightResult::Err {
                        reason: e.to_string(),
                    };
                }
            } {
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
    let stored = snapshot
        .table(project_id, scope_id, table_name)
        .and_then(|t| t.rows.get(&row_key))
        .ok_or_else(|| AedbError::Validation("row not found".into()).to_string())?;
    let row = snapshot
        .materialize_row(stored)
        .map_err(|e| e.to_string())?;
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
    errors: &mut Vec<String>,
) -> u64 {
    match snapshot.try_kv_scan_prefix(project_id, scope_id, prefix, usize::MAX) {
        Ok(entries) => entries
            .into_iter()
            .map(|(_, entry)| entry.version)
            .max()
            .unwrap_or(0),
        Err(e) => {
            errors.push(e.to_string());
            0
        }
    }
}

fn snapshot_kv_version_for_key(
    snapshot: &KeyspaceSnapshot,
    project_id: &str,
    scope_id: &str,
    key: &[u8],
    errors: &mut Vec<String>,
) -> u64 {
    match snapshot.try_kv_get(project_id, scope_id, key) {
        Ok(entry) => entry.map(|entry| entry.version).unwrap_or(0),
        Err(e) => {
            errors.push(e.to_string());
            0
        }
    }
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
mod tests;
