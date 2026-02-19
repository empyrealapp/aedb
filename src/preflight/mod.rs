use crate::catalog::Catalog;
use crate::commit::tx::{
    PreflightPlan, ReadBound, ReadKey, ReadRange, ReadRangeEntry, ReadSet, ReadSetEntry,
    WriteIntent,
};
use crate::commit::validation::{Mutation, validate_mutation};
use crate::error::AedbError;
use crate::storage::encoded_key::EncodedKey;
use crate::storage::keyspace::KeyspaceSnapshot;
use primitive_types::U256;

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
    if let Err(e) = validate_mutation(catalog, mutation) {
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
        _ => PreflightResult::Ok { affected_rows: 1 },
    }
}

pub fn preflight_plan(
    snapshot: &KeyspaceSnapshot,
    catalog: &Catalog,
    mutation: &Mutation,
    base_seq: u64,
) -> PreflightPlan {
    let mut errors = Vec::new();
    if let Err(e) = validate_mutation(catalog, mutation) {
        errors.push(e.to_string());
    }
    let estimated_affected_rows = match mutation {
        Mutation::UpsertBatch { rows, .. } | Mutation::UpsertBatchOnConflict { rows, .. } => {
            rows.len()
        }
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
        Mutation::UpsertBatch {
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
        Mutation::KvIncU256 { .. } => {
            // Commutative increment executes against current apply-time value.
        }
        Mutation::KvDecU256 {
            project_id,
            scope_id,
            key,
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
}
