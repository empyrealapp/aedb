use crate::catalog::Catalog;
use crate::catalog::namespace_key;
use crate::catalog::schema::TableSchema;
use crate::catalog::types::{Row, Value};
use crate::commit::validation::Mutation;
use crate::error::AedbError;
use crate::storage::encoded_key::EncodedKey;
use crate::storage::index::extract_index_key_encoded;
use crate::storage::keyspace::{Keyspace, NamespaceId};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct IndexKey {
    table_name: String,
    index_name: String,
    columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub(super) struct GlobalUniqueIndexState {
    entries: HashMap<IndexKey, HashMap<EncodedKey, (String, EncodedKey)>>,
}

struct RowEnforcementInput<'a> {
    project_id: &'a str,
    scope_id: &'a str,
    table_name: &'a str,
    incoming_pk: &'a [Value],
    incoming_row: &'a Row,
}

impl GlobalUniqueIndexState {
    pub(super) fn from_snapshot(catalog: &Catalog, keyspace: &Keyspace) -> Result<Self, AedbError> {
        let mut state = Self {
            entries: HashMap::new(),
        };
        for (idx, schema) in global_unique_index_definitions(catalog) {
            state.entries.entry(idx.clone()).or_default();
            let Some(by_value) = state.entries.get_mut(&idx) else {
                continue;
            };
            for (ns_id, ns_data) in keyspace.namespaces.iter() {
                let NamespaceId::Project(ns_key) = ns_id else {
                    continue;
                };
                if !ns_key.starts_with("_global::") {
                    continue;
                }
                let Some(table) = ns_data.tables.get(&idx.table_name) else {
                    continue;
                };
                let Some(table_schema) = schema.get(&**ns_key) else {
                    continue;
                };
                for (pk, row) in &table.rows {
                    let index_key = extract_index_key_encoded(row, table_schema, &idx.columns)?;
                    by_value.insert(index_key, (ns_key.clone(), pk.clone()));
                }
            }
        }
        Ok(state)
    }

    pub(super) fn total_entries(&self) -> usize {
        self.entries.values().map(|v| v.len()).sum()
    }

    pub(super) fn enforce_and_apply(
        &mut self,
        catalog: &Catalog,
        keyspace: &Keyspace,
        mutation: &Mutation,
    ) -> Result<(), AedbError> {
        match mutation {
            Mutation::Insert {
                project_id,
                scope_id,
                table_name,
                primary_key,
                row,
            } => self.enforce_for_row(
                catalog,
                keyspace,
                RowEnforcementInput {
                    project_id,
                    scope_id,
                    table_name,
                    incoming_pk: primary_key,
                    incoming_row: row,
                },
            ),
            Mutation::Upsert {
                project_id,
                scope_id,
                table_name,
                primary_key,
                row,
            } => self.enforce_for_row(
                catalog,
                keyspace,
                RowEnforcementInput {
                    project_id,
                    scope_id,
                    table_name,
                    incoming_pk: primary_key,
                    incoming_row: row,
                },
            ),
            Mutation::UpsertOnConflict {
                project_id,
                scope_id,
                table_name,
                row,
                ..
            } => {
                let schema = table_schema_for(catalog, project_id, scope_id, table_name)?;
                let pk = extract_pk_from_row(&schema, row)?;
                self.enforce_for_row(
                    catalog,
                    keyspace,
                    RowEnforcementInput {
                        project_id,
                        scope_id,
                        table_name,
                        incoming_pk: &pk,
                        incoming_row: row,
                    },
                )
            }
            Mutation::UpsertBatch {
                project_id,
                scope_id,
                table_name,
                rows,
            } => {
                let schema = table_schema_for(catalog, project_id, scope_id, table_name)?;
                for row in rows {
                    let pk = extract_pk_from_row(&schema, row)?;
                    self.enforce_for_row(
                        catalog,
                        keyspace,
                        RowEnforcementInput {
                            project_id,
                            scope_id,
                            table_name,
                            incoming_pk: &pk,
                            incoming_row: row,
                        },
                    )?;
                }
                Ok(())
            }
            Mutation::InsertBatch {
                project_id,
                scope_id,
                table_name,
                rows,
            } => {
                let schema = table_schema_for(catalog, project_id, scope_id, table_name)?;
                for row in rows {
                    let pk = extract_pk_from_row(&schema, row)?;
                    self.enforce_for_row(
                        catalog,
                        keyspace,
                        RowEnforcementInput {
                            project_id,
                            scope_id,
                            table_name,
                            incoming_pk: &pk,
                            incoming_row: row,
                        },
                    )?;
                }
                Ok(())
            }
            Mutation::UpsertBatchOnConflict {
                project_id,
                scope_id,
                table_name,
                rows,
                ..
            } => {
                let schema = table_schema_for(catalog, project_id, scope_id, table_name)?;
                for row in rows {
                    let pk = extract_pk_from_row(&schema, row)?;
                    self.enforce_for_row(
                        catalog,
                        keyspace,
                        RowEnforcementInput {
                            project_id,
                            scope_id,
                            table_name,
                            incoming_pk: &pk,
                            incoming_row: row,
                        },
                    )?;
                }
                Ok(())
            }
            Mutation::Delete {
                project_id,
                scope_id,
                table_name,
                primary_key,
            } => self.remove_for_delete(
                catalog,
                keyspace,
                project_id,
                scope_id,
                table_name,
                primary_key,
            ),
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
                if table_has_global_unique_entries(catalog, project_id, scope_id, table_name) {
                    return Err(AedbError::Validation(
                        "predicate mutations are not supported on tables with global unique indexes"
                            .into(),
                    ));
                }
                Ok(())
            }
            Mutation::TableIncU256 {
                project_id,
                scope_id,
                table_name,
                primary_key,
                column,
                amount_be,
            }
            | Mutation::TableDecU256 {
                project_id,
                scope_id,
                table_name,
                primary_key,
                column,
                amount_be,
            } => {
                let schema = table_schema_for(catalog, project_id, scope_id, table_name)?;
                let row_key = EncodedKey::from_values(primary_key);
                let existing = row_for_pk(
                    keyspace,
                    &namespace_key(project_id, scope_id),
                    table_name,
                    &row_key,
                )
                .ok_or_else(|| AedbError::Validation("row not found".into()))?;
                let Some(col_idx) = schema.columns.iter().position(|c| c.name == *column) else {
                    return Err(AedbError::Validation(format!("column not found: {column}")));
                };
                let current = match existing.values.get(col_idx) {
                    Some(Value::U256(bytes)) => {
                        primitive_types::U256::from_big_endian(bytes.as_slice())
                    }
                    _ => {
                        return Err(AedbError::Validation(format!(
                            "column {column} must be U256"
                        )));
                    }
                };
                let amount = primitive_types::U256::from_big_endian(amount_be);
                let next = if matches!(mutation, Mutation::TableIncU256 { .. }) {
                    current.saturating_add(amount)
                } else if current < amount {
                    return Err(AedbError::Underflow);
                } else {
                    current - amount
                };
                let mut next_be = [0u8; 32];
                next.to_big_endian(&mut next_be);
                let mut next_row = existing.clone();
                next_row.values[col_idx] = Value::U256(next_be);
                self.enforce_for_row(
                    catalog,
                    keyspace,
                    RowEnforcementInput {
                        project_id,
                        scope_id,
                        table_name,
                        incoming_pk: primary_key,
                        incoming_row: &next_row,
                    },
                )
            }
            Mutation::Ddl(_) => Ok(()),
            _ => Ok(()),
        }
    }

    fn enforce_for_row(
        &mut self,
        catalog: &Catalog,
        keyspace: &Keyspace,
        input: RowEnforcementInput<'_>,
    ) -> Result<(), AedbError> {
        if input.project_id != "_global" {
            return Ok(());
        }
        let current_ns = namespace_key(input.project_id, input.scope_id);
        let incoming_pk_encoded = EncodedKey::from_values(input.incoming_pk);
        let schema = table_schema_for(catalog, input.project_id, input.scope_id, input.table_name)?;
        let defs = defs_for_table(catalog, input.table_name);
        for idx in defs {
            let index_key = extract_index_key_encoded(input.incoming_row, &schema, &idx.columns)?;
            let map = self.entries.entry(idx.clone()).or_default();
            if let Some((existing_ns, existing_pk)) = map.get(&index_key)
                && !(existing_ns == &current_ns && existing_pk == &incoming_pk_encoded)
            {
                return Err(AedbError::Validation(format!(
                    "global unique constraint violation on {} ({})",
                    input.table_name, idx.index_name
                )));
            }

            if let Some(existing_row) = row_for_pk(
                keyspace,
                &current_ns,
                input.table_name,
                &incoming_pk_encoded,
            ) {
                let previous_key = extract_index_key_encoded(existing_row, &schema, &idx.columns)?;
                map.remove(&previous_key);
            }
            map.insert(index_key, (current_ns.clone(), incoming_pk_encoded.clone()));
        }
        Ok(())
    }

    fn remove_for_delete(
        &mut self,
        catalog: &Catalog,
        keyspace: &Keyspace,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        primary_key: &[Value],
    ) -> Result<(), AedbError> {
        if project_id != "_global" {
            return Ok(());
        }
        let ns = namespace_key(project_id, scope_id);
        let pk = EncodedKey::from_values(primary_key);
        let Some(row) = row_for_pk(keyspace, &ns, table_name, &pk) else {
            return Ok(());
        };
        let schema = table_schema_for(catalog, project_id, scope_id, table_name)?;
        for idx in defs_for_table(catalog, table_name) {
            let key = extract_index_key_encoded(row, &schema, &idx.columns)?;
            if let Some(map) = self.entries.get_mut(&idx) {
                map.remove(&key);
            }
        }
        Ok(())
    }
}

fn table_has_global_unique_entries(
    catalog: &Catalog,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
) -> bool {
    if project_id != "_global" {
        return false;
    }
    let ns = namespace_key(project_id, scope_id);
    catalog.indexes.iter().any(|((idx_ns, idx_table, _), def)| {
        (idx_ns == &ns || idx_ns.starts_with("_global::"))
            && idx_table == table_name
            && matches!(
                def.index_type,
                crate::catalog::schema::IndexType::UniqueHash
            )
    })
}

fn row_for_pk<'a>(
    keyspace: &'a Keyspace,
    ns: &str,
    table_name: &str,
    pk: &EncodedKey,
) -> Option<&'a Row> {
    keyspace
        .namespaces
        .get(&NamespaceId::Project(ns.to_string()))?
        .tables
        .get(table_name)?
        .rows
        .get(pk)
}

fn defs_for_table(catalog: &Catalog, table_name: &str) -> Vec<IndexKey> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for ((ns, t, idx_name), def) in &catalog.indexes {
        if !ns.starts_with("_global::") || t != table_name {
            continue;
        }
        if !matches!(
            def.index_type,
            crate::catalog::schema::IndexType::UniqueHash
        ) {
            continue;
        }
        let key = IndexKey {
            table_name: t.clone(),
            index_name: idx_name.clone(),
            columns: def.columns.clone(),
        };
        let dedupe = format!(
            "{}:{}:{}",
            key.table_name,
            key.index_name,
            key.columns.join(",")
        );
        if seen.insert(dedupe) {
            out.push(key);
        }
    }
    out
}

fn global_unique_index_definitions(
    catalog: &Catalog,
) -> Vec<(IndexKey, HashMap<String, TableSchema>)> {
    let mut defs = Vec::new();
    let mut seen = HashSet::new();
    for ((ns, table_name, idx_name), def) in &catalog.indexes {
        if !ns.starts_with("_global::") {
            continue;
        }
        if !matches!(
            def.index_type,
            crate::catalog::schema::IndexType::UniqueHash
        ) {
            continue;
        }
        let idx = IndexKey {
            table_name: table_name.clone(),
            index_name: idx_name.clone(),
            columns: def.columns.clone(),
        };
        let signature = format!(
            "{}:{}:{}",
            idx.table_name,
            idx.index_name,
            idx.columns.join(",")
        );
        if !seen.insert(signature) {
            continue;
        }
        let schemas = catalog
            .tables
            .iter()
            .filter_map(|((tbl_ns, tbl_name), schema)| {
                if tbl_name == table_name && tbl_ns.starts_with("_global::") {
                    Some((tbl_ns.clone(), schema.clone()))
                } else {
                    None
                }
            })
            .collect::<HashMap<_, _>>();
        defs.push((idx, schemas));
    }
    defs
}

fn table_schema_for(
    catalog: &Catalog,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
) -> Result<TableSchema, AedbError> {
    let ns = namespace_key(project_id, scope_id);
    catalog
        .tables
        .get(&(ns, table_name.to_string()))
        .cloned()
        .ok_or_else(|| AedbError::Validation("table missing".into()))
}

fn extract_pk_from_row(schema: &TableSchema, row: &Row) -> Result<Vec<Value>, AedbError> {
    let mut pk = Vec::with_capacity(schema.primary_key.len());
    for col in &schema.primary_key {
        let idx = schema
            .columns
            .iter()
            .position(|c| c.name == *col)
            .ok_or_else(|| AedbError::Validation(format!("primary key column missing: {col}")))?;
        pk.push(row.values[idx].clone());
    }
    Ok(pk)
}
