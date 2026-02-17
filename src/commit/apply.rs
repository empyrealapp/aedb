use crate::catalog::Catalog;
use crate::catalog::namespace_key;
use crate::catalog::schema::{ColumnDef, Constraint, ForeignKeyAction, TableSchema};
use crate::catalog::types::{ColumnType, Row, Value};
use crate::commit::validation::{
    ConflictAction, ConflictTarget, Mutation, TableUpdateExpr, UpdateExpr,
};
use crate::error::AedbError;
use crate::error::ResourceType as ErrorResourceType;
use crate::query::operators::{compile_expr, eval_compiled_expr_public};
use crate::storage::encoded_key::EncodedKey;
use crate::storage::index::extract_index_key_encoded;
use crate::storage::keyspace::{Keyspace, NamespaceId, SecondaryIndex, SecondaryIndexStore};
use primitive_types::U256;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn apply_mutation(
    catalog: &mut Catalog,
    keyspace: &mut Keyspace,
    mutation: Mutation,
    commit_seq: u64,
) -> Result<(), AedbError> {
    match mutation {
        Mutation::Upsert {
            project_id,
            scope_id,
            table_name,
            primary_key,
            row,
        } => {
            ensure_internal_audit_schema_for_upsert(catalog, &project_id, &scope_id, &table_name)?;
            apply_upsert_once(
                catalog,
                keyspace,
                &project_id,
                &scope_id,
                &table_name,
                primary_key,
                row,
                commit_seq,
            )?
        }
        Mutation::UpsertBatch {
            project_id,
            scope_id,
            table_name,
            rows,
        } => {
            ensure_internal_audit_schema_for_upsert(catalog, &project_id, &scope_id, &table_name)?;
            let ns = namespace_key(&project_id, &scope_id);
            let schema = catalog
                .tables
                .get(&(ns.clone(), table_name.clone()))
                .ok_or_else(|| AedbError::Validation("table missing".into()))?
                .clone();
            for row in rows {
                let primary_key = extract_primary_key_from_row(&schema, &row)?;
                apply_upsert_once(
                    catalog,
                    keyspace,
                    &project_id,
                    &scope_id,
                    &table_name,
                    primary_key,
                    row,
                    commit_seq,
                )?;
            }
        }
        Mutation::UpsertOnConflict {
            project_id,
            scope_id,
            table_name,
            row,
            conflict_target,
            conflict_action,
        } => apply_upsert_on_conflict_once(
            catalog,
            keyspace,
            project_id,
            scope_id,
            table_name,
            row,
            conflict_target,
            conflict_action,
            commit_seq,
        )?,
        Mutation::UpsertBatchOnConflict {
            project_id,
            scope_id,
            table_name,
            rows,
            conflict_target,
            conflict_action,
        } => {
            for row in rows {
                apply_upsert_on_conflict_once(
                    catalog,
                    keyspace,
                    project_id.clone(),
                    scope_id.clone(),
                    table_name.clone(),
                    row,
                    conflict_target.clone(),
                    conflict_action.clone(),
                    commit_seq,
                )?;
            }
        }
        Mutation::Delete {
            project_id,
            scope_id,
            table_name,
            primary_key,
        } => {
            apply_delete_internal(
                catalog,
                keyspace,
                &project_id,
                &scope_id,
                &table_name,
                &primary_key,
                commit_seq,
            )?;
        }
        Mutation::DeleteWhere {
            project_id,
            scope_id,
            table_name,
            predicate,
            limit,
        } => {
            apply_delete_where_internal(
                catalog,
                keyspace,
                &project_id,
                &scope_id,
                &table_name,
                &predicate,
                limit,
                commit_seq,
            )?;
        }
        Mutation::UpdateWhere {
            project_id,
            scope_id,
            table_name,
            predicate,
            updates,
            limit,
        } => {
            apply_update_where_internal(
                catalog,
                keyspace,
                &project_id,
                &scope_id,
                &table_name,
                &predicate,
                &updates,
                limit,
                commit_seq,
            )?;
        }
        Mutation::UpdateWhereExpr {
            project_id,
            scope_id,
            table_name,
            predicate,
            updates,
            limit,
        } => {
            apply_update_where_expr_internal(
                catalog,
                keyspace,
                &project_id,
                &scope_id,
                &table_name,
                &predicate,
                &updates,
                limit,
                commit_seq,
            )?;
        }
        Mutation::Ddl(op) => {
            prevalidate_ddl_with_data(catalog, keyspace, &op)?;
            catalog.apply_ddl(op.clone())?;
            match op {
                crate::catalog::DdlOperation::DropProject { project_id, .. } => {
                    keyspace.drop_project(&project_id);
                }
                crate::catalog::DdlOperation::DropScope {
                    project_id,
                    scope_id,
                    ..
                } => {
                    keyspace.drop_scope(&project_id, &scope_id);
                }
                crate::catalog::DdlOperation::DropTable {
                    project_id,
                    scope_id,
                    table_name,
                    ..
                } => {
                    keyspace.drop_table(&project_id, &scope_id, &table_name);
                }
                crate::catalog::DdlOperation::CreateIndex {
                    project_id,
                    scope_id,
                    table_name,
                    index_name,
                    columns,
                    ..
                } => {
                    rebuild_index_for_table(
                        keyspace,
                        catalog,
                        &project_id,
                        &scope_id,
                        &table_name,
                        &index_name,
                        &columns,
                    )?;
                }
                crate::catalog::DdlOperation::DropIndex {
                    project_id,
                    scope_id,
                    table_name,
                    index_name,
                    ..
                } => {
                    let ns = namespace_key(&project_id, &scope_id);
                    if let Some(table) = keyspace.table_by_namespace_key_mut(&ns, &table_name) {
                        table.indexes.remove(&index_name);
                    }
                }
                crate::catalog::DdlOperation::AlterTable {
                    project_id,
                    scope_id,
                    table_name,
                    alteration,
                } => match alteration {
                    crate::catalog::schema::TableAlteration::AddConstraint(
                        crate::catalog::schema::Constraint::Unique { name, columns },
                    ) => {
                        rebuild_index_for_table(
                            keyspace,
                            catalog,
                            &project_id,
                            &scope_id,
                            &table_name,
                            &name,
                            &columns,
                        )?;
                    }
                    crate::catalog::schema::TableAlteration::DropConstraint { name } => {
                        let ns = namespace_key(&project_id, &scope_id);
                        if !catalog.indexes.contains_key(&(
                            ns.clone(),
                            table_name.clone(),
                            name.clone(),
                        )) && let Some(table) =
                            keyspace.table_by_namespace_key_mut(&ns, &table_name)
                        {
                            table.indexes.remove(&name);
                        }
                    }
                    _ => {}
                },
                crate::catalog::DdlOperation::CreateAsyncIndex {
                    project_id,
                    scope_id,
                    table_name,
                    index_name,
                    ..
                } => {
                    let ns = namespace_key(&project_id, &scope_id);
                    keyspace.insert_async_projection(
                        NamespaceId::Project(ns),
                        table_name,
                        index_name,
                        crate::storage::keyspace::AsyncProjectionData {
                            rows: im::OrdMap::new(),
                            materialized_seq: 0,
                        },
                    );
                }
                crate::catalog::DdlOperation::DropAsyncIndex {
                    project_id,
                    scope_id,
                    table_name,
                    index_name,
                    ..
                } => {
                    let ns = namespace_key(&project_id, &scope_id);
                    keyspace.remove_async_projection(
                        &NamespaceId::Project(ns),
                        &table_name,
                        &index_name,
                    );
                }
                crate::catalog::DdlOperation::EnableKvProjection {
                    project_id,
                    scope_id,
                } => {
                    keyspace.table_mut(&project_id, &scope_id, crate::catalog::KV_INDEX_TABLE);
                }
                crate::catalog::DdlOperation::DisableKvProjection {
                    project_id,
                    scope_id,
                } => {
                    keyspace.drop_table(&project_id, &scope_id, crate::catalog::KV_INDEX_TABLE);
                }
                crate::catalog::DdlOperation::GrantPermission {
                    caller_id,
                    permission,
                    actor_id,
                    ..
                } => {
                    append_authz_audit_row(
                        catalog,
                        keyspace,
                        commit_seq,
                        AuthzAuditContext {
                            action: "grant",
                            actor_id: actor_id.as_deref().unwrap_or(""),
                            target_caller_id: &caller_id,
                            delegable: false,
                        },
                        &permission,
                    )?;
                }
                crate::catalog::DdlOperation::RevokePermission {
                    caller_id,
                    permission,
                    actor_id,
                    ..
                } => {
                    append_authz_audit_row(
                        catalog,
                        keyspace,
                        commit_seq,
                        AuthzAuditContext {
                            action: "revoke",
                            actor_id: actor_id.as_deref().unwrap_or(""),
                            target_caller_id: &caller_id,
                            delegable: false,
                        },
                        &permission,
                    )?;
                }
                crate::catalog::DdlOperation::SetReadPolicy {
                    project_id,
                    scope_id: _,
                    table_name,
                    actor_id,
                    ..
                } => {
                    append_authz_audit_row(
                        catalog,
                        keyspace,
                        commit_seq,
                        AuthzAuditContext {
                            action: "set_read_policy",
                            actor_id: actor_id.as_deref().unwrap_or(""),
                            target_caller_id: "",
                            delegable: false,
                        },
                        &crate::permission::Permission::PolicyBypass {
                            project_id: project_id.clone(),
                            table_name: Some(table_name.clone()),
                        },
                    )?;
                }
                crate::catalog::DdlOperation::ClearReadPolicy {
                    project_id,
                    scope_id: _,
                    table_name,
                    actor_id,
                } => {
                    append_authz_audit_row(
                        catalog,
                        keyspace,
                        commit_seq,
                        AuthzAuditContext {
                            action: "clear_read_policy",
                            actor_id: actor_id.as_deref().unwrap_or(""),
                            target_caller_id: "",
                            delegable: false,
                        },
                        &crate::permission::Permission::PolicyBypass {
                            project_id: project_id.clone(),
                            table_name: Some(table_name.clone()),
                        },
                    )?;
                }
                crate::catalog::DdlOperation::TransferOwnership {
                    resource_type,
                    project_id,
                    scope_id,
                    table_name,
                    new_owner_id,
                    actor_id,
                } => {
                    append_ownership_audit_row(
                        catalog,
                        keyspace,
                        commit_seq,
                        "transfer_ownership",
                        actor_id.as_deref().unwrap_or(""),
                        &new_owner_id,
                        &resource_type,
                        &project_id,
                        scope_id.as_deref(),
                        table_name.as_deref(),
                    )?;
                }
                _ => {}
            }
        }
        Mutation::KvSet {
            project_id,
            scope_id,
            key,
            value,
        } => keyspace.kv_set(&project_id, &scope_id, key, value, commit_seq),
        Mutation::KvDel {
            project_id,
            scope_id,
            key,
        } => {
            let _ = keyspace.kv_del(&project_id, &scope_id, &key, commit_seq);
        }
        Mutation::KvIncU256 {
            project_id,
            scope_id,
            key,
            amount_be,
        } => {
            let _ = keyspace.kv_inc_u256(
                &project_id,
                &scope_id,
                key,
                U256::from_big_endian(&amount_be),
                commit_seq,
            )?;
        }
        Mutation::KvDecU256 {
            project_id,
            scope_id,
            key,
            amount_be,
        } => {
            let _ = keyspace.kv_dec_u256(
                &project_id,
                &scope_id,
                key,
                U256::from_big_endian(&amount_be),
                commit_seq,
            )?;
        }
        Mutation::TableIncU256 {
            project_id,
            scope_id,
            table_name,
            primary_key,
            column,
            amount_be,
        } => apply_table_u256_arithmetic(
            catalog,
            keyspace,
            &project_id,
            &scope_id,
            &table_name,
            &primary_key,
            &column,
            U256::from_big_endian(&amount_be),
            true,
            commit_seq,
        )?,
        Mutation::TableDecU256 {
            project_id,
            scope_id,
            table_name,
            primary_key,
            column,
            amount_be,
        } => apply_table_u256_arithmetic(
            catalog,
            keyspace,
            &project_id,
            &scope_id,
            &table_name,
            &primary_key,
            &column,
            U256::from_big_endian(&amount_be),
            false,
            commit_seq,
        )?,
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_table_u256_arithmetic(
    catalog: &mut Catalog,
    keyspace: &mut Keyspace,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    primary_key: &[Value],
    column: &str,
    amount: U256,
    increment: bool,
    commit_seq: u64,
) -> Result<(), AedbError> {
    let ns = namespace_key(project_id, scope_id);
    let schema = catalog
        .tables
        .get(&(ns, table_name.to_string()))
        .ok_or_else(|| {
            AedbError::Validation(format!(
                "table does not exist: {project_id}.{scope_id}.{table_name}"
            ))
        })?
        .clone();
    let column_idx = schema
        .columns
        .iter()
        .position(|c| c.name == column)
        .ok_or_else(|| AedbError::Validation(format!("column not found: {column}")))?;
    let current_row = keyspace
        .table_by_namespace_key(&namespace_key(project_id, scope_id), table_name)
        .and_then(|t| t.rows.get(&EncodedKey::from_values(primary_key)))
        .ok_or_else(|| AedbError::Validation("row not found".into()))?
        .clone();
    let current_value = match current_row.values.get(column_idx) {
        Some(Value::U256(bytes)) => U256::from_big_endian(bytes.as_slice()),
        _ => {
            return Err(AedbError::Validation(format!(
                "column {column} must be U256"
            )));
        }
    };
    let next = if increment {
        current_value
            .checked_add(amount)
            .ok_or(AedbError::Overflow)?
    } else {
        if current_value < amount {
            return Err(AedbError::Underflow);
        }
        current_value - amount
    };
    let mut next_be = [0u8; 32];
    next.to_big_endian(&mut next_be);
    let mut next_row = current_row;
    next_row.values[column_idx] = Value::U256(next_be);
    apply_upsert_once(
        catalog,
        keyspace,
        project_id,
        scope_id,
        table_name,
        primary_key.to_vec(),
        next_row,
        commit_seq,
    )
}

pub fn apply_mutation_trusted_if_eligible(
    catalog: &mut Catalog,
    keyspace: &mut Keyspace,
    mutation: Mutation,
    commit_seq: u64,
    validated_at_seq: u64,
    current_seq: u64,
) -> Option<Result<(), AedbError>> {
    if validated_at_seq != current_seq {
        return None;
    }
    match mutation {
        Mutation::Upsert {
            project_id,
            scope_id,
            table_name,
            primary_key,
            row,
        } => Some(apply_upsert_trusted_fast(
            catalog,
            keyspace,
            &project_id,
            &scope_id,
            &table_name,
            primary_key,
            row,
            commit_seq,
        )),
        Mutation::UpsertBatch {
            project_id,
            scope_id,
            table_name,
            rows,
        } => {
            let ns = namespace_key(&project_id, &scope_id);
            let schema = match catalog.tables.get(&(ns, table_name.clone())) {
                Some(schema) => schema.clone(),
                None => return Some(Err(AedbError::Validation("table missing".into()))),
            };
            if !table_allows_trusted_fast_upsert(
                catalog,
                &schema,
                &project_id,
                &scope_id,
                &table_name,
            ) {
                return None;
            }
            let mut result = Ok(());
            for row in rows {
                let primary_key = match extract_primary_key_from_row(&schema, &row) {
                    Ok(pk) => pk,
                    Err(err) => {
                        result = Err(err);
                        break;
                    }
                };
                if let Err(err) = apply_upsert_trusted_fast(
                    catalog,
                    keyspace,
                    &project_id,
                    &scope_id,
                    &table_name,
                    primary_key,
                    row,
                    commit_seq,
                ) {
                    result = Err(err);
                    break;
                }
            }
            Some(result)
        }
        _ => None,
    }
}

const AUTHZ_AUDIT_TABLE: &str = "authz_audit";
const ASSERTION_AUDIT_TABLE: &str = "assertion_audit";
const SYSTEM_SCOPE_ID: &str = "app";

struct AuthzAuditContext<'a> {
    action: &'a str,
    actor_id: &'a str,
    target_caller_id: &'a str,
    delegable: bool,
}

fn append_authz_audit_row(
    catalog: &mut Catalog,
    keyspace: &mut Keyspace,
    commit_seq: u64,
    context: AuthzAuditContext<'_>,
    permission: &crate::permission::Permission,
) -> Result<(), AedbError> {
    ensure_authz_audit_schema(catalog)?;
    let (project, scope, table) = permission_namespace(permission);
    keyspace.upsert_row(
        crate::catalog::SYSTEM_PROJECT_ID,
        SYSTEM_SCOPE_ID,
        AUTHZ_AUDIT_TABLE,
        vec![Value::Integer(commit_seq as i64)],
        Row {
            values: vec![
                Value::Integer(commit_seq as i64),
                Value::Timestamp(now_micros() as i64),
                Value::Text(context.action.into()),
                Value::Text(context.actor_id.into()),
                Value::Text(context.target_caller_id.into()),
                Value::Text(format!("{permission:?}").into()),
                project
                    .map(|p| Value::Text(p.into()))
                    .unwrap_or(Value::Null),
                scope.map(|s| Value::Text(s.into())).unwrap_or(Value::Null),
                table.map(|t| Value::Text(t.into())).unwrap_or(Value::Null),
                Value::Boolean(context.delegable),
                Value::Null,
                Value::Null,
                Value::Null,
            ],
        },
        commit_seq,
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn append_ownership_audit_row(
    catalog: &mut Catalog,
    keyspace: &mut Keyspace,
    commit_seq: u64,
    action: &str,
    actor_id: &str,
    target_caller_id: &str,
    resource_type: &crate::catalog::ResourceType,
    project_id: &str,
    scope_id: Option<&str>,
    table_name: Option<&str>,
) -> Result<(), AedbError> {
    ensure_authz_audit_schema(catalog)?;
    let resource_id = match (scope_id, table_name) {
        (Some(scope), Some(table)) => format!("{project_id}.{scope}.{table}"),
        (Some(scope), None) => format!("{project_id}.{scope}"),
        (None, _) => project_id.to_string(),
    };
    keyspace.upsert_row(
        crate::catalog::SYSTEM_PROJECT_ID,
        SYSTEM_SCOPE_ID,
        AUTHZ_AUDIT_TABLE,
        vec![Value::Integer(commit_seq as i64)],
        Row {
            values: vec![
                Value::Integer(commit_seq as i64),
                Value::Timestamp(now_micros() as i64),
                Value::Text(action.into()),
                Value::Text(actor_id.into()),
                Value::Text(target_caller_id.into()),
                Value::Null,
                Value::Text(project_id.into()),
                scope_id
                    .map(|s| Value::Text(s.into()))
                    .unwrap_or(Value::Null),
                table_name
                    .map(|t| Value::Text(t.into()))
                    .unwrap_or(Value::Null),
                Value::Boolean(false),
                Value::Text(format!("{resource_type:?}").into()),
                Value::Text(resource_id.into()),
                Value::Null,
            ],
        },
        commit_seq,
    );
    Ok(())
}

fn ensure_internal_audit_schema_for_upsert(
    catalog: &mut Catalog,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
) -> Result<(), AedbError> {
    if project_id == crate::catalog::SYSTEM_PROJECT_ID
        && scope_id == SYSTEM_SCOPE_ID
        && table_name == ASSERTION_AUDIT_TABLE
    {
        ensure_assertion_audit_schema(catalog)?;
    }
    Ok(())
}

fn ensure_authz_audit_schema(catalog: &mut Catalog) -> Result<(), AedbError> {
    ensure_system_project_scope(catalog);

    let key = (
        namespace_key(crate::catalog::SYSTEM_PROJECT_ID, SYSTEM_SCOPE_ID),
        AUTHZ_AUDIT_TABLE.to_string(),
    );
    if catalog.tables.contains_key(&key) {
        return Ok(());
    }
    catalog.tables.insert(
        key,
        TableSchema {
            project_id: crate::catalog::SYSTEM_PROJECT_ID.to_string(),
            scope_id: SYSTEM_SCOPE_ID.to_string(),
            table_name: AUTHZ_AUDIT_TABLE.to_string(),
            owner_id: Some("system".to_string()),
            columns: vec![
                ColumnDef {
                    name: "seq".to_string(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "ts_micros".to_string(),
                    col_type: ColumnType::Timestamp,
                    nullable: false,
                },
                ColumnDef {
                    name: "action".to_string(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
                ColumnDef {
                    name: "actor_id".to_string(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
                ColumnDef {
                    name: "target_caller_id".to_string(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
                ColumnDef {
                    name: "permission".to_string(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
                ColumnDef {
                    name: "project_id".to_string(),
                    col_type: ColumnType::Text,
                    nullable: true,
                },
                ColumnDef {
                    name: "scope_id".to_string(),
                    col_type: ColumnType::Text,
                    nullable: true,
                },
                ColumnDef {
                    name: "table_name".to_string(),
                    col_type: ColumnType::Text,
                    nullable: true,
                },
                ColumnDef {
                    name: "delegable".to_string(),
                    col_type: ColumnType::Boolean,
                    nullable: false,
                },
                ColumnDef {
                    name: "resource_type".to_string(),
                    col_type: ColumnType::Text,
                    nullable: true,
                },
                ColumnDef {
                    name: "resource_id".to_string(),
                    col_type: ColumnType::Text,
                    nullable: true,
                },
                ColumnDef {
                    name: "metadata".to_string(),
                    col_type: ColumnType::Text,
                    nullable: true,
                },
            ],
            primary_key: vec!["seq".to_string()],
            constraints: Vec::new(),
            foreign_keys: Vec::new(),
        },
    );
    Ok(())
}

fn ensure_assertion_audit_schema(catalog: &mut Catalog) -> Result<(), AedbError> {
    ensure_system_project_scope(catalog);
    let key = (
        namespace_key(crate::catalog::SYSTEM_PROJECT_ID, SYSTEM_SCOPE_ID),
        ASSERTION_AUDIT_TABLE.to_string(),
    );
    if catalog.tables.contains_key(&key) {
        return Ok(());
    }
    catalog.tables.insert(
        key,
        TableSchema {
            project_id: crate::catalog::SYSTEM_PROJECT_ID.to_string(),
            scope_id: SYSTEM_SCOPE_ID.to_string(),
            table_name: ASSERTION_AUDIT_TABLE.to_string(),
            owner_id: Some("system".to_string()),
            columns: vec![
                ColumnDef {
                    name: "seq".to_string(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "ts_micros".to_string(),
                    col_type: ColumnType::Timestamp,
                    nullable: false,
                },
                ColumnDef {
                    name: "caller_id".to_string(),
                    col_type: ColumnType::Text,
                    nullable: true,
                },
                ColumnDef {
                    name: "assertion_index".to_string(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "assertion".to_string(),
                    col_type: ColumnType::Json,
                    nullable: false,
                },
                ColumnDef {
                    name: "actual".to_string(),
                    col_type: ColumnType::Json,
                    nullable: false,
                },
                ColumnDef {
                    name: "error".to_string(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
            ],
            primary_key: vec!["seq".to_string()],
            constraints: vec![],
            foreign_keys: vec![],
        },
    );
    Ok(())
}

fn ensure_system_project_scope(catalog: &mut Catalog) {
    if !catalog
        .projects
        .contains_key(crate::catalog::SYSTEM_PROJECT_ID)
    {
        catalog.projects.insert(
            crate::catalog::SYSTEM_PROJECT_ID.to_string(),
            crate::catalog::project::ProjectMeta {
                project_id: crate::catalog::SYSTEM_PROJECT_ID.to_string(),
                created_at_micros: now_micros(),
                owner_id: Some("system".to_string()),
            },
        );
    }
    if !catalog.scopes.contains_key(&(
        crate::catalog::SYSTEM_PROJECT_ID.to_string(),
        SYSTEM_SCOPE_ID.to_string(),
    )) {
        catalog.scopes.insert(
            (
                crate::catalog::SYSTEM_PROJECT_ID.to_string(),
                SYSTEM_SCOPE_ID.to_string(),
            ),
            crate::catalog::project::ScopeMeta {
                project_id: crate::catalog::SYSTEM_PROJECT_ID.to_string(),
                scope_id: SYSTEM_SCOPE_ID.to_string(),
                created_at_micros: now_micros(),
                owner_id: Some("system".to_string()),
            },
        );
    }
}

fn permission_namespace(
    permission: &crate::permission::Permission,
) -> (Option<&str>, Option<&str>, Option<&str>) {
    match permission {
        crate::permission::Permission::KvRead {
            project_id,
            scope_id,
            ..
        } => (Some(project_id.as_str()), scope_id.as_deref(), None),
        crate::permission::Permission::KvWrite {
            project_id,
            scope_id,
            ..
        } => (Some(project_id.as_str()), scope_id.as_deref(), None),
        crate::permission::Permission::ScopeAdmin {
            project_id,
            scope_id,
        } => (Some(project_id.as_str()), Some(scope_id.as_str()), None),
        crate::permission::Permission::TableRead {
            project_id,
            scope_id,
            table_name,
        }
        | crate::permission::Permission::TableWrite {
            project_id,
            scope_id,
            table_name,
        }
        | crate::permission::Permission::IndexRead {
            project_id,
            scope_id,
            table_name,
            ..
        } => (
            Some(project_id.as_str()),
            Some(scope_id.as_str()),
            Some(table_name.as_str()),
        ),
        crate::permission::Permission::ProjectAdmin { project_id }
        | crate::permission::Permission::TableDdl { project_id } => {
            (Some(project_id.as_str()), None, None)
        }
        crate::permission::Permission::PolicyBypass {
            project_id,
            table_name,
        } => (Some(project_id.as_str()), None, table_name.as_deref()),
        crate::permission::Permission::GlobalAdmin => (None, None, None),
    }
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

#[allow(clippy::too_many_arguments)]
fn apply_upsert_trusted_fast(
    catalog: &mut Catalog,
    keyspace: &mut Keyspace,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    primary_key: Vec<Value>,
    row: Row,
    commit_seq: u64,
) -> Result<(), AedbError> {
    let ns = namespace_key(project_id, scope_id);
    let schema = catalog
        .tables
        .get(&(ns, table_name.to_string()))
        .ok_or_else(|| AedbError::Validation("table missing".into()))?
        .clone();
    if !table_allows_trusted_fast_upsert(catalog, &schema, project_id, scope_id, table_name) {
        return Err(AedbError::Validation(
            "trusted fast path is not eligible for this table".into(),
        ));
    }
    let old_row = keyspace
        .get_row(project_id, scope_id, table_name, &primary_key)
        .cloned();
    keyspace.upsert_row(
        project_id,
        scope_id,
        table_name,
        primary_key.clone(),
        row.clone(),
        commit_seq,
    );
    maintain_secondary_indexes(
        catalog,
        keyspace,
        &schema,
        project_id,
        scope_id,
        table_name,
        &primary_key,
        old_row.as_ref(),
        Some(&row),
    )?;
    Ok(())
}

fn table_allows_trusted_fast_upsert(
    catalog: &Catalog,
    schema: &TableSchema,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
) -> bool {
    if !schema.constraints.is_empty() || !schema.foreign_keys.is_empty() {
        return false;
    }
    let ns = namespace_key(project_id, scope_id);
    let has_referencing_fk = catalog
        .tables
        .iter()
        .any(|((_dep_ns, _dep_table), dep_schema)| {
            dep_schema.foreign_keys.iter().any(|fk| {
                namespace_key(&fk.references_project_id, &fk.references_scope_id) == ns
                    && fk.references_table == table_name
            })
        });
    if has_referencing_fk {
        return false;
    }
    let has_unique_index = catalog.indexes.iter().any(|((idx_ns, idx_table, _), def)| {
        idx_ns == &ns
            && idx_table == table_name
            && matches!(
                def.index_type,
                crate::catalog::schema::IndexType::UniqueHash
            )
    });
    !has_unique_index
}

#[allow(clippy::too_many_arguments)]
fn apply_upsert_once(
    catalog: &mut Catalog,
    keyspace: &mut Keyspace,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    primary_key: Vec<Value>,
    mut row: Row,
    commit_seq: u64,
) -> Result<(), AedbError> {
    let ns = namespace_key(project_id, scope_id);
    let schema = catalog
        .tables
        .get(&(ns, table_name.to_string()))
        .ok_or_else(|| AedbError::Validation("table missing".into()))?
        .clone();
    let old_row = keyspace
        .get_row(project_id, scope_id, table_name, &primary_key)
        .cloned();
    apply_default_constraints(&schema, &mut row)?;
    validate_row_constraints(
        catalog,
        keyspace,
        &schema,
        project_id,
        scope_id,
        table_name,
        &primary_key,
        &row,
        old_row.as_ref(),
    )?;
    if let Some(before) = &old_row {
        handle_referencing_foreign_keys_on_update(
            catalog, keyspace, project_id, scope_id, table_name, &schema, before, &row, commit_seq,
        )?;
    }
    keyspace.upsert_row(
        project_id,
        scope_id,
        table_name,
        primary_key.clone(),
        row.clone(),
        commit_seq,
    );
    maintain_secondary_indexes(
        catalog,
        keyspace,
        &schema,
        project_id,
        scope_id,
        table_name,
        &primary_key,
        old_row.as_ref(),
        Some(&row),
    )?;
    Ok(())
}

fn extract_primary_key_from_row(schema: &TableSchema, row: &Row) -> Result<Vec<Value>, AedbError> {
    let mut primary_key = Vec::with_capacity(schema.primary_key.len());
    for pk_name in &schema.primary_key {
        let idx = schema
            .columns
            .iter()
            .position(|c| c.name == *pk_name)
            .ok_or_else(|| {
                AedbError::Validation(format!("primary key column missing: {pk_name}"))
            })?;
        let value = row.values.get(idx).ok_or_else(|| {
            AedbError::Validation(format!(
                "primary key column value missing from row: {pk_name}"
            ))
        })?;
        primary_key.push(value.clone());
    }
    Ok(primary_key)
}

#[allow(clippy::too_many_arguments)]
fn find_existing_conflict_row(
    catalog: &Catalog,
    keyspace: &Keyspace,
    schema: &TableSchema,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    proposed: &Row,
    conflict_target: &ConflictTarget,
) -> Result<Option<(EncodedKey, Row)>, AedbError> {
    match conflict_target {
        ConflictTarget::PrimaryKey => {
            let proposed_pk = extract_primary_key_from_row(schema, proposed)?;
            let encoded = EncodedKey::from_values(&proposed_pk);
            let row = keyspace
                .get_row_by_encoded(project_id, scope_id, table_name, &encoded)
                .cloned();
            Ok(row.map(|r| (encoded, r)))
        }
        ConflictTarget::Index(index_name) => lookup_existing_by_unique_index(
            catalog, keyspace, schema, project_id, scope_id, table_name, index_name, proposed,
        ),
        ConflictTarget::Columns(columns) => {
            if &schema.primary_key == columns {
                return find_existing_conflict_row(
                    catalog,
                    keyspace,
                    schema,
                    project_id,
                    scope_id,
                    table_name,
                    proposed,
                    &ConflictTarget::PrimaryKey,
                );
            }
            let ns = namespace_key(project_id, scope_id);
            let index_name = catalog
                .indexes
                .iter()
                .find(|((idx_ns, idx_table, _), idx_def)| {
                    idx_ns == &ns
                        && idx_table == table_name
                        && idx_def.columns == *columns
                        && matches!(
                            idx_def.index_type,
                            crate::catalog::schema::IndexType::UniqueHash
                        )
                })
                .map(|((_, _, name), _)| name.clone())
                .ok_or_else(|| {
                    AedbError::Validation("conflict columns must match a unique index".into())
                })?;
            lookup_existing_by_unique_index(
                catalog,
                keyspace,
                schema,
                project_id,
                scope_id,
                table_name,
                &index_name,
                proposed,
            )
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn lookup_existing_by_unique_index(
    catalog: &Catalog,
    keyspace: &Keyspace,
    schema: &TableSchema,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    index_name: &str,
    proposed: &Row,
) -> Result<Option<(EncodedKey, Row)>, AedbError> {
    let ns = namespace_key(project_id, scope_id);
    let idx_def = catalog
        .indexes
        .get(&(ns.clone(), table_name.to_string(), index_name.to_string()))
        .ok_or_else(|| AedbError::NotFound {
            resource_type: ErrorResourceType::Index,
            resource_id: format!("{project_id}.{scope_id}.{table_name}.{index_name}"),
        })?;
    if !matches!(
        idx_def.index_type,
        crate::catalog::schema::IndexType::UniqueHash
    ) {
        return Err(AedbError::Validation(format!(
            "conflict index is not unique: {index_name}"
        )));
    }

    let lookup_key = extract_index_key_encoded(proposed, schema, &idx_def.columns)?;
    let table = keyspace
        .table_by_namespace_key(&ns, table_name)
        .ok_or_else(|| AedbError::NotFound {
            resource_type: ErrorResourceType::Table,
            resource_id: format!("{project_id}.{scope_id}.{table_name}"),
        })?;
    let Some(index) = table.indexes.get(index_name) else {
        return Ok(None);
    };
    let Some(encoded_pk) = index.unique_existing(&lookup_key) else {
        return Ok(None);
    };
    let existing = table.rows.get(&encoded_pk).cloned();
    Ok(existing.map(|row| (encoded_pk, row)))
}

fn apply_default_constraints(schema: &TableSchema, row: &mut Row) -> Result<(), AedbError> {
    for constraint in &schema.constraints {
        if let Constraint::Default { column, value } = constraint
            && let Some(idx) = schema.columns.iter().position(|c| c.name == *column)
            && row
                .values
                .get(idx)
                .is_some_and(|v| matches!(v, Value::Null))
        {
            row.values[idx] = value.clone();
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_row_constraints(
    catalog: &Catalog,
    keyspace: &Keyspace,
    schema: &TableSchema,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    primary_key: &[Value],
    row: &Row,
    old_row: Option<&Row>,
) -> Result<(), AedbError> {
    for constraint in &schema.constraints {
        match constraint {
            Constraint::NotNull { column } => {
                let idx = schema
                    .columns
                    .iter()
                    .position(|c| c.name == *column)
                    .ok_or_else(|| AedbError::UnknownColumn {
                        table: table_name.to_string(),
                        column: column.clone(),
                    })?;
                if matches!(row.values.get(idx), Some(Value::Null) | None) {
                    return Err(AedbError::NotNullViolation {
                        table: table_name.to_string(),
                        column: column.clone(),
                    });
                }
            }
            Constraint::Check { name, expr } => {
                let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
                let compiled = compile_expr(expr, &columns, table_name).map_err(|e| {
                    AedbError::Validation(format!("check {name} compile error: {e:?}"))
                })?;
                if !eval_compiled_expr_public(&compiled, row) {
                    return Err(AedbError::CheckConstraintFailed {
                        table: table_name.to_string(),
                        constraint: name.clone(),
                    });
                }
            }
            Constraint::Unique { name, columns } => {
                validate_unique_constraint(
                    catalog,
                    keyspace,
                    schema,
                    project_id,
                    scope_id,
                    table_name,
                    primary_key,
                    row,
                    columns,
                )
                .map_err(|_| AedbError::UniqueViolation {
                    table: table_name.to_string(),
                    index: name.clone(),
                    key: format!("{primary_key:?}"),
                })?;
            }
            Constraint::Default { .. } => {}
        }
    }
    validate_foreign_keys(catalog, keyspace, schema, row, old_row)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_unique_constraint(
    catalog: &Catalog,
    keyspace: &Keyspace,
    schema: &TableSchema,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    primary_key: &[Value],
    row: &Row,
    columns: &[String],
) -> Result<(), AedbError> {
    if columns.is_empty() {
        return Ok(());
    }
    if has_null_in_columns(schema, row, columns)? {
        return Ok(());
    }
    let lookup_key = extract_index_key_encoded(row, schema, columns)?;
    let pk_encoded = EncodedKey::from_values(primary_key);
    let ns = namespace_key(project_id, scope_id);
    let maybe_index = catalog
        .indexes
        .iter()
        .find(|((idx_ns, idx_table, _), idx_def)| {
            idx_ns == &ns
                && idx_table == table_name
                && idx_def.columns == columns
                && matches!(
                    idx_def.index_type,
                    crate::catalog::schema::IndexType::UniqueHash
                )
        })
        .map(|((_, _, idx_name), _)| idx_name.clone());
    if let Some(index_name) = maybe_index
        && let Some(table) = keyspace.table_by_namespace_key(&ns, table_name)
        && let Some(index) = table.indexes.get(&index_name)
        && let Some(existing) = index.unique_existing(&lookup_key)
        && existing != pk_encoded
    {
        return Err(AedbError::UniqueViolation {
            table: table_name.to_string(),
            index: index_name,
            key: format!("{lookup_key:?}"),
        });
    } else if let Some(table) = keyspace.table_by_namespace_key(&ns, table_name) {
        for (pk, existing_row) in &table.rows {
            if *pk == pk_encoded {
                continue;
            }
            let existing_key = extract_index_key_encoded(existing_row, schema, columns)?;
            if existing_key == lookup_key {
                return Err(AedbError::UniqueViolation {
                    table: table_name.to_string(),
                    index: columns.join(","),
                    key: format!("{lookup_key:?}"),
                });
            }
        }
    }
    Ok(())
}

fn validate_foreign_keys(
    catalog: &Catalog,
    keyspace: &Keyspace,
    schema: &TableSchema,
    row: &Row,
    _old_row: Option<&Row>,
) -> Result<(), AedbError> {
    for fk in &schema.foreign_keys {
        let Some(ref_schema) = catalog.tables.get(&(
            namespace_key(&fk.references_project_id, &fk.references_scope_id),
            fk.references_table.clone(),
        )) else {
            return Err(AedbError::NotFound {
                resource_type: ErrorResourceType::Table,
                resource_id: format!(
                    "{}.{}.{}",
                    fk.references_project_id, fk.references_scope_id, fk.references_table
                ),
            });
        };
        let mut values = Vec::with_capacity(fk.columns.len());
        let mut any_null = false;
        for col in &fk.columns {
            let idx = schema
                .columns
                .iter()
                .position(|c| c.name == *col)
                .ok_or_else(|| AedbError::UnknownColumn {
                    table: schema.table_name.clone(),
                    column: col.clone(),
                })?;
            let value = row.values.get(idx).cloned().unwrap_or(Value::Null);
            if matches!(value, Value::Null) {
                any_null = true;
            }
            values.push(value);
        }
        if any_null {
            continue;
        }

        if ref_schema.primary_key == fk.references_columns {
            let exists = keyspace
                .get_row(
                    &fk.references_project_id,
                    &fk.references_scope_id,
                    &fk.references_table,
                    &values,
                )
                .is_some();
            if !exists {
                return Err(AedbError::ForeignKeyViolation {
                    fk_name: fk.name.clone(),
                    table: schema.table_name.clone(),
                    ref_table: fk.references_table.clone(),
                    ref_key: format!("{values:?}"),
                });
            }
        } else {
            // For non-PK unique references, prefer the unique hash index.
            let ref_ns = namespace_key(&fk.references_project_id, &fk.references_scope_id);
            let lookup_key = EncodedKey::from_values(&values);
            let maybe_index_name = catalog
                .indexes
                .iter()
                .find(|((idx_ns, idx_table, _), idx_def)| {
                    idx_ns == &ref_ns
                        && idx_table == &fk.references_table
                        && idx_def.columns == fk.references_columns
                        && matches!(
                            idx_def.index_type,
                            crate::catalog::schema::IndexType::UniqueHash
                        )
                })
                .map(|((_, _, idx_name), _)| idx_name.clone());
            if let Some(index_name) = maybe_index_name
                && let Some(ref_table) =
                    keyspace.table_by_namespace_key(&ref_ns, &fk.references_table)
                && let Some(index) = ref_table.indexes.get(&index_name)
                && index.unique_existing(&lookup_key).is_some()
            {
                continue;
            }

            // Fallback path if runtime index is not present yet.
            let ref_table = keyspace
                .table_by_namespace_key(&ref_ns, &fk.references_table)
                .ok_or_else(|| AedbError::NotFound {
                    resource_type: ErrorResourceType::Table,
                    resource_id: format!(
                        "{}.{}.{}",
                        fk.references_project_id, fk.references_scope_id, fk.references_table
                    ),
                })?;
            let matched = ref_table.rows.iter().any(|(_pk, r)| {
                let mut same = true;
                for (i, ref_col) in fk.references_columns.iter().enumerate() {
                    let Some(idx) = ref_schema.columns.iter().position(|c| c.name == *ref_col)
                    else {
                        return false;
                    };
                    if r.values.get(idx) != values.get(i) {
                        same = false;
                        break;
                    }
                }
                same
            });
            if !matched {
                return Err(AedbError::ForeignKeyViolation {
                    fk_name: fk.name.clone(),
                    table: schema.table_name.clone(),
                    ref_table: fk.references_table.clone(),
                    ref_key: format!("{values:?}"),
                });
            }
        }
    }
    Ok(())
}

fn apply_delete_internal(
    catalog: &Catalog,
    keyspace: &mut Keyspace,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    primary_key: &[Value],
    commit_seq: u64,
) -> Result<(), AedbError> {
    let ns = namespace_key(project_id, scope_id);
    let schema = catalog
        .tables
        .get(&(ns.clone(), table_name.to_string()))
        .ok_or_else(|| AedbError::NotFound {
            resource_type: ErrorResourceType::Table,
            resource_id: format!("{project_id}.{scope_id}.{table_name}"),
        })?
        .clone();
    let Some(old_row) = keyspace
        .get_row(project_id, scope_id, table_name, primary_key)
        .cloned()
    else {
        return Ok(());
    };

    handle_referencing_foreign_keys(
        catalog, keyspace, project_id, scope_id, table_name, &old_row, commit_seq,
    )?;

    let removed = keyspace.delete_row(project_id, scope_id, table_name, primary_key, commit_seq);
    maintain_secondary_indexes(
        catalog,
        keyspace,
        &schema,
        project_id,
        scope_id,
        table_name,
        primary_key,
        removed.as_ref(),
        None,
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_delete_where_internal(
    catalog: &Catalog,
    keyspace: &mut Keyspace,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    predicate: &crate::query::plan::Expr,
    limit: Option<usize>,
    commit_seq: u64,
) -> Result<(), AedbError> {
    let ns = namespace_key(project_id, scope_id);
    let schema = catalog
        .tables
        .get(&(ns.clone(), table_name.to_string()))
        .ok_or_else(|| AedbError::NotFound {
            resource_type: ErrorResourceType::Table,
            resource_id: format!("{project_id}.{scope_id}.{table_name}"),
        })?
        .clone();
    let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let compiled = compile_expr(predicate, &columns, table_name)
        .map_err(|e| AedbError::Validation(format!("invalid predicate: {e:?}")))?;
    let mut to_delete = Vec::new();
    if let Some(table) = keyspace.table_by_namespace_key(&ns, table_name) {
        for row in table.rows.values() {
            if !eval_compiled_expr_public(&compiled, row) {
                continue;
            }
            to_delete.push(extract_primary_key_from_row(&schema, row)?);
            if limit.is_some_and(|max| to_delete.len() >= max) {
                break;
            }
        }
    }
    for pk in to_delete {
        apply_delete_internal(
            catalog, keyspace, project_id, scope_id, table_name, &pk, commit_seq,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_update_where_internal(
    catalog: &mut Catalog,
    keyspace: &mut Keyspace,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    predicate: &crate::query::plan::Expr,
    updates: &[(String, Value)],
    limit: Option<usize>,
    commit_seq: u64,
) -> Result<(), AedbError> {
    let ns = namespace_key(project_id, scope_id);
    let schema = catalog
        .tables
        .get(&(ns.clone(), table_name.to_string()))
        .ok_or_else(|| AedbError::NotFound {
            resource_type: ErrorResourceType::Table,
            resource_id: format!("{project_id}.{scope_id}.{table_name}"),
        })?
        .clone();
    let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let compiled = compile_expr(predicate, &columns, table_name)
        .map_err(|e| AedbError::Validation(format!("invalid predicate: {e:?}")))?;
    let mut update_indices = Vec::with_capacity(updates.len());
    for (column, value) in updates {
        let Some(idx) = schema.columns.iter().position(|c| c.name == *column) else {
            return Err(AedbError::UnknownColumn {
                table: table_name.to_string(),
                column: column.clone(),
            });
        };
        update_indices.push((idx, value.clone()));
    }
    let mut staged = Vec::new();
    if let Some(table) = keyspace.table_by_namespace_key(&ns, table_name) {
        for row in table.rows.values() {
            if !eval_compiled_expr_public(&compiled, row) {
                continue;
            }
            let primary_key = extract_primary_key_from_row(&schema, row)?;
            let mut next_row = row.clone();
            for (idx, value) in &update_indices {
                next_row.values[*idx] = value.clone();
            }
            staged.push((primary_key, next_row));
            if limit.is_some_and(|max| staged.len() >= max) {
                break;
            }
        }
    }
    for (primary_key, row) in staged {
        apply_upsert_once(
            catalog,
            keyspace,
            project_id,
            scope_id,
            table_name,
            primary_key,
            row,
            commit_seq,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_update_where_expr_internal(
    catalog: &mut Catalog,
    keyspace: &mut Keyspace,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    predicate: &crate::query::plan::Expr,
    updates: &[(String, TableUpdateExpr)],
    limit: Option<usize>,
    commit_seq: u64,
) -> Result<(), AedbError> {
    let ns = namespace_key(project_id, scope_id);
    let schema = catalog
        .tables
        .get(&(ns.clone(), table_name.to_string()))
        .ok_or_else(|| AedbError::NotFound {
            resource_type: ErrorResourceType::Table,
            resource_id: format!("{project_id}.{scope_id}.{table_name}"),
        })?
        .clone();
    let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let compiled = compile_expr(predicate, &columns, table_name)
        .map_err(|e| AedbError::Validation(format!("invalid predicate: {e:?}")))?;
    let mut update_indices = Vec::with_capacity(updates.len());
    for (column, expr) in updates {
        let Some(idx) = schema.columns.iter().position(|c| c.name == *column) else {
            return Err(AedbError::UnknownColumn {
                table: table_name.to_string(),
                column: column.clone(),
            });
        };
        let resolved = match expr {
            TableUpdateExpr::Value(value) => ResolvedTableUpdateExpr::Value(value.clone()),
            TableUpdateExpr::CopyColumn(source) => {
                let Some(source_idx) = schema.columns.iter().position(|c| c.name == *source) else {
                    return Err(AedbError::UnknownColumn {
                        table: table_name.to_string(),
                        column: source.clone(),
                    });
                };
                ResolvedTableUpdateExpr::CopyColumn(source_idx)
            }
            TableUpdateExpr::AddI64(delta) => ResolvedTableUpdateExpr::AddI64(*delta),
            TableUpdateExpr::Coalesce(fallback) => {
                ResolvedTableUpdateExpr::Coalesce(fallback.clone())
            }
        };
        update_indices.push((idx, resolved));
    }
    let mut staged = Vec::new();
    if let Some(table) = keyspace.table_by_namespace_key(&ns, table_name) {
        for row in table.rows.values() {
            if !eval_compiled_expr_public(&compiled, row) {
                continue;
            }
            let primary_key = extract_primary_key_from_row(&schema, row)?;
            let mut next_row = row.clone();
            for (idx, expr) in &update_indices {
                let next_value = evaluate_table_update_expr(expr, &next_row, *idx)?;
                next_row.values[*idx] = next_value;
            }
            staged.push((primary_key, next_row));
            if limit.is_some_and(|max| staged.len() >= max) {
                break;
            }
        }
    }
    for (primary_key, row) in staged {
        apply_upsert_once(
            catalog,
            keyspace,
            project_id,
            scope_id,
            table_name,
            primary_key,
            row,
            commit_seq,
        )?;
    }
    Ok(())
}

enum ResolvedTableUpdateExpr {
    Value(Value),
    CopyColumn(usize),
    AddI64(i64),
    Coalesce(Value),
}

fn evaluate_table_update_expr(
    expr: &ResolvedTableUpdateExpr,
    row: &Row,
    target_idx: usize,
) -> Result<Value, AedbError> {
    let current = row
        .values
        .get(target_idx)
        .ok_or_else(|| AedbError::Validation("target column out of bounds".into()))?;
    match expr {
        ResolvedTableUpdateExpr::Value(value) => Ok(value.clone()),
        ResolvedTableUpdateExpr::CopyColumn(source_idx) => {
            let value = row.values.get(*source_idx).ok_or_else(|| {
                AedbError::Validation("source column out of bounds during update".into())
            })?;
            Ok(value.clone())
        }
        ResolvedTableUpdateExpr::AddI64(delta) => match current {
            Value::Integer(value) => value
                .checked_add(*delta)
                .map(Value::Integer)
                .ok_or(AedbError::Overflow),
            _ => Err(AedbError::Validation(
                "AddI64 requires Integer current value".into(),
            )),
        },
        ResolvedTableUpdateExpr::Coalesce(fallback) => {
            if matches!(current, Value::Null) {
                Ok(fallback.clone())
            } else {
                Ok(current.clone())
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_referencing_foreign_keys(
    catalog: &Catalog,
    keyspace: &mut Keyspace,
    ref_project_id: &str,
    ref_scope_id: &str,
    ref_table_name: &str,
    ref_row: &Row,
    commit_seq: u64,
) -> Result<(), AedbError> {
    let target_ns = namespace_key(ref_project_id, ref_scope_id);
    for ((dep_ns, dep_table_name), dep_schema) in &catalog.tables {
        for fk in &dep_schema.foreign_keys {
            if namespace_key(&fk.references_project_id, &fk.references_scope_id) != target_ns
                || fk.references_table != ref_table_name
            {
                continue;
            }
            let mut referenced_vals = Vec::with_capacity(fk.references_columns.len());
            for ref_col in &fk.references_columns {
                let idx = catalog
                    .tables
                    .get(&(target_ns.clone(), ref_table_name.to_string()))
                    .and_then(|s| s.columns.iter().position(|c| c.name == *ref_col))
                    .ok_or_else(|| {
                        AedbError::Validation(format!(
                            "referenced column missing for fk {}",
                            fk.name
                        ))
                    })?;
                referenced_vals.push(ref_row.values[idx].clone());
            }
            let Some(dep_table) = keyspace
                .table_by_namespace_key(dep_ns, dep_table_name)
                .cloned()
            else {
                continue;
            };
            let mut matched_pks: Vec<EncodedKey> = Vec::new();
            for (pk, row) in dep_table.rows {
                let mut matched = true;
                for (i, fk_col) in fk.columns.iter().enumerate() {
                    let fk_idx = dep_schema
                        .columns
                        .iter()
                        .position(|c| c.name == *fk_col)
                        .ok_or_else(|| AedbError::Validation("fk column missing".into()))?;
                    if row.values.get(fk_idx) != referenced_vals.get(i) {
                        matched = false;
                        break;
                    }
                }
                if matched {
                    matched_pks.push(pk);
                }
            }
            if matched_pks.is_empty() {
                continue;
            }
            match fk.on_delete {
                ForeignKeyAction::Restrict | ForeignKeyAction::NoAction => {
                    return Err(AedbError::ForeignKeyViolation {
                        fk_name: fk.name.clone(),
                        table: dep_table_name.clone(),
                        ref_table: ref_table_name.to_string(),
                        ref_key: "restrict".into(),
                    });
                }
                ForeignKeyAction::Cascade => {
                    for pk_encoded in matched_pks {
                        let (dep_project_id, dep_scope_id) = split_namespace(dep_ns)
                            .ok_or_else(|| AedbError::Validation("invalid namespace key".into()))?;
                        apply_delete_internal_encoded(
                            catalog,
                            keyspace,
                            &dep_project_id,
                            &dep_scope_id,
                            dep_table_name,
                            &pk_encoded,
                            commit_seq,
                        )?;
                    }
                }
                ForeignKeyAction::SetNull | ForeignKeyAction::SetDefault => {
                    for pk_encoded in matched_pks {
                        let (dep_project_id, dep_scope_id) = split_namespace(dep_ns)
                            .ok_or_else(|| AedbError::Validation("invalid namespace key".into()))?;
                        let dep_ns_key = namespace_key(&dep_project_id, &dep_scope_id);
                        let dep_schema = catalog
                            .tables
                            .get(&(dep_ns_key.clone(), dep_table_name.clone()))
                            .ok_or_else(|| {
                                AedbError::Validation("dependent table missing".into())
                            })?;
                        let mut row = keyspace
                            .get_row_by_encoded(
                                &dep_project_id,
                                &dep_scope_id,
                                dep_table_name,
                                &pk_encoded,
                            )
                            .cloned()
                            .ok_or_else(|| AedbError::Validation("dependent row missing".into()))?;
                        for fk_col in &fk.columns {
                            if let Some(idx) =
                                dep_schema.columns.iter().position(|c| c.name == *fk_col)
                            {
                                match fk.on_delete {
                                    ForeignKeyAction::SetNull => row.values[idx] = Value::Null,
                                    ForeignKeyAction::SetDefault => {
                                        if let Some(default_value) =
                                            dep_schema.constraints.iter().find_map(|c| match c {
                                                Constraint::Default { column, value }
                                                    if column == fk_col =>
                                                {
                                                    Some(value.clone())
                                                }
                                                _ => None,
                                            })
                                        {
                                            row.values[idx] = default_value;
                                        } else {
                                            row.values[idx] = Value::Null;
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        validate_row_constraints(
                            catalog,
                            keyspace,
                            dep_schema,
                            &dep_project_id,
                            &dep_scope_id,
                            dep_table_name,
                            &extract_primary_key_from_row(dep_schema, &row)?,
                            &row,
                            None,
                        )?;
                        keyspace.upsert_row_by_encoded_pk(
                            &dep_project_id,
                            &dep_scope_id,
                            dep_table_name,
                            pk_encoded.clone(),
                            row.clone(),
                            commit_seq,
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

fn apply_delete_internal_encoded(
    catalog: &Catalog,
    keyspace: &mut Keyspace,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    primary_key: &EncodedKey,
    commit_seq: u64,
) -> Result<(), AedbError> {
    let ns = namespace_key(project_id, scope_id);
    let schema = catalog
        .tables
        .get(&(ns.clone(), table_name.to_string()))
        .ok_or_else(|| AedbError::Validation("table missing".into()))?
        .clone();
    let Some(old_row) = keyspace
        .get_row_by_encoded(project_id, scope_id, table_name, primary_key)
        .cloned()
    else {
        return Ok(());
    };
    let primary_values = extract_primary_key_from_row(&schema, &old_row)?;
    handle_referencing_foreign_keys(
        catalog, keyspace, project_id, scope_id, table_name, &old_row, commit_seq,
    )?;
    let removed =
        keyspace.delete_row_by_encoded(project_id, scope_id, table_name, primary_key, commit_seq);
    maintain_secondary_indexes(
        catalog,
        keyspace,
        &schema,
        project_id,
        scope_id,
        table_name,
        &primary_values,
        removed.as_ref(),
        None,
    )?;
    Ok(())
}

fn split_namespace(ns: &str) -> Option<(String, String)> {
    let (project, scope) = ns.split_once("::")?;
    Some((project.to_string(), scope.to_string()))
}

#[allow(clippy::too_many_arguments)]
fn apply_upsert_on_conflict_once(
    catalog: &Catalog,
    keyspace: &mut Keyspace,
    project_id: String,
    scope_id: String,
    table_name: String,
    mut row: Row,
    conflict_target: ConflictTarget,
    conflict_action: ConflictAction,
    commit_seq: u64,
) -> Result<(), AedbError> {
    let ns = namespace_key(&project_id, &scope_id);
    let schema = catalog
        .tables
        .get(&(ns.clone(), table_name.clone()))
        .ok_or_else(|| AedbError::Validation("table missing".into()))?
        .clone();

    apply_default_constraints(&schema, &mut row)?;
    let proposed_pk = extract_primary_key_from_row(&schema, &row)?;
    let existing = find_existing_conflict_row(
        catalog,
        keyspace,
        &schema,
        &project_id,
        &scope_id,
        &table_name,
        &row,
        &conflict_target,
    )?;

    if let Some((existing_pk, existing_row)) = existing {
        let mut final_row = existing_row.clone();
        let should_write = !matches!(conflict_action, ConflictAction::DoNothing);
        match conflict_action {
            ConflictAction::DoNothing => {}
            ConflictAction::DoMerge => {
                for idx in 0..final_row.values.len() {
                    if let Some(proposed) = row.values.get(idx)
                        && !matches!(proposed, Value::Null)
                    {
                        final_row.values[idx] = proposed.clone();
                    }
                }
            }
            ConflictAction::DoUpdate(updates) => {
                for (name, value) in updates {
                    if let Some(col_idx) = schema.columns.iter().position(|c| c.name == name) {
                        final_row.values[col_idx] = value;
                    }
                }
            }
            ConflictAction::DoUpdateWith(updates) => {
                for (name, expr) in updates {
                    if let Some(col_idx) = schema.columns.iter().position(|c| c.name == name) {
                        final_row.values[col_idx] =
                            evaluate_update_expr(&schema, &existing_row, &row, expr)?;
                    }
                }
            }
        }
        if should_write {
            let existing_pk_values = extract_primary_key_from_row(&schema, &existing_row)?;
            validate_row_constraints(
                catalog,
                keyspace,
                &schema,
                &project_id,
                &scope_id,
                &table_name,
                &existing_pk_values,
                &final_row,
                Some(&existing_row),
            )?;
            handle_referencing_foreign_keys_on_update(
                catalog,
                keyspace,
                &project_id,
                &scope_id,
                &table_name,
                &schema,
                &existing_row,
                &final_row,
                commit_seq,
            )?;
            keyspace.upsert_row_by_encoded_pk(
                &project_id,
                &scope_id,
                &table_name,
                existing_pk.clone(),
                final_row.clone(),
                commit_seq,
            );
            maintain_secondary_indexes(
                catalog,
                keyspace,
                &schema,
                &project_id,
                &scope_id,
                &table_name,
                &existing_pk_values,
                Some(&existing_row),
                Some(&final_row),
            )?;
        }
    } else {
        validate_row_constraints(
            catalog,
            keyspace,
            &schema,
            &project_id,
            &scope_id,
            &table_name,
            &proposed_pk,
            &row,
            None,
        )?;
        keyspace.upsert_row(
            &project_id,
            &scope_id,
            &table_name,
            proposed_pk.clone(),
            row.clone(),
            commit_seq,
        );
        maintain_secondary_indexes(
            catalog,
            keyspace,
            &schema,
            &project_id,
            &scope_id,
            &table_name,
            &proposed_pk,
            None,
            Some(&row),
        )?;
    }
    Ok(())
}

fn evaluate_update_expr(
    schema: &TableSchema,
    existing: &Row,
    proposed: &Row,
    expr: UpdateExpr,
) -> Result<Value, AedbError> {
    match expr {
        UpdateExpr::Value(v) => Ok(v),
        UpdateExpr::Existing(column) => {
            let idx = schema
                .columns
                .iter()
                .position(|c| c.name == column)
                .ok_or_else(|| AedbError::Validation("update expr column missing".into()))?;
            Ok(existing.values[idx].clone())
        }
        UpdateExpr::Proposed(column) => {
            let idx = schema
                .columns
                .iter()
                .position(|c| c.name == column)
                .ok_or_else(|| AedbError::Validation("update expr column missing".into()))?;
            Ok(proposed.values[idx].clone())
        }
        UpdateExpr::AddI64 {
            existing_column,
            proposed_column,
        } => {
            let e_idx = schema
                .columns
                .iter()
                .position(|c| c.name == existing_column)
                .ok_or_else(|| AedbError::Validation("update expr column missing".into()))?;
            let p_idx = schema
                .columns
                .iter()
                .position(|c| c.name == proposed_column)
                .ok_or_else(|| AedbError::Validation("update expr column missing".into()))?;
            let e = match existing.values[e_idx] {
                Value::Integer(v) => v,
                _ => {
                    return Err(AedbError::Validation(
                        "AddI64 requires integer existing value".into(),
                    ));
                }
            };
            let p = match proposed.values[p_idx] {
                Value::Integer(v) => v,
                _ => {
                    return Err(AedbError::Validation(
                        "AddI64 requires integer proposed value".into(),
                    ));
                }
            };
            Ok(Value::Integer(e.saturating_add(p)))
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_referencing_foreign_keys_on_update(
    catalog: &Catalog,
    keyspace: &mut Keyspace,
    ref_project_id: &str,
    ref_scope_id: &str,
    ref_table_name: &str,
    ref_schema: &TableSchema,
    old_row: &Row,
    new_row: &Row,
    commit_seq: u64,
) -> Result<(), AedbError> {
    let target_ns = namespace_key(ref_project_id, ref_scope_id);
    for ((dep_ns, dep_table_name), dep_schema) in &catalog.tables {
        for fk in &dep_schema.foreign_keys {
            if namespace_key(&fk.references_project_id, &fk.references_scope_id) != target_ns
                || fk.references_table != ref_table_name
            {
                continue;
            }
            let mut old_values = Vec::with_capacity(fk.references_columns.len());
            let mut new_values = Vec::with_capacity(fk.references_columns.len());
            for ref_col in &fk.references_columns {
                let idx = ref_schema
                    .columns
                    .iter()
                    .position(|c| c.name == *ref_col)
                    .ok_or_else(|| AedbError::Validation("referenced column missing".into()))?;
                old_values.push(old_row.values[idx].clone());
                new_values.push(new_row.values[idx].clone());
            }
            if old_values == new_values {
                continue;
            }
            let Some(dep_table) = keyspace
                .table_by_namespace_key(dep_ns, dep_table_name)
                .cloned()
            else {
                continue;
            };
            let mut matched_pks: Vec<EncodedKey> = Vec::new();
            for (pk, row) in dep_table.rows {
                let mut matched = true;
                for (i, fk_col) in fk.columns.iter().enumerate() {
                    let fk_idx = dep_schema
                        .columns
                        .iter()
                        .position(|c| c.name == *fk_col)
                        .ok_or_else(|| AedbError::Validation("fk column missing".into()))?;
                    if row.values.get(fk_idx) != old_values.get(i) {
                        matched = false;
                        break;
                    }
                }
                if matched {
                    matched_pks.push(pk);
                }
            }
            if matched_pks.is_empty() {
                continue;
            }
            match fk.on_update {
                ForeignKeyAction::Restrict | ForeignKeyAction::NoAction => {
                    return Err(AedbError::Validation(format!(
                        "foreign key update restrict violation: {}",
                        fk.name
                    )));
                }
                ForeignKeyAction::Cascade
                | ForeignKeyAction::SetNull
                | ForeignKeyAction::SetDefault => {
                    for pk_encoded in matched_pks {
                        let (dep_project_id, dep_scope_id) = split_namespace(dep_ns)
                            .ok_or_else(|| AedbError::Validation("invalid namespace key".into()))?;
                        let dep_schema = catalog
                            .tables
                            .get(&(
                                namespace_key(&dep_project_id, &dep_scope_id),
                                dep_table_name.clone(),
                            ))
                            .ok_or_else(|| {
                                AedbError::Validation("dependent table missing".into())
                            })?;
                        let mut row = keyspace
                            .get_row_by_encoded(
                                &dep_project_id,
                                &dep_scope_id,
                                dep_table_name,
                                &pk_encoded,
                            )
                            .cloned()
                            .ok_or_else(|| AedbError::Validation("dependent row missing".into()))?;
                        for (i, fk_col) in fk.columns.iter().enumerate() {
                            if let Some(idx) =
                                dep_schema.columns.iter().position(|c| c.name == *fk_col)
                            {
                                match fk.on_update {
                                    ForeignKeyAction::Cascade => {
                                        row.values[idx] = new_values[i].clone()
                                    }
                                    ForeignKeyAction::SetNull => row.values[idx] = Value::Null,
                                    ForeignKeyAction::SetDefault => {
                                        row.values[idx] = dep_schema
                                            .constraints
                                            .iter()
                                            .find_map(|c| match c {
                                                Constraint::Default { column, value }
                                                    if column == fk_col =>
                                                {
                                                    Some(value.clone())
                                                }
                                                _ => None,
                                            })
                                            .unwrap_or(Value::Null);
                                    }
                                    _ => {}
                                }
                            }
                        }
                        validate_row_constraints(
                            catalog,
                            keyspace,
                            dep_schema,
                            &dep_project_id,
                            &dep_scope_id,
                            dep_table_name,
                            &extract_primary_key_from_row(dep_schema, &row)?,
                            &row,
                            None,
                        )?;
                        keyspace.upsert_row_by_encoded_pk(
                            &dep_project_id,
                            &dep_scope_id,
                            dep_table_name,
                            pk_encoded,
                            row,
                            commit_seq,
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

fn prevalidate_ddl_with_data(
    catalog: &Catalog,
    keyspace: &Keyspace,
    op: &crate::catalog::DdlOperation,
) -> Result<(), AedbError> {
    match op {
        crate::catalog::DdlOperation::DropTable {
            project_id,
            scope_id,
            table_name,
            ..
        } => {
            let target_ns = namespace_key(project_id, scope_id);
            for ((dep_ns, dep_table_name), dep_schema) in &catalog.tables {
                for fk in &dep_schema.foreign_keys {
                    if namespace_key(&fk.references_project_id, &fk.references_scope_id)
                        == target_ns
                        && fk.references_table == *table_name
                    {
                        let has_rows = keyspace
                            .table_by_namespace_key(dep_ns, dep_table_name)
                            .map(|t| !t.rows.is_empty())
                            .unwrap_or(false);
                        if has_rows {
                            return Err(AedbError::Validation(format!(
                                "cannot drop table; referenced by foreign key {}",
                                fk.name
                            )));
                        }
                    }
                }
            }
            Ok(())
        }
        crate::catalog::DdlOperation::AlterTable {
            project_id,
            scope_id,
            table_name,
            alteration,
        } => {
            let ns = namespace_key(project_id, scope_id);
            let schema = catalog
                .tables
                .get(&(ns, table_name.clone()))
                .ok_or_else(|| AedbError::Validation("table missing".into()))?;
            match alteration {
                crate::catalog::schema::TableAlteration::AddConstraint(constraint) => {
                    if let Some(table) = keyspace
                        .table_by_namespace_key(&namespace_key(project_id, scope_id), table_name)
                    {
                        for row in table.rows.values() {
                            let pk = extract_primary_key_from_row(schema, row)?;
                            let mut tmp_schema = schema.clone();
                            tmp_schema.constraints.push(constraint.clone());
                            validate_row_constraints(
                                catalog,
                                keyspace,
                                &tmp_schema,
                                project_id,
                                scope_id,
                                table_name,
                                &pk,
                                row,
                                None,
                            )?;
                        }
                    }
                    Ok(())
                }
                crate::catalog::schema::TableAlteration::AddForeignKey(fk) => {
                    validate_foreign_key_definition(catalog, project_id, fk)?;
                    if let Some(table) = keyspace
                        .table_by_namespace_key(&namespace_key(project_id, scope_id), table_name)
                    {
                        let mut tmp_schema = schema.clone();
                        tmp_schema.foreign_keys.push(fk.clone());
                        for row in table.rows.values() {
                            validate_foreign_keys(catalog, keyspace, &tmp_schema, row, None)?;
                        }
                    }
                    Ok(())
                }
                _ => Ok(()),
            }
        }
        _ => Ok(()),
    }
}

fn validate_foreign_key_definition(
    catalog: &Catalog,
    project_id: &str,
    fk: &crate::catalog::schema::ForeignKey,
) -> Result<(), AedbError> {
    if fk.columns.is_empty() || fk.references_columns.is_empty() {
        return Err(AedbError::Validation(
            "foreign key columns cannot be empty".into(),
        ));
    }
    if fk.columns.len() != fk.references_columns.len() {
        return Err(AedbError::Validation(
            "foreign key columns length mismatch".into(),
        ));
    }
    if fk.references_project_id != project_id {
        if fk.references_project_id != "_global" {
            return Err(AedbError::Validation(
                "project foreign keys may only reference _global or same project".into(),
            ));
        }
        if matches!(fk.on_delete, ForeignKeyAction::Cascade) {
            return Err(AedbError::Validation(
                "project -> _global foreign keys cannot use ON DELETE CASCADE".into(),
            ));
        }
    }
    if project_id == "_global" && fk.references_project_id != "_global" {
        return Err(AedbError::Validation(
            "_global tables cannot reference project tables".into(),
        ));
    }
    let ref_key = (
        namespace_key(&fk.references_project_id, &fk.references_scope_id),
        fk.references_table.clone(),
    );
    let ref_table = catalog
        .tables
        .get(&ref_key)
        .ok_or_else(|| AedbError::NotFound {
            resource_type: ErrorResourceType::Table,
            resource_id: format!(
                "{}.{}.{}",
                fk.references_project_id, fk.references_scope_id, fk.references_table
            ),
        })?;
    for col in &fk.references_columns {
        if !ref_table.columns.iter().any(|c| c.name == *col) {
            return Err(AedbError::UnknownColumn {
                table: fk.references_table.clone(),
                column: col.clone(),
            });
        }
    }
    let refs_pk = ref_table.primary_key == fk.references_columns;
    let refs_unique = catalog.indexes.values().any(|idx| {
        idx.project_id == fk.references_project_id
            && idx.scope_id == fk.references_scope_id
            && idx.table_name == fk.references_table
            && idx.columns == fk.references_columns
            && matches!(
                idx.index_type,
                crate::catalog::schema::IndexType::UniqueHash
            )
    });
    if !refs_pk && !refs_unique {
        return Err(AedbError::Validation(
            "foreign key references must target PK or unique index".into(),
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn maintain_secondary_indexes(
    catalog: &Catalog,
    keyspace: &mut Keyspace,
    schema: &TableSchema,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    primary_key: &[crate::catalog::types::Value],
    old_row: Option<&crate::catalog::types::Row>,
    new_row: Option<&crate::catalog::types::Row>,
) -> Result<(), AedbError> {
    let table = keyspace
        .table_by_namespace_key_mut(&namespace_key(project_id, scope_id), table_name)
        .ok_or_else(|| AedbError::NotFound {
            resource_type: ErrorResourceType::Table,
            resource_id: format!("{project_id}.{scope_id}.{table_name}"),
        })?;
    let encoded_pk = EncodedKey::from_values(primary_key);
    let modified_columns_mask = calculate_modified_columns_bitmask(schema, old_row, new_row);

    for ((p, t, idx_name), idx_def) in &catalog.indexes {
        if p != &namespace_key(project_id, scope_id) || t != table_name {
            continue;
        }
        if old_row.is_some()
            && new_row.is_some()
            && idx_def.partial_filter.is_none()
            && idx_def.columns_bitmask & modified_columns_mask == 0
        {
            continue;
        }
        let index = table
            .indexes
            .entry(idx_name.clone())
            .or_insert_with(|| SecondaryIndex {
                store: match idx_def.index_type {
                    crate::catalog::schema::IndexType::BTree
                    | crate::catalog::schema::IndexType::Art => {
                        SecondaryIndexStore::BTree(im::OrdMap::new())
                    }
                    crate::catalog::schema::IndexType::Hash => {
                        SecondaryIndexStore::Hash(im::HashMap::new())
                    }
                    crate::catalog::schema::IndexType::UniqueHash => {
                        SecondaryIndexStore::UniqueHash(im::HashMap::new())
                    }
                },
                columns_bitmask: idx_def.columns_bitmask,
                partial_filter: idx_def.partial_filter.clone(),
            });
        if let Some(before) = old_row
            && index.should_include_row(before, schema, table_name)?
        {
            let old_key = extract_index_key_encoded(before, schema, &idx_def.columns)?;
            index.remove(&old_key, &encoded_pk);
        }
        if let Some(after) = new_row
            && index.should_include_row(after, schema, table_name)?
        {
            if matches!(
                idx_def.index_type,
                crate::catalog::schema::IndexType::UniqueHash
            ) && has_null_in_columns(schema, after, &idx_def.columns)?
            {
                continue;
            }
            let new_key = extract_index_key_encoded(after, schema, &idx_def.columns)?;
            if matches!(
                idx_def.index_type,
                crate::catalog::schema::IndexType::UniqueHash
            ) && index
                .unique_existing(&new_key)
                .is_some_and(|existing| existing != encoded_pk)
            {
                return Err(AedbError::Validation(format!(
                    "unique index violation on {idx_name}"
                )));
            }
            index.insert(new_key, encoded_pk.clone());
        }
    }
    Ok(())
}

fn rebuild_index_for_table(
    keyspace: &mut Keyspace,
    catalog: &Catalog,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    index_name: &str,
    columns: &[String],
) -> Result<(), AedbError> {
    let ns = namespace_key(project_id, scope_id);
    let schema = catalog
        .tables
        .get(&(ns.clone(), table_name.to_string()))
        .ok_or_else(|| AedbError::Validation("table missing".into()))?;
    let table = keyspace.table_mut_by_namespace_key(&ns, table_name);

    let mut index = crate::storage::keyspace::SecondaryIndex {
        store: match catalog
            .indexes
            .get(&(ns.clone(), table_name.to_string(), index_name.to_string()))
            .map(|d| &d.index_type)
            .unwrap_or(&crate::catalog::schema::IndexType::BTree)
        {
            crate::catalog::schema::IndexType::BTree | crate::catalog::schema::IndexType::Art => {
                crate::storage::keyspace::SecondaryIndexStore::BTree(im::OrdMap::new())
            }
            crate::catalog::schema::IndexType::Hash => {
                crate::storage::keyspace::SecondaryIndexStore::Hash(im::HashMap::new())
            }
            crate::catalog::schema::IndexType::UniqueHash => {
                crate::storage::keyspace::SecondaryIndexStore::UniqueHash(im::HashMap::new())
            }
        },
        columns_bitmask: catalog
            .indexes
            .get(&(ns.clone(), table_name.to_string(), index_name.to_string()))
            .map(|d| d.columns_bitmask)
            .unwrap_or(0),
        partial_filter: catalog
            .indexes
            .get(&(ns.clone(), table_name.to_string(), index_name.to_string()))
            .and_then(|d| d.partial_filter.clone()),
    };
    for (pk, row) in &table.rows {
        if index.should_include_row(row, schema, table_name)? {
            if matches!(
                catalog
                    .indexes
                    .get(&(ns.clone(), table_name.to_string(), index_name.to_string()))
                    .map(|d| &d.index_type),
                Some(crate::catalog::schema::IndexType::UniqueHash)
            ) && has_null_in_columns(schema, row, columns)?
            {
                continue;
            }
            let index_key = extract_index_key_encoded(row, schema, columns)?;
            if matches!(
                catalog
                    .indexes
                    .get(&(ns.clone(), table_name.to_string(), index_name.to_string()))
                    .map(|d| &d.index_type),
                Some(crate::catalog::schema::IndexType::UniqueHash)
            ) && index
                .unique_existing(&index_key)
                .is_some_and(|existing| existing != *pk)
            {
                return Err(AedbError::Validation(format!(
                    "unique index violation on {index_name}"
                )));
            }
            index.insert(index_key, pk.clone());
        }
    }
    table.indexes.insert(index_name.to_string(), index);
    Ok(())
}

fn has_null_in_columns(
    schema: &TableSchema,
    row: &Row,
    columns: &[String],
) -> Result<bool, AedbError> {
    for col in columns {
        let idx = schema
            .columns
            .iter()
            .position(|c| c.name == *col)
            .ok_or_else(|| AedbError::Validation(format!("column not found: {col}")))?;
        if matches!(row.values.get(idx), Some(Value::Null) | None) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn calculate_modified_columns_bitmask(
    schema: &TableSchema,
    old_row: Option<&crate::catalog::types::Row>,
    new_row: Option<&crate::catalog::types::Row>,
) -> u128 {
    match (old_row, new_row) {
        (Some(before), Some(after)) => {
            let mut mask = 0u128;
            for idx in 0..schema.columns.len() {
                if idx >= 128 {
                    break;
                }
                let lhs = before.values.get(idx);
                let rhs = after.values.get(idx);
                if lhs != rhs {
                    mask |= 1u128 << idx;
                }
            }
            mask
        }
        _ => u128::MAX,
    }
}
