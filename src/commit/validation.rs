use crate::catalog::schema::TableSchema;
use crate::catalog::types::{ColumnType, Row, Value};
use crate::catalog::{Catalog, DdlOperation, KV_INDEX_TABLE, ResourceType, namespace_key};
use crate::config::AedbConfig;
use crate::error::AedbError;
use crate::error::ResourceType as ErrorResourceType;
use crate::permission::{CallerContext, Permission};
use crate::query::plan::Expr;
use primitive_types::U256;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConflictTarget {
    PrimaryKey,
    Index(String),
    Columns(Vec<String>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConflictAction {
    DoNothing,
    DoMerge,
    DoUpdate(Vec<(String, Value)>),
    DoUpdateWith(Vec<(String, UpdateExpr)>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum UpdateExpr {
    Value(Value),
    Existing(String),
    Proposed(String),
    AddI64 {
        existing_column: String,
        proposed_column: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TableUpdateExpr {
    Value(Value),
    CopyColumn(String),
    AddI64(i64),
    Coalesce(Value),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Mutation {
    Upsert {
        project_id: String,
        scope_id: String,
        table_name: String,
        primary_key: Vec<Value>,
        row: Row,
    },
    UpsertBatch {
        project_id: String,
        scope_id: String,
        table_name: String,
        rows: Vec<Row>,
    },
    UpsertOnConflict {
        project_id: String,
        scope_id: String,
        table_name: String,
        row: Row,
        conflict_target: ConflictTarget,
        conflict_action: ConflictAction,
    },
    UpsertBatchOnConflict {
        project_id: String,
        scope_id: String,
        table_name: String,
        rows: Vec<Row>,
        conflict_target: ConflictTarget,
        conflict_action: ConflictAction,
    },
    Delete {
        project_id: String,
        scope_id: String,
        table_name: String,
        primary_key: Vec<Value>,
    },
    DeleteWhere {
        project_id: String,
        scope_id: String,
        table_name: String,
        predicate: Expr,
        limit: Option<usize>,
    },
    UpdateWhere {
        project_id: String,
        scope_id: String,
        table_name: String,
        predicate: Expr,
        updates: Vec<(String, Value)>,
        limit: Option<usize>,
    },
    UpdateWhereExpr {
        project_id: String,
        scope_id: String,
        table_name: String,
        predicate: Expr,
        updates: Vec<(String, TableUpdateExpr)>,
        limit: Option<usize>,
    },
    Ddl(DdlOperation),
    KvSet {
        project_id: String,
        scope_id: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    KvDel {
        project_id: String,
        scope_id: String,
        key: Vec<u8>,
    },
    KvIncU256 {
        project_id: String,
        scope_id: String,
        key: Vec<u8>,
        amount_be: [u8; 32],
    },
    KvDecU256 {
        project_id: String,
        scope_id: String,
        key: Vec<u8>,
        amount_be: [u8; 32],
    },
    TableIncU256 {
        project_id: String,
        scope_id: String,
        table_name: String,
        primary_key: Vec<Value>,
        column: String,
        amount_be: [u8; 32],
    },
    TableDecU256 {
        project_id: String,
        scope_id: String,
        table_name: String,
        primary_key: Vec<Value>,
        column: String,
        amount_be: [u8; 32],
    },
}

/// Early validation of KV mutation sizes to prevent DoS via oversized keys/values.
/// This check happens BEFORE the mutation is queued, preventing memory allocation
/// of oversized data. Called at the API boundary before committing.
pub fn validate_kv_sizes_early(mutation: &Mutation, config: &AedbConfig) -> Result<(), AedbError> {
    match mutation {
        Mutation::KvSet { key, value, .. } => {
            if key.len() > config.max_kv_key_bytes {
                return Err(AedbError::Validation(format!(
                    "kv key size {} exceeds maximum {}",
                    key.len(),
                    config.max_kv_key_bytes
                )));
            }
            if value.len() > config.max_kv_value_bytes {
                return Err(AedbError::Validation(format!(
                    "kv value size {} exceeds maximum {}",
                    value.len(),
                    config.max_kv_value_bytes
                )));
            }
        }
        Mutation::KvDel { key, .. }
        | Mutation::KvIncU256 { key, .. }
        | Mutation::KvDecU256 { key, .. } => {
            if key.len() > config.max_kv_key_bytes {
                return Err(AedbError::Validation(format!(
                    "kv key size {} exceeds maximum {}",
                    key.len(),
                    config.max_kv_key_bytes
                )));
            }
        }
        // Other mutation types have different size constraints validated elsewhere
        _ => {}
    }
    Ok(())
}

pub fn validate_mutation(catalog: &Catalog, mutation: &Mutation) -> Result<(), AedbError> {
    validate_mutation_with_config(catalog, mutation, &AedbConfig::default())
}

pub fn validate_mutation_with_config(
    catalog: &Catalog,
    mutation: &Mutation,
    config: &AedbConfig,
) -> Result<(), AedbError> {
    match mutation {
        Mutation::Upsert {
            project_id,
            scope_id,
            table_name,
            primary_key,
            row,
        } => {
            ensure_not_managed_table(table_name)?;
            validate_upsert_row(catalog, project_id, scope_id, table_name, primary_key, row)
        }
        Mutation::UpsertBatch {
            project_id,
            scope_id,
            table_name,
            rows,
        } => {
            ensure_not_managed_table(table_name)?;
            let schema = table_schema(catalog, project_id, scope_id, table_name)?;
            for row in rows {
                let primary_key = extract_primary_key(schema, row)?;
                validate_row_against_schema(schema, &primary_key, row)?;
            }
            Ok(())
        }
        Mutation::UpsertOnConflict {
            project_id,
            scope_id,
            table_name,
            row,
            conflict_target,
            conflict_action,
        } => {
            ensure_not_managed_table(table_name)?;
            let schema = table_schema(catalog, project_id, scope_id, table_name)?;
            let primary_key = extract_primary_key(schema, row)?;
            validate_row_against_schema(schema, &primary_key, row)?;
            validate_conflict_target(
                catalog,
                schema,
                project_id,
                scope_id,
                table_name,
                conflict_target,
            )?;
            validate_conflict_action(schema, conflict_action)?;
            Ok(())
        }
        Mutation::UpsertBatchOnConflict {
            project_id,
            scope_id,
            table_name,
            rows,
            conflict_target,
            conflict_action,
        } => {
            ensure_not_managed_table(table_name)?;
            let schema = table_schema(catalog, project_id, scope_id, table_name)?;
            for row in rows {
                let primary_key = extract_primary_key(schema, row)?;
                validate_row_against_schema(schema, &primary_key, row)?;
            }
            validate_conflict_target(
                catalog,
                schema,
                project_id,
                scope_id,
                table_name,
                conflict_target,
            )?;
            validate_conflict_action(schema, conflict_action)?;
            Ok(())
        }
        Mutation::Delete {
            project_id,
            scope_id,
            table_name,
            primary_key,
        } => {
            ensure_not_managed_table(table_name)?;
            let schema = table_schema(catalog, project_id, scope_id, table_name)?;
            if primary_key.len() != schema.primary_key.len() {
                return Err(AedbError::Validation("primary key length mismatch".into()));
            }
            Ok(())
        }
        Mutation::DeleteWhere {
            project_id,
            scope_id,
            table_name,
            predicate,
            limit,
        } => {
            ensure_not_managed_table(table_name)?;
            let schema = table_schema(catalog, project_id, scope_id, table_name)?;
            validate_expr_columns(schema, predicate)?;
            if let Some(limit) = limit
                && *limit == 0
            {
                return Err(AedbError::Validation("limit must be > 0".into()));
            }
            Ok(())
        }
        Mutation::UpdateWhere {
            project_id,
            scope_id,
            table_name,
            predicate,
            updates,
            limit,
        } => {
            ensure_not_managed_table(table_name)?;
            let schema = table_schema(catalog, project_id, scope_id, table_name)?;
            validate_expr_columns(schema, predicate)?;
            if updates.is_empty() {
                return Err(AedbError::Validation("updates cannot be empty".into()));
            }
            if let Some(limit) = limit
                && *limit == 0
            {
                return Err(AedbError::Validation("limit must be > 0".into()));
            }
            for (column, value) in updates {
                if schema.primary_key.iter().any(|pk| pk == column) {
                    return Err(AedbError::Validation(
                        "update_where cannot modify primary key columns".into(),
                    ));
                }
                let Some(col) = schema.columns.iter().find(|c| c.name == *column) else {
                    return Err(AedbError::UnknownColumn {
                        table: schema.table_name.clone(),
                        column: column.clone(),
                    });
                };
                if !matches!(value, Value::Null) && !value_matches_type(value, &col.col_type) {
                    return Err(AedbError::TypeMismatch {
                        table: schema.table_name.clone(),
                        column: column.clone(),
                        expected: format!("{:?}", col.col_type),
                        actual: value_type_name(value).to_string(),
                    });
                }
                if matches!(value, Value::Null) && !col.nullable {
                    return Err(AedbError::NotNullViolation {
                        table: schema.table_name.clone(),
                        column: column.clone(),
                    });
                }
            }
            Ok(())
        }
        Mutation::UpdateWhereExpr {
            project_id,
            scope_id,
            table_name,
            predicate,
            updates,
            limit,
        } => {
            ensure_not_managed_table(table_name)?;
            let schema = table_schema(catalog, project_id, scope_id, table_name)?;
            validate_expr_columns(schema, predicate)?;
            if updates.is_empty() {
                return Err(AedbError::Validation("updates cannot be empty".into()));
            }
            if let Some(limit) = limit
                && *limit == 0
            {
                return Err(AedbError::Validation("limit must be > 0".into()));
            }
            for (column, expr) in updates {
                validate_table_update_expr(schema, column, expr)?;
            }
            Ok(())
        }
        Mutation::Ddl(ddl) => {
            validate_ddl_for_managed_tables(ddl)?;
            let mut cloned = catalog.clone();
            cloned.apply_ddl(ddl.clone())
        }
        Mutation::KvSet {
            project_id,
            scope_id,
            key,
            value,
        } => validate_kv(catalog, project_id, scope_id, key, Some(value), config),
        Mutation::KvDel {
            project_id,
            scope_id,
            key,
        } => validate_kv(catalog, project_id, scope_id, key, None, config),
        Mutation::KvIncU256 {
            project_id,
            scope_id,
            key,
            ..
        }
        | Mutation::KvDecU256 {
            project_id,
            scope_id,
            key,
            ..
        } => validate_kv(
            catalog,
            project_id,
            scope_id,
            key,
            Some(&vec![0u8; 32]),
            config,
        ),
        Mutation::TableIncU256 {
            project_id,
            scope_id,
            table_name,
            primary_key,
            column,
            ..
        }
        | Mutation::TableDecU256 {
            project_id,
            scope_id,
            table_name,
            primary_key,
            column,
            ..
        } => validate_table_u256_field_update(
            catalog,
            project_id,
            scope_id,
            table_name,
            primary_key,
            column,
        ),
    }
}

fn validate_table_u256_field_update(
    catalog: &Catalog,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    primary_key: &[Value],
    column: &str,
) -> Result<(), AedbError> {
    ensure_not_managed_table(table_name)?;
    let schema = table_schema(catalog, project_id, scope_id, table_name)?;
    if primary_key.len() != schema.primary_key.len() {
        return Err(AedbError::Validation("primary key length mismatch".into()));
    }
    let Some(col) = schema.columns.iter().find(|c| c.name == column) else {
        return Err(AedbError::UnknownColumn {
            table: schema.table_name.clone(),
            column: column.to_string(),
        });
    };
    if !matches!(col.col_type, ColumnType::U256) {
        return Err(AedbError::Validation(format!(
            "column {column} must be U256 for atomic u256 mutations"
        )));
    }
    Ok(())
}

fn validate_table_update_expr(
    schema: &TableSchema,
    column: &str,
    expr: &TableUpdateExpr,
) -> Result<(), AedbError> {
    if schema.primary_key.iter().any(|pk| pk == column) {
        return Err(AedbError::Validation(
            "update_where cannot modify primary key columns".into(),
        ));
    }
    let Some(target_col) = schema.columns.iter().find(|c| c.name == column) else {
        return Err(AedbError::UnknownColumn {
            table: schema.table_name.clone(),
            column: column.to_string(),
        });
    };

    match expr {
        TableUpdateExpr::Value(value) => {
            if !matches!(value, Value::Null) && !value_matches_type(value, &target_col.col_type) {
                return Err(AedbError::TypeMismatch {
                    table: schema.table_name.clone(),
                    column: column.to_string(),
                    expected: format!("{:?}", target_col.col_type),
                    actual: value_type_name(value).to_string(),
                });
            }
            if matches!(value, Value::Null) && !target_col.nullable {
                return Err(AedbError::NotNullViolation {
                    table: schema.table_name.clone(),
                    column: column.to_string(),
                });
            }
        }
        TableUpdateExpr::CopyColumn(source) => {
            let Some(source_col) = schema.columns.iter().find(|c| c.name == *source) else {
                return Err(AedbError::UnknownColumn {
                    table: schema.table_name.clone(),
                    column: source.clone(),
                });
            };
            if source_col.col_type != target_col.col_type {
                return Err(AedbError::TypeMismatch {
                    table: schema.table_name.clone(),
                    column: column.to_string(),
                    expected: format!("{:?}", target_col.col_type),
                    actual: format!("{:?}", source_col.col_type),
                });
            }
            if source_col.nullable && !target_col.nullable {
                return Err(AedbError::Validation(format!(
                    "cannot copy nullable column {source} into non-nullable column {column}"
                )));
            }
        }
        TableUpdateExpr::AddI64(_) => {
            if !matches!(target_col.col_type, ColumnType::Integer) {
                return Err(AedbError::Validation(format!(
                    "AddI64 requires Integer target column: {column}"
                )));
            }
            if target_col.nullable {
                return Err(AedbError::Validation(format!(
                    "AddI64 requires non-nullable Integer target column: {column}"
                )));
            }
        }
        TableUpdateExpr::Coalesce(fallback) => {
            if !matches!(fallback, Value::Null)
                && !value_matches_type(fallback, &target_col.col_type)
            {
                return Err(AedbError::TypeMismatch {
                    table: schema.table_name.clone(),
                    column: column.to_string(),
                    expected: format!("{:?}", target_col.col_type),
                    actual: value_type_name(fallback).to_string(),
                });
            }
            if matches!(fallback, Value::Null) && !target_col.nullable {
                return Err(AedbError::NotNullViolation {
                    table: schema.table_name.clone(),
                    column: column.to_string(),
                });
            }
        }
    }

    Ok(())
}

fn ensure_not_managed_table(table_name: &str) -> Result<(), AedbError> {
    if table_name == KV_INDEX_TABLE {
        return Err(AedbError::Validation(format!(
            "table {KV_INDEX_TABLE} is managed and read-only"
        )));
    }
    Ok(())
}

fn validate_ddl_for_managed_tables(ddl: &DdlOperation) -> Result<(), AedbError> {
    match ddl {
        DdlOperation::CreateTable { table_name, .. }
        | DdlOperation::AlterTable { table_name, .. }
        | DdlOperation::DropTable { table_name, .. } => ensure_not_managed_table(table_name),
        _ => Ok(()),
    }
}

pub fn validate_permissions(
    catalog: &Catalog,
    caller: Option<&CallerContext>,
    mutation: &Mutation,
) -> Result<(), AedbError> {
    let Some(caller) = caller else {
        return Ok(());
    };
    if let Some((project_id, scope_id, key)) = kv_write_target(mutation) {
        if catalog.has_kv_write_permission(&caller.caller_id, project_id, scope_id, key) {
            return Ok(());
        }
        return Err(AedbError::PermissionDenied(format!(
            "caller={} missing kv write permission for key prefix",
            caller.caller_id
        )));
    }
    if let Mutation::Ddl(ddl) = mutation {
        match ddl {
            DdlOperation::GrantPermission { permission, .. }
            | DdlOperation::RevokePermission { permission, .. } => {
                if can_administer_permission(catalog, &caller.caller_id, permission) {
                    return Ok(());
                }
                return Err(AedbError::PermissionDenied(format!(
                    "caller={} missing admin rights for permission {:?}",
                    caller.caller_id, permission
                )));
            }
            DdlOperation::TransferOwnership {
                resource_type,
                project_id,
                scope_id,
                table_name,
                ..
            } => {
                let allowed = catalog.has_permission(&caller.caller_id, &Permission::GlobalAdmin)
                    || match resource_type {
                        ResourceType::Project => {
                            catalog.is_owner_of_project(&caller.caller_id, project_id)
                        }
                        ResourceType::Scope => scope_id.as_ref().is_some_and(|scope| {
                            catalog.is_owner_of_scope(&caller.caller_id, project_id, scope)
                                || catalog.is_owner_of_project(&caller.caller_id, project_id)
                        }),
                        ResourceType::Table => match (scope_id.as_ref(), table_name.as_ref()) {
                            (Some(scope), Some(table)) => {
                                catalog.is_owner_of_table(
                                    &caller.caller_id,
                                    project_id,
                                    scope,
                                    table,
                                ) || catalog.is_owner_of_project(&caller.caller_id, project_id)
                            }
                            _ => false,
                        },
                    };
                if allowed {
                    return Ok(());
                }
                return Err(AedbError::PermissionDenied(format!(
                    "caller={} missing ownership transfer authority",
                    caller.caller_id
                )));
            }
            _ => {}
        }
    }
    let required = required_permission(mutation)?;
    if catalog.has_permission(&caller.caller_id, &required) {
        return Ok(());
    }
    Err(AedbError::PermissionDenied(format!(
        "caller={} missing permission {:?}",
        caller.caller_id, required
    )))
}

fn kv_write_target(mutation: &Mutation) -> Option<(&str, &str, &[u8])> {
    match mutation {
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
            ..
        }
        | Mutation::KvIncU256 {
            project_id,
            scope_id,
            key,
            ..
        }
        | Mutation::KvDecU256 {
            project_id,
            scope_id,
            key,
            ..
        } => Some((project_id.as_str(), scope_id.as_str(), key.as_slice())),
        _ => None,
    }
}

pub fn required_permission(mutation: &Mutation) -> Result<Permission, AedbError> {
    match mutation {
        Mutation::Upsert {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::UpsertBatch {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::UpsertOnConflict {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::UpsertBatchOnConflict {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::Delete {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::DeleteWhere {
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
        }
        | Mutation::TableIncU256 {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | Mutation::TableDecU256 {
            project_id,
            scope_id,
            table_name,
            ..
        } => Ok(Permission::TableWrite {
            project_id: project_id.clone(),
            scope_id: scope_id.clone(),
            table_name: table_name.clone(),
        }),
        Mutation::Ddl(ddl) => match ddl {
            DdlOperation::CreateProject { .. } => Ok(Permission::GlobalAdmin),
            DdlOperation::DropProject { project_id, .. } => Ok(Permission::ProjectAdmin {
                project_id: project_id.clone(),
            }),
            DdlOperation::GrantPermission { .. } | DdlOperation::RevokePermission { .. } => {
                Ok(Permission::GlobalAdmin)
            }
            DdlOperation::CreateScope { project_id, .. } => Ok(Permission::ProjectAdmin {
                project_id: project_id.clone(),
            }),
            DdlOperation::DropScope {
                project_id,
                scope_id,
                ..
            } => Ok(Permission::ScopeAdmin {
                project_id: project_id.clone(),
                scope_id: scope_id.clone(),
            }),
            DdlOperation::CreateTable { project_id, .. }
            | DdlOperation::AlterTable { project_id, .. }
            | DdlOperation::DropTable { project_id, .. }
            | DdlOperation::CreateIndex { project_id, .. }
            | DdlOperation::DropIndex { project_id, .. }
            | DdlOperation::CreateAsyncIndex { project_id, .. }
            | DdlOperation::DropAsyncIndex { project_id, .. }
            | DdlOperation::EnableKvProjection { project_id, .. }
            | DdlOperation::DisableKvProjection { project_id, .. }
            | DdlOperation::SetReadPolicy { project_id, .. }
            | DdlOperation::ClearReadPolicy { project_id, .. } => Ok(Permission::TableDdl {
                project_id: project_id.clone(),
            }),
            DdlOperation::TransferOwnership {
                resource_type,
                project_id,
                scope_id,
                table_name: _,
                ..
            } => match resource_type {
                ResourceType::Project => Ok(Permission::ProjectAdmin {
                    project_id: project_id.clone(),
                }),
                ResourceType::Scope => Ok(Permission::ScopeAdmin {
                    project_id: project_id.clone(),
                    scope_id: scope_id.clone().ok_or_else(|| {
                        AedbError::Validation("scope transfer requires scope_id".into())
                    })?,
                }),
                ResourceType::Table => Ok(Permission::TableDdl {
                    project_id: project_id.clone(),
                }),
            },
        },
        Mutation::KvSet {
            project_id,
            scope_id,
            ..
        }
        | Mutation::KvDel {
            project_id,
            scope_id,
            ..
        }
        | Mutation::KvIncU256 {
            project_id,
            scope_id,
            ..
        }
        | Mutation::KvDecU256 {
            project_id,
            scope_id,
            ..
        } => Ok(Permission::KvWrite {
            project_id: project_id.clone(),
            scope_id: Some(scope_id.clone()),
            prefix: None,
        }),
    }
}

fn can_administer_permission(catalog: &Catalog, caller_id: &str, permission: &Permission) -> bool {
    if catalog.has_permission(caller_id, &Permission::GlobalAdmin) {
        return true;
    }
    match permission {
        Permission::GlobalAdmin => false,
        Permission::ProjectAdmin { project_id } => {
            catalog.is_owner_of_project(caller_id, project_id)
        }
        Permission::TableDdl { project_id }
        | Permission::KvRead {
            project_id,
            scope_id: None,
            ..
        }
        | Permission::KvWrite {
            project_id,
            scope_id: None,
            ..
        }
        | Permission::PolicyBypass {
            project_id,
            table_name: None,
        } => {
            catalog.is_owner_of_project(caller_id, project_id)
                || catalog.has_delegable_grant(caller_id, permission)
                || catalog.has_permission(
                    caller_id,
                    &Permission::ProjectAdmin {
                        project_id: project_id.clone(),
                    },
                )
        }
        Permission::ScopeAdmin {
            project_id,
            scope_id,
        }
        | Permission::KvRead {
            project_id,
            scope_id: Some(scope_id),
            ..
        }
        | Permission::KvWrite {
            project_id,
            scope_id: Some(scope_id),
            ..
        }
        | Permission::TableRead {
            project_id,
            scope_id,
            ..
        }
        | Permission::TableWrite {
            project_id,
            scope_id,
            ..
        }
        | Permission::IndexRead {
            project_id,
            scope_id,
            ..
        } => {
            let has_scope_owner = catalog.is_owner_of_scope(caller_id, project_id, scope_id);
            let has_table_owner = match permission {
                Permission::TableRead { table_name, .. }
                | Permission::TableWrite { table_name, .. }
                | Permission::IndexRead { table_name, .. }
                | Permission::PolicyBypass {
                    table_name: Some(table_name),
                    ..
                } => catalog.is_owner_of_table(caller_id, project_id, scope_id, table_name),
                _ => false,
            };
            catalog.has_permission(
                caller_id,
                &Permission::ProjectAdmin {
                    project_id: project_id.clone(),
                },
            ) || catalog.has_permission(
                caller_id,
                &Permission::ScopeAdmin {
                    project_id: project_id.clone(),
                    scope_id: scope_id.clone(),
                },
            ) || has_scope_owner
                || has_table_owner
                || catalog.has_delegable_grant(caller_id, permission)
        }
        Permission::PolicyBypass {
            project_id,
            table_name: Some(_),
        } => {
            catalog.has_permission(
                caller_id,
                &Permission::ProjectAdmin {
                    project_id: project_id.clone(),
                },
            ) || catalog.is_owner_of_project(caller_id, project_id)
                || catalog.has_delegable_grant(caller_id, permission)
        }
    }
}

fn validate_kv(
    catalog: &Catalog,
    project_id: &str,
    scope_id: &str,
    key: &[u8],
    value: Option<&Vec<u8>>,
    config: &AedbConfig,
) -> Result<(), AedbError> {
    if !catalog.projects.contains_key(project_id) {
        return Err(AedbError::Validation(format!(
            "project does not exist: {project_id}"
        )));
    }
    if !catalog
        .scopes
        .contains_key(&(project_id.to_string(), scope_id.to_string()))
    {
        return Err(AedbError::Validation(format!(
            "scope does not exist: {project_id}.{scope_id}"
        )));
    }
    if key.len() > config.max_kv_key_bytes {
        return Err(AedbError::Validation("kv key too large".into()));
    }
    if let Some(v) = value
        && v.len() > config.max_kv_value_bytes
    {
        return Err(AedbError::Validation("kv value too large".into()));
    }
    Ok(())
}

fn validate_upsert_row(
    catalog: &Catalog,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    primary_key: &[Value],
    row: &Row,
) -> Result<(), AedbError> {
    let schema = table_schema(catalog, project_id, scope_id, table_name)?;
    validate_row_against_schema(schema, primary_key, row)
}

fn validate_conflict_target(
    catalog: &Catalog,
    schema: &TableSchema,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    target: &ConflictTarget,
) -> Result<(), AedbError> {
    match target {
        ConflictTarget::PrimaryKey => Ok(()),
        ConflictTarget::Index(index_name) => {
            let ns = namespace_key(project_id, scope_id);
            let idx = catalog
                .indexes
                .get(&(ns, table_name.to_string(), index_name.clone()))
                .ok_or_else(|| {
                    AedbError::Validation(format!("conflict index does not exist: {index_name}"))
                })?;
            if !matches!(
                idx.index_type,
                crate::catalog::schema::IndexType::UniqueHash
            ) {
                return Err(AedbError::Validation(format!(
                    "conflict index is not unique: {index_name}"
                )));
            }
            Ok(())
        }
        ConflictTarget::Columns(columns) => {
            if columns.is_empty() {
                return Err(AedbError::Validation(
                    "conflict columns cannot be empty".into(),
                ));
            }
            for column in columns {
                if !schema.columns.iter().any(|c| c.name == *column) {
                    return Err(AedbError::Validation(format!(
                        "conflict column not found: {column}"
                    )));
                }
            }
            if &schema.primary_key == columns {
                return Ok(());
            }
            let has_unique = catalog.indexes.values().any(|idx| {
                idx.project_id == project_id
                    && idx.scope_id == scope_id
                    && idx.table_name == table_name
                    && idx.columns == *columns
                    && matches!(
                        idx.index_type,
                        crate::catalog::schema::IndexType::UniqueHash
                    )
            });
            if !has_unique {
                return Err(AedbError::Validation(
                    "conflict columns must match PK or a unique index".into(),
                ));
            }
            Ok(())
        }
    }
}

fn validate_conflict_action(
    schema: &TableSchema,
    action: &ConflictAction,
) -> Result<(), AedbError> {
    match action {
        ConflictAction::DoNothing | ConflictAction::DoMerge => Ok(()),
        ConflictAction::DoUpdate(updates) => {
            for (name, value) in updates {
                if schema.primary_key.iter().any(|pk| pk == name) {
                    return Err(AedbError::Validation(format!(
                        "cannot update primary key column in conflict action: {name}"
                    )));
                }
                let col = schema
                    .columns
                    .iter()
                    .find(|c| c.name == *name)
                    .ok_or_else(|| AedbError::UnknownColumn {
                        table: schema.table_name.clone(),
                        column: name.clone(),
                    })?;
                if matches!(value, Value::Null) && !col.nullable {
                    return Err(AedbError::NotNullViolation {
                        table: schema.table_name.clone(),
                        column: col.name.clone(),
                    });
                }
                if !matches!(value, Value::Null) && !value_matches_type(value, &col.col_type) {
                    return Err(AedbError::TypeMismatch {
                        table: schema.table_name.clone(),
                        column: col.name.clone(),
                        expected: format!("{:?}", col.col_type),
                        actual: value_type_name(value).to_string(),
                    });
                }
            }
            Ok(())
        }
        ConflictAction::DoUpdateWith(updates) => {
            for (name, expr) in updates {
                if schema.primary_key.iter().any(|pk| pk == name) {
                    return Err(AedbError::Validation(format!(
                        "cannot update primary key column in conflict action: {name}"
                    )));
                }
                if !schema.columns.iter().any(|c| c.name == *name) {
                    return Err(AedbError::UnknownColumn {
                        table: schema.table_name.clone(),
                        column: name.clone(),
                    });
                }
                validate_update_expr(schema, expr)?;
            }
            Ok(())
        }
    }
}

fn validate_update_expr(schema: &TableSchema, expr: &UpdateExpr) -> Result<(), AedbError> {
    match expr {
        UpdateExpr::Value(_) => Ok(()),
        UpdateExpr::Existing(c) | UpdateExpr::Proposed(c) => {
            if schema.columns.iter().any(|col| col.name == *c) {
                Ok(())
            } else {
                Err(AedbError::Validation(format!(
                    "update expression column not found: {c}"
                )))
            }
        }
        UpdateExpr::AddI64 {
            existing_column,
            proposed_column,
        } => {
            for c in [existing_column, proposed_column] {
                let Some(col) = schema.columns.iter().find(|col| col.name == *c) else {
                    return Err(AedbError::Validation(format!(
                        "update expression column not found: {c}"
                    )));
                };
                if !matches!(col.col_type, ColumnType::Integer) {
                    return Err(AedbError::Validation(format!(
                        "AddI64 requires integer column: {c}"
                    )));
                }
            }
            Ok(())
        }
    }
}

fn extract_primary_key(schema: &TableSchema, row: &Row) -> Result<Vec<Value>, AedbError> {
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
        if matches!(value, Value::Null) {
            return Err(AedbError::Validation(format!(
                "primary key column cannot be null: {pk_name}"
            )));
        }
        primary_key.push(value.clone());
    }
    Ok(primary_key)
}

pub fn amount_to_u256(amount_be: &[u8; 32]) -> U256 {
    U256::from_big_endian(amount_be)
}

fn table_schema<'a>(
    catalog: &'a Catalog,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
) -> Result<&'a TableSchema, AedbError> {
    if !catalog.projects.contains_key(project_id) {
        return Err(AedbError::NotFound {
            resource_type: ErrorResourceType::Project,
            resource_id: project_id.to_string(),
        });
    }
    catalog
        .tables
        .get(&(namespace_key(project_id, scope_id), table_name.to_string()))
        .ok_or_else(|| AedbError::NotFound {
            resource_type: ErrorResourceType::Table,
            resource_id: format!("{project_id}.{scope_id}.{table_name}"),
        })
}

fn validate_row_against_schema(
    schema: &TableSchema,
    primary_key: &[Value],
    row: &Row,
) -> Result<(), AedbError> {
    if row.values.len() != schema.columns.len() {
        return Err(AedbError::Validation("row column count mismatch".into()));
    }
    if primary_key.len() != schema.primary_key.len() {
        return Err(AedbError::Validation("primary key length mismatch".into()));
    }

    for (i, col) in schema.columns.iter().enumerate() {
        let value = &row.values[i];
        if matches!(value, Value::Null) && !col.nullable {
            return Err(AedbError::NotNullViolation {
                table: schema.table_name.clone(),
                column: col.name.clone(),
            });
        }
        if !matches!(value, Value::Null) && !value_matches_type(value, &col.col_type) {
            return Err(AedbError::TypeMismatch {
                table: schema.table_name.clone(),
                column: col.name.clone(),
                expected: format!("{:?}", col.col_type),
                actual: value_type_name(value).to_string(),
            });
        }
    }
    Ok(())
}

fn value_matches_type(value: &Value, ty: &ColumnType) -> bool {
    matches!(
        (value, ty),
        (Value::Text(_), ColumnType::Text)
            | (Value::Integer(_), ColumnType::Integer)
            | (Value::Float(_), ColumnType::Float)
            | (Value::Boolean(_), ColumnType::Boolean)
            | (Value::U256(_), ColumnType::U256)
            | (Value::I256(_), ColumnType::I256)
            | (Value::Blob(_), ColumnType::Blob)
            | (Value::Timestamp(_), ColumnType::Timestamp)
            | (Value::Json(_), ColumnType::Json)
    )
}

fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Text(_) => "Text",
        Value::Integer(_) => "Integer",
        Value::Float(_) => "Float",
        Value::Boolean(_) => "Boolean",
        Value::U256(_) => "U256",
        Value::I256(_) => "I256",
        Value::Blob(_) => "Blob",
        Value::Timestamp(_) => "Timestamp",
        Value::Json(_) => "Json",
        Value::Null => "Null",
    }
}

fn validate_expr_columns(schema: &TableSchema, expr: &Expr) -> Result<(), AedbError> {
    match expr {
        Expr::Eq(col, _)
        | Expr::Ne(col, _)
        | Expr::Lt(col, _)
        | Expr::Lte(col, _)
        | Expr::Gt(col, _)
        | Expr::Gte(col, _)
        | Expr::In(col, _)
        | Expr::Between(col, _, _)
        | Expr::IsNull(col)
        | Expr::IsNotNull(col)
        | Expr::Like(col, _) => {
            if schema.columns.iter().any(|c| c.name == *col) {
                Ok(())
            } else {
                Err(AedbError::Validation(format!("column not found: {col}")))
            }
        }
        Expr::And(lhs, rhs) | Expr::Or(lhs, rhs) => {
            validate_expr_columns(schema, lhs)?;
            validate_expr_columns(schema, rhs)
        }
        Expr::Not(inner) => validate_expr_columns(schema, inner),
    }
}
