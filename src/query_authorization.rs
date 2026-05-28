use crate::SYSTEM_CALLER_ID;
use crate::catalog::Catalog;
use crate::error::AedbError;
use crate::permission::{CallerContext, Permission};
use crate::query::error::QueryError;
use crate::query::plan::{Expr, Query, QueryOptions};

pub(crate) fn authorize_and_bind_query_for_caller(
    project_id: &str,
    scope_id: &str,
    mut query: Query,
    options: &QueryOptions,
    caller: Option<&CallerContext>,
    catalog: &Catalog,
) -> Result<Query, QueryError> {
    let Some(caller) = caller else {
        return Ok(query);
    };

    ensure_query_caller_allowed(caller)?;
    let (base_project_id, base_scope_id, base_table_name) =
        crate::lib_helpers::resolve_query_table_ref(project_id, scope_id, &query.table);
    let base_alias = query
        .table_alias
        .clone()
        .unwrap_or_else(|| base_table_name.clone());
    let mut seen_aliases = std::collections::BTreeSet::new();
    if !seen_aliases.insert(base_alias.clone()) {
        return Err(QueryError::InvalidQuery {
            reason: format!("duplicate table alias in query: {base_alias}"),
        });
    }
    for join in &query.joins {
        let (_, _, join_table_name) =
            crate::lib_helpers::resolve_query_table_ref(project_id, scope_id, &join.table);
        let join_alias = join.alias.clone().unwrap_or(join_table_name);
        if !seen_aliases.insert(join_alias.clone()) {
            return Err(QueryError::InvalidQuery {
                reason: format!("duplicate table alias in query: {join_alias}"),
            });
        }
    }
    let table_read = Permission::TableRead {
        project_id: base_project_id.clone(),
        scope_id: base_scope_id.clone(),
        table_name: base_table_name.clone(),
    };
    if !catalog.has_permission(&caller.caller_id, &table_read) {
        return Err(QueryError::PermissionDenied {
            permission: "permission denied".into(),
            scope: String::new(),
        });
    }
    if let Some(index_name) = &options.async_index {
        let index_read = Permission::IndexRead {
            project_id: base_project_id.clone(),
            scope_id: base_scope_id.clone(),
            table_name: base_table_name.clone(),
            index_name: index_name.clone(),
        };
        if !catalog.has_permission(&caller.caller_id, &index_read) {
            return Err(QueryError::PermissionDenied {
                permission: "permission denied".into(),
                scope: String::new(),
            });
        }
    }
    for join in &query.joins {
        let (join_project_id, join_scope_id, join_table_name) =
            crate::lib_helpers::resolve_query_table_ref(project_id, scope_id, &join.table);
        let join_required = Permission::TableRead {
            project_id: join_project_id,
            scope_id: join_scope_id,
            table_name: join_table_name,
        };
        if !catalog.has_permission(&caller.caller_id, &join_required) {
            return Err(QueryError::PermissionDenied {
                permission: "permission denied".into(),
                scope: String::new(),
            });
        }
    }
    let caller_id = caller.caller_id.as_str();
    let has_policy_bypass = |project_id: &str, table_name: &str| {
        catalog.has_permission(
            caller_id,
            &Permission::PolicyBypass {
                project_id: project_id.to_string(),
                table_name: Some(table_name.to_string()),
            },
        ) || catalog.has_permission(
            caller_id,
            &Permission::PolicyBypass {
                project_id: project_id.to_string(),
                table_name: None,
            },
        )
    };
    let mut policies = Vec::new();
    if !has_policy_bypass(&base_project_id, &base_table_name)
        && let Some(policy) =
            catalog.read_policy_for_table(&base_project_id, &base_scope_id, &base_table_name)
    {
        let bound_policy = bind_policy_expr(&policy, &caller.caller_id);
        if query.joins.is_empty() {
            policies.push(bound_policy);
        } else {
            policies.push(qualify_policy_columns(&bound_policy, &base_alias));
        }
    }
    for join in &query.joins {
        let (join_project_id, join_scope_id, join_table_name) =
            crate::lib_helpers::resolve_query_table_ref(project_id, scope_id, &join.table);
        if !has_policy_bypass(&join_project_id, &join_table_name)
            && let Some(policy) =
                catalog.read_policy_for_table(&join_project_id, &join_scope_id, &join_table_name)
        {
            let bound_policy = bind_policy_expr(&policy, &caller.caller_id);
            let join_alias = join.alias.clone().unwrap_or(join_table_name);
            policies.push(qualify_policy_columns(&bound_policy, &join_alias));
        }
    }
    for policy in policies {
        query.predicate = Some(match query.predicate.take() {
            Some(existing) => Expr::And(Box::new(existing), Box::new(policy)),
            None => policy,
        });
    }

    Ok(query)
}

pub(crate) fn bind_policy_expr(expr: &Expr, caller_id: &str) -> Expr {
    fn bind_value(
        value: &crate::catalog::types::Value,
        caller_id: &str,
    ) -> crate::catalog::types::Value {
        match value {
            crate::catalog::types::Value::Text(text) if text.as_str() == "$caller_id" => {
                crate::catalog::types::Value::Text(caller_id.into())
            }
            _ => value.clone(),
        }
    }
    fn bind_like_pattern(pattern: &str, caller_id: &str) -> String {
        pattern.replace("$caller_id", caller_id)
    }
    match expr {
        Expr::Eq(col, v) => Expr::Eq(col.clone(), bind_value(v, caller_id)),
        Expr::Ne(col, v) => Expr::Ne(col.clone(), bind_value(v, caller_id)),
        Expr::Lt(col, v) => Expr::Lt(col.clone(), bind_value(v, caller_id)),
        Expr::Lte(col, v) => Expr::Lte(col.clone(), bind_value(v, caller_id)),
        Expr::Gt(col, v) => Expr::Gt(col.clone(), bind_value(v, caller_id)),
        Expr::Gte(col, v) => Expr::Gte(col.clone(), bind_value(v, caller_id)),
        Expr::In(col, vals) => Expr::In(
            col.clone(),
            vals.iter().map(|v| bind_value(v, caller_id)).collect(),
        ),
        Expr::Between(col, lo, hi) => Expr::Between(
            col.clone(),
            bind_value(lo, caller_id),
            bind_value(hi, caller_id),
        ),
        Expr::IsNull(col) => Expr::IsNull(col.clone()),
        Expr::IsNotNull(col) => Expr::IsNotNull(col.clone()),
        Expr::Like(col, pattern) => Expr::Like(col.clone(), bind_like_pattern(pattern, caller_id)),
        Expr::And(lhs, rhs) => Expr::And(
            Box::new(bind_policy_expr(lhs, caller_id)),
            Box::new(bind_policy_expr(rhs, caller_id)),
        ),
        Expr::Or(lhs, rhs) => Expr::Or(
            Box::new(bind_policy_expr(lhs, caller_id)),
            Box::new(bind_policy_expr(rhs, caller_id)),
        ),
        Expr::Not(inner) => Expr::Not(Box::new(bind_policy_expr(inner, caller_id))),
    }
}

pub(crate) fn qualify_policy_columns(expr: &Expr, alias: &str) -> Expr {
    fn qualify_column(column: &str, alias: &str) -> String {
        if column.contains('.') {
            column.to_string()
        } else {
            format!("{alias}.{column}")
        }
    }

    match expr {
        Expr::Eq(col, v) => Expr::Eq(qualify_column(col, alias), v.clone()),
        Expr::Ne(col, v) => Expr::Ne(qualify_column(col, alias), v.clone()),
        Expr::Lt(col, v) => Expr::Lt(qualify_column(col, alias), v.clone()),
        Expr::Lte(col, v) => Expr::Lte(qualify_column(col, alias), v.clone()),
        Expr::Gt(col, v) => Expr::Gt(qualify_column(col, alias), v.clone()),
        Expr::Gte(col, v) => Expr::Gte(qualify_column(col, alias), v.clone()),
        Expr::In(col, vals) => Expr::In(qualify_column(col, alias), vals.clone()),
        Expr::Between(col, lo, hi) => {
            Expr::Between(qualify_column(col, alias), lo.clone(), hi.clone())
        }
        Expr::IsNull(col) => Expr::IsNull(qualify_column(col, alias)),
        Expr::IsNotNull(col) => Expr::IsNotNull(qualify_column(col, alias)),
        Expr::Like(col, pattern) => Expr::Like(qualify_column(col, alias), pattern.clone()),
        Expr::And(lhs, rhs) => Expr::And(
            Box::new(qualify_policy_columns(lhs, alias)),
            Box::new(qualify_policy_columns(rhs, alias)),
        ),
        Expr::Or(lhs, rhs) => Expr::Or(
            Box::new(qualify_policy_columns(lhs, alias)),
            Box::new(qualify_policy_columns(rhs, alias)),
        ),
        Expr::Not(inner) => Expr::Not(Box::new(qualify_policy_columns(inner, alias))),
    }
}

pub(crate) fn ensure_external_caller_allowed(caller: &CallerContext) -> Result<(), AedbError> {
    if caller.caller_id.trim().is_empty() {
        return Err(AedbError::PermissionDenied(
            "caller_id must be non-empty".into(),
        ));
    }
    if caller.caller_id == SYSTEM_CALLER_ID && !caller.is_internal_system() {
        return Err(AedbError::PermissionDenied(
            "caller_id 'system' is reserved for internal use".into(),
        ));
    }
    Ok(())
}

pub(crate) fn ensure_query_caller_allowed(caller: &CallerContext) -> Result<(), QueryError> {
    if caller.caller_id.trim().is_empty() {
        return Err(QueryError::PermissionDenied {
            permission: "caller_id must be non-empty".into(),
            scope: caller.caller_id.clone(),
        });
    }
    if caller.caller_id == SYSTEM_CALLER_ID && !caller.is_internal_system() {
        return Err(QueryError::PermissionDenied {
            permission: "caller_id 'system' is reserved for internal use".into(),
            scope: caller.caller_id.clone(),
        });
    }
    Ok(())
}
