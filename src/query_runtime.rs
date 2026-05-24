use crate::QueryDiagnostics;
use crate::catalog::{Catalog, namespace_key};
use crate::error::AedbError;
use crate::permission::CallerContext;
use crate::query::error::QueryError;
use crate::query::executor::{QueryResult, execute_query_with_options};
use crate::query::plan::{Order, Query, QueryOptions};
use crate::query::planner::{ExecutionStage, build_physical_plan};
use crate::query_authorization::authorize_and_bind_query_for_caller;
use crate::snapshot::reader::SnapshotReadView;

pub(crate) fn ensure_stable_order_from_catalog(
    project_id: &str,
    scope_id: &str,
    catalog: &Catalog,
    mut query: Query,
) -> Query {
    if query.joins.is_empty() && query.order_by.is_empty() {
        let (q_project, q_scope, q_table) =
            resolve_query_table_ref(project_id, scope_id, &query.table);
        if let Some(schema) = catalog
            .tables
            .get(&(namespace_key(&q_project, &q_scope), q_table))
        {
            for pk in &schema.primary_key {
                query = query.order_by(pk, Order::Asc);
            }
        }
    }
    query
}

pub(crate) struct QueryExecutionContext<'a> {
    pub view: &'a SnapshotReadView,
    pub project_id: &'a str,
    pub scope_id: &'a str,
    pub options: &'a QueryOptions,
    pub caller: Option<&'a CallerContext>,
    pub max_scan_rows: usize,
    pub cursor_signing_key: Option<&'a [u8; 32]>,
}

pub(crate) fn execute_query_against_view(
    ctx: QueryExecutionContext<'_>,
    mut query: Query,
) -> Result<QueryResult, QueryError> {
    let snapshot = &ctx.view.keyspace;
    let catalog = &ctx.view.catalog;
    let seq = ctx.view.seq;

    query = authorize_and_bind_query_for_caller(
        ctx.project_id,
        ctx.scope_id,
        query,
        ctx.options,
        ctx.caller,
        catalog.as_ref(),
    )?;

    execute_query_with_options(
        snapshot.as_ref(),
        catalog.as_ref(),
        ctx.project_id,
        ctx.scope_id,
        query,
        ctx.options,
        seq,
        ctx.max_scan_rows,
        ctx.cursor_signing_key,
    )
}

pub(crate) fn explain_query_against_view(
    view: &SnapshotReadView,
    project_id: &str,
    scope_id: &str,
    query: Query,
    options: &QueryOptions,
    caller: Option<&CallerContext>,
    max_scan_rows: usize,
) -> Result<QueryDiagnostics, QueryError> {
    let snapshot = &view.keyspace;
    let catalog = &view.catalog;
    let snapshot_seq = view.seq;
    let query = authorize_and_bind_query_for_caller(
        project_id,
        scope_id,
        query,
        options,
        caller,
        catalog.as_ref(),
    )?;
    let bounded_by_limit_or_cursor = query.limit.is_some() || options.cursor.is_some();
    let access_path = crate::query::executor::explain_access_path_for_query(
        snapshot.as_ref(),
        catalog.as_ref(),
        project_id,
        scope_id,
        &query,
        options,
    )?;
    let index_used = explain_index_used(options, &query, &access_path.selected_indexes);
    if !query.joins.is_empty() {
        let mut estimated = snapshot
            .table(project_id, scope_id, &query.table)
            .map(|t| t.rows.len() as u64)
            .unwrap_or(0);
        for join in &query.joins {
            let (jp, js, jt) = resolve_query_table_ref(project_id, scope_id, &join.table);
            let join_rows = snapshot
                .table(&jp, &js, &jt)
                .map(|t| t.rows.len() as u64)
                .unwrap_or(0);
            estimated = estimated.saturating_mul(join_rows.max(1));
        }
        let mut stages = vec![ExecutionStage::Scan];
        if access_path.residual_filter_required {
            stages.push(ExecutionStage::Filter);
        }
        if !query.order_by.is_empty() {
            stages.push(ExecutionStage::Sort);
        }
        if !query.select.is_empty() && query.select[0] != "*" {
            stages.push(ExecutionStage::Project);
        }
        if query.limit.is_some() {
            stages.push(ExecutionStage::Limit);
        }
        return Ok(QueryDiagnostics {
            snapshot_seq,
            estimated_scan_rows: estimated,
            max_scan_rows: max_scan_rows as u64,
            index_used,
            selected_indexes: access_path.selected_indexes,
            predicate_evaluation_path: access_path.predicate_evaluation_path,
            plan_trace: access_path.plan_trace,
            stages,
            bounded_by_limit_or_cursor,
            has_joins: true,
        });
    }

    let (q_project, q_scope, q_table) = resolve_query_table_ref(project_id, scope_id, &query.table);
    let schema = catalog
        .tables
        .get(&(namespace_key(&q_project, &q_scope), q_table.clone()))
        .ok_or_else(|| QueryError::TableNotFound {
            project_id: q_project.clone(),
            table: q_table.clone(),
        })?;
    let estimated_scan_rows = snapshot
        .table(&q_project, &q_scope, &q_table)
        .map(|t| t.rows.len() as u64)
        .unwrap_or(0);
    let mut planned = build_physical_plan(
        schema,
        &query,
        index_used.clone(),
        estimated_scan_rows,
        access_path.residual_filter_required,
    )?;
    apply_access_path_stage_adjustments(&mut planned.stages, &access_path);
    Ok(QueryDiagnostics {
        snapshot_seq,
        estimated_scan_rows,
        max_scan_rows: max_scan_rows as u64,
        index_used,
        selected_indexes: access_path.selected_indexes,
        predicate_evaluation_path: access_path.predicate_evaluation_path,
        plan_trace: access_path.plan_trace,
        stages: planned.stages,
        bounded_by_limit_or_cursor,
        has_joins: false,
    })
}

fn apply_access_path_stage_adjustments(
    stages: &mut Vec<ExecutionStage>,
    access_path: &crate::query::executor::AccessPathDiagnostics,
) {
    if access_path.order_satisfied_by_row_source {
        stages.retain(|stage| *stage != ExecutionStage::Sort);
    }
}

fn explain_index_used(
    options: &QueryOptions,
    query: &Query,
    selected_indexes: &[String],
) -> Option<String> {
    options
        .async_index
        .clone()
        .or_else(|| query.use_index.clone())
        .or_else(|| selected_indexes.first().cloned())
}

pub(crate) fn query_error_to_aedb(error: QueryError) -> AedbError {
    match error {
        QueryError::PermissionDenied { .. } => {
            AedbError::PermissionDenied("permission denied".into())
        }
        other => AedbError::Validation(other.to_string()),
    }
}

pub(crate) fn resolve_query_table_ref(
    project_id: &str,
    scope_id: &str,
    table_ref: &str,
) -> (String, String, String) {
    if let Some(name) = table_ref.strip_prefix("_global.") {
        return (
            "_global".to_string(),
            crate::catalog::DEFAULT_SCOPE_ID.to_string(),
            name.to_string(),
        );
    }
    (
        project_id.to_string(),
        scope_id.to_string(),
        table_ref.to_string(),
    )
}
