use crate::catalog::Catalog;
use crate::catalog::namespace_key;
use crate::catalog::schema::TableSchema;
use crate::catalog::types::Row;
use crate::query::error::QueryError;
use crate::query::operators::{
    AggregateOperator, FilterOperator, LimitOperator, Operator, ScanOperator, SortOperator,
    compile_expr,
};
use crate::query::plan::{Query, QueryOptions};
use crate::query::planner::{ExecutionStage, build_physical_plan};
use crate::storage::encoded_key::EncodedKey;
use crate::storage::keyspace::KeyspaceSnapshot;

mod aggregate;
mod cursor;
mod indexing;
mod join;
mod predicate;
mod validate;

use aggregate::{aggregate_col_idx, aggregate_output_name};
use cursor::{
    CursorToken, decode_cursor, encode_cursor, extract_pk_key, extract_sort_key, row_after_cursor,
};
use indexing::{indexed_pks_for_predicate, indexed_pks_for_predicate_with_trace};
use predicate::extract_primary_key_values;
use validate::validate_query;

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub rows: Vec<Row>,
    pub rows_examined: usize,
    pub cursor: Option<String>,
    pub truncated: bool,
    pub snapshot_seq: u64,
    pub materialized_seq: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AccessPathDiagnostics {
    pub selected_indexes: Vec<String>,
    pub predicate_evaluation_path: crate::PredicateEvaluationPath,
    pub plan_trace: Vec<String>,
}

pub fn execute_query(
    snapshot: &KeyspaceSnapshot,
    catalog: &Catalog,
    project_id: &str,
    scope_id: &str,
    query: Query,
) -> Result<QueryResult, QueryError> {
    execute_query_with_options(
        snapshot,
        catalog,
        project_id,
        scope_id,
        query,
        &QueryOptions::default(),
        0,
        10_000,
    )
}

pub(crate) fn explain_access_path_for_query(
    snapshot: &KeyspaceSnapshot,
    catalog: &Catalog,
    project_id: &str,
    scope_id: &str,
    query: &Query,
    options: &QueryOptions,
) -> Result<AccessPathDiagnostics, QueryError> {
    if !query.joins.is_empty() {
        let mut trace = Vec::new();
        trace.push("join query: predicate evaluation happens after join execution".to_string());
        if query.predicate.is_some() {
            trace.push("post-join filter stage evaluates query predicate".to_string());
        }
        return Ok(AccessPathDiagnostics {
            selected_indexes: Vec::new(),
            predicate_evaluation_path: crate::PredicateEvaluationPath::JoinExecution,
            plan_trace: trace,
        });
    }

    let mut selected_indexes = Vec::new();
    let mut trace = Vec::new();
    let mut predicate_evaluation_path = crate::PredicateEvaluationPath::None;

    let mut effective_options = options.clone();
    if effective_options.async_index.is_none() {
        effective_options.async_index = query.use_index.clone();
    }

    if let Some(async_index) = &effective_options.async_index {
        selected_indexes.push(async_index.clone());
        trace.push(format!(
            "selected async index projection '{async_index}' as row source"
        ));
        predicate_evaluation_path = crate::PredicateEvaluationPath::AsyncIndexProjection;
        if query.predicate.is_some() {
            trace.push("query predicate is evaluated as filter on projected rows".to_string());
        }
        return Ok(AccessPathDiagnostics {
            selected_indexes,
            predicate_evaluation_path,
            plan_trace: trace,
        });
    }

    let table_key = (namespace_key(project_id, scope_id), query.table.clone());
    let schema = catalog
        .tables
        .get(&table_key)
        .ok_or_else(|| QueryError::TableNotFound {
            project_id: project_id.to_string(),
            table: query.table.clone(),
        })?;
    let table = snapshot.table(project_id, scope_id, &query.table);

    if let Some(predicate) = query.predicate.as_ref() {
        if query.limit != Some(0)
            && query.group_by.is_empty()
            && query.aggregates.is_empty()
            && query.having.is_none()
            && query.order_by.is_empty()
            && options.cursor.is_none()
            && extract_primary_key_values(predicate, &schema.primary_key).is_some()
        {
            trace.push("primary-key equality predicate detected; using direct row lookup".into());
            return Ok(AccessPathDiagnostics {
                selected_indexes,
                predicate_evaluation_path: crate::PredicateEvaluationPath::PrimaryKeyEqLookup,
                plan_trace: trace,
            });
        }

        if let Some(table) = table {
            if let Some(indexed) = indexed_pks_for_predicate_with_trace(
                catalog,
                project_id,
                scope_id,
                &query.table,
                table,
                predicate,
            )? {
                if !indexed.selected_indexes.is_empty() {
                    selected_indexes.extend(indexed.selected_indexes.clone());
                    predicate_evaluation_path =
                        crate::PredicateEvaluationPath::SecondaryIndexLookup;
                } else {
                    predicate_evaluation_path = crate::PredicateEvaluationPath::FullScanFilter;
                }
                trace.extend(indexed.plan_trace);
                if matches!(
                    predicate_evaluation_path,
                    crate::PredicateEvaluationPath::FullScanFilter
                ) {
                    trace.push(
                        "no matching secondary index; evaluating predicate during table scan"
                            .to_string(),
                    );
                } else {
                    trace.push(
                        "residual predicate is evaluated on rows returned by index lookup"
                            .to_string(),
                    );
                }
                return Ok(AccessPathDiagnostics {
                    selected_indexes,
                    predicate_evaluation_path,
                    plan_trace: trace,
                });
            }
        }

        trace.push("predicate not indexable for current schema/index set".to_string());
        return Ok(AccessPathDiagnostics {
            selected_indexes,
            predicate_evaluation_path: crate::PredicateEvaluationPath::FullScanFilter,
            plan_trace: trace,
        });
    }

    trace.push("no predicate supplied; full table scan path".to_string());
    Ok(AccessPathDiagnostics {
        selected_indexes,
        predicate_evaluation_path,
        plan_trace: trace,
    })
}

#[allow(clippy::too_many_arguments)]
pub fn execute_query_with_options(
    snapshot: &KeyspaceSnapshot,
    catalog: &Catalog,
    project_id: &str,
    scope_id: &str,
    query: Query,
    options: &QueryOptions,
    snapshot_seq: u64,
    max_scan_rows: usize,
) -> Result<QueryResult, QueryError> {
    let mut options = options.clone();
    if options.async_index.is_none() {
        options.async_index = query.use_index.clone();
    }

    // Validate expression depth to prevent stack overflow from deeply nested expressions
    if let Some(pred) = &query.predicate {
        pred.validate_depth()
            .map_err(|e| QueryError::InvalidQuery {
                reason: e.to_string(),
            })?;
    }
    if let Some(having) = &query.having {
        having
            .validate_depth()
            .map_err(|e| QueryError::InvalidQuery {
                reason: e.to_string(),
            })?;
    }

    if !options.allow_full_scan && query.limit.is_none() && query.predicate.is_none() {
        return Err(QueryError::InvalidQuery {
            reason: "full scan requires limit/cursor or allow_full_scan".into(),
        });
    }

    let cursor_state = match &options.cursor {
        Some(encoded) => Some(decode_cursor(encoded)?),
        None => None,
    };
    if let Some(cursor) = &cursor_state
        && cursor.snapshot_seq != snapshot_seq
    {
        return Err(QueryError::InvalidQuery {
            reason: "cursor snapshot_seq mismatch".into(),
        });
    }

    if !query.joins.is_empty() {
        return join::execute_join_query(
            snapshot,
            catalog,
            project_id,
            scope_id,
            query,
            options,
            snapshot_seq,
            max_scan_rows,
            cursor_state,
        );
    }

    let table_key = (namespace_key(project_id, scope_id), query.table.clone());
    let schema = catalog
        .tables
        .get(&table_key)
        .ok_or_else(|| QueryError::TableNotFound {
            project_id: project_id.to_string(),
            table: query.table.clone(),
        })?;
    let table = snapshot.table(project_id, scope_id, &query.table);
    let mut materialized_seq = None;
    if let Some(result) =
        try_primary_key_point_query(schema, table, &query, &cursor_state, snapshot_seq)?
    {
        return Ok(result);
    }
    validate_query(schema, &query)?;

    let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let page_size = query.limit.unwrap_or_else(|| {
        cursor_state
            .as_ref()
            .map(|c| c.page_size)
            .unwrap_or(max_scan_rows.min(100))
    });
    let effective_page_size = page_size.min(max_scan_rows);

    let estimated_rows: usize;
    let row_source: Box<dyn Iterator<Item = Row> + Send> =
        if let Some(async_index) = &options.async_index {
            let projection = snapshot
                .async_index(project_id, scope_id, &query.table, async_index)
                .ok_or_else(|| QueryError::InvalidQuery {
                    reason: "async index not found".into(),
                })?;
            materialized_seq = Some(projection.materialized_seq);
            estimated_rows = projection.rows.len();
            let rows: Vec<Row> = projection.rows.values().cloned().collect();
            Box::new(rows.into_iter())
        } else if let (Some(predicate), Some(table)) = (&query.predicate, table) {
            let indexed_pks = indexed_pks_for_predicate(
                catalog,
                project_id,
                scope_id,
                &query.table,
                table,
                predicate,
            )?;
            match indexed_pks {
                Some(pks) => {
                    estimated_rows = pks.len();
                    let rows: Vec<Row> = pks
                        .into_iter()
                        .filter_map(|pk| table.rows.get(&pk).cloned())
                        .collect();
                    Box::new(rows.into_iter())
                }
                None => {
                    estimated_rows = table.rows.len();
                    let rows: Vec<Row> = table.rows.values().cloned().collect();
                    Box::new(rows.into_iter())
                }
            }
        } else {
            let rows: Vec<Row> = table
                .map(|t| t.rows.values().cloned().collect())
                .unwrap_or_default();
            estimated_rows = rows.len();
            Box::new(rows.into_iter())
        };

    if estimated_rows > max_scan_rows
        && query_requires_full_evaluation(&query, cursor_state.is_some())
    {
        return Err(QueryError::ScanBoundExceeded {
            estimated_rows: estimated_rows as u64,
            max_scan_rows: max_scan_rows as u64,
        });
    }
    let physical_plan = build_physical_plan(
        schema,
        &query,
        options.async_index.clone(),
        estimated_rows as u64,
        query.predicate.is_some(),
    )?;

    let mut root: Box<dyn Operator + Send> = Box::new(ScanOperator::new(row_source));
    let mut selected_indices: Option<Vec<usize>> = None;
    let mut row_columns = columns.clone();
    for stage in &physical_plan.stages {
        match stage {
            ExecutionStage::Scan => {}
            ExecutionStage::Limit => {
                if cursor_state.is_none()
                    && query.order_by.is_empty()
                    && query.aggregates.is_empty()
                    && query.having.is_none()
                {
                    root = Box::new(LimitOperator::new(
                        root,
                        effective_page_size.saturating_add(1),
                    ));
                }
            }
            ExecutionStage::Filter => {
                if let Some(predicate) = query.predicate.clone() {
                    let compiled = compile_expr(&predicate, &columns, &query.table)?;
                    root = Box::new(FilterOperator::new(root, compiled));
                }
            }
            ExecutionStage::Sort => {
                let order_by = query
                    .order_by
                    .iter()
                    .map(|(order_col, order)| {
                        row_columns
                            .iter()
                            .position(|c| c == order_col)
                            .map(|idx| (idx, *order))
                            .ok_or_else(|| QueryError::ColumnNotFound {
                                table: query.table.clone(),
                                column: order_col.clone(),
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let top_k_limit = if cursor_state.is_none() {
                    Some(effective_page_size.saturating_add(1))
                } else {
                    None
                };
                root = Box::new(SortOperator::new_with_limit(root, order_by, top_k_limit));
            }
            ExecutionStage::Aggregate => {
                let group_by_idx = query
                    .group_by
                    .iter()
                    .map(|name| {
                        columns.iter().position(|c| c == name).ok_or_else(|| {
                            QueryError::ColumnNotFound {
                                table: query.table.clone(),
                                column: name.clone(),
                            }
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let agg_col_idx = query
                    .aggregates
                    .iter()
                    .map(|agg| aggregate_col_idx(agg, &columns))
                    .collect::<Result<Vec<_>, _>>()?;
                root = Box::new(AggregateOperator::new(
                    root,
                    query.aggregates.clone(),
                    group_by_idx,
                    agg_col_idx,
                ));
                row_columns = query.group_by.clone();
                row_columns.extend(query.aggregates.iter().map(aggregate_output_name));
            }
            ExecutionStage::Having => {
                if query.aggregates.is_empty() {
                    return Err(QueryError::InvalidQuery {
                        reason: "having requires aggregate or group_by".into(),
                    });
                }
                if let Some(having) = query.having.clone() {
                    let compiled = compile_expr(&having, &row_columns, &query.table)?;
                    root = Box::new(FilterOperator::new(root, compiled));
                }
            }
            ExecutionStage::Project => {
                selected_indices = Some(
                    query
                        .select
                        .iter()
                        .map(|col| {
                            row_columns.iter().position(|c| c == col).ok_or_else(|| {
                                QueryError::ColumnNotFound {
                                    table: query.table.clone(),
                                    column: col.clone(),
                                }
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                );
            }
        }
    }

    let sort_indices: Vec<(usize, crate::query::plan::Order)> = if !query.order_by.is_empty() {
        query
            .order_by
            .iter()
            .filter_map(|(name, ord)| {
                row_columns
                    .iter()
                    .position(|c| c == name)
                    .map(|i| (i, *ord))
            })
            .collect()
    } else {
        Vec::new()
    };
    let pk_indices: Vec<usize> = if !query.aggregates.is_empty() {
        (0..row_columns.len()).collect()
    } else {
        schema
            .primary_key
            .iter()
            .filter_map(|pk| row_columns.iter().position(|c| c == pk))
            .collect()
    };
    let mut sliced: Vec<Row> = Vec::new();
    while let Some(row) = root.next() {
        if let Some(cursor) = &cursor_state
            && !row_after_cursor(&row, cursor, &sort_indices, &pk_indices)
        {
            continue;
        }
        sliced.push(row);
        if sliced.len() > effective_page_size {
            break;
        }
    }
    let has_more = sliced.len() > effective_page_size;
    if has_more {
        sliced.truncate(effective_page_size);
    }
    let cursor_last_row = sliced.last().cloned();
    let sliced: Vec<Row> = if let Some(selected) = &selected_indices {
        sliced
            .into_iter()
            .map(|row| Row {
                values: selected
                    .iter()
                    .map(|idx| row.values[*idx].clone())
                    .collect(),
            })
            .collect()
    } else {
        sliced
    };
    let cursor = if has_more {
        let last_row = cursor_last_row.ok_or_else(|| QueryError::InvalidQuery {
            reason: "invalid cursor state".into(),
        })?;
        Some(encode_cursor(&CursorToken {
            snapshot_seq,
            last_sort_key: extract_sort_key(&last_row, &sort_indices),
            last_pk: extract_pk_key(&last_row, &pk_indices),
            page_size,
            remaining_limit: None,
        })?)
    } else {
        None
    };

    Ok(QueryResult {
        rows: sliced,
        rows_examined: root.rows_examined(),
        truncated: cursor.is_some(),
        cursor,
        snapshot_seq,
        materialized_seq,
    })
}

fn query_requires_full_evaluation(query: &Query, has_cursor: bool) -> bool {
    has_cursor
        || !query.group_by.is_empty()
        || !query.aggregates.is_empty()
        || query.having.is_some()
        || query.limit.is_none()
}

fn try_primary_key_point_query(
    schema: &TableSchema,
    table: Option<&crate::storage::keyspace::TableData>,
    query: &Query,
    cursor_state: &Option<CursorToken>,
    snapshot_seq: u64,
) -> Result<Option<QueryResult>, QueryError> {
    if cursor_state.is_some()
        || query.predicate.is_none()
        || !query.group_by.is_empty()
        || !query.aggregates.is_empty()
        || query.having.is_some()
        || !query.order_by.is_empty()
    {
        return Ok(None);
    }
    if query.limit == Some(0) {
        return Ok(Some(QueryResult {
            rows: Vec::new(),
            rows_examined: 0,
            cursor: None,
            truncated: false,
            snapshot_seq,
            materialized_seq: None,
        }));
    }

    let Some(predicate) = query.predicate.as_ref() else {
        return Ok(None);
    };
    validate::validate_expr_types(schema, predicate)?;
    let Some(primary_key) = extract_primary_key_values(predicate, &schema.primary_key) else {
        return Ok(None);
    };

    let selected_indices = resolve_selected_indices(schema, query)?;
    let encoded_pk = EncodedKey::from_values(&primary_key);
    let maybe_row = table.and_then(|t| t.rows.get(&encoded_pk));
    let rows = match maybe_row {
        Some(row) => vec![project_selected_row(row, selected_indices.as_deref())],
        None => Vec::new(),
    };

    Ok(Some(QueryResult {
        rows,
        rows_examined: 1,
        cursor: None,
        truncated: false,
        snapshot_seq,
        materialized_seq: None,
    }))
}

fn resolve_selected_indices(
    schema: &TableSchema,
    query: &Query,
) -> Result<Option<Vec<usize>>, QueryError> {
    if query.select.len() == 1 && query.select[0] == "*" {
        return Ok(None);
    }
    let mut indices = Vec::with_capacity(query.select.len());
    for col in &query.select {
        let column_index = schema
            .columns
            .iter()
            .position(|c| c.name == *col)
            .ok_or_else(|| QueryError::ColumnNotFound {
                table: query.table.clone(),
                column: col.clone(),
            })?;
        indices.push(column_index);
    }
    Ok(Some(indices))
}

fn project_selected_row(row: &Row, selected_indices: Option<&[usize]>) -> Row {
    match selected_indices {
        Some(indices) => Row {
            values: indices.iter().map(|idx| row.values[*idx].clone()).collect(),
        },
        None => row.clone(),
    }
}
#[cfg(test)]
mod tests;
