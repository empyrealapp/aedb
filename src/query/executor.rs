use crate::catalog::Catalog;
use crate::catalog::namespace_key;
use crate::catalog::schema::TableSchema;
use crate::catalog::types::{Row, Value};
use crate::query::error::QueryError;
use crate::query::operators::{
    AggregateOperator, FilterOperator, Operator, ScanOperator, SortOperator, compile_expr,
};
use crate::query::plan::{Aggregate, Expr, JoinType, Query, QueryOptions};
use crate::query::planner::{ExecutionStage, build_physical_plan};
use crate::storage::encoded_key::EncodedKey;
use crate::storage::keyspace::KeyspaceSnapshot;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::Bound;

type IndexBounds = (Bound<EncodedKey>, Bound<EncodedKey>);

enum IndexLookup {
    Range { column: String, bounds: IndexBounds },
    MultiEq { column: String, values: Vec<Value> },
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub rows: Vec<Row>,
    pub rows_examined: usize,
    pub cursor: Option<String>,
    pub snapshot_seq: u64,
    pub materialized_seq: Option<u64>,
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
        &QueryOptions {
            allow_full_scan: true,
            ..QueryOptions::default()
        },
        0,
        usize::MAX,
    )
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
        return execute_join_query(
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
    validate_query(schema, &query)?;

    let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let page_size = query.limit.unwrap_or_else(|| {
        cursor_state
            .as_ref()
            .map(|c| c.page_size)
            .unwrap_or(max_scan_rows.min(100))
    });
    let effective_page_size = page_size;
    if let Some(result) =
        try_primary_key_point_query(schema, table, &query, &cursor_state, snapshot_seq)?
    {
        return Ok(result);
    }

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
            let rows = projection.rows.clone();
            Box::new(rows.into_iter().map(|(_, row)| row))
        } else if let (Some(predicate), Some(table)) = (&query.predicate, table) {
            let table_rows = table.rows.clone();
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
                    Box::new(
                        pks.into_iter()
                            .filter_map(move |pk| table_rows.get(&pk).cloned()),
                    )
                }
                None => {
                    estimated_rows = table.rows.len();
                    let rows = table.rows.clone();
                    Box::new(rows.into_iter().map(|(_, row)| row))
                }
            }
        } else {
            let rows = table.map(|t| t.rows.clone()).unwrap_or_default();
            estimated_rows = rows.len();
            Box::new(rows.into_iter().map(|(_, row)| row))
        };

    if estimated_rows > max_scan_rows && query.limit.is_none() && options.cursor.is_none() {
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
            ExecutionStage::Scan | ExecutionStage::Limit => {}
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
                root = Box::new(SortOperator::new(root, order_by));
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
        cursor,
        snapshot_seq,
        materialized_seq,
    })
}

#[allow(clippy::too_many_arguments)]
fn execute_join_query(
    snapshot: &KeyspaceSnapshot,
    catalog: &Catalog,
    project_id: &str,
    scope_id: &str,
    query: Query,
    options: QueryOptions,
    snapshot_seq: u64,
    max_scan_rows: usize,
    cursor_state: Option<CursorToken>,
) -> Result<QueryResult, QueryError> {
    if cursor_state.is_some() {
        return Err(QueryError::InvalidQuery {
            reason: "cursor pagination is not supported for join queries".into(),
        });
    }
    let (base_ns_project, base_ns_scope, base_table) =
        resolve_table_ref(project_id, scope_id, &query.table);
    let base_schema = catalog
        .tables
        .get(&(
            namespace_key(&base_ns_project, &base_ns_scope),
            base_table.clone(),
        ))
        .ok_or_else(|| QueryError::TableNotFound {
            project_id: base_ns_project.clone(),
            table: base_table.clone(),
        })?;
    let base_alias = query.table_alias.clone().unwrap_or(base_table.clone());
    let mut columns: Vec<String> = base_schema
        .columns
        .iter()
        .map(|c| format!("{base_alias}.{}", c.name))
        .collect();
    let base_count = snapshot
        .table(&base_ns_project, &base_ns_scope, &base_table)
        .map(|t| t.rows.len())
        .unwrap_or(0);
    if !options.allow_full_scan && base_count > max_scan_rows {
        return Err(QueryError::ScanBoundExceeded {
            estimated_rows: base_count as u64,
            max_scan_rows: max_scan_rows as u64,
        });
    }
    let mut rows: Vec<Row> = snapshot
        .table(&base_ns_project, &base_ns_scope, &base_table)
        .map(|t| t.rows.values().cloned().collect())
        .unwrap_or_default();

    for join in &query.joins {
        let (jp, js, jt) = resolve_table_ref(project_id, scope_id, &join.table);
        let join_schema = catalog
            .tables
            .get(&(namespace_key(&jp, &js), jt.clone()))
            .ok_or_else(|| QueryError::TableNotFound {
                project_id: jp.clone(),
                table: jt.clone(),
            })?;
        let join_alias = join.alias.clone().unwrap_or(jt.clone());
        let join_rows: Vec<Row> = snapshot
            .table(&jp, &js, &jt)
            .map(|t| t.rows.values().cloned().collect())
            .unwrap_or_default();
        if !options.allow_full_scan
            && rows.len().saturating_mul(join_rows.len().max(1)) > max_scan_rows
        {
            return Err(QueryError::ScanBoundExceeded {
                estimated_rows: rows.len().saturating_mul(join_rows.len().max(1)) as u64,
                max_scan_rows: max_scan_rows as u64,
            });
        }
        let join_col_offset = columns.len();
        let mut next_columns = columns.clone();
        next_columns.extend(
            join_schema
                .columns
                .iter()
                .map(|c| format!("{join_alias}.{}", c.name)),
        );
        let (left_idx, right_idx) = match join.join_type {
            JoinType::Cross => (None, None),
            _ => {
                let left = join
                    .left_column
                    .as_ref()
                    .ok_or_else(|| QueryError::InvalidQuery {
                        reason: "join requires left_column".into(),
                    })?;
                let right = join
                    .right_column
                    .as_ref()
                    .ok_or_else(|| QueryError::InvalidQuery {
                        reason: "join requires right_column".into(),
                    })?;
                let left_idx = columns.iter().position(|c| c == left).ok_or_else(|| {
                    QueryError::ColumnNotFound {
                        table: query.table.clone(),
                        column: left.clone(),
                    }
                })?;
                let right_idx = join_schema
                    .columns
                    .iter()
                    .position(|c| format!("{join_alias}.{}", c.name) == *right || c.name == *right)
                    .ok_or_else(|| QueryError::ColumnNotFound {
                        table: join.table.clone(),
                        column: right.clone(),
                    })?;
                (Some(left_idx), Some(right_idx))
            }
        };

        let mut joined = Vec::new();
        match join.join_type {
            JoinType::Cross => {
                for left in &rows {
                    for right in &join_rows {
                        let mut values = left.values.clone();
                        values.extend(right.values.clone());
                        joined.push(Row { values });
                    }
                }
            }
            JoinType::Inner | JoinType::Left => {
                // Hash join for equality predicates.
                let mut right_map: std::collections::BTreeMap<Value, Vec<&Row>> =
                    std::collections::BTreeMap::new();
                for right in &join_rows {
                    right_map
                        .entry(right.values[right_idx.expect("right idx")].clone())
                        .or_default()
                        .push(right);
                }
                for left in &rows {
                    let key = left.values[left_idx.expect("left idx")].clone();
                    if let Some(matches) = right_map.get(&key) {
                        for right in matches {
                            let mut values = left.values.clone();
                            values.extend(right.values.clone());
                            joined.push(Row { values });
                        }
                    } else if matches!(join.join_type, JoinType::Left) {
                        let mut values = left.values.clone();
                        values.extend(std::iter::repeat_n(Value::Null, join_schema.columns.len()));
                        joined.push(Row { values });
                    }
                }
            }
            JoinType::Right => {
                let mut left_map: std::collections::BTreeMap<Value, Vec<&Row>> =
                    std::collections::BTreeMap::new();
                for left in &rows {
                    left_map
                        .entry(left.values[left_idx.expect("left idx")].clone())
                        .or_default()
                        .push(left);
                }
                for right in &join_rows {
                    let key = right.values[right_idx.expect("right idx")].clone();
                    if let Some(matches) = left_map.get(&key) {
                        for left in matches {
                            let mut values = left.values.clone();
                            values.extend(right.values.clone());
                            joined.push(Row { values });
                        }
                    } else {
                        let mut values =
                            std::iter::repeat_n(Value::Null, join_col_offset).collect::<Vec<_>>();
                        values.extend(right.values.clone());
                        joined.push(Row { values });
                    }
                }
            }
        }
        rows = joined;
        if !options.allow_full_scan && rows.len() > max_scan_rows {
            return Err(QueryError::ScanBoundExceeded {
                estimated_rows: rows.len() as u64,
                max_scan_rows: max_scan_rows as u64,
            });
        }
        columns = next_columns;
    }

    if let Some(predicate) = &query.predicate {
        let compiled = compile_expr(predicate, &columns, "join")?;
        rows.retain(|r| crate::query::operators::eval_compiled_expr_public(&compiled, r));
    }

    if !query.order_by.is_empty() {
        let order_pairs: Vec<(usize, crate::query::plan::Order)> = query
            .order_by
            .iter()
            .map(|(col, ord)| {
                columns
                    .iter()
                    .position(|c| c == col)
                    .map(|idx| (idx, *ord))
                    .ok_or_else(|| QueryError::ColumnNotFound {
                        table: "join".into(),
                        column: col.clone(),
                    })
            })
            .collect::<Result<_, _>>()?;
        rows.sort_by(|a, b| {
            for (idx, ord) in &order_pairs {
                let cmp = a.values[*idx].cmp(&b.values[*idx]);
                let ord_cmp = match ord {
                    crate::query::plan::Order::Asc => cmp,
                    crate::query::plan::Order::Desc => cmp.reverse(),
                };
                if !ord_cmp.is_eq() {
                    return ord_cmp;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    if !query.select.is_empty() && query.select[0] != "*" {
        let idxs: Vec<usize> = query
            .select
            .iter()
            .map(|col| {
                columns
                    .iter()
                    .position(|c| c == col)
                    .ok_or_else(|| QueryError::ColumnNotFound {
                        table: "join".into(),
                        column: col.clone(),
                    })
            })
            .collect::<Result<_, _>>()?;
        rows = rows
            .into_iter()
            .map(|r| Row {
                values: idxs.iter().map(|i| r.values[*i].clone()).collect(),
            })
            .collect();
    }

    let hard_limit = query.limit.unwrap_or(max_scan_rows).min(max_scan_rows);
    if rows.len() > hard_limit {
        rows.truncate(hard_limit);
    }
    let _ = options;
    Ok(QueryResult {
        rows_examined: rows.len(),
        rows,
        cursor: None,
        snapshot_seq,
        materialized_seq: None,
    })
}

fn resolve_table_ref(
    project_id: &str,
    scope_id: &str,
    table_ref: &str,
) -> (String, String, String) {
    if let Some(name) = table_ref.strip_prefix("_global.") {
        return ("_global".to_string(), "app".to_string(), name.to_string());
    }
    (
        project_id.to_string(),
        scope_id.to_string(),
        table_ref.to_string(),
    )
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
            snapshot_seq,
            materialized_seq: None,
        }));
    }

    let predicate = query.predicate.as_ref().expect("checked is_some");
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
        let idx = schema
            .columns
            .iter()
            .position(|c| c.name == *col)
            .ok_or_else(|| QueryError::ColumnNotFound {
                table: query.table.clone(),
                column: col.clone(),
            })?;
        indices.push(idx);
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

fn extract_primary_key_values(predicate: &Expr, primary_key: &[String]) -> Option<Vec<Value>> {
    if primary_key.is_empty() {
        return None;
    }
    let mut equalities: HashMap<String, Value> = HashMap::new();
    if !collect_eq_constraints(predicate, &mut equalities) {
        return None;
    }
    if equalities.len() != primary_key.len() {
        return None;
    }
    let mut values = Vec::with_capacity(primary_key.len());
    for key_col in primary_key {
        let value = equalities.get(key_col)?;
        values.push(value.clone());
    }
    Some(values)
}

fn collect_eq_constraints(expr: &Expr, equalities: &mut HashMap<String, Value>) -> bool {
    match expr {
        Expr::Eq(column, value) => {
            if let Some(existing) = equalities.get(column) {
                existing == value
            } else {
                equalities.insert(column.clone(), value.clone());
                true
            }
        }
        Expr::And(lhs, rhs) => {
            collect_eq_constraints(lhs, equalities) && collect_eq_constraints(rhs, equalities)
        }
        _ => false,
    }
}

fn validate_query(schema: &TableSchema, query: &Query) -> Result<(), QueryError> {
    for (col, _) in &query.order_by {
        if !schema.columns.iter().any(|c| c.name == *col) {
            return Err(QueryError::ColumnNotFound {
                table: query.table.clone(),
                column: col.clone(),
            });
        }
    }
    for col in &query.group_by {
        if !schema.columns.iter().any(|c| c.name == *col) {
            return Err(QueryError::ColumnNotFound {
                table: query.table.clone(),
                column: col.clone(),
            });
        }
    }
    if let Some(expr) = &query.predicate {
        validate_expr_types(schema, expr)?;
    }
    Ok(())
}

fn extract_sort_key(row: &Row, sort_indices: &[(usize, crate::query::plan::Order)]) -> Vec<Value> {
    if sort_indices.is_empty() {
        return row.values.clone();
    }
    sort_indices
        .iter()
        .map(|(idx, _)| row.values[*idx].clone())
        .collect()
}

fn extract_pk_key(row: &Row, pk_indices: &[usize]) -> Vec<Value> {
    if pk_indices.is_empty() {
        return row.values.clone();
    }
    pk_indices
        .iter()
        .map(|idx| row.values[*idx].clone())
        .collect()
}

fn row_after_cursor(
    row: &Row,
    cursor: &CursorToken,
    sort_indices: &[(usize, crate::query::plan::Order)],
    pk_indices: &[usize],
) -> bool {
    let row_sort = extract_sort_key(row, sort_indices);
    let row_pk = extract_pk_key(row, pk_indices);
    if sort_indices.is_empty() {
        return row_pk > cursor.last_pk;
    }
    for ((_, order), (lhs, rhs)) in sort_indices
        .iter()
        .zip(row_sort.iter().zip(cursor.last_sort_key.iter()))
    {
        let cmp = lhs.cmp(rhs);
        if cmp.is_eq() {
            continue;
        }
        return match order {
            crate::query::plan::Order::Asc => cmp.is_gt(),
            crate::query::plan::Order::Desc => cmp.is_lt(),
        };
    }
    row_pk > cursor.last_pk
}

fn validate_expr_types(
    schema: &TableSchema,
    expr: &crate::query::plan::Expr,
) -> Result<(), QueryError> {
    use crate::catalog::types::ColumnType;
    use crate::query::plan::Expr;

    let find_col_type = |name: &str| -> Result<ColumnType, QueryError> {
        schema
            .columns
            .iter()
            .find(|c| c.name == name)
            .map(|c| c.col_type.clone())
            .ok_or_else(|| QueryError::ColumnNotFound {
                table: schema.table_name.clone(),
                column: name.to_string(),
            })
    };

    let value_compatible = |col_type: &ColumnType, value: &Value| -> bool {
        matches!(value, Value::Null)
            || match col_type {
                ColumnType::Integer => matches!(
                    value,
                    Value::Integer(_) | Value::Float(_) | Value::Timestamp(_)
                ),
                ColumnType::Float => matches!(
                    value,
                    Value::Integer(_) | Value::Float(_) | Value::Timestamp(_)
                ),
                ColumnType::Timestamp => matches!(
                    value,
                    Value::Integer(_) | Value::Float(_) | Value::Timestamp(_)
                ),
                ColumnType::Text => matches!(value, Value::Text(_)),
                ColumnType::Boolean => matches!(value, Value::Boolean(_)),
                ColumnType::U256 => matches!(value, Value::U256(_)),
                ColumnType::I256 => matches!(value, Value::I256(_)),
                ColumnType::Blob => matches!(value, Value::Blob(_)),
                ColumnType::Json => matches!(value, Value::Json(_) | Value::Text(_)),
            }
    };

    match expr {
        Expr::Eq(c, v)
        | Expr::Ne(c, v)
        | Expr::Lt(c, v)
        | Expr::Lte(c, v)
        | Expr::Gt(c, v)
        | Expr::Gte(c, v) => {
            let t = find_col_type(c)?;
            if !value_compatible(&t, v) {
                return Err(QueryError::TypeMismatch {
                    column: c.clone(),
                    expected: format!("{t:?}"),
                    got: format!("{v:?}"),
                });
            }
        }
        Expr::In(c, values) => {
            let t = find_col_type(c)?;
            if !values.iter().all(|v| value_compatible(&t, v)) {
                return Err(QueryError::TypeMismatch {
                    column: c.clone(),
                    expected: format!("{t:?}"),
                    got: "IN literal".to_string(),
                });
            }
        }
        Expr::Between(c, lo, hi) => {
            let t = find_col_type(c)?;
            if !value_compatible(&t, lo) || !value_compatible(&t, hi) {
                return Err(QueryError::TypeMismatch {
                    column: c.clone(),
                    expected: format!("{t:?}"),
                    got: "BETWEEN literal".to_string(),
                });
            }
        }
        Expr::Like(c, _) => {
            let t = find_col_type(c)?;
            if !matches!(t, ColumnType::Text) {
                return Err(QueryError::TypeMismatch {
                    column: c.clone(),
                    expected: "Text".to_string(),
                    got: format!("{t:?}"),
                });
            }
        }
        Expr::IsNull(c) | Expr::IsNotNull(c) => {
            let _ = find_col_type(c)?;
        }
        Expr::And(a, b) | Expr::Or(a, b) => {
            validate_expr_types(schema, a)?;
            validate_expr_types(schema, b)?;
        }
        Expr::Not(a) => validate_expr_types(schema, a)?,
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct CursorToken {
    snapshot_seq: u64,
    last_sort_key: Vec<Value>,
    last_pk: Vec<Value>,
    page_size: usize,
    remaining_limit: Option<usize>,
}

fn encode_cursor(cursor: &CursorToken) -> Result<String, QueryError> {
    let bytes = rmp_serde::to_vec(cursor).map_err(|e| QueryError::InternalError(e.to_string()))?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

fn decode_cursor(encoded: &str) -> Result<CursorToken, QueryError> {
    if !encoded.len().is_multiple_of(2) {
        return Err(QueryError::InvalidQuery {
            reason: "invalid cursor".into(),
        });
    }
    let mut bytes = Vec::with_capacity(encoded.len() / 2);
    let chars: Vec<char> = encoded.chars().collect();
    for i in (0..chars.len()).step_by(2) {
        let pair = [chars[i], chars[i + 1]].iter().collect::<String>();
        let b = u8::from_str_radix(&pair, 16).map_err(|_| QueryError::InvalidQuery {
            reason: "invalid cursor".into(),
        })?;
        bytes.push(b);
    }
    rmp_serde::from_slice(&bytes).map_err(|e| QueryError::InvalidQuery {
        reason: e.to_string(),
    })
}

fn aggregate_col_idx(agg: &Aggregate, columns: &[String]) -> Result<Option<usize>, QueryError> {
    let idx = match agg {
        Aggregate::Count => return Ok(None),
        Aggregate::Sum(col) | Aggregate::Min(col) | Aggregate::Max(col) | Aggregate::Avg(col) => {
            columns
                .iter()
                .position(|c| c == col)
                .ok_or_else(|| QueryError::ColumnNotFound {
                    table: "".to_string(),
                    column: col.clone(),
                })?
        }
    };
    Ok(Some(idx))
}

fn aggregate_output_name(agg: &Aggregate) -> String {
    match agg {
        Aggregate::Count => "count_star".to_string(),
        Aggregate::Sum(col) => format!("sum_{col}"),
        Aggregate::Min(col) => format!("min_{col}"),
        Aggregate::Max(col) => format!("max_{col}"),
        Aggregate::Avg(col) => format!("avg_{col}"),
    }
}

fn indexed_pks_for_predicate(
    catalog: &Catalog,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    table: &crate::storage::keyspace::TableData,
    predicate: &crate::query::plan::Expr,
) -> Result<Option<Vec<EncodedKey>>, QueryError> {
    let mut equalities = HashMap::new();
    let eq_only = collect_eq_constraints(predicate, &mut equalities);
    let Some(lookup) = extract_indexable_predicate(predicate) else {
        if !eq_only {
            return Ok(None);
        }
        // Composite + leftmost-prefix support for conjunctions of equality predicates.
        let ns = namespace_key(project_id, scope_id);
        let mut best: Option<(String, usize)> = None;
        for ((p, t, idx_name), idx_def) in &catalog.indexes {
            if p != &ns || t != table_name || !table.indexes.contains_key(idx_name) {
                continue;
            }
            if let Some(filter) = &idx_def.partial_filter
                && !expr_implied_by_eq_constraints(filter, &equalities)
            {
                continue;
            }
            let mut prefix_cols = 0usize;
            for col in &idx_def.columns {
                if equalities.contains_key(col) {
                    prefix_cols += 1;
                } else {
                    break;
                }
            }
            if prefix_cols == 0 {
                continue;
            }
            if best.as_ref().map(|(_, c)| *c).unwrap_or(0) < prefix_cols {
                best = Some((idx_name.clone(), prefix_cols));
            }
        }
        let Some((idx_name, prefix_cols)) = best else {
            return Ok(None);
        };
        let index = table
            .indexes
            .get(&idx_name)
            .ok_or_else(|| QueryError::InvalidQuery {
                reason: "index not found".into(),
            })?;
        let idx_def = catalog
            .indexes
            .get(&(ns, table_name.to_string(), idx_name.clone()))
            .ok_or_else(|| QueryError::InvalidQuery {
                reason: "index definition not found".into(),
            })?;
        let prefix_values = idx_def
            .columns
            .iter()
            .take(prefix_cols)
            .filter_map(|c| equalities.get(c).cloned())
            .collect::<Vec<_>>();
        let encoded = EncodedKey::from_values(&prefix_values);
        let pks = if prefix_cols == idx_def.columns.len() {
            index.scan_eq(&encoded)
        } else {
            index.scan_prefix(&encoded)
        };
        return Ok(Some(pks));
    };
    let column = match &lookup {
        IndexLookup::Range { column, .. } => column,
        IndexLookup::MultiEq { column, .. } => column,
    };

    let mut selected_index_name: Option<String> = None;
    let ns = namespace_key(project_id, scope_id);
    for ((p, t, idx_name), idx_def) in &catalog.indexes {
        if p == &ns
            && t == table_name
            && idx_def.columns.len() == 1
            && idx_def.columns[0] == *column
            && idx_def
                .partial_filter
                .as_ref()
                .map(|f| expr_implied_by_eq_constraints(f, &equalities))
                .unwrap_or(true)
            && table.indexes.contains_key(idx_name)
        {
            selected_index_name = Some(idx_name.clone());
            break;
        }
    }

    let Some(index_name) = selected_index_name else {
        return Ok(None);
    };
    let Some(index) = table.indexes.get(&index_name) else {
        return Ok(None);
    };

    let pks = match lookup {
        IndexLookup::Range { bounds, .. } => index.scan_range(bounds.0, bounds.1),
        IndexLookup::MultiEq { values, .. } => values
            .into_iter()
            .flat_map(|v| index.scan_eq(&EncodedKey::from_values(&[v])))
            .collect(),
    };
    Ok(Some(pks))
}

fn expr_implied_by_eq_constraints(
    expr: &crate::query::plan::Expr,
    equalities: &HashMap<String, Value>,
) -> bool {
    use crate::query::plan::Expr;
    match expr {
        Expr::Eq(col, val) => equalities.get(col) == Some(val),
        Expr::And(lhs, rhs) => {
            expr_implied_by_eq_constraints(lhs, equalities)
                && expr_implied_by_eq_constraints(rhs, equalities)
        }
        _ => false,
    }
}

fn extract_indexable_predicate(predicate: &crate::query::plan::Expr) -> Option<IndexLookup> {
    use crate::query::plan::Expr;

    match predicate {
        Expr::Eq(c, v) => Some(IndexLookup::Range {
            column: c.clone(),
            bounds: (
                Bound::Included(EncodedKey::from_values(std::slice::from_ref(v))),
                Bound::Included(EncodedKey::from_values(std::slice::from_ref(v))),
            ),
        }),
        Expr::In(c, values) => Some(IndexLookup::MultiEq {
            column: c.clone(),
            values: values.clone(),
        }),
        Expr::Lt(c, v) => Some(IndexLookup::Range {
            column: c.clone(),
            bounds: (
                Bound::Unbounded,
                Bound::Excluded(EncodedKey::from_values(std::slice::from_ref(v))),
            ),
        }),
        Expr::Lte(c, v) => Some(IndexLookup::Range {
            column: c.clone(),
            bounds: (
                Bound::Unbounded,
                Bound::Included(EncodedKey::from_values(std::slice::from_ref(v))),
            ),
        }),
        Expr::Gt(c, v) => Some(IndexLookup::Range {
            column: c.clone(),
            bounds: (
                Bound::Excluded(EncodedKey::from_values(std::slice::from_ref(v))),
                Bound::Unbounded,
            ),
        }),
        Expr::Gte(c, v) => Some(IndexLookup::Range {
            column: c.clone(),
            bounds: (
                Bound::Included(EncodedKey::from_values(std::slice::from_ref(v))),
                Bound::Unbounded,
            ),
        }),
        Expr::Between(c, lo, hi) => Some(IndexLookup::Range {
            column: c.clone(),
            bounds: (
                Bound::Included(EncodedKey::from_values(std::slice::from_ref(lo))),
                Bound::Included(EncodedKey::from_values(std::slice::from_ref(hi))),
            ),
        }),
        Expr::Like(c, pattern) => {
            let prefix = like_prefix(pattern)?;
            let start = Bound::Included(EncodedKey::from_values(&[Value::Text(
                prefix.clone().into(),
            )]));
            let end = match next_prefix(&prefix) {
                Some(next) => Bound::Excluded(EncodedKey::from_values(&[Value::Text(next.into())])),
                None => Bound::Unbounded,
            };
            Some(IndexLookup::Range {
                column: c.clone(),
                bounds: (start, end),
            })
        }
        Expr::And(lhs, rhs) => {
            extract_indexable_predicate(lhs).or_else(|| extract_indexable_predicate(rhs))
        }
        _ => None,
    }
}

fn like_prefix(pattern: &str) -> Option<String> {
    if !pattern.ends_with('%') {
        return None;
    }
    let mut prefix = String::new();
    for ch in pattern.chars() {
        if ch == '%' || ch == '_' {
            break;
        }
        prefix.push(ch);
    }
    if prefix.is_empty() {
        return None;
    }
    Some(prefix)
}

fn next_prefix(prefix: &str) -> Option<String> {
    let mut bytes = prefix.as_bytes().to_vec();
    for i in (0..bytes.len()).rev() {
        if bytes[i] != u8::MAX {
            bytes[i] += 1;
            bytes.truncate(i + 1);
            return String::from_utf8(bytes).ok();
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{execute_query, execute_query_with_options};
    use crate::catalog::Catalog;
    use crate::catalog::namespace_key;
    use crate::catalog::schema::{ColumnDef, IndexType};
    use crate::catalog::types::{ColumnType, Row, Value};
    use crate::query::error::QueryError;
    use crate::query::plan::{Aggregate, Expr, Order, Query, QueryOptions, col, lit};
    use crate::storage::encoded_key::EncodedKey;
    use crate::storage::index::extract_index_key_encoded;
    use crate::storage::keyspace::{Keyspace, NamespaceId, SecondaryIndex};

    fn setup() -> (Keyspace, Catalog) {
        let mut keyspace = Keyspace::default();
        let mut catalog = Catalog::default();
        catalog.create_project("A").expect("project A");
        catalog.create_project("B").expect("project B");
        for p in ["A", "B"] {
            catalog
                .create_table(
                    p,
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
                        ColumnDef {
                            name: "age".into(),
                            col_type: ColumnType::Integer,
                            nullable: false,
                        },
                        ColumnDef {
                            name: "email".into(),
                            col_type: ColumnType::Text,
                            nullable: true,
                        },
                    ],
                    vec!["id".into()],
                )
                .expect("table");
        }
        for i in 0..100 {
            keyspace.upsert_row(
                "A",
                "app",
                "users",
                vec![Value::Integer(i)],
                Row {
                    values: vec![
                        Value::Integer(i),
                        Value::Text(format!("u{i}").into()),
                        Value::Integer(18 + (i % 50)),
                        if i == 0 {
                            Value::Null
                        } else if i % 2 == 0 {
                            Value::Text(format!("u{i}@gmail.com").into())
                        } else {
                            Value::Text(format!("u{i}@example.com").into())
                        },
                    ],
                },
                i as u64 + 1,
            );
            keyspace.upsert_row(
                "B",
                "app",
                "users",
                vec![Value::Integer(i)],
                Row {
                    values: vec![
                        Value::Integer(i),
                        Value::Text(format!("b{i}").into()),
                        Value::Integer(99),
                        Value::Text(format!("b{i}@other.com").into()),
                    ],
                },
                i as u64 + 10_000,
            );
        }
        catalog
            .create_index(
                "A",
                "app",
                "users",
                "by_age",
                vec!["age".into()],
                IndexType::BTree,
                None,
            )
            .expect("create index");
        catalog
            .create_index(
                "A",
                "app",
                "users",
                "by_name",
                vec!["name".into()],
                IndexType::BTree,
                None,
            )
            .expect("create name index");
        let schema = catalog
            .tables
            .get(&(namespace_key("A", "app"), "users".to_string()))
            .expect("schema")
            .clone();
        let table = keyspace
            .table_by_namespace_key_mut(&namespace_key("A", "app"), "users")
            .expect("table");
        let mut index = SecondaryIndex::default();
        for (pk, row) in &table.rows {
            let age_key =
                extract_index_key_encoded(row, &schema, &["age".into()]).expect("age index key");
            index.insert(age_key, pk.clone());
        }
        table.indexes.insert("by_age".into(), index);
        let mut by_name = SecondaryIndex::default();
        for (pk, row) in &table.rows {
            let key =
                extract_index_key_encoded(row, &schema, &["name".into()]).expect("name index key");
            by_name.insert(key, pk.clone());
        }
        table.indexes.insert("by_name".into(), by_name);
        (keyspace, catalog)
    }

    #[test]
    fn query_correctness_suite() {
        let (keyspace, catalog) = setup();
        let snapshot = keyspace.snapshot();

        let all = execute_query(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["*"]).from("users"),
        )
        .expect("all");
        assert_eq!(all.rows.len(), 100);

        let filtered = execute_query(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["*"])
                .from("users")
                .where_(Expr::Gt("age".into(), Value::Integer(30))),
        )
        .expect("filtered");
        assert!(
            filtered
                .rows
                .iter()
                .all(|r| matches!(r.values[2], Value::Integer(v) if v > 30))
        );

        let ordered = execute_query(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["*"])
                .from("users")
                .order_by("age", Order::Desc)
                .order_by("id", Order::Asc),
        )
        .expect("ordered");
        for w in ordered.rows.windows(2) {
            assert!(w[0].values[2] >= w[1].values[2]);
        }

        let limited = execute_query(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["*"]).from("users").limit(5),
        )
        .expect("limit");
        assert_eq!(limited.rows.len(), 5);

        let counted = execute_query(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["*"])
                .from("users")
                .aggregate(Aggregate::Count),
        )
        .expect("count");
        assert_eq!(counted.rows[0].values[0], Value::Integer(100));

        let grouped = execute_query(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["*"])
                .from("users")
                .group_by(&["age"])
                .aggregate(Aggregate::Count),
        )
        .expect("grouped");
        assert!(!grouped.rows.is_empty());

        let compound = execute_query(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["*"]).from("users").where_(
                Expr::Gt("age".into(), Value::Integer(30))
                    .and(Expr::Like("email".into(), "%@gmail.com".into())),
            ),
        )
        .expect("compound");
        assert!(compound.rows.iter().all(|r| {
            matches!(&r.values[2], Value::Integer(v) if *v > 30)
                && matches!(&r.values[3], Value::Text(s) if s.ends_with("@gmail.com"))
        }));

        let project_b = execute_query(
            &snapshot,
            &catalog,
            "B",
            "app",
            Query::select(&["*"])
                .from("users")
                .where_(Expr::Eq("age".into(), Value::Integer(99))),
        )
        .expect("project B");
        assert_eq!(project_b.rows.len(), 100);
    }

    #[test]
    fn builder_supports_not_is_not_null_and_like_underscore() {
        let (keyspace, catalog) = setup();
        let snapshot = keyspace.snapshot();

        let query = Query::select(&["id", "email"]).from("users").where_(
            col("email")
                .is_not_null()
                .and(col("name").like(lit("u_")))
                .and(col("age").gt(lit(20)).not().not()),
        );
        let result = execute_query(&snapshot, &catalog, "A", "app", query).expect("query");
        assert!(!result.rows.is_empty());
        assert!(
            result
                .rows
                .iter()
                .all(|r| matches!(&r.values[1], Value::Text(_)))
        );
    }

    #[test]
    fn having_filters_post_aggregation() {
        let (keyspace, catalog) = setup();
        let snapshot = keyspace.snapshot();

        let result = execute_query(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["age", "count_star"])
                .from("users")
                .group_by(&["age"])
                .aggregate(Aggregate::Count)
                .having(Expr::Gt("count_star".into(), Value::Integer(1))),
        )
        .expect("having");

        assert!(
            result
                .rows
                .iter()
                .all(|r| matches!(r.values[1], Value::Integer(v) if v > 1))
        );
    }

    #[test]
    fn index_backed_range_scan_reduces_examined_rows() {
        let (keyspace, catalog) = setup();
        let snapshot = keyspace.snapshot();

        let full = execute_query(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["*"]).from("users"),
        )
        .expect("full");
        let ranged = execute_query(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["*"]).from("users").where_(Expr::Between(
                "age".into(),
                Value::Integer(40),
                Value::Integer(41),
            )),
        )
        .expect("range");
        assert!(ranged.rows.len() < full.rows.len());
        assert!(ranged.rows_examined < full.rows_examined);
    }

    #[test]
    fn primary_key_eq_uses_point_lookup_path() {
        let (keyspace, catalog) = setup();
        let snapshot = keyspace.snapshot();

        let result = execute_query(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["id", "name"])
                .from("users")
                .where_(Expr::Eq("id".into(), Value::Integer(42)))
                .limit(1),
        )
        .expect("pk point query");

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], Value::Integer(42));
        assert_eq!(result.rows_examined, 1);
    }

    #[test]
    fn primary_key_with_non_pk_eq_falls_back_to_general_path() {
        let (keyspace, catalog) = setup();
        let snapshot = keyspace.snapshot();

        let result = execute_query(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["id", "name"])
                .from("users")
                .where_(
                    Expr::Eq("id".into(), Value::Integer(42))
                        .and(Expr::Eq("age".into(), Value::Integer(60))),
                )
                .limit(1),
        )
        .expect("mixed eq query");

        assert_eq!(result.rows.len(), 1);
        assert!(result.rows_examined > 1);
    }

    #[test]
    fn use_index_hint_selects_async_projection() {
        let mut keyspace = Keyspace::default();
        let mut catalog = Catalog::default();
        catalog.create_project("A").expect("project A");
        catalog
            .create_table(
                "A",
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
        keyspace.upsert_row(
            "A",
            "app",
            "users",
            vec![Value::Integer(1)],
            Row {
                values: vec![Value::Integer(1), Value::Text("alice".into())],
            },
            1,
        );
        keyspace.insert_async_projection(
            NamespaceId::Project(namespace_key("A", "app")),
            "users".into(),
            "users_view".into(),
            crate::storage::keyspace::AsyncProjectionData {
                rows: {
                    let mut rows = im::OrdMap::new();
                    rows.insert(
                        EncodedKey::from_values(&[Value::Integer(9)]),
                        Row {
                            values: vec![Value::Integer(9), Value::Text("projection".into())],
                        },
                    );
                    rows
                },
                materialized_seq: 123,
            },
        );
        let snapshot = keyspace.snapshot();

        let result = execute_query(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["*"]).from("users").use_index("users_view"),
        )
        .expect("hint query");

        assert_eq!(result.materialized_seq, Some(123));
        assert_eq!(result.rows[0].values[0], Value::Integer(9));
    }

    #[test]
    fn in_and_like_prefix_can_use_index_path() {
        let (keyspace, catalog) = setup();
        let snapshot = keyspace.snapshot();

        let by_in = execute_query(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["*"]).from("users").where_(Expr::In(
                "age".into(),
                vec![Value::Integer(40), Value::Integer(41)],
            )),
        )
        .expect("in");
        assert!(
            by_in
                .rows
                .iter()
                .all(|r| { matches!(r.values[2], Value::Integer(40) | Value::Integer(41)) })
        );

        let by_prefix = execute_query(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["*"])
                .from("users")
                .where_(Expr::Like("name".into(), "u1%".into())),
        )
        .expect("prefix like");
        assert!(
            by_prefix
                .rows
                .iter()
                .all(|r| matches!(&r.values[1], Value::Text(s) if s.starts_with("u1")))
        );
    }

    #[test]
    fn composite_index_respects_leftmost_prefix_rule() {
        let (mut keyspace, mut catalog) = setup();
        catalog
            .create_index(
                "A",
                "app",
                "users",
                "by_age_name",
                vec!["age".into(), "name".into()],
                IndexType::BTree,
                None,
            )
            .expect("composite index");
        let schema = catalog
            .tables
            .get(&(namespace_key("A", "app"), "users".to_string()))
            .expect("schema")
            .clone();
        let table = keyspace
            .table_by_namespace_key_mut(&namespace_key("A", "app"), "users")
            .expect("table");
        let mut by_age_name = SecondaryIndex::default();
        for (pk, row) in &table.rows {
            let key = extract_index_key_encoded(row, &schema, &["age".into(), "name".into()])
                .expect("composite key");
            by_age_name.insert(key, pk.clone());
        }
        table.indexes.insert("by_age_name".into(), by_age_name);

        let snapshot = keyspace.snapshot();

        let good = execute_query(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["id", "name", "age"])
                .from("users")
                .where_(Expr::Eq("age".into(), Value::Integer(40))),
        )
        .expect("leftmost predicate should use composite index");
        assert!(
            good.rows_examined < 100,
            "leftmost-prefix query should avoid full scan"
        );

        let bad = execute_query(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["id", "name", "age"])
                .from("users")
                .where_(Expr::Eq(
                    "email".into(),
                    Value::Text("u1@example.com".into()),
                )),
        )
        .expect("non-leftmost predicate falls back");
        assert!(
            bad.rows_examined >= good.rows_examined,
            "non-leftmost query should not be better than leftmost"
        );
    }

    #[test]
    fn partial_index_only_indexes_matching_rows() {
        let (mut keyspace, mut catalog) = setup();
        catalog
            .create_index(
                "A",
                "app",
                "users",
                "adults_only",
                vec!["age".into()],
                IndexType::BTree,
                Some(Expr::Gte("age".into(), Value::Integer(50))),
            )
            .expect("partial index");
        let schema = catalog
            .tables
            .get(&(namespace_key("A", "app"), "users".to_string()))
            .expect("schema")
            .clone();
        let table = keyspace
            .table_by_namespace_key_mut(&namespace_key("A", "app"), "users")
            .expect("table");
        let mut adults_only = SecondaryIndex {
            partial_filter: Some(Expr::Gte("age".into(), Value::Integer(50))),
            ..SecondaryIndex::default()
        };
        for (pk, row) in &table.rows {
            if adults_only
                .should_include_row(row, &schema, "users")
                .expect("partial eval")
            {
                let key =
                    extract_index_key_encoded(row, &schema, &["age".into()]).expect("index key");
                adults_only.insert(key, pk.clone());
            }
        }
        table.indexes.insert("adults_only".into(), adults_only);

        let snapshot = keyspace.snapshot();
        let result = execute_query(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["id", "age"])
                .from("users")
                .where_(Expr::Gte("age".into(), Value::Integer(50))),
        )
        .expect("partial query");
        assert!(!result.rows.is_empty());
        assert!(
            result
                .rows
                .iter()
                .all(|r| matches!(r.values[1], Value::Integer(v) if v >= 50))
        );
    }

    #[test]
    fn bounded_scan_is_enforced_when_full_scan_not_allowed() {
        let (keyspace, catalog) = setup();
        let snapshot = keyspace.snapshot();
        let err = execute_query_with_options(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["*"]).from("users"),
            &QueryOptions::default(),
            1,
            10_000,
        )
        .expect_err("should reject full scan");
        assert!(matches!(err, QueryError::InvalidQuery { .. }));
    }

    #[test]
    fn join_scan_bound_is_enforced_when_full_scan_not_allowed() {
        let (keyspace, mut catalog) = setup();
        catalog
            .create_table(
                "A",
                "app",
                "profiles",
                vec![
                    ColumnDef {
                        name: "user_id".into(),
                        col_type: ColumnType::Integer,
                        nullable: false,
                    },
                    ColumnDef {
                        name: "country".into(),
                        col_type: ColumnType::Text,
                        nullable: false,
                    },
                ],
                vec!["user_id".into()],
            )
            .expect("profiles table");
        let mut keyspace = keyspace;
        for i in 0..50 {
            keyspace.upsert_row(
                "A",
                "app",
                "profiles",
                vec![Value::Integer(i)],
                Row::from_values(vec![Value::Integer(i), Value::Text("US".into())]),
                1,
            );
        }
        let snapshot = keyspace.snapshot();
        let err = execute_query_with_options(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["u.id", "p.country"])
                .from("users")
                .alias("u")
                .inner_join("profiles", "u.id", "user_id")
                .with_last_join_alias("p")
                .limit(10),
            &QueryOptions::default(),
            1,
            1_000,
        )
        .expect_err("join scan bound");
        assert!(matches!(err, QueryError::ScanBoundExceeded { .. }));
    }

    #[test]
    fn type_mismatch_rejected_at_plan_time() {
        let (keyspace, catalog) = setup();
        let snapshot = keyspace.snapshot();
        let err = execute_query(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["*"])
                .from("users")
                .where_(Expr::Gt("age".into(), Value::Text("oops".into()))),
        )
        .expect_err("type mismatch");
        assert!(matches!(err, QueryError::TypeMismatch { .. }));
    }

    #[test]
    fn cursor_pagination_returns_stable_pages() {
        let (keyspace, catalog) = setup();
        let snapshot = keyspace.snapshot();
        let mut options = QueryOptions::default();
        let mut all = Vec::new();
        loop {
            let page = execute_query_with_options(
                &snapshot,
                &catalog,
                "A",
                "app",
                Query::select(&["*"])
                    .from("users")
                    .order_by("id", Order::Asc)
                    .limit(10),
                &options,
                42,
                10_000,
            )
            .expect("page");
            all.extend(page.rows.clone());
            if let Some(cursor) = page.cursor {
                options.cursor = Some(cursor);
            } else {
                break;
            }
        }
        assert_eq!(all.len(), 100);
        for (i, row) in all.iter().enumerate().take(100) {
            assert_eq!(row.values[0], Value::Integer(i as i64));
        }
    }

    #[test]
    fn inner_join_returns_matching_rows() {
        let (keyspace, mut catalog) = setup();
        catalog
            .create_table(
                "A",
                "app",
                "profiles",
                vec![
                    ColumnDef {
                        name: "user_id".into(),
                        col_type: ColumnType::Integer,
                        nullable: false,
                    },
                    ColumnDef {
                        name: "country".into(),
                        col_type: ColumnType::Text,
                        nullable: false,
                    },
                ],
                vec!["user_id".into()],
            )
            .expect("profiles table");
        let mut keyspace = keyspace;
        for i in 0..50 {
            keyspace.upsert_row(
                "A",
                "app",
                "profiles",
                vec![Value::Integer(i)],
                Row::from_values(vec![Value::Integer(i), Value::Text("US".into())]),
                1,
            );
        }
        let snapshot = keyspace.snapshot();
        let result = execute_query(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["u.id", "p.country"])
                .from("users")
                .alias("u")
                .inner_join("profiles", "u.id", "user_id")
                .with_last_join_alias("p")
                .limit(100),
        )
        .expect("join query");
        assert_eq!(result.rows.len(), 50);
    }

    #[test]
    fn left_join_supports_global_table_reference() {
        let (mut keyspace, mut catalog) = setup();
        catalog.create_project("_global").expect("global project");
        catalog
            .create_table(
                "_global",
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
            .expect("global users");
        for i in 0..20 {
            keyspace.upsert_row(
                "_global",
                "app",
                "users",
                vec![Value::Integer(i)],
                Row::from_values(vec![Value::Integer(i), Value::Text(format!("g{i}").into())]),
                1,
            );
        }
        let snapshot = keyspace.snapshot();
        let result = execute_query(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["u.id", "g.name"])
                .from("users")
                .alias("u")
                .left_join("_global.users", "u.id", "id")
                .with_last_join_alias("g")
                .limit(5),
        )
        .expect("left join");
        assert_eq!(result.rows.len(), 5);
    }

    #[test]
    fn invalid_cursor_is_rejected() {
        let (keyspace, catalog) = setup();
        let snapshot = keyspace.snapshot();
        let options = QueryOptions {
            cursor: Some("xyz".into()),
            ..QueryOptions::default()
        };
        let err = execute_query_with_options(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["*"])
                .from("users")
                .order_by("id", Order::Asc),
            &options,
            42,
            10_000,
        )
        .expect_err("invalid cursor should fail");
        assert!(matches!(err, QueryError::InvalidQuery { .. }));
    }

    #[test]
    fn cursor_snapshot_mismatch_is_rejected() {
        let (keyspace, catalog) = setup();
        let snapshot = keyspace.snapshot();
        let first = execute_query_with_options(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["*"])
                .from("users")
                .order_by("id", Order::Asc)
                .limit(10),
            &QueryOptions::default(),
            42,
            10_000,
        )
        .expect("first page");
        let options = QueryOptions {
            cursor: first.cursor,
            ..QueryOptions::default()
        };
        let err = execute_query_with_options(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["*"])
                .from("users")
                .order_by("id", Order::Asc)
                .limit(10),
            &options,
            43,
            10_000,
        )
        .expect_err("snapshot mismatch");
        assert!(matches!(err, QueryError::InvalidQuery { .. }));
    }

    #[test]
    fn join_query_rejects_cursor_pagination() {
        let (keyspace, mut catalog) = setup();
        catalog
            .create_table(
                "A",
                "app",
                "profiles",
                vec![
                    ColumnDef {
                        name: "user_id".into(),
                        col_type: ColumnType::Integer,
                        nullable: false,
                    },
                    ColumnDef {
                        name: "country".into(),
                        col_type: ColumnType::Text,
                        nullable: false,
                    },
                ],
                vec!["user_id".into()],
            )
            .expect("profiles table");
        let snapshot = keyspace.snapshot();

        let first = execute_query_with_options(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["*"])
                .from("users")
                .order_by("id", Order::Asc)
                .limit(5),
            &QueryOptions::default(),
            7,
            10_000,
        )
        .expect("first page");

        let options = QueryOptions {
            cursor: first.cursor,
            ..QueryOptions::default()
        };
        let err = execute_query_with_options(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["u.id", "p.country"])
                .from("users")
                .alias("u")
                .inner_join("profiles", "u.id", "user_id")
                .with_last_join_alias("p")
                .limit(5),
            &options,
            7,
            10_000,
        )
        .expect_err("join cursor should fail");
        assert!(matches!(err, QueryError::InvalidQuery { .. }));
    }

    #[test]
    fn right_join_includes_unmatched_right_rows_with_nulls() {
        let (mut keyspace, mut catalog) = setup();
        catalog
            .create_table(
                "A",
                "app",
                "profiles",
                vec![
                    ColumnDef {
                        name: "user_id".into(),
                        col_type: ColumnType::Integer,
                        nullable: false,
                    },
                    ColumnDef {
                        name: "country".into(),
                        col_type: ColumnType::Text,
                        nullable: false,
                    },
                ],
                vec!["user_id".into()],
            )
            .expect("profiles table");
        for i in 90..110 {
            keyspace.upsert_row(
                "A",
                "app",
                "profiles",
                vec![Value::Integer(i)],
                Row::from_values(vec![Value::Integer(i), Value::Text("US".into())]),
                1,
            );
        }
        let snapshot = keyspace.snapshot();
        let result = execute_query(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["u.id", "p.user_id"])
                .from("users")
                .alias("u")
                .right_join("profiles", "u.id", "user_id")
                .with_last_join_alias("p")
                .order_by("p.user_id", Order::Asc)
                .limit(200),
        )
        .expect("right join");

        assert_eq!(result.rows.len(), 20);
        let unmatched = result
            .rows
            .iter()
            .filter(|r| matches!(r.values[0], Value::Null))
            .count();
        assert_eq!(unmatched, 10);
    }

    #[test]
    fn cross_join_cardinality_and_limit_are_correct() {
        let (mut keyspace, mut catalog) = setup();
        catalog
            .create_table(
                "A",
                "app",
                "profiles",
                vec![
                    ColumnDef {
                        name: "user_id".into(),
                        col_type: ColumnType::Integer,
                        nullable: false,
                    },
                    ColumnDef {
                        name: "country".into(),
                        col_type: ColumnType::Text,
                        nullable: false,
                    },
                ],
                vec!["user_id".into()],
            )
            .expect("profiles table");
        for i in 0..5 {
            keyspace.upsert_row(
                "A",
                "app",
                "profiles",
                vec![Value::Integer(i)],
                Row::from_values(vec![Value::Integer(i), Value::Text("US".into())]),
                1,
            );
        }
        let snapshot = keyspace.snapshot();
        let result = execute_query(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["u.id", "p.user_id"])
                .from("users")
                .alias("u")
                .cross_join("profiles")
                .with_last_join_alias("p")
                .limit(123),
        )
        .expect("cross join");
        assert_eq!(result.rows.len(), 123);
    }

    #[test]
    fn descending_cursor_pagination_is_stable() {
        let (keyspace, catalog) = setup();
        let snapshot = keyspace.snapshot();
        let mut options = QueryOptions::default();
        let mut all = Vec::new();
        loop {
            let page = execute_query_with_options(
                &snapshot,
                &catalog,
                "A",
                "app",
                Query::select(&["*"])
                    .from("users")
                    .order_by("id", Order::Desc)
                    .limit(11),
                &options,
                55,
                10_000,
            )
            .expect("page");
            all.extend(page.rows.clone());
            if let Some(cursor) = page.cursor {
                options.cursor = Some(cursor);
            } else {
                break;
            }
        }
        assert_eq!(all.len(), 100);
        for w in all.windows(2) {
            assert!(w[0].values[0] > w[1].values[0]);
        }
    }

    #[test]
    fn contradictory_pk_equalities_return_empty_result() {
        let (keyspace, catalog) = setup();
        let snapshot = keyspace.snapshot();
        let result = execute_query(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["id"])
                .from("users")
                .where_(
                    Expr::Eq("id".into(), Value::Integer(1))
                        .and(Expr::Eq("id".into(), Value::Integer(2))),
                )
                .limit(10),
        )
        .expect("query");
        assert!(result.rows.is_empty());
    }
}
