use super::aggregate::{aggregate_col_idx, aggregate_output_name};
use super::pagination::{
    compute_page_window, compute_remaining_limit_after_page, compute_split_recommended,
};
use super::{
    CursorToken, QueryResult, encode_cursor, extract_pk_key, extract_sort_key, row_after_cursor,
};
use crate::catalog::Catalog;
use crate::catalog::namespace_key;
use crate::catalog::types::{ColumnType, Row, Value};
use crate::query::error::QueryError;
use crate::query::operators::{
    AggregateOperator, CompiledExpr, Operator, ScanOperator, compile_expr,
    eval_compiled_expr_public, value_compare,
};
use crate::query::plan::{CompareOp, Expr, JoinCond, JoinType, Query, QueryOptions};
use crate::storage::encoded_key::EncodedKey;
use crate::storage::keyspace::KeyspaceSnapshot;
use crate::storage::keyspace::memory_accounting::row_mem_cost;
use std::collections::HashMap;

/// Hard ceiling on the bytes of materialized rows a single join query may buffer
/// in memory at once. Row-count limits (`max_scan_rows`) do not bound memory
/// when rows are large (big Text/Json columns), so this guards the host — e.g.
/// a WASM program — against OOM from a query that returns few but huge rows.
const MAX_JOIN_MATERIALIZED_BYTES: u64 = 512 * 1024 * 1024;

/// Estimate the resident byte cost of a materialized row set and reject it if it
/// exceeds `max_bytes`. Enforced unconditionally (even under `allow_full_scan`):
/// the scan-row override relaxes the row-count bound, not the host
/// memory-safety ceiling.
pub(super) fn enforce_materialization_budget(
    rows: &[Row],
    max_bytes: u64,
) -> Result<(), QueryError> {
    let mut total: u64 = 0;
    for row in rows {
        total = total.saturating_add(row_mem_cost(row) as u64);
        if total > max_bytes {
            return Err(QueryError::MaterializationBudgetExceeded {
                estimated_bytes: total,
                max_bytes,
            });
        }
    }
    Ok(())
}

pub(super) struct JoinQueryExecutionRequest<'a> {
    pub snapshot: &'a KeyspaceSnapshot,
    pub catalog: &'a Catalog,
    pub project_id: &'a str,
    pub scope_id: &'a str,
    pub query: Query,
    pub options: QueryOptions,
    pub snapshot_seq: u64,
    pub max_scan_rows: usize,
    pub cursor_state: Option<CursorToken>,
    pub cursor_signing_key: Option<&'a [u8; 32]>,
}

pub(super) fn execute_join_query(
    request: JoinQueryExecutionRequest<'_>,
) -> Result<QueryResult, QueryError> {
    let JoinQueryExecutionRequest {
        snapshot,
        catalog,
        project_id,
        scope_id,
        query,
        options,
        snapshot_seq,
        max_scan_rows,
        cursor_state,
        cursor_signing_key,
    } = request;

    if query.offset.unwrap_or(0) > 0 && query.limit.is_none() {
        return Err(QueryError::InvalidQuery {
            reason: "OFFSET requires LIMIT".into(),
        });
    }
    if cursor_state.is_some() && query.offset.unwrap_or(0) > 0 {
        return Err(QueryError::InvalidQuery {
            reason: "OFFSET cannot be combined with cursor pagination".into(),
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
    // Column types tracked in lockstep with `columns` so the PK-probe fast path
    // can be gated on exact left/right type equality (see `can_probe_right_primary_key`).
    let mut column_types: Vec<ColumnType> = base_schema
        .columns
        .iter()
        .map(|c| c.col_type.clone())
        .collect();
    let base_count = snapshot
        .table(&base_ns_project, &base_ns_scope, &base_table)
        .map(|t| t.rows.len())
        .unwrap_or(0);
    // Push down null-rejecting conjuncts that reference only the base table so
    // the join consumes a smaller input. The full predicate is still applied
    // after the join, so this only removes rows that could not have survived it.
    // Transitivity additionally propagates WHERE equality constants across join
    // equi-keys (e.g. WHERE u.id = 5 with u.id = p.user_id ⇒ p.user_id = 5).
    let derived = compute_transitive_constants(&query, project_id, scope_id);
    let base_pushdown = alias_pushdown_expr(query.predicate.as_ref(), &base_alias, &derived);
    let base_filter = compile_pushdown(base_pushdown.as_ref(), &columns)?;
    // When the pushed-down base predicate is indexable, drive the base row
    // source from an index lookup instead of scanning the whole table — only the
    // matching primary keys are paged in.
    let mut rows: Vec<Row> = if let Some(base_rows) = indexed_base_rows(IndexedBaseRowsRequest {
        snapshot,
        catalog,
        schema: base_schema,
        project_id: &base_ns_project,
        scope_id: &base_ns_scope,
        table_name: &base_table,
        base_alias: &base_alias,
        pushdown: base_pushdown.as_ref(),
        residual_filter: base_filter.as_ref(),
    })? {
        base_rows
    } else {
        // No usable base index: fall back to a full scan, but guard against
        // unbounded base tables first.
        if !options.allow_full_scan && base_count > max_scan_rows {
            return Err(QueryError::ScanBoundExceeded {
                estimated_rows: base_count as u64,
                max_scan_rows: max_scan_rows as u64,
            });
        }
        materialize_table_rows(
            snapshot,
            snapshot.table(&base_ns_project, &base_ns_scope, &base_table),
            base_filter.as_ref(),
        )?
    };
    enforce_materialization_budget(&rows, MAX_JOIN_MATERIALIZED_BYTES)?;
    if !options.allow_full_scan && rows.len() > max_scan_rows {
        return Err(QueryError::ScanBoundExceeded {
            estimated_rows: rows.len() as u64,
            max_scan_rows: max_scan_rows as u64,
        });
    }

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
        let join_table = snapshot.table(&jp, &js, &jt);
        let join_qualified_cols: Vec<String> = join_schema
            .columns
            .iter()
            .map(|c| format!("{join_alias}.{}", c.name))
            .collect();
        let join_pushdown = alias_pushdown_expr(query.predicate.as_ref(), &join_alias, &derived);
        let join_filter = compile_pushdown(join_pushdown.as_ref(), &join_qualified_cols)?;
        let join_col_offset = columns.len();
        let mut next_columns = columns.clone();
        next_columns.extend(
            join_schema
                .columns
                .iter()
                .map(|c| format!("{join_alias}.{}", c.name)),
        );
        let mut next_column_types = column_types.clone();
        next_column_types.extend(join_schema.columns.iter().map(|c| c.col_type.clone()));
        // A single-column equality ON can use the optimized single-column path
        // (including the PK-probe fast path); composite/non-equi/boolean ON falls
        // to the general executor.
        let simple_equi = join
            .on
            .as_ref()
            .and_then(|on| simple_equi_columns(on, &columns, &join_qualified_cols));
        let effective_left = simple_equi
            .as_ref()
            .map(|(l, _)| l.clone())
            .or_else(|| join.left_column.clone());
        let effective_right = simple_equi
            .as_ref()
            .map(|(_, r)| r.clone())
            .or_else(|| join.right_column.clone());

        let mut joined = Vec::new();
        if let (Some(on), true) = (&join.on, simple_equi.is_none()) {
            // General join condition: composite and/or non-equi predicate over
            // columns from both sides. Right-side WHERE pushdown still applies.
            let join_rows = materialize_table_rows(snapshot, join_table, join_filter.as_ref())?;
            let estimated_join_rows = rows.len().saturating_mul(join_rows.len().max(1));
            if !options.allow_full_scan && estimated_join_rows > max_scan_rows {
                return Err(QueryError::ScanBoundExceeded {
                    estimated_rows: estimated_join_rows as u64,
                    max_scan_rows: max_scan_rows as u64,
                });
            }
            execute_on_join(OnJoinRequest {
                join_type: join.join_type,
                left_rows: &rows,
                right_rows: &join_rows,
                combined_columns: &next_columns,
                left_col_count: columns.len(),
                right_col_count: join_schema.columns.len(),
                on,
                out: &mut joined,
            })?;
        } else {
            let (left_idx, right_idx) = match join.join_type {
                JoinType::Cross => (None, None),
                _ => {
                    let left = effective_left
                        .as_ref()
                        .ok_or_else(|| QueryError::InvalidQuery {
                            reason: "join requires left_column".into(),
                        })?;
                    let right =
                        effective_right
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
                        .position(|c| {
                            format!("{join_alias}.{}", c.name) == *right || c.name == *right
                        })
                        .ok_or_else(|| QueryError::ColumnNotFound {
                            table: join.table.clone(),
                            column: right.clone(),
                        })?;
                    (Some(left_idx), Some(right_idx))
                }
            };
            // The PK-probe fast path re-encodes the left value with `EncodedKey`
            // and looks it up in the right table's primary-key map. `EncodedKey`
            // is type-tagged (Integer != U64 even for equal numbers), so this is
            // only correct when the two columns share an exact type. On a type
            // mismatch we fall through to the hash-join path, whose `join_key`
            // unifies numerically-equal values across integer/float types.
            let join_key_types_match = match (left_idx, right_idx) {
                (Some(li), Some(ri)) => {
                    column_types.get(li) == join_schema.columns.get(ri).map(|c| &c.col_type)
                }
                _ => false,
            };
            let can_probe_right_primary_key =
                matches!(join.join_type, JoinType::Inner | JoinType::Left)
                    && join_key_types_match
                    && right_idx
                        .map(|idx| is_single_column_primary_key_join(join_schema, idx))
                        .unwrap_or(false);
            let estimated_join_rows = if matches!(join.join_type, JoinType::Cross) {
                rows.len()
                    .saturating_mul(join_table.map(|table| table.rows.len()).unwrap_or(0).max(1))
            } else if can_probe_right_primary_key {
                rows.len()
            } else {
                rows.len()
                    .saturating_mul(join_table.map(|table| table.rows.len()).unwrap_or(0).max(1))
            };
            if !options.allow_full_scan && estimated_join_rows > max_scan_rows {
                return Err(QueryError::ScanBoundExceeded {
                    estimated_rows: estimated_join_rows as u64,
                    max_scan_rows: max_scan_rows as u64,
                });
            }

            match join.join_type {
                JoinType::Cross => {
                    let join_rows =
                        materialize_table_rows(snapshot, join_table, join_filter.as_ref())?;
                    for left in &rows {
                        for right in &join_rows {
                            push_joined_row(&mut joined, left, right);
                        }
                    }
                }
                JoinType::Inner | JoinType::Left => {
                    let right_idx = right_idx.ok_or_else(|| QueryError::InvalidQuery {
                        reason: "join requires right join key".into(),
                    })?;
                    let left_idx = left_idx.ok_or_else(|| QueryError::InvalidQuery {
                        reason: "join requires left join key".into(),
                    })?;
                    if can_probe_right_primary_key {
                        for left in &rows {
                            let matched_stored = join_table.and_then(|table| {
                                let encoded = EncodedKey::from_values(std::slice::from_ref(
                                    &left.values[left_idx],
                                ));
                                table.rows.get(&encoded)
                            });
                            let matched = match matched_stored {
                                Some(stored) => {
                                    let row = snapshot.materialize_row(stored)?.into_owned();
                                    // Apply the pushed-down right-table filter to the
                                    // probed row; a row that fails it counts as no match.
                                    match &join_filter {
                                        Some(filter)
                                            if !eval_compiled_expr_public(filter, &row) =>
                                        {
                                            None
                                        }
                                        _ => Some(row),
                                    }
                                }
                                None => None,
                            };
                            if let Some(right) = &matched {
                                push_joined_row(&mut joined, left, right);
                            } else if matches!(join.join_type, JoinType::Left) {
                                push_left_with_nulls(&mut joined, left, join_schema.columns.len());
                            }
                        }
                    } else {
                        let join_rows =
                            materialize_table_rows(snapshot, join_table, join_filter.as_ref())?;
                        // Hash join for non-PK equality predicates.
                        let mut right_map: HashMap<JoinKey, Vec<&Row>> = HashMap::new();
                        for right in &join_rows {
                            // SQL three-valued logic: NULL/NaN never equal anything,
                            // so excluding them from the probe map makes such-keyed
                            // left rows fall through to the outer-join null-extension
                            // branch instead of spuriously matching another such row.
                            if is_unjoinable_hash_key(&right.values[right_idx]) {
                                continue;
                            }
                            right_map
                                .entry(join_key(&right.values[right_idx]))
                                .or_default()
                                .push(right);
                        }
                        for left in &rows {
                            if let Some(matches) = right_map.get(&join_key(&left.values[left_idx]))
                            {
                                for right in matches {
                                    push_joined_row(&mut joined, left, right);
                                }
                            } else if matches!(join.join_type, JoinType::Left) {
                                push_left_with_nulls(&mut joined, left, join_schema.columns.len());
                            }
                        }
                    }
                }
                JoinType::Right => {
                    let left_idx = left_idx.ok_or_else(|| QueryError::InvalidQuery {
                        reason: "join requires left join key".into(),
                    })?;
                    let right_idx = right_idx.ok_or_else(|| QueryError::InvalidQuery {
                        reason: "join requires right join key".into(),
                    })?;
                    let join_rows =
                        materialize_table_rows(snapshot, join_table, join_filter.as_ref())?;
                    let mut left_map: HashMap<JoinKey, Vec<&Row>> = HashMap::new();
                    for left in &rows {
                        // NULL/NaN never equal anything (see the inner/left hash-join
                        // note): keep them out of the probe map so such-keyed right
                        // rows fall through to right-join null-extension.
                        if is_unjoinable_hash_key(&left.values[left_idx]) {
                            continue;
                        }
                        left_map
                            .entry(join_key(&left.values[left_idx]))
                            .or_default()
                            .push(left);
                    }
                    for right in &join_rows {
                        if let Some(matches) = left_map.get(&join_key(&right.values[right_idx])) {
                            for left in matches {
                                push_joined_row(&mut joined, left, right);
                            }
                        } else {
                            push_nulls_with_right(&mut joined, join_col_offset, right);
                        }
                    }
                }
            }
        }
        rows = joined;
        enforce_materialization_budget(&rows, MAX_JOIN_MATERIALIZED_BYTES)?;
        if !options.allow_full_scan && rows.len() > max_scan_rows {
            return Err(QueryError::ScanBoundExceeded {
                estimated_rows: rows.len() as u64,
                max_scan_rows: max_scan_rows as u64,
            });
        }
        columns = next_columns;
        column_types = next_column_types;
    }

    if let Some(predicate) = &query.predicate {
        let compiled = compile_expr(predicate, &columns, "join")?;
        rows.retain(|r| crate::query::operators::eval_compiled_expr_public(&compiled, r));
    }

    if !query.aggregates.is_empty() {
        let group_by_idx = query
            .group_by
            .iter()
            .map(|name| {
                columns
                    .iter()
                    .position(|c| c == name)
                    .ok_or_else(|| QueryError::ColumnNotFound {
                        table: "join".into(),
                        column: name.clone(),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let agg_col_idx = query
            .aggregates
            .iter()
            .map(|agg| aggregate_col_idx(agg, &columns))
            .collect::<Result<Vec<_>, _>>()?;

        let mut aggregate = AggregateOperator::new(
            Box::new(ScanOperator::new(rows)),
            query.aggregates.clone(),
            group_by_idx,
            agg_col_idx,
        );
        let mut aggregated_rows = Vec::new();
        while let Some(row) = aggregate.next() {
            aggregated_rows.push(row);
        }
        rows = aggregated_rows;
        columns = query.group_by.clone();
        columns.extend(query.aggregates.iter().map(aggregate_output_name));
    }

    if let Some(having) = &query.having {
        if query.aggregates.is_empty() {
            return Err(QueryError::InvalidQuery {
                reason: "having requires aggregate or group_by".into(),
            });
        }
        let compiled = compile_expr(having, &columns, "join")?;
        rows.retain(|r| crate::query::operators::eval_compiled_expr_public(&compiled, r));
    }

    let rows_examined = rows.len();
    let page_window = compute_page_window(&query, &cursor_state, max_scan_rows)?;
    // When the result is ordered and capped (no cursor, no DISTINCT — both need
    // every row), only the first `offset + page + lookahead` rows are ever
    // consumed, so a bounded partial sort (top-k) avoids fully sorting the set.
    let topk_bound = if cursor_state.is_none()
        && !query.distinct
        && query.limit.is_some()
        && !query.order_by.is_empty()
    {
        Some(
            page_window
                .row_offset_count
                .saturating_add(page_window.page_read_limit),
        )
    } else {
        None
    };
    order_join_rows(&mut rows, &columns, &query, topk_bound)?;

    if query.distinct {
        // Project, then deduplicate the output rows before applying offset/limit.
        // Cursor pagination with DISTINCT is rejected before execution.
        let projected = project_join_rows(rows, &columns, &query)?;
        let mut seen: std::collections::HashSet<Vec<Value>> = std::collections::HashSet::new();
        let mut distinct_rows: Vec<Row> = Vec::new();
        for row in projected {
            if seen.insert(row.values.clone()) {
                distinct_rows.push(row);
            }
        }
        let mut sliced: Vec<Row> = distinct_rows
            .into_iter()
            .skip(page_window.row_offset_count)
            .collect();
        let has_more = sliced.len() > page_window.effective_page_size;
        if has_more {
            sliced.truncate(page_window.effective_page_size);
        }
        let split_budget = query.limit.unwrap_or(max_scan_rows);
        let split_recommended = compute_split_recommended(rows_examined, split_budget);
        return Ok(QueryResult {
            rows_examined,
            rows: sliced,
            truncated: has_more,
            cursor: None,
            snapshot_seq,
            materialized_seq: None,
            split_recommended,
        });
    }

    let sort_indices: Vec<(usize, crate::query::plan::Order)> = if !query.order_by.is_empty() {
        query
            .order_by
            .iter()
            .filter_map(|(name, ord)| columns.iter().position(|c| c == name).map(|i| (i, *ord)))
            .collect()
    } else {
        Vec::new()
    };
    let pk_indices: Vec<usize> = (0..columns.len()).collect();
    let mut sliced = Vec::new();
    let mut skipped = 0usize;
    for row in rows {
        if let Some(cursor) = &cursor_state
            && !row_after_cursor(&row, cursor, &sort_indices, &pk_indices)
        {
            continue;
        }
        if skipped < page_window.row_offset_count {
            skipped += 1;
            continue;
        }
        sliced.push(row);
        if sliced.len() > page_window.effective_page_size {
            break;
        }
    }
    let has_more = sliced.len() > page_window.effective_page_size;
    if has_more {
        sliced.truncate(page_window.effective_page_size);
    }
    let cursor_last_row = sliced.last().cloned();

    sliced = project_join_rows(sliced, &columns, &query)?;

    let returned_rows = sliced.len();
    let remaining_limit_after_page = compute_remaining_limit_after_page(
        query.limit,
        cursor_state.as_ref().and_then(|c| c.remaining_limit),
        returned_rows,
    );
    let cursor = if has_more && page_window.row_offset_count == 0 {
        let last_row = cursor_last_row.ok_or_else(|| QueryError::InvalidQuery {
            reason: "invalid cursor state".into(),
        })?;
        Some(encode_cursor(
            &CursorToken {
                snapshot_seq,
                last_sort_key: extract_sort_key(&last_row, &sort_indices),
                last_pk: extract_pk_key(&last_row, &pk_indices),
                page_size: page_window.page_size,
                remaining_limit: remaining_limit_after_page,
            },
            cursor_signing_key,
        )?)
    } else {
        None
    };
    let split_budget = query.limit.unwrap_or(max_scan_rows);
    let split_recommended = compute_split_recommended(rows_examined, split_budget);
    Ok(QueryResult {
        rows_examined,
        rows: sliced,
        truncated: has_more,
        cursor,
        snapshot_seq,
        materialized_seq: None,
        split_recommended,
    })
}

pub(super) fn resolve_table_ref(
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

fn is_single_column_primary_key_join(
    join_schema: &crate::catalog::schema::TableSchema,
    right_idx: usize,
) -> bool {
    if join_schema.primary_key.len() != 1 {
        return false;
    }
    join_schema
        .columns
        .get(right_idx)
        .map(|column| column.name == join_schema.primary_key[0])
        .unwrap_or(false)
}

/// Flatten the top-level `AND` chain of `expr` into its individual conjuncts.
/// `Or`/`Not`/leaf nodes are returned whole — only `And` is split — so each
/// conjunct can be considered for pushdown independently.
fn collect_conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::And(a, b) => {
            collect_conjuncts(a, out);
            collect_conjuncts(b, out);
        }
        other => out.push(other),
    }
}

/// Collect every column name referenced anywhere in `expr`.
fn collect_expr_columns<'a>(expr: &'a Expr, out: &mut Vec<&'a str>) {
    match expr {
        Expr::Eq(c, _)
        | Expr::Ne(c, _)
        | Expr::Lt(c, _)
        | Expr::Lte(c, _)
        | Expr::Gt(c, _)
        | Expr::Gte(c, _)
        | Expr::In(c, _)
        | Expr::Between(c, _, _)
        | Expr::IsNull(c)
        | Expr::IsNotNull(c)
        | Expr::Like(c, _) => out.push(c),
        Expr::And(a, b) | Expr::Or(a, b) => {
            collect_expr_columns(a, out);
            collect_expr_columns(b, out);
        }
        Expr::Not(inner) => collect_expr_columns(inner, out),
    }
}

/// A conjunct is *null-rejecting* when it cannot evaluate to TRUE if its
/// referenced columns are all NULL. Such a conjunct is safe to apply to a
/// single table *before* a join — including the null-extended side of an outer
/// join — because any row it removes would also be rejected by the unchanged
/// post-join filter, whether that row matched or was null-extended. `IsNull`
/// and `Not` are conservatively treated as not null-rejecting.
fn is_null_rejecting(expr: &Expr) -> bool {
    match expr {
        Expr::Eq(..)
        | Expr::Ne(..)
        | Expr::Lt(..)
        | Expr::Lte(..)
        | Expr::Gt(..)
        | Expr::Gte(..)
        | Expr::In(..)
        | Expr::Between(..)
        | Expr::Like(..)
        | Expr::IsNotNull(_) => true,
        Expr::And(a, b) => is_null_rejecting(a) || is_null_rejecting(b),
        Expr::Or(a, b) => is_null_rejecting(a) && is_null_rejecting(b),
        Expr::IsNull(_) | Expr::Not(_) => false,
    }
}

/// Build the conjunction of `where` conjuncts that reference only `alias`'s
/// columns and are null-rejecting, so the result can be pushed down as a
/// pre-join filter on that table without changing query semantics. Columns in a
/// join predicate are always alias-qualified (`alias.column`), so membership is
/// decided by the `"{alias}."` prefix. Returns `None` when nothing is pushable.
fn pushdown_predicate_for_alias(predicate: &Expr, alias: &str) -> Option<Expr> {
    let prefix = format!("{alias}.");
    let mut conjuncts = Vec::new();
    collect_conjuncts(predicate, &mut conjuncts);

    let mut pushable: Option<Expr> = None;
    for conjunct in conjuncts {
        let mut cols = Vec::new();
        collect_expr_columns(conjunct, &mut cols);
        if cols.is_empty() {
            continue;
        }
        if cols.iter().all(|col| col.starts_with(&prefix)) && is_null_rejecting(conjunct) {
            pushable = Some(match pushable {
                Some(acc) => Expr::And(Box::new(acc), Box::new(conjunct.clone())),
                None => conjunct.clone(),
            });
        }
    }
    pushable
}

/// Build the pushdown predicate for `alias`: the pushable WHERE conjuncts plus
/// any transitively-derived `column = constant` facts for this table's columns.
fn alias_pushdown_expr(
    where_pred: Option<&Expr>,
    alias: &str,
    derived: &HashMap<String, Value>,
) -> Option<Expr> {
    let mut acc = where_pred.and_then(|p| pushdown_predicate_for_alias(p, alias));
    let prefix = format!("{alias}.");
    let mut derived_cols: Vec<(&String, &Value)> = derived
        .iter()
        .filter(|(col, _)| col.starts_with(&prefix))
        .collect();
    // Deterministic order so the compiled-expression cache key is stable.
    derived_cols.sort_by(|a, b| a.0.cmp(b.0));
    for (col, val) in derived_cols {
        let eq = Expr::Eq(col.clone(), val.clone());
        acc = Some(match acc {
            Some(existing) => existing.and(eq),
            None => eq,
        });
    }
    acc
}

/// Compile an optional pushdown predicate against `columns` (the alias-qualified
/// column list for a single table).
fn compile_pushdown(
    pushdown: Option<&Expr>,
    columns: &[String],
) -> Result<Option<CompiledExpr>, QueryError> {
    pushdown
        .map(|expr| compile_expr(expr, columns, "join"))
        .transpose()
}

fn collect_joincond_eq_edges(on: &JoinCond, edges: &mut Vec<(String, String)>) {
    match on {
        JoinCond::Compare {
            left,
            op: CompareOp::Eq,
            right,
        } => edges.push((left.clone(), right.clone())),
        JoinCond::And(a, b) => {
            collect_joincond_eq_edges(a, edges);
            collect_joincond_eq_edges(b, edges);
        }
        // Only conjunctive equalities are safe to treat as always-true edges.
        _ => {}
    }
}

fn uf_intern(name: &str, index: &mut HashMap<String, usize>, parent: &mut Vec<usize>) -> usize {
    if let Some(i) = index.get(name) {
        *i
    } else {
        let i = parent.len();
        parent.push(i);
        index.insert(name.to_string(), i);
        i
    }
}

fn uf_find(parent: &mut [usize], mut x: usize) -> usize {
    while parent[x] != x {
        parent[x] = parent[parent[x]];
        x = parent[x];
    }
    x
}

/// Derive implied `column = constant` facts by propagating WHERE equality
/// constants across join equi-keys (predicate transitivity). E.g. `WHERE u.id =
/// 5` with join `u.id = p.user_id` yields `p.user_id = 5`. Only top-level AND
/// equalities (in both WHERE and ON) participate, so each fact holds for every
/// output row — making it safe to push into each table's pre-join
/// materialization for all join types. Returns a map of alias-qualified column
/// to constant.
fn compute_transitive_constants(
    query: &Query,
    project_id: &str,
    scope_id: &str,
) -> HashMap<String, Value> {
    let Some(predicate) = query.predicate.as_ref() else {
        return HashMap::new();
    };
    let mut edges: Vec<(String, String)> = Vec::new();
    for join in &query.joins {
        match &join.on {
            Some(on) => collect_joincond_eq_edges(on, &mut edges),
            None => {
                if let (Some(left), Some(right)) = (&join.left_column, &join.right_column) {
                    let join_alias = join
                        .alias
                        .clone()
                        .unwrap_or_else(|| resolve_table_ref(project_id, scope_id, &join.table).2);
                    let right_qualified = if right.contains('.') {
                        right.clone()
                    } else {
                        format!("{join_alias}.{right}")
                    };
                    edges.push((left.clone(), right_qualified));
                }
            }
        }
    }
    if edges.is_empty() {
        return HashMap::new();
    }

    let mut index: HashMap<String, usize> = HashMap::new();
    let mut parent: Vec<usize> = Vec::new();
    for (a, b) in &edges {
        let ia = uf_intern(a, &mut index, &mut parent);
        let ib = uf_intern(b, &mut index, &mut parent);
        let ra = uf_find(&mut parent, ia);
        let rb = uf_find(&mut parent, ib);
        if ra != rb {
            parent[ra] = rb;
        }
    }

    // Seed components from WHERE top-level equalities on edge columns.
    let mut conjuncts = Vec::new();
    collect_conjuncts(predicate, &mut conjuncts);
    let mut root_const: HashMap<usize, Option<Value>> = HashMap::new();
    for conjunct in conjuncts {
        if let Expr::Eq(col, val) = conjunct
            && let Some(i) = index.get(col).copied()
        {
            let root = uf_find(&mut parent, i);
            root_const
                .entry(root)
                .and_modify(|slot| {
                    if let Some(existing) = slot
                        && existing != val
                    {
                        // Conflicting constants in one component: derive nothing
                        // (the original WHERE conjuncts still enforce correctness).
                        *slot = None;
                    }
                })
                .or_insert_with(|| Some(val.clone()));
        }
    }

    // Emit a fact for every edge column in a consistently-seeded component.
    let mut derived = HashMap::new();
    for (name, i) in index.into_iter() {
        let root = uf_find(&mut parent, i);
        if let Some(Some(val)) = root_const.get(&root) {
            derived.insert(name, val.clone());
        }
    }
    derived
}

/// Return a clone of `expr` with every `"{alias}."`-qualified column name
/// reduced to its bare column name, so it can be matched against a single
/// table's schema (for index selection). Assumes every column is qualified with
/// `alias` — true for the output of [`pushdown_predicate_for_alias`].
fn strip_alias_prefix(expr: &Expr, alias: &str) -> Expr {
    let prefix = format!("{alias}.");
    let strip = |c: &str| -> String { c.strip_prefix(&prefix).unwrap_or(c).to_string() };
    match expr {
        Expr::Eq(c, v) => Expr::Eq(strip(c), v.clone()),
        Expr::Ne(c, v) => Expr::Ne(strip(c), v.clone()),
        Expr::Lt(c, v) => Expr::Lt(strip(c), v.clone()),
        Expr::Lte(c, v) => Expr::Lte(strip(c), v.clone()),
        Expr::Gt(c, v) => Expr::Gt(strip(c), v.clone()),
        Expr::Gte(c, v) => Expr::Gte(strip(c), v.clone()),
        Expr::In(c, vs) => Expr::In(strip(c), vs.clone()),
        Expr::Between(c, lo, hi) => Expr::Between(strip(c), lo.clone(), hi.clone()),
        Expr::IsNull(c) => Expr::IsNull(strip(c)),
        Expr::IsNotNull(c) => Expr::IsNotNull(strip(c)),
        Expr::Like(c, p) => Expr::Like(strip(c), p.clone()),
        Expr::And(a, b) => Expr::And(
            Box::new(strip_alias_prefix(a, alias)),
            Box::new(strip_alias_prefix(b, alias)),
        ),
        Expr::Or(a, b) => Expr::Or(
            Box::new(strip_alias_prefix(a, alias)),
            Box::new(strip_alias_prefix(b, alias)),
        ),
        Expr::Not(inner) => Expr::Not(Box::new(strip_alias_prefix(inner, alias))),
    }
}

pub(super) struct IndexedBaseRowsRequest<'a> {
    pub snapshot: &'a KeyspaceSnapshot,
    pub catalog: &'a Catalog,
    pub schema: &'a crate::catalog::schema::TableSchema,
    pub project_id: &'a str,
    pub scope_id: &'a str,
    pub table_name: &'a str,
    pub base_alias: &'a str,
    pub pushdown: Option<&'a Expr>,
    pub residual_filter: Option<&'a CompiledExpr>,
}

/// Materialize the base table's rows from a primary-key point lookup or index
/// lookup over the pushed-down base predicate, paging in only the matching
/// primary keys. Returns `None` when there is no pushable base predicate or no
/// index covers it, so the caller falls back to a full scan. When the index
/// lookup is not predicate-exact, the compiled `residual_filter` is applied to
/// each candidate row.
fn indexed_base_rows(req: IndexedBaseRowsRequest<'_>) -> Result<Option<Vec<Row>>, QueryError> {
    let Some(pushdown) = req.pushdown else {
        return Ok(None);
    };
    let Some(table) = req
        .snapshot
        .table(req.project_id, req.scope_id, req.table_name)
    else {
        // No data for this table in the snapshot: an empty base needs no scan.
        return Ok(Some(Vec::new()));
    };
    // Index selection matches against bare schema column names.
    let bare = strip_alias_prefix(pushdown, req.base_alias);

    // Primary-key point lookup: a full PK equality resolves to at most one row.
    if let Some(pk_values) =
        super::predicate::extract_primary_key_values(&bare, &req.schema.primary_key)
    {
        let encoded = EncodedKey::from_values(&pk_values);
        let mut rows = Vec::new();
        if let Some(row) = req.snapshot.get_row_by_encoded(
            req.project_id,
            req.scope_id,
            req.table_name,
            &encoded,
        )? {
            let row = row.into_owned();
            // The PK equality is fully satisfied by the lookup; apply the residual
            // filter only for any extra pushed-down conjuncts.
            if req
                .residual_filter
                .is_none_or(|filter| eval_compiled_expr_public(filter, &row))
            {
                rows.push(row);
            }
        }
        return Ok(Some(rows));
    }

    let Some(indexed) = super::indexing::indexed_pks_for_predicate_limited(
        req.catalog,
        req.project_id,
        req.scope_id,
        req.table_name,
        table,
        req.snapshot.kv_segment_store.as_deref(),
        &bare,
        None,
    )?
    else {
        return Ok(None);
    };
    let residual = if indexed.predicate_exact {
        None
    } else {
        req.residual_filter
    };
    let mut rows = Vec::with_capacity(indexed.pks.len());
    for pk in &indexed.pks {
        if let Some(row) =
            req.snapshot
                .get_row_by_encoded(req.project_id, req.scope_id, req.table_name, pk)?
        {
            let row = row.into_owned();
            if let Some(filter) = residual
                && !eval_compiled_expr_public(filter, &row)
            {
                continue;
            }
            rows.push(row);
        }
    }
    Ok(Some(rows))
}

/// Materialize all rows of `table`, skipping any that fail `filter`.
fn materialize_table_rows(
    snapshot: &KeyspaceSnapshot,
    table: Option<&crate::storage::keyspace::TableData>,
    filter: Option<&CompiledExpr>,
) -> Result<Vec<Row>, QueryError> {
    let mut out = Vec::new();
    if let Some(table) = table {
        out.reserve(table.rows.len());
        for stored in table.rows.values() {
            let row = snapshot.materialize_row(stored)?.into_owned();
            if let Some(filter) = filter
                && !eval_compiled_expr_public(filter, &row)
            {
                continue;
            }
            out.push(row);
        }
    }
    Ok(out)
}

/// A [`JoinCond`] compiled against the combined (left ++ right) column layout.
/// Column references resolve to indices into that combined row; indices below
/// `left_col_count` address the left row, the rest the right row.
enum CompiledJoinCond {
    Compare {
        left_idx: usize,
        op: CompareOp,
        right_idx: usize,
    },
    And(Box<CompiledJoinCond>, Box<CompiledJoinCond>),
    Or(Box<CompiledJoinCond>, Box<CompiledJoinCond>),
    Not(Box<CompiledJoinCond>),
}

fn compile_join_cond(cond: &JoinCond, columns: &[String]) -> Result<CompiledJoinCond, QueryError> {
    match cond {
        JoinCond::Compare { left, op, right } => {
            let resolve = |name: &str| {
                columns
                    .iter()
                    .position(|c| c == name)
                    .ok_or_else(|| QueryError::ColumnNotFound {
                        table: "join".into(),
                        column: name.to_string(),
                    })
            };
            Ok(CompiledJoinCond::Compare {
                left_idx: resolve(left)?,
                op: *op,
                right_idx: resolve(right)?,
            })
        }
        JoinCond::And(a, b) => Ok(CompiledJoinCond::And(
            Box::new(compile_join_cond(a, columns)?),
            Box::new(compile_join_cond(b, columns)?),
        )),
        JoinCond::Or(a, b) => Ok(CompiledJoinCond::Or(
            Box::new(compile_join_cond(a, columns)?),
            Box::new(compile_join_cond(b, columns)?),
        )),
        JoinCond::Not(inner) => Ok(CompiledJoinCond::Not(Box::new(compile_join_cond(
            inner, columns,
        )?))),
    }
}

#[inline]
fn combined_value<'a>(left: &'a Row, right: &'a Row, left_len: usize, idx: usize) -> &'a Value {
    if idx < left_len {
        &left.values[idx]
    } else {
        &right.values[idx - left_len]
    }
}

/// Canonical hash-join key part. Integer-family values
/// (`U8`/`U64`/`Integer`/`Timestamp`) collapse to a single `i128`
/// representation so cross-type equi joins match consistently with
/// [`value_compare`]. Other value types key by themselves. (Float-to-integer
/// equality is unified for comparisons but not in the hash-join key.)
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
enum JoinKey {
    Int(i128),
    Other(Value),
}

/// Values that can never satisfy a hash-join equality, mirroring SQL three-valued
/// logic: NULL never equals anything, and neither does NaN (`value_compare`
/// returns `None` for both). The single-column hash join trusts `join_key`
/// equality directly — unlike the composite-ON path, it has no post-match
/// `value_compare` re-check — so these keys must be excluded from the probe map.
/// They then fall through to the outer-join null-extension branch instead of
/// spuriously matching another NULL/NaN-keyed row.
fn is_unjoinable_hash_key(value: &Value) -> bool {
    matches!(value, Value::Null) || matches!(value, Value::Float(f) if f.is_nan())
}

fn join_key(value: &Value) -> JoinKey {
    // 2^53: above this an f64 can no longer represent every integer, so the
    // Float<->Integer mapping stops being bijective. We only fold Floats into
    // the integer bucket within this range, where it is provably consistent
    // with `value_compare`'s numeric equality. Larger floats stay `Other`.
    const F64_EXACT_INT_LIMIT: f64 = 9_007_199_254_740_992.0;
    match value {
        Value::U8(x) => JoinKey::Int(*x as i128),
        Value::U64(x) => JoinKey::Int(*x as i128),
        Value::Integer(x) => JoinKey::Int(*x as i128),
        Value::Timestamp(x) => JoinKey::Int(*x as i128),
        Value::Float(x) if x.fract() == 0.0 && x.abs() < F64_EXACT_INT_LIMIT => {
            JoinKey::Int(*x as i128)
        }
        // Wide integers fold into the shared integer bucket when they fit, so a
        // U256/I256 column hash-joins against an Integer/U64 column the same way
        // `value_compare` matches them. Out-of-range values stay `Other` (they
        // can only equal another identical wide value, handled by byte equality).
        Value::U256(b) => crate::query::operators::u256_to_i128(b)
            .map(JoinKey::Int)
            .unwrap_or_else(|| JoinKey::Other(value.clone())),
        Value::I256(b) => crate::query::operators::i256_to_i128(b)
            .map(JoinKey::Int)
            .unwrap_or_else(|| JoinKey::Other(value.clone())),
        other => JoinKey::Other(other.clone()),
    }
}

/// Compare two values for a join condition using the same cross-type coercion
/// as `WHERE` filters ([`value_compare`]). NULL never satisfies a comparison
/// (SQL three-valued logic collapses unknown to non-match here).
fn join_compare(a: &Value, op: CompareOp, b: &Value) -> bool {
    let Some(ord) = value_compare(a, b) else {
        return false;
    };
    match op {
        CompareOp::Eq => ord.is_eq(),
        CompareOp::Ne => !ord.is_eq(),
        CompareOp::Lt => ord.is_lt(),
        CompareOp::Lte => ord.is_le(),
        CompareOp::Gt => ord.is_gt(),
        CompareOp::Gte => ord.is_ge(),
    }
}

fn eval_join_cond(cond: &CompiledJoinCond, left: &Row, right: &Row, left_len: usize) -> bool {
    match cond {
        CompiledJoinCond::Compare {
            left_idx,
            op,
            right_idx,
        } => join_compare(
            combined_value(left, right, left_len, *left_idx),
            *op,
            combined_value(left, right, left_len, *right_idx),
        ),
        CompiledJoinCond::And(a, b) => {
            eval_join_cond(a, left, right, left_len) && eval_join_cond(b, left, right, left_len)
        }
        CompiledJoinCond::Or(a, b) => {
            eval_join_cond(a, left, right, left_len) || eval_join_cond(b, left, right, left_len)
        }
        CompiledJoinCond::Not(inner) => !eval_join_cond(inner, left, right, left_len),
    }
}

fn collect_compiled_conjuncts<'a>(cond: &'a CompiledJoinCond, out: &mut Vec<&'a CompiledJoinCond>) {
    match cond {
        CompiledJoinCond::And(a, b) => {
            collect_compiled_conjuncts(a, out);
            collect_compiled_conjuncts(b, out);
        }
        other => out.push(other),
    }
}

/// Extract cross-side equality conjuncts usable as hash-join keys, returned as
/// `(left_combined_idx, right_combined_idx)` pairs. Same-side equalities and
/// non-equi comparisons are skipped (still enforced via the full condition).
fn extract_equi_keys(cond: &CompiledJoinCond, left_len: usize) -> Vec<(usize, usize)> {
    let mut conjuncts = Vec::new();
    collect_compiled_conjuncts(cond, &mut conjuncts);
    let mut keys = Vec::new();
    for conjunct in conjuncts {
        if let CompiledJoinCond::Compare {
            left_idx,
            op: CompareOp::Eq,
            right_idx,
        } = conjunct
        {
            let (a, b) = (*left_idx, *right_idx);
            if a < left_len && b >= left_len {
                keys.push((a, b));
            } else if a >= left_len && b < left_len {
                keys.push((b, a));
            }
        }
    }
    keys
}

/// If `on` is a single cross-side equality (`left = right`), return the
/// `(left_column, right_column)` names so the join can use the optimized
/// single-column path (including the PK-probe fast path). Composite, non-equi,
/// or boolean conditions return `None` and fall to the general executor.
fn simple_equi_columns(
    on: &JoinCond,
    left_cols: &[String],
    right_cols: &[String],
) -> Option<(String, String)> {
    let JoinCond::Compare {
        left,
        op: CompareOp::Eq,
        right,
    } = on
    else {
        return None;
    };
    let in_left = |c: &str| left_cols.iter().any(|x| x == c);
    let in_right = |c: &str| right_cols.iter().any(|x| x == c);
    if in_left(left) && in_right(right) {
        Some((left.clone(), right.clone()))
    } else if in_right(left) && in_left(right) {
        Some((right.clone(), left.clone()))
    } else {
        None
    }
}

pub(super) struct OnJoinRequest<'a> {
    pub join_type: JoinType,
    pub left_rows: &'a [Row],
    pub right_rows: &'a [Row],
    pub combined_columns: &'a [String],
    pub left_col_count: usize,
    pub right_col_count: usize,
    pub on: &'a JoinCond,
    pub out: &'a mut Vec<Row>,
}

/// Execute a join driven by a general [`JoinCond`]. Uses a hash join keyed on
/// the cross-side equality conjuncts (composite keys supported) when any exist,
/// otherwise a nested-loop join (required for pure non-equi conditions). The
/// full condition is evaluated on every candidate pair, so residual non-equi
/// predicates and outer-join null-extension are handled correctly.
fn execute_on_join(req: OnJoinRequest<'_>) -> Result<(), QueryError> {
    let compiled = compile_join_cond(req.on, req.combined_columns)?;
    let left_len = req.left_col_count;
    let equi = extract_equi_keys(&compiled, left_len);

    // Right join: preserve unmatched right rows, null-extending the left side.
    if matches!(req.join_type, JoinType::Right) {
        for right in req.right_rows {
            let mut matched = false;
            for left in req.left_rows {
                if eval_join_cond(&compiled, left, right, left_len) {
                    push_joined_row(req.out, left, right);
                    matched = true;
                }
            }
            if !matched {
                push_nulls_with_right(req.out, left_len, right);
            }
        }
        return Ok(());
    }

    let preserve_left = matches!(req.join_type, JoinType::Left);

    if equi.is_empty() {
        // Nested-loop join for pure non-equi / cross-product conditions.
        for left in req.left_rows {
            let mut matched = false;
            for right in req.right_rows {
                if eval_join_cond(&compiled, left, right, left_len) {
                    push_joined_row(req.out, left, right);
                    matched = true;
                }
            }
            if !matched && preserve_left {
                push_left_with_nulls(req.out, left, req.right_col_count);
            }
        }
        return Ok(());
    }

    // Hash join on the composite equality key. NULL keys never match (SQL
    // semantics), so they are excluded from the build side and fall through to
    // the left-join null-extension branch.
    let left_key_idx: Vec<usize> = equi.iter().map(|(l, _)| *l).collect();
    let right_key_idx: Vec<usize> = equi.iter().map(|(_, r)| *r - left_len).collect();
    let mut build: HashMap<Vec<JoinKey>, Vec<&Row>> = HashMap::new();
    for right in req.right_rows {
        if right_key_idx
            .iter()
            .any(|i| matches!(right.values[*i], Value::Null))
        {
            continue;
        }
        let key: Vec<JoinKey> = right_key_idx
            .iter()
            .map(|i| join_key(&right.values[*i]))
            .collect();
        build.entry(key).or_default().push(right);
    }
    for left in req.left_rows {
        let mut matched = false;
        let left_null = left_key_idx
            .iter()
            .any(|i| matches!(left.values[*i], Value::Null));
        let key: Vec<JoinKey> = left_key_idx
            .iter()
            .map(|i| join_key(&left.values[*i]))
            .collect();
        if !left_null && let Some(candidates) = build.get(&key) {
            for right in candidates {
                if eval_join_cond(&compiled, left, right, left_len) {
                    push_joined_row(req.out, left, right);
                    matched = true;
                }
            }
        }
        if !matched && preserve_left {
            push_left_with_nulls(req.out, left, req.right_col_count);
        }
    }
    Ok(())
}

fn order_join_rows(
    rows: &mut Vec<Row>,
    columns: &[String],
    query: &Query,
    bound: Option<usize>,
) -> Result<(), QueryError> {
    if query.order_by.is_empty() {
        return Ok(());
    }
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
    // Cross-type-aware ordering, consistent with WHERE/filter comparison; fall
    // back to native ordering for NULLs (value_compare returns None).
    let cmp = |a: &Row, b: &Row| {
        for (idx, ord) in &order_pairs {
            let cmp = value_compare(&a.values[*idx], &b.values[*idx])
                .unwrap_or_else(|| a.values[*idx].cmp(&b.values[*idx]));
            let ord_cmp = match ord {
                crate::query::plan::Order::Asc => cmp,
                crate::query::plan::Order::Desc => cmp.reverse(),
            };
            if !ord_cmp.is_eq() {
                return ord_cmp;
            }
        }
        std::cmp::Ordering::Equal
    };
    // Top-k: partition so the `k` smallest rows come first, drop the rest, then
    // sort only those. O(n + k log k) instead of O(n log n).
    if let Some(k) = bound
        && k >= 1
        && rows.len() > k
    {
        rows.select_nth_unstable_by(k - 1, &cmp);
        rows.truncate(k);
    }
    rows.sort_by(cmp);
    Ok(())
}

fn project_join_rows(
    rows: Vec<Row>,
    columns: &[String],
    query: &Query,
) -> Result<Vec<Row>, QueryError> {
    let compiled_computed = super::computed::compile_computed(&query.computed, columns)?;
    let select_all = query.select.is_empty() || query.select[0] == "*";
    if select_all && compiled_computed.is_empty() {
        return Ok(rows);
    }
    let selected_indices: Vec<usize> = if select_all {
        (0..columns.len()).collect()
    } else {
        query
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
            .collect::<Result<_, _>>()?
    };
    Ok(rows
        .into_iter()
        .map(|row| {
            let base: Vec<Value> = selected_indices
                .iter()
                .map(|idx| row.values[*idx].clone())
                .collect();
            super::computed::append_computed(base, &compiled_computed, &row)
        })
        .collect())
}

fn push_joined_row(out: &mut Vec<Row>, left: &Row, right: &Row) {
    let mut values = Vec::with_capacity(left.values.len().saturating_add(right.values.len()));
    values.extend_from_slice(&left.values);
    values.extend_from_slice(&right.values);
    out.push(Row { values });
}

fn push_left_with_nulls(out: &mut Vec<Row>, left: &Row, null_count: usize) {
    let mut values = Vec::with_capacity(left.values.len().saturating_add(null_count));
    values.extend_from_slice(&left.values);
    values.extend(std::iter::repeat_n(Value::Null, null_count));
    out.push(Row { values });
}

fn push_nulls_with_right(out: &mut Vec<Row>, null_count: usize, right: &Row) {
    let mut values = Vec::with_capacity(null_count.saturating_add(right.values.len()));
    values.extend(std::iter::repeat_n(Value::Null, null_count));
    values.extend_from_slice(&right.values);
    out.push(Row { values });
}

#[cfg(test)]
mod join_key_tests {
    use super::{JoinKey, join_key};
    use crate::catalog::types::Value;
    use primitive_types::U256;

    fn to_be(v: U256) -> [u8; 32] {
        let mut out = [0u8; 32];
        v.to_big_endian(&mut out);
        out
    }

    #[test]
    fn integer_types_share_a_bucket() {
        assert_eq!(join_key(&Value::Integer(5)), join_key(&Value::U64(5)));
        assert_eq!(join_key(&Value::Integer(5)), join_key(&Value::U8(5)));
        assert_eq!(join_key(&Value::Integer(5)), join_key(&Value::Timestamp(5)));
    }

    #[test]
    fn integer_valued_float_matches_integer() {
        // Regression: a hash equi-join on a Float column vs an Integer column
        // must match numerically-equal values, like `value_compare` does.
        assert_eq!(join_key(&Value::Float(5.0)), join_key(&Value::Integer(5)));
        // Non-integer floats stay distinct from integers.
        assert_ne!(join_key(&Value::Float(5.5)), join_key(&Value::Integer(5)));
        // Beyond 2^53 the f64<->int mapping is no longer exact: stay `Other`.
        let big = 9_007_199_254_740_993.0_f64; // 2^53 + 1, not exactly representable
        assert!(matches!(join_key(&Value::Float(big)), JoinKey::Other(_)));
    }

    #[test]
    fn wide_integers_fold_into_shared_bucket_when_in_range() {
        assert_eq!(
            join_key(&Value::U256(to_be(U256::from(7u8)))),
            join_key(&Value::Integer(7))
        );
        // Out-of-range U256 stays Other and only matches identical wide values.
        let huge = Value::U256(to_be(U256::MAX));
        assert!(matches!(join_key(&huge), JoinKey::Other(_)));
        assert_eq!(join_key(&huge), join_key(&Value::U256(to_be(U256::MAX))));
    }
}
