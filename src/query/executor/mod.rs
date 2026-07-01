use crate::catalog::Catalog;
use crate::catalog::namespace_key;
use crate::catalog::types::Row;
use crate::query::error::QueryError;
use crate::query::plan::Query;
use crate::query::plan::QueryOptions;
use crate::query::planner::build_physical_plan;
use crate::storage::encoded_key::{EncodedKey, prefix_successor};
use crate::storage::keyspace::KeyspaceSnapshot;

mod access_path;
mod aggregate;
mod composite_index;
mod computed;
mod cursor;
mod execution_setup;
mod index_diagnostics;
mod index_lookup;
mod index_utils;
mod indexing;
mod join;
mod operator_pipeline;
mod ordered_scan;
mod pagination;
mod point_lookup;
mod predicate;
mod read_set;
mod validate;

pub(crate) use access_path::{AccessPathDiagnostics, explain_access_path_for_query};
use cursor::{CursorToken, encode_cursor, extract_pk_key, extract_sort_key, row_after_cursor};
use execution_setup::prepare_execution_setup;
use indexing::indexed_pks_for_predicate_limited;
use operator_pipeline::{OperatorPipelineRequest, build_operator_pipeline};
use ordered_scan::{
    OrderedIndexScanRequest, OrderedPredicateIndexScanRequest, ordered_index_scan_for_query,
    ordered_predicate_index_scan_for_query,
};
use pagination::{
    compute_page_window, compute_remaining_limit_after_page, compute_split_recommended,
};
use point_lookup::{PrimaryKeyPointQueryRequest, try_primary_key_point_query};
pub(crate) use predicate::extract_primary_key_prefix;
pub use read_set::ReadSetCollector;
use validate::validate_query;

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub rows: Vec<Row>,
    pub rows_examined: usize,
    pub cursor: Option<String>,
    pub truncated: bool,
    pub snapshot_seq: u64,
    pub materialized_seq: Option<u64>,
    /// `true` when this page consumed at least 75% of the effective scan
    /// budget (the smaller of the caller's `limit` and the configured
    /// `max_scan_rows`). Clients that observe this flag should issue another
    /// paginated request rather than re-running the same query without a
    /// cursor — useful for soft-fanout and rate-limiting decisions.
    pub split_recommended: bool,
}

/// Compute the `[start, end)` key bounds for a primary-key prefix range scan.
/// The band covers every row whose encoded primary key begins with the encoded
/// `prefix` values. When the encoded prefix has no lexicographic successor (all
/// trailing `0xFF` bytes), the upper bound is left unbounded.
pub(crate) fn pk_prefix_scan_bounds(
    prefix: &[crate::catalog::types::Value],
) -> (std::ops::Bound<EncodedKey>, std::ops::Bound<EncodedKey>) {
    let start_key = EncodedKey::from_values(prefix);
    let end = match prefix_successor(&start_key) {
        Some(successor) => std::ops::Bound::Excluded(successor),
        None => std::ops::Bound::Unbounded,
    };
    (std::ops::Bound::Included(start_key), end)
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
        None,
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
    cursor_signing_key: Option<&[u8; 32]>,
) -> Result<QueryResult, QueryError> {
    execute_query_with_options_capturing_signed(SignedQueryExecutionRequest {
        snapshot,
        catalog,
        project_id,
        scope_id,
        query,
        options,
        snapshot_seq,
        max_scan_rows,
        read_set: None,
        cursor_signing_key,
    })
}

pub struct CapturingQueryExecutionRequest<'a, 'r> {
    pub snapshot: &'a KeyspaceSnapshot,
    pub catalog: &'a Catalog,
    pub project_id: &'a str,
    pub scope_id: &'a str,
    pub query: Query,
    pub options: &'a QueryOptions,
    pub snapshot_seq: u64,
    pub max_scan_rows: usize,
    pub read_set: Option<&'r mut ReadSetCollector>,
}

pub fn execute_query_with_options_capturing(
    request: CapturingQueryExecutionRequest<'_, '_>,
) -> Result<QueryResult, QueryError> {
    execute_query_with_options_capturing_signed(SignedQueryExecutionRequest {
        snapshot: request.snapshot,
        catalog: request.catalog,
        project_id: request.project_id,
        scope_id: request.scope_id,
        query: request.query,
        options: request.options,
        snapshot_seq: request.snapshot_seq,
        max_scan_rows: request.max_scan_rows,
        read_set: request.read_set,
        cursor_signing_key: None,
    })
}

struct SignedQueryExecutionRequest<'a, 'r> {
    snapshot: &'a KeyspaceSnapshot,
    catalog: &'a Catalog,
    project_id: &'a str,
    scope_id: &'a str,
    query: Query,
    options: &'a QueryOptions,
    snapshot_seq: u64,
    max_scan_rows: usize,
    read_set: Option<&'r mut ReadSetCollector>,
    cursor_signing_key: Option<&'a [u8; 32]>,
}

fn execute_query_with_options_capturing_signed(
    request: SignedQueryExecutionRequest<'_, '_>,
) -> Result<QueryResult, QueryError> {
    let SignedQueryExecutionRequest {
        snapshot,
        catalog,
        project_id,
        scope_id,
        query,
        options,
        snapshot_seq,
        max_scan_rows,
        mut read_set,
        cursor_signing_key,
    } = request;

    let execution_setup =
        prepare_execution_setup(&query, options, snapshot_seq, cursor_signing_key)?;
    let options = execution_setup.options;
    let cursor_state = execution_setup.cursor_state;

    if query.distinct && cursor_state.is_some() {
        return Err(QueryError::InvalidQuery {
            reason: "DISTINCT cannot be combined with cursor pagination".into(),
        });
    }
    computed::validate_computed(&query)?;

    if !query.joins.is_empty() {
        // Join paths fall back to coarse table-range capture: record each
        // touched table as a full structural-version-bounded range so the
        // reactive layer stays correct without per-row pk capture.
        if let Some(collector) = read_set.as_deref_mut() {
            collector.record_full_table_scan(snapshot, project_id, scope_id, &query.table);
            for join in &query.joins {
                let (jp, js, jt) = join::resolve_table_ref(project_id, scope_id, &join.table);
                collector.record_full_table_scan(snapshot, &jp, &js, &jt);
            }
        }
        return join::execute_join_query(join::JoinQueryExecutionRequest {
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
        });
    }

    let (exec_project_id, exec_scope_id, exec_table_name) =
        join::resolve_table_ref(project_id, scope_id, &query.table);
    let mut query = query;
    query.table = exec_table_name;
    let table_key = (
        namespace_key(&exec_project_id, &exec_scope_id),
        query.table.clone(),
    );
    let schema = catalog
        .tables
        .get(&table_key)
        .ok_or_else(|| QueryError::TableNotFound {
            project_id: exec_project_id.clone(),
            table: query.table.clone(),
        })?;
    let table = snapshot.table(&exec_project_id, &exec_scope_id, &query.table);
    let mut materialized_seq = None;
    // The point-lookup fast path projects directly; skip it when the output needs
    // computed columns or DISTINCT handling so those run through the pipeline.
    if !query.distinct
        && query.computed.is_empty()
        && let Some(result) = try_primary_key_point_query(PrimaryKeyPointQueryRequest {
            snapshot,
            schema,
            project_id: &exec_project_id,
            scope_id: &exec_scope_id,
            query: &query,
            cursor_state: &cursor_state,
            snapshot_seq,
            read_set: read_set.as_deref_mut(),
        })?
    {
        return Ok(result);
    }
    validate_query(schema, &query)?;

    let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let page_window = compute_page_window(&query, &cursor_state, max_scan_rows)?;
    // DISTINCT must see every candidate row before deduplicating, so the row
    // source and pipeline are left unbounded (offset/limit are applied after
    // dedup, in the DISTINCT branch below).
    let (source_offset, source_limit) = if query.distinct {
        (0usize, usize::MAX)
    } else {
        (page_window.row_offset_count, page_window.page_read_limit)
    };

    let mut has_residual_filter = query.predicate.is_some();
    let estimated_rows: usize;
    let mut row_source_satisfies_order = false;
    let mut row_source_index_used = options.async_index.clone();
    let mut row_source_applies_offset = false;
    let row_source: Box<dyn Iterator<Item = Row> + Send + '_> = if let Some(async_index) =
        &options.async_index
    {
        let projection = snapshot
            .async_index(&exec_project_id, &exec_scope_id, &query.table, async_index)
            .ok_or_else(|| QueryError::InvalidQuery {
                reason: "async index not found".into(),
            })?;
        materialized_seq = Some(projection.materialized_seq);
        estimated_rows = projection.rows.len();
        // Async-index projections expose their own materialized_seq; fall
        // back to recording a coarse table range for the underlying table
        // so writes invalidate subscribers.
        if let Some(collector) = read_set.as_deref_mut() {
            collector.record_full_table_scan(
                snapshot,
                &exec_project_id,
                &exec_scope_id,
                &query.table,
            );
        }
        Box::new(projection.rows.values().cloned())
    } else if let (Some(predicate), Some(table)) = (&query.predicate, table)
        && let Some(ordered_scan) =
            ordered_predicate_index_scan_for_query(OrderedPredicateIndexScanRequest {
                catalog,
                project_id: &exec_project_id,
                scope_id: &exec_scope_id,
                schema,
                query: &query,
                table,
                predicate,
                segment_store: snapshot.kv_segment_store.as_deref(),
                offset: source_offset,
                limit: source_limit,
                has_cursor: cursor_state.is_some(),
            })?
    {
        has_residual_filter = false;
        row_source_satisfies_order = true;
        row_source_applies_offset = true;
        row_source_index_used = Some(ordered_scan.index_name);
        if let Some(collector) = read_set.as_deref_mut() {
            collector.record_touched_pks(
                snapshot,
                schema,
                &exec_project_id,
                &exec_scope_id,
                &query.table,
                &ordered_scan.pks,
            );
        }
        estimated_rows = ordered_scan.pks.len();
        let mut materialized = Vec::with_capacity(ordered_scan.pks.len());
        for pk in ordered_scan.pks {
            if let Some(row) =
                snapshot.get_row_by_encoded(&exec_project_id, &exec_scope_id, &query.table, &pk)?
            {
                materialized.push(row.into_owned());
            }
        }
        Box::new(materialized.into_iter())
    } else if let (Some(predicate), Some(table)) = (&query.predicate, table) {
        let candidate_limit = if cursor_state.is_none()
            && !query.distinct
            && query.order_by.is_empty()
            && query.aggregates.is_empty()
            && query.having.is_none()
        {
            Some(page_window.row_source_window_limit)
        } else {
            None
        };
        let indexed_pks = indexed_pks_for_predicate_limited(
            catalog,
            &exec_project_id,
            &exec_scope_id,
            &query.table,
            table,
            snapshot.kv_segment_store.as_deref(),
            predicate,
            candidate_limit,
        )?;
        match indexed_pks {
            Some(indexed) => {
                has_residual_filter = !indexed.predicate_exact;
                let mut pks = indexed.pks;
                // Deep-OFFSET fast path: when the index lookup fully satisfies the
                // predicate and nothing downstream reorders the rows (no ORDER BY,
                // GROUP BY, aggregates, HAVING, or cursor — all implied by
                // `candidate_limit`/`group_by` here), the row-source order is the
                // output order. We can drop the first `offset` primary keys before
                // materializing, avoiding the cost of paging in rows only to skip
                // them. `candidate_limit` already bounded `pks` to `offset+page+1`.
                if indexed.predicate_exact
                    && candidate_limit.is_some()
                    && query.group_by.is_empty()
                    && page_window.row_offset_count > 0
                {
                    let skip = page_window.row_offset_count.min(pks.len());
                    pks.drain(0..skip);
                    row_source_applies_offset = true;
                }
                if let Some(collector) = read_set.as_deref_mut() {
                    collector.record_touched_pks(
                        snapshot,
                        schema,
                        &exec_project_id,
                        &exec_scope_id,
                        &query.table,
                        &pks,
                    );
                }
                estimated_rows = pks.len();
                let mut materialized = Vec::with_capacity(pks.len());
                for pk in pks {
                    if let Some(row) = snapshot.get_row_by_encoded(
                        &exec_project_id,
                        &exec_scope_id,
                        &query.table,
                        &pk,
                    )? {
                        materialized.push(row.into_owned());
                    }
                }
                Box::new(materialized.into_iter())
            }
            None => {
                // No secondary index matched. If the predicate pins a leading
                // prefix of the primary key, scan only that contiguous PK-ordered
                // key band instead of the whole table. Rows are returned in
                // primary-key order — identical to a full scan — so downstream
                // ordering/pagination is unchanged; this only narrows the source.
                let pk_prefix =
                    predicate::extract_primary_key_prefix(predicate, &schema.primary_key);
                // Scope the reactive read-set to the scanned band. A `TablePrefix`
                // range still invalidates on future inserts into the band (a new row
                // whose PK begins with the prefix is a TableRow write that the range
                // matches), but writes to *other* bands (e.g. other instances' rows in
                // a shared table) no longer wake this subscription. Falls back to a
                // whole-table range when no prefix is pinned.
                if let Some(collector) = read_set {
                    match &pk_prefix {
                        Some(prefix) => collector.record_table_prefix(
                            snapshot,
                            &exec_project_id,
                            &exec_scope_id,
                            &query.table,
                            prefix.values.clone(),
                        ),
                        None => collector.record_full_table_scan(
                            snapshot,
                            &exec_project_id,
                            &exec_scope_id,
                            &query.table,
                        ),
                    }
                }
                let (start, end) = match &pk_prefix {
                    Some(prefix) => {
                        // An exact prefix match needs no residual filter: every
                        // row in the band satisfies the predicate.
                        has_residual_filter = !prefix.exact;
                        pk_prefix_scan_bounds(&prefix.values)
                    }
                    None => (std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
                };
                let scanned = snapshot.tier_scan_rows(
                    &exec_project_id,
                    &exec_scope_id,
                    &query.table,
                    start,
                    end,
                    usize::MAX,
                )?;
                estimated_rows = scanned.len();
                let materialized: Vec<crate::catalog::types::Row> =
                    scanned.into_iter().map(|(_, row)| row).collect();
                Box::new(materialized.into_iter())
            }
        }
    } else if let Some(table) = table
        && let Some(ordered_scan) = ordered_index_scan_for_query(OrderedIndexScanRequest {
            catalog,
            project_id: &exec_project_id,
            scope_id: &exec_scope_id,
            schema,
            query: &query,
            table,
            segment_store: snapshot.kv_segment_store.as_deref(),
            offset: source_offset,
            limit: source_limit,
            has_cursor: cursor_state.is_some(),
        })?
    {
        row_source_satisfies_order = true;
        row_source_applies_offset = true;
        row_source_index_used = Some(ordered_scan.index_name);
        if let Some(collector) = read_set {
            collector.record_full_table_scan(
                snapshot,
                &exec_project_id,
                &exec_scope_id,
                &query.table,
            );
        }
        estimated_rows = ordered_scan.pks.len();
        let mut materialized = Vec::with_capacity(ordered_scan.pks.len());
        for pk in ordered_scan.pks {
            if let Some(row) =
                snapshot.get_row_by_encoded(&exec_project_id, &exec_scope_id, &query.table, &pk)?
            {
                materialized.push(row.into_owned());
            }
        }
        Box::new(materialized.into_iter())
    } else {
        if let Some(collector) = read_set {
            collector.record_full_table_scan(
                snapshot,
                &exec_project_id,
                &exec_scope_id,
                &query.table,
            );
        }
        let scanned = snapshot.tier_scan_rows(
            &exec_project_id,
            &exec_scope_id,
            &query.table,
            std::ops::Bound::Unbounded,
            std::ops::Bound::Unbounded,
            usize::MAX,
        )?;
        estimated_rows = scanned.len();
        let materialized: Vec<crate::catalog::types::Row> =
            scanned.into_iter().map(|(_, row)| row).collect();
        Box::new(materialized.into_iter())
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
        row_source_index_used,
        estimated_rows as u64,
        has_residual_filter,
    )?;

    let pipeline = build_operator_pipeline(OperatorPipelineRequest {
        row_source,
        physical_plan: &physical_plan,
        query: &query,
        columns,
        row_source_satisfies_order,
        cursor_absent: cursor_state.is_none(),
        row_source_window_limit: page_window.row_source_window_limit,
    })?;
    let mut root = pipeline.root;
    let selected_indices = pipeline.selected_indices;
    let row_columns = pipeline.row_columns;
    // Computed projections are evaluated against the full (pre-projection) row
    // layout and appended after the selected columns.
    let compiled_computed = computed::compile_computed(&query.computed, &row_columns)?;

    if query.distinct {
        // Drain the (bounded) row source, project, then deduplicate the output
        // rows before applying offset/limit. Cursor pagination is rejected above.
        let mut seen: std::collections::HashSet<Vec<crate::catalog::types::Value>> =
            std::collections::HashSet::new();
        let mut distinct_rows: Vec<Row> = Vec::new();
        while let Some(row) = root.next() {
            let base: Vec<crate::catalog::types::Value> = match &selected_indices {
                Some(selected) => selected
                    .iter()
                    .map(|idx| row.values[*idx].clone())
                    .collect(),
                None => row.values.clone(),
            };
            let projected = computed::append_computed(base, &compiled_computed, &row);
            if seen.insert(projected.values.clone()) {
                distinct_rows.push(projected);
            }
        }
        let rows_examined = root.rows_examined();
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
            rows: sliced,
            rows_examined,
            truncated: has_more,
            cursor: None,
            snapshot_seq,
            materialized_seq,
            split_recommended,
        });
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
    let mut skipped = 0usize;
    let effective_row_offset_count = if row_source_applies_offset {
        0
    } else {
        page_window.row_offset_count
    };
    while let Some(row) = root.next() {
        if let Some(cursor) = &cursor_state
            && !row_after_cursor(&row, cursor, &sort_indices, &pk_indices)
        {
            continue;
        }
        if skipped < effective_row_offset_count {
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
    let sliced: Vec<Row> = if compiled_computed.is_empty() {
        if let Some(selected) = &selected_indices {
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
        }
    } else {
        sliced
            .into_iter()
            .map(|row| {
                let base: Vec<crate::catalog::types::Value> = match &selected_indices {
                    Some(selected) => selected
                        .iter()
                        .map(|idx| row.values[*idx].clone())
                        .collect(),
                    None => row.values.clone(),
                };
                computed::append_computed(base, &compiled_computed, &row)
            })
            .collect()
    };
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

    let rows_examined = root.rows_examined();
    let split_budget = query.limit.unwrap_or(max_scan_rows);
    let split_recommended = compute_split_recommended(rows_examined, split_budget);
    Ok(QueryResult {
        rows: sliced,
        rows_examined,
        truncated: has_more,
        cursor,
        snapshot_seq,
        materialized_seq,
        split_recommended,
    })
}

fn query_requires_full_evaluation(query: &Query, has_cursor: bool) -> bool {
    has_cursor
        || query.distinct
        || !query.group_by.is_empty()
        || !query.aggregates.is_empty()
        || query.having.is_some()
        || query.limit.is_none()
}

#[cfg(test)]
mod index_tests;
#[cfg(test)]
mod join_tests;
#[cfg(test)]
mod ordered_tests;
#[cfg(test)]
mod page_tests;
#[cfg(test)]
mod pagination_tests;
#[cfg(test)]
mod read_set_tests;
#[cfg(test)]
mod scan_bound_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod validation_tests;
