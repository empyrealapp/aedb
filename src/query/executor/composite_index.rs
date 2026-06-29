use crate::catalog::namespace_key;
use crate::catalog::schema::IndexType;
use crate::catalog::types::Value;
use crate::query::error::QueryError;
use crate::query::executor::index_diagnostics::{
    plan_trace_if_diagnostic, selected_indexes_if_diagnostic,
};
use crate::query::executor::index_utils::expr_implied_by_eq_constraints;
use crate::query::executor::indexing::{IndexLookupContext, IndexLookupResult};
use crate::storage::encoded_key::EncodedKey;
use crate::storage::keyspace::SecondaryIndex;
use std::collections::HashMap;

struct CompositeIndexSelection<'a> {
    index_name: &'a str,
    prefix_cols: usize,
    index_cols: usize,
}

#[derive(Default)]
pub(super) struct CompositeSelectionCriteria<'a> {
    pub(super) first_column: Option<&'a str>,
    pub(super) min_index_cols: usize,
}

pub(super) fn composite_prefix_index_lookup(
    context: &IndexLookupContext<'_>,
    equalities: &HashMap<String, Value>,
    criteria: CompositeSelectionCriteria<'_>,
    candidate_limit: Option<usize>,
) -> Result<Option<IndexLookupResult>, QueryError> {
    let Some(selection) = select_composite_prefix_index(context, equalities, criteria) else {
        return Ok(None);
    };
    let selected_index = context
        .table
        .indexes
        .get(selection.index_name)
        .ok_or_else(|| QueryError::InvalidQuery {
            reason: "index not found".into(),
        })?;
    let prefix_values = composite_prefix_values(context, selection.index_name, equalities)?;
    let encoded = EncodedKey::from_values(&prefix_values[..selection.prefix_cols]);
    let pks = scan_composite_prefix(
        selected_index,
        &encoded,
        selection.prefix_cols,
        selection.index_cols,
        candidate_limit,
        context.segment_store,
    )?;
    Ok(Some(IndexLookupResult {
        pks,
        selected_indexes: selected_indexes_if_diagnostic(
            context.include_diagnostics,
            selection.index_name,
        ),
        predicate_exact: true,
        plan_trace: plan_trace_if_diagnostic(context.include_diagnostics, || {
            format!(
                "selected composite index '{}' with leftmost prefix columns={}",
                selection.index_name, selection.prefix_cols
            )
        }),
    }))
}

fn select_composite_prefix_index<'a>(
    context: &'a IndexLookupContext<'_>,
    equalities: &HashMap<String, Value>,
    criteria: CompositeSelectionCriteria<'_>,
) -> Option<CompositeIndexSelection<'a>> {
    let ns = namespace_key(context.project_id, context.scope_id);
    let mut best: Option<CompositeIndexSelection<'a>> = None;
    for ((p, t, idx_name), idx_def) in &context.catalog.indexes {
        if p != &ns
            || t != context.table_name
            || idx_def.columns.len() < criteria.min_index_cols
            || !context.table.indexes.contains_key(idx_name)
        {
            continue;
        }
        if let Some(first_column) = criteria.first_column
            && idx_def.columns.first().map(String::as_str) != Some(first_column)
        {
            continue;
        }
        if let Some(filter) = &idx_def.partial_filter
            && !expr_implied_by_eq_constraints(filter, equalities)
        {
            continue;
        }
        let prefix_cols = idx_def
            .columns
            .iter()
            .take_while(|col| equalities.contains_key(*col))
            .count();
        if prefix_cols == 0 {
            continue;
        }
        // A Hash/UniqueHash store can only serve a full-key equality lookup
        // (scan_eq); a partial prefix routes through scan_prefix*, which returns
        // no rows for hash stores. Skip them for partial prefixes so the caller
        // falls back to a residual full-scan filter rather than dropping rows.
        if prefix_cols < idx_def.columns.len()
            && !matches!(idx_def.index_type, IndexType::BTree | IndexType::Art)
        {
            continue;
        }
        if best
            .as_ref()
            .map(|selection| selection.prefix_cols)
            .unwrap_or(0)
            < prefix_cols
        {
            best = Some(CompositeIndexSelection {
                index_name: idx_name.as_str(),
                prefix_cols,
                index_cols: idx_def.columns.len(),
            });
        }
    }
    best
}

fn composite_prefix_values(
    context: &IndexLookupContext<'_>,
    index_name: &str,
    equalities: &HashMap<String, Value>,
) -> Result<Vec<Value>, QueryError> {
    let ns = namespace_key(context.project_id, context.scope_id);
    let idx_def = context
        .catalog
        .indexes
        .get(&(ns, context.table_name.to_string(), index_name.to_string()))
        .ok_or_else(|| QueryError::InvalidQuery {
            reason: "index definition not found".into(),
        })?;
    Ok(idx_def
        .columns
        .iter()
        .filter_map(|c| equalities.get(c).cloned())
        .collect())
}

fn scan_composite_prefix(
    index: &SecondaryIndex,
    encoded_prefix: &EncodedKey,
    prefix_cols: usize,
    index_cols: usize,
    candidate_limit: Option<usize>,
    store: Option<&crate::storage::kv_segment::KvSegmentStore>,
) -> Result<Vec<EncodedKey>, QueryError> {
    if prefix_cols == index_cols {
        return Ok(index.tier_scan_eq_limit(
            encoded_prefix,
            candidate_limit.unwrap_or(usize::MAX),
            store,
        )?);
    }
    let limit = candidate_limit.unwrap_or(usize::MAX);
    Ok(index.tier_scan_prefix_window(Some(encoded_prefix), 0, limit, store)?)
}
