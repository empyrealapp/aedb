use crate::catalog::Catalog;
use crate::catalog::namespace_key;
use crate::catalog::types::Value;
use crate::query::error::QueryError;
use crate::storage::encoded_key::EncodedKey;
use std::collections::HashMap;

use super::composite_index::{CompositeSelectionCriteria, composite_prefix_index_lookup};
use super::index_diagnostics::{
    merge_selected_indexes_if_diagnostic, merge_trace_if_diagnostic,
    merge_trace_single_if_diagnostic, plan_trace_if_diagnostic, selected_indexes_if_diagnostic,
};
use super::index_lookup::{IndexLookup, extract_indexable_predicate};
use super::index_utils::{expr_implied_by_eq_constraints, intersect_pks, union_pks};
use super::predicate::collect_eq_constraints;

pub(super) struct IndexLookupContext<'a> {
    pub(super) catalog: &'a Catalog,
    pub(super) project_id: &'a str,
    pub(super) scope_id: &'a str,
    pub(super) table_name: &'a str,
    pub(super) table: &'a crate::storage::keyspace::TableData,
    pub(super) include_diagnostics: bool,
}

#[derive(Debug, Clone)]
pub(super) struct IndexLookupResult {
    pub pks: Vec<EncodedKey>,
    pub selected_indexes: Vec<String>,
    pub plan_trace: Vec<String>,
    pub predicate_exact: bool,
}

pub(super) fn indexed_pks_for_predicate_limited(
    catalog: &Catalog,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    table: &crate::storage::keyspace::TableData,
    predicate: &crate::query::plan::Expr,
    candidate_limit: Option<usize>,
) -> Result<Option<IndexLookupResult>, QueryError> {
    let context = IndexLookupContext {
        catalog,
        project_id,
        scope_id,
        table_name,
        table,
        include_diagnostics: false,
    };
    indexed_pks_for_predicate_inner(&context, predicate, candidate_limit)
}

pub(super) fn indexed_pks_for_predicate_with_trace(
    catalog: &Catalog,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    table: &crate::storage::keyspace::TableData,
    predicate: &crate::query::plan::Expr,
) -> Result<Option<IndexLookupResult>, QueryError> {
    let context = IndexLookupContext {
        catalog,
        project_id,
        scope_id,
        table_name,
        table,
        include_diagnostics: true,
    };
    indexed_pks_for_predicate_inner(&context, predicate, None)
}

fn indexed_pks_for_predicate_inner(
    context: &IndexLookupContext<'_>,
    predicate: &crate::query::plan::Expr,
    candidate_limit: Option<usize>,
) -> Result<Option<IndexLookupResult>, QueryError> {
    let mut result = indexed_pks_for_predicate_uncapped(context, predicate, candidate_limit)?;
    if let Some(result) = result.as_mut()
        && result.predicate_exact
        && let Some(limit) = candidate_limit
    {
        result.pks.truncate(limit);
    }
    Ok(result)
}

fn indexed_pks_for_predicate_uncapped(
    context: &IndexLookupContext<'_>,
    predicate: &crate::query::plan::Expr,
    candidate_limit: Option<usize>,
) -> Result<Option<IndexLookupResult>, QueryError> {
    use crate::query::plan::Expr;

    match predicate {
        Expr::And(lhs, rhs) => {
            let left = indexed_pks_for_predicate_uncapped(context, lhs, None)?;
            let right = indexed_pks_for_predicate_uncapped(context, rhs, None)?;
            return Ok(match (left, right) {
                (Some(left), Some(right)) => Some(IndexLookupResult {
                    pks: intersect_pks(left.pks, right.pks),
                    selected_indexes: merge_selected_indexes_if_diagnostic(
                        context.include_diagnostics,
                        left.selected_indexes,
                        right.selected_indexes,
                    ),
                    predicate_exact: left.predicate_exact && right.predicate_exact,
                    plan_trace: merge_trace_if_diagnostic(
                        context.include_diagnostics,
                        "AND predicate combines indexed candidates with intersection",
                        left.plan_trace,
                        right.plan_trace,
                    ),
                }),
                (Some(left), None) => Some(IndexLookupResult {
                    plan_trace: merge_trace_single_if_diagnostic(
                        context.include_diagnostics,
                        "AND predicate uses indexed left side; right side will be residual filter",
                        left.plan_trace,
                    ),
                    predicate_exact: false,
                    ..left
                }),
                (None, Some(right)) => Some(IndexLookupResult {
                    plan_trace: merge_trace_single_if_diagnostic(
                        context.include_diagnostics,
                        "AND predicate uses indexed right side; left side will be residual filter",
                        right.plan_trace,
                    ),
                    predicate_exact: false,
                    ..right
                }),
                (None, None) => None,
            });
        }
        Expr::Or(lhs, rhs) => {
            let left = indexed_pks_for_predicate_uncapped(context, lhs, None)?;
            let right = indexed_pks_for_predicate_uncapped(context, rhs, None)?;
            return Ok(match (left, right) {
                (Some(left), Some(right)) => Some(IndexLookupResult {
                    pks: union_pks(left.pks, right.pks),
                    selected_indexes: merge_selected_indexes_if_diagnostic(
                        context.include_diagnostics,
                        left.selected_indexes,
                        right.selected_indexes,
                    ),
                    predicate_exact: left.predicate_exact && right.predicate_exact,
                    plan_trace: merge_trace_if_diagnostic(
                        context.include_diagnostics,
                        "OR predicate combines indexed candidates with union",
                        left.plan_trace,
                        right.plan_trace,
                    ),
                }),
                _ => None,
            });
        }
        _ => {}
    }

    let Some(lookup) = extract_indexable_predicate(predicate) else {
        let mut equalities = HashMap::new();
        let eq_only = collect_eq_constraints(predicate, &mut equalities);
        if !eq_only {
            return Ok(None);
        }
        return composite_prefix_index_lookup(
            context,
            &equalities,
            CompositeSelectionCriteria::default(),
            candidate_limit,
        );
    };
    let column = match &lookup {
        IndexLookup::Range { column, .. }
        | IndexLookup::Eq { column, .. }
        | IndexLookup::In { column, .. } => *column,
    };

    let mut selected_index_name: Option<&str> = None;
    let ns = namespace_key(context.project_id, context.scope_id);
    let mut equalities = None;
    for ((p, t, idx_name), idx_def) in &context.catalog.indexes {
        if p != &ns
            || t != context.table_name
            || idx_def.columns.len() != 1
            || idx_def.columns[0] != column
            || !context.table.indexes.contains_key(idx_name)
        {
            continue;
        }
        if let Some(filter) = &idx_def.partial_filter {
            let equalities = equalities.get_or_insert_with(|| {
                let mut equalities = HashMap::new();
                collect_eq_constraints(predicate, &mut equalities);
                equalities
            });
            if !expr_implied_by_eq_constraints(filter, equalities) {
                continue;
            }
        }
        selected_index_name = Some(idx_name.as_str());
        break;
    }

    let Some(index_name) = selected_index_name else {
        if let IndexLookup::Eq { column, value, .. } = lookup
            && let Some(result) = indexed_pks_for_leftmost_composite_eq(
                context,
                column,
                value,
                predicate,
                candidate_limit,
            )?
        {
            return Ok(Some(result));
        }
        return Ok(None);
    };
    let Some(index) = context.table.indexes.get(index_name) else {
        return Ok(None);
    };
    let trace_column = context.include_diagnostics.then(|| column.to_string());

    let predicate_exact = lookup.predicate_exact();
    let lookup_limit = if predicate_exact {
        candidate_limit
    } else {
        None
    };
    let pks = match lookup {
        IndexLookup::Range { bounds, .. } => {
            index.scan_range_limit(bounds.0, bounds.1, lookup_limit.unwrap_or(usize::MAX))
        }
        IndexLookup::Eq { value, .. } => index.scan_eq_limit(
            &EncodedKey::from_values(std::slice::from_ref(value)),
            lookup_limit.unwrap_or(usize::MAX),
        ),
        IndexLookup::In { values, .. } => {
            let limit = lookup_limit.unwrap_or(usize::MAX);
            let mut pks = Vec::new();
            for value in values {
                let remaining = limit.saturating_sub(pks.len());
                if remaining == 0 {
                    break;
                }
                pks.extend(index.scan_eq_limit(
                    &EncodedKey::from_values(std::slice::from_ref(value)),
                    remaining,
                ));
            }
            pks
        }
    };
    Ok(Some(IndexLookupResult {
        pks,
        selected_indexes: selected_indexes_if_diagnostic(context.include_diagnostics, index_name),
        predicate_exact,
        plan_trace: plan_trace_if_diagnostic(context.include_diagnostics, || {
            let column = trace_column.as_deref().unwrap_or("");
            format!("selected single-column index '{index_name}' for predicate on '{column}'")
        }),
    }))
}

fn indexed_pks_for_leftmost_composite_eq(
    context: &IndexLookupContext<'_>,
    column: &str,
    value: &Value,
    predicate: &crate::query::plan::Expr,
    candidate_limit: Option<usize>,
) -> Result<Option<IndexLookupResult>, QueryError> {
    let mut equalities = HashMap::new();
    collect_eq_constraints(predicate, &mut equalities);
    if !equalities.contains_key(column) {
        equalities.insert(column.to_string(), value.clone());
    }

    composite_prefix_index_lookup(
        context,
        &equalities,
        CompositeSelectionCriteria {
            first_column: Some(column),
            min_index_cols: 2,
        },
        candidate_limit,
    )
}
