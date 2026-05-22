use crate::catalog::Catalog;
use crate::catalog::namespace_key;
use crate::catalog::types::Value;
use crate::query::error::QueryError;
use crate::storage::encoded_key::EncodedKey;
use std::collections::{HashMap, HashSet};
use std::ops::Bound;

use super::predicate::collect_eq_constraints;

type IndexBounds = (Bound<EncodedKey>, Bound<EncodedKey>);

enum IndexLookup<'a> {
    Range {
        column: &'a str,
        bounds: IndexBounds,
        predicate_exact: bool,
    },
    Eq {
        column: &'a str,
        value: &'a Value,
        predicate_exact: bool,
    },
    In {
        column: &'a str,
        values: &'a [Value],
        predicate_exact: bool,
    },
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
    indexed_pks_for_predicate_inner(
        catalog,
        project_id,
        scope_id,
        table_name,
        table,
        predicate,
        candidate_limit,
        false,
    )
}

pub(super) fn indexed_pks_for_predicate_with_trace(
    catalog: &Catalog,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    table: &crate::storage::keyspace::TableData,
    predicate: &crate::query::plan::Expr,
) -> Result<Option<IndexLookupResult>, QueryError> {
    indexed_pks_for_predicate_inner(
        catalog, project_id, scope_id, table_name, table, predicate, None, true,
    )
}

fn indexed_pks_for_predicate_inner(
    catalog: &Catalog,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    table: &crate::storage::keyspace::TableData,
    predicate: &crate::query::plan::Expr,
    candidate_limit: Option<usize>,
    include_diagnostics: bool,
) -> Result<Option<IndexLookupResult>, QueryError> {
    let mut result = indexed_pks_for_predicate_uncapped(
        catalog,
        project_id,
        scope_id,
        table_name,
        table,
        predicate,
        candidate_limit,
        include_diagnostics,
    )?;
    if let Some(result) = result.as_mut()
        && result.predicate_exact
        && let Some(limit) = candidate_limit
    {
        result.pks.truncate(limit);
    }
    Ok(result)
}

fn indexed_pks_for_predicate_uncapped(
    catalog: &Catalog,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    table: &crate::storage::keyspace::TableData,
    predicate: &crate::query::plan::Expr,
    candidate_limit: Option<usize>,
    include_diagnostics: bool,
) -> Result<Option<IndexLookupResult>, QueryError> {
    use crate::query::plan::Expr;

    match predicate {
        Expr::And(lhs, rhs) => {
            let left = indexed_pks_for_predicate_uncapped(
                catalog,
                project_id,
                scope_id,
                table_name,
                table,
                lhs,
                None,
                include_diagnostics,
            )?;
            let right = indexed_pks_for_predicate_uncapped(
                catalog,
                project_id,
                scope_id,
                table_name,
                table,
                rhs,
                None,
                include_diagnostics,
            )?;
            return Ok(match (left, right) {
                (Some(left), Some(right)) => Some(IndexLookupResult {
                    pks: intersect_pks(left.pks, right.pks),
                    selected_indexes: merge_selected_indexes_if_diagnostic(
                        include_diagnostics,
                        left.selected_indexes,
                        right.selected_indexes,
                    ),
                    predicate_exact: left.predicate_exact && right.predicate_exact,
                    plan_trace: merge_trace_if_diagnostic(
                        include_diagnostics,
                        "AND predicate combines indexed candidates with intersection",
                        left.plan_trace,
                        right.plan_trace,
                    ),
                }),
                (Some(left), None) => Some(IndexLookupResult {
                    plan_trace: merge_trace_single_if_diagnostic(
                        include_diagnostics,
                        "AND predicate uses indexed left side; right side will be residual filter",
                        left.plan_trace,
                    ),
                    predicate_exact: false,
                    ..left
                }),
                (None, Some(right)) => Some(IndexLookupResult {
                    plan_trace: merge_trace_single_if_diagnostic(
                        include_diagnostics,
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
            let left = indexed_pks_for_predicate_uncapped(
                catalog,
                project_id,
                scope_id,
                table_name,
                table,
                lhs,
                None,
                include_diagnostics,
            )?;
            let right = indexed_pks_for_predicate_uncapped(
                catalog,
                project_id,
                scope_id,
                table_name,
                table,
                rhs,
                None,
                include_diagnostics,
            )?;
            return Ok(match (left, right) {
                (Some(left), Some(right)) => Some(IndexLookupResult {
                    pks: union_pks(left.pks, right.pks),
                    selected_indexes: merge_selected_indexes_if_diagnostic(
                        include_diagnostics,
                        left.selected_indexes,
                        right.selected_indexes,
                    ),
                    predicate_exact: left.predicate_exact && right.predicate_exact,
                    plan_trace: merge_trace_if_diagnostic(
                        include_diagnostics,
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
        let selected_index =
            table
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
            selected_index.scan_eq_limit(&encoded, candidate_limit.unwrap_or(usize::MAX))
        } else {
            match candidate_limit {
                Some(limit) => selected_index.scan_prefix_window(Some(&encoded), 0, limit),
                None => selected_index.scan_prefix(&encoded),
            }
        };
        return Ok(Some(IndexLookupResult {
            pks,
            selected_indexes: selected_indexes_if_diagnostic(include_diagnostics, &idx_name),
            predicate_exact: true,
            plan_trace: plan_trace_if_diagnostic(include_diagnostics, || {
                format!(
                    "selected composite index '{idx_name}' with leftmost prefix columns={prefix_cols}"
                )
            }),
        }));
    };
    let column = match &lookup {
        IndexLookup::Range { column, .. }
        | IndexLookup::Eq { column, .. }
        | IndexLookup::In { column, .. } => *column,
    };

    let mut selected_index_name: Option<&str> = None;
    let ns = namespace_key(project_id, scope_id);
    let mut equalities = None;
    for ((p, t, idx_name), idx_def) in &catalog.indexes {
        if p != &ns
            || t != table_name
            || idx_def.columns.len() != 1
            || idx_def.columns[0] != column
            || !table.indexes.contains_key(idx_name)
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
                catalog,
                project_id,
                scope_id,
                table_name,
                table,
                column,
                value,
                predicate,
                candidate_limit,
                include_diagnostics,
            )?
        {
            return Ok(Some(result));
        }
        return Ok(None);
    };
    let Some(index) = table.indexes.get(index_name) else {
        return Ok(None);
    };
    let trace_column = include_diagnostics.then(|| column.to_string());

    let predicate_exact = lookup.predicate_exact();
    let lookup_limit = if predicate_exact {
        candidate_limit
    } else {
        None
    };
    let pks = match lookup {
        IndexLookup::Range { bounds, .. } => {
            let pks =
                index.scan_range_limit(bounds.0, bounds.1, lookup_limit.unwrap_or(usize::MAX));
            pks
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
        selected_indexes: selected_indexes_if_diagnostic(include_diagnostics, index_name),
        predicate_exact,
        plan_trace: plan_trace_if_diagnostic(include_diagnostics, || {
            let column = trace_column.as_deref().unwrap_or("");
            format!("selected single-column index '{index_name}' for predicate on '{column}'")
        }),
    }))
}

#[allow(clippy::too_many_arguments)]
fn indexed_pks_for_leftmost_composite_eq(
    catalog: &Catalog,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    table: &crate::storage::keyspace::TableData,
    column: &str,
    value: &Value,
    predicate: &crate::query::plan::Expr,
    candidate_limit: Option<usize>,
    include_diagnostics: bool,
) -> Result<Option<IndexLookupResult>, QueryError> {
    let ns = namespace_key(project_id, scope_id);
    let mut equalities = HashMap::new();
    collect_eq_constraints(predicate, &mut equalities);
    if !equalities.contains_key(column) {
        equalities.insert(column.to_string(), value.clone());
    }

    let mut best: Option<(&str, usize)> = None;
    for ((p, t, idx_name), idx_def) in &catalog.indexes {
        if p != &ns
            || t != table_name
            || idx_def.columns.len() <= 1
            || idx_def.columns.first().map(String::as_str) != Some(column)
            || !table.indexes.contains_key(idx_name)
        {
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
        if best.as_ref().map(|(_, cols)| *cols).unwrap_or(0) < prefix_cols {
            best = Some((idx_name.as_str(), prefix_cols));
        }
    }

    let Some((idx_name, prefix_cols)) = best else {
        return Ok(None);
    };
    let selected_index = table
        .indexes
        .get(idx_name)
        .ok_or_else(|| QueryError::InvalidQuery {
            reason: "index not found".into(),
        })?;
    let idx_def = catalog
        .indexes
        .get(&(ns, table_name.to_string(), idx_name.to_string()))
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
        selected_index.scan_eq_limit(&encoded, candidate_limit.unwrap_or(usize::MAX))
    } else {
        match candidate_limit {
            Some(limit) => selected_index.scan_prefix_window(Some(&encoded), 0, limit),
            None => selected_index.scan_prefix(&encoded),
        }
    };

    Ok(Some(IndexLookupResult {
        pks,
        selected_indexes: selected_indexes_if_diagnostic(include_diagnostics, idx_name),
        predicate_exact: true,
        plan_trace: plan_trace_if_diagnostic(include_diagnostics, || {
            format!(
                "selected composite index '{idx_name}' with leftmost prefix columns={prefix_cols}"
            )
        }),
    }))
}

fn selected_indexes_if_diagnostic(include_diagnostics: bool, index_name: &str) -> Vec<String> {
    if include_diagnostics {
        vec![index_name.to_string()]
    } else {
        Vec::new()
    }
}

fn plan_trace_if_diagnostic<F>(include_diagnostics: bool, trace: F) -> Vec<String>
where
    F: FnOnce() -> String,
{
    if include_diagnostics {
        vec![trace()]
    } else {
        Vec::new()
    }
}

fn merge_selected_indexes_if_diagnostic(
    include_diagnostics: bool,
    left: Vec<String>,
    right: Vec<String>,
) -> Vec<String> {
    if !include_diagnostics {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(left.len() + right.len());
    for name in left.into_iter().chain(right) {
        if !out.contains(&name) {
            out.push(name);
        }
    }
    out
}

fn merge_trace_if_diagnostic(
    include_diagnostics: bool,
    header: &str,
    mut left: Vec<String>,
    right: Vec<String>,
) -> Vec<String> {
    if !include_diagnostics {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(1 + left.len() + right.len());
    out.push(header.to_string());
    out.append(&mut left);
    out.extend(right);
    out
}

fn merge_trace_single_if_diagnostic(
    include_diagnostics: bool,
    header: &str,
    mut trace: Vec<String>,
) -> Vec<String> {
    if !include_diagnostics {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(1 + trace.len());
    out.push(header.to_string());
    out.append(&mut trace);
    out
}

fn intersect_pks(left: Vec<EncodedKey>, right: Vec<EncodedKey>) -> Vec<EncodedKey> {
    let mut right_set: HashSet<EncodedKey> = HashSet::with_capacity(right.len());
    right_set.extend(right);
    let mut out = Vec::with_capacity(left.len().min(right_set.len()));
    for pk in left {
        if right_set.contains(&pk) {
            out.push(pk);
        }
    }
    out
}

fn union_pks(left: Vec<EncodedKey>, right: Vec<EncodedKey>) -> Vec<EncodedKey> {
    let mut seen: HashSet<EncodedKey> = HashSet::with_capacity(left.len() + right.len());
    let mut out = Vec::with_capacity(left.len() + right.len());
    for pk in left.into_iter().chain(right) {
        if seen.insert(pk.clone()) {
            out.push(pk);
        }
    }
    out
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

fn extract_indexable_predicate(predicate: &crate::query::plan::Expr) -> Option<IndexLookup<'_>> {
    use crate::query::plan::Expr;

    match predicate {
        Expr::Eq(c, v) => Some(IndexLookup::Eq {
            column: c,
            value: v,
            predicate_exact: true,
        }),
        Expr::In(c, values) => Some(IndexLookup::In {
            column: c,
            values,
            predicate_exact: true,
        }),
        Expr::Lt(c, v) => Some(IndexLookup::Range {
            column: c,
            bounds: (
                Bound::Unbounded,
                Bound::Excluded(EncodedKey::from_values(std::slice::from_ref(v))),
            ),
            predicate_exact: true,
        }),
        Expr::Lte(c, v) => Some(IndexLookup::Range {
            column: c,
            bounds: (
                Bound::Unbounded,
                Bound::Included(EncodedKey::from_values(std::slice::from_ref(v))),
            ),
            predicate_exact: true,
        }),
        Expr::Gt(c, v) => Some(IndexLookup::Range {
            column: c,
            bounds: (
                Bound::Excluded(EncodedKey::from_values(std::slice::from_ref(v))),
                Bound::Unbounded,
            ),
            predicate_exact: true,
        }),
        Expr::Gte(c, v) => Some(IndexLookup::Range {
            column: c,
            bounds: (
                Bound::Included(EncodedKey::from_values(std::slice::from_ref(v))),
                Bound::Unbounded,
            ),
            predicate_exact: true,
        }),
        Expr::Between(c, lo, hi) => Some(IndexLookup::Range {
            column: c,
            bounds: (
                Bound::Included(EncodedKey::from_values(std::slice::from_ref(lo))),
                Bound::Included(EncodedKey::from_values(std::slice::from_ref(hi))),
            ),
            predicate_exact: true,
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
                column: c,
                bounds: (start, end),
                predicate_exact: like_prefix_is_exact(pattern),
            })
        }
        _ => None,
    }
}

impl IndexLookup<'_> {
    fn predicate_exact(&self) -> bool {
        match self {
            Self::Range {
                predicate_exact, ..
            }
            | Self::Eq {
                predicate_exact, ..
            }
            | Self::In {
                predicate_exact, ..
            } => *predicate_exact,
        }
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

fn like_prefix_is_exact(pattern: &str) -> bool {
    let mut wildcard_seen = false;
    for ch in pattern.chars() {
        match ch {
            '%' => wildcard_seen = true,
            '_' => return false,
            _ if wildcard_seen => return false,
            _ => {}
        }
    }
    wildcard_seen
}

fn next_prefix(prefix: &str) -> Option<String> {
    let mut bytes = prefix.as_bytes().to_vec();
    for byte_index in (0..bytes.len()).rev() {
        if bytes[byte_index] != u8::MAX {
            bytes[byte_index] += 1;
            bytes.truncate(byte_index + 1);
            return String::from_utf8(bytes).ok();
        }
    }
    None
}
