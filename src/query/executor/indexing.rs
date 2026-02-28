use crate::catalog::Catalog;
use crate::catalog::namespace_key;
use crate::catalog::types::Value;
use crate::query::error::QueryError;
use crate::storage::encoded_key::EncodedKey;
use std::collections::{HashMap, HashSet};
use std::ops::Bound;

use super::predicate::collect_eq_constraints;

type IndexBounds = (Bound<EncodedKey>, Bound<EncodedKey>);

enum IndexLookup {
    Range { column: String, bounds: IndexBounds },
    MultiEq { column: String, values: Vec<Value> },
}

pub(super) fn indexed_pks_for_predicate(
    catalog: &Catalog,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    table: &crate::storage::keyspace::TableData,
    predicate: &crate::query::plan::Expr,
) -> Result<Option<Vec<EncodedKey>>, QueryError> {
    use crate::query::plan::Expr;

    match predicate {
        Expr::And(lhs, rhs) => {
            let left =
                indexed_pks_for_predicate(catalog, project_id, scope_id, table_name, table, lhs)?;
            let right =
                indexed_pks_for_predicate(catalog, project_id, scope_id, table_name, table, rhs)?;
            return Ok(match (left, right) {
                (Some(left), Some(right)) => Some(intersect_pks(left, right)),
                (Some(left), None) => Some(left),
                (None, Some(right)) => Some(right),
                (None, None) => None,
            });
        }
        Expr::Or(lhs, rhs) => {
            let left =
                indexed_pks_for_predicate(catalog, project_id, scope_id, table_name, table, lhs)?;
            let right =
                indexed_pks_for_predicate(catalog, project_id, scope_id, table_name, table, rhs)?;
            return Ok(match (left, right) {
                (Some(left), Some(right)) => Some(union_pks(left, right)),
                _ => None,
            });
        }
        _ => {}
    }

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
            selected_index.scan_eq(&encoded)
        } else {
            selected_index.scan_prefix(&encoded)
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
    for byte_index in (0..bytes.len()).rev() {
        if bytes[byte_index] != u8::MAX {
            bytes[byte_index] += 1;
            bytes.truncate(byte_index + 1);
            return String::from_utf8(bytes).ok();
        }
    }
    None
}
