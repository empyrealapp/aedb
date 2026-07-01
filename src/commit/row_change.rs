//! Row-level commit deltas.
//!
//! Given the committed keyspace *before* a transaction plus its mutations,
//! derive the concrete per-row changes it makes: which rows in which tables were
//! inserted, updated, or deleted, with new column values where they are known.
//! This lets a subscription layer compute per-subscriber diffs from the
//! commit-delta feed (`subscribe_commits`) without re-querying.
//!
//! Derivation reads only the *pre-commit* keyspace, so it is independent of which
//! apply path (general, coordinator, deferred-parallel) a commit takes, and it
//! reuses apply's own predicate compilation and primary-key extraction so the
//! resolved set matches exactly what apply changed (same row-policy binding, same
//! primary-key-prefix scan bounds, same `limit` semantics).
//!
//! Only table-row mutations produce changes; KV, counter, order-book, and DDL
//! mutations yield none. Expression-based updates (`UpdateWhereExpr`) report the
//! affected primary keys with `new_row: None` (the post-values are not derived).

use crate::catalog::Catalog;
use crate::catalog::namespace_key;
use crate::catalog::schema::TableSchema;
use crate::catalog::types::{Row, Value};
use crate::commit::apply::{compile_table_predicate, extract_primary_key_from_row};
use crate::commit::validation::Mutation;
use crate::permission::CallerContext;
use crate::query::executor::{extract_primary_key_prefix, pk_prefix_scan_bounds};
use crate::query::operators::eval_compiled_expr_public;
use crate::query::plan::Expr;
use crate::storage::keyspace::Keyspace;
use std::ops::Bound;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowChangeKind {
    Insert,
    Update,
    Delete,
}

/// A single resolved row-level change produced by a committed transaction.
#[derive(Debug, Clone, PartialEq)]
pub struct RowChange {
    pub project_id: String,
    pub scope_id: String,
    pub table_name: String,
    pub primary_key: Vec<Value>,
    pub kind: RowChangeKind,
    /// New column values for `Insert`/`Update` when known; `None` for `Delete`
    /// and for expression-based updates whose post-value is not derived.
    pub new_row: Option<Row>,
}

/// Derive the row-level changes made by `mutations` against the pre-commit
/// `pre` keyspace. `caller` must be the committing envelope's caller so that
/// row-policy-bound predicates resolve to the same set apply used.
pub(crate) fn derive_row_changes(
    pre: &Keyspace,
    catalog: &Catalog,
    mutations: &[Mutation],
    caller: Option<&CallerContext>,
    max_scan_rows: usize,
) -> Vec<RowChange> {
    let mut out = Vec::new();
    for mutation in mutations {
        derive_one(pre, catalog, mutation, caller, max_scan_rows, &mut out);
    }
    out
}

fn schema_for<'a>(
    catalog: &'a Catalog,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
) -> Option<&'a TableSchema> {
    catalog
        .tables
        .get(&(namespace_key(project_id, scope_id), table_name.to_string()))
}

/// Whether a row with `primary_key` currently exists in `pre` (drives the
/// Insert-vs-Update distinction for point upserts). Uses the exact single-key
/// band `[pk, successor(pk))`.
fn row_exists(
    pre: &Keyspace,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    primary_key: &[Value],
) -> bool {
    let (start, end) = pk_prefix_scan_bounds(primary_key);
    pre.tier_scan_rows(project_id, scope_id, table_name, start, end, 1)
        .map(|rows| !rows.is_empty())
        .unwrap_or(false)
}

fn upsert_kind(
    pre: &Keyspace,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    primary_key: &[Value],
) -> RowChangeKind {
    if row_exists(pre, project_id, scope_id, table_name, primary_key) {
        RowChangeKind::Update
    } else {
        RowChangeKind::Insert
    }
}

/// Resolve the `(primary_key, old_row)` pairs a predicate mutation touches,
/// mirroring apply exactly: same effective (row-policy-bound) predicate, same
/// primary-key-prefix scan bounds, same post-filter `limit` counting.
#[allow(clippy::too_many_arguments)]
fn resolve_matches(
    pre: &Keyspace,
    catalog: &Catalog,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    predicate: &Expr,
    limit: Option<usize>,
    caller: Option<&CallerContext>,
    max_scan_rows: usize,
) -> Vec<(Vec<Value>, Row)> {
    let Some(schema) = schema_for(catalog, project_id, scope_id, table_name) else {
        return Vec::new();
    };
    let Ok(compiled) = compile_table_predicate(
        catalog, schema, project_id, scope_id, table_name, predicate, caller,
    ) else {
        return Vec::new();
    };
    let (start, end) = match extract_primary_key_prefix(predicate, &schema.primary_key) {
        Some(prefix) => pk_prefix_scan_bounds(&prefix.values),
        None => (Bound::Unbounded, Bound::Unbounded),
    };
    let scanned = pre
        .tier_scan_rows(project_id, scope_id, table_name, start, end, max_scan_rows)
        .unwrap_or_default();
    let mut matched = Vec::new();
    for (_pk_encoded, row) in scanned {
        if !eval_compiled_expr_public(&compiled, &row) {
            continue;
        }
        let Ok(pk) = extract_primary_key_from_row(schema, &row) else {
            continue;
        };
        matched.push((pk, row));
        if limit.is_some_and(|max| matched.len() >= max) {
            break;
        }
    }
    matched
}

fn derive_one(
    pre: &Keyspace,
    catalog: &Catalog,
    mutation: &Mutation,
    caller: Option<&CallerContext>,
    max_scan_rows: usize,
    out: &mut Vec<RowChange>,
) {
    match mutation {
        Mutation::Insert {
            project_id,
            scope_id,
            table_name,
            primary_key,
            row,
        }
        | Mutation::Upsert {
            project_id,
            scope_id,
            table_name,
            primary_key,
            row,
        } => {
            let kind = upsert_kind(pre, project_id, scope_id, table_name, primary_key);
            out.push(RowChange {
                project_id: project_id.clone(),
                scope_id: scope_id.clone(),
                table_name: table_name.clone(),
                primary_key: primary_key.clone(),
                kind,
                new_row: Some(row.clone()),
            });
        }
        Mutation::InsertBatch {
            project_id,
            scope_id,
            table_name,
            rows,
        }
        | Mutation::UpsertBatch {
            project_id,
            scope_id,
            table_name,
            rows,
        } => {
            let Some(schema) = schema_for(catalog, project_id, scope_id, table_name) else {
                return;
            };
            for row in rows {
                let Ok(pk) = extract_primary_key_from_row(schema, row) else {
                    continue;
                };
                let kind = upsert_kind(pre, project_id, scope_id, table_name, &pk);
                out.push(RowChange {
                    project_id: project_id.clone(),
                    scope_id: scope_id.clone(),
                    table_name: table_name.clone(),
                    primary_key: pk,
                    kind,
                    new_row: Some(row.clone()),
                });
            }
        }
        Mutation::Delete {
            project_id,
            scope_id,
            table_name,
            primary_key,
        } => {
            out.push(RowChange {
                project_id: project_id.clone(),
                scope_id: scope_id.clone(),
                table_name: table_name.clone(),
                primary_key: primary_key.clone(),
                kind: RowChangeKind::Delete,
                new_row: None,
            });
        }
        Mutation::DeleteWhere {
            project_id,
            scope_id,
            table_name,
            predicate,
            limit,
        } => {
            for (pk, _old) in resolve_matches(
                pre,
                catalog,
                project_id,
                scope_id,
                table_name,
                predicate,
                *limit,
                caller,
                max_scan_rows,
            ) {
                out.push(RowChange {
                    project_id: project_id.clone(),
                    scope_id: scope_id.clone(),
                    table_name: table_name.clone(),
                    primary_key: pk,
                    kind: RowChangeKind::Delete,
                    new_row: None,
                });
            }
        }
        Mutation::UpdateWhere {
            project_id,
            scope_id,
            table_name,
            predicate,
            updates,
            limit,
        } => {
            let column_index = |name: &str| {
                schema_for(catalog, project_id, scope_id, table_name)
                    .and_then(|s| s.columns.iter().position(|c| c.name == name))
            };
            for (pk, old_row) in resolve_matches(
                pre,
                catalog,
                project_id,
                scope_id,
                table_name,
                predicate,
                *limit,
                caller,
                max_scan_rows,
            ) {
                let mut new_row = old_row;
                for (col, value) in updates {
                    if let Some(i) = column_index(col)
                        && let Some(slot) = new_row.values.get_mut(i)
                    {
                        *slot = value.clone();
                    }
                }
                out.push(RowChange {
                    project_id: project_id.clone(),
                    scope_id: scope_id.clone(),
                    table_name: table_name.clone(),
                    primary_key: pk,
                    kind: RowChangeKind::Update,
                    new_row: Some(new_row),
                });
            }
        }
        Mutation::UpdateWhereExpr {
            project_id,
            scope_id,
            table_name,
            predicate,
            limit,
            ..
        } => {
            // Post-values require evaluating the update expressions; report the
            // affected primary keys without new column values.
            for (pk, _old) in resolve_matches(
                pre,
                catalog,
                project_id,
                scope_id,
                table_name,
                predicate,
                *limit,
                caller,
                max_scan_rows,
            ) {
                out.push(RowChange {
                    project_id: project_id.clone(),
                    scope_id: scope_id.clone(),
                    table_name: table_name.clone(),
                    primary_key: pk,
                    kind: RowChangeKind::Update,
                    new_row: None,
                });
            }
        }
        // KV, counter, order-book, DDL, and table-cell atomic mutations do not
        // produce table row-level changes in this feed.
        _ => {}
    }
}
