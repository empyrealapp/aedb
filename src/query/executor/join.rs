use super::{
    CursorToken, QueryResult, aggregate_col_idx, aggregate_output_name, encode_cursor,
    extract_pk_key, extract_sort_key, row_after_cursor,
};
use crate::catalog::Catalog;
use crate::catalog::namespace_key;
use crate::catalog::types::{Row, Value};
use crate::query::error::QueryError;
use crate::query::operators::{AggregateOperator, Operator, ScanOperator, compile_expr};
use crate::query::plan::{JoinType, Query, QueryOptions};
use crate::storage::encoded_key::EncodedKey;
use crate::storage::keyspace::KeyspaceSnapshot;
use std::collections::HashMap;

#[allow(clippy::too_many_arguments)]
pub(super) fn execute_join_query(
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
        let join_table = snapshot.table(&jp, &js, &jt);
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
        let can_probe_right_primary_key =
            matches!(join.join_type, JoinType::Inner | JoinType::Left)
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

        let mut joined = Vec::new();
        match join.join_type {
            JoinType::Cross => {
                let join_rows: Vec<&Row> = join_table
                    .map(|table| table.rows.values().collect())
                    .unwrap_or_default();
                for left in &rows {
                    for right in &join_rows {
                        let mut values = left.values.clone();
                        values.extend(right.values.clone());
                        joined.push(Row { values });
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
                        let key = left.values[left_idx].clone();
                        let matched = join_table.and_then(|table| {
                            let encoded = EncodedKey::from_values(&[key]);
                            table.rows.get(&encoded)
                        });
                        if let Some(right) = matched {
                            let mut values = left.values.clone();
                            values.extend(right.values.clone());
                            joined.push(Row { values });
                        } else if matches!(join.join_type, JoinType::Left) {
                            let mut values = left.values.clone();
                            values.extend(std::iter::repeat_n(
                                Value::Null,
                                join_schema.columns.len(),
                            ));
                            joined.push(Row { values });
                        }
                    }
                } else {
                    let join_rows: Vec<&Row> = join_table
                        .map(|table| table.rows.values().collect())
                        .unwrap_or_default();
                    // Hash join for non-PK equality predicates.
                    let mut right_map: HashMap<Value, Vec<&Row>> = HashMap::new();
                    for right in &join_rows {
                        right_map
                            .entry(right.values[right_idx].clone())
                            .or_default()
                            .push(right);
                    }
                    for left in &rows {
                        let key = left.values[left_idx].clone();
                        if let Some(matches) = right_map.get(&key) {
                            for right in matches {
                                let mut values = left.values.clone();
                                values.extend(right.values.clone());
                                joined.push(Row { values });
                            }
                        } else if matches!(join.join_type, JoinType::Left) {
                            let mut values = left.values.clone();
                            values.extend(std::iter::repeat_n(
                                Value::Null,
                                join_schema.columns.len(),
                            ));
                            joined.push(Row { values });
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
                let join_rows: Vec<&Row> = join_table
                    .map(|table| table.rows.values().collect())
                    .unwrap_or_default();
                let mut left_map: HashMap<Value, Vec<&Row>> = HashMap::new();
                for left in &rows {
                    left_map
                        .entry(left.values[left_idx].clone())
                        .or_default()
                        .push(left);
                }
                for right in &join_rows {
                    let key = right.values[right_idx].clone();
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

    let rows_examined = rows.len();
    let page_size = query.limit.unwrap_or_else(|| {
        cursor_state
            .as_ref()
            .map(|c| c.page_size)
            .unwrap_or(max_scan_rows.min(100))
    });
    let effective_page_size = page_size.min(max_scan_rows);
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
    for row in rows {
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
        sliced = sliced
            .into_iter()
            .map(|r| Row {
                values: idxs.iter().map(|i| r.values[*i].clone()).collect(),
            })
            .collect();
    }

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
        rows_examined,
        rows: sliced,
        truncated: cursor.is_some(),
        cursor,
        snapshot_seq,
        materialized_seq: None,
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
