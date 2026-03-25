use crate::catalog::schema::TableSchema;
use crate::catalog::types::Value;
use crate::query::error::QueryError;
use crate::query::plan::{
    MAX_AGGREGATES, MAX_EXPR_IN_LIST_VALUES, MAX_GROUP_BY_COLUMNS, MAX_LIKE_PATTERN_BYTES,
    MAX_ORDER_BY_COLUMNS, Query,
};

pub(super) fn validate_query(schema: &TableSchema, query: &Query) -> Result<(), QueryError> {
    if query.order_by.len() > MAX_ORDER_BY_COLUMNS {
        return Err(QueryError::InvalidQuery {
            reason: format!(
                "ORDER BY has {} columns, exceeds maximum of {}",
                query.order_by.len(),
                MAX_ORDER_BY_COLUMNS
            ),
        });
    }
    if query.group_by.len() > MAX_GROUP_BY_COLUMNS {
        return Err(QueryError::InvalidQuery {
            reason: format!(
                "GROUP BY has {} columns, exceeds maximum of {}",
                query.group_by.len(),
                MAX_GROUP_BY_COLUMNS
            ),
        });
    }
    if query.aggregates.len() > MAX_AGGREGATES {
        return Err(QueryError::InvalidQuery {
            reason: format!(
                "query has {} aggregates, exceeds maximum of {}",
                query.aggregates.len(),
                MAX_AGGREGATES
            ),
        });
    }
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

pub(super) fn validate_expr_types(
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
                ColumnType::U8 => matches!(
                    value,
                    Value::U8(_)
                        | Value::U64(_)
                        | Value::Integer(_)
                        | Value::Float(_)
                        | Value::Timestamp(_)
                ),
                ColumnType::U64 => {
                    matches!(value, Value::U8(_) | Value::U64(_) | Value::Integer(_))
                }
                ColumnType::Integer => matches!(
                    value,
                    Value::U8(_)
                        | Value::U64(_)
                        | Value::Integer(_)
                        | Value::Float(_)
                        | Value::Timestamp(_)
                ),
                ColumnType::Float => matches!(
                    value,
                    Value::U8(_)
                        | Value::U64(_)
                        | Value::Integer(_)
                        | Value::Float(_)
                        | Value::Timestamp(_)
                ),
                ColumnType::Timestamp => matches!(
                    value,
                    Value::U8(_)
                        | Value::U64(_)
                        | Value::Integer(_)
                        | Value::Float(_)
                        | Value::Timestamp(_)
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
            if values.len() > MAX_EXPR_IN_LIST_VALUES {
                return Err(QueryError::InvalidQuery {
                    reason: format!(
                        "IN list has {} values, exceeds maximum of {}",
                        values.len(),
                        MAX_EXPR_IN_LIST_VALUES
                    ),
                });
            }
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
        Expr::Like(c, pattern) => {
            let t = find_col_type(c)?;
            if !matches!(t, ColumnType::Text) {
                return Err(QueryError::TypeMismatch {
                    column: c.clone(),
                    expected: "Text".to_string(),
                    got: format!("{t:?}"),
                });
            }
            if pattern.len() > MAX_LIKE_PATTERN_BYTES {
                return Err(QueryError::InvalidQuery {
                    reason: format!(
                        "LIKE pattern is {} bytes, exceeds maximum of {}",
                        pattern.len(),
                        MAX_LIKE_PATTERN_BYTES
                    ),
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
