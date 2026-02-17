use crate::catalog::schema::TableSchema;
use crate::catalog::types::{ColumnType, Row, Value};
use crate::catalog::{Catalog, namespace_key};
use crate::commit::tx::{AssertionActual, ReadAssertion};
use crate::commit::validation::CompareOp;
use crate::error::AedbError;
use crate::query::plan::Expr;
use crate::storage::encoded_key::EncodedKey;
use crate::storage::keyspace::Keyspace;
use primitive_types::U256;
use std::cmp::Ordering;

pub fn validate_assertions(
    catalog: &Catalog,
    assertions: &[ReadAssertion],
) -> Result<(), AedbError> {
    for assertion in assertions {
        validate_assertion(catalog, assertion)?;
    }
    Ok(())
}

pub fn evaluate_assertions(
    catalog: &Catalog,
    keyspace: &Keyspace,
    assertions: &[ReadAssertion],
) -> Result<(), AedbError> {
    for (index, assertion) in assertions.iter().enumerate() {
        match evaluate_assertion(catalog, keyspace, assertion)? {
            None => {}
            Some(actual) => {
                return Err(AedbError::AssertionFailed {
                    index,
                    assertion: Box::new(assertion.clone()),
                    actual: Box::new(actual),
                });
            }
        }
    }
    Ok(())
}

fn validate_assertion(catalog: &Catalog, assertion: &ReadAssertion) -> Result<(), AedbError> {
    match assertion {
        ReadAssertion::KeyEquals { .. }
        | ReadAssertion::KeyCompare { .. }
        | ReadAssertion::KeyExists { .. }
        | ReadAssertion::KeyVersion { .. } => Ok(()),
        ReadAssertion::RowVersion {
            project_id,
            scope_id,
            table_name,
            primary_key,
            ..
        }
        | ReadAssertion::RowExists {
            project_id,
            scope_id,
            table_name,
            primary_key,
            ..
        } => {
            let schema = table_schema(catalog, project_id, scope_id, table_name)?;
            if primary_key.len() != schema.primary_key.len() {
                return Err(AedbError::Validation("primary key length mismatch".into()));
            }
            Ok(())
        }
        ReadAssertion::RowColumnCompare {
            project_id,
            scope_id,
            table_name,
            primary_key,
            column,
            threshold,
            ..
        } => {
            let schema = table_schema(catalog, project_id, scope_id, table_name)?;
            if primary_key.len() != schema.primary_key.len() {
                return Err(AedbError::Validation("primary key length mismatch".into()));
            }
            let Some(col) = schema.columns.iter().find(|c| c.name == *column) else {
                return Err(AedbError::UnknownColumn {
                    table: schema.table_name.clone(),
                    column: column.clone(),
                });
            };
            if matches!(threshold, Value::Null) && !col.nullable {
                return Err(AedbError::NotNullViolation {
                    table: schema.table_name.clone(),
                    column: column.clone(),
                });
            }
            if !matches!(threshold, Value::Null) && !value_matches_type(threshold, &col.col_type) {
                return Err(AedbError::TypeMismatch {
                    table: schema.table_name.clone(),
                    column: column.clone(),
                    expected: format!("{:?}", col.col_type),
                    actual: format!("{threshold:?}"),
                });
            }
            Ok(())
        }
        ReadAssertion::CountCompare {
            project_id,
            scope_id,
            table_name,
            filter,
            ..
        } => {
            let schema = table_schema(catalog, project_id, scope_id, table_name)?;
            if let Some(expr) = filter {
                expr.validate_depth()?;
                validate_filter_columns(schema, expr)?;
            }
            Ok(())
        }
        ReadAssertion::SumCompare {
            project_id,
            scope_id,
            table_name,
            column,
            filter,
            threshold,
            ..
        } => {
            let schema = table_schema(catalog, project_id, scope_id, table_name)?;
            let Some(col) = schema.columns.iter().find(|c| c.name == *column) else {
                return Err(AedbError::UnknownColumn {
                    table: schema.table_name.clone(),
                    column: column.clone(),
                });
            };
            if !matches!(
                col.col_type,
                ColumnType::Integer | ColumnType::Float | ColumnType::U256 | ColumnType::Timestamp
            ) {
                return Err(AedbError::Validation(format!(
                    "column {column} is not numeric for SumCompare"
                )));
            }
            if !value_matches_type(threshold, &col.col_type) {
                return Err(AedbError::Validation(format!(
                    "sum threshold type mismatch for column {column}"
                )));
            }
            if let Some(expr) = filter {
                expr.validate_depth()?;
                validate_filter_columns(schema, expr)?;
            }
            Ok(())
        }
        ReadAssertion::All(assertions) | ReadAssertion::Any(assertions) => {
            for inner in assertions {
                validate_assertion(catalog, inner)?;
            }
            Ok(())
        }
        ReadAssertion::Not(inner) => validate_assertion(catalog, inner),
    }
}

fn evaluate_assertion(
    catalog: &Catalog,
    keyspace: &Keyspace,
    assertion: &ReadAssertion,
) -> Result<Option<AssertionActual>, AedbError> {
    match assertion {
        ReadAssertion::KeyEquals {
            project_id,
            scope_id,
            key,
            expected,
        } => {
            let current = keyspace.kv_get(project_id, scope_id, key);
            let actual = current.map(|entry| entry.value.clone());
            if actual.as_ref() == Some(expected) {
                Ok(None)
            } else {
                Ok(Some(
                    actual.map_or(AssertionActual::Missing, AssertionActual::Bytes),
                ))
            }
        }
        ReadAssertion::KeyCompare {
            project_id,
            scope_id,
            key,
            op,
            threshold,
        } => {
            let Some(current) = keyspace.kv_get(project_id, scope_id, key) else {
                return Ok(Some(AssertionActual::Missing));
            };
            if compare_bytes(&current.value, threshold, *op) {
                Ok(None)
            } else {
                Ok(Some(AssertionActual::Bytes(current.value.clone())))
            }
        }
        ReadAssertion::KeyExists {
            project_id,
            scope_id,
            key,
            expected,
        } => {
            let exists = keyspace.kv_get(project_id, scope_id, key).is_some();
            if exists == *expected {
                Ok(None)
            } else {
                Ok(Some(AssertionActual::Bool(exists)))
            }
        }
        ReadAssertion::KeyVersion {
            project_id,
            scope_id,
            key,
            expected_seq,
        } => {
            let version = keyspace.kv_version(project_id, scope_id, key);
            if version == *expected_seq {
                Ok(None)
            } else {
                Ok(Some(AssertionActual::Version(version)))
            }
        }
        ReadAssertion::RowVersion {
            project_id,
            scope_id,
            table_name,
            primary_key,
            expected_seq,
        } => {
            let version = keyspace.get_row_version(project_id, scope_id, table_name, primary_key);
            if version == *expected_seq {
                Ok(None)
            } else {
                Ok(Some(AssertionActual::Version(version)))
            }
        }
        ReadAssertion::RowExists {
            project_id,
            scope_id,
            table_name,
            primary_key,
            expected,
        } => {
            let ns = namespace_key(project_id, scope_id);
            let exists = keyspace
                .table_by_namespace_key(&ns, table_name)
                .and_then(|table| table.rows.get(&EncodedKey::from_values(primary_key)))
                .is_some();
            if exists == *expected {
                Ok(None)
            } else {
                Ok(Some(AssertionActual::Bool(exists)))
            }
        }
        ReadAssertion::RowColumnCompare {
            project_id,
            scope_id,
            table_name,
            primary_key,
            column,
            op,
            threshold,
        } => {
            let schema = table_schema(catalog, project_id, scope_id, table_name)?;
            let Some(column_idx) = schema.columns.iter().position(|c| c.name == *column) else {
                return Err(AedbError::Validation(format!("column not found: {column}")));
            };
            let ns = namespace_key(project_id, scope_id);
            let Some(table) = keyspace.table_by_namespace_key(&ns, table_name) else {
                return Ok(Some(AssertionActual::Missing));
            };
            let Some(row) = table.rows.get(&EncodedKey::from_values(primary_key)) else {
                return Ok(Some(AssertionActual::Missing));
            };
            let Some(current) = row.values.get(column_idx) else {
                return Ok(Some(AssertionActual::Missing));
            };
            if compare_values(current, threshold, *op) {
                Ok(None)
            } else {
                Ok(Some(AssertionActual::Value(current.clone())))
            }
        }
        ReadAssertion::CountCompare {
            project_id,
            scope_id,
            table_name,
            filter,
            op,
            threshold,
        } => {
            let schema = table_schema(catalog, project_id, scope_id, table_name)?;
            let ns = namespace_key(project_id, scope_id);
            let count = keyspace
                .table_by_namespace_key(&ns, table_name)
                .map(|table| {
                    table
                        .rows
                        .values()
                        .filter(|row| match_filter(row, schema, filter).unwrap_or(false))
                        .count() as u64
                })
                .unwrap_or(0);
            if compare_ord(count.cmp(threshold), *op) {
                Ok(None)
            } else {
                Ok(Some(AssertionActual::Count(count)))
            }
        }
        ReadAssertion::SumCompare {
            project_id,
            scope_id,
            table_name,
            column,
            filter,
            op,
            threshold,
        } => {
            let schema = table_schema(catalog, project_id, scope_id, table_name)?;
            let Some(column_idx) = schema.columns.iter().position(|c| c.name == *column) else {
                return Err(AedbError::Validation(format!("column not found: {column}")));
            };
            let ns = namespace_key(project_id, scope_id);
            let sum = match keyspace.table_by_namespace_key(&ns, table_name) {
                None => zero_for_threshold(threshold),
                Some(table) => {
                    sum_rows_for_column(table.rows.values(), schema, column_idx, filter)?
                }
            };
            if compare_values(&sum, threshold, *op) {
                Ok(None)
            } else {
                Ok(Some(AssertionActual::Value(sum)))
            }
        }
        ReadAssertion::All(assertions) => {
            for assertion in assertions {
                if let Some(actual) = evaluate_assertion(catalog, keyspace, assertion)? {
                    return Ok(Some(actual));
                }
            }
            Ok(None)
        }
        ReadAssertion::Any(assertions) => {
            let mut last_actual = AssertionActual::Missing;
            for assertion in assertions {
                match evaluate_assertion(catalog, keyspace, assertion)? {
                    None => return Ok(None),
                    Some(actual) => last_actual = actual,
                }
            }
            Ok(Some(last_actual))
        }
        ReadAssertion::Not(assertion) => match evaluate_assertion(catalog, keyspace, assertion)? {
            None => Ok(Some(AssertionActual::Bool(true))),
            Some(_) => Ok(None),
        },
    }
}

fn table_schema<'a>(
    catalog: &'a Catalog,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
) -> Result<&'a TableSchema, AedbError> {
    catalog
        .tables
        .get(&(namespace_key(project_id, scope_id), table_name.to_string()))
        .ok_or_else(|| {
            AedbError::Validation(format!(
                "table does not exist: {project_id}.{scope_id}.{table_name}"
            ))
        })
}

fn match_filter(row: &Row, schema: &TableSchema, filter: &Option<Expr>) -> Result<bool, AedbError> {
    match filter {
        None => Ok(true),
        Some(expr) => eval_expr(expr, row, schema),
    }
}

fn eval_expr(expr: &Expr, row: &Row, schema: &TableSchema) -> Result<bool, AedbError> {
    match expr {
        Expr::Eq(col, value) => compare_col(row, schema, col, value, CompareOp::Eq),
        Expr::Ne(col, value) => compare_col(row, schema, col, value, CompareOp::Ne),
        Expr::Lt(col, value) => compare_col(row, schema, col, value, CompareOp::Lt),
        Expr::Lte(col, value) => compare_col(row, schema, col, value, CompareOp::Lte),
        Expr::Gt(col, value) => compare_col(row, schema, col, value, CompareOp::Gt),
        Expr::Gte(col, value) => compare_col(row, schema, col, value, CompareOp::Gte),
        Expr::In(col, values) => {
            let v = col_value(row, schema, col)?;
            Ok(values.iter().any(|candidate| v == candidate))
        }
        Expr::Between(col, start, end) => {
            let v = col_value(row, schema, col)?;
            Ok(v >= start && v <= end)
        }
        Expr::IsNull(col) => Ok(matches!(col_value(row, schema, col)?, Value::Null)),
        Expr::IsNotNull(col) => Ok(!matches!(col_value(row, schema, col)?, Value::Null)),
        Expr::Like(col, pattern) => {
            let Value::Text(text) = col_value(row, schema, col)? else {
                return Ok(false);
            };
            Ok(text.contains(pattern))
        }
        Expr::And(lhs, rhs) => Ok(eval_expr(lhs, row, schema)? && eval_expr(rhs, row, schema)?),
        Expr::Or(lhs, rhs) => Ok(eval_expr(lhs, row, schema)? || eval_expr(rhs, row, schema)?),
        Expr::Not(inner) => Ok(!eval_expr(inner, row, schema)?),
    }
}

fn validate_filter_columns(schema: &TableSchema, expr: &Expr) -> Result<(), AedbError> {
    match expr {
        Expr::Eq(col, _)
        | Expr::Ne(col, _)
        | Expr::Lt(col, _)
        | Expr::Lte(col, _)
        | Expr::Gt(col, _)
        | Expr::Gte(col, _)
        | Expr::In(col, _)
        | Expr::Between(col, _, _)
        | Expr::IsNull(col)
        | Expr::IsNotNull(col)
        | Expr::Like(col, _) => {
            if schema.columns.iter().any(|c| c.name == *col) {
                Ok(())
            } else {
                Err(AedbError::Validation(format!("column not found: {col}")))
            }
        }
        Expr::And(lhs, rhs) | Expr::Or(lhs, rhs) => {
            validate_filter_columns(schema, lhs)?;
            validate_filter_columns(schema, rhs)
        }
        Expr::Not(inner) => validate_filter_columns(schema, inner),
    }
}

fn compare_col(
    row: &Row,
    schema: &TableSchema,
    col: &str,
    value: &Value,
    op: CompareOp,
) -> Result<bool, AedbError> {
    let v = col_value(row, schema, col)?;
    Ok(compare_values(v, value, op))
}

fn col_value<'a>(row: &'a Row, schema: &TableSchema, col: &str) -> Result<&'a Value, AedbError> {
    let Some(idx) = schema.columns.iter().position(|c| c.name == col) else {
        return Err(AedbError::Validation(format!("column not found: {col}")));
    };
    row.values
        .get(idx)
        .ok_or_else(|| AedbError::Validation(format!("column value missing: {col}")))
}

fn sum_rows_for_column<'a>(
    rows: impl Iterator<Item = &'a Row>,
    schema: &TableSchema,
    column_idx: usize,
    filter: &Option<Expr>,
) -> Result<Value, AedbError> {
    let col_type = &schema.columns[column_idx].col_type;
    match col_type {
        ColumnType::Integer | ColumnType::Timestamp => {
            let mut sum: i64 = 0;
            for row in rows {
                if !match_filter(row, schema, filter)? {
                    continue;
                }
                if let Some(Value::Integer(v) | Value::Timestamp(v)) = row.values.get(column_idx) {
                    sum = sum.checked_add(*v).ok_or(AedbError::Overflow)?;
                }
            }
            if matches!(col_type, ColumnType::Timestamp) {
                Ok(Value::Timestamp(sum))
            } else {
                Ok(Value::Integer(sum))
            }
        }
        ColumnType::Float => {
            let mut sum = 0.0f64;
            for row in rows {
                if !match_filter(row, schema, filter)? {
                    continue;
                }
                if let Some(Value::Float(v)) = row.values.get(column_idx) {
                    sum += *v;
                }
            }
            Ok(Value::Float(sum))
        }
        ColumnType::U256 => {
            let mut sum = U256::zero();
            for row in rows {
                if !match_filter(row, schema, filter)? {
                    continue;
                }
                if let Some(Value::U256(v)) = row.values.get(column_idx) {
                    let add = U256::from_big_endian(v);
                    sum = sum.checked_add(add).ok_or(AedbError::Overflow)?;
                }
            }
            let mut out = [0u8; 32];
            sum.to_big_endian(&mut out);
            Ok(Value::U256(out))
        }
        _ => Err(AedbError::Validation(
            "unsupported column type for SumCompare".into(),
        )),
    }
}

fn zero_for_threshold(threshold: &Value) -> Value {
    match threshold {
        Value::Integer(_) => Value::Integer(0),
        Value::Timestamp(_) => Value::Timestamp(0),
        Value::Float(_) => Value::Float(0.0),
        Value::U256(_) => Value::U256([0u8; 32]),
        _ => Value::Null,
    }
}

fn compare_bytes(left: &[u8], right: &[u8], op: CompareOp) -> bool {
    let ordering = if left.len() == 32 && right.len() == 32 {
        let l = U256::from_big_endian(left);
        let r = U256::from_big_endian(right);
        l.cmp(&r)
    } else {
        left.cmp(right)
    };
    compare_ord(ordering, op)
}

fn compare_values(left: &Value, right: &Value, op: CompareOp) -> bool {
    compare_ord(left.cmp(right), op)
}

fn compare_ord(ordering: Ordering, op: CompareOp) -> bool {
    match op {
        CompareOp::Eq => ordering == Ordering::Equal,
        CompareOp::Ne => ordering != Ordering::Equal,
        CompareOp::Gt => ordering == Ordering::Greater,
        CompareOp::Gte => matches!(ordering, Ordering::Greater | Ordering::Equal),
        CompareOp::Lt => ordering == Ordering::Less,
        CompareOp::Lte => matches!(ordering, Ordering::Less | Ordering::Equal),
    }
}

fn value_matches_type(value: &Value, col_type: &ColumnType) -> bool {
    matches!(
        (value, col_type),
        (Value::Text(_), ColumnType::Text)
            | (Value::Integer(_), ColumnType::Integer)
            | (Value::Float(_), ColumnType::Float)
            | (Value::Boolean(_), ColumnType::Boolean)
            | (Value::U256(_), ColumnType::U256)
            | (Value::I256(_), ColumnType::I256)
            | (Value::Blob(_), ColumnType::Blob)
            | (Value::Timestamp(_), ColumnType::Timestamp)
            | (Value::Json(_), ColumnType::Json)
    )
}
