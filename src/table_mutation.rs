use crate::catalog::schema::TableSchema;
use crate::catalog::types::{Row, Value};
use crate::commit::validation::TableUpdateExpr;
use crate::error::AedbError;

pub(crate) fn extract_primary_key_values(
    schema: &TableSchema,
    row: &Row,
) -> Result<Vec<Value>, AedbError> {
    let mut primary_key = Vec::with_capacity(schema.primary_key.len());
    for pk_name in &schema.primary_key {
        let column_index = schema
            .columns
            .iter()
            .position(|c| c.name == *pk_name)
            .ok_or_else(|| {
                AedbError::Validation(format!("primary key column missing: {pk_name}"))
            })?;
        let value = row.values.get(column_index).ok_or_else(|| {
            AedbError::Validation(format!(
                "row missing primary key value for column: {pk_name}"
            ))
        })?;
        primary_key.push(value.clone());
    }
    Ok(primary_key)
}

pub(crate) fn apply_table_update_exprs(
    schema: &TableSchema,
    row: &Row,
    updates: &[(String, TableUpdateExpr)],
) -> Result<Row, AedbError> {
    let mut next = row.clone();
    for (target_column, expr) in updates {
        let target_idx = schema
            .columns
            .iter()
            .position(|c| c.name == *target_column)
            .ok_or_else(|| AedbError::UnknownColumn {
                table: schema.table_name.clone(),
                column: target_column.clone(),
            })?;
        let current = next.values.get(target_idx).cloned().ok_or_else(|| {
            AedbError::Validation(format!(
                "target column index out of bounds: {target_column}"
            ))
        })?;
        let value = match expr {
            TableUpdateExpr::Value(value) => value.clone(),
            TableUpdateExpr::CopyColumn(source_column) => {
                let source_idx = schema
                    .columns
                    .iter()
                    .position(|c| c.name == *source_column)
                    .ok_or_else(|| AedbError::UnknownColumn {
                        table: schema.table_name.clone(),
                        column: source_column.clone(),
                    })?;
                next.values.get(source_idx).cloned().ok_or_else(|| {
                    AedbError::Validation(format!(
                        "source column index out of bounds: {source_column}"
                    ))
                })?
            }
            TableUpdateExpr::AddI64(delta) => match current {
                Value::Integer(current) => current
                    .checked_add(*delta)
                    .map(Value::Integer)
                    .ok_or(AedbError::Overflow)?,
                _ => {
                    return Err(AedbError::Validation(format!(
                        "AddI64 requires Integer current value for column {target_column}"
                    )));
                }
            },
            TableUpdateExpr::Coalesce(fallback) => {
                if matches!(current, Value::Null) {
                    fallback.clone()
                } else {
                    current
                }
            }
        };
        next.values[target_idx] = value;
    }
    Ok(next)
}
