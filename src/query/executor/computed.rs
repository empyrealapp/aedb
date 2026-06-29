use crate::catalog::types::{Row, Value};
use crate::query::error::QueryError;
use crate::query::plan::{ArithOp, ComputedColumn, Query, ScalarExpr};

/// A [`ScalarExpr`] compiled against a fixed column layout: column references
/// resolve to row indices.
pub(super) enum CompiledScalar {
    Column(usize),
    Literal(Value),
    Binary(Box<CompiledScalar>, ArithOp, Box<CompiledScalar>),
}

fn compile_scalar(expr: &ScalarExpr, columns: &[String]) -> Result<CompiledScalar, QueryError> {
    match expr {
        ScalarExpr::Column(name) => columns
            .iter()
            .position(|c| c == name)
            .map(CompiledScalar::Column)
            .ok_or_else(|| QueryError::ColumnNotFound {
                table: "computed".into(),
                column: name.clone(),
            }),
        ScalarExpr::Literal(value) => Ok(CompiledScalar::Literal(value.clone())),
        ScalarExpr::Binary(lhs, op, rhs) => Ok(CompiledScalar::Binary(
            Box::new(compile_scalar(lhs, columns)?),
            *op,
            Box::new(compile_scalar(rhs, columns)?),
        )),
    }
}

/// Compile each computed column's expression against `columns` (the full
/// pre-projection row layout). Returns an empty vec when there are none.
pub(super) fn compile_computed(
    computed: &[ComputedColumn],
    columns: &[String],
) -> Result<Vec<CompiledScalar>, QueryError> {
    computed
        .iter()
        .map(|c| compile_scalar(&c.expr, columns))
        .collect()
}

fn as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::U8(v) => Some(*v as f64),
        Value::U64(v) => Some(*v as f64),
        Value::Integer(v) => Some(*v as f64),
        Value::Timestamp(v) => Some(*v as f64),
        Value::Float(v) => Some(*v),
        _ => None,
    }
}

fn as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::U8(v) => Some(*v as i64),
        Value::U64(v) => i64::try_from(*v).ok(),
        Value::Integer(v) => Some(*v),
        Value::Timestamp(v) => Some(*v),
        _ => None,
    }
}

/// Evaluate arithmetic with the same cross-type coercion as comparisons. Integer
/// inputs stay integral for +/-/* (and exact division); any float input or
/// inexact division produces a `Float`. NULL or non-numeric operands yield NULL.
fn arith(a: &Value, op: ArithOp, b: &Value) -> Value {
    if matches!(a, Value::Null) || matches!(b, Value::Null) {
        return Value::Null;
    }
    if let (Some(x), Some(y)) = (as_i64(a), as_i64(b)) {
        match op {
            ArithOp::Add => return x.checked_add(y).map_or(Value::Null, Value::Integer),
            ArithOp::Sub => return x.checked_sub(y).map_or(Value::Null, Value::Integer),
            ArithOp::Mul => return x.checked_mul(y).map_or(Value::Null, Value::Integer),
            ArithOp::Div => {
                if y != 0 && x % y == 0 {
                    return Value::Integer(x / y);
                }
                // Fall through to float division below.
            }
        }
    }
    let (Some(x), Some(y)) = (as_f64(a), as_f64(b)) else {
        return Value::Null;
    };
    let result = match op {
        ArithOp::Add => x + y,
        ArithOp::Sub => x - y,
        ArithOp::Mul => x * y,
        ArithOp::Div => {
            if y == 0.0 {
                return Value::Null;
            }
            x / y
        }
    };
    Value::Float(result)
}

pub(super) fn eval_scalar(expr: &CompiledScalar, row: &Row) -> Value {
    match expr {
        CompiledScalar::Column(idx) => row.values.get(*idx).cloned().unwrap_or(Value::Null),
        CompiledScalar::Literal(value) => value.clone(),
        CompiledScalar::Binary(lhs, op, rhs) => {
            arith(&eval_scalar(lhs, row), *op, &eval_scalar(rhs, row))
        }
    }
}

/// Append the computed-column values to `base` (the already-projected `select`
/// values) by evaluating each compiled expression against the full `full_row`.
pub(super) fn append_computed(
    mut base: Vec<Value>,
    compiled: &[CompiledScalar],
    full_row: &Row,
) -> Row {
    for scalar in compiled {
        base.push(eval_scalar(scalar, full_row));
    }
    Row { values: base }
}

/// Computed projections cannot be combined with aggregates (the post-aggregate
/// row layout does not expose the base columns the expressions reference).
pub(super) fn validate_computed(query: &Query) -> Result<(), QueryError> {
    if !query.computed.is_empty() && !query.aggregates.is_empty() {
        return Err(QueryError::InvalidQuery {
            reason: "computed projections are not supported with aggregates".into(),
        });
    }
    Ok(())
}
