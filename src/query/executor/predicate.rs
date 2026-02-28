use crate::catalog::types::Value;
use crate::query::plan::Expr;
use std::collections::HashMap;

pub(super) fn extract_primary_key_values(
    predicate: &Expr,
    primary_key: &[String],
) -> Option<Vec<Value>> {
    if primary_key.is_empty() {
        return None;
    }
    let mut equalities: HashMap<String, Value> = HashMap::new();
    if !collect_eq_constraints(predicate, &mut equalities) {
        return None;
    }
    if equalities.len() != primary_key.len() {
        return None;
    }
    let mut values = Vec::with_capacity(primary_key.len());
    for key_col in primary_key {
        let value = equalities.get(key_col)?;
        values.push(value.clone());
    }
    Some(values)
}

pub(super) fn collect_eq_constraints(expr: &Expr, equalities: &mut HashMap<String, Value>) -> bool {
    match expr {
        Expr::Eq(column, value) => {
            if let Some(existing) = equalities.get(column) {
                existing == value
            } else {
                equalities.insert(column.clone(), value.clone());
                true
            }
        }
        Expr::And(lhs, rhs) => {
            collect_eq_constraints(lhs, equalities) && collect_eq_constraints(rhs, equalities)
        }
        _ => false,
    }
}
