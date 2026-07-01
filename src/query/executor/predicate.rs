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

/// A leading-prefix constraint over the primary key, extracted from a query
/// predicate. `values` pins the first `values.len()` primary-key columns (a
/// contiguous prefix, `1 ..= primary_key.len()`) by equality. `exact` is true
/// when the predicate is *exactly* those equalities, so the bounded key-range
/// scan returns precisely the matching rows and no residual filter is needed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PrimaryKeyPrefix {
    pub values: Vec<Value>,
    pub exact: bool,
}

/// Detect a predicate that pins a *leading prefix* of the primary key by
/// equality (e.g. `pk0 = a` or `pk0 = a AND pk1 = b` on a composite key
/// `(pk0, pk1, pk2)`). Such a predicate maps to a contiguous, PK-ordered key
/// band that can be range-scanned instead of scanning the whole table.
///
/// Returns `None` when no leading prefix is constrained (the first PK column has
/// no equality). The returned prefix is the *longest* leading run of PK columns
/// that carry an equality constraint.
pub(crate) fn extract_primary_key_prefix(
    predicate: &Expr,
    primary_key: &[String],
) -> Option<PrimaryKeyPrefix> {
    if primary_key.is_empty() {
        return None;
    }
    // Gather every top-level equality (walking AND nodes). `pure_conjunction` is
    // true only when the whole predicate is a conjunction of equalities with no
    // other operators — a prerequisite for an exact (filter-free) match.
    let mut equalities: HashMap<String, Value> = HashMap::new();
    let pure_conjunction = collect_eq_constraints(predicate, &mut equalities);

    let mut values = Vec::new();
    for key_col in primary_key {
        match equalities.get(key_col) {
            Some(value) => values.push(value.clone()),
            None => break,
        }
    }
    if values.is_empty() {
        return None;
    }
    // Exact when the predicate is a pure conjunction of equalities and every one
    // of those equalities lands on a column of the covered prefix (no extra
    // equality on a non-prefix or non-PK column that still needs filtering).
    let exact = pure_conjunction && equalities.len() == values.len();
    Some(PrimaryKeyPrefix { values, exact })
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

#[cfg(test)]
mod prefix_tests {
    use super::*;
    use crate::query::plan::Expr;

    fn pk() -> Vec<String> {
        vec!["a".into(), "b".into(), "c".into()]
    }

    fn eq(col: &str, v: &str) -> Expr {
        Expr::Eq(col.into(), Value::Text(v.into()))
    }

    #[test]
    fn single_leading_column_is_exact_prefix() {
        // `a = x` on PK (a, b, c): the band covers every row with a = x, and the
        // predicate is exactly that — no residual filter.
        let p = extract_primary_key_prefix(&eq("a", "x"), &pk()).unwrap();
        assert_eq!(p.values, vec![Value::Text("x".into())]);
        assert!(p.exact);
    }

    #[test]
    fn two_leading_columns_is_exact_prefix() {
        let pred = eq("a", "x").and(eq("b", "y"));
        let p = extract_primary_key_prefix(&pred, &pk()).unwrap();
        assert_eq!(
            p.values,
            vec![Value::Text("x".into()), Value::Text("y".into())]
        );
        assert!(p.exact);
    }

    #[test]
    fn full_primary_key_is_exact_prefix() {
        let pred = eq("a", "x").and(eq("b", "y")).and(eq("c", "z"));
        let p = extract_primary_key_prefix(&pred, &pk()).unwrap();
        assert_eq!(p.values.len(), 3);
        assert!(p.exact);
    }

    #[test]
    fn gap_in_key_columns_yields_inexact_prefix() {
        // `a = x AND c = z` skips `b`: only `a` is a usable leading prefix, and
        // the `c` equality must still be applied as a residual filter.
        let pred = eq("a", "x").and(eq("c", "z"));
        let p = extract_primary_key_prefix(&pred, &pk()).unwrap();
        assert_eq!(p.values, vec![Value::Text("x".into())]);
        assert!(!p.exact);
    }

    #[test]
    fn extra_non_key_equality_is_inexact() {
        let pred = eq("a", "x").and(Expr::Eq("other".into(), Value::Integer(1)));
        let p = extract_primary_key_prefix(&pred, &pk()).unwrap();
        assert_eq!(p.values, vec![Value::Text("x".into())]);
        assert!(!p.exact);
    }

    #[test]
    fn non_leading_column_has_no_prefix() {
        assert!(extract_primary_key_prefix(&eq("b", "y"), &pk()).is_none());
    }

    #[test]
    fn top_level_or_has_no_prefix() {
        let pred = eq("a", "x").or(eq("a", "y"));
        assert!(extract_primary_key_prefix(&pred, &pk()).is_none());
    }

    #[test]
    fn conjoined_non_eq_keeps_prefix_but_marks_residual() {
        // `a = x AND b > y`: the `a` prefix bounds the scan; the range predicate
        // on `b` remains a residual filter.
        let pred = Expr::And(
            Box::new(eq("a", "x")),
            Box::new(Expr::Gt("b".into(), Value::Text("y".into()))),
        );
        let p = extract_primary_key_prefix(&pred, &pk()).unwrap();
        assert_eq!(p.values, vec![Value::Text("x".into())]);
        assert!(!p.exact);
    }

    #[test]
    fn empty_primary_key_has_no_prefix() {
        assert!(extract_primary_key_prefix(&eq("a", "x"), &[]).is_none());
    }
}
