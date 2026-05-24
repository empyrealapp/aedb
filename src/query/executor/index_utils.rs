use crate::catalog::types::Value;
use crate::storage::encoded_key::EncodedKey;
use std::collections::{HashMap, HashSet};

pub(super) fn intersect_pks(left: Vec<EncodedKey>, right: Vec<EncodedKey>) -> Vec<EncodedKey> {
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

pub(super) fn union_pks(left: Vec<EncodedKey>, right: Vec<EncodedKey>) -> Vec<EncodedKey> {
    let mut seen: HashSet<EncodedKey> = HashSet::with_capacity(left.len() + right.len());
    let mut out = Vec::with_capacity(left.len() + right.len());
    for pk in left.into_iter().chain(right) {
        if seen.insert(pk.clone()) {
            out.push(pk);
        }
    }
    out
}

pub(super) fn expr_implied_by_eq_constraints(
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
