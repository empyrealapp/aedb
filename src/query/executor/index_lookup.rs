use crate::catalog::types::Value;
use crate::query::plan::Expr;
use crate::storage::encoded_key::EncodedKey;
use std::ops::Bound;

pub(super) type IndexBounds = (Bound<EncodedKey>, Bound<EncodedKey>);

pub(super) enum IndexLookup<'a> {
    Range {
        column: &'a str,
        bounds: IndexBounds,
        predicate_exact: bool,
    },
    Eq {
        column: &'a str,
        value: &'a Value,
        predicate_exact: bool,
    },
    In {
        column: &'a str,
        values: &'a [Value],
        predicate_exact: bool,
    },
}

impl IndexLookup<'_> {
    pub(super) fn predicate_exact(&self) -> bool {
        match self {
            Self::Range {
                predicate_exact, ..
            }
            | Self::Eq {
                predicate_exact, ..
            }
            | Self::In {
                predicate_exact, ..
            } => *predicate_exact,
        }
    }
}

pub(super) fn extract_indexable_predicate(predicate: &Expr) -> Option<IndexLookup<'_>> {
    match predicate {
        Expr::Eq(c, v) => Some(IndexLookup::Eq {
            column: c,
            value: v,
            predicate_exact: true,
        }),
        Expr::In(c, values) => Some(IndexLookup::In {
            column: c,
            values,
            predicate_exact: true,
        }),
        Expr::Lt(c, v) => Some(IndexLookup::Range {
            column: c,
            bounds: (
                Bound::Unbounded,
                Bound::Excluded(EncodedKey::from_values(std::slice::from_ref(v))),
            ),
            predicate_exact: true,
        }),
        Expr::Lte(c, v) => Some(IndexLookup::Range {
            column: c,
            bounds: (
                Bound::Unbounded,
                Bound::Included(EncodedKey::from_values(std::slice::from_ref(v))),
            ),
            predicate_exact: true,
        }),
        Expr::Gt(c, v) => Some(IndexLookup::Range {
            column: c,
            bounds: (
                Bound::Excluded(EncodedKey::from_values(std::slice::from_ref(v))),
                Bound::Unbounded,
            ),
            predicate_exact: true,
        }),
        Expr::Gte(c, v) => Some(IndexLookup::Range {
            column: c,
            bounds: (
                Bound::Included(EncodedKey::from_values(std::slice::from_ref(v))),
                Bound::Unbounded,
            ),
            predicate_exact: true,
        }),
        Expr::Between(c, lo, hi) => Some(IndexLookup::Range {
            column: c,
            bounds: (
                Bound::Included(EncodedKey::from_values(std::slice::from_ref(lo))),
                Bound::Included(EncodedKey::from_values(std::slice::from_ref(hi))),
            ),
            predicate_exact: true,
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
                column: c,
                bounds: (start, end),
                predicate_exact: like_prefix_is_exact(pattern),
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

fn like_prefix_is_exact(pattern: &str) -> bool {
    let mut wildcard_seen = false;
    for ch in pattern.chars() {
        match ch {
            '%' => wildcard_seen = true,
            '_' => return false,
            _ if wildcard_seen => return false,
            _ => {}
        }
    }
    wildcard_seen
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
