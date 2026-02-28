use crate::catalog::types::{Row, Value};
use crate::query::error::QueryError;
use serde::{Deserialize, Serialize};

pub(super) fn extract_sort_key(
    row: &Row,
    sort_indices: &[(usize, crate::query::plan::Order)],
) -> Vec<Value> {
    if sort_indices.is_empty() {
        return row.values.clone();
    }
    sort_indices
        .iter()
        .map(|(idx, _)| row.values[*idx].clone())
        .collect()
}

pub(super) fn extract_pk_key(row: &Row, pk_indices: &[usize]) -> Vec<Value> {
    if pk_indices.is_empty() {
        return row.values.clone();
    }
    pk_indices
        .iter()
        .map(|idx| row.values[*idx].clone())
        .collect()
}

pub(super) fn row_after_cursor(
    row: &Row,
    cursor: &CursorToken,
    sort_indices: &[(usize, crate::query::plan::Order)],
    pk_indices: &[usize],
) -> bool {
    let row_sort = extract_sort_key(row, sort_indices);
    let row_pk = extract_pk_key(row, pk_indices);
    if sort_indices.is_empty() {
        return row_pk > cursor.last_pk;
    }
    for ((_, order), (lhs, rhs)) in sort_indices
        .iter()
        .zip(row_sort.iter().zip(cursor.last_sort_key.iter()))
    {
        let cmp = lhs.cmp(rhs);
        if cmp.is_eq() {
            continue;
        }
        return match order {
            crate::query::plan::Order::Asc => cmp.is_gt(),
            crate::query::plan::Order::Desc => cmp.is_lt(),
        };
    }
    row_pk > cursor.last_pk
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct CursorToken {
    pub(super) snapshot_seq: u64,
    pub(super) last_sort_key: Vec<Value>,
    pub(super) last_pk: Vec<Value>,
    pub(super) page_size: usize,
    pub(super) remaining_limit: Option<usize>,
}

pub(super) fn encode_cursor(cursor: &CursorToken) -> Result<String, QueryError> {
    let bytes = rmp_serde::to_vec(cursor).map_err(|e| QueryError::InternalError(e.to_string()))?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

pub(super) fn decode_cursor(encoded: &str) -> Result<CursorToken, QueryError> {
    let encoded_size_bytes = encoded.len();
    if !encoded_size_bytes.is_multiple_of(2) {
        return Err(QueryError::InvalidQuery {
            reason: "invalid cursor".into(),
        });
    }
    let mut decoded_bytes = Vec::with_capacity(encoded_size_bytes / 2);
    let encoded_bytes = encoded.as_bytes();
    for byte_offset in (0..encoded_bytes.len()).step_by(2) {
        let hi = decode_hex_nibble(encoded_bytes[byte_offset]).ok_or_else(|| {
            QueryError::InvalidQuery {
                reason: "invalid cursor".into(),
            }
        })?;
        let lo = decode_hex_nibble(encoded_bytes[byte_offset + 1]).ok_or_else(|| {
            QueryError::InvalidQuery {
                reason: "invalid cursor".into(),
            }
        })?;
        decoded_bytes.push((hi << 4) | lo);
    }
    rmp_serde::from_slice(&decoded_bytes).map_err(|e| QueryError::InvalidQuery {
        reason: e.to_string(),
    })
}

fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
