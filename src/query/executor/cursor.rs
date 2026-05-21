use crate::catalog::types::{Row, Value};
use crate::query::error::QueryError;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Length in bytes of the HMAC-SHA256 tag appended to signed cursor payloads.
const HMAC_TAG_LEN: usize = 32;

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

/// Encode a `CursorToken` to a hex string.
///
/// When `signing_key` is `Some`, the msgpack payload is suffixed with an
/// HMAC-SHA256 tag computed over the payload bytes, then the concatenation is
/// hex-encoded. When `None`, the legacy unsigned `hex(rmp_serde(token))`
/// format is produced so embedders that have not opted into signing remain
/// compatible.
pub(super) fn encode_cursor(
    cursor: &CursorToken,
    signing_key: Option<&[u8; 32]>,
) -> Result<String, QueryError> {
    let payload =
        rmp_serde::to_vec(cursor).map_err(|e| QueryError::InternalError(e.to_string()))?;
    let mut buf = payload;
    if let Some(key) = signing_key {
        let mut mac = HmacSha256::new_from_slice(key)
            .map_err(|e| QueryError::InternalError(e.to_string()))?;
        mac.update(&buf);
        let tag = mac.finalize().into_bytes();
        buf.extend_from_slice(&tag);
    }
    Ok(buf.iter().map(|b| format!("{b:02x}")).collect())
}

/// Decode a `CursorToken` from a hex string.
///
/// When `signing_key` is `Some`, the trailing 32 bytes are interpreted as an
/// HMAC-SHA256 tag and verified in constant time against the payload prefix.
/// When `None`, the entire decoded payload is treated as msgpack (legacy
/// unsigned format).
///
/// Any decoding or verification failure surfaces as `QueryError::InvalidCursor`
/// so callers cannot distinguish tampered from malformed input.
pub(super) fn decode_cursor(
    encoded: &str,
    signing_key: Option<&[u8; 32]>,
) -> Result<CursorToken, QueryError> {
    let encoded_size_bytes = encoded.len();
    if !encoded_size_bytes.is_multiple_of(2) {
        return Err(QueryError::InvalidCursor);
    }
    let mut decoded_bytes = Vec::with_capacity(encoded_size_bytes / 2);
    let encoded_bytes = encoded.as_bytes();
    for byte_offset in (0..encoded_bytes.len()).step_by(2) {
        let hi = decode_hex_nibble(encoded_bytes[byte_offset]).ok_or(QueryError::InvalidCursor)?;
        let lo =
            decode_hex_nibble(encoded_bytes[byte_offset + 1]).ok_or(QueryError::InvalidCursor)?;
        decoded_bytes.push((hi << 4) | lo);
    }
    let payload_slice: &[u8] = if let Some(key) = signing_key {
        if decoded_bytes.len() < HMAC_TAG_LEN {
            return Err(QueryError::InvalidCursor);
        }
        let split_at = decoded_bytes.len() - HMAC_TAG_LEN;
        let (payload, tag) = decoded_bytes.split_at(split_at);
        let mut mac = HmacSha256::new_from_slice(key).map_err(|_| QueryError::InvalidCursor)?;
        mac.update(payload);
        mac.verify_slice(tag)
            .map_err(|_| QueryError::InvalidCursor)?;
        payload
    } else {
        &decoded_bytes
    };
    rmp_serde::from_slice(payload_slice).map_err(|_| QueryError::InvalidCursor)
}

fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_token() -> CursorToken {
        CursorToken {
            snapshot_seq: 42,
            last_sort_key: vec![Value::Integer(7)],
            last_pk: vec![Value::Integer(99)],
            page_size: 50,
            remaining_limit: Some(150),
        }
    }

    #[test]
    fn round_trip_with_key_succeeds() {
        let key = [0xA5u8; 32];
        let token = sample_token();
        let encoded = encode_cursor(&token, Some(&key)).expect("encode");
        let decoded = decode_cursor(&encoded, Some(&key)).expect("decode");
        assert_eq!(decoded, token);
    }

    #[test]
    fn unsigned_config_round_trips_back_compat() {
        let token = sample_token();
        let encoded = encode_cursor(&token, None).expect("encode");
        let decoded = decode_cursor(&encoded, None).expect("decode");
        assert_eq!(decoded, token);
    }

    #[test]
    fn tampered_tag_is_rejected() {
        let key = [0x11u8; 32];
        let token = sample_token();
        let mut encoded = encode_cursor(&token, Some(&key)).expect("encode");
        // Flip the final hex nibble — this corrupts the trailing HMAC tag and
        // must be rejected without leaking the reason via error variant.
        let mut bytes = encoded.into_bytes();
        let last = bytes.len() - 1;
        bytes[last] = match bytes[last] {
            b'0' => b'1',
            _ => b'0',
        };
        encoded = String::from_utf8(bytes).expect("utf8");
        let err = decode_cursor(&encoded, Some(&key)).expect_err("must reject tampered tag");
        assert!(matches!(err, QueryError::InvalidCursor));
    }

    #[test]
    fn tampered_payload_is_rejected() {
        let key = [0x33u8; 32];
        let token = sample_token();
        let encoded = encode_cursor(&token, Some(&key)).expect("encode");
        // Flip a byte in the payload region (somewhere before the trailing 32-byte tag).
        let mut bytes = encoded.into_bytes();
        let target = 4; // first payload hex char
        bytes[target] = match bytes[target] {
            b'0' => b'1',
            _ => b'0',
        };
        let tampered = String::from_utf8(bytes).expect("utf8");
        let err = decode_cursor(&tampered, Some(&key)).expect_err("must reject tampered payload");
        assert!(matches!(err, QueryError::InvalidCursor));
    }

    #[test]
    fn wrong_key_is_rejected() {
        let key = [0x77u8; 32];
        let other = [0x88u8; 32];
        let token = sample_token();
        let encoded = encode_cursor(&token, Some(&key)).expect("encode");
        let err = decode_cursor(&encoded, Some(&other)).expect_err("must reject wrong key");
        assert!(matches!(err, QueryError::InvalidCursor));
    }

    #[test]
    fn unsigned_cursor_cannot_be_decoded_with_key() {
        let key = [0xEFu8; 32];
        let token = sample_token();
        let encoded = encode_cursor(&token, None).expect("encode");
        let err = decode_cursor(&encoded, Some(&key)).expect_err("signed decode of unsigned");
        assert!(matches!(err, QueryError::InvalidCursor));
    }
}
