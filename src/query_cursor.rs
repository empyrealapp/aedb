use crate::catalog::types::Value;
use crate::error::AedbError;
use subtle::ConstantTimeEq;

pub(crate) fn parse_cursor_seq(cursor: &str) -> Result<u64, AedbError> {
    if cursor.len() < 2 {
        return Err(AedbError::Decode("invalid cursor".into()));
    }
    let encoded = cursor.as_bytes();
    if !encoded.len().is_multiple_of(2) {
        return Err(AedbError::Decode("invalid cursor".into()));
    }
    let mut bytes = Vec::with_capacity(encoded.len() / 2);
    for chunk in encoded.chunks_exact(2) {
        let hi = decode_hex_nibble(chunk[0])
            .ok_or_else(|| AedbError::Decode("invalid cursor".into()))?;
        let lo = decode_hex_nibble(chunk[1])
            .ok_or_else(|| AedbError::Decode("invalid cursor".into()))?;
        bytes.push((hi << 4) | lo);
    }
    const INNER_HMAC_TAG_LEN: usize = 32;

    #[derive(serde::Deserialize)]
    struct CursorSeq {
        snapshot_seq: u64,
    }

    #[derive(serde::Deserialize)]
    struct CursorToken {
        snapshot_seq: u64,
        last_sort_key: Vec<Value>,
        last_pk: Vec<Value>,
        page_size: usize,
        remaining_limit: Option<usize>,
    }

    let try_parse = |slice: &[u8]| -> Option<u64> {
        if let Ok(token) = rmp_serde::from_slice::<CursorSeq>(slice) {
            return Some(token.snapshot_seq);
        }
        if let Ok(token) = rmp_serde::from_slice::<CursorToken>(slice) {
            let _ = (
                token.last_sort_key,
                token.last_pk,
                token.page_size,
                token.remaining_limit,
            );
            return Some(token.snapshot_seq);
        }
        None
    };

    if let Some(seq) = try_parse(&bytes) {
        return Ok(seq);
    }
    if bytes.len() > INNER_HMAC_TAG_LEN {
        let trimmed = &bytes[..bytes.len() - INNER_HMAC_TAG_LEN];
        if let Some(seq) = try_parse(trimmed) {
            return Ok(seq);
        }
    }
    Err(AedbError::Decode("invalid cursor".into()))
}

fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SignedQueryCursor {
    version: u8,
    raw_cursor: String,
    mac: [u8; 32],
}

pub(crate) fn sign_query_cursor(cursor: &str, key: &[u8; 32]) -> Result<String, AedbError> {
    let payload = SignedQueryCursor {
        version: 1,
        raw_cursor: cursor.to_string(),
        mac: *blake3::keyed_hash(key, cursor.as_bytes()).as_bytes(),
    };
    let bytes = rmp_serde::to_vec(&payload).map_err(|e| AedbError::Encode(e.to_string()))?;
    Ok(hex::encode(bytes))
}

pub(crate) fn verify_signed_query_cursor(
    cursor: &str,
    key: &[u8; 32],
) -> Result<(String, u64), AedbError> {
    let bytes = hex::decode(cursor).map_err(|_| AedbError::Decode("invalid cursor".into()))?;
    let payload: SignedQueryCursor =
        rmp_serde::from_slice(&bytes).map_err(|_| AedbError::Decode("invalid cursor".into()))?;
    if payload.version != 1 {
        return Err(AedbError::Decode("invalid cursor".into()));
    }
    let expected = *blake3::keyed_hash(key, payload.raw_cursor.as_bytes()).as_bytes();
    if payload.mac.ct_eq(&expected).unwrap_u8() != 1 {
        return Err(AedbError::Validation("cursor signature mismatch".into()));
    }
    let snapshot_seq = parse_cursor_seq(&payload.raw_cursor)?;
    Ok((payload.raw_cursor, snapshot_seq))
}
