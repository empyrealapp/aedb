use crate::catalog::types::Value;
use crate::query::error::QueryError;
use crate::query::executor::cursor::{CursorToken, decode_cursor, encode_cursor};

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
    // Flip the final hex nibble. This corrupts the trailing HMAC tag and
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
    // Flip a byte in the payload region before the trailing 32-byte tag.
    let mut bytes = encoded.into_bytes();
    let target = 4;
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
