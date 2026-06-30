use crate::catalog::types::Value;
use crate::storage::encoded_key::{EncodedKey, prefix_successor};

#[test]
fn integer_order_is_preserved() {
    let a = EncodedKey::from_single(&Value::Integer(-1));
    let b = EncodedKey::from_single(&Value::Integer(0));
    let c = EncodedKey::from_single(&Value::Integer(42));
    assert!(a < b);
    assert!(b < c);
}

#[test]
fn u64_order_is_preserved() {
    let a = EncodedKey::from_single(&Value::U64(0));
    let b = EncodedKey::from_single(&Value::U64(1));
    let c = EncodedKey::from_single(&Value::U64(42));
    assert!(a < b);
    assert!(b < c);
}

/// Build a big-endian two's-complement I256 from a signed i128.
fn i256(n: i128) -> Value {
    let mut bytes = [0u8; 32];
    let be = n.to_be_bytes(); // 16 bytes
    // Sign-extend into the upper 16 bytes.
    let fill = if n < 0 { 0xFF } else { 0x00 };
    bytes[..16].iter_mut().for_each(|b| *b = fill);
    bytes[16..].copy_from_slice(&be);
    Value::I256(bytes)
}

#[test]
fn i256_signed_order_is_preserved() {
    // Negatives must sort before positives, and within each sign correctly.
    let values = [
        i256(i128::MIN),
        i256(-1_000_000),
        i256(-2),
        i256(-1),
        i256(0),
        i256(1),
        i256(2),
        i256(1_000_000),
        i256(i128::MAX),
    ];
    for window in values.windows(2) {
        let lo = EncodedKey::from_single(&window[0]);
        let hi = EncodedKey::from_single(&window[1]);
        assert!(lo < hi, "expected {:?} < {:?}", window[0], window[1]);
    }
}

#[test]
fn composite_order_is_lexicographic() {
    let a = EncodedKey::from_values(&[Value::Integer(1), Value::Text("a".into())]);
    let b = EncodedKey::from_values(&[Value::Integer(1), Value::Text("b".into())]);
    let c = EncodedKey::from_values(&[Value::Integer(2), Value::Text("a".into())]);
    assert!(a < b);
    assert!(b < c);
}

#[test]
fn prefix_successor_works() {
    let key = EncodedKey::from_bytes(vec![0x10, 0xAA, 0x00]);
    let next = prefix_successor(&key).expect("next");
    assert_eq!(next.as_slice(), &[0x10, 0xAA, 0x01]);
}
