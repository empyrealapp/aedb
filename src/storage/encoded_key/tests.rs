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
