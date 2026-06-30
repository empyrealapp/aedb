use crate::catalog::types::{Row, Value};
use proptest::prelude::*;

fn arb_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        any::<bool>().prop_map(Value::Boolean),
        any::<u8>().prop_map(Value::U8),
        any::<u64>().prop_map(Value::U64),
        any::<i64>().prop_map(Value::Integer),
        prop::array::uniform32(any::<u8>()).prop_map(Value::U256),
        prop::array::uniform32(any::<u8>()).prop_map(Value::I256),
        any::<i64>().prop_map(Value::Timestamp),
        any::<f64>()
            .prop_filter("finite float only", |v| v.is_finite())
            .prop_map(Value::Float),
        "\\PC{0,32}".prop_map(|s| Value::Text(s.into())),
        "\\PC{0,32}".prop_map(|s| Value::Json(s.into())),
        prop::collection::vec(any::<u8>(), 0..64).prop_map(Value::Blob),
        Just(Value::Null),
    ]
}

fn encode<T: serde::Serialize>(value: &T) -> Vec<u8> {
    rmp_serde::to_vec(value).expect("encode should succeed")
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> T {
    rmp_serde::from_slice(bytes).expect("decode should succeed")
}

proptest! {
    #[test]
    fn roundtrip_value(v in arb_value()) {
        let bytes = encode(&v);
        let decoded: Value = decode(&bytes);
        prop_assert_eq!(v, decoded);
    }

    #[test]
    fn roundtrip_row(values in prop::collection::vec(arb_value(), 0..32)) {
        let row = Row::from_values(values);
        let bytes = encode(&row);
        let decoded: Row = decode(&bytes);
        prop_assert_eq!(row, decoded);
    }

    #[test]
    fn ordering_stable(a in arb_value(), b in arb_value()) {
        let orig = a.cmp(&b);
        let a2: Value = decode(&encode(&a));
        let b2: Value = decode(&encode(&b));
        let decoded_cmp = a2.cmp(&b2);
        prop_assert_eq!(orig, decoded_cmp);
    }

    /// `Value::cmp` (full-scan order) must agree with the `EncodedKey` byte order
    /// (index range-scan order) for I256, including negatives. This invariant
    /// keeps the scan and index paths consistent.
    #[test]
    fn i256_value_order_matches_encoded_key_order(
        a in prop::array::uniform32(any::<u8>()),
        b in prop::array::uniform32(any::<u8>()),
    ) {
        use crate::storage::encoded_key::EncodedKey;
        let (va, vb) = (Value::I256(a), Value::I256(b));
        let value_cmp = va.cmp(&vb);
        let key_cmp = EncodedKey::from_single(&va).cmp(&EncodedKey::from_single(&vb));
        prop_assert_eq!(value_cmp, key_cmp);
    }
}

#[test]
fn i256_orders_negatives_before_positives() {
    let neg_one = Value::I256([0xFF; 32]); // -1
    let mut neg_two = [0xFFu8; 32];
    neg_two[31] = 0xFE; // -2
    let neg_two = Value::I256(neg_two);
    let zero = Value::I256([0x00; 32]);
    let mut one = [0x00u8; 32];
    one[31] = 0x01;
    let one = Value::I256(one);

    assert!(neg_two < neg_one, "-2 < -1");
    assert!(neg_one < zero, "-1 < 0");
    assert!(zero < one, "0 < 1");
}

#[test]
fn u256_i256_use_msgpack_ext_markers() {
    let u_bytes = encode(&Value::U256([7u8; 32]));
    let i_bytes = encode(&Value::I256([9u8; 32]));
    assert!(u_bytes.contains(&0xC7), "u256 should contain ext8 marker");
    assert!(i_bytes.contains(&0xC7), "i256 should contain ext8 marker");
}
