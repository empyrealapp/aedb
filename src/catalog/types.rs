use compact_str::CompactString;
use serde::de::Deserializer;
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ColumnType {
    Text,
    Integer,
    Float,
    Boolean,
    U256,
    I256,
    Blob,
    Timestamp,
    Json,
}

#[derive(Serialize, Deserialize)]
#[serde(rename = "_ExtStruct")]
struct MsgpackExt((i8, serde_bytes::ByteBuf));

fn serialize_u256_ext<S>(value: &[u8; 32], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    MsgpackExt((1, serde_bytes::ByteBuf::from(value.to_vec()))).serialize(serializer)
}

fn deserialize_u256_ext<'de, D>(deserializer: D) -> Result<[u8; 32], D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum UPayload {
        Ext(MsgpackExt),
        Raw([u8; 32]),
        Vec(Vec<u8>),
    }
    match UPayload::deserialize(deserializer)? {
        UPayload::Ext(MsgpackExt((tag, bytes))) => {
            if tag != 1 {
                return Err(serde::de::Error::custom("invalid U256 ext tag"));
            }
            let slice = bytes.as_slice();
            if slice.len() != 32 {
                return Err(serde::de::Error::custom("invalid U256 ext length"));
            }
            let mut out = [0u8; 32];
            out.copy_from_slice(slice);
            Ok(out)
        }
        UPayload::Raw(v) => Ok(v),
        UPayload::Vec(v) => {
            if v.len() != 32 {
                return Err(serde::de::Error::custom("invalid U256 length"));
            }
            let mut out = [0u8; 32];
            out.copy_from_slice(&v);
            Ok(out)
        }
    }
}

fn serialize_i256_ext<S>(value: &[u8; 32], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    MsgpackExt((2, serde_bytes::ByteBuf::from(value.to_vec()))).serialize(serializer)
}

fn deserialize_i256_ext<'de, D>(deserializer: D) -> Result<[u8; 32], D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum IPayload {
        Ext(MsgpackExt),
        Raw([u8; 32]),
        Vec(Vec<u8>),
    }
    match IPayload::deserialize(deserializer)? {
        IPayload::Ext(MsgpackExt((tag, bytes))) => {
            if tag != 2 {
                return Err(serde::de::Error::custom("invalid I256 ext tag"));
            }
            let slice = bytes.as_slice();
            if slice.len() != 32 {
                return Err(serde::de::Error::custom("invalid I256 ext length"));
            }
            let mut out = [0u8; 32];
            out.copy_from_slice(slice);
            Ok(out)
        }
        IPayload::Raw(v) => Ok(v),
        IPayload::Vec(v) => {
            if v.len() != 32 {
                return Err(serde::de::Error::custom("invalid I256 length"));
            }
            let mut out = [0u8; 32];
            out.copy_from_slice(&v);
            Ok(out)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Text(CompactString),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    #[serde(
        serialize_with = "serialize_u256_ext",
        deserialize_with = "deserialize_u256_ext"
    )]
    U256([u8; 32]),
    #[serde(
        serialize_with = "serialize_i256_ext",
        deserialize_with = "deserialize_i256_ext"
    )]
    I256([u8; 32]),
    Blob(Vec<u8>),
    Timestamp(i64),
    Json(CompactString),
    Null,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Row {
    pub values: Vec<Value>,
}

impl Row {
    pub fn from_values(values: Vec<Value>) -> Self {
        Self { values }
    }
}

impl Value {
    fn kind_rank(&self) -> u8 {
        match self {
            Value::Null => 0,
            Value::Boolean(_) => 1,
            Value::Integer(_) => 2,
            Value::U256(_) => 3,
            Value::I256(_) => 4,
            Value::Timestamp(_) => 5,
            Value::Float(_) => 6,
            Value::Text(_) => 7,
            Value::Json(_) => 8,
            Value::Blob(_) => 9,
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        let rank_cmp = self.kind_rank().cmp(&other.kind_rank());
        if rank_cmp != Ordering::Equal {
            return rank_cmp;
        }

        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::U256(a), Value::U256(b)) => a.cmp(b),
            (Value::I256(a), Value::I256(b)) => a.cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => a.total_cmp(b),
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Json(a), Value::Json(b)) => a.cmp(b),
            (Value::Blob(a), Value::Blob(b)) => a.cmp(b),
            _ => Ordering::Equal,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Row, Value};
    use proptest::prelude::*;

    fn arb_value() -> impl Strategy<Value = Value> {
        let leaf = prop_oneof![
            any::<bool>().prop_map(Value::Boolean),
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
        ];
        leaf
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
    }

    #[test]
    fn u256_i256_use_msgpack_ext_markers() {
        let u_bytes = encode(&Value::U256([7u8; 32]));
        let i_bytes = encode(&Value::I256([9u8; 32]));
        assert!(u_bytes.contains(&0xC7), "u256 should contain ext8 marker");
        assert!(i_bytes.contains(&0xC7), "i256 should contain ext8 marker");
    }
}
