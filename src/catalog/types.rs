use compact_str::CompactString;
use serde::de::Deserializer;
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ColumnType {
    Text,
    U8,
    U64,
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
    U8(u8),
    U64(u64),
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

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind_rank().hash(state);
        match self {
            Value::Text(v) => v.hash(state),
            Value::U8(v) => v.hash(state),
            Value::U64(v) => v.hash(state),
            Value::Integer(v) => v.hash(state),
            Value::Float(v) => v.to_bits().hash(state),
            Value::Boolean(v) => v.hash(state),
            Value::U256(v) => v.hash(state),
            Value::I256(v) => v.hash(state),
            Value::Blob(v) => v.hash(state),
            Value::Timestamp(v) => v.hash(state),
            Value::Json(v) => v.hash(state),
            Value::Null => {}
        }
    }
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
            Value::U8(_) => 2,
            Value::U64(_) => 3,
            Value::Integer(_) => 4,
            Value::U256(_) => 5,
            Value::I256(_) => 6,
            Value::Timestamp(_) => 7,
            Value::Float(_) => 8,
            Value::Text(_) => 9,
            Value::Json(_) => 10,
            Value::Blob(_) => 11,
        }
    }
}

/// Order two big-endian two's-complement 256-bit signed integers.
///
/// A raw byte compare is wrong for signed values: negatives (high bit set) would
/// sort *after* positives. We branch on the sign bit; within the same sign an
/// unsigned byte compare already yields the correct signed order (e.g. for two
/// negatives, `-1 = 0xFF..FF` compares greater than `-2 = 0xFF..FE`, and `-1 >
/// -2`). This mirrors the sign-flipped I256 encoding in
/// [`crate::storage::encoded_key`] so the in-memory order and the index
/// range-scan order agree.
fn cmp_i256_be(a: &[u8; 32], b: &[u8; 32]) -> Ordering {
    match (a[0] & 0x80 != 0, b[0] & 0x80 != 0) {
        (false, true) => Ordering::Greater,
        (true, false) => Ordering::Less,
        _ => a.cmp(b),
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
            (Value::U8(a), Value::U8(b)) => a.cmp(b),
            (Value::U64(a), Value::U64(b)) => a.cmp(b),
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::U256(a), Value::U256(b)) => a.cmp(b),
            (Value::I256(a), Value::I256(b)) => cmp_i256_be(a, b),
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
#[path = "types_tests.rs"]
mod types_tests;
