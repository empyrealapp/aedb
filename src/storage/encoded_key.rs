use crate::catalog::types::Value;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EncodedKey {
    bytes: SmallVec<[u8; 64]>,
}

impl EncodedKey {
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self {
            bytes: bytes.into(),
        }
    }

    pub fn from_values(values: &[Value]) -> Self {
        let mut out = SmallVec::<[u8; 64]>::new();
        for value in values {
            encode_value(value, &mut out);
        }
        Self { bytes: out }
    }

    pub fn from_single(value: &Value) -> Self {
        Self::from_values(std::slice::from_ref(value))
    }
}

pub fn prefix_successor(prefix: &EncodedKey) -> Option<EncodedKey> {
    let mut next = prefix.bytes.clone();
    for i in (0..next.len()).rev() {
        if next[i] != 0xFF {
            next[i] += 1;
            next.truncate(i + 1);
            return Some(EncodedKey { bytes: next });
        }
    }
    None
}

fn encode_value(v: &Value, out: &mut SmallVec<[u8; 64]>) {
    match v {
        Value::Integer(i) => {
            out.push(0x10);
            let shifted = (*i as u64) ^ 0x8000_0000_0000_0000;
            out.extend_from_slice(&shifted.to_be_bytes());
        }
        Value::Timestamp(ts) => {
            out.push(0x11);
            let shifted = (*ts as u64) ^ 0x8000_0000_0000_0000;
            out.extend_from_slice(&shifted.to_be_bytes());
        }
        Value::U256(bytes) => {
            out.push(0x12);
            out.extend_from_slice(bytes);
        }
        Value::I256(bytes) => {
            out.push(0x13);
            out.extend_from_slice(bytes);
        }
        Value::Text(s) => {
            out.push(0x14);
            append_text(s, out);
        }
        Value::Json(s) => {
            out.push(0x15);
            append_text(s, out);
        }
        Value::Boolean(b) => {
            out.push(0x16);
            out.push(if *b { 1 } else { 0 });
        }
        Value::Float(f) => {
            out.push(0x17);
            // total order preserving float encoding:
            // https://www.rfc-editor.org/rfc/rfc7049#section-3.9 style approach.
            let bits = f.to_bits();
            let mapped = if (bits >> 63) == 1 {
                !bits
            } else {
                bits ^ 0x8000_0000_0000_0000
            };
            out.extend_from_slice(&mapped.to_be_bytes());
        }
        Value::Blob(b) => {
            out.push(0x18);
            out.extend_from_slice(&(b.len() as u32).to_be_bytes());
            out.extend_from_slice(b);
        }
        Value::Null => {
            out.push(0xFF);
        }
    }
}

fn append_text(s: &str, out: &mut SmallVec<[u8; 64]>) {
    for byte in s.as_bytes() {
        if *byte == 0 {
            // Escape interior nulls so terminator remains unambiguous.
            out.extend_from_slice(&[0x00, 0xFF]);
        } else {
            out.push(*byte);
        }
    }
    out.push(0x00);
}

#[cfg(test)]
mod tests {
    use super::{EncodedKey, prefix_successor};
    use crate::catalog::types::Value;

    #[test]
    fn integer_order_is_preserved() {
        let a = EncodedKey::from_single(&Value::Integer(-1));
        let b = EncodedKey::from_single(&Value::Integer(0));
        let c = EncodedKey::from_single(&Value::Integer(42));
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
}
