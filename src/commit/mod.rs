pub mod action;
pub mod apply;
pub mod assertions;
pub mod executor;
pub mod row_change;
pub mod tx;
pub mod validation;

use crate::catalog::types::{Row, Value};
use crate::error::AedbError;

/// Per-envelope aggregate read-byte budget.
///
/// Threaded through mutation and assertion evaluation so the cost of all
/// reads (RMW scans, sum/count/value-compare scans) within a single envelope
/// is bounded. Enforced incrementally so a malicious envelope cannot drain
/// CPU by reading many bytes and only being rejected at the end.
#[derive(Debug)]
pub struct ReadByteBudget {
    bytes_consumed: usize,
    cap: usize,
}

impl ReadByteBudget {
    pub fn new(cap: usize) -> Self {
        Self {
            bytes_consumed: 0,
            cap,
        }
    }

    /// Charge `bytes` against the budget. Returns an error if the running
    /// total would exceed the cap.
    pub fn charge(&mut self, bytes: usize) -> Result<(), AedbError> {
        let next = self.bytes_consumed.saturating_add(bytes);
        if next > self.cap {
            return Err(AedbError::Validation(format!(
                "envelope exceeds max_read_bytes_per_envelope: bytes_consumed={next}, max_read_bytes_per_envelope={}",
                self.cap
            )));
        }
        self.bytes_consumed = next;
        Ok(())
    }

    pub fn consumed(&self) -> usize {
        self.bytes_consumed
    }

    pub fn cap(&self) -> usize {
        self.cap
    }
}

/// Estimated heap+stack byte cost of a `Value`. Used by the read-byte budget
/// to keep cost accounting independent of any specific encoding.
pub fn value_byte_size(value: &Value) -> usize {
    use std::mem::size_of;
    let variant = size_of::<Value>();
    let heap = match value {
        Value::Text(s) => s.len(),
        Value::Json(s) => s.len(),
        Value::Blob(v) => v.len(),
        Value::U256(_) | Value::I256(_) => 0, // stored inline in the variant
        Value::U8(_)
        | Value::U64(_)
        | Value::Integer(_)
        | Value::Float(_)
        | Value::Boolean(_)
        | Value::Timestamp(_)
        | Value::Null => 0,
    };
    variant + heap
}

/// Estimated byte cost of a `Row`, summing per-column costs.
pub fn row_byte_size(row: &Row) -> usize {
    let mut total = std::mem::size_of::<Row>();
    for v in &row.values {
        total = total.saturating_add(value_byte_size(v));
    }
    total
}
