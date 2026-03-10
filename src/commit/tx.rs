use crate::catalog::types::Value;
use crate::commit::validation::{CompareOp, Mutation};
use crate::error::AedbError;
use crate::permission::CallerContext;
use crate::query::plan::Expr;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct IdempotencyKey(pub [u8; 16]);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReadKey {
    TableRow {
        project_id: String,
        scope_id: String,
        table_name: String,
        primary_key: Vec<crate::catalog::types::Value>,
    },
    KvKey {
        project_id: String,
        scope_id: String,
        key: Vec<u8>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadSetEntry {
    pub key: ReadKey,
    pub version_at_read: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReadBound<T> {
    Unbounded,
    Included(T),
    Excluded(T),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReadRange {
    TableRange {
        project_id: String,
        scope_id: String,
        table_name: String,
        start: ReadBound<Vec<crate::catalog::types::Value>>,
        end: ReadBound<Vec<crate::catalog::types::Value>>,
    },
    KvRange {
        project_id: String,
        scope_id: String,
        start: ReadBound<Vec<u8>>,
        end: ReadBound<Vec<u8>>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadRangeEntry {
    pub range: ReadRange,
    pub max_version_at_read: u64,
    #[serde(default)]
    pub structural_version_at_read: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ReadSet {
    #[serde(default)]
    pub points: Vec<ReadSetEntry>,
    #[serde(default)]
    pub ranges: Vec<ReadRangeEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReadAssertion {
    AccumulatorAvailableAtLeast {
        project_id: String,
        scope_id: String,
        accumulator_name: String,
        min_amount: i64,
    },
    AccumulatorExposureWithinMargin {
        project_id: String,
        scope_id: String,
        accumulator_name: String,
        additional_exposure: i64,
    },
    KeyEquals {
        project_id: String,
        scope_id: String,
        key: Vec<u8>,
        expected: Vec<u8>,
    },
    KeyCompare {
        project_id: String,
        scope_id: String,
        key: Vec<u8>,
        op: CompareOp,
        threshold: Vec<u8>,
    },
    KeyExists {
        project_id: String,
        scope_id: String,
        key: Vec<u8>,
        expected: bool,
    },
    KeyVersion {
        project_id: String,
        scope_id: String,
        key: Vec<u8>,
        expected_seq: u64,
    },
    RowVersion {
        project_id: String,
        scope_id: String,
        table_name: String,
        primary_key: Vec<Value>,
        expected_seq: u64,
    },
    RowExists {
        project_id: String,
        scope_id: String,
        table_name: String,
        primary_key: Vec<Value>,
        expected: bool,
    },
    RowColumnCompare {
        project_id: String,
        scope_id: String,
        table_name: String,
        primary_key: Vec<Value>,
        column: String,
        op: CompareOp,
        threshold: Value,
    },
    CountCompare {
        project_id: String,
        scope_id: String,
        table_name: String,
        filter: Option<Expr>,
        op: CompareOp,
        threshold: u64,
    },
    SumCompare {
        project_id: String,
        scope_id: String,
        table_name: String,
        column: String,
        filter: Option<Expr>,
        op: CompareOp,
        threshold: Value,
    },
    All(Vec<ReadAssertion>),
    Any(Vec<ReadAssertion>),
    Not(Box<ReadAssertion>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AssertionActual {
    Missing,
    Bool(bool),
    Version(u64),
    Bytes(Vec<u8>),
    Value(Value),
    Count(u64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteIntent {
    pub mutations: Vec<Mutation>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum WriteClass {
    Economic,
    #[default]
    Standard,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalCommitPayload {
    pub mutations: Vec<Mutation>,
    pub assertions: Vec<ReadAssertion>,
    pub idempotency_key: Option<IdempotencyKey>,
    #[serde(default)]
    pub request_fingerprint: Option<[u8; 32]>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IdempotencyRecord {
    pub commit_seq: u64,
    pub recorded_at_micros: u64,
    #[serde(default)]
    pub request_fingerprint: Option<[u8; 32]>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionEnvelope {
    pub caller: Option<CallerContext>,
    pub idempotency_key: Option<IdempotencyKey>,
    #[serde(default)]
    pub write_class: WriteClass,
    pub assertions: Vec<ReadAssertion>,
    pub read_set: ReadSet,
    pub write_intent: WriteIntent,
    pub base_seq: u64,
}

#[derive(Serialize)]
struct IdempotencyFingerprintPayload<'a> {
    caller: Option<&'a CallerContext>,
    write_class: WriteClass,
    assertions: &'a [ReadAssertion],
    read_set: &'a ReadSet,
    write_intent: &'a WriteIntent,
    base_seq: u64,
}

impl TransactionEnvelope {
    pub fn request_fingerprint(&self) -> Result<[u8; 32], AedbError> {
        let payload = IdempotencyFingerprintPayload {
            caller: self.caller.as_ref(),
            write_class: self.write_class,
            assertions: &self.assertions,
            read_set: &self.read_set,
            write_intent: &self.write_intent,
            base_seq: self.base_seq,
        };
        let encoded = rmp_serde::to_vec(&payload).map_err(|e| AedbError::Encode(e.to_string()))?;
        Ok(*blake3::hash(&encoded).as_bytes())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreflightPlan {
    pub valid: bool,
    pub read_set: ReadSet,
    pub write_intent: WriteIntent,
    pub base_seq: u64,
    pub estimated_affected_rows: usize,
    pub errors: Vec<String>,
}
