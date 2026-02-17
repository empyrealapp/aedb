pub mod error;
pub mod executor;
pub mod operators;
pub mod plan;
pub mod planner;

use crate::storage::keyspace::KvEntry;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvCursor {
    pub snapshot_seq: u64,
    pub last_key: Vec<u8>,
    pub page_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvScanResult {
    pub entries: Vec<(Vec<u8>, KvEntry)>,
    pub cursor: Option<KvCursor>,
    pub snapshot_seq: u64,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedKvEntry {
    pub scope_id: String,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub version: u64,
}
