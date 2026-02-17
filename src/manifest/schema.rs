use crate::checkpoint::writer::CheckpointMeta;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentMeta {
    pub filename: String,
    pub segment_seq: u64,
    #[serde(default)]
    pub sha256_hex: String,
    #[serde(default)]
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct Manifest {
    pub durable_seq: u64,
    pub visible_seq: u64,
    pub active_segment_seq: u64,
    pub checkpoints: Vec<CheckpointMeta>,
    pub segments: Vec<SegmentMeta>,
}
