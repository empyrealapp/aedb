use crate::commit::validation::Mutation;
use crate::error::AedbError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MigrationRecord {
    pub version: u64,
    pub name: String,
    pub project_id: String,
    pub scope_id: String,
    pub applied_at_micros: u64,
    pub applied_seq: u64,
    pub checksum_hex: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Migration {
    pub version: u64,
    pub name: String,
    pub project_id: String,
    pub scope_id: String,
    pub mutations: Vec<Mutation>,
    #[serde(default)]
    pub down_mutations: Option<Vec<Mutation>>,
}

pub fn migration_key(version: u64) -> Vec<u8> {
    format!("__migrations/{version:020}").into_bytes()
}

pub fn encode_record(record: &MigrationRecord) -> Result<Vec<u8>, AedbError> {
    serde_json::to_vec(record).map_err(|e| AedbError::Encode(e.to_string()))
}

pub fn decode_record(bytes: &[u8]) -> Result<MigrationRecord, AedbError> {
    serde_json::from_slice(bytes).map_err(|e| AedbError::Decode(e.to_string()))
}

pub fn checksum_hex(migration: &Migration) -> Result<String, AedbError> {
    let bytes = rmp_serde::to_vec(migration).map_err(|e| AedbError::Encode(e.to_string()))?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    Ok(hex::encode(hasher.finalize()))
}
