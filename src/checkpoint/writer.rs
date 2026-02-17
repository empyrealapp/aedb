use crate::catalog::Catalog;
use crate::commit::tx::{IdempotencyKey, IdempotencyRecord};
use crate::error::AedbError;
use crate::storage::keyspace::KeyspaceSnapshot;
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::NamedTempFile;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointMeta {
    pub filename: String,
    pub seq: u64,
    pub sha256_hex: String,
    pub created_at_micros: u64,
    pub key_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct CheckpointData {
    pub seq: u64,
    pub keyspace: KeyspaceSnapshot,
    pub catalog: Catalog,
    #[serde(default)]
    pub idempotency: HashMap<IdempotencyKey, IdempotencyRecord>,
}

pub fn write_checkpoint(
    snapshot: &KeyspaceSnapshot,
    catalog: &Catalog,
    seq: u64,
    dir: &Path,
) -> Result<CheckpointMeta, AedbError> {
    write_checkpoint_with_key(snapshot, catalog, seq, dir, None, None, HashMap::new())
}

pub fn write_checkpoint_with_key(
    snapshot: &KeyspaceSnapshot,
    catalog: &Catalog,
    seq: u64,
    dir: &Path,
    encryption_key: Option<&[u8; 32]>,
    key_id: Option<String>,
    idempotency: HashMap<IdempotencyKey, IdempotencyRecord>,
) -> Result<CheckpointMeta, AedbError> {
    fs::create_dir_all(dir)?;
    let checkpoint = CheckpointData {
        seq,
        keyspace: snapshot.clone(),
        catalog: catalog.clone(),
        idempotency,
    };
    let encoded = rmp_serde::to_vec(&checkpoint).map_err(|e| AedbError::Encode(e.to_string()))?;
    let compressed = zstd::stream::encode_all(encoded.as_slice(), 3)
        .map_err(|e| AedbError::Io(std::io::Error::other(e.to_string())))?;

    let created_at_micros = now_micros();
    let payload = if let Some(key) = encryption_key {
        encrypt_checkpoint_payload(&compressed, key, seq, created_at_micros)?
    } else {
        let hash = Sha256::digest(&compressed);
        let mut out = compressed.clone();
        out.extend_from_slice(&hash);
        out
    };

    let hash = Sha256::digest(&payload);
    let filename = format!("checkpoint_{seq:016}.aedb.zst");
    let final_path = dir.join(&filename);
    let mut tmp = NamedTempFile::new_in(dir)?;
    tmp.write_all(&payload)?;
    tmp.flush()?;
    tmp.as_file().sync_all()?;
    tmp.persist(&final_path)
        .map_err(|e| AedbError::Io(e.error))?;

    Ok(CheckpointMeta {
        filename,
        seq,
        sha256_hex: hex_string(hash.as_slice()),
        created_at_micros,
        key_id,
    })
}

fn encrypt_checkpoint_payload(
    plaintext: &[u8],
    key: &[u8; 32],
    seq: u64,
    created_at_micros: u64,
) -> Result<Vec<u8>, AedbError> {
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| AedbError::Validation(format!("invalid encryption key: {e}")))?;

    // Construct 96-bit nonce from seq (64-bit) + timestamp_hash (32-bit)
    // Use full timestamp in hash to avoid collision if same seq created far apart in time
    let mut nonce_bytes = [0u8; 12];
    nonce_bytes[..8].copy_from_slice(&seq.to_be_bytes());

    // Hash the full timestamp to get 32 bits, avoiding truncation collision risk
    let timestamp_hash = Sha256::digest(created_at_micros.to_be_bytes());
    nonce_bytes[8..].copy_from_slice(&timestamp_hash[..4]);

    let nonce = Nonce::from_slice(&nonce_bytes);
    let ciphertext = cipher
        .encrypt(nonce, plaintext)
        .map_err(|e| AedbError::Validation(format!("checkpoint encryption failed: {e}")))?;
    let mut out = Vec::with_capacity(8 + nonce_bytes.len() + ciphertext.len());
    out.extend_from_slice(b"AEDBENC1");
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

fn hex_string(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}
