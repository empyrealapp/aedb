use crate::catalog::Catalog;
use crate::commit::tx::{IdempotencyKey, IdempotencyRecord};
use crate::error::AedbError;
use crate::storage::keyspace::KeyspaceSnapshot;
use aes_gcm::aead::rand_core::RngCore;
use aes_gcm::aead::{Aead, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Nonce};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::NamedTempFile;

/// Upper bound on the number of zstd worker threads spawned for a single
/// checkpoint compression. Checkpointing is periodic and runs on a blocking
/// task, so a few workers materially cut wall-clock on large databases without
/// monopolizing the host. zstd internally falls back to single-threaded work
/// when the payload is too small to split, so this is safe for tiny snapshots.
const MAX_CHECKPOINT_COMPRESSION_WORKERS: u32 = 4;

fn resolve_compression_workers() -> u32 {
    std::thread::available_parallelism()
        .map(|n| (n.get() as u32).min(MAX_CHECKPOINT_COMPRESSION_WORKERS))
        .unwrap_or(1)
        .max(1)
}

/// `Write` adapter that tees every byte into one or two SHA-256 hashers while
/// forwarding to the inner writer. Lets the streaming compressor compute the
/// payload digests in a single pass without buffering the whole compressed
/// blob in memory.
struct HashingWriter<W: Write> {
    inner: W,
    body: Sha256,
    payload: Sha256,
}

impl<W: Write> HashingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            body: Sha256::new(),
            payload: Sha256::new(),
        }
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.body.update(&buf[..n]);
        self.payload.update(&buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

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
    write_checkpoint_with_key(snapshot, catalog, seq, dir, None, None, HashMap::new(), 3)
}

#[allow(clippy::too_many_arguments)]
pub fn write_checkpoint_with_key(
    snapshot: &KeyspaceSnapshot,
    catalog: &Catalog,
    seq: u64,
    dir: &Path,
    encryption_key: Option<&[u8; 32]>,
    key_id: Option<String>,
    idempotency: HashMap<IdempotencyKey, IdempotencyRecord>,
    compression_level: i32,
) -> Result<CheckpointMeta, AedbError> {
    fs::create_dir_all(dir)?;
    let snapshot = snapshot.materialized_for_checkpoint()?;
    let checkpoint = CheckpointData {
        seq,
        keyspace: snapshot,
        catalog: catalog.clone(),
        idempotency,
    };

    let created_at_micros = now_micros();
    let filename = format!("checkpoint_{seq:016}.aedb.zst");
    let final_path = dir.join(&filename);
    let mut tmp = NamedTempFile::new_in(dir)?;

    let payload_hash = if let Some(key) = encryption_key {
        // AES-GCM is one-shot and needs the full plaintext, so the encrypted
        // path still buffers the compressed body in memory. We avoid the
        // separate uncompressed `encoded` buffer by streaming straight into the
        // compressed `Vec`.
        let mut compressed = Vec::new();
        compress_checkpoint(&checkpoint, &mut compressed, compression_level)?;
        let payload = encrypt_checkpoint_payload(&compressed, key)?;
        tmp.write_all(&payload)?;
        Sha256::digest(&payload).to_vec()
    } else {
        // Stream serialize -> zstd -> hash -> temp file so peak memory stays
        // near a single compressed copy instead of buffering the encoded body,
        // the compressed body, and a concatenated payload all at once.
        let file = tmp.as_file().try_clone()?;
        let writer = HashingWriter::new(BufWriter::new(file));
        let mut writer = compress_checkpoint(&checkpoint, writer, compression_level)?;
        // Trailer = SHA-256 of the compressed body only. Snapshot the body
        // hasher before appending the trailer so it covers exactly the body.
        let body_hash = writer.body.clone().finalize();
        // Writing the trailer through `HashingWriter` also folds it into the
        // payload hasher, so the final payload digest covers body || trailer.
        writer.write_all(&body_hash)?;
        writer.flush()?;
        writer.payload.clone().finalize().to_vec()
    };

    tmp.as_file().sync_all()?;
    tmp.persist(&final_path)
        .map_err(|e| AedbError::Io(e.error))?;
    // Make the rename itself durable so the checkpoint file is recoverable on
    // its own, without depending on a later manifest write to fsync the
    // directory. Without this, a crash after the rename but before any
    // subsequent directory fsync could lose the just-created checkpoint.
    crate::manifest::atomic::fsync_dir(dir)?;

    Ok(CheckpointMeta {
        filename,
        seq,
        sha256_hex: hex_string(&payload_hash),
        created_at_micros,
        key_id,
    })
}

/// Serializes `checkpoint` as MessagePack and streams it through a
/// (optionally multithreaded) zstd encoder into `writer`, returning the writer.
fn compress_checkpoint<W: Write>(
    checkpoint: &CheckpointData,
    writer: W,
    compression_level: i32,
) -> Result<W, AedbError> {
    let mut encoder = zstd::stream::Encoder::new(writer, compression_level)
        .map_err(|e| AedbError::Io(std::io::Error::other(e.to_string())))?;
    let workers = resolve_compression_workers();
    if workers > 1 {
        // Best-effort: if the linked zstd lacks multithread support this errors
        // and we transparently fall back to single-threaded compression.
        let _ = encoder.multithread(workers);
    }
    rmp_serde::encode::write(&mut encoder, checkpoint)
        .map_err(|e| AedbError::Encode(e.to_string()))?;
    encoder
        .finish()
        .map_err(|e| AedbError::Io(std::io::Error::other(e.to_string())))
}

fn encrypt_checkpoint_payload(plaintext: &[u8], key: &[u8; 32]) -> Result<Vec<u8>, AedbError> {
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| AedbError::Validation(format!("invalid encryption key: {e}")))?;

    let mut nonce_bytes = [0u8; 12];
    OsRng.fill_bytes(&mut nonce_bytes);

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
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}
