use crate::catalog::Catalog;
use crate::checkpoint::writer::CheckpointData;
use crate::commit::tx::{IdempotencyKey, IdempotencyRecord};
use crate::error::AedbError;
use crate::storage::keyspace::Keyspace;
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::Path;

/// Upper bound on decompressed checkpoint size. A checkpoint is fully
/// materialized in memory when loaded, so any realistic checkpoint is far below
/// this; the cap exists only to stop a maliciously crafted (highly compressible)
/// checkpoint from expanding without limit and exhausting memory on recovery.
const MAX_CHECKPOINT_DECOMPRESSED_BYTES: u64 = 64 * 1024 * 1024 * 1024;

pub fn load_checkpoint(
    path: &Path,
) -> Result<
    (
        Keyspace,
        Catalog,
        u64,
        HashMap<IdempotencyKey, IdempotencyRecord>,
    ),
    AedbError,
> {
    load_checkpoint_with_key(path, None)
}

pub fn load_checkpoint_with_key(
    path: &Path,
    encryption_key: Option<&[u8; 32]>,
) -> Result<
    (
        Keyspace,
        Catalog,
        u64,
        HashMap<IdempotencyKey, IdempotencyRecord>,
    ),
    AedbError,
> {
    let bytes = fs::read(path)?;
    let compressed: Cow<'_, [u8]> = if bytes.starts_with(b"AEDBENC1") {
        let key = encryption_key
            .ok_or_else(|| AedbError::Validation("checkpoint requires key".into()))?;
        Cow::Owned(decrypt_checkpoint_payload(&bytes, key)?)
    } else {
        if bytes.len() < 32 {
            return Err(AedbError::Decode("checkpoint too small".into()));
        }
        let (compressed, trailer_hash) = bytes.split_at(bytes.len() - 32);
        let actual = Sha256::digest(compressed);
        if actual.as_slice() != trailer_hash {
            return Err(AedbError::Validation("checkpoint hash mismatch".into()));
        }
        Cow::Borrowed(compressed)
    };
    let decoder = zstd::stream::Decoder::new(compressed.as_ref())
        .map_err(|e| AedbError::Io(std::io::Error::other(e.to_string())))?;
    let mut limited = decoder.take(MAX_CHECKPOINT_DECOMPRESSED_BYTES);
    let data: CheckpointData =
        rmp_serde::from_read(&mut limited).map_err(|e| AedbError::Decode(e.to_string()))?;
    let mut keyspace = Keyspace {
        primary_index_backend: data.keyspace.primary_index_backend,
        value_store: None,
        kv_segment_store: None,
        persistent_value_inline_threshold_bytes: usize::MAX,
        namespaces: data.keyspace.namespaces,
        async_indexes: data.keyspace.async_indexes,
        mem_bytes: 0,
    };
    keyspace.refresh_mem_bytes();
    Ok((keyspace, data.catalog, data.seq, data.idempotency))
}

fn decrypt_checkpoint_payload(bytes: &[u8], key: &[u8; 32]) -> Result<Vec<u8>, AedbError> {
    const ENCRYPTED_MAGIC_SIZE_BYTES: usize = 8;
    const NONCE_SIZE_BYTES: usize = 12;
    let encrypted_header_size_bytes = ENCRYPTED_MAGIC_SIZE_BYTES + NONCE_SIZE_BYTES;
    if bytes.len() < encrypted_header_size_bytes {
        return Err(AedbError::Decode("encrypted checkpoint too small".into()));
    }
    let nonce_offset_bytes = ENCRYPTED_MAGIC_SIZE_BYTES;
    let ciphertext_offset_bytes = encrypted_header_size_bytes;
    debug_assert!(ciphertext_offset_bytes <= bytes.len());
    let nonce = Nonce::from_slice(&bytes[nonce_offset_bytes..ciphertext_offset_bytes]);
    let ciphertext = &bytes[ciphertext_offset_bytes..];
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| AedbError::Validation(format!("invalid encryption key: {e}")))?;
    cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| AedbError::Validation(format!("checkpoint decryption failed: {e}")))
}

#[cfg(test)]
mod tests;
