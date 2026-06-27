use crate::error::AedbError;
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Component, Path, PathBuf};
use tempfile::NamedTempFile;
use uuid::Uuid;

pub const BACKUP_MANIFEST_FILE: &str = "backup_manifest.json";
pub const BACKUP_MANIFEST_HMAC_FILE: &str = "backup_manifest.hmac";

const BACKUP_ARCHIVE_MAGIC: &[u8; 8] = b"AEDBARC1";
const BACKUP_ARCHIVE_FLAG_ENCRYPTED: u8 = 0x01;
const BACKUP_ARCHIVE_ENTRY_FILE: u8 = 0x01;
const BACKUP_ARCHIVE_ENTRY_CHUNKED_FILE: u8 = 0x02;
const BACKUP_ARCHIVE_ENTRY_END: u8 = 0xFF;
const MAX_BACKUP_ARCHIVE_PATH_BYTES: u32 = 4_096;
const MAX_BACKUP_ARCHIVE_PAYLOAD_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const BACKUP_ARCHIVE_CHUNK_BYTES: usize = 4 * 1024 * 1024;
const BACKUP_ARCHIVE_CHUNKED_FILE_THRESHOLD_BYTES: u64 = 8 * 1024 * 1024;
const BACKUP_ARCHIVE_IO_BUFFER_BYTES: usize = 1024 * 1024;

/// Upper bound on zstd worker threads used while compressing archive payloads.
/// Mirrors the checkpoint writer cap so a backup never monopolizes the host.
const MAX_BACKUP_COMPRESSION_WORKERS: u32 = 4;

fn resolve_backup_compression_workers() -> u32 {
    std::thread::available_parallelism()
        .map(|n| (n.get() as u32).min(MAX_BACKUP_COMPRESSION_WORKERS))
        .unwrap_or(1)
        .max(1)
}

/// Compress an in-memory block with zstd at the configured level, using multiple
/// worker threads when the host has spare parallelism. zstd transparently falls
/// back to single-threaded work for blocks too small to split, so this is safe
/// for the per-chunk and small-file paths as well.
fn compress_block(raw: &[u8], compression_level: i32) -> Result<Vec<u8>, AedbError> {
    let mut encoder = zstd::stream::Encoder::new(Vec::new(), compression_level)
        .map_err(|e| AedbError::Encode(e.to_string()))?;
    let workers = resolve_backup_compression_workers();
    if workers > 1 {
        // Best-effort: a build without multithread support just stays serial.
        let _ = encoder.multithread(workers);
    }
    encoder
        .write_all(raw)
        .map_err(|e| AedbError::Encode(e.to_string()))?;
    encoder
        .finish()
        .map_err(|e| AedbError::Encode(e.to_string()))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupManifest {
    pub backup_id: String,
    pub backup_type: String,
    #[serde(default)]
    pub parent_backup_id: Option<String>,
    #[serde(default)]
    pub from_seq: Option<u64>,
    pub created_at_micros: u64,
    pub aedb_version: String,
    pub checkpoint_seq: u64,
    pub wal_head_seq: u64,
    pub checkpoint_file: String,
    pub wal_segments: Vec<String>,
    pub file_sha256: BTreeMap<String, String>,
}

pub fn write_backup_manifest(
    dir: &Path,
    manifest: &BackupManifest,
    signing_key: Option<&[u8]>,
) -> Result<(), AedbError> {
    fs::create_dir_all(dir)?;
    let bytes =
        serde_json::to_vec_pretty(manifest).map_err(|e| AedbError::Encode(e.to_string()))?;
    write_file_atomic_synced(dir, BACKUP_MANIFEST_FILE, &bytes)?;
    if let Some(key) = signing_key {
        let sig = hmac_hex(key, &bytes)?;
        write_file_atomic_synced(dir, BACKUP_MANIFEST_HMAC_FILE, sig.as_bytes())?;
    } else {
        let _ = fs::remove_file(dir.join(BACKUP_MANIFEST_HMAC_FILE));
        sync_dir(dir)?;
    }
    Ok(())
}

pub fn load_backup_manifest(
    dir: &Path,
    signing_key: Option<&[u8]>,
) -> Result<BackupManifest, AedbError> {
    let bytes = fs::read(dir.join(BACKUP_MANIFEST_FILE))?;
    if let Some(key) = signing_key {
        let expected_hex = fs::read_to_string(dir.join(BACKUP_MANIFEST_HMAC_FILE))
            .map_err(|_| AedbError::Validation("backup manifest hmac missing".into()))?;
        verify_hmac_hex(key, &bytes, expected_hex.trim())?;
    }
    let manifest: BackupManifest =
        serde_json::from_slice(&bytes).map_err(|e| AedbError::Decode(e.to_string()))?;
    validate_backup_manifest(&manifest)?;
    Ok(manifest)
}

pub fn verify_backup_files(dir: &Path, manifest: &BackupManifest) -> Result<(), AedbError> {
    for (rel, expected) in &manifest.file_sha256 {
        if !is_valid_sha256_hex(expected) {
            return Err(AedbError::Validation(format!(
                "invalid sha256 entry in backup manifest for path: {rel}"
            )));
        }
        let resolved = resolve_backup_path(dir, rel)?;
        let actual = sha256_file_hex(&resolved)?;
        if &actual != expected {
            return Err(AedbError::Validation(format!(
                "backup file checksum mismatch: {rel}"
            )));
        }
    }
    Ok(())
}

pub fn write_backup_archive(
    dir: &Path,
    archive_path: &Path,
    encryption_key: Option<&[u8; 32]>,
    compression_level: i32,
) -> Result<(), AedbError> {
    if !dir.exists() {
        return Err(AedbError::Validation(
            "backup source directory not found".into(),
        ));
    }
    if let Some(parent) = archive_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut rel_files = collect_relative_files(dir)?;
    rel_files.sort();

    let archive_flags = if encryption_key.is_some() {
        BACKUP_ARCHIVE_FLAG_ENCRYPTED
    } else {
        0
    };
    let salt = *Uuid::new_v4().as_bytes();

    let archive_file = fs::File::create(archive_path)?;
    let mut writer = BufWriter::with_capacity(BACKUP_ARCHIVE_IO_BUFFER_BYTES, archive_file);
    writer.write_all(BACKUP_ARCHIVE_MAGIC)?;
    writer.write_all(&[archive_flags])?;
    writer.write_all(&salt)?;

    for (entry_index, rel) in rel_files.iter().enumerate() {
        let resolved = resolve_backup_path(dir, rel)?;
        let file_len = resolved.metadata()?.len();
        if file_len >= BACKUP_ARCHIVE_CHUNKED_FILE_THRESHOLD_BYTES {
            write_chunked_archive_file(
                &mut writer,
                rel,
                &resolved,
                file_len,
                &salt,
                entry_index as u64,
                encryption_key,
                compression_level,
            )?;
        } else {
            write_legacy_archive_file(
                &mut writer,
                rel,
                &resolved,
                &salt,
                entry_index as u64,
                encryption_key,
                compression_level,
            )?;
        }
    }

    write_u8(&mut writer, BACKUP_ARCHIVE_ENTRY_END)?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    if let Some(parent) = archive_path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

pub fn extract_backup_archive(
    archive_path: &Path,
    dir: &Path,
    encryption_key: Option<&[u8; 32]>,
) -> Result<(), AedbError> {
    if dir.exists() && fs::read_dir(dir)?.next().is_some() {
        return Err(AedbError::Validation(
            "archive extract target directory must be empty".into(),
        ));
    }
    fs::create_dir_all(dir)?;

    let mut reader = BufReader::with_capacity(
        BACKUP_ARCHIVE_IO_BUFFER_BYTES,
        fs::File::open(archive_path)?,
    );
    let mut magic = [0u8; 8];
    reader.read_exact(&mut magic)?;
    if &magic != BACKUP_ARCHIVE_MAGIC {
        return Err(AedbError::Validation("invalid backup archive magic".into()));
    }

    let mut flag_buf = [0u8; 1];
    reader.read_exact(&mut flag_buf)?;
    let encrypted = (flag_buf[0] & BACKUP_ARCHIVE_FLAG_ENCRYPTED) != 0;
    if encrypted && encryption_key.is_none() {
        return Err(AedbError::Validation(
            "backup archive requires checkpoint key".into(),
        ));
    }

    let mut salt = [0u8; 16];
    reader.read_exact(&mut salt)?;

    let mut entry_index = 0u64;
    loop {
        let entry = read_u8(&mut reader)?;
        if entry == BACKUP_ARCHIVE_ENTRY_END {
            break;
        }
        let rel = read_archive_relative_path(&mut reader)?;
        match entry {
            BACKUP_ARCHIVE_ENTRY_FILE => {
                extract_legacy_archive_file(
                    &mut reader,
                    dir,
                    &rel,
                    &salt,
                    entry_index,
                    encrypted,
                    encryption_key,
                )?;
            }
            BACKUP_ARCHIVE_ENTRY_CHUNKED_FILE => {
                extract_chunked_archive_file(
                    &mut reader,
                    dir,
                    &rel,
                    &salt,
                    entry_index,
                    encrypted,
                    encryption_key,
                )?;
            }
            _ => return Err(AedbError::Validation("invalid backup archive entry".into())),
        }
        entry_index = entry_index.saturating_add(1);
    }
    sync_dir(dir)?;
    Ok(())
}

pub fn sha256_file_hex(path: &Path) -> Result<String, AedbError> {
    let file = fs::File::open(path)?;
    let mut reader = BufReader::with_capacity(BACKUP_ARCHIVE_IO_BUFFER_BYTES, file);
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; BACKUP_ARCHIVE_IO_BUFFER_BYTES];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex_string(hasher.finalize().as_slice()))
}

/// SHA-256 of an in-memory buffer, hex-encoded. Matches `sha256_file_hex` so a
/// payload already read into memory can be verified without re-reading the file.
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex_string(Sha256::digest(bytes).as_slice())
}

fn hmac_hex(key: &[u8], bytes: &[u8]) -> Result<String, AedbError> {
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = <HmacSha256 as Mac>::new_from_slice(key)
        .map_err(|e| AedbError::Validation(format!("invalid hmac key: {e}")))?;
    mac.update(bytes);
    Ok(hex_string(&mac.finalize().into_bytes()))
}

fn write_file_atomic_synced(dir: &Path, filename: &str, bytes: &[u8]) -> Result<(), AedbError> {
    let final_path = dir.join(filename);
    let mut tmp = NamedTempFile::new_in(dir)?;
    tmp.write_all(bytes)?;
    tmp.flush()?;
    tmp.as_file().sync_all()?;
    tmp.persist(&final_path)
        .map_err(|e| AedbError::Io(e.error))?;
    fs::File::open(&final_path)?.sync_all()?;
    sync_dir(dir)?;
    Ok(())
}

fn write_legacy_archive_file<W: Write>(
    writer: &mut W,
    rel: &str,
    resolved: &Path,
    salt: &[u8; 16],
    entry_index: u64,
    encryption_key: Option<&[u8; 32]>,
    compression_level: i32,
) -> Result<(), AedbError> {
    validate_archive_path_len(rel)?;
    // Files reaching this path are below the chunking threshold, so reading the
    // whole payload into memory to feed the multithreaded encoder is bounded.
    let raw = fs::read(resolved)?;
    let compressed = compress_block(&raw, compression_level)?;

    let payload = if let Some(key) = encryption_key {
        let nonce = derive_archive_nonce(salt, entry_index, rel);
        encrypt_archive_payload(&compressed, key, &nonce)?
    } else {
        compressed
    };
    validate_archive_payload_len(payload.len() as u64)?;

    write_u8(writer, BACKUP_ARCHIVE_ENTRY_FILE)?;
    write_u32(writer, rel.len() as u32)?;
    writer.write_all(rel.as_bytes())?;
    write_u64(writer, payload.len() as u64)?;
    writer.write_all(&payload)?;
    Ok(())
}

fn write_chunked_archive_file<W: Write>(
    writer: &mut W,
    rel: &str,
    resolved: &Path,
    file_len: u64,
    salt: &[u8; 16],
    entry_index: u64,
    encryption_key: Option<&[u8; 32]>,
    compression_level: i32,
) -> Result<(), AedbError> {
    validate_archive_path_len(rel)?;
    write_u8(writer, BACKUP_ARCHIVE_ENTRY_CHUNKED_FILE)?;
    write_u32(writer, rel.len() as u32)?;
    writer.write_all(rel.as_bytes())?;
    write_u64(writer, file_len)?;

    let mut reader =
        BufReader::with_capacity(BACKUP_ARCHIVE_IO_BUFFER_BYTES, fs::File::open(resolved)?);
    let mut buf = vec![0u8; BACKUP_ARCHIVE_CHUNK_BYTES];
    let mut chunk_index = 0u64;
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        let raw = &buf[..n];
        let compressed = compress_block(raw, compression_level)?;
        let payload = if let Some(key) = encryption_key {
            let nonce = derive_archive_chunk_nonce(salt, entry_index, chunk_index, rel);
            encrypt_archive_payload(&compressed, key, &nonce)?
        } else {
            compressed
        };
        validate_archive_payload_len(payload.len() as u64)?;
        write_u32(writer, n as u32)?;
        write_u64(writer, payload.len() as u64)?;
        writer.write_all(&payload)?;
        chunk_index = chunk_index.saturating_add(1);
    }
    Ok(())
}

fn read_archive_relative_path<R: Read>(reader: &mut R) -> Result<String, AedbError> {
    let path_len_u32 = read_u32(reader)?;
    if path_len_u32 == 0 {
        return Err(AedbError::Validation(
            "backup archive path must not be empty".into(),
        ));
    }
    if path_len_u32 > MAX_BACKUP_ARCHIVE_PATH_BYTES {
        return Err(AedbError::Validation(
            "backup archive path exceeds max length".into(),
        ));
    }
    let path_len = usize::try_from(path_len_u32)
        .map_err(|_| AedbError::Validation("backup archive path exceeds platform limits".into()))?;
    let mut path_bytes = vec![0u8; path_len];
    reader.read_exact(&mut path_bytes)?;
    let rel = String::from_utf8(path_bytes)
        .map_err(|_| AedbError::Validation("backup archive path is not utf-8".into()))?;
    validate_safe_relative_path(&rel, "backup archive path")?;
    Ok(rel)
}

fn extract_legacy_archive_file<R: Read>(
    reader: &mut R,
    dir: &Path,
    rel: &str,
    salt: &[u8; 16],
    entry_index: u64,
    encrypted: bool,
    encryption_key: Option<&[u8; 32]>,
) -> Result<(), AedbError> {
    let payload = read_archive_payload(reader)?;
    let compressed = if encrypted {
        let Some(key) = encryption_key else {
            return Err(AedbError::Validation(
                "backup archive missing encryption key".into(),
            ));
        };
        let expected_nonce = derive_archive_nonce(salt, entry_index, rel);
        decrypt_archive_payload(&payload, key, &expected_nonce)?
    } else {
        payload
    };

    let out = resolve_backup_output_path(dir, rel)?;
    write_compressed_payload_file_synced(&out, compressed.as_slice())?;
    Ok(())
}

fn write_compressed_payload_file_synced(path: &Path, compressed: &[u8]) -> Result<(), AedbError> {
    let decoder =
        zstd::stream::Decoder::new(compressed).map_err(|e| AedbError::Decode(e.to_string()))?;
    // Bound decompression to the per-file payload cap so a maliciously crafted
    // (highly compressible) archive entry cannot expand without limit and
    // exhaust memory/disk on restore. The `+ 1` lets us detect overruns.
    let mut limited = decoder.take(MAX_BACKUP_ARCHIVE_PAYLOAD_BYTES + 1);
    let mut file =
        BufWriter::with_capacity(BACKUP_ARCHIVE_IO_BUFFER_BYTES, fs::File::create(path)?);
    let written =
        std::io::copy(&mut limited, &mut file).map_err(|e| AedbError::Decode(e.to_string()))?;
    if written > MAX_BACKUP_ARCHIVE_PAYLOAD_BYTES {
        return Err(AedbError::Decode(
            "backup archive entry exceeds maximum decompressed size".into(),
        ));
    }
    file.flush()?;
    file.get_ref().sync_all()?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn extract_chunked_archive_file<R: Read>(
    reader: &mut R,
    dir: &Path,
    rel: &str,
    salt: &[u8; 16],
    entry_index: u64,
    encrypted: bool,
    encryption_key: Option<&[u8; 32]>,
) -> Result<(), AedbError> {
    let file_len = read_u64(reader)?;
    let out = resolve_backup_output_path(dir, rel)?;
    let mut file =
        BufWriter::with_capacity(BACKUP_ARCHIVE_IO_BUFFER_BYTES, fs::File::create(&out)?);
    let mut remaining = file_len;
    let mut chunk_index = 0u64;
    while remaining > 0 {
        let raw_len_u32 = read_u32(reader)?;
        if raw_len_u32 == 0 {
            return Err(AedbError::Validation(
                "chunked backup archive contains empty chunk".into(),
            ));
        }
        let raw_len = usize::try_from(raw_len_u32).map_err(|_| {
            AedbError::Validation("backup archive chunk exceeds platform limits".into())
        })?;
        if raw_len > BACKUP_ARCHIVE_CHUNK_BYTES || raw_len as u64 > remaining {
            return Err(AedbError::Validation(
                "chunked backup archive chunk length is invalid".into(),
            ));
        }

        let payload = read_archive_payload(reader)?;
        let compressed = if encrypted {
            let Some(key) = encryption_key else {
                return Err(AedbError::Validation(
                    "backup archive missing encryption key".into(),
                ));
            };
            let expected_nonce = derive_archive_chunk_nonce(salt, entry_index, chunk_index, rel);
            decrypt_archive_payload(&payload, key, &expected_nonce)?
        } else {
            payload
        };

        write_archive_chunk(&compressed, raw_len, &mut file)?;
        remaining -= raw_len as u64;
        chunk_index = chunk_index.saturating_add(1);
    }
    file.flush()?;
    file.get_ref().sync_all()?;
    if let Some(parent) = out.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn read_archive_payload<R: Read>(reader: &mut R) -> Result<Vec<u8>, AedbError> {
    let payload_len_u64 = read_u64(reader)?;
    validate_archive_payload_len(payload_len_u64)?;
    let payload_len = usize::try_from(payload_len_u64).map_err(|_| {
        AedbError::Validation("backup archive payload exceeds platform limits".into())
    })?;
    let mut payload = vec![0u8; payload_len];
    reader.read_exact(&mut payload)?;
    Ok(payload)
}

fn write_archive_chunk<W: Write>(
    compressed: &[u8],
    expected_len: usize,
    writer: &mut W,
) -> Result<(), AedbError> {
    let decoder =
        zstd::stream::Decoder::new(compressed).map_err(|e| AedbError::Decode(e.to_string()))?;
    let mut limited = decoder.take(expected_len as u64 + 1);
    let written =
        std::io::copy(&mut limited, writer).map_err(|e| AedbError::Decode(e.to_string()))?;
    if written != expected_len as u64 {
        return Err(AedbError::Decode(
            "chunked backup archive decoded length mismatch".into(),
        ));
    }
    Ok(())
}

fn validate_archive_path_len(rel: &str) -> Result<(), AedbError> {
    if rel.len() > MAX_BACKUP_ARCHIVE_PATH_BYTES as usize {
        return Err(AedbError::Validation(
            "backup archive path exceeds max length".into(),
        ));
    }
    Ok(())
}

fn validate_archive_payload_len(payload_len: u64) -> Result<(), AedbError> {
    if payload_len > MAX_BACKUP_ARCHIVE_PAYLOAD_BYTES {
        return Err(AedbError::Validation(
            "backup archive payload exceeds max size".into(),
        ));
    }
    Ok(())
}

fn sync_dir(dir: &Path) -> Result<(), AedbError> {
    fs::File::open(dir)?.sync_all()?;
    Ok(())
}

fn verify_hmac_hex(key: &[u8], bytes: &[u8], expected_hex: &str) -> Result<(), AedbError> {
    let expected = decode_hex(expected_hex)?;
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = <HmacSha256 as Mac>::new_from_slice(key)
        .map_err(|e| AedbError::Validation(format!("invalid hmac key: {e}")))?;
    mac.update(bytes);
    mac.verify_slice(&expected)
        .map_err(|_| AedbError::Validation("backup manifest hmac mismatch".into()))
}

fn decode_hex(input: &str) -> Result<Vec<u8>, AedbError> {
    let trimmed = input.trim();
    if !trimmed.len().is_multiple_of(2) {
        return Err(AedbError::Validation(
            "backup manifest hmac must be hex".into(),
        ));
    }
    let mut out = Vec::with_capacity(trimmed.len() / 2);
    for pair in trimmed.as_bytes().chunks_exact(2) {
        let hi = hex_nibble(pair[0])
            .ok_or_else(|| AedbError::Validation("backup manifest hmac must be hex".into()))?;
        let lo = hex_nibble(pair[1])
            .ok_or_else(|| AedbError::Validation("backup manifest hmac must be hex".into()))?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn hex_nibble(ch: u8) -> Option<u8> {
    match ch {
        b'0'..=b'9' => Some(ch - b'0'),
        b'a'..=b'f' => Some(ch - b'a' + 10),
        b'A'..=b'F' => Some(ch - b'A' + 10),
        _ => None,
    }
}

fn is_valid_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.as_bytes().iter().all(|b| b.is_ascii_hexdigit())
}

fn validate_backup_manifest(manifest: &BackupManifest) -> Result<(), AedbError> {
    validate_safe_relative_path(&manifest.checkpoint_file, "checkpoint_file")?;
    for seg in &manifest.wal_segments {
        validate_safe_relative_path(seg, "wal_segments[]")?;
    }
    for rel in manifest.file_sha256.keys() {
        validate_safe_relative_path(rel, "file_sha256 key")?;
    }
    if manifest.backup_type == "full"
        && !manifest.file_sha256.contains_key(&manifest.checkpoint_file)
    {
        return Err(AedbError::Validation(
            "backup manifest missing checkpoint checksum".into(),
        ));
    }
    for seg in &manifest.wal_segments {
        let rel = format!("wal_tail/{seg}");
        if !manifest.file_sha256.contains_key(&rel) {
            return Err(AedbError::Validation(format!(
                "backup manifest missing checksum for wal segment: {seg}"
            )));
        }
    }
    Ok(())
}

fn validate_safe_relative_path(path: &str, field: &str) -> Result<(), AedbError> {
    if path.is_empty() {
        return Err(AedbError::Validation(format!(
            "{field} cannot be empty in backup manifest"
        )));
    }
    if path.contains('\\') {
        return Err(AedbError::Validation(format!(
            "{field} must not contain backslashes"
        )));
    }
    let candidate = Path::new(path);
    if candidate.is_absolute() {
        return Err(AedbError::Validation(format!(
            "{field} must be a relative path"
        )));
    }
    for component in candidate.components() {
        match component {
            Component::Normal(_) => {}
            Component::CurDir
            | Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => {
                return Err(AedbError::Validation(format!(
                    "{field} contains disallowed path component"
                )));
            }
        }
    }
    Ok(())
}

pub(crate) fn resolve_backup_path(dir: &Path, rel: &str) -> Result<std::path::PathBuf, AedbError> {
    validate_safe_relative_path(rel, "backup path")?;
    let base = fs::canonicalize(dir)?;
    let candidate = dir.join(rel);
    let canonical = fs::canonicalize(&candidate)?;
    if !canonical.starts_with(&base) {
        return Err(AedbError::Validation(
            "backup path escapes backup directory".into(),
        ));
    }
    Ok(canonical)
}

fn collect_relative_files(dir: &Path) -> Result<Vec<String>, AedbError> {
    fn walk(base: &Path, cur: &Path, out: &mut Vec<String>) -> Result<(), AedbError> {
        for entry in fs::read_dir(cur)? {
            let entry = entry?;
            let path = entry.path();
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                walk(base, &path, out)?;
            } else if file_type.is_file() {
                let rel = path
                    .strip_prefix(base)
                    .map_err(|e| AedbError::Validation(format!("invalid backup file path: {e}")))?;
                let rel = rel.to_string_lossy().replace('\\', "/");
                validate_safe_relative_path(&rel, "backup archive path")?;
                out.push(rel);
            }
        }
        Ok(())
    }

    let mut out = Vec::new();
    walk(dir, dir, &mut out)?;
    Ok(out)
}

fn resolve_backup_output_path(dir: &Path, rel: &str) -> Result<std::path::PathBuf, AedbError> {
    validate_safe_relative_path(rel, "backup archive path")?;
    let base = fs::canonicalize(dir)?;
    let out = dir.join(rel);
    if !out.starts_with(dir) {
        return Err(AedbError::Validation(
            "backup path escapes backup directory".into(),
        ));
    }
    let parent = out.parent().ok_or_else(|| {
        AedbError::Validation("backup archive output path must have parent".into())
    })?;
    ensure_no_symlink_components(
        dir,
        parent
            .strip_prefix(dir)
            .map_err(|_| AedbError::Validation("backup path escapes backup directory".into()))?,
    )?;
    fs::create_dir_all(parent)?;
    let canonical_parent = fs::canonicalize(parent)?;
    if !canonical_parent.starts_with(&base) {
        return Err(AedbError::Validation(
            "backup path escapes backup directory".into(),
        ));
    }
    Ok(out)
}

fn ensure_no_symlink_components(base: &Path, rel_parent: &Path) -> Result<(), AedbError> {
    let mut current = PathBuf::from(base);
    for component in rel_parent.components() {
        let Component::Normal(part) = component else {
            return Err(AedbError::Validation(
                "backup archive path contains disallowed path component".into(),
            ));
        };
        current.push(part);
        if let Ok(metadata) = fs::symlink_metadata(&current)
            && metadata.file_type().is_symlink()
        {
            return Err(AedbError::Validation(
                "backup archive output path traverses symlink".into(),
            ));
        }
    }
    Ok(())
}

fn derive_archive_nonce(salt: &[u8; 16], index: u64, rel: &str) -> [u8; 12] {
    let mut input = Vec::with_capacity(16 + 8 + rel.len());
    input.extend_from_slice(salt);
    input.extend_from_slice(&index.to_le_bytes());
    input.extend_from_slice(rel.as_bytes());
    let hash = blake3::hash(&input);
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&hash.as_bytes()[..12]);
    nonce
}

fn derive_archive_chunk_nonce(
    salt: &[u8; 16],
    index: u64,
    chunk_index: u64,
    rel: &str,
) -> [u8; 12] {
    let mut input = Vec::with_capacity(16 + 8 + 8 + rel.len());
    input.extend_from_slice(salt);
    input.extend_from_slice(&index.to_le_bytes());
    input.extend_from_slice(&chunk_index.to_le_bytes());
    input.extend_from_slice(rel.as_bytes());
    let hash = blake3::hash(&input);
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&hash.as_bytes()[..12]);
    nonce
}

fn encrypt_archive_payload(
    payload: &[u8],
    key: &[u8; 32],
    nonce: &[u8; 12],
) -> Result<Vec<u8>, AedbError> {
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| AedbError::Validation(format!("invalid encryption key: {e}")))?;
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(nonce), payload)
        .map_err(|e| AedbError::Validation(format!("backup archive encryption failed: {e}")))?;
    let mut out = Vec::with_capacity(12 + ciphertext.len());
    out.extend_from_slice(nonce);
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

fn decrypt_archive_payload(
    payload: &[u8],
    key: &[u8; 32],
    expected_nonce: &[u8; 12],
) -> Result<Vec<u8>, AedbError> {
    if payload.len() < 12 {
        return Err(AedbError::Decode(
            "encrypted backup archive payload too small".into(),
        ));
    }
    let nonce = &payload[..12];
    if nonce != expected_nonce {
        return Err(AedbError::Validation(
            "backup archive nonce mismatch".into(),
        ));
    }
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| AedbError::Validation(format!("invalid encryption key: {e}")))?;
    cipher
        .decrypt(Nonce::from_slice(nonce), &payload[12..])
        .map_err(|e| AedbError::Validation(format!("backup archive decryption failed: {e}")))
}

fn write_u8<W: Write>(writer: &mut W, value: u8) -> Result<(), AedbError> {
    writer.write_all(&[value])?;
    Ok(())
}

fn write_u32<W: Write>(writer: &mut W, value: u32) -> Result<(), AedbError> {
    writer.write_all(&value.to_le_bytes())?;
    Ok(())
}

fn write_u64<W: Write>(writer: &mut W, value: u64) -> Result<(), AedbError> {
    writer.write_all(&value.to_le_bytes())?;
    Ok(())
}

fn read_u8<R: Read>(reader: &mut R) -> Result<u8, AedbError> {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn read_u32<R: Read>(reader: &mut R) -> Result<u32, AedbError> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u64<R: Read>(reader: &mut R) -> Result<u64, AedbError> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
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

#[cfg(test)]
mod tests;
