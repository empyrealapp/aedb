use crate::error::AedbError;
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Component, Path};
use uuid::Uuid;

pub const BACKUP_MANIFEST_FILE: &str = "backup_manifest.json";
pub const BACKUP_MANIFEST_HMAC_FILE: &str = "backup_manifest.hmac";

const BACKUP_ARCHIVE_MAGIC: &[u8; 8] = b"AEDBARC1";
const BACKUP_ARCHIVE_FLAG_ENCRYPTED: u8 = 0x01;
const BACKUP_ARCHIVE_ENTRY_FILE: u8 = 0x01;
const BACKUP_ARCHIVE_ENTRY_END: u8 = 0xFF;

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
    pub file_sha256: HashMap<String, String>,
}

pub fn write_backup_manifest(
    dir: &Path,
    manifest: &BackupManifest,
    signing_key: Option<&[u8]>,
) -> Result<(), AedbError> {
    fs::create_dir_all(dir)?;
    let bytes =
        serde_json::to_vec_pretty(manifest).map_err(|e| AedbError::Encode(e.to_string()))?;
    fs::write(dir.join(BACKUP_MANIFEST_FILE), &bytes)?;
    if let Some(key) = signing_key {
        let sig = hmac_hex(key, &bytes)?;
        fs::write(dir.join(BACKUP_MANIFEST_HMAC_FILE), sig)?;
    } else {
        let _ = fs::remove_file(dir.join(BACKUP_MANIFEST_HMAC_FILE));
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

    let mut writer = BufWriter::new(fs::File::create(archive_path)?);
    writer.write_all(BACKUP_ARCHIVE_MAGIC)?;
    writer.write_all(&[archive_flags])?;
    writer.write_all(&salt)?;

    for (idx, rel) in rel_files.iter().enumerate() {
        let resolved = resolve_backup_path(dir, rel)?;
        let raw = fs::read(&resolved)?;
        let compressed = zstd::stream::encode_all(raw.as_slice(), 3)
            .map_err(|e| AedbError::Encode(e.to_string()))?;

        let payload = if let Some(key) = encryption_key {
            let nonce = derive_archive_nonce(&salt, idx as u64, rel);
            encrypt_archive_payload(&compressed, key, &nonce)?
        } else {
            compressed
        };

        write_u8(&mut writer, BACKUP_ARCHIVE_ENTRY_FILE)?;
        write_u32(&mut writer, rel.len() as u32)?;
        writer.write_all(rel.as_bytes())?;
        write_u64(&mut writer, payload.len() as u64)?;
        writer.write_all(&payload)?;
    }

    write_u8(&mut writer, BACKUP_ARCHIVE_ENTRY_END)?;
    writer.flush()?;
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

    let mut reader = BufReader::new(fs::File::open(archive_path)?);
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

    let mut idx = 0u64;
    loop {
        let entry = read_u8(&mut reader)?;
        if entry == BACKUP_ARCHIVE_ENTRY_END {
            break;
        }
        if entry != BACKUP_ARCHIVE_ENTRY_FILE {
            return Err(AedbError::Validation("invalid backup archive entry".into()));
        }

        let path_len = read_u32(&mut reader)? as usize;
        if path_len == 0 {
            return Err(AedbError::Validation(
                "backup archive path must not be empty".into(),
            ));
        }
        let mut path_bytes = vec![0u8; path_len];
        reader.read_exact(&mut path_bytes)?;
        let rel = String::from_utf8(path_bytes)
            .map_err(|_| AedbError::Validation("backup archive path is not utf-8".into()))?;

        validate_safe_relative_path(&rel, "backup archive path")?;

        let payload_len = read_u64(&mut reader)? as usize;
        let mut payload = vec![0u8; payload_len];
        reader.read_exact(&mut payload)?;

        let compressed = if encrypted {
            let key = encryption_key.expect("checked above");
            let expected_nonce = derive_archive_nonce(&salt, idx, &rel);
            decrypt_archive_payload(&payload, key, &expected_nonce)?
        } else {
            payload
        };

        let bytes = zstd::stream::decode_all(compressed.as_slice())
            .map_err(|e| AedbError::Decode(e.to_string()))?;

        let out = resolve_backup_output_path(dir, &rel)?;
        fs::write(out, bytes)?;
        idx = idx.saturating_add(1);
    }
    Ok(())
}

pub fn sha256_file_hex(path: &Path) -> Result<String, AedbError> {
    let file = fs::File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 16 * 1024];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex_string(hasher.finalize().as_slice()))
}

fn hmac_hex(key: &[u8], bytes: &[u8]) -> Result<String, AedbError> {
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = <HmacSha256 as Mac>::new_from_slice(key)
        .map_err(|e| AedbError::Validation(format!("invalid hmac key: {e}")))?;
    mac.update(bytes);
    Ok(hex_string(&mac.finalize().into_bytes()))
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
    let parent = out.parent().ok_or_else(|| {
        AedbError::Validation("backup archive output path must have parent".into())
    })?;
    fs::create_dir_all(parent)?;
    let canonical_parent = fs::canonicalize(parent)?;
    if !canonical_parent.starts_with(&base) {
        return Err(AedbError::Validation(
            "backup path escapes backup directory".into(),
        ));
    }
    Ok(out)
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
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_manifest() -> BackupManifest {
        BackupManifest {
            backup_id: "bk_1".into(),
            backup_type: "full".into(),
            parent_backup_id: None,
            from_seq: None,
            created_at_micros: 1,
            aedb_version: "0.1.0".into(),
            checkpoint_seq: 1,
            wal_head_seq: 2,
            checkpoint_file: "checkpoint_1.aedbcp".into(),
            wal_segments: vec!["segment_2.aedbwal".into()],
            file_sha256: HashMap::from([
                ("checkpoint_1.aedbcp".into(), "a".repeat(64)),
                ("wal_tail/segment_2.aedbwal".into(), "b".repeat(64)),
            ]),
        }
    }

    #[test]
    fn backup_manifest_rejects_unsafe_paths() {
        let mut manifest = sample_manifest();
        manifest.wal_segments = vec!["../segment_2.aedbwal".into()];
        let err = validate_backup_manifest(&manifest).expect_err("must reject parent path");
        assert!(matches!(err, AedbError::Validation(_)));
    }

    #[test]
    fn backup_manifest_requires_wal_checksums() {
        let mut manifest = sample_manifest();
        manifest.file_sha256.remove("wal_tail/segment_2.aedbwal");
        let err = validate_backup_manifest(&manifest).expect_err("must require checksum");
        assert!(matches!(err, AedbError::Validation(_)));
    }

    #[test]
    fn backup_manifest_rejects_invalid_checksum_hex() {
        let mut manifest = sample_manifest();
        manifest
            .file_sha256
            .insert("checkpoint_1.aedbcp".into(), "not_hex".into());
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("checkpoint_1.aedbcp"), b"x").expect("write");
        std::fs::create_dir_all(dir.path().join("wal_tail")).expect("wal dir");
        std::fs::write(dir.path().join("wal_tail/segment_2.aedbwal"), b"x").expect("write wal");
        let err = verify_backup_files(dir.path(), &manifest).expect_err("must reject checksum");
        assert!(matches!(err, AedbError::Validation(_)));
    }

    #[test]
    fn backup_archive_roundtrip_plain_and_encrypted() {
        let src = tempfile::tempdir().expect("src");
        let dst_plain = tempfile::tempdir().expect("dst plain");
        let dst_enc = tempfile::tempdir().expect("dst enc");
        let archive_plain = src.path().join("backup_plain.aedbarc");
        let archive_enc = src.path().join("backup_enc.aedbarc");
        std::fs::create_dir_all(src.path().join("wal_tail")).expect("wal dir");
        std::fs::write(src.path().join("backup_manifest.json"), b"{\"x\":1}").expect("manifest");
        std::fs::write(src.path().join("wal_tail/segment_1.aedbwal"), b"segment").expect("wal");

        write_backup_archive(src.path(), &archive_plain, None).expect("write plain");
        extract_backup_archive(&archive_plain, dst_plain.path(), None).expect("extract plain");
        assert_eq!(
            std::fs::read(dst_plain.path().join("backup_manifest.json")).expect("read manifest"),
            b"{\"x\":1}".to_vec()
        );

        let key = [9u8; 32];
        write_backup_archive(src.path(), &archive_enc, Some(&key)).expect("write enc");
        extract_backup_archive(&archive_enc, dst_enc.path(), Some(&key)).expect("extract enc");
        assert_eq!(
            std::fs::read(dst_enc.path().join("wal_tail/segment_1.aedbwal")).expect("read wal"),
            b"segment".to_vec()
        );

        let wrong = [7u8; 32];
        let err = extract_backup_archive(
            &archive_enc,
            tempfile::tempdir().expect("tmp").path(),
            Some(&wrong),
        )
        .expect_err("wrong key must fail");
        assert!(format!("{err}").contains("decryption failed"));
    }

    #[cfg(unix)]
    #[test]
    fn backup_manifest_rejects_symlink_escape() {
        let mut manifest = sample_manifest();
        let dir = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside");
        let outside_file = outside.path().join("outside.bin");
        std::fs::write(&outside_file, b"outside").expect("write outside");
        std::os::unix::fs::symlink(&outside_file, dir.path().join("checkpoint_1.aedbcp"))
            .expect("symlink checkpoint");
        std::fs::create_dir_all(dir.path().join("wal_tail")).expect("wal dir");
        std::fs::write(dir.path().join("wal_tail/segment_2.aedbwal"), b"x").expect("write wal");
        manifest.file_sha256.insert(
            "checkpoint_1.aedbcp".into(),
            sha256_file_hex(&outside_file).expect("hash"),
        );
        manifest.file_sha256.insert(
            "wal_tail/segment_2.aedbwal".into(),
            sha256_file_hex(&dir.path().join("wal_tail/segment_2.aedbwal")).expect("hash wal"),
        );
        let err = verify_backup_files(dir.path(), &manifest).expect_err("must reject escape");
        assert!(matches!(err, AedbError::Validation(_)));
    }
}
