use crate::backup::{
    BackupManifest, load_backup_manifest, resolve_backup_path, sha256_file_hex, verify_backup_files,
};
use crate::config::AedbConfig;
use crate::error::AedbError;
use crate::manifest::schema::SegmentMeta;
use crate::wal::frame::{FrameError, FrameReader};
use crate::wal::segment::{SEGMENT_HEADER_SIZE, SegmentHeader};
use sha2::{Digest, Sha256};
use std::fs;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};

const WAL_SEQUENTIAL_READ_BUFFER_BYTES: usize = 1024 * 1024;

pub(crate) fn read_segments(dir: &Path) -> Result<Vec<SegmentMeta>, AedbError> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with("segment_") && name.ends_with(".aedbwal") {
            let seq = name
                .trim_start_matches("segment_")
                .trim_end_matches(".aedbwal")
                .parse::<u64>()
                .map_err(|e| AedbError::Decode(e.to_string()))?;
            let path = entry.path();
            let size_bytes = fs::metadata(&path)?.len();
            out.push(SegmentMeta {
                filename: name,
                segment_seq: seq,
                sha256_hex: sha256_file_hex(&path)?,
                size_bytes,
            });
        }
    }
    out.sort_by_key(|s| s.segment_seq);
    Ok(out)
}

pub(crate) fn read_segments_for_checkpoint(
    dir: &Path,
    checkpoint_seq: u64,
) -> Result<Vec<SegmentMeta>, AedbError> {
    let segments = read_segments(dir)?;
    let last_segment = segments.last().cloned();
    let mut filtered = Vec::with_capacity(segments.len());
    for segment in segments {
        let path = dir.join(&segment.filename);
        let keep = match scan_segment_seq_range(&path)? {
            Some((_, max_seq)) => max_seq > checkpoint_seq,
            None => false,
        };
        if keep {
            filtered.push(segment);
        }
    }

    // Keep at least the current active segment so manifest metadata remains anchored
    // even if all observed frames are covered by the checkpoint.
    if filtered.is_empty()
        && let Some(last) = last_segment
    {
        filtered.push(last);
    }

    filtered.sort_by_key(|segment| segment.segment_seq);
    Ok(filtered)
}

pub(crate) fn segment_seq_from_name(name: &str) -> Option<u64> {
    if !name.starts_with("segment_") || !name.ends_with(".aedbwal") {
        return None;
    }
    let middle = name
        .trim_start_matches("segment_")
        .trim_end_matches(".aedbwal");
    middle.parse::<u64>().ok()
}

pub(crate) fn scan_segment_seq_range(path: &Path) -> Result<Option<(u64, u64)>, AedbError> {
    let file = File::open(path)?;
    let size_bytes = file.metadata()?.len();
    if size_bytes <= SEGMENT_HEADER_SIZE as u64 {
        return Ok(None);
    }
    let mut reader = BufReader::with_capacity(WAL_SEQUENTIAL_READ_BUFFER_BYTES, file);
    let mut header = [0u8; SEGMENT_HEADER_SIZE];
    reader.read_exact(&mut header)?;
    let payload_size_bytes = size_bytes.saturating_sub(SEGMENT_HEADER_SIZE as u64);
    let mut frame_reader = FrameReader::new(reader.take(payload_size_bytes));
    let mut min_seq = u64::MAX;
    let mut max_seq = 0u64;
    loop {
        match frame_reader.next_frame() {
            Ok(Some(frame)) => {
                min_seq = min_seq.min(frame.commit_seq);
                max_seq = max_seq.max(frame.commit_seq);
            }
            Ok(None) | Err(FrameError::Truncation) => break,
            Err(FrameError::Corruption) => {
                return Err(AedbError::Validation(
                    "wal frame corruption detected while scanning backup segment".into(),
                ));
            }
            Err(FrameError::Io(e)) => return Err(AedbError::Io(std::io::Error::other(e))),
        }
    }
    if min_seq == u64::MAX {
        return Ok(None);
    }
    Ok(Some((min_seq, max_seq)))
}

pub(crate) fn copy_file_prefix_sha256_hex(
    src: &Path,
    dst: &Path,
    size_bytes: u64,
) -> Result<String, AedbError> {
    let mut reader = File::open(src)?;
    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut writer = File::create(dst)?;
    let mut hasher = Sha256::new();
    let mut remaining = size_bytes;
    let mut buf = vec![0u8; WAL_SEQUENTIAL_READ_BUFFER_BYTES];
    while remaining > 0 {
        let to_read = buf.len().min(remaining as usize);
        let n = reader.read(&mut buf[..to_read])?;
        if n == 0 {
            return Err(AedbError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "backup source file shorter than manifest segment size",
            )));
        }
        writer.write_all(&buf[..n])?;
        hasher.update(&buf[..n]);
        remaining -= n as u64;
    }
    writer.sync_all()?;
    if let Some(parent) = dst.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(hex::encode(hasher.finalize()))
}

pub(crate) fn validate_backup_chain(chain: &[(PathBuf, BackupManifest)]) -> Result<(), AedbError> {
    let Some((_, full)) = chain.first() else {
        return Err(AedbError::Validation("backup chain cannot be empty".into()));
    };
    if full.backup_type != "full" {
        return Err(AedbError::Validation(
            "backup chain must start with a full backup".into(),
        ));
    }
    for chain_index in 1..chain.len() {
        let prev = &chain[chain_index - 1].1;
        let cur = &chain[chain_index].1;
        if cur.backup_type != "incremental" {
            return Err(AedbError::Validation(format!(
                "chain entry {chain_index} is not incremental"
            )));
        }
        if cur.parent_backup_id.as_deref() != Some(prev.backup_id.as_str()) {
            return Err(AedbError::Validation(format!(
                "chain entry {chain_index} parent mismatch"
            )));
        }
        let expected_from = prev.wal_head_seq.saturating_add(1);
        if cur.from_seq != Some(expected_from) {
            return Err(AedbError::Validation(format!(
                "chain entry {chain_index} from_seq mismatch"
            )));
        }
        if cur.wal_head_seq < expected_from.saturating_sub(1) {
            return Err(AedbError::Validation(format!(
                "chain entry {chain_index} wal_head_seq invalid"
            )));
        }
    }
    Ok(())
}

pub(crate) fn load_verified_backup_chain(
    backup_dirs: &[PathBuf],
    config: &AedbConfig,
) -> Result<Vec<(PathBuf, BackupManifest)>, AedbError> {
    if backup_dirs.is_empty() {
        return Err(AedbError::Validation("backup chain cannot be empty".into()));
    }
    let mut chain = Vec::with_capacity(backup_dirs.len());
    for dir in backup_dirs {
        let manifest = load_backup_manifest(dir, config.hmac_key())?;
        verify_backup_files(dir, &manifest)?;
        chain.push((dir.clone(), manifest));
    }
    validate_backup_chain(&chain)?;
    Ok(chain)
}

pub(crate) fn validate_backup_chain_compatibility(
    chain: &[(PathBuf, BackupManifest)],
    strict_recovery: bool,
) -> Result<(), AedbError> {
    let current = parse_aedb_version(env!("CARGO_PKG_VERSION"))?;
    for (dir, manifest) in chain {
        let backup = parse_aedb_version(&manifest.aedb_version)?;
        if backup.major != current.major || backup.minor != current.minor {
            return Err(AedbError::Validation(format!(
                "backup {} was created by incompatible AEDB version {}",
                dir.display(),
                manifest.aedb_version
            )));
        }
        if backup.patch > current.patch {
            return Err(AedbError::Validation(format!(
                "backup {} was created by newer AEDB version {}",
                dir.display(),
                manifest.aedb_version
            )));
        }
        if strict_recovery && backup.patch < current.patch {
            return Err(AedbError::Validation(format!(
                "strict restore requires matching AEDB patch version; backup {} uses {}",
                dir.display(),
                manifest.aedb_version
            )));
        }
    }
    Ok(())
}

pub(crate) fn resolve_target_seq_for_time(
    chain: &[(PathBuf, BackupManifest)],
    target_time_micros: u64,
    hash_chain_required: bool,
    strict_recovery: bool,
) -> Result<u64, AedbError> {
    let Some((_, full)) = chain.first() else {
        return Err(AedbError::Validation("backup chain cannot be empty".into()));
    };
    let mut best_seq = full.checkpoint_seq;
    for (dir, manifest) in chain {
        let mut wal_paths = Vec::new();
        for seg in &manifest.wal_segments {
            wal_paths.push(resolve_backup_path(dir, &format!("wal_tail/{seg}"))?);
        }
        wal_paths.sort_by_key(|p| {
            p.file_name()
                .and_then(|n| segment_seq_from_name(&n.to_string_lossy()))
                .unwrap_or(0)
        });
        let valid_prefix_len = crate::recovery::scanner::validated_hash_chain_prefix_len(
            &wal_paths,
            hash_chain_required,
            strict_recovery,
        )?;
        for wal in wal_paths.into_iter().take(valid_prefix_len) {
            let file = File::open(&wal)?;
            if file.metadata()?.len() <= SEGMENT_HEADER_SIZE as u64 {
                continue;
            }
            let mut reader = BufReader::with_capacity(WAL_SEQUENTIAL_READ_BUFFER_BYTES, file);
            let mut header = [0u8; SEGMENT_HEADER_SIZE];
            reader.read_exact(&mut header)?;
            let mut frame_reader = FrameReader::new(reader);
            loop {
                match frame_reader.next_frame() {
                    Ok(Some(frame)) => {
                        if frame.commit_seq <= full.checkpoint_seq
                            || frame.commit_seq > manifest.wal_head_seq
                        {
                            continue;
                        }
                        if frame.timestamp_micros <= target_time_micros
                            && frame.commit_seq > best_seq
                        {
                            best_seq = frame.commit_seq;
                        }
                    }
                    Ok(None) | Err(FrameError::Truncation) => break,
                    Err(FrameError::Corruption) => {
                        return Err(AedbError::Validation(
                            "wal frame corruption detected while resolving target time".into(),
                        ));
                    }
                    Err(FrameError::Io(e)) => {
                        return Err(AedbError::Io(std::io::Error::other(e)));
                    }
                }
            }
        }
    }
    Ok(best_seq)
}

#[derive(Clone, Copy)]
struct AedbVersion {
    major: u64,
    minor: u64,
    patch: u64,
}

fn parse_aedb_version(raw: &str) -> Result<AedbVersion, AedbError> {
    let numeric = raw.split_once('-').map_or(raw, |(prefix, _)| prefix);
    let mut parts = numeric.split('.');
    let major = parts
        .next()
        .ok_or_else(|| AedbError::Validation(format!("invalid AEDB version: {raw}")))?
        .parse::<u64>()
        .map_err(|_| AedbError::Validation(format!("invalid AEDB version: {raw}")))?;
    let minor = parts
        .next()
        .ok_or_else(|| AedbError::Validation(format!("invalid AEDB version: {raw}")))?
        .parse::<u64>()
        .map_err(|_| AedbError::Validation(format!("invalid AEDB version: {raw}")))?;
    let patch = parts
        .next()
        .ok_or_else(|| AedbError::Validation(format!("invalid AEDB version: {raw}")))?
        .parse::<u64>()
        .map_err(|_| AedbError::Validation(format!("invalid AEDB version: {raw}")))?;
    if parts.next().is_some() {
        return Err(AedbError::Validation(format!(
            "invalid AEDB version: {raw}"
        )));
    }
    Ok(AedbVersion {
        major,
        minor,
        patch,
    })
}

pub(crate) fn verify_hash_chain_batch(
    wal_paths: &[PathBuf],
    mut expected_prev_hash: Option<[u8; 32]>,
) -> Result<Option<(u64, [u8; 32])>, AedbError> {
    let mut last: Option<(u64, [u8; 32])> = None;
    for wal in wal_paths {
        let mut file = File::open(wal)?;
        if file.metadata()?.len() < SEGMENT_HEADER_SIZE as u64 {
            return Err(AedbError::Decode("segment too small".into()));
        }
        let mut header = [0u8; SEGMENT_HEADER_SIZE];
        file.read_exact(&mut header)?;
        let parsed = SegmentHeader::from_bytes(&header)
            .map_err(|e| AedbError::Validation(format!("bad segment header: {e}")))?;
        if let Some(expected_prev) = expected_prev_hash
            && parsed.prev_segment_hash != expected_prev
        {
            return Err(AedbError::Validation("segment hash chain mismatch".into()));
        }

        let mut hasher = blake3::Hasher::new();
        hasher.update(&header);
        let mut buffer = vec![0u8; WAL_SEQUENTIAL_READ_BUFFER_BYTES];
        loop {
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            hasher.update(&buffer[..n]);
        }
        let hash = *blake3::Hasher::finalize(&hasher).as_bytes();
        expected_prev_hash = Some(hash);
        last = Some((parsed.segment_seq, hash));
    }
    Ok(last)
}
