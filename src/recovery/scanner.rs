use crate::error::AedbError;
use crate::wal::segment::{SEGMENT_HEADER_SIZE, SegmentHeader};
use std::fs;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

const WAL_SCAN_BUFFER_BYTES: usize = 1024 * 1024;

pub fn scan_segments(data_dir: &Path) -> Result<Vec<PathBuf>, AedbError> {
    Ok(scan_segment_entries(data_dir)?
        .into_iter()
        .map(|(_, path)| path)
        .collect())
}

/// Like [`scan_segments`] but keeps each segment's parsed sequence number,
/// sorted ascending. Used by recovery to discover WAL segments created after
/// the manifest was last written (post-checkpoint rotations).
pub(crate) fn scan_segment_entries(data_dir: &Path) -> Result<Vec<(u64, PathBuf)>, AedbError> {
    let mut segments: Vec<(u64, PathBuf)> = fs::read_dir(data_dir)?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            parse_seq(&name).map(|seq| (seq, path))
        })
        .collect();
    segments.sort_by_key(|(seq, _)| *seq);
    Ok(segments)
}

pub fn verify_hash_chain(paths: &[PathBuf]) -> Result<(), AedbError> {
    let mut prev_hash = [0u8; 32];
    for path in paths {
        let mut file = BufReader::with_capacity(WAL_SCAN_BUFFER_BYTES, fs::File::open(path)?);
        let mut header = [0u8; SEGMENT_HEADER_SIZE];
        file.read_exact(&mut header)?;
        let parsed = SegmentHeader::from_bytes(&header)
            .map_err(|e| AedbError::Validation(format!("bad segment header: {e}")))?;
        if parsed.prev_segment_hash != prev_hash {
            return Err(AedbError::Validation("segment hash chain mismatch".into()));
        }

        let mut hasher = blake3::Hasher::new();
        hasher.update(&header);
        let mut buffer = vec![0u8; WAL_SCAN_BUFFER_BYTES];
        loop {
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            hasher.update(&buffer[..n]);
        }
        prev_hash = *blake3::Hasher::finalize(&hasher).as_bytes();
    }
    Ok(())
}

pub fn verify_hash_chain_if_required(paths: &[PathBuf], required: bool) -> Result<(), AedbError> {
    if !required {
        return Ok(());
    }
    if paths.iter().any(|path| {
        fs::metadata(path).map_or(true, |metadata| metadata.len() < SEGMENT_HEADER_SIZE as u64)
    }) {
        return Err(AedbError::Decode("segment too small".into()));
    }
    verify_hash_chain(paths)
}

/// Return the number of prefix segments that pass hash-chain validation.
///
/// - If `required=false`, all segments are considered valid.
/// - If `required=true` and `strict=true`, any validation problem returns an error.
/// - If `required=true` and `strict=false`, validation stops at the first bad segment and
///   returns the validated prefix length.
pub fn validated_hash_chain_prefix_len(
    paths: &[PathBuf],
    required: bool,
    strict: bool,
) -> Result<usize, AedbError> {
    validated_hash_chain_prefix_len_from_checkpoint(paths, required, strict, false)
}

pub fn validated_hash_chain_prefix_len_from_checkpoint(
    paths: &[PathBuf],
    required: bool,
    strict: bool,
    allow_checkpoint_tail_anchor: bool,
) -> Result<usize, AedbError> {
    if !required {
        return Ok(paths.len());
    }

    let mut prev_hash = [0u8; 32];
    let mut valid_segment_count = 0usize;

    for path in paths {
        let segment_metadata = match fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(e) => {
                if strict {
                    return Err(AedbError::Io(e));
                }
                break;
            }
        };
        let segment_size_bytes = segment_metadata.len();
        if segment_size_bytes < SEGMENT_HEADER_SIZE as u64 {
            if strict {
                return Err(AedbError::Decode("segment too small".into()));
            }
            break;
        }

        let file = match fs::File::open(path) {
            Ok(f) => f,
            Err(e) => {
                if strict {
                    return Err(AedbError::Io(e));
                }
                break;
            }
        };
        let mut file = BufReader::with_capacity(WAL_SCAN_BUFFER_BYTES, file);
        let mut header = [0u8; SEGMENT_HEADER_SIZE];
        if let Err(e) = file.read_exact(&mut header) {
            if strict {
                return Err(AedbError::Io(e));
            }
            break;
        }
        let parsed = match SegmentHeader::from_bytes(&header) {
            Ok(h) => h,
            Err(e) => {
                if strict {
                    return Err(AedbError::Validation(format!("bad segment header: {e}")));
                }
                break;
            }
        };
        if parsed.prev_segment_hash != prev_hash
            && !(allow_checkpoint_tail_anchor && valid_segment_count == 0 && parsed.segment_seq > 1)
        {
            if strict {
                return Err(AedbError::Validation("segment hash chain mismatch".into()));
            }
            break;
        }

        let mut hasher = blake3::Hasher::new();
        hasher.update(&header);
        let mut buffer = vec![0u8; WAL_SCAN_BUFFER_BYTES];
        loop {
            match file.read(&mut buffer) {
                Ok(0) => break,
                Ok(n) => {
                    hasher.update(&buffer[..n]);
                }
                Err(e) => {
                    if strict {
                        return Err(AedbError::Io(e));
                    }
                    return Ok(valid_segment_count);
                }
            }
        }
        prev_hash = *blake3::Hasher::finalize(&hasher).as_bytes();
        valid_segment_count += 1;
    }

    debug_assert!(valid_segment_count <= paths.len());
    Ok(valid_segment_count)
}

fn parse_seq(name: &str) -> Option<u64> {
    if !name.starts_with("segment_") || !name.ends_with(".aedbwal") {
        return None;
    }
    let middle = name
        .trim_start_matches("segment_")
        .trim_end_matches(".aedbwal");
    middle.parse::<u64>().ok()
}

#[cfg(test)]
mod tests;
