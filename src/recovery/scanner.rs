use crate::error::AedbError;
use crate::wal::segment::{SEGMENT_HEADER_SIZE, SegmentHeader};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

pub fn scan_segments(data_dir: &Path) -> Result<Vec<PathBuf>, AedbError> {
    let mut segments: Vec<(u64, PathBuf)> = fs::read_dir(data_dir)?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            parse_seq(&name).map(|seq| (seq, path))
        })
        .collect();
    segments.sort_by_key(|(seq, _)| *seq);
    Ok(segments.into_iter().map(|(_, path)| path).collect())
}

pub fn verify_hash_chain(paths: &[PathBuf]) -> Result<(), AedbError> {
    let mut prev_hash = [0u8; 32];
    for path in paths {
        let mut file = fs::File::open(path)?;
        let mut header = [0u8; SEGMENT_HEADER_SIZE];
        file.read_exact(&mut header)?;
        let parsed = SegmentHeader::from_bytes(&header)
            .map_err(|e| AedbError::Validation(format!("bad segment header: {e}")))?;
        if parsed.prev_segment_hash != prev_hash {
            return Err(AedbError::Validation("segment hash chain mismatch".into()));
        }

        let mut hasher = blake3::Hasher::new();
        hasher.update(&header);
        let mut buffer = [0u8; 64 * 1024];
        loop {
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            hasher.update(&buffer[..n]);
        }
        prev_hash = *hasher.finalize().as_bytes();
    }
    Ok(())
}

pub fn verify_hash_chain_if_required(paths: &[PathBuf], required: bool) -> Result<(), AedbError> {
    if !required {
        return Ok(());
    }
    if paths
        .iter()
        .any(|path| fs::metadata(path).map_or(true, |m| m.len() < SEGMENT_HEADER_SIZE as u64))
    {
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
    if !required {
        return Ok(paths.len());
    }

    let mut prev_hash = [0u8; 32];
    let mut valid = 0usize;

    for path in paths {
        let meta = match fs::metadata(path) {
            Ok(m) => m,
            Err(e) => {
                if strict {
                    return Err(AedbError::Io(e));
                }
                break;
            }
        };
        if meta.len() < SEGMENT_HEADER_SIZE as u64 {
            if strict {
                return Err(AedbError::Decode("segment too small".into()));
            }
            break;
        }

        let mut file = match fs::File::open(path) {
            Ok(f) => f,
            Err(e) => {
                if strict {
                    return Err(AedbError::Io(e));
                }
                break;
            }
        };
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
        if parsed.prev_segment_hash != prev_hash {
            if strict {
                return Err(AedbError::Validation("segment hash chain mismatch".into()));
            }
            break;
        }

        let mut hasher = blake3::Hasher::new();
        hasher.update(&header);
        let mut buffer = [0u8; 64 * 1024];
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
                    return Ok(valid);
                }
            }
        }
        prev_hash = *hasher.finalize().as_bytes();
        valid += 1;
    }

    Ok(valid)
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
mod tests {
    use super::*;
    use crate::wal::segment::SegmentHeader;
    use std::io::Write;

    fn write_segment(path: &Path, header: SegmentHeader, payload: &[u8]) {
        let mut file = std::fs::File::create(path).expect("create segment");
        file.write_all(&header.to_bytes()).expect("write header");
        file.write_all(payload).expect("write payload");
        file.sync_all().expect("sync");
    }

    fn segment_hash(path: &Path) -> [u8; 32] {
        let bytes = std::fs::read(path).expect("read segment");
        *blake3::hash(&bytes).as_bytes()
    }

    #[test]
    fn permissive_mode_trims_invalid_chain_tail() {
        let dir = tempfile::tempdir().expect("tempdir");
        let seg1 = dir.path().join("segment_0000000000000001.aedbwal");
        let seg2 = dir.path().join("segment_0000000000000002.aedbwal");

        let h1 = SegmentHeader::new(1, 1, [0u8; 32]);
        write_segment(&seg1, h1, b"frame-data-1");
        let hash1 = segment_hash(&seg1);

        // Valid successor header for seq=2 would use hash1; intentionally break it.
        let h2_bad = SegmentHeader::new(1, 2, [9u8; 32]);
        write_segment(&seg2, h2_bad, b"frame-data-2");

        let paths = vec![seg1.clone(), seg2.clone()];
        assert_eq!(
            validated_hash_chain_prefix_len(&paths, true, false).expect("permissive"),
            1
        );

        let strict_err = validated_hash_chain_prefix_len(&paths, true, true).expect_err("strict");
        assert!(
            strict_err
                .to_string()
                .contains("segment hash chain mismatch")
        );

        let h2_good = SegmentHeader::new(1, 2, hash1);
        write_segment(&seg2, h2_good, b"frame-data-2");
        assert_eq!(
            validated_hash_chain_prefix_len(&paths, true, true).expect("strict valid"),
            2
        );
    }
}
