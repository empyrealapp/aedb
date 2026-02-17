use crate::backup::sha256_file_hex;
use crate::error::AedbError;
use crate::manifest::schema::{Manifest, SegmentMeta};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::fs;
use std::io::Write;
use std::path::Path;
use tempfile::NamedTempFile;

pub fn write_manifest_atomic(manifest: &Manifest, dir: &Path) -> Result<(), AedbError> {
    write_manifest_atomic_signed(manifest, dir, None)
}

pub fn write_manifest_atomic_signed(
    manifest: &Manifest,
    dir: &Path,
    signing_key: Option<&[u8]>,
) -> Result<(), AedbError> {
    fs::create_dir_all(dir)?;
    let primary = dir.join("manifest.json");
    let prev = dir.join("manifest.json.prev");
    let sig = dir.join("manifest.hmac");
    let sig_prev = dir.join("manifest.hmac.prev");

    if primary.exists() {
        let data = fs::read(&primary)?;
        fs::write(&prev, data)?;
        fsync_file(&prev)?;
    }

    let mut tmp = NamedTempFile::new_in(dir)?;
    let bytes =
        serde_json::to_vec_pretty(manifest).map_err(|e| AedbError::Encode(e.to_string()))?;
    tmp.write_all(&bytes)?;
    tmp.flush()?;
    tmp.as_file().sync_all()?;
    tmp.persist(&primary).map_err(|e| AedbError::Io(e.error))?;
    if let Some(key) = signing_key {
        if sig.exists() {
            let data = fs::read(&sig)?;
            fs::write(&sig_prev, data)?;
            fsync_file(&sig_prev)?;
        }
        let signature = hmac_hex(key, &bytes)?;
        fs::write(&sig, signature)?;
        fsync_file(&sig)?;
    } else {
        let _ = fs::remove_file(&sig);
        let _ = fs::remove_file(&sig_prev);
    }
    fsync_dir(dir)?;
    Ok(())
}

pub fn load_manifest(dir: &Path) -> Result<Manifest, AedbError> {
    load_manifest_signed_mode(dir, None, false)
}

pub fn load_manifest_signed(dir: &Path, signing_key: Option<&[u8]>) -> Result<Manifest, AedbError> {
    load_manifest_signed_mode(dir, signing_key, false)
}

pub fn load_manifest_signed_mode(
    dir: &Path,
    signing_key: Option<&[u8]>,
    strict_recovery: bool,
) -> Result<Manifest, AedbError> {
    let primary = dir.join("manifest.json");
    if let Ok(m) = try_read_manifest_signed(dir, &primary, signing_key) {
        return Ok(m);
    }

    let prev = dir.join("manifest.json.prev");
    if let Ok(m) = try_read_manifest_signed(dir, &prev, signing_key) {
        return Ok(m);
    }

    if signing_key.is_some() || strict_recovery {
        return Err(AedbError::Unavailable {
            message: "manifest unavailable and reconstruction disabled".into(),
        });
    }

    reconstruct_manifest(dir)
}

fn try_read_manifest_signed(
    dir: &Path,
    path: &Path,
    signing_key: Option<&[u8]>,
) -> Result<Manifest, AedbError> {
    let bytes = fs::read(path)?;
    if let Some(key) = signing_key {
        verify_manifest_hmac(dir, path, key, &bytes)?;
    }
    serde_json::from_slice(&bytes).map_err(|e| AedbError::Decode(e.to_string()))
}

fn verify_manifest_hmac(
    dir: &Path,
    path: &Path,
    key: &[u8],
    bytes: &[u8],
) -> Result<(), AedbError> {
    let primary = dir.join("manifest.json");
    let sig_path = if path == primary {
        dir.join("manifest.hmac")
    } else {
        dir.join("manifest.hmac.prev")
    };
    let expected = fs::read_to_string(&sig_path).map_err(|_| AedbError::IntegrityError {
        message: "manifest hmac missing".into(),
    })?;
    let expected_bytes = decode_hex(expected.trim())?;
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(|e| AedbError::InvalidConfig {
        message: format!("invalid hmac key: {e}"),
    })?;
    mac.update(bytes);
    mac.verify_slice(&expected_bytes)
        .map_err(|_| AedbError::IntegrityError {
            message: "manifest hmac mismatch".into(),
        })
}

fn hmac_hex(key: &[u8], bytes: &[u8]) -> Result<String, AedbError> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(|e| AedbError::InvalidConfig {
        message: format!("invalid hmac key: {e}"),
    })?;
    mac.update(bytes);
    Ok(bytes_hex(&mac.finalize().into_bytes()))
}

fn bytes_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

fn decode_hex(input: &str) -> Result<Vec<u8>, AedbError> {
    if !input.len().is_multiple_of(2) {
        return Err(AedbError::IntegrityError {
            message: "manifest hmac must be hex".into(),
        });
    }
    let mut out = Vec::with_capacity(input.len() / 2);
    for pair in input.as_bytes().chunks_exact(2) {
        let hi = hex_nibble(pair[0]).ok_or_else(|| AedbError::IntegrityError {
            message: "manifest hmac must be hex".into(),
        })?;
        let lo = hex_nibble(pair[1]).ok_or_else(|| AedbError::IntegrityError {
            message: "manifest hmac must be hex".into(),
        })?;
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

fn reconstruct_manifest(dir: &Path) -> Result<Manifest, AedbError> {
    let mut manifest = Manifest::default();

    let mut checkpoints = Vec::new();
    let mut segments = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(seq) = parse_checkpoint_seq(&name) {
            checkpoints.push((seq, name.clone()));
        }
        if let Some(seq) = parse_segment_seq(&name) {
            segments.push((seq, name.clone()));
        }
    }

    checkpoints.sort_by_key(|(seq, _)| *seq);
    segments.sort_by_key(|(seq, _)| *seq);

    if let Some((seq, _)) = checkpoints.last() {
        manifest.visible_seq = *seq;
        manifest.durable_seq = *seq;
    }
    manifest.checkpoints = checkpoints
        .iter()
        .map(
            |(seq, filename)| crate::checkpoint::writer::CheckpointMeta {
                filename: filename.clone(),
                seq: *seq,
                sha256_hex: sha256_file_hex(&dir.join(filename)).unwrap_or_default(),
                created_at_micros: 0,
                key_id: None,
            },
        )
        .collect();
    if let Some((seq, _)) = segments.last() {
        manifest.active_segment_seq = *seq;
    }
    manifest.segments = segments
        .into_iter()
        .map(|(segment_seq, filename)| {
            let path = dir.join(&filename);
            let sha256_hex = sha256_file_hex(&path).unwrap_or_default();
            let size_bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            SegmentMeta {
                filename,
                segment_seq,
                sha256_hex,
                size_bytes,
            }
        })
        .collect();
    Ok(manifest)
}

fn parse_checkpoint_seq(name: &str) -> Option<u64> {
    if !name.starts_with("checkpoint_") || !name.ends_with(".aedb.zst") {
        return None;
    }
    let middle = name
        .trim_start_matches("checkpoint_")
        .trim_end_matches(".aedb.zst");
    middle.parse::<u64>().ok()
}

fn parse_segment_seq(name: &str) -> Option<u64> {
    if !name.starts_with("segment_") || !name.ends_with(".aedbwal") {
        return None;
    }
    let middle = name
        .trim_start_matches("segment_")
        .trim_end_matches(".aedbwal");
    middle.parse::<u64>().ok()
}

fn fsync_file(path: &Path) -> Result<(), AedbError> {
    let file = fs::OpenOptions::new().read(true).open(path)?;
    file.sync_all()?;
    Ok(())
}

fn fsync_dir(path: &Path) -> Result<(), AedbError> {
    let dir = fs::File::open(path)?;
    dir.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        load_manifest, load_manifest_signed, load_manifest_signed_mode, write_manifest_atomic,
        write_manifest_atomic_signed,
    };
    use crate::manifest::schema::Manifest;
    use tempfile::tempdir;

    #[test]
    fn manifest_roundtrip_and_prev_fallback() {
        let dir = tempdir().expect("temp");
        let m1 = Manifest {
            durable_seq: 1,
            visible_seq: 1,
            active_segment_seq: 1,
            checkpoints: vec![],
            segments: vec![],
        };
        write_manifest_atomic(&m1, dir.path()).expect("write 1");

        let m2 = Manifest {
            durable_seq: 2,
            visible_seq: 2,
            active_segment_seq: 2,
            checkpoints: vec![],
            segments: vec![],
        };
        write_manifest_atomic(&m2, dir.path()).expect("write 2");

        let loaded = load_manifest(dir.path()).expect("load primary");
        assert_eq!(loaded, m2);

        std::fs::write(dir.path().join("manifest.json"), b"{broken").expect("corrupt primary");
        let fallback = load_manifest(dir.path()).expect("fallback");
        assert_eq!(fallback, m1);
    }

    #[test]
    fn manifest_hmac_sign_and_verify() {
        let dir = tempdir().expect("temp");
        let m = Manifest {
            durable_seq: 3,
            visible_seq: 3,
            active_segment_seq: 3,
            checkpoints: vec![],
            segments: vec![],
        };
        let key = b"super-secret-key";
        write_manifest_atomic_signed(&m, dir.path(), Some(key)).expect("write signed");
        let loaded = load_manifest_signed(dir.path(), Some(key)).expect("verify");
        assert_eq!(loaded, m);
        std::fs::write(dir.path().join("manifest.hmac"), "bad").expect("corrupt sig");
        std::fs::write(dir.path().join("manifest.hmac.prev"), "bad").expect("corrupt sig prev");
        assert!(load_manifest_signed(dir.path(), Some(key)).is_err());
    }

    #[test]
    fn strict_recovery_rejects_reconstruction_without_manifest() {
        let dir = tempdir().expect("temp");
        std::fs::write(
            dir.path().join("segment_0000000000000001.aedbwal"),
            b"placeholder",
        )
        .expect("segment");
        let err = load_manifest_signed_mode(dir.path(), None, true).expect_err("strict fail");
        assert!(format!("{err}").contains("reconstruction disabled"));
    }

    #[test]
    fn reconstruction_collects_all_checkpoints() {
        let dir = tempdir().expect("temp");
        std::fs::write(
            dir.path().join("checkpoint_0000000000000002.aedb.zst"),
            b"x",
        )
        .expect("cp2");
        std::fs::write(
            dir.path().join("checkpoint_0000000000000005.aedb.zst"),
            b"y",
        )
        .expect("cp5");
        let m = load_manifest(dir.path()).expect("reconstruct");
        assert_eq!(m.checkpoints.len(), 2);
        assert_eq!(m.checkpoints[0].seq, 2);
        assert_eq!(m.checkpoints[1].seq, 5);
        assert_eq!(m.visible_seq, 5);
        assert_eq!(m.durable_seq, 5);
    }
}
