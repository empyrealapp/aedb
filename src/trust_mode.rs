use crate::config::AedbConfig;
use crate::error::AedbError;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;

const TRUST_MODE_MARKER_FILE: &str = "trust_mode.json";
const TRUST_MODE_MARKER_HMAC_FILE: &str = "trust_mode.hmac";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct TrustModeMarker {
    #[serde(default)]
    ever_non_strict_recovery: bool,
    #[serde(default)]
    ever_hash_chain_disabled: bool,
}

pub(crate) fn enforce_and_record_trust_mode(
    dir: &Path,
    config: &AedbConfig,
) -> Result<(), AedbError> {
    let mut marker = load_trust_mode_marker(dir, config.hmac_key())?.unwrap_or_default();
    if config.strict_recovery()
        && (marker.ever_non_strict_recovery || marker.ever_hash_chain_disabled)
    {
        return Err(AedbError::Validation(
            "strict open denied: data directory was previously opened with non-strict recovery or hash-chain disabled"
                .into(),
        ));
    }

    let mut changed = false;
    if !config.strict_recovery() && !marker.ever_non_strict_recovery {
        marker.ever_non_strict_recovery = true;
        changed = true;
    }
    if !config.hash_chain_required && !marker.ever_hash_chain_disabled {
        marker.ever_hash_chain_disabled = true;
        changed = true;
    }
    if changed {
        persist_trust_mode_marker(dir, &marker, config.hmac_key())?;
    }
    Ok(())
}

fn trust_mode_marker_path(dir: &Path) -> PathBuf {
    dir.join(TRUST_MODE_MARKER_FILE)
}

fn trust_mode_marker_hmac_path(dir: &Path) -> PathBuf {
    dir.join(TRUST_MODE_MARKER_HMAC_FILE)
}

fn hmac_hex(key: &[u8], bytes: &[u8]) -> Result<String, AedbError> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(|e| AedbError::InvalidConfig {
        message: format!("invalid hmac key: {e}"),
    })?;
    mac.update(bytes);
    Ok(hex::encode(mac.finalize().into_bytes()))
}

fn verify_trust_mode_marker_hmac(dir: &Path, key: &[u8], bytes: &[u8]) -> Result<(), AedbError> {
    let expected = fs::read_to_string(trust_mode_marker_hmac_path(dir)).map_err(|_| {
        AedbError::IntegrityError {
            message: "trust mode marker hmac missing".into(),
        }
    })?;
    let expected_bytes = hex::decode(expected.trim()).map_err(|_| AedbError::IntegrityError {
        message: "trust mode marker hmac must be hex".into(),
    })?;
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(|e| AedbError::InvalidConfig {
        message: format!("invalid hmac key: {e}"),
    })?;
    mac.update(bytes);
    mac.verify_slice(&expected_bytes)
        .map_err(|_| AedbError::IntegrityError {
            message: "trust mode marker hmac mismatch".into(),
        })
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), AedbError> {
    let dir = path
        .parent()
        .ok_or_else(|| AedbError::Validation(format!("path has no parent: {}", path.display())))?;
    let mut tmp = NamedTempFile::new_in(dir)?;
    tmp.write_all(bytes)?;
    tmp.flush()?;
    tmp.as_file().sync_all()?;
    tmp.persist(path).map_err(|e| AedbError::Io(e.error))?;
    fs::File::open(path)?.sync_all()?;
    fs::File::open(dir)?.sync_all()?;
    Ok(())
}

fn load_trust_mode_marker(
    dir: &Path,
    signing_key: Option<&[u8]>,
) -> Result<Option<TrustModeMarker>, AedbError> {
    let path = trust_mode_marker_path(dir);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(&path)?;
    if let Some(key) = signing_key {
        verify_trust_mode_marker_hmac(dir, key, &bytes)?;
    }
    let marker: TrustModeMarker =
        serde_json::from_slice(&bytes).map_err(|e| AedbError::Validation(e.to_string()))?;
    Ok(Some(marker))
}

fn persist_trust_mode_marker(
    dir: &Path,
    marker: &TrustModeMarker,
    signing_key: Option<&[u8]>,
) -> Result<(), AedbError> {
    let bytes = serde_json::to_vec(marker).map_err(|e| AedbError::Encode(e.to_string()))?;
    write_atomic(&trust_mode_marker_path(dir), &bytes)?;
    let hmac_path = trust_mode_marker_hmac_path(dir);
    if let Some(key) = signing_key {
        let signature = hmac_hex(key, &bytes)?;
        write_atomic(&hmac_path, signature.as_bytes())?;
    } else if hmac_path.exists() {
        fs::remove_file(&hmac_path)?;
        fs::File::open(dir)?.sync_all()?;
    }
    Ok(())
}
