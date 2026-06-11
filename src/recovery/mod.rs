mod integrity;
pub mod replay;
pub mod scanner;

use crate::backup::sha256_hex;
use crate::catalog::Catalog;
use crate::checkpoint::loader::load_checkpoint_bytes_with_key;
use crate::commit::tx::{IdempotencyKey, IdempotencyRecord};
use crate::config::{AedbConfig, RecoveryMode, StorageMode};
use crate::error::{AedbError, RecoveryIntegrityDiagnostic, RecoveryIntegrityKind};
use crate::manifest::atomic::load_manifest_signed_mode;
use crate::manifest::schema::Manifest;
use crate::recovery::integrity::{is_valid_sha256_hex, sha256_prefix_hex};
use crate::recovery::replay::{ReplaySegmentsRequest, replay_segments};
use crate::recovery::scanner::scan_segments;
use crate::storage::keyspace::Keyspace;
use crate::storage::kv_segment::KvSegmentStore;
use crate::storage::value_store::PersistentValueStore;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{info, warn};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredState {
    pub keyspace: Keyspace,
    pub catalog: Catalog,
    pub current_seq: u64,
    pub idempotency: HashMap<IdempotencyKey, IdempotencyRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryOutcome {
    pub state: RecoveredState,
    pub warnings: Vec<RecoveryIntegrityDiagnostic>,
}

type LoadedCheckpoint = (
    Keyspace,
    Catalog,
    u64,
    HashMap<IdempotencyKey, IdempotencyRecord>,
);

pub fn recover(data_dir: &Path) -> Result<RecoveredState, AedbError> {
    recover_with_config(data_dir, &AedbConfig::default())
}

pub fn recover_with_config(
    data_dir: &Path,
    config: &AedbConfig,
) -> Result<RecoveredState, AedbError> {
    recover_with_config_and_diagnostics(data_dir, config).map(|outcome| outcome.state)
}

pub fn recover_with_config_and_diagnostics(
    data_dir: &Path,
    config: &AedbConfig,
) -> Result<RecoveryOutcome, AedbError> {
    recover_with_optional_target(data_dir, config, None)
}

pub fn recover_at_seq(data_dir: &Path, target_seq: u64) -> Result<RecoveredState, AedbError> {
    recover_at_seq_with_config(data_dir, target_seq, &AedbConfig::default())
}

pub fn recover_at_seq_with_config(
    data_dir: &Path,
    target_seq: u64,
    config: &AedbConfig,
) -> Result<RecoveredState, AedbError> {
    recover_with_optional_target(data_dir, config, Some(target_seq)).map(|outcome| outcome.state)
}

fn recover_with_optional_target(
    data_dir: &Path,
    config: &AedbConfig,
    target_seq: Option<u64>,
) -> Result<RecoveryOutcome, AedbError> {
    info!("recovery: load manifest");
    let manifest =
        load_manifest_signed_mode(data_dir, config.hmac_key(), config.strict_recovery())?;
    let explicit_manifest = has_explicit_manifest(data_dir);
    let replay_to = effective_replay_target(target_seq, &manifest, explicit_manifest);
    let mut keyspace = Keyspace::with_backend(config.primary_index_backend);
    let mut catalog = Catalog::default();
    let mut seq = 0u64;
    let mut idempotency = HashMap::new();

    if let Some((ks, cat, loaded_seq, checkpoint_idempotency)) =
        load_latest_valid_checkpoint(&manifest, data_dir, config, replay_to)?
    {
        keyspace = ks;
        keyspace.set_backend(config.primary_index_backend);
        catalog = cat;
        seq = replay_to
            .map(|target_seq| loaded_seq.min(target_seq))
            .unwrap_or(loaded_seq);
        idempotency = checkpoint_idempotency;
    }

    info!("recovery: scan segments");
    let (segments, replay_limits) =
        segment_paths_for_replay(data_dir, &manifest, explicit_manifest, config)?;
    info!("recovery: replay segments");
    let replayed = replay_segments(ReplaySegmentsRequest {
        segments: &segments,
        from_seq_exclusive: seq,
        to_seq_inclusive: replay_to,
        segment_replay_byte_limits: Some(&replay_limits.limits),
        hash_chain_required: config.hash_chain_required,
        strict_recovery: config.strict_recovery(),
        keyspace: &mut keyspace,
        catalog: &mut catalog,
        idempotency: &mut idempotency,
    })?;
    attach_configured_value_store(&mut keyspace, config, data_dir)?;
    Ok(RecoveryOutcome {
        state: RecoveredState {
            keyspace,
            catalog,
            current_seq: replayed,
            idempotency,
        },
        warnings: replay_limits.warnings,
    })
}

fn effective_replay_target(
    target_seq: Option<u64>,
    manifest: &Manifest,
    explicit_manifest: bool,
) -> Option<u64> {
    match (target_seq, explicit_manifest) {
        (Some(target_seq), true) => Some(target_seq.min(manifest.durable_seq)),
        (Some(target_seq), false) => Some(target_seq),
        (None, true) => Some(manifest.durable_seq),
        (None, false) => None,
    }
}

fn attach_configured_value_store(
    keyspace: &mut Keyspace,
    config: &AedbConfig,
    data_dir: &Path,
) -> Result<(), AedbError> {
    if matches!(config.storage_mode, StorageMode::DiskBacked) {
        let store = Arc::new(PersistentValueStore::open_with_hot_cache_bytes(
            data_dir,
            config.persistent_value_hot_cache_bytes,
        )?);
        let segment_store = Arc::new(KvSegmentStore::open_with_block_cache_bytes(
            data_dir,
            config.kv_segment_block_cache_bytes,
        )?);
        keyspace
            .attach_persistent_value_store(store, config.persistent_value_inline_threshold_bytes)?;
        keyspace.attach_kv_segment_store(segment_store);
        keyspace.spill_kv_values_to_memory_target(config.max_memory_estimate_bytes)?;
        keyspace.flush_kv_to_segments_to_memory_target(config.max_memory_estimate_bytes)?;
    } else {
        keyspace.detach_persistent_value_store();
    }
    Ok(())
}

fn has_explicit_manifest(data_dir: &Path) -> bool {
    data_dir.join("manifest.json").exists() || data_dir.join("manifest.json.prev").exists()
}

#[derive(Debug, Default)]
struct ReplaySelection {
    limits: HashMap<PathBuf, u64>,
    warnings: Vec<RecoveryIntegrityDiagnostic>,
}

fn segment_paths_for_replay(
    data_dir: &Path,
    manifest: &Manifest,
    explicit_manifest: bool,
    config: &AedbConfig,
) -> Result<(Vec<PathBuf>, ReplaySelection), AedbError> {
    if !explicit_manifest {
        return Ok((scan_segments(data_dir)?, ReplaySelection::default()));
    }

    let mut segments = manifest.segments.clone();
    segments.sort_by_key(|segment| segment.segment_seq);
    let mut segment_paths = Vec::with_capacity(segments.len());
    let mut replay_selection = ReplaySelection::default();
    let active_segment_seq = manifest.active_segment_seq;
    for segment in segments {
        let is_active_segment = segment.segment_seq == active_segment_seq;
        let path = data_dir.join(&segment.filename);
        if !path.exists() {
            let diagnostic = manifest_segment_diagnostic(
                RecoveryIntegrityKind::ManifestWalSegmentMissing,
                data_dir,
                manifest,
                &segment,
                config.recovery_mode,
                SegmentDiagnosticDetails::default(),
            );
            handle_manifest_segment_diagnostic(&mut replay_selection, diagnostic)?;
            continue;
        }
        let actual_size_bytes = std::fs::metadata(&path)?.len();
        if segment.size_bytes == 0 {
            if config.strict_recovery() && !is_active_segment {
                let diagnostic = manifest_segment_diagnostic(
                    RecoveryIntegrityKind::ManifestWalSegmentMissingSizeMetadata,
                    data_dir,
                    manifest,
                    &segment,
                    config.recovery_mode,
                    SegmentDiagnosticDetails::with_actual_size(actual_size_bytes),
                );
                handle_manifest_segment_diagnostic(&mut replay_selection, diagnostic)?;
            }
            warn!(
                filename = %segment.filename,
                "recovery: wal segment size metadata missing, skipping strict size check"
            );
        } else {
            if actual_size_bytes < segment.size_bytes {
                let diagnostic = manifest_segment_diagnostic(
                    RecoveryIntegrityKind::ManifestWalSegmentTruncated,
                    data_dir,
                    manifest,
                    &segment,
                    config.recovery_mode,
                    SegmentDiagnosticDetails::with_actual_size(actual_size_bytes),
                );
                handle_manifest_segment_diagnostic(&mut replay_selection, diagnostic)?;
                continue;
            }
            if actual_size_bytes > segment.size_bytes
                && !is_active_segment
                && config.strict_recovery()
            {
                let diagnostic = manifest_segment_diagnostic(
                    RecoveryIntegrityKind::ManifestWalSegmentSizeMismatch,
                    data_dir,
                    manifest,
                    &segment,
                    config.recovery_mode,
                    SegmentDiagnosticDetails::with_actual_size(actual_size_bytes),
                );
                handle_manifest_segment_diagnostic(&mut replay_selection, diagnostic)?;
            }
        }
        if segment.sha256_hex.is_empty() {
            if config.strict_recovery() && !is_active_segment {
                let diagnostic = manifest_segment_diagnostic(
                    RecoveryIntegrityKind::ManifestWalSegmentMissingChecksumMetadata,
                    data_dir,
                    manifest,
                    &segment,
                    config.recovery_mode,
                    SegmentDiagnosticDetails::with_actual_size(actual_size_bytes),
                );
                handle_manifest_segment_diagnostic(&mut replay_selection, diagnostic)?;
            }
            warn!(
                filename = %segment.filename,
                "recovery: wal segment sha256 metadata missing, skipping integrity check in permissive mode"
            );
        } else if !is_valid_sha256_hex(&segment.sha256_hex) {
            if config.strict_recovery() && !is_active_segment {
                let diagnostic = manifest_segment_diagnostic(
                    RecoveryIntegrityKind::ManifestWalSegmentInvalidChecksumMetadata,
                    data_dir,
                    manifest,
                    &segment,
                    config.recovery_mode,
                    SegmentDiagnosticDetails::with_actual_size(actual_size_bytes),
                );
                handle_manifest_segment_diagnostic(&mut replay_selection, diagnostic)?;
            }
            warn!(
                filename = %segment.filename,
                "recovery: invalid wal segment sha256 metadata, skipping integrity check"
            );
        } else {
            let hash_len = if segment.size_bytes > 0 {
                segment.size_bytes
            } else {
                actual_size_bytes
            };
            let actual = sha256_prefix_hex(&path, hash_len)?;
            if actual != segment.sha256_hex {
                let diagnostic = manifest_segment_diagnostic(
                    RecoveryIntegrityKind::ManifestWalSegmentChecksumMismatch,
                    data_dir,
                    manifest,
                    &segment,
                    config.recovery_mode,
                    SegmentDiagnosticDetails {
                        actual_size_bytes: Some(actual_size_bytes),
                        actual_sha256_hex: Some(actual),
                    },
                );
                return Err(recovery_integrity_error(diagnostic));
            }
        }
        let replay_limit_path = path.clone();
        segment_paths.push(path);
        if segment.size_bytes > 0 {
            replay_selection
                .limits
                .insert(replay_limit_path, segment.size_bytes);
        }
    }
    Ok((segment_paths, replay_selection))
}

#[derive(Default)]
struct SegmentDiagnosticDetails {
    actual_size_bytes: Option<u64>,
    actual_sha256_hex: Option<String>,
}

impl SegmentDiagnosticDetails {
    fn with_actual_size(actual_size_bytes: u64) -> Self {
        Self {
            actual_size_bytes: Some(actual_size_bytes),
            ..Self::default()
        }
    }
}

fn manifest_segment_diagnostic(
    kind: RecoveryIntegrityKind,
    data_dir: &Path,
    manifest: &Manifest,
    segment: &crate::manifest::schema::SegmentMeta,
    recovery_mode: RecoveryMode,
    details: SegmentDiagnosticDetails,
) -> RecoveryIntegrityDiagnostic {
    RecoveryIntegrityDiagnostic {
        kind,
        segment_filename: segment.filename.clone(),
        manifest_path: data_dir.join("manifest.json"),
        wal_dir: data_dir.to_path_buf(),
        recovery_mode,
        durable_seq: manifest.durable_seq,
        visible_seq: manifest.visible_seq,
        active_segment_seq: manifest.active_segment_seq,
        segment_seq: segment.segment_seq,
        latest_checkpoint_seq: manifest.checkpoints.iter().map(|cp| cp.seq).max(),
        expected_size_bytes: (segment.size_bytes > 0).then_some(segment.size_bytes),
        actual_size_bytes: details.actual_size_bytes,
        expected_sha256_hex: (!segment.sha256_hex.is_empty()).then(|| segment.sha256_hex.clone()),
        actual_sha256_hex: details.actual_sha256_hex,
    }
}

fn handle_manifest_segment_diagnostic(
    replay_selection: &mut ReplaySelection,
    diagnostic: RecoveryIntegrityDiagnostic,
) -> Result<(), AedbError> {
    match diagnostic.recovery_mode {
        RecoveryMode::Strict => Err(recovery_integrity_error(diagnostic)),
        RecoveryMode::Permissive => {
            warn!(
                kind = %diagnostic.kind,
                filename = %diagnostic.segment_filename,
                "recovery: skipping manifest-referenced wal segment in permissive mode"
            );
            replay_selection.warnings.push(diagnostic);
            Ok(())
        }
    }
}

fn recovery_integrity_error(diagnostic: RecoveryIntegrityDiagnostic) -> AedbError {
    AedbError::RecoveryIntegrity {
        diagnostic: Box::new(diagnostic),
    }
}

fn load_latest_valid_checkpoint(
    manifest: &crate::manifest::schema::Manifest,
    data_dir: &Path,
    config: &AedbConfig,
    max_seq: Option<u64>,
) -> Result<Option<LoadedCheckpoint>, AedbError> {
    let mut candidates = 0usize;
    let mut failures = 0usize;
    for cp in manifest.checkpoints.iter().rev() {
        if let Some(limit) = max_seq
            && cp.seq > limit
        {
            continue;
        }
        let cp_path = data_dir.join(&cp.filename);
        if !cp_path.exists() {
            continue;
        }
        candidates = candidates.saturating_add(1);
        // Read the payload once and reuse it for both the manifest-digest check
        // and decoding, instead of streaming the file for a hash and then
        // reading it a second time inside the loader.
        let bytes = fs::read(&cp_path)?;
        let mut manifest_hash_verified = false;
        if cp.sha256_hex.is_empty() {
            if config.strict_recovery() {
                failures = failures.saturating_add(1);
                warn!(
                    filename = %cp.filename,
                    seq = cp.seq,
                    "recovery: checkpoint metadata missing sha256 in strict mode"
                );
                continue;
            }
        } else if !is_valid_sha256_hex(&cp.sha256_hex) {
            failures = failures.saturating_add(1);
            warn!(
                filename = %cp.filename,
                seq = cp.seq,
                "recovery: checkpoint metadata has invalid sha256"
            );
            continue;
        } else {
            let actual = sha256_hex(&bytes);
            if actual != cp.sha256_hex {
                failures = failures.saturating_add(1);
                warn!(
                    filename = %cp.filename,
                    seq = cp.seq,
                    "recovery: checkpoint sha256 mismatch"
                );
                continue;
            }
            manifest_hash_verified = true;
        }
        info!("recovery: load checkpoint {}", cp.filename);
        // Skip the loader's internal trailer re-hash when the full payload was
        // already verified against the manifest digest above.
        match load_checkpoint_bytes_with_key(
            &bytes,
            config.checkpoint_key(),
            !manifest_hash_verified,
        ) {
            Ok(loaded) => return Ok(Some(loaded)),
            Err(err) => {
                failures = failures.saturating_add(1);
                warn!(
                    filename = %cp.filename,
                    seq = cp.seq,
                    error = %err,
                    "recovery: checkpoint load failed, trying older checkpoint"
                );
            }
        }
    }
    if config.strict_recovery() && candidates > 0 && failures == candidates {
        return Err(AedbError::Validation(
            "all referenced checkpoints failed to load in strict recovery mode".into(),
        ));
    }
    Ok(None)
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
