pub mod replay;
pub mod scanner;

use crate::backup::sha256_file_hex;
use crate::catalog::Catalog;
use crate::checkpoint::loader::load_checkpoint_with_key;
use crate::commit::tx::{IdempotencyKey, IdempotencyRecord};
use crate::config::{AedbConfig, RecoveryMode, StorageMode};
use crate::error::{AedbError, RecoveryIntegrityDiagnostic, RecoveryIntegrityKind};
use crate::manifest::atomic::load_manifest_signed_mode;
use crate::manifest::schema::Manifest;
use crate::recovery::replay::replay_segments;
use crate::recovery::scanner::scan_segments;
use crate::storage::keyspace::Keyspace;
use crate::storage::kv_segment::KvSegmentStore;
use crate::storage::value_store::PersistentValueStore;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
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
    info!("recovery: load manifest");
    let manifest =
        load_manifest_signed_mode(data_dir, config.hmac_key(), config.strict_recovery())?;
    let explicit_manifest = has_explicit_manifest(data_dir);
    let mut keyspace = Keyspace::with_backend(config.primary_index_backend);
    let mut catalog = Catalog::default();
    let mut seq = 0u64;
    let mut idempotency = HashMap::new();

    if let Some((ks, cat, loaded_seq, checkpoint_idempotency)) =
        load_latest_valid_checkpoint(&manifest, data_dir, config, None)?
    {
        keyspace = ks;
        keyspace.set_backend(config.primary_index_backend);
        catalog = cat;
        seq = loaded_seq;
        idempotency = checkpoint_idempotency;
    }

    info!("recovery: scan segments");
    let (segments, replay_limits) =
        segment_paths_for_replay(data_dir, &manifest, explicit_manifest, config)?;
    info!("recovery: replay segments");
    let replay_to = if explicit_manifest {
        Some(manifest.durable_seq)
    } else {
        None
    };
    let replayed = replay_segments(
        &segments,
        seq,
        replay_to,
        Some(&replay_limits.limits),
        config.hash_chain_required,
        config.strict_recovery(),
        &mut keyspace,
        &mut catalog,
        &mut idempotency,
    )?;
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

pub fn recover_at_seq(data_dir: &Path, target_seq: u64) -> Result<RecoveredState, AedbError> {
    recover_at_seq_with_config(data_dir, target_seq, &AedbConfig::default())
}

pub fn recover_at_seq_with_config(
    data_dir: &Path,
    target_seq: u64,
    config: &AedbConfig,
) -> Result<RecoveredState, AedbError> {
    let manifest =
        load_manifest_signed_mode(data_dir, config.hmac_key(), config.strict_recovery())?;
    let explicit_manifest = has_explicit_manifest(data_dir);
    let effective_target = if explicit_manifest {
        target_seq.min(manifest.durable_seq)
    } else {
        target_seq
    };
    let mut keyspace = Keyspace::with_backend(config.primary_index_backend);
    let mut catalog = Catalog::default();
    let mut seq = 0u64;
    let mut idempotency = HashMap::new();

    if let Some((ks, cat, loaded_seq, checkpoint_idempotency)) =
        load_latest_valid_checkpoint(&manifest, data_dir, config, Some(effective_target))?
    {
        keyspace = ks;
        keyspace.set_backend(config.primary_index_backend);
        catalog = cat;
        seq = loaded_seq.min(effective_target);
        idempotency = checkpoint_idempotency;
    }

    let (segments, replay_limits) =
        segment_paths_for_replay(data_dir, &manifest, explicit_manifest, config)?;
    let replayed = replay_segments(
        &segments,
        seq,
        Some(effective_target),
        Some(&replay_limits.limits),
        config.hash_chain_required,
        config.strict_recovery(),
        &mut keyspace,
        &mut catalog,
        &mut idempotency,
    )?;
    attach_configured_value_store(&mut keyspace, config, data_dir)?;
    Ok(RecoveredState {
        keyspace,
        catalog,
        current_seq: replayed,
        idempotency,
    })
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
                None,
                None,
                None,
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
                    Some(actual_size_bytes),
                    None,
                    None,
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
                    Some(actual_size_bytes),
                    None,
                    None,
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
                    Some(actual_size_bytes),
                    None,
                    None,
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
                    Some(actual_size_bytes),
                    None,
                    None,
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
                    Some(actual_size_bytes),
                    None,
                    None,
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
                    Some(actual_size_bytes),
                    None,
                    Some(actual),
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

fn manifest_segment_diagnostic(
    kind: RecoveryIntegrityKind,
    data_dir: &Path,
    manifest: &Manifest,
    segment: &crate::manifest::schema::SegmentMeta,
    recovery_mode: RecoveryMode,
    actual_size_bytes: Option<u64>,
    expected_sha256_hex: Option<String>,
    actual_sha256_hex: Option<String>,
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
        actual_size_bytes,
        expected_sha256_hex: expected_sha256_hex
            .or_else(|| (!segment.sha256_hex.is_empty()).then(|| segment.sha256_hex.clone())),
        actual_sha256_hex,
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
            let actual = sha256_file_hex(&cp_path)?;
            if actual != cp.sha256_hex {
                failures = failures.saturating_add(1);
                warn!(
                    filename = %cp.filename,
                    seq = cp.seq,
                    "recovery: checkpoint sha256 mismatch"
                );
                continue;
            }
        }
        info!("recovery: load checkpoint {}", cp.filename);
        match load_checkpoint_with_key(&cp_path, config.checkpoint_key()) {
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

fn is_valid_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.as_bytes().iter().all(|b| b.is_ascii_hexdigit())
}

fn sha256_prefix_hex(path: &Path, bytes_to_hash: u64) -> Result<String, AedbError> {
    use sha2::{Digest, Sha256};

    let mut file = File::open(path)?;
    let mut reader = std::io::BufReader::with_capacity(1024 * 1024, &mut file);
    let mut hasher = Sha256::new();
    let mut remaining_size_bytes = bytes_to_hash;
    let mut buffer = vec![0u8; 1024 * 1024];
    while remaining_size_bytes > 0 {
        let read_size_bytes =
            usize::try_from(remaining_size_bytes.min(buffer.len() as u64)).unwrap_or(buffer.len());
        debug_assert!(read_size_bytes <= buffer.len());
        let read_count = reader.read(&mut buffer[..read_size_bytes])?;
        if read_count == 0 {
            break;
        }
        hasher.update(&buffer[..read_count]);
        remaining_size_bytes = remaining_size_bytes.saturating_sub(read_count as u64);
    }
    if remaining_size_bytes > 0 {
        return Err(AedbError::Validation(
            "segment shorter than expected".into(),
        ));
    }
    Ok(hex_string(hasher.finalize().as_slice()))
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
mod tests {
    use super::{recover_with_config, recover_with_config_and_diagnostics};
    use crate::catalog::schema::ColumnDef;
    use crate::catalog::types::{ColumnType, Row, Value};
    use crate::catalog::{Catalog, DdlOperation};
    use crate::checkpoint::writer::CheckpointMeta;
    use crate::checkpoint::writer::write_checkpoint;
    use crate::commit::executor::CommitExecutor;
    use crate::commit::tx::WalCommitPayload;
    use crate::commit::validation::Mutation;
    use crate::error::{AedbError, RecoveryIntegrityKind};
    use crate::manifest::atomic::write_manifest_atomic;
    use crate::manifest::schema::{Manifest, SegmentMeta};
    use crate::recovery::replay::replay_segments;
    use crate::storage::keyspace::Keyspace;
    use crate::wal::frame::append_frame_bytes;
    use crate::wal::segment::SegmentHeader;
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    fn non_strict_config() -> crate::config::AedbConfig {
        crate::config::AedbConfig {
            recovery_mode: crate::config::RecoveryMode::Permissive,
            ..crate::config::AedbConfig::default()
        }
    }

    fn strict_config() -> crate::config::AedbConfig {
        crate::config::AedbConfig::default()
    }

    fn write_test_segment(dir: &std::path::Path, filename: &str) -> u64 {
        let path = dir.join(filename);
        let mut file = File::create(&path).expect("segment file");
        file.write_all(&SegmentHeader::new(1, 1, [0u8; 32]).to_bytes())
            .expect("header");
        file.write_all(b"frame-bytes").expect("payload");
        file.sync_all().expect("sync");
        std::fs::metadata(path).expect("metadata").len()
    }

    fn single_segment_manifest(
        filename: &str,
        active_segment_seq: u64,
        size_bytes: u64,
        sha256_hex: String,
    ) -> Manifest {
        Manifest {
            durable_seq: 1,
            visible_seq: 1,
            active_segment_seq,
            checkpoints: vec![],
            segments: vec![SegmentMeta {
                filename: filename.into(),
                segment_seq: 1,
                sha256_hex,
                size_bytes,
            }],
        }
    }

    #[tokio::test]
    async fn recover_replays_wal_without_checkpoint() {
        let dir = tempdir().expect("temp");
        let exec = CommitExecutor::new(dir.path()).expect("executor");
        exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
            owner_id: None,
            if_not_exists: true,
            project_id: "p".into(),
        }))
        .await
        .expect("project");
        exec.submit(Mutation::Ddl(DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: false,
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "name".into(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
            ],
            primary_key: vec!["id".into()],
        }))
        .await
        .expect("table");

        for i in 0..100 {
            exec.submit(Mutation::Upsert {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                primary_key: vec![Value::Integer(i)],
                row: Row {
                    values: vec![Value::Integer(i), Value::Text(format!("u{i}").into())],
                },
            })
            .await
            .expect("insert");
        }

        let recovered = recover_with_config(dir.path(), &non_strict_config()).expect("recover");
        assert_eq!(recovered.current_seq, 102);
        assert!(
            recovered
                .keyspace
                .get_row("p", "app", "users", &[Value::Integer(99)])
                .is_some()
        );
    }

    #[test]
    fn permissive_replay_skips_unknown_frame_and_recovers_later_frames() {
        let dir = tempdir().expect("temp");
        let segment_path = dir.path().join("segment-000001.wal");
        let mut file = File::create(&segment_path).expect("segment file");
        file.write_all(&SegmentHeader::new(1, 1, [0u8; 32]).to_bytes())
            .expect("header");

        let create_p = rmp_serde::to_vec(&WalCommitPayload {
            mutations: vec![Mutation::Ddl(DdlOperation::CreateProject {
                owner_id: None,
                if_not_exists: true,
                project_id: "p".into(),
            })],
            assertions: Vec::new(),
            idempotency_key: None,
            request_fingerprint: None,
        })
        .expect("payload p");
        let create_q = rmp_serde::to_vec(&WalCommitPayload {
            mutations: vec![Mutation::Ddl(DdlOperation::CreateProject {
                owner_id: None,
                if_not_exists: true,
                project_id: "q".into(),
            })],
            assertions: Vec::new(),
            idempotency_key: None,
            request_fingerprint: None,
        })
        .expect("payload q");
        let mut frame_bytes = Vec::new();
        append_frame_bytes(&mut frame_bytes, 1, 1, 0x02, &create_p).expect("frame 1");
        append_frame_bytes(&mut frame_bytes, 2, 2, 0x7f, b"invalid").expect("frame 2");
        append_frame_bytes(&mut frame_bytes, 3, 3, 0x02, &create_q).expect("frame 3");
        file.write_all(&frame_bytes).expect("frames");
        file.flush().expect("flush");

        let mut keyspace = Keyspace::default();
        let mut catalog = Catalog::default();
        let mut idempotency = HashMap::new();
        let max_seq = replay_segments(
            &[segment_path],
            0,
            None,
            None,
            false,
            false,
            &mut keyspace,
            &mut catalog,
            &mut idempotency,
        )
        .expect("permissive replay");
        assert_eq!(max_seq, 3);
        assert!(catalog.projects.contains_key("p"));
        assert!(catalog.projects.contains_key("q"));
    }

    #[tokio::test]
    async fn recover_uses_checkpoint_and_replays_only_durable_tail() {
        let dir = tempdir().expect("temp");
        let exec = CommitExecutor::new(dir.path()).expect("executor");
        exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
            owner_id: None,
            if_not_exists: true,
            project_id: "p".into(),
        }))
        .await
        .expect("project");
        exec.submit(Mutation::Ddl(DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: false,
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "name".into(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
            ],
            primary_key: vec!["id".into()],
        }))
        .await
        .expect("table");

        for i in 0..10 {
            exec.submit(Mutation::Upsert {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                primary_key: vec![Value::Integer(i)],
                row: Row {
                    values: vec![Value::Integer(i), Value::Text(format!("u{i}").into())],
                },
            })
            .await
            .expect("insert");
        }

        let (snapshot, catalog, seq) = exec.snapshot_state().await;
        let cp = write_checkpoint(&snapshot, &catalog, seq, dir.path()).expect("checkpoint");
        let manifest = Manifest {
            durable_seq: seq,
            visible_seq: seq,
            active_segment_seq: 1,
            checkpoints: vec![cp],
            segments: vec![SegmentMeta {
                filename: "segment_0000000000000001.aedbwal".into(),
                segment_seq: 1,
                sha256_hex: String::new(),
                size_bytes: 0,
            }],
        };
        write_manifest_atomic(&manifest, dir.path()).expect("manifest");

        for i in 10..20 {
            exec.submit(Mutation::Upsert {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                primary_key: vec![Value::Integer(i)],
                row: Row {
                    values: vec![Value::Integer(i), Value::Text(format!("u{i}").into())],
                },
            })
            .await
            .expect("insert tail");
        }

        let recovered = recover_with_config(dir.path(), &non_strict_config()).expect("recover");
        assert_eq!(recovered.current_seq, seq);
        assert!(
            recovered
                .keyspace
                .get_row("p", "app", "users", &[Value::Integer(19)])
                .is_none()
        );
    }

    #[tokio::test]
    async fn recover_replays_only_through_manifest_durable_seq() {
        let dir = tempdir().expect("temp");
        let exec = CommitExecutor::new(dir.path()).expect("executor");
        exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
            owner_id: None,
            if_not_exists: true,
            project_id: "p".into(),
        }))
        .await
        .expect("project");
        exec.submit(Mutation::Ddl(DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: false,
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "name".into(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
            ],
            primary_key: vec!["id".into()],
        }))
        .await
        .expect("table");

        for i in 0..10 {
            exec.submit(Mutation::Upsert {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                primary_key: vec![Value::Integer(i)],
                row: Row {
                    values: vec![Value::Integer(i), Value::Text(format!("u{i}").into())],
                },
            })
            .await
            .expect("insert");
        }

        let (_snapshot, _catalog, seq) = exec.snapshot_state().await;
        let durable_seq = seq - 5;
        let manifest = Manifest {
            durable_seq,
            visible_seq: seq,
            active_segment_seq: 1,
            checkpoints: vec![],
            segments: vec![SegmentMeta {
                filename: "segment_0000000000000001.aedbwal".into(),
                segment_seq: 1,
                sha256_hex: String::new(),
                size_bytes: 0,
            }],
        };
        write_manifest_atomic(&manifest, dir.path()).expect("manifest");

        let recovered = recover_with_config(dir.path(), &non_strict_config()).expect("recover");
        assert_eq!(recovered.current_seq, durable_seq);
        assert!(
            recovered
                .keyspace
                .get_row("p", "app", "users", &[Value::Integer(4)])
                .is_some()
        );
        assert!(
            recovered
                .keyspace
                .get_row("p", "app", "users", &[Value::Integer(9)])
                .is_none()
        );
    }

    #[tokio::test]
    async fn recover_falls_back_to_older_checkpoint_when_latest_is_corrupt() {
        let dir = tempdir().expect("temp");
        let exec = CommitExecutor::new(dir.path()).expect("executor");
        exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
            owner_id: None,
            if_not_exists: true,
            project_id: "p".into(),
        }))
        .await
        .expect("project");
        exec.submit(Mutation::Ddl(DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: false,
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "name".into(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
            ],
            primary_key: vec!["id".into()],
        }))
        .await
        .expect("table");
        exec.submit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(1)],
            row: Row {
                values: vec![Value::Integer(1), Value::Text("ok".into())],
            },
        })
        .await
        .expect("insert");

        let (snapshot, catalog, seq) = exec.snapshot_state().await;
        let good_cp = write_checkpoint(&snapshot, &catalog, seq, dir.path()).expect("good cp");

        let bad_name = format!("checkpoint_{:016}.aedb.zst", seq + 10);
        std::fs::write(dir.path().join(&bad_name), b"corrupt").expect("write bad cp");
        let bad_cp = CheckpointMeta {
            filename: bad_name,
            seq: seq + 10,
            sha256_hex: String::new(),
            created_at_micros: 0,
            key_id: None,
        };

        let manifest = Manifest {
            durable_seq: seq,
            visible_seq: seq,
            active_segment_seq: 1,
            checkpoints: vec![good_cp, bad_cp],
            segments: vec![SegmentMeta {
                filename: "segment_0000000000000001.aedbwal".into(),
                segment_seq: 1,
                sha256_hex: String::new(),
                size_bytes: 0,
            }],
        };
        write_manifest_atomic(&manifest, dir.path()).expect("manifest");

        let recovered = recover_with_config(dir.path(), &non_strict_config()).expect("recover");
        assert_eq!(recovered.current_seq, seq);
        assert!(
            recovered
                .keyspace
                .get_row("p", "app", "users", &[Value::Integer(1)])
                .is_some()
        );
    }

    #[tokio::test]
    async fn recover_uses_discovered_checkpoint_without_manifest_reference() {
        let dir = tempdir().expect("temp");
        let exec = CommitExecutor::new(dir.path()).expect("executor");
        exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
            owner_id: None,
            if_not_exists: true,
            project_id: "p".into(),
        }))
        .await
        .expect("project");
        exec.submit(Mutation::Ddl(DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: false,
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "name".into(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
            ],
            primary_key: vec!["id".into()],
        }))
        .await
        .expect("table");
        exec.submit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(7)],
            row: Row {
                values: vec![Value::Integer(7), Value::Text("from_cp".into())],
            },
        })
        .await
        .expect("insert");

        let (snapshot, catalog, seq) = exec.snapshot_state().await;
        let cp = write_checkpoint(&snapshot, &catalog, seq, dir.path()).expect("checkpoint");
        assert!(dir.path().join(cp.filename).exists());

        let recovered = recover_with_config(dir.path(), &non_strict_config()).expect("recover");
        assert_eq!(recovered.current_seq, seq);
        assert!(
            recovered
                .keyspace
                .get_row("p", "app", "users", &[Value::Integer(7)])
                .is_some()
        );
    }

    #[tokio::test]
    async fn strict_recovery_fails_when_all_referenced_checkpoints_are_corrupt() {
        let dir = tempdir().expect("temp");
        let exec = CommitExecutor::new(dir.path()).expect("executor");
        exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
            owner_id: None,
            if_not_exists: true,
            project_id: "p".into(),
        }))
        .await
        .expect("project");
        let (snapshot, catalog, seq) = exec.snapshot_state().await;
        let cp = write_checkpoint(&snapshot, &catalog, seq, dir.path()).expect("checkpoint");
        std::fs::write(dir.path().join(&cp.filename), b"corrupt").expect("corrupt cp");

        let manifest = Manifest {
            durable_seq: seq,
            visible_seq: seq,
            active_segment_seq: 1,
            checkpoints: vec![cp],
            segments: vec![],
        };
        write_manifest_atomic(&manifest, dir.path()).expect("manifest");
        let err = recover_with_config(dir.path(), &strict_config()).expect_err("strict fail");
        assert!(
            err.to_string()
                .contains("all referenced checkpoints failed")
        );
    }

    #[tokio::test]
    async fn strict_recovery_fails_on_manifest_checkpoint_sha_mismatch() {
        let dir = tempdir().expect("temp");
        let exec = CommitExecutor::new(dir.path()).expect("executor");
        exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
            owner_id: None,
            if_not_exists: true,
            project_id: "p".into(),
        }))
        .await
        .expect("project");
        let (snapshot, catalog, seq) = exec.snapshot_state().await;
        let mut cp = write_checkpoint(&snapshot, &catalog, seq, dir.path()).expect("checkpoint");
        cp.sha256_hex = "00".repeat(32);

        let manifest = Manifest {
            durable_seq: seq,
            visible_seq: seq,
            active_segment_seq: 1,
            checkpoints: vec![cp],
            segments: vec![],
        };
        write_manifest_atomic(&manifest, dir.path()).expect("manifest");
        let err = recover_with_config(dir.path(), &strict_config()).expect_err("strict fail");
        assert!(
            err.to_string()
                .contains("all referenced checkpoints failed")
        );
    }

    #[tokio::test]
    async fn strict_recovery_fails_on_manifest_segment_sha_mismatch() {
        let dir = tempdir().expect("temp");
        let exec = CommitExecutor::new(dir.path()).expect("executor");
        exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
            owner_id: None,
            if_not_exists: true,
            project_id: "p".into(),
        }))
        .await
        .expect("project");
        let (_snapshot, _catalog, seq) = exec.snapshot_state().await;
        let segment_filename = "segment_0000000000000001.aedbwal";
        let segment_size = std::fs::metadata(dir.path().join(segment_filename))
            .expect("segment metadata")
            .len();

        let manifest = Manifest {
            durable_seq: seq,
            visible_seq: seq,
            active_segment_seq: 2,
            checkpoints: vec![],
            segments: vec![SegmentMeta {
                filename: segment_filename.into(),
                segment_seq: 1,
                sha256_hex: "00".repeat(32),
                size_bytes: segment_size,
            }],
        };
        write_manifest_atomic(&manifest, dir.path()).expect("manifest");
        let err = recover_with_config(dir.path(), &strict_config()).expect_err("strict fail");
        match err {
            AedbError::RecoveryIntegrity { diagnostic } => {
                assert_eq!(
                    diagnostic.kind,
                    RecoveryIntegrityKind::ManifestWalSegmentChecksumMismatch
                );
                assert_eq!(diagnostic.segment_filename, segment_filename);
                let json = serde_json::to_string(&diagnostic).expect("json diagnostic");
                assert!(json.contains("manifest_wal_segment_checksum_mismatch"));
            }
            other => panic!("expected recovery integrity diagnostic, got {other:?}"),
        }
    }

    #[test]
    fn strict_recovery_reports_structured_missing_manifest_wal_segment() {
        let dir = tempdir().expect("temp");
        let filename = "segment_0000000000000001.aedbwal";
        let manifest = single_segment_manifest(filename, 2, 128, "11".repeat(32));
        write_manifest_atomic(&manifest, dir.path()).expect("manifest");

        let err = recover_with_config(dir.path(), &strict_config()).expect_err("strict fail");
        match err {
            AedbError::RecoveryIntegrity { diagnostic } => {
                assert_eq!(
                    diagnostic.kind,
                    RecoveryIntegrityKind::ManifestWalSegmentMissing
                );
                assert_eq!(diagnostic.segment_filename, filename);
                assert_eq!(diagnostic.manifest_path, dir.path().join("manifest.json"));
                assert_eq!(diagnostic.wal_dir, dir.path());
                assert_eq!(diagnostic.durable_seq, 1);
                assert_eq!(diagnostic.visible_seq, 1);
                assert_eq!(diagnostic.active_segment_seq, 2);
                assert_eq!(diagnostic.segment_seq, 1);
                assert!(diagnostic.operator_message().contains("restore/repair"));
            }
            other => panic!("expected recovery integrity diagnostic, got {other:?}"),
        }
    }

    #[test]
    fn strict_recovery_reports_missing_size_metadata_distinctly() {
        let dir = tempdir().expect("temp");
        let filename = "segment_0000000000000001.aedbwal";
        write_test_segment(dir.path(), filename);
        let manifest = single_segment_manifest(filename, 2, 0, "11".repeat(32));
        write_manifest_atomic(&manifest, dir.path()).expect("manifest");

        let err = recover_with_config(dir.path(), &strict_config()).expect_err("strict fail");
        match err {
            AedbError::RecoveryIntegrity { diagnostic } => {
                assert_eq!(
                    diagnostic.kind,
                    RecoveryIntegrityKind::ManifestWalSegmentMissingSizeMetadata
                );
                assert_eq!(diagnostic.segment_filename, filename);
                assert!(diagnostic.actual_size_bytes.is_some());
            }
            other => panic!("expected recovery integrity diagnostic, got {other:?}"),
        }
    }

    #[test]
    fn strict_recovery_reports_missing_checksum_metadata_distinctly() {
        let dir = tempdir().expect("temp");
        let filename = "segment_0000000000000001.aedbwal";
        let segment_size_bytes = write_test_segment(dir.path(), filename);
        let manifest = single_segment_manifest(filename, 2, segment_size_bytes, String::new());
        write_manifest_atomic(&manifest, dir.path()).expect("manifest");

        let err = recover_with_config(dir.path(), &strict_config()).expect_err("strict fail");
        match err {
            AedbError::RecoveryIntegrity { diagnostic } => {
                assert_eq!(
                    diagnostic.kind,
                    RecoveryIntegrityKind::ManifestWalSegmentMissingChecksumMetadata
                );
                assert_eq!(diagnostic.segment_filename, filename);
                assert_eq!(diagnostic.expected_size_bytes, Some(segment_size_bytes));
            }
            other => panic!("expected recovery integrity diagnostic, got {other:?}"),
        }
    }

    #[test]
    fn permissive_recovery_records_skipped_manifest_wal_segments() {
        let dir = tempdir().expect("temp");
        let filename = "segment_0000000000000001.aedbwal";
        let manifest = single_segment_manifest(filename, 2, 128, "11".repeat(32));
        write_manifest_atomic(&manifest, dir.path()).expect("manifest");

        let outcome =
            recover_with_config_and_diagnostics(dir.path(), &non_strict_config()).expect("recover");
        assert_eq!(outcome.state.current_seq, 0);
        assert_eq!(outcome.warnings.len(), 1);
        assert_eq!(
            outcome.warnings[0].kind,
            RecoveryIntegrityKind::ManifestWalSegmentMissing
        );
        assert_eq!(outcome.warnings[0].segment_filename, filename);
    }

    #[tokio::test]
    async fn permissive_recovery_can_continue_without_valid_checkpoint() {
        let dir = tempdir().expect("temp");
        let exec = CommitExecutor::new(dir.path()).expect("executor");
        exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
            owner_id: None,
            if_not_exists: true,
            project_id: "p".into(),
        }))
        .await
        .expect("project");
        let (snapshot, catalog, seq) = exec.snapshot_state().await;
        let cp = write_checkpoint(&snapshot, &catalog, seq, dir.path()).expect("checkpoint");
        std::fs::write(dir.path().join(&cp.filename), b"corrupt").expect("corrupt cp");

        let manifest = Manifest {
            durable_seq: 0,
            visible_seq: 0,
            active_segment_seq: 1,
            checkpoints: vec![cp],
            segments: vec![],
        };
        write_manifest_atomic(&manifest, dir.path()).expect("manifest");
        let recovered = recover_with_config(dir.path(), &non_strict_config()).expect("recover");
        assert_eq!(recovered.current_seq, 0);
    }
}
