pub mod replay;
pub mod scanner;

use crate::backup::sha256_file_hex;
use crate::catalog::Catalog;
use crate::checkpoint::loader::load_checkpoint_with_key;
use crate::commit::tx::{IdempotencyKey, IdempotencyRecord};
use crate::config::AedbConfig;
use crate::error::AedbError;
use crate::manifest::atomic::load_manifest_signed_mode;
use crate::manifest::schema::Manifest;
use crate::recovery::replay::replay_segments;
use crate::recovery::scanner::scan_segments;
use crate::storage::keyspace::Keyspace;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredState {
    pub keyspace: Keyspace,
    pub catalog: Catalog,
    pub current_seq: u64,
    pub idempotency: HashMap<IdempotencyKey, IdempotencyRecord>,
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
        Some(&replay_limits),
        config.hash_chain_required,
        config.strict_recovery(),
        &mut keyspace,
        &mut catalog,
        &mut idempotency,
    )?;
    Ok(RecoveredState {
        keyspace,
        catalog,
        current_seq: replayed,
        idempotency,
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
        Some(&replay_limits),
        config.hash_chain_required,
        config.strict_recovery(),
        &mut keyspace,
        &mut catalog,
        &mut idempotency,
    )?;
    Ok(RecoveredState {
        keyspace,
        catalog,
        current_seq: replayed,
        idempotency,
    })
}

fn has_explicit_manifest(data_dir: &Path) -> bool {
    data_dir.join("manifest.json").exists() || data_dir.join("manifest.json.prev").exists()
}

fn segment_paths_for_replay(
    data_dir: &Path,
    manifest: &Manifest,
    explicit_manifest: bool,
    config: &AedbConfig,
) -> Result<(Vec<PathBuf>, HashMap<PathBuf, u64>), AedbError> {
    if !explicit_manifest {
        return Ok((scan_segments(data_dir)?, HashMap::new()));
    }

    let mut segments = manifest.segments.clone();
    segments.sort_by_key(|segment| segment.segment_seq);
    let mut paths = Vec::with_capacity(segments.len());
    let mut replay_limits = HashMap::new();
    let active_segment_seq = manifest.active_segment_seq;
    for segment in segments {
        let is_active_segment = segment.segment_seq == active_segment_seq;
        let path = data_dir.join(&segment.filename);
        if !path.exists() {
            if config.strict_recovery() {
                return Err(AedbError::Validation(format!(
                    "manifest referenced wal segment missing: {}",
                    segment.filename
                )));
            }
            warn!(
                filename = %segment.filename,
                "recovery: manifest referenced wal segment missing, skipping in permissive mode"
            );
            continue;
        }
        let actual_size_bytes = std::fs::metadata(&path)?.len();
        if segment.size_bytes == 0 {
            if config.strict_recovery() && !is_active_segment {
                return Err(AedbError::Validation(format!(
                    "manifest referenced wal segment missing size metadata: {}",
                    segment.filename
                )));
            }
            warn!(
                filename = %segment.filename,
                "recovery: wal segment size metadata missing, skipping strict size check"
            );
        } else {
            if actual_size_bytes < segment.size_bytes {
                if config.strict_recovery() {
                    return Err(AedbError::Validation(format!(
                        "manifest wal segment truncated: {}",
                        segment.filename
                    )));
                }
                warn!(
                    filename = %segment.filename,
                    "recovery: wal segment shorter than manifest size, skipping segment in permissive mode"
                );
                continue;
            }
            if actual_size_bytes > segment.size_bytes
                && !is_active_segment
                && config.strict_recovery()
            {
                return Err(AedbError::Validation(format!(
                    "manifest wal segment size mismatch: {}",
                    segment.filename
                )));
            }
        }
        if segment.sha256_hex.is_empty() {
            if config.strict_recovery() && !is_active_segment {
                return Err(AedbError::Validation(format!(
                    "manifest referenced wal segment missing sha256 metadata: {}",
                    segment.filename
                )));
            }
            warn!(
                filename = %segment.filename,
                "recovery: wal segment sha256 metadata missing, skipping integrity check in permissive mode"
            );
        } else if !is_valid_sha256_hex(&segment.sha256_hex) {
            if config.strict_recovery() && !is_active_segment {
                return Err(AedbError::Validation(format!(
                    "manifest wal segment sha256 metadata invalid: {}",
                    segment.filename
                )));
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
                if config.strict_recovery() && !is_active_segment {
                    return Err(AedbError::Validation(format!(
                        "manifest wal segment sha256 mismatch: {}",
                        segment.filename
                    )));
                }
                warn!(
                    filename = %segment.filename,
                    "recovery: wal segment sha256 mismatch on active/permissive segment, skipping integrity check"
                );
            }
        }
        let limit_path = path.clone();
        paths.push(path);
        if segment.size_bytes > 0 {
            replay_limits.insert(limit_path, segment.size_bytes);
        }
    }
    Ok((paths, replay_limits))
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
    let mut reader = std::io::BufReader::new(&mut file);
    let mut hasher = Sha256::new();
    let mut remaining = bytes_to_hash;
    let mut buf = [0u8; 16 * 1024];
    while remaining > 0 {
        let read_len = usize::try_from(remaining.min(buf.len() as u64)).unwrap_or(buf.len());
        let n = reader.read(&mut buf[..read_len])?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        remaining = remaining.saturating_sub(n as u64);
    }
    if remaining > 0 {
        return Err(AedbError::Validation(
            "segment shorter than expected".into(),
        ));
    }
    Ok(format!("{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::recover_with_config;
    use crate::catalog::DdlOperation;
    use crate::catalog::schema::ColumnDef;
    use crate::catalog::types::{ColumnType, Row, Value};
    use crate::checkpoint::writer::CheckpointMeta;
    use crate::checkpoint::writer::write_checkpoint;
    use crate::commit::executor::CommitExecutor;
    use crate::commit::validation::Mutation;
    use crate::manifest::atomic::write_manifest_atomic;
    use crate::manifest::schema::{Manifest, SegmentMeta};
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

        let manifest = Manifest {
            durable_seq: seq,
            visible_seq: seq,
            active_segment_seq: 2,
            checkpoints: vec![],
            segments: vec![SegmentMeta {
                filename: "segment_0000000000000001.aedbwal".into(),
                segment_seq: 1,
                sha256_hex: "00".repeat(32),
                size_bytes: 1,
            }],
        };
        write_manifest_atomic(&manifest, dir.path()).expect("manifest");
        let err = recover_with_config(dir.path(), &strict_config()).expect_err("strict fail");
        assert!(
            err.to_string().contains("segment sha256 mismatch")
                || err.to_string().contains("segment size mismatch")
        );
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
