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
use crate::recovery::replay::{ReplaySegmentsRequest, replay_segments};
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
            .expect("get_row")
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
    let max_seq = replay_segments(ReplaySegmentsRequest {
        segments: &[segment_path],
        from_seq_exclusive: 0,
        to_seq_inclusive: None,
        segment_replay_byte_limits: None,
        hash_chain_required: false,
        strict_recovery: false,
        keyspace: &mut keyspace,
        catalog: &mut catalog,
        idempotency: &mut idempotency,
    })
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
            .expect("get_row")
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
            .expect("get_row")
            .is_some()
    );
    assert!(
        recovered
            .keyspace
            .get_row("p", "app", "users", &[Value::Integer(9)])
            .expect("get_row")
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
            .expect("get_row")
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
            .expect("get_row")
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
            assert_eq!(diagnostic.expected_size_bytes, Some(segment_size));
            assert_eq!(diagnostic.actual_size_bytes, Some(segment_size));
            assert_eq!(diagnostic.expected_sha256_hex, Some("00".repeat(32)));
            assert!(
                diagnostic
                    .actual_sha256_hex
                    .as_ref()
                    .is_some_and(|actual| actual.len() == 64 && actual != &"00".repeat(32)),
                "diagnostic should include the computed segment checksum"
            );
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
