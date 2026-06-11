use super::{
    AedbConfig, AedbError, AedbInstance, CallerContext, ColumnDef, ColumnType, ConsistencyMode,
    DdlOperation, DurabilityMode, Expr, Mutation, Permission, Query,
    REACTIVE_ACK_CACHE_MAX_ENTRIES, ReactiveCheckpointAckCacheKey, ReactiveCheckpointAckState,
    RecoveryMode, Row, StorageMode, TransactionEnvelope, Value, WriteClass, WriteIntent,
    reclaim_eligible_wal_segments,
};
use crate::catalog::SYSTEM_PROJECT_ID;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tempfile::tempdir;

#[test]
fn checkpoint_compression_level_is_validated() {
    let cfg = AedbConfig {
        checkpoint_compression_level: 23,
        ..AedbConfig::default()
    };
    let err = crate::lib_helpers::validate_config(&cfg)
        .expect_err("out-of-range compression level must be rejected");
    assert!(matches!(err, AedbError::InvalidConfig { .. }));

    let cfg = AedbConfig {
        checkpoint_compression_level: 1,
        ..AedbConfig::default()
    };
    crate::lib_helpers::validate_config(&cfg).expect("valid compression level");
}

#[tokio::test]
async fn disk_backed_checkpoint_is_self_contained_when_value_file_is_missing() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        storage_mode: StorageMode::DiskBacked,
        persistent_value_inline_threshold_bytes: 16,
        persistent_value_hot_cache_bytes: 0,
        max_kv_value_bytes: 256 * 1024,
        max_transaction_bytes: 512 * 1024,
        ..AedbConfig::default()
    };
    let value: Vec<u8> = (0..128 * 1024).map(|i| (i % 199) as u8).collect();

    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.kv_set("p", "app", b"blob".to_vec(), value.clone())
        .await
        .expect("set large value");
    db.checkpoint_now().await.expect("checkpoint");
    db.shutdown().await.expect("shutdown");
    drop(db);

    fs::remove_file(dir.path().join("values.aedbdat")).expect("remove value store");

    let reopened = AedbInstance::open(config, dir.path()).expect("reopen from checkpoint");
    let recovered = reopened
        .kv_get_no_auth("p", "app", b"blob", ConsistencyMode::AtLatest)
        .await
        .expect("recovered get")
        .expect("recovered value");
    assert_eq!(recovered.value, value);
    reopened.shutdown().await.expect("shutdown reopened");
}

#[tokio::test]
async fn event_outbox_and_reactive_processor_checkpoint_lag_work() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "app").await.expect("scope");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: Some("system".into()),
        if_not_exists: true,
        project_id: SYSTEM_PROJECT_ID.into(),
        scope_id: "app".into(),
        table_name: "reactive_processor_checkpoints".into(),
        columns: vec![
            ColumnDef {
                name: "processor_name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "checkpoint_seq".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "updated_at_micros".into(),
                col_type: ColumnType::Timestamp,
                nullable: false,
            },
        ],
        primary_key: vec!["processor_name".into()],
    }))
    .await
    .expect("create processor checkpoint table");

    let first = db
        .emit_event(
            "arcana",
            "app",
            "hand_settled",
            "hand_1".into(),
            r#"{"user_id":"u1","wager":100,"pnl":-100}"#.into(),
        )
        .await
        .expect("emit first event");
    let second = db
        .emit_event(
            "arcana",
            "app",
            "hand_settled",
            "hand_2".into(),
            r#"{"user_id":"u2","wager":75,"pnl":50}"#.into(),
        )
        .await
        .expect("emit second event");

    let page = db
        .read_event_stream(Some("hand_settled"), 0, 10, ConsistencyMode::AtLatest)
        .await
        .expect("read stream");
    assert_eq!(page.events.len(), 2);
    assert_eq!(page.events[0].event_key, "hand_1");
    assert_eq!(page.events[1].event_key, "hand_2");
    assert_eq!(page.next_commit_seq, Some(second.commit_seq));

    db.ack_reactive_processor_checkpoint("points_processor", first.commit_seq)
        .await
        .expect("ack checkpoint");
    let lag = db
        .reactive_processor_lag("points_processor", ConsistencyMode::AtLatest)
        .await
        .expect("lag");
    assert_eq!(lag.processor_name, "points_processor");
    assert_eq!(lag.checkpoint_seq, first.commit_seq);
    assert!(lag.head_seq >= second.commit_seq);
    assert_eq!(
        lag.lag_commits,
        lag.head_seq.saturating_sub(first.commit_seq)
    );
}

#[tokio::test]
async fn reactive_processor_checkpoint_ack_batches_by_watermark() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "app").await.expect("scope");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: Some("system".into()),
        if_not_exists: true,
        project_id: SYSTEM_PROJECT_ID.into(),
        scope_id: "app".into(),
        table_name: "reactive_processor_checkpoints".into(),
        columns: vec![
            ColumnDef {
                name: "processor_name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "checkpoint_seq".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "updated_at_micros".into(),
                col_type: ColumnType::Timestamp,
                nullable: false,
            },
        ],
        primary_key: vec!["processor_name".into()],
    }))
    .await
    .expect("create processor checkpoint table");

    let first = db
        .emit_event(
            "arcana",
            "app",
            "hand_settled",
            "hand_1".into(),
            r#"{"user_id":"u1","wager":100,"pnl":-100}"#.into(),
        )
        .await
        .expect("emit first event");
    let second = db
        .emit_event(
            "arcana",
            "app",
            "hand_settled",
            "hand_2".into(),
            r#"{"user_id":"u2","wager":75,"pnl":50}"#.into(),
        )
        .await
        .expect("emit second event");

    let persisted = db
        .ack_reactive_processor_checkpoint_batched("points_processor", first.commit_seq, 2)
        .await
        .expect("first batched ack");
    assert!(persisted.is_some(), "first ack should persist baseline");

    let deferred = db
        .ack_reactive_processor_checkpoint_batched("points_processor", first.commit_seq + 1, 2)
        .await
        .expect("deferred batched ack");
    assert!(deferred.is_none(), "ack below watermark should be deferred");

    let lag_after_defer = db
        .reactive_processor_lag("points_processor", ConsistencyMode::AtLatest)
        .await
        .expect("lag after deferred ack");
    assert_eq!(lag_after_defer.checkpoint_seq, first.commit_seq);

    let persisted_after_watermark = db
        .ack_reactive_processor_checkpoint_batched("points_processor", first.commit_seq + 2, 2)
        .await
        .expect("persist on watermark");
    assert!(
        persisted_after_watermark.is_some(),
        "watermark crossing should persist checkpoint"
    );

    let lag = db
        .reactive_processor_lag("points_processor", ConsistencyMode::AtLatest)
        .await
        .expect("lag");
    assert_eq!(lag.processor_name, "points_processor");
    assert_eq!(lag.checkpoint_seq, first.commit_seq + 2);
    assert!(lag.head_seq >= second.commit_seq);
}

#[tokio::test]
async fn reactive_processor_checkpoint_ack_rejects_regression() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "app").await.expect("scope");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: Some("system".into()),
        if_not_exists: true,
        project_id: SYSTEM_PROJECT_ID.into(),
        scope_id: "app".into(),
        table_name: "reactive_processor_checkpoints".into(),
        columns: vec![
            ColumnDef {
                name: "processor_name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "checkpoint_seq".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "updated_at_micros".into(),
                col_type: ColumnType::Timestamp,
                nullable: false,
            },
        ],
        primary_key: vec!["processor_name".into()],
    }))
    .await
    .expect("create processor checkpoint table");

    let first = db
        .emit_event(
            "arcana",
            "app",
            "hand_settled",
            "hand_1".into(),
            r#"{"user_id":"u1","wager":100,"pnl":-100}"#.into(),
        )
        .await
        .expect("emit first event");
    let second = db
        .emit_event(
            "arcana",
            "app",
            "hand_settled",
            "hand_2".into(),
            r#"{"user_id":"u2","wager":75,"pnl":50}"#.into(),
        )
        .await
        .expect("emit second event");

    db.ack_reactive_processor_checkpoint("points_processor", second.commit_seq)
        .await
        .expect("persist newer checkpoint");

    let err = db
        .ack_reactive_processor_checkpoint("points_processor", first.commit_seq)
        .await
        .expect_err("checkpoint regression must fail");
    assert!(matches!(err, AedbError::Validation(_)));

    let lag = db
        .reactive_processor_lag("points_processor", ConsistencyMode::AtLatest)
        .await
        .expect("lag");
    assert_eq!(lag.checkpoint_seq, second.commit_seq);
}

#[tokio::test]
async fn reactive_processor_checkpoint_batched_rejects_stale_persist_after_concurrent_advance() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "app").await.expect("scope");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: Some("system".into()),
        if_not_exists: true,
        project_id: SYSTEM_PROJECT_ID.into(),
        scope_id: "app".into(),
        table_name: "reactive_processor_checkpoints".into(),
        columns: vec![
            ColumnDef {
                name: "processor_name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "checkpoint_seq".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "updated_at_micros".into(),
                col_type: ColumnType::Timestamp,
                nullable: false,
            },
        ],
        primary_key: vec!["processor_name".into()],
    }))
    .await
    .expect("create processor checkpoint table");

    let first = db
        .emit_event(
            "arcana",
            "app",
            "hand_settled",
            "hand_1".into(),
            r#"{"user_id":"u1","wager":100,"pnl":-100}"#.into(),
        )
        .await
        .expect("emit first event");
    let second = db
        .emit_event(
            "arcana",
            "app",
            "hand_settled",
            "hand_2".into(),
            r#"{"user_id":"u2","wager":75,"pnl":50}"#.into(),
        )
        .await
        .expect("emit second event");

    let stale = db
        .build_reactive_processor_checkpoint_envelope("points_processor", first.commit_seq)
        .await
        .expect("stale envelope");

    db.ack_reactive_processor_checkpoint("points_processor", second.commit_seq)
        .await
        .expect("persist newer checkpoint");

    let err = db
        .commit_envelope_prevalidated_internal("stale_reactive_ack_test", stale)
        .await
        .expect_err("stale asserted ack must fail");
    assert!(matches!(err, AedbError::Conflict(_)));

    let lag = db
        .reactive_processor_lag("points_processor", ConsistencyMode::AtLatest)
        .await
        .expect("lag");
    assert_eq!(lag.checkpoint_seq, second.commit_seq);
}

#[tokio::test]
async fn reactive_processor_checkpoint_batched_as_isolated_by_caller() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "app").await.expect("scope");
    db.emit_event("arcana", "app", "bootstrap", "evt-1".into(), "{}".into())
        .await
        .expect("bootstrap system scope");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: Some("system".into()),
        if_not_exists: true,
        project_id: SYSTEM_PROJECT_ID.into(),
        scope_id: "app".into(),
        table_name: "reactive_processor_checkpoints".into(),
        columns: vec![
            ColumnDef {
                name: "processor_name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "checkpoint_seq".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "updated_at_micros".into(),
                col_type: ColumnType::Timestamp,
                nullable: false,
            },
        ],
        primary_key: vec!["processor_name".into()],
    }))
    .await
    .expect("create processor checkpoint table");
    db.ack_reactive_processor_checkpoint("bootstrap", 1)
        .await
        .expect("bootstrap processor checkpoint table");

    for caller in ["alice", "bob"] {
        db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
            actor_id: None,
            delegable: false,
            caller_id: caller.into(),
            permission: Permission::TableWrite {
                project_id: SYSTEM_PROJECT_ID.into(),
                scope_id: "app".into(),
                table_name: "reactive_processor_checkpoints".into(),
            },
        }))
        .await
        .expect("grant system table write");
    }

    let first = db
        .ack_reactive_processor_checkpoint_batched_as(
            CallerContext::new("alice"),
            "shared_processor",
            42,
            100,
        )
        .await
        .expect("alice ack");
    assert!(first.is_some(), "alice baseline ack should persist");

    let second = db
        .ack_reactive_processor_checkpoint_batched_as(
            CallerContext::new("bob"),
            "shared_processor",
            42,
            100,
        )
        .await
        .expect("bob ack");
    assert!(
        second.is_some(),
        "bob baseline ack should persist independently"
    );
}

#[tokio::test]
async fn reactive_processor_checkpoint_batched_as_does_not_poison_cache_on_permission_failure() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "app").await.expect("scope");
    db.emit_event("arcana", "app", "bootstrap", "evt-1".into(), "{}".into())
        .await
        .expect("bootstrap system scope");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: Some("system".into()),
        if_not_exists: true,
        project_id: SYSTEM_PROJECT_ID.into(),
        scope_id: "app".into(),
        table_name: "reactive_processor_checkpoints".into(),
        columns: vec![
            ColumnDef {
                name: "processor_name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "checkpoint_seq".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "updated_at_micros".into(),
                col_type: ColumnType::Timestamp,
                nullable: false,
            },
        ],
        primary_key: vec!["processor_name".into()],
    }))
    .await
    .expect("create processor checkpoint table");
    db.ack_reactive_processor_checkpoint("bootstrap", 1)
        .await
        .expect("bootstrap processor checkpoint table");

    let denied = db
        .ack_reactive_processor_checkpoint_batched_as(
            CallerContext::new("mallory"),
            "perm_test_processor",
            100,
            100,
        )
        .await
        .expect_err("mallory should be denied before grant");
    assert!(matches!(denied, AedbError::PermissionDenied(_)));

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "mallory".into(),
        permission: Permission::TableWrite {
            project_id: SYSTEM_PROJECT_ID.into(),
            scope_id: "app".into(),
            table_name: "reactive_processor_checkpoints".into(),
        },
    }))
    .await
    .expect("grant system table write");

    let persisted = db
        .ack_reactive_processor_checkpoint_batched_as(
            CallerContext::new("mallory"),
            "perm_test_processor",
            100,
            100,
        )
        .await
        .expect("mallory should succeed after grant");
    assert!(
        persisted.is_some(),
        "failed pre-grant attempt must not suppress later successful persist"
    );
}

#[tokio::test]
async fn reactive_processor_checkpoint_batched_cache_is_bounded() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "app").await.expect("scope");
    db.emit_event("arcana", "app", "bootstrap", "evt-1".into(), "{}".into())
        .await
        .expect("bootstrap system scope");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: Some("system".into()),
        if_not_exists: true,
        project_id: SYSTEM_PROJECT_ID.into(),
        scope_id: "app".into(),
        table_name: "reactive_processor_checkpoints".into(),
        columns: vec![
            ColumnDef {
                name: "processor_name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "checkpoint_seq".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "updated_at_micros".into(),
                col_type: ColumnType::Timestamp,
                nullable: false,
            },
        ],
        primary_key: vec!["processor_name".into()],
    }))
    .await
    .expect("create processor checkpoint table");
    {
        let mut cache = db.reactive_processor_ack_watermarks.lock();
        for i in 0..(REACTIVE_ACK_CACHE_MAX_ENTRIES + 200) {
            cache.insert(
                ReactiveCheckpointAckCacheKey {
                    processor_name: format!("cache-seed-{i}"),
                    caller_id: None,
                },
                ReactiveCheckpointAckState {
                    last_persisted_seq: i as u64,
                    last_touch_micros: i as u64,
                },
            );
        }
    }

    let persisted = db
        .ack_reactive_processor_checkpoint_batched("cache-boundary", 1, 1)
        .await
        .expect("ack should succeed");
    assert!(persisted.is_some());
    let cache_len = db.reactive_processor_ack_watermarks.lock().len();
    assert!(
        cache_len <= REACTIVE_ACK_CACHE_MAX_ENTRIES,
        "cache should be pruned to cap; got {}",
        cache_len
    );
}

#[test]
fn wal_segment_gc_reclaims_checkpoint_covered_segments() {
    let dir = tempdir().expect("temp");
    for seq in 1_u64..=4 {
        std::fs::write(
            dir.path().join(format!("segment_{seq:016}.aedbwal")),
            format!("segment-{seq}"),
        )
        .expect("write segment");
    }
    let manifest = crate::manifest::schema::Manifest {
        durable_seq: 50,
        visible_seq: 50,
        active_segment_seq: 4,
        checkpoints: vec![crate::checkpoint::writer::CheckpointMeta {
            filename: "checkpoint_0000000000000050.aedb.zst".into(),
            seq: 50,
            sha256_hex: "00".repeat(32),
            created_at_micros: 1,
            key_id: None,
        }],
        segments: Vec::new(),
    };
    crate::manifest::atomic::write_manifest_atomic_signed(&manifest, dir.path(), None)
        .expect("write manifest");
    let snapshot_manager = Arc::new(parking_lot::Mutex::new(
        crate::snapshot::gc::SnapshotManager::default(),
    ));

    let reclaimed = reclaim_eligible_wal_segments(dir.path(), &snapshot_manager, None).expect("gc");

    assert_eq!(reclaimed, 3);
    assert!(!dir.path().join("segment_0000000000000001.aedbwal").exists());
    assert!(!dir.path().join("segment_0000000000000002.aedbwal").exists());
    assert!(!dir.path().join("segment_0000000000000003.aedbwal").exists());
    assert!(dir.path().join("segment_0000000000000004.aedbwal").exists());
}

#[tokio::test]
async fn checkpoint_now_enables_clean_restart() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
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
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(5)],
        row: Row {
            values: vec![Value::Integer(5), Value::Text("bob".into())],
        },
    })
    .await
    .expect("insert");

    let seq = db.checkpoint_now().await.expect("checkpoint");
    assert!(seq >= 3);

    let reopened = AedbInstance::open(AedbConfig::default(), dir.path()).expect("reopen");
    let rows = reopened
        .query(
            "p",
            "app",
            Query::select(&["*"])
                .from("users")
                .where_(Expr::Eq("id".into(), Value::Integer(5))),
        )
        .await
        .expect("query");
    assert_eq!(rows.rows.len(), 1);
}

#[tokio::test]
async fn checkpoint_retention_keeps_recent_checkpoints_and_prunes_older_files() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig::default().with_checkpoint_retention_count(2);
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    // Drive several checkpoints, each at a strictly higher seq.
    let mut checkpoint_seqs = Vec::new();
    for i in 0..4u64 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("k{i}").into_bytes(),
            value: format!("v{i}").into_bytes(),
        })
        .await
        .expect("write");
        checkpoint_seqs.push(db.checkpoint_now().await.expect("checkpoint"));
    }

    // Manifest retains exactly the two newest checkpoints, newest last.
    let manifest = crate::manifest::atomic::load_manifest(dir.path()).expect("manifest");
    assert_eq!(manifest.checkpoints.len(), 2, "retention count honored");
    let retained_seqs: Vec<u64> = manifest.checkpoints.iter().map(|cp| cp.seq).collect();
    let mut expected = checkpoint_seqs.clone();
    expected.sort_unstable();
    assert_eq!(retained_seqs, expected[expected.len() - 2..].to_vec());

    // Superseded checkpoint files are pruned from disk; only retained remain.
    let on_disk: Vec<String> = fs::read_dir(dir.path())
        .expect("read dir")
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| name.starts_with("checkpoint_") && name.ends_with(".aedb.zst"))
        .collect();
    assert_eq!(
        on_disk.len(),
        2,
        "older checkpoint files pruned: {on_disk:?}"
    );

    // A clean reopen still recovers the latest state.
    drop(db);
    let reopened = AedbInstance::open(config, dir.path()).expect("reopen");
    let entry = reopened
        .kv_get_no_auth("p", "app", b"k3", ConsistencyMode::AtLatest)
        .await
        .expect("kv_get")
        .expect("present");
    assert_eq!(entry.value, b"v3".to_vec());
}

#[tokio::test]
async fn checkpoint_does_not_prune_older_checkpoints_when_prior_manifest_unreadable() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig::production([7u8; 32]).with_checkpoint_retention_count(3);
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    for i in 0..2u64 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("k{i}").into_bytes(),
            value: b"v".to_vec(),
        })
        .await
        .expect("write");
        db.checkpoint_now().await.expect("checkpoint");
    }

    let count_checkpoints = || {
        fs::read_dir(dir.path())
            .expect("read dir")
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| name.starts_with("checkpoint_") && name.ends_with(".aedb.zst"))
            .count()
    };
    let before = count_checkpoints();
    assert!(before >= 2, "expected retained checkpoints, got {before}");

    // Make the signed manifest unreadable: under an HMAC key reconstruction is
    // disabled, so the next checkpoint sees no prior manifest.
    let _ = fs::remove_file(dir.path().join("manifest.hmac"));
    let _ = fs::remove_file(dir.path().join("manifest.hmac.prev"));

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"k2".to_vec(),
        value: b"v".to_vec(),
    })
    .await
    .expect("write");
    db.checkpoint_now().await.expect("cp3");

    // The new checkpoint is written, but older files are NOT pruned away.
    assert!(
        count_checkpoints() > before,
        "older checkpoints must survive an unreadable prior manifest"
    );
}

#[tokio::test]
async fn checkpoint_now_in_batch_mode_flushes_wal_and_recovers_tail() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        durability_mode: DurabilityMode::Batch,
        batch_interval_ms: 60_000,
        batch_max_bytes: usize::MAX,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"tail".to_vec(),
        value: b"v1".to_vec(),
    })
    .await
    .expect("tail write");

    let before = db.head_state().await;
    assert!(before.visible_head_seq > before.durable_head_seq);

    let cp_seq = db.checkpoint_now().await.expect("checkpoint");
    let after = db.head_state().await;
    assert_eq!(after.visible_head_seq, after.durable_head_seq);
    assert!(after.durable_head_seq >= cp_seq);
    drop(db);

    let recovered = crate::recovery::recover_with_config(dir.path(), &config).expect("recover");
    let tail = recovered
        .keyspace
        .kv_get("p", "app", b"tail")
        .expect("tail recovered");
    assert_eq!(tail.value, b"v1".to_vec());
}

#[tokio::test]
async fn checkpoint_now_allows_commits_while_running() {
    let dir = tempdir().expect("temp");
    let db = Arc::new(AedbInstance::open(AedbConfig::default(), dir.path()).expect("open"));
    db.create_project("p").await.expect("project");

    // Build enough state to make checkpoint work measurable.
    for i in 0..2_000u32 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("seed:{i:06}").into_bytes(),
            value: vec![b'x'; 1024],
        })
        .await
        .expect("seed write");
    }

    let checkpoint_db = Arc::clone(&db);
    let checkpoint_task = tokio::spawn(async move { checkpoint_db.checkpoint_now().await });

    // Wait briefly for checkpoint to begin.
    for _ in 0..10 {
        if !checkpoint_task.is_finished() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(
        !checkpoint_task.is_finished(),
        "checkpoint should still be running"
    );

    let commit = db
        .commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"concurrent:write".to_vec(),
            value: b"ok".to_vec(),
        })
        .await
        .expect("commit should proceed during checkpoint");
    let checkpoint_seq = checkpoint_task
        .await
        .expect("checkpoint join")
        .expect("checkpoint");
    assert!(commit.commit_seq >= checkpoint_seq);
}

#[tokio::test]
async fn checkpoint_now_serializes_checkpoint_writers() {
    let dir = tempdir().expect("temp");
    let db = Arc::new(AedbInstance::open(AedbConfig::default(), dir.path()).expect("open"));
    db.create_project("p").await.expect("project");

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"k".to_vec(),
        value: b"v".to_vec(),
    })
    .await
    .expect("seed");

    let db1 = Arc::clone(&db);
    let db2 = Arc::clone(&db);
    let t1 = tokio::spawn(async move { db1.checkpoint_now().await });
    let t2 = tokio::spawn(async move { db2.checkpoint_now().await });

    let s1 = t1.await.expect("checkpoint task 1").expect("checkpoint 1");
    let s2 = t2.await.expect("checkpoint task 2").expect("checkpoint 2");
    assert_eq!(s1, s2);
}

#[tokio::test]
async fn checkpoint_captures_transaction_all_or_none() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig::default();
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let base_seq = db
        .snapshot_probe(ConsistencyMode::AtLatest)
        .await
        .expect("base");
    let commit = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: Default::default(),
            write_intent: WriteIntent {
                mutations: vec![
                    Mutation::KvSet {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        key: b"tx:part:a".to_vec(),
                        value: b"1".to_vec(),
                    },
                    Mutation::KvSet {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        key: b"tx:part:b".to_vec(),
                        value: b"2".to_vec(),
                    },
                ],
            },
            base_seq,
        })
        .await
        .expect("atomic multi-mutation commit");
    let checkpoint_seq = db.checkpoint_now().await.expect("checkpoint");
    assert!(checkpoint_seq >= commit.commit_seq);

    let recovered_at_cp =
        crate::recovery::recover_at_seq_with_config(dir.path(), checkpoint_seq, &config)
            .expect("recover at checkpoint");
    let snapshot_at_cp = recovered_at_cp.keyspace.snapshot();
    assert_eq!(
        snapshot_at_cp
            .kv_get("p", "app", b"tx:part:a")
            .map(|entry| entry.value.clone()),
        Some(b"1".to_vec())
    );
    assert_eq!(
        snapshot_at_cp
            .kv_get("p", "app", b"tx:part:b")
            .map(|entry| entry.value.clone()),
        Some(b"2".to_vec())
    );

    if commit.commit_seq > 0 {
        let recovered_before =
            crate::recovery::recover_at_seq_with_config(dir.path(), commit.commit_seq - 1, &config)
                .expect("recover before commit seq");
        let before_snapshot = recovered_before.keyspace.snapshot();
        assert!(before_snapshot.kv_get("p", "app", b"tx:part:a").is_none());
        assert!(before_snapshot.kv_get("p", "app", b"tx:part:b").is_none());
    }
}

#[tokio::test]
async fn checkpoint_manifest_trims_fully_covered_segments() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        max_segment_bytes: 4096,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    for i in 0..400u32 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("seg:{i:04}").into_bytes(),
            value: vec![b'x'; 256],
        })
        .await
        .expect("seed");
    }

    let all_segments = crate::lib_helpers::read_segments(dir.path()).expect("all segments");
    assert!(
        all_segments.len() > 1,
        "test must create multiple wal segments"
    );

    let checkpoint_seq = db.checkpoint_now().await.expect("checkpoint");
    let manifest = crate::manifest::atomic::load_manifest_signed(dir.path(), config.hmac_key())
        .expect("manifest");

    assert!(
        manifest.segments.len() < all_segments.len(),
        "checkpoint manifest should drop fully covered historical segments"
    );
    for segment in &manifest.segments {
        let path = dir.path().join(&segment.filename);
        let range = crate::lib_helpers::scan_segment_seq_range(&path).expect("scan segment");
        if let Some((_, max_seq)) = range {
            assert!(
                max_seq > checkpoint_seq || segment.segment_seq == manifest.active_segment_seq,
                "manifest retained a segment fully covered by checkpoint"
            );
        }
    }
}

#[tokio::test]
#[ignore = "manual perf probe: commit latency with and without concurrent checkpoint"]
async fn benchmark_commit_latency_during_checkpoint() {
    fn percentile(sorted: &[u128], p: f64) -> u128 {
        if sorted.is_empty() {
            return 0;
        }
        let percentile_index = ((sorted.len() as f64 - 1.0) * p).round() as usize;
        sorted[percentile_index.min(sorted.len() - 1)]
    }

    async fn run_phase(
        db: &Arc<AedbInstance>,
        start: usize,
        count: usize,
        with_checkpoint: bool,
    ) -> (f64, u128, u128) {
        for i in 0..10_000usize {
            db.commit(Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: format!("warmup:{i:05}").into_bytes(),
                value: vec![b'w'; 256],
            })
            .await
            .expect("warmup seed");
        }

        let checkpoint_task = if with_checkpoint {
            Some(tokio::spawn({
                let db = Arc::clone(db);
                async move { db.checkpoint_now().await }
            }))
        } else {
            None
        };

        let phase_started = Instant::now();
        let mut latencies_us = Vec::with_capacity(count);
        for i in 0..count {
            let started = Instant::now();
            db.commit(Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: format!(
                    "bench:{}:{:06}",
                    if with_checkpoint { "cp" } else { "base" },
                    start + i
                )
                .into_bytes(),
                value: vec![b'x'; 512],
            })
            .await
            .expect("bench commit");
            latencies_us.push(started.elapsed().as_micros());
        }

        if let Some(task) = checkpoint_task {
            let _ = task
                .await
                .expect("checkpoint task join")
                .expect("checkpoint");
        }

        latencies_us.sort_unstable();
        let elapsed_secs = phase_started.elapsed().as_secs_f64().max(0.000_001);
        let tps = count as f64 / elapsed_secs;
        let p50_us = percentile(&latencies_us, 0.50);
        let p99_us = percentile(&latencies_us, 0.99);
        (tps, p50_us, p99_us)
    }

    let dir = tempdir().expect("temp");
    let config = AedbConfig::default();
    let db = Arc::new(AedbInstance::open(config, dir.path()).expect("open"));
    db.create_project("p").await.expect("project");

    let (base_tps, base_p50, base_p99) = run_phase(&db, 0, 800, false).await;
    let (cp_tps, cp_p50, cp_p99) = run_phase(&db, 1_000_000, 800, true).await;

    eprintln!(
        "checkpoint_perf: base_tps={:.2} base_p50_us={} base_p99_us={} | cp_tps={:.2} cp_p50_us={} cp_p99_us={} | tps_ratio={:.3} p50_ratio={:.3} p99_ratio={:.3}",
        base_tps,
        base_p50,
        base_p99,
        cp_tps,
        cp_p50,
        cp_p99,
        cp_tps / base_tps.max(0.000_001),
        (cp_p50 as f64) / (base_p50.max(1) as f64),
        (cp_p99 as f64) / (base_p99.max(1) as f64),
    );
}

#[tokio::test]
#[ignore = "manual perf probe: parallel commit throughput with and without concurrent checkpoint"]
async fn benchmark_parallel_commit_throughput_during_checkpoint() {
    fn percentile(sorted: &[u128], p: f64) -> u128 {
        if sorted.is_empty() {
            return 0;
        }
        let percentile_index = ((sorted.len() as f64 - 1.0) * p).round() as usize;
        sorted[percentile_index.min(sorted.len() - 1)]
    }

    async fn run_parallel_phase(
        db: &Arc<AedbInstance>,
        workers: usize,
        commits_per_worker: usize,
        with_checkpoint: bool,
        offset: usize,
    ) -> (f64, u128, u128) {
        let checkpoint_task = if with_checkpoint {
            Some(tokio::spawn({
                let db = Arc::clone(db);
                async move { db.checkpoint_now().await }
            }))
        } else {
            None
        };

        let phase_started = Instant::now();
        let mut tasks = Vec::with_capacity(workers);
        for worker in 0..workers {
            let db = Arc::clone(db);
            tasks.push(tokio::spawn(async move {
                let mut latencies = Vec::with_capacity(commits_per_worker);
                for i in 0..commits_per_worker {
                    let started = Instant::now();
                    db.commit(Mutation::KvSet {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        key: format!(
                            "par:{}:{:02}:{:06}",
                            if with_checkpoint { "cp" } else { "base" },
                            worker,
                            offset + i
                        )
                        .into_bytes(),
                        value: vec![b'p'; 256],
                    })
                    .await
                    .expect("parallel bench commit");
                    latencies.push(started.elapsed().as_micros());
                }
                latencies
            }));
        }

        let mut all_latencies = Vec::with_capacity(workers * commits_per_worker);
        for task in tasks {
            let mut worker_latencies = task.await.expect("worker join");
            all_latencies.append(&mut worker_latencies);
        }

        if let Some(task) = checkpoint_task {
            let _ = task
                .await
                .expect("checkpoint task join")
                .expect("checkpoint");
        }

        all_latencies.sort_unstable();
        let elapsed_secs = phase_started.elapsed().as_secs_f64().max(0.000_001);
        let total = workers * commits_per_worker;
        let tps = total as f64 / elapsed_secs;
        let p50_us = percentile(&all_latencies, 0.50);
        let p99_us = percentile(&all_latencies, 0.99);
        (tps, p50_us, p99_us)
    }

    let dir = tempdir().expect("temp");
    let mut config = AedbConfig {
        durability_mode: DurabilityMode::Batch,
        batch_interval_ms: 10,
        batch_max_bytes: usize::MAX,
        recovery_mode: RecoveryMode::Permissive,
        hash_chain_required: false,
        ..AedbConfig::default()
    };
    config.manifest_hmac_key = None;
    let db = Arc::new(AedbInstance::open(config, dir.path()).expect("open"));
    db.create_project("p").await.expect("project");

    // Seed state so the checkpoint has meaningful work.
    for i in 0..12_000usize {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("parallel-seed:{i:05}").into_bytes(),
            value: vec![b's'; 256],
        })
        .await
        .expect("seed");
    }

    let workers = 8usize;
    let commits_per_worker = 300usize;
    let (base_tps, base_p50, base_p99) =
        run_parallel_phase(&db, workers, commits_per_worker, false, 0).await;
    let (cp_tps, cp_p50, cp_p99) =
        run_parallel_phase(&db, workers, commits_per_worker, true, 1_000_000).await;

    eprintln!(
        "checkpoint_parallel_perf: workers={} commits_per_worker={} | base_tps={:.2} base_p50_us={} base_p99_us={} | cp_tps={:.2} cp_p50_us={} cp_p99_us={} | tps_ratio={:.3} p50_ratio={:.3} p99_ratio={:.3}",
        workers,
        commits_per_worker,
        base_tps,
        base_p50,
        base_p99,
        cp_tps,
        cp_p50,
        cp_p99,
        cp_tps / base_tps.max(0.000_001),
        (cp_p50 as f64) / (base_p50.max(1) as f64),
        (cp_p99 as f64) / (base_p99.max(1) as f64),
    );
}

#[tokio::test]
#[ignore = "manual perf probe: compare checkpoint compression levels under parallel load"]
async fn benchmark_parallel_checkpoint_compression_levels() {
    async fn seed(db: &Arc<AedbInstance>) {
        for i in 0..12_000usize {
            db.commit(Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: format!("parallel-seed:{i:05}").into_bytes(),
                value: vec![b's'; 256],
            })
            .await
            .expect("seed");
        }
    }

    async fn run_parallel_phase(
        db: &Arc<AedbInstance>,
        workers: usize,
        commits_per_worker: usize,
        with_checkpoint: bool,
        offset: usize,
    ) -> f64 {
        let checkpoint_task = if with_checkpoint {
            Some(tokio::spawn({
                let db = Arc::clone(db);
                async move { db.checkpoint_now().await }
            }))
        } else {
            None
        };

        let phase_started = Instant::now();
        let mut tasks = Vec::with_capacity(workers);
        for worker in 0..workers {
            let db = Arc::clone(db);
            tasks.push(tokio::spawn(async move {
                for i in 0..commits_per_worker {
                    db.commit(Mutation::KvSet {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        key: format!(
                            "cmp:{}:{:02}:{:06}",
                            if with_checkpoint { "cp" } else { "base" },
                            worker,
                            offset + i
                        )
                        .into_bytes(),
                        value: vec![b'c'; 256],
                    })
                    .await
                    .expect("parallel bench commit");
                }
            }));
        }
        for task in tasks {
            task.await.expect("worker join");
        }
        if let Some(task) = checkpoint_task {
            let _ = task
                .await
                .expect("checkpoint task join")
                .expect("checkpoint");
        }

        let elapsed_secs = phase_started.elapsed().as_secs_f64().max(0.000_001);
        let total = workers * commits_per_worker;
        total as f64 / elapsed_secs
    }

    let workers = 8usize;
    let commits_per_worker = 300usize;
    let levels = [3, 1, 0];

    let mut baseline_tps = 0.0f64;
    for (idx, level) in levels.iter().copied().enumerate() {
        let dir = tempdir().expect("temp");
        let mut config = AedbConfig {
            durability_mode: DurabilityMode::Batch,
            batch_interval_ms: 10,
            batch_max_bytes: usize::MAX,
            recovery_mode: RecoveryMode::Permissive,
            hash_chain_required: false,
            ..AedbConfig::default()
        };
        config.manifest_hmac_key = None;
        config.checkpoint_compression_level = level;
        let db = Arc::new(AedbInstance::open(config, dir.path()).expect("open"));
        db.create_project("p").await.expect("project");
        seed(&db).await;

        let base =
            run_parallel_phase(&db, workers, commits_per_worker, false, idx * 1_000_000).await;
        let cp = run_parallel_phase(
            &db,
            workers,
            commits_per_worker,
            true,
            idx * 1_000_000 + 500_000,
        )
        .await;
        if idx == 0 {
            baseline_tps = base;
        }

        eprintln!(
            "checkpoint_compression_perf: level={} workers={} commits_per_worker={} base_tps={:.2} cp_tps={:.2} cp_to_base={:.3} cp_to_level3_base={:.3}",
            level,
            workers,
            commits_per_worker,
            base,
            cp,
            cp / base.max(0.000_001),
            cp / baseline_tps.max(0.000_001),
        );
    }
}

#[tokio::test]
async fn snapshot_probe_returns_snapshot_seq() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    let seq = db
        .snapshot_probe(ConsistencyMode::AtLatest)
        .await
        .expect("probe");
    assert!(seq >= 1);
}

#[tokio::test]
async fn snapshot_probe_does_not_consume_snapshot_slot() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        max_concurrent_snapshots: 1,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open(config, dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let _lease = db
        .acquire_snapshot(ConsistencyMode::AtLatest)
        .await
        .expect("lease");

    let seq = db
        .snapshot_probe(ConsistencyMode::AtLatest)
        .await
        .expect("probe");
    assert!(seq >= 1);
}

#[tokio::test]
async fn snapshot_probe_remains_live_under_snapshot_capacity_pressure() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        max_concurrent_snapshots: 1,
        ..AedbConfig::default()
    };
    let db = Arc::new(AedbInstance::open(config, dir.path()).expect("open"));
    db.create_project("p").await.expect("project");

    let _lease = db
        .acquire_snapshot(ConsistencyMode::AtLatest)
        .await
        .expect("lease");

    let mut tasks = Vec::new();
    for _ in 0..4 {
        let db = Arc::clone(&db);
        tasks.push(tokio::spawn(async move {
            for _ in 0..128 {
                let seq = db
                    .snapshot_probe(ConsistencyMode::AtLatest)
                    .await
                    .expect("probe");
                assert!(seq >= 1);
                tokio::task::yield_now().await;
            }
        }));
    }

    tokio::time::timeout(Duration::from_secs(2), async {
        for task in tasks {
            task.await.expect("probe task");
        }
    })
    .await
    .expect("snapshot probes timed out under snapshot pressure");
}

#[tokio::test]
async fn checkpoint_now_completes_under_hot_load() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        durability_mode: DurabilityMode::Batch,
        batch_interval_ms: 60_000,
        batch_max_bytes: usize::MAX,
        ..AedbConfig::default()
    };
    let db = Arc::new(AedbInstance::open(config, dir.path()).expect("open"));
    db.create_project("p").await.expect("project");

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writes = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let reads = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let writer_db = Arc::clone(&db);
    let writer_stop = Arc::clone(&stop);
    let writes_ref = Arc::clone(&writes);
    let writer = tokio::spawn(async move {
        let mut i = 0u64;
        while !writer_stop.load(Ordering::Relaxed) {
            let _ = writer_db
                .commit(Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: format!("checkpoint-hot:{i}").into_bytes(),
                    value: i.to_string().into_bytes(),
                })
                .await;
            writes_ref.fetch_add(1, Ordering::Relaxed);
            i = i.saturating_add(1);
            tokio::task::yield_now().await;
        }
    });

    let reader_db = Arc::clone(&db);
    let reader_stop = Arc::clone(&stop);
    let reads_ref = Arc::clone(&reads);
    let reader = tokio::spawn(async move {
        while !reader_stop.load(Ordering::Relaxed) {
            let _ = reader_db.snapshot_probe(ConsistencyMode::AtLatest).await;
            reads_ref.fetch_add(1, Ordering::Relaxed);
            tokio::task::yield_now().await;
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    let writes_before = writes.load(Ordering::Relaxed);
    let reads_before = reads.load(Ordering::Relaxed);

    let checkpoint_seq = tokio::time::timeout(Duration::from_secs(10), db.checkpoint_now())
        .await
        .expect("checkpoint_now timed out")
        .expect("checkpoint_now");
    assert!(checkpoint_seq >= 1);

    let writes_after = writes.load(Ordering::Relaxed);
    let reads_after = reads.load(Ordering::Relaxed);
    assert!(
        writes_after > writes_before,
        "writes stalled during checkpoint"
    );
    assert!(
        reads_after > reads_before,
        "reads stalled during checkpoint"
    );

    stop.store(true, Ordering::Relaxed);
    tokio::time::timeout(Duration::from_secs(5), writer)
        .await
        .expect("writer join timed out")
        .expect("writer join");
    tokio::time::timeout(Duration::from_secs(5), reader)
        .await
        .expect("reader join timed out")
        .expect("reader join");
}

#[tokio::test]
async fn at_checkpoint_falls_back_when_version_evicted() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        max_versions: 2,
        min_version_age_ms: 0,
        version_gc_interval_ms: 1,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open(config, dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"checkpoint:key".to_vec(),
        value: b"v1".to_vec(),
    })
    .await
    .expect("seed checkpoint value");
    let checkpoint_seq = db.checkpoint_now().await.expect("checkpoint");
    for i in 0..8 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("tail:{i}").into_bytes(),
            value: vec![i as u8],
        })
        .await
        .expect("tail write");
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    let cp_view_seq = db
        .snapshot_probe(ConsistencyMode::AtCheckpoint)
        .await
        .expect("checkpoint snapshot");
    assert_eq!(cp_view_seq, checkpoint_seq);
}

#[tokio::test]
async fn strict_restore_rejects_older_backup_version() {
    let dir = tempdir().expect("data dir");
    let backup_dir = tempdir().expect("backup dir");
    let config = AedbConfig::production([7u8; 32]);
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    db.create_project("arcana").await.expect("create project");
    db.create_scope("arcana", "app")
        .await
        .expect("create scope");
    db.backup_full(backup_dir.path())
        .await
        .expect("backup full");

    let mut manifest = crate::backup::load_backup_manifest(backup_dir.path(), config.hmac_key())
        .expect("manifest");
    manifest.aedb_version = "0.2.0".into();
    crate::backup::write_backup_manifest(backup_dir.path(), &manifest, config.hmac_key())
        .expect("rewrite manifest");

    let restore_dir = tempdir().expect("restore dir");
    let err = AedbInstance::restore_from_backup_chain(
        &[backup_dir.path().to_path_buf()],
        restore_dir.path(),
        &config,
        None,
    )
    .expect_err("strict restore must reject older backup version");
    assert!(format!("{err}").contains("matching AEDB patch version"));
}

#[tokio::test]
async fn restore_at_time_rejects_backup_wal_with_invalid_hash_chain() {
    let dir = tempdir().expect("data dir");
    let backup_dir = tempdir().expect("backup dir");
    let restore_dir = tempdir().expect("restore dir");
    let config = AedbConfig::production([8u8; 32]);
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    db.create_project("arcana").await.expect("create project");
    db.create_scope("arcana", "app")
        .await
        .expect("create scope");
    db.commit(Mutation::KvSet {
        project_id: "arcana".into(),
        scope_id: "app".into(),
        key: b"k".to_vec(),
        value: b"v".to_vec(),
    })
    .await
    .expect("write kv");
    db.backup_full(backup_dir.path())
        .await
        .expect("backup full");

    let manifest = crate::backup::load_backup_manifest(backup_dir.path(), config.hmac_key())
        .expect("manifest");
    let wal_name = manifest.wal_segments.first().expect("wal segment");
    let wal_path = backup_dir.path().join("wal_tail").join(wal_name);
    let mut wal_bytes = fs::read(&wal_path).expect("read wal");
    wal_bytes[0] ^= 0xFF;
    fs::write(&wal_path, wal_bytes).expect("corrupt wal");
    let mut manifest = crate::backup::load_backup_manifest(backup_dir.path(), config.hmac_key())
        .expect("manifest");
    manifest.file_sha256.insert(
        format!("wal_tail/{wal_name}"),
        crate::backup::sha256_file_hex(&wal_path).expect("rehash corrupted wal"),
    );
    crate::backup::write_backup_manifest(backup_dir.path(), &manifest, config.hmac_key())
        .expect("rewrite manifest");

    let err = AedbInstance::restore_from_backup_chain_at_time(
        &[backup_dir.path().to_path_buf()],
        restore_dir.path(),
        &config,
        u64::MAX,
    )
    .expect_err("time restore must reject invalid hash chain");
    assert!(
        format!("{err}").contains("bad segment header")
            || format!("{err}").contains("segment hash chain mismatch")
    );
}
