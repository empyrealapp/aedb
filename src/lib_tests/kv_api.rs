use super::*;

#[tokio::test]
async fn kv_write_helpers_respect_scope_boundaries() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "private").await.expect("scope");

    db.kv_set("p", "app", b"counter".to_vec(), b"100".to_vec())
        .await
        .expect("write default scope");
    db.kv_set("p", "private", b"counter".to_vec(), b"7".to_vec())
        .await
        .expect("write private scope");

    let app_reader = CallerContext::new("app_reader");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: app_reader.caller_id.clone(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: None,
        },
    }))
    .await
    .expect("grant app read");

    let private_reader = CallerContext::new("private_reader");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: private_reader.caller_id.clone(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("private".into()),
            prefix: None,
        },
    }))
    .await
    .expect("grant private read");

    let app_value = db
        .kv_get(
            "p",
            "app",
            b"counter",
            ConsistencyMode::AtLatest,
            &app_reader,
        )
        .await
        .expect("app read")
        .expect("app key");
    assert_eq!(app_value.value, b"100".to_vec());

    let private_denied = db
        .kv_get(
            "p",
            "private",
            b"counter",
            ConsistencyMode::AtLatest,
            &app_reader,
        )
        .await
        .expect_err("app reader cannot read private scope");
    assert!(matches!(
        private_denied,
        crate::query::error::QueryError::PermissionDenied { .. }
    ));

    let private_value = db
        .kv_get(
            "p",
            "private",
            b"counter",
            ConsistencyMode::AtLatest,
            &private_reader,
        )
        .await
        .expect("private read")
        .expect("private key");
    assert_eq!(private_value.value, b"7".to_vec());
}

#[tokio::test]
async fn kv_set_many_atomic_writes_all_entries_in_one_commit() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let result = db
        .kv_set_many_atomic(
            "p",
            "app",
            vec![
                (b"batch:a".to_vec(), b"1".to_vec()),
                (b"batch:b".to_vec(), b"2".to_vec()),
                (b"batch:c".to_vec(), b"3".to_vec()),
            ],
        )
        .await
        .expect("batch kv set");

    let caller = CallerContext::new("reader");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: caller.caller_id.clone(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: Some(b"batch:".to_vec()),
        },
    }))
    .await
    .expect("grant read");

    for (key, value) in [
        (b"batch:a".as_slice(), b"1".to_vec()),
        (b"batch:b".as_slice(), b"2".to_vec()),
        (b"batch:c".as_slice(), b"3".to_vec()),
    ] {
        let entry = db
            .kv_get("p", "app", key, ConsistencyMode::AtLatest, &caller)
            .await
            .expect("read batch key")
            .expect("batch key exists");
        assert_eq!(entry.value, value);
        assert_eq!(entry.version, result.commit_seq);
    }

    let err = db
        .kv_set_many_atomic("p", "app", Vec::new())
        .await
        .expect_err("empty batch rejected");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[tokio::test]
async fn kv_projection_table_materializes_kv_state() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.enable_kv_projection("p", "app")
        .await
        .expect("enable kv projection");

    db.kv_set("p", "app", b"user:1".to_vec(), b"alice".to_vec())
        .await
        .expect("kv set 1");
    db.kv_set("p", "app", b"user:2".to_vec(), b"bob".to_vec())
        .await
        .expect("kv set 2");

    let mut projected = None;
    for _ in 0..25 {
        let result = db
            .query(
                "p",
                "app",
                Query::select(&["*"])
                    .from(crate::catalog::KV_INDEX_TABLE)
                    .limit(10),
            )
            .await
            .expect("query projection table");
        if result.rows.len() == 2 {
            projected = Some(result.rows);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    let rows = projected.expect("projection rows");
    let mut saw_user1 = false;
    let mut saw_user2 = false;
    for row in rows {
        let project = row.values.first().expect("project_id");
        let scope = row.values.get(1).expect("scope_id");
        let key = row.values.get(2).expect("key");
        let value = row.values.get(3).expect("value");
        let commit_seq = row.values.get(4).expect("commit_seq");
        assert_eq!(project, &Value::Text("p".into()));
        assert_eq!(scope, &Value::Text("app".into()));
        assert!(matches!(commit_seq, Value::Integer(v) if *v > 0));
        match (key, value) {
            (Value::Blob(k), Value::Blob(v)) if k == b"user:1" && v == b"alice" => {
                saw_user1 = true;
            }
            (Value::Blob(k), Value::Blob(v)) if k == b"user:2" && v == b"bob" => {
                saw_user2 = true;
            }
            other => panic!("unexpected kv projection row: {other:?}"),
        }
    }
    assert!(saw_user1, "missing user:1 projection row");
    assert!(saw_user2, "missing user:2 projection row");
}

#[tokio::test]
async fn kv_projection_table_is_managed_and_read_only() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.enable_kv_projection("p", "app")
        .await
        .expect("enable projection");

    let err = db
        .commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: crate::catalog::KV_INDEX_TABLE.into(),
            primary_key: vec![Value::Blob(b"k".to_vec())],
            row: Row {
                values: vec![
                    Value::Text("p".into()),
                    Value::Text("app".into()),
                    Value::Blob(b"k".to_vec()),
                    Value::Blob(b"v".to_vec()),
                    Value::Integer(1),
                    Value::Timestamp(1),
                ],
            },
        })
        .await
        .expect_err("managed table writes must fail");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[tokio::test]
async fn kv_query_apis_are_scope_aware() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "tenant1").await.expect("scope");

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "tenant1".into(),
        key: b"user:1".to_vec(),
        value: b"alice".to_vec(),
    })
    .await
    .expect("seed kv");

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "alice".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("tenant1".into()),
            prefix: None,
        },
    }))
    .await
    .expect("grant kv read");

    let caller = CallerContext::new("alice");
    let hit = db
        .kv_get(
            "p",
            "tenant1",
            b"user:1",
            ConsistencyMode::AtLatest,
            &caller,
        )
        .await
        .expect("read in granted scope")
        .expect("value exists");
    assert_eq!(hit.value, b"alice".to_vec());

    let denied = db
        .kv_get("p", "app", b"user:1", ConsistencyMode::AtLatest, &caller)
        .await
        .expect_err("read should be denied in ungranted scope");
    assert!(matches!(
        denied,
        crate::query::error::QueryError::PermissionDenied { .. }
    ));
}

#[tokio::test]
async fn kv_scan_all_scopes_requires_project_wide_read() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "s1").await.expect("scope s1");
    db.create_scope("p", "s2").await.expect("scope s2");

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "s1".into(),
        key: b"user:1".to_vec(),
        value: b"alice".to_vec(),
    })
    .await
    .expect("seed s1");
    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "s2".into(),
        key: b"user:2".to_vec(),
        value: b"bob".to_vec(),
    })
    .await
    .expect("seed s2");

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "alice".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: None,
            prefix: None,
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant project-wide read");

    let alice = CallerContext::new("alice");
    let entries = db
        .kv_scan_all_scopes("p", b"user:", 10, ConsistencyMode::AtLatest, &alice)
        .await
        .expect("scan all scopes");
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().any(|e| e.scope_id == "s1"));
    assert!(entries.iter().any(|e| e.scope_id == "s2"));

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "bob".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("s1".into()),
            prefix: None,
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant scope-only read");
    let bob = CallerContext::new("bob");
    let denied = db
        .kv_scan_all_scopes("p", b"user:", 10, ConsistencyMode::AtLatest, &bob)
        .await
        .expect_err("scope-only grant cannot scan all scopes");
    assert!(matches!(
        denied,
        crate::query::error::QueryError::PermissionDenied { .. }
    ));
}

#[tokio::test]
async fn production_profile_requires_disk_backed_kv_payloads() {
    let dir = tempdir().expect("temp");
    let mut weak = AedbConfig::production([7u8; 32]);
    weak.storage_mode = StorageMode::InMemory;
    let err = AedbInstance::open_production(weak, dir.path())
        .err()
        .expect("production profile must reject in-memory storage");
    assert!(matches!(err, AedbError::InvalidConfig { .. }));

    let dir = tempdir().expect("temp");
    let mut weak = AedbConfig::production([8u8; 32]);
    weak.persistent_value_inline_threshold_bytes = 1;
    let err = AedbInstance::open_production(weak, dir.path())
        .err()
        .expect("production profile must reject inline KV payloads");
    assert!(matches!(err, AedbError::InvalidConfig { .. }));

    let cfg = AedbConfig::production([9u8; 32]);
    assert_eq!(cfg.storage_mode, StorageMode::DiskBacked);
    assert_eq!(cfg.persistent_value_inline_threshold_bytes, 0);
}

#[tokio::test]
async fn disk_backed_kv_values_spill_and_recover_after_reopen() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        storage_mode: StorageMode::DiskBacked,
        persistent_value_inline_threshold_bytes: 32,
        max_kv_value_bytes: 256 * 1024,
        max_transaction_bytes: 512 * 1024,
        max_memory_estimate_bytes: 8 * 1024,
        ..AedbConfig::default()
    };
    let value: Vec<u8> = (0..128 * 1024).map(|i| (i % 251) as u8).collect();

    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.kv_set("p", "app", b"blob".to_vec(), value.clone())
        .await
        .expect("set large value");

    let memory_estimate = db.estimated_memory_bytes().await;
    assert!(
        memory_estimate < value.len() / 4,
        "large value should not remain inline in keyspace memory estimate: {memory_estimate}"
    );
    let metrics = db.operational_metrics().await;
    assert!(
        metrics.persistent_value_store_bytes > value.len() as u64,
        "value store metrics should include spilled payload bytes"
    );
    assert_eq!(
        metrics.persistent_value_hot_cache_capacity_bytes,
        config.persistent_value_hot_cache_bytes
    );
    assert!(
        metrics.persistent_value_hot_cache_bytes
            <= metrics.persistent_value_hot_cache_capacity_bytes
    );
    let value_file = dir.path().join("values.aedbdat");
    assert!(
        fs::metadata(&value_file)
            .expect("value store metadata")
            .len()
            > value.len() as u64,
        "value store should contain spilled payload bytes"
    );

    let got = db
        .kv_get_no_auth("p", "app", b"blob", ConsistencyMode::AtLatest)
        .await
        .expect("get")
        .expect("value");
    assert_eq!(got.value, value);
    db.shutdown().await.expect("shutdown");
    drop(db);

    let reopened = AedbInstance::open(config, dir.path()).expect("reopen");
    let recovered = reopened
        .kv_get_no_auth("p", "app", b"blob", ConsistencyMode::AtLatest)
        .await
        .expect("recovered get")
        .expect("recovered value");
    assert_eq!(recovered.value, value);
    reopened.shutdown().await.expect("shutdown reopened");
}

fn corrupt_file_byte(path: &Path, at: usize) {
    let mut bytes = fs::read(path).expect("read file to corrupt");
    assert!(bytes.len() > at, "file too small to corrupt at offset {at}");
    bytes[at] ^= 0x5a;
    fs::write(path, bytes).expect("rewrite corrupted file");
}

fn first_kv_segment_path(dir: &Path) -> PathBuf {
    fs::read_dir(dir.join("kv_segments"))
        .expect("segment dir")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| path.extension().is_some_and(|ext| ext == "aedbkv"))
        .expect("kv segment file")
}

#[tokio::test]
async fn disk_backed_spilled_value_corruption_fails_closed_for_reads_and_commit_guards() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        storage_mode: StorageMode::DiskBacked,
        persistent_value_inline_threshold_bytes: 0,
        persistent_value_hot_cache_bytes: 0,
        max_kv_value_bytes: 256 * 1024,
        max_transaction_bytes: 512 * 1024,
        ..AedbConfig::default()
    };
    let value = vec![7u8; 128 * 1024];
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.kv_set("p", "app", b"blob".to_vec(), value)
        .await
        .expect("set spilled value");

    corrupt_file_byte(&dir.path().join("values.aedbdat"), 16);

    let err = db
        .kv_get_no_auth("p", "app", b"blob", ConsistencyMode::AtLatest)
        .await
        .expect_err("point lookup must return a structured corruption error");
    assert!(
        matches!(err, QueryError::InternalError(message) if message.contains("persistent value hash mismatch"))
    );

    let preflight = db
        .preflight(Mutation::KvMutateU64 {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"blob".to_vec(),
            op: crate::commit::validation::KvU64MutatorOp::Set,
            operand_be: 1u64.to_be_bytes(),
            expected_seq: Some(1),
        })
        .await;
    assert!(
        matches!(preflight, crate::preflight::PreflightResult::Err { reason } if reason.contains("persistent value hash mismatch"))
    );

    let commit_err = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: vec![ReadAssertion::KeyEquals {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"blob".to_vec(),
                expected: b"anything".to_vec(),
            }],
            read_set: Default::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"after-corruption".to_vec(),
                    value: b"blocked".to_vec(),
                }],
            },
            base_seq: 0,
        })
        .await
        .expect_err("commit assertion must return corruption error");
    assert!(
        matches!(commit_err, AedbError::IntegrityError { ref message } if message.contains("persistent value hash mismatch")),
        "unexpected commit error: {commit_err:?}"
    );

    db.shutdown().await.expect("shutdown");
    drop(db);

    let reopened = AedbInstance::open(config, dir.path()).expect("reopen");
    let recovered = reopened
        .kv_get_no_auth("p", "app", b"blob", ConsistencyMode::AtLatest)
        .await
        .expect("WAL recovery should not panic")
        .expect("WAL should recover the committed value");
    assert_eq!(recovered.value.len(), 128 * 1024);
    reopened.shutdown().await.expect("shutdown reopened");
}

#[tokio::test]
async fn disk_backed_kv_segment_corruption_fails_closed_for_point_prefix_and_secure_reads() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        storage_mode: StorageMode::DiskBacked,
        persistent_value_inline_threshold_bytes: 1024,
        persistent_value_hot_cache_bytes: 0,
        kv_segment_block_cache_bytes: 0,
        max_memory_estimate_bytes: 10 * 1024,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open(config, dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.kv_set_many_atomic(
        "p",
        "app",
        (0..512u16)
            .map(|i| (format!("seg:{i:03}").into_bytes(), vec![i as u8; 16]))
            .collect(),
    )
    .await
    .expect("seed segment values");

    let caller = CallerContext::new("alice");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "alice".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: Some(b"seg:".to_vec()),
        },
    }))
    .await
    .expect("grant read");

    corrupt_file_byte(&first_kv_segment_path(dir.path()), 16);

    let point_err = db
        .kv_get_no_auth("p", "app", b"seg:000", ConsistencyMode::AtLatest)
        .await
        .expect_err("point lookup must fail on corrupt segment");
    assert!(
        matches!(point_err, QueryError::InternalError(message) if message.contains("KV segment"))
    );

    let prefix_err = db
        .kv_scan_prefix_no_auth("p", "app", b"seg:", 10, ConsistencyMode::AtLatest)
        .await
        .expect_err("prefix scan must fail on corrupt segment");
    assert!(
        matches!(prefix_err, QueryError::InternalError(message) if message.contains("KV segment"))
    );

    let secure_err = db
        .kv_scan_prefix(
            "p",
            "app",
            b"seg:",
            10,
            None,
            ConsistencyMode::AtLatest,
            &caller,
        )
        .await
        .expect_err("secure scan must not disclose partial rows after corruption");
    assert!(
        matches!(secure_err, QueryError::InternalError(message) if message.contains("KV segment"))
    );
}

#[tokio::test]
async fn production_profile_spills_all_kv_payloads_by_default() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig::production([7u8; 32]);
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.kv_set("p", "app", b"tiny".to_vec(), b"x".to_vec())
        .await
        .expect("set tiny value");

    let metrics = db.operational_metrics().await;
    assert!(
        metrics.persistent_value_store_bytes > 8,
        "production config should spill even tiny KV payloads to values.aedbdat"
    );
    let got = db
        .kv_get_no_auth("p", "app", b"tiny", ConsistencyMode::AtLatest)
        .await
        .expect("get")
        .expect("value");
    assert_eq!(got.value, b"x");
    db.shutdown().await.expect("shutdown");
    drop(db);

    let reopened = AedbInstance::open(config, dir.path()).expect("reopen");
    let recovered = reopened
        .kv_get_no_auth("p", "app", b"tiny", ConsistencyMode::AtLatest)
        .await
        .expect("recovered get")
        .expect("recovered value");
    assert_eq!(recovered.value, b"x");
    reopened.shutdown().await.expect("shutdown reopened");
}

#[tokio::test]
async fn unused_kv_segment_reclaim_keeps_live_disk_state() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        storage_mode: StorageMode::DiskBacked,
        persistent_value_inline_threshold_bytes: 0,
        persistent_value_hot_cache_bytes: 0,
        kv_segment_block_cache_bytes: 16 * 1024,
        max_memory_estimate_bytes: 10 * 1024,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open(config, dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    for generation_number in 0..5u8 {
        let entries = (0..512u16)
            .map(|entry_number| {
                (
                    format!("k{entry_number:04}").into_bytes(),
                    vec![generation_number; 32],
                )
            })
            .collect::<Vec<_>>();
        db.kv_set_many_atomic("p", "app", entries)
            .await
            .expect("set generation");
    }
    let read_tx = db
        .begin_read_tx(ConsistencyMode::AtLatest)
        .await
        .expect("read tx");
    let read_tx_seq = read_tx.snapshot_seq();
    for generation_number in 5..10u8 {
        let entries = (0..512u16)
            .map(|entry_number| {
                (
                    format!("k{entry_number:04}").into_bytes(),
                    vec![generation_number; 32],
                )
            })
            .collect::<Vec<_>>();
        db.kv_set_many_atomic("p", "app", entries)
            .await
            .expect("set later generation");
    }

    let segment_dir = dir.path().join("kv_segments");
    let before_reclaim = fs::read_dir(&segment_dir)
        .expect("segment dir")
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".aedbkv"))
        .count();
    assert!(before_reclaim > 1);

    let reclaimed = db
        .reclaim_unused_kv_segments()
        .await
        .expect("reclaim unused segments");
    assert!(reclaimed > 0);
    let after_reclaim_with_read_tx = fs::read_dir(&segment_dir)
        .expect("segment dir")
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".aedbkv"))
        .count();
    assert!(after_reclaim_with_read_tx < before_reclaim);
    let old_key = b"k0000".to_vec();
    let old_value = db
        .kv_get_no_auth("p", "app", &old_key, ConsistencyMode::AtSeq(read_tx_seq))
        .await
        .expect("old read")
        .expect("old value");
    assert_eq!(old_value.value, vec![4u8; 32]);
    drop(read_tx);
    let reclaimed_after_drop = db
        .reclaim_unused_kv_segments()
        .await
        .expect("reclaim after read tx");
    assert!(reclaimed_after_drop > 0);
    let after_reclaim = fs::read_dir(&segment_dir)
        .expect("segment dir")
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".aedbkv"))
        .count();
    assert!(after_reclaim < after_reclaim_with_read_tx);

    for entry_number in [0u16, 1, 127, 255, 511] {
        let key = format!("k{entry_number:04}").into_bytes();
        let got = db
            .kv_get_no_auth("p", "app", &key, ConsistencyMode::AtLatest)
            .await
            .expect("read after reclaim")
            .expect("value");
        assert_eq!(got.value, vec![9u8; 32]);
    }
}

#[tokio::test]
async fn disk_backed_small_kv_values_spill_under_memory_pressure() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        storage_mode: StorageMode::DiskBacked,
        persistent_value_inline_threshold_bytes: 1024,
        persistent_value_hot_cache_bytes: 0,
        max_kv_value_bytes: 1024,
        max_transaction_bytes: 4096,
        max_memory_estimate_bytes: 2048,
        ..AedbConfig::default()
    };

    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    for i in 0..32u8 {
        db.kv_set(
            "p",
            "app",
            format!("small:{i:02}").into_bytes(),
            vec![i; 256],
        )
        .await
        .expect("set small value");
    }

    let memory_estimate = db.estimated_memory_bytes().await;
    assert!(
        memory_estimate <= config.max_memory_estimate_bytes,
        "small KV payloads should spill before memory guard rejects: {memory_estimate}"
    );
    let metrics = db.operational_metrics().await;
    assert!(
        metrics.persistent_value_store_bytes > 8 + 16 * 256,
        "persistent value store should contain pressure-spilled small payloads"
    );
    for i in 0..32u8 {
        let key = format!("small:{i:02}");
        let got = db
            .kv_get_no_auth("p", "app", key.as_bytes(), ConsistencyMode::AtLatest)
            .await
            .expect("get")
            .expect("value");
        assert_eq!(got.value, vec![i; 256]);
    }
    db.shutdown().await.expect("shutdown");
    drop(db);

    let reopened = AedbInstance::open(config.clone(), dir.path()).expect("reopen");
    let recovered_memory_estimate = reopened.estimated_memory_bytes().await;
    assert!(
        recovered_memory_estimate <= config.max_memory_estimate_bytes,
        "recovery should re-spill small KV payloads under memory target: {recovered_memory_estimate}"
    );
    for i in 0..32u8 {
        let key = format!("small:{i:02}");
        let got = reopened
            .kv_get_no_auth("p", "app", key.as_bytes(), ConsistencyMode::AtLatest)
            .await
            .expect("recovered get")
            .expect("recovered value");
        assert_eq!(got.value, vec![i; 256]);
    }
    reopened.shutdown().await.expect("shutdown reopened");
}

#[tokio::test]
async fn disk_backed_kv_spilled_versions_remain_snapshot_consistent() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        storage_mode: StorageMode::DiskBacked,
        persistent_value_inline_threshold_bytes: 16,
        persistent_value_hot_cache_bytes: 0,
        max_kv_value_bytes: 256 * 1024,
        max_transaction_bytes: 512 * 1024,
        ..AedbConfig::default()
    };
    let first_value: Vec<u8> = (0..96 * 1024).map(|i| (i % 251) as u8).collect();
    let second_value: Vec<u8> = (0..96 * 1024).map(|i| 255 - (i % 251) as u8).collect();

    let db = AedbInstance::open(config, dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    let first = db
        .kv_set("p", "app", b"blob".to_vec(), first_value.clone())
        .await
        .expect("first set");
    let second = db
        .kv_set("p", "app", b"blob".to_vec(), second_value.clone())
        .await
        .expect("second set");

    let at_first = db
        .kv_get_no_auth(
            "p",
            "app",
            b"blob",
            ConsistencyMode::AtSeq(first.commit_seq),
        )
        .await
        .expect("get first snapshot")
        .expect("first snapshot value");
    assert_eq!(at_first.value, first_value);

    let at_second = db
        .kv_get_no_auth(
            "p",
            "app",
            b"blob",
            ConsistencyMode::AtSeq(second.commit_seq),
        )
        .await
        .expect("get second snapshot")
        .expect("second snapshot value");
    assert_eq!(at_second.value, second_value);
    db.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn kv_sub_u256_soft_noop() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    db.commit(Mutation::KvSubU256Ex {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"soft-noop".to_vec(),
        amount_be: u256_be_test(5),
        on_missing: KvU256MissingPolicy::TreatAsZero,
        on_underflow: KvU256UnderflowPolicy::NoOp,
    })
    .await
    .expect("soft noop");

    let entry = db
        .kv_get_no_auth("p", "app", b"soft-noop", ConsistencyMode::AtLatest)
        .await
        .expect("read");
    assert!(
        entry.is_none(),
        "no-op decrement must not create/update key"
    );
}

#[tokio::test]
async fn kv_sub_u256_strict_reject() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let before = db.head_state().await.visible_head_seq;
    let err = db
        .commit(Mutation::KvSubU256Ex {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"strict-reject".to_vec(),
            amount_be: u256_be_test(1),
            on_missing: KvU256MissingPolicy::TreatAsZero,
            on_underflow: KvU256UnderflowPolicy::Reject,
        })
        .await
        .expect_err("strict underflow must reject");
    assert!(
        matches!(
            err,
            AedbError::Underflow | AedbError::Validation(_) | AedbError::Conflict(_)
        ),
        "expected strict reject failure, got {err:?}"
    );
    assert_eq!(db.head_state().await.visible_head_seq, before);
}

#[tokio::test]
async fn kv_max_min_u256() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"maxmin".to_vec(),
        value: u256_be_test(10).to_vec(),
    })
    .await
    .expect("seed");

    db.commit(Mutation::KvMaxU256 {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"maxmin".to_vec(),
        candidate_be: u256_be_test(12),
        on_missing: KvU256MissingPolicy::TreatAsZero,
    })
    .await
    .expect("max");

    db.commit(Mutation::KvMinU256 {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"maxmin".to_vec(),
        candidate_be: u256_be_test(3),
        on_missing: KvU256MissingPolicy::TreatAsZero,
    })
    .await
    .expect("min");

    let entry = db
        .kv_get_no_auth("p", "app", b"maxmin", ConsistencyMode::AtLatest)
        .await
        .expect("read")
        .expect("present");
    assert_eq!(
        primitive_types::U256::from_big_endian(&entry.value),
        primitive_types::U256::from(3u64)
    );
}

#[tokio::test]
async fn kv_sub_u64_soft_noop() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    db.commit(Mutation::KvSubU64Ex {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"soft-noop-u64".to_vec(),
        amount_be: u64_be_test(5),
        on_missing: KvU64MissingPolicy::TreatAsZero,
        on_underflow: KvU64UnderflowPolicy::NoOp,
    })
    .await
    .expect("soft noop");

    let entry = db
        .kv_get_no_auth("p", "app", b"soft-noop-u64", ConsistencyMode::AtLatest)
        .await
        .expect("read");
    assert!(
        entry.is_none(),
        "no-op decrement must not create/update key"
    );
}

#[tokio::test]
async fn kv_sub_u64_strict_reject() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let before = db.head_state().await.visible_head_seq;
    let err = db
        .commit(Mutation::KvSubU64Ex {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"strict-reject-u64".to_vec(),
            amount_be: u64_be_test(1),
            on_missing: KvU64MissingPolicy::TreatAsZero,
            on_underflow: KvU64UnderflowPolicy::Reject,
        })
        .await
        .expect_err("strict underflow must reject");
    assert!(
        matches!(
            err,
            AedbError::Underflow | AedbError::Validation(_) | AedbError::Conflict(_)
        ),
        "expected strict reject failure, got {err:?}"
    );
    assert_eq!(db.head_state().await.visible_head_seq, before);
}

#[tokio::test]
async fn kv_sub_int_ex_supports_u64() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"generic-u64".to_vec(),
        value: u64_be_test(10).to_vec(),
    })
    .await
    .expect("seed");

    db.commit(Mutation::KvSubIntEx {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"generic-u64".to_vec(),
        amount: KvIntegerAmount::U64(u64_be_test(4)),
        on_missing: KvIntegerMissingPolicy::Reject,
        on_underflow: KvIntegerUnderflowPolicy::Reject,
    })
    .await
    .expect("sub");

    let entry = db
        .kv_get_no_auth("p", "app", b"generic-u64", ConsistencyMode::AtLatest)
        .await
        .expect("read")
        .expect("present");
    let final_value = u64::from_be_bytes(entry.value.try_into().expect("u64 bytes"));
    assert_eq!(final_value, 6);
}

#[tokio::test]
async fn kv_sub_int_ex_supports_u256_noop_on_underflow() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    db.commit(Mutation::KvSubIntEx {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"generic-u256".to_vec(),
        amount: KvIntegerAmount::U256(u256_be_test(3)),
        on_missing: KvIntegerMissingPolicy::TreatAsZero,
        on_underflow: KvIntegerUnderflowPolicy::NoOp,
    })
    .await
    .expect("noop");

    let entry = db
        .kv_get_no_auth("p", "app", b"generic-u256", ConsistencyMode::AtLatest)
        .await
        .expect("read");
    assert!(entry.is_none(), "u256 no-op decrement must not create key");
}

#[tokio::test]
async fn kv_max_min_u64() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"maxmin-u64".to_vec(),
        value: u64_be_test(10).to_vec(),
    })
    .await
    .expect("seed");

    db.commit(Mutation::KvMaxU64 {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"maxmin-u64".to_vec(),
        candidate_be: u64_be_test(12),
        on_missing: KvU64MissingPolicy::TreatAsZero,
    })
    .await
    .expect("max");

    db.commit(Mutation::KvMinU64 {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"maxmin-u64".to_vec(),
        candidate_be: u64_be_test(3),
        on_missing: KvU64MissingPolicy::TreatAsZero,
    })
    .await
    .expect("min");

    let entry = db
        .kv_get_no_auth("p", "app", b"maxmin-u64", ConsistencyMode::AtLatest)
        .await
        .expect("read")
        .expect("present");
    let final_value = u64::from_be_bytes(entry.value.try_into().expect("u64 bytes"));
    assert_eq!(final_value, 3u64);
}

#[tokio::test]
async fn counter_add_and_read_sharded_respects_snapshot_consistency() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    for shard_hint in 0..32u32 {
        db.counter_add_sharded(
            "p",
            "app",
            b"ctr:orders".to_vec(),
            u64_be_test(1),
            8,
            shard_hint,
        )
        .await
        .expect("counter add");
    }
    let mid_seq = db.head_state().await.visible_head_seq;

    for shard_hint in 32..64u32 {
        db.counter_add_sharded(
            "p",
            "app",
            b"ctr:orders".to_vec(),
            u64_be_test(1),
            8,
            shard_hint,
        )
        .await
        .expect("counter add");
    }

    let at_mid = db
        .counter_read_sharded(
            "p",
            "app",
            b"ctr:orders",
            8,
            ConsistencyMode::AtSeq(mid_seq),
        )
        .await
        .expect("counter read at seq");
    assert_eq!(at_mid, 32);

    let at_latest = db
        .counter_read_sharded("p", "app", b"ctr:orders", 8, ConsistencyMode::AtLatest)
        .await
        .expect("counter read latest");
    assert_eq!(at_latest, 64);
}

#[tokio::test]
async fn counter_shard_count_validation_is_enforced() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let err_zero = db
        .counter_add_sharded("p", "app", b"ctr:invalid".to_vec(), u64_be_test(1), 0, 1)
        .await
        .expect_err("zero shard_count must reject");
    assert!(matches!(err_zero, AedbError::Validation(_)));

    let err_too_many = db
        .counter_add_sharded(
            "p",
            "app",
            b"ctr:invalid".to_vec(),
            u64_be_test(1),
            MAX_COUNTER_SHARDS + 1,
            1,
        )
        .await
        .expect_err("oversized shard_count must reject");
    assert!(matches!(err_too_many, AedbError::Validation(_)));

    let read_err = db
        .counter_read_sharded("p", "app", b"ctr:invalid", 0, ConsistencyMode::AtLatest)
        .await
        .expect_err("zero shard_count read must reject");
    assert!(matches!(read_err, AedbError::Validation(_)));
}

#[tokio::test]
async fn transaction_envelope_can_commit_table_kv_and_integer_atomically() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "ledger".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "amount".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "status".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("ledger table");
    let envelope = TransactionEnvelope {
        caller: None,
        idempotency_key: Some(IdempotencyKey([11u8; 16])),
        write_class: WriteClass::Standard,
        assertions: Vec::new(),
        read_set: Default::default(),
        write_intent: WriteIntent {
            mutations: vec![
                Mutation::Upsert {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    table_name: "ledger".into(),
                    primary_key: vec![Value::Integer(1)],
                    row: Row::from_values(vec![
                        Value::Integer(1),
                        Value::Integer(75),
                        Value::Text("posted".into()),
                    ]),
                },
                Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"ledger:last".to_vec(),
                    value: b"1".to_vec(),
                },
                Mutation::KvAddU64Ex {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"house_balance".to_vec(),
                    amount_be: 75u64.to_be_bytes(),
                    on_missing: KvU64MissingPolicy::TreatAsZero,
                    on_overflow: KvU64OverflowPolicy::Reject,
                },
            ],
        },
        base_seq: db.head_state().await.visible_head_seq,
    };

    let result = db
        .commit_envelope(envelope.clone())
        .await
        .expect("mixed transaction envelope");
    assert!(
        result.commit_seq > 0,
        "transaction must advance the commit sequence"
    );

    let row = db
        .query(
            "p",
            "app",
            Query::select(&["amount", "status"])
                .from("ledger")
                .where_(Expr::Eq("id".into(), Value::Integer(1)))
                .limit(1),
        )
        .await
        .expect("query ledger row");
    assert_eq!(row.rows.len(), 1);
    assert_eq!(row.rows[0].values[0], Value::Integer(75));
    assert_eq!(row.rows[0].values[1], Value::Text("posted".into()));

    let marker = db
        .kv_get_no_auth("p", "app", b"ledger:last", ConsistencyMode::AtLatest)
        .await
        .expect("read kv marker");
    assert_eq!(
        marker.as_ref().map(|entry| entry.value.as_slice()),
        Some(b"1".as_slice())
    );

    let balance = db
        .kv_get_no_auth("p", "app", b"house_balance", ConsistencyMode::AtLatest)
        .await
        .expect("read integer balance")
        .expect("integer balance exists");
    let balance_bytes: [u8; 8] = balance
        .value
        .as_slice()
        .try_into()
        .expect("u64 balance bytes");
    assert_eq!(u64::from_be_bytes(balance_bytes), 75);

    let replay = db
        .commit_envelope(envelope)
        .await
        .expect("idempotent replay");
    assert_eq!(
        replay.idempotency,
        crate::commit::executor::IdempotencyOutcome::Duplicate,
        "second commit must be treated as replay"
    );

    let stable_row = db
        .query(
            "p",
            "app",
            Query::select(&["amount", "status"])
                .from("ledger")
                .where_(Expr::Eq("id".into(), Value::Integer(1)))
                .limit(1),
        )
        .await
        .expect("query stable row");
    assert_eq!(stable_row.rows[0].values[0], Value::Integer(75));
    assert_eq!(stable_row.rows[0].values[1], Value::Text("posted".into()));
    let stable_marker = db
        .kv_get_no_auth("p", "app", b"ledger:last", ConsistencyMode::AtLatest)
        .await
        .expect("read stable marker");
    assert_eq!(
        stable_marker.as_ref().map(|entry| entry.value.as_slice()),
        Some(b"1".as_slice())
    );
    let stable_balance = db
        .kv_get_no_auth("p", "app", b"house_balance", ConsistencyMode::AtLatest)
        .await
        .expect("read stable integer balance")
        .expect("stable integer balance exists");
    let stable_balance_bytes: [u8; 8] = stable_balance
        .value
        .as_slice()
        .try_into()
        .expect("stable u64 balance bytes");
    assert_eq!(u64::from_be_bytes(stable_balance_bytes), 75);
}

#[tokio::test]
async fn commit_rejects_oversized_kv_before_enqueue() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        max_kv_value_bytes: 4,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open(config, dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let err = db
        .kv_set("p", "app", b"k".to_vec(), b"too-large".to_vec())
        .await
        .expect_err("oversized kv write should fail");
    assert!(matches!(err, AedbError::Validation(ref msg) if msg.contains("kv value size")));
}

#[tokio::test]
async fn batch_mutations_respect_max_batch_rows() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        max_batch_rows: 3,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open(config, dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");
    create_table(
        &db,
        "p",
        "app",
        "items",
        vec![ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Integer,
            nullable: false,
        }],
        vec!["id"],
    )
    .await;

    let rows = (1_i64..=4_i64)
        .map(|id| Row::from_values(vec![Value::Integer(id)]))
        .collect::<Vec<_>>();
    let insert_err = db
        .commit(Mutation::InsertBatch {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "items".into(),
            rows: rows.clone(),
        })
        .await
        .expect_err("insert batch should be bounded");
    assert!(matches!(insert_err, AedbError::Validation(ref msg) if msg.contains("max_batch_rows")));

    let upsert_err = db
        .commit(Mutation::UpsertBatch {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "items".into(),
            rows,
        })
        .await
        .expect_err("upsert batch should be bounded");
    assert!(matches!(upsert_err, AedbError::Validation(ref msg) if msg.contains("max_batch_rows")));
}

#[tokio::test]
async fn memory_limit_is_enforced_before_wal_commit() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        max_memory_estimate_bytes: 1_000,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open(config, dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");
    create_table(
        &db,
        "p",
        "app",
        "items",
        vec![ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Integer,
            nullable: false,
        }],
        vec!["id"],
    )
    .await;

    let err = db
        .commit(Mutation::Insert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "items".into(),
            primary_key: vec![Value::Integer(1)],
            row: Row::from_values(vec![Value::Integer(1)]),
        })
        .await
        .expect_err("memory limit should reject commit");
    assert!(
        matches!(err, AedbError::Validation(ref msg) if msg.contains("memory estimate exceeded before WAL commit"))
    );

    let rows = db
        .query_with_options(
            "p",
            "app",
            Query::select(&["id"]).from("items").limit(10),
            QueryOptions::default(),
        )
        .await
        .expect("query rows");
    assert!(
        rows.rows.is_empty(),
        "rejected commit must leave no row behind"
    );
}

#[test]
fn mutation_write_keys_kv_and_scope_variants() {
    use crate::commit::tx::WriteKey;

    let kv = Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "s".into(),
        key: b"k".to_vec(),
        value: b"v".to_vec(),
    };
    let keys = kv.write_keys();
    assert_eq!(keys.len(), 1);
    assert!(matches!(
        &keys[0],
        WriteKey::KvKey { project_id, scope_id, key }
            if project_id == "p" && scope_id == "s" && key == b"k"
    ));

    let emit = Mutation::EmitEvent {
        project_id: "p".into(),
        scope_id: "s".into(),
        topic: "topic".into(),
        event_key: "ek".into(),
        payload_json: "{}".into(),
    };
    let keys = emit.write_keys();
    assert!(matches!(
        &keys[0],
        WriteKey::ScopeAll { project_id, scope_id }
            if project_id == "p" && scope_id == "s"
    ));
}
