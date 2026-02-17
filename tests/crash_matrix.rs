use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::checkpoint::writer::write_checkpoint_with_key;
use aedb::commit::tx::{IdempotencyKey, ReadSet, TransactionEnvelope, WriteClass, WriteIntent};
use aedb::commit::validation::Mutation;
use aedb::config::{AedbConfig, DurabilityMode, RecoveryMode};
use aedb::error::AedbError;
use aedb::manifest::atomic::load_manifest_signed;
use aedb::query::plan::{ConsistencyMode, Expr, Query};
use aedb::recovery::recover_with_config;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::tempdir;

fn production_config() -> AedbConfig {
    AedbConfig::production([7u8; 32])
}

fn batch_config() -> AedbConfig {
    AedbConfig {
        durability_mode: DurabilityMode::Batch,
        batch_interval_ms: 60_000,
        batch_max_bytes: usize::MAX,
        recovery_mode: RecoveryMode::Permissive,
        hash_chain_required: false,
        ..AedbConfig::default()
    }
}

fn timeout_config() -> AedbConfig {
    AedbConfig {
        durability_mode: DurabilityMode::Full,
        recovery_mode: RecoveryMode::Permissive,
        hash_chain_required: false,
        partition_lock_timeout_ms: 1,
        epoch_apply_timeout_ms: 1,
        ..AedbConfig::default()
    }
}

fn segment_paths(dir: &Path) -> Vec<PathBuf> {
    let mut out: Vec<(u64, PathBuf)> = fs::read_dir(dir)
        .expect("read dir")
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            if !name.starts_with("segment_") || !name.ends_with(".aedbwal") {
                return None;
            }
            let seq = name
                .trim_start_matches("segment_")
                .trim_end_matches(".aedbwal")
                .parse::<u64>()
                .ok()?;
            Some((seq, path))
        })
        .collect();
    out.sort_by_key(|(seq, _)| *seq);
    out.into_iter().map(|(_, p)| p).collect()
}

fn corrupt_file_byte(path: &Path, at: usize) {
    let mut bytes = fs::read(path).expect("read file to corrupt");
    assert!(bytes.len() > at, "file too small to corrupt at offset {at}");
    bytes[at] ^= 0xFF;
    fs::write(path, bytes).expect("rewrite corrupted file");
}

fn env_or_u64(default: u64, var: &str) -> u64 {
    std::env::var(var)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

async fn setup_a17_cycle_table(dir: &Path, writer_config: &AedbConfig) {
    let bootstrap = AedbInstance::open(writer_config.clone(), dir).expect("open");
    seed_project(&bootstrap).await;
    bootstrap
        .commit(Mutation::Ddl(DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: false,
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "a17_cycles".into(),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "value".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
            ],
            primary_key: vec!["id".into()],
        }))
        .await
        .expect("create cycle table");
    bootstrap
        .checkpoint_now()
        .await
        .expect("initial checkpoint");
    bootstrap.shutdown().await.expect("bootstrap shutdown");
}

async fn a17_write_cycle(dir: &Path, writer_config: &AedbConfig, cycle: u64) {
    let db = AedbInstance::open(writer_config.clone(), dir).expect("reopen");
    let cycle_i64 = i64::try_from(cycle).expect("cycle must fit i64");
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "a17_cycles".into(),
        primary_key: vec![Value::Integer(cycle_i64)],
        row: Row::from_values(vec![Value::Integer(cycle_i64), Value::Integer(cycle_i64)]),
    })
    .await
    .expect("cycle write");
    db.checkpoint_now().await.expect("cycle checkpoint");
    db.shutdown().await.expect("writer shutdown");
}

async fn assert_a17_cycle_row(dir: &Path, writer_config: &AedbConfig, cycle: u64) {
    let cycle_i64 = i64::try_from(cycle).expect("cycle must fit i64");
    let verify = AedbInstance::open(writer_config.clone(), dir)
        .unwrap_or_else(|e| panic!("reopen verify failed at cycle {cycle}: {e}"));
    let row = verify
        .query(
            "p",
            "app",
            Query::select(&["id", "value"])
                .from("a17_cycles")
                .where_(Expr::Eq("id".into(), Value::Integer(cycle_i64)))
                .limit(1),
        )
        .await
        .expect("query cycle row");
    assert_eq!(row.rows.len(), 1, "cycle row should exist at cycle {cycle}");
    verify.shutdown().await.expect("verify shutdown");
}

async fn seed_project(db: &AedbInstance) {
    db.create_project("p").await.expect("create project");
}

#[tokio::test]
async fn crash_matrix_baseline_graceful_shutdown_recovers_all_commits() {
    let dir = tempdir().expect("temp dir");
    let config = production_config();
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    seed_project(&db).await;

    for i in 0..1_000u64 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("k:{i}").into_bytes(),
            value: vec![u8::try_from(i % 251).unwrap_or(0)],
        })
        .await
        .expect("commit");
    }
    db.shutdown().await.expect("shutdown");
    drop(db);

    let recovered = recover_with_config(dir.path(), &config).expect("recover");
    assert_eq!(recovered.current_seq, 1_001);
    assert!(recovered.keyspace.kv_get("p", "app", b"k:0").is_some());
    assert!(recovered.keyspace.kv_get("p", "app", b"k:999").is_some());
}

#[tokio::test]
async fn crash_matrix_mid_commit_economic_survives_restart() {
    let dir = tempdir().expect("temp dir");
    let config = production_config();
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    seed_project(&db).await;

    let result = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: Some(IdempotencyKey([1u8; 16])),
            write_class: WriteClass::Economic,
            assertions: Vec::new(),
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"economic".to_vec(),
                    value: b"ok".to_vec(),
                }],
            },
            base_seq: 0,
        })
        .await
        .expect("economic commit");
    db.checkpoint_now().await.expect("manifest before restart");
    drop(db);

    let recovered = recover_with_config(dir.path(), &config).expect("recover");
    let entry = recovered
        .keyspace
        .kv_get("p", "app", b"economic")
        .expect("economic key");
    assert_eq!(entry.version, result.commit_seq);
    assert_eq!(entry.value, b"ok".to_vec());
}

#[tokio::test]
async fn crash_matrix_mid_commit_batch_loses_unflushed_tail() {
    let dir = tempdir().expect("temp dir");
    let config = batch_config();
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    seed_project(&db).await;

    for i in 0..64u64 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("b:{i}").into_bytes(),
            value: vec![1u8],
        })
        .await
        .expect("batch commit");
    }
    let head_before = db.head_state().await;
    assert!(head_before.visible_head_seq > head_before.durable_head_seq);
    drop(db);

    let recovered = recover_with_config(dir.path(), &config).expect("recover");
    assert!(recovered.current_seq >= head_before.durable_head_seq);
    assert!(recovered.current_seq <= head_before.visible_head_seq);
}

#[tokio::test]
async fn crash_matrix_mid_checkpoint_tmp_file_is_ignored() {
    let dir = tempdir().expect("temp dir");
    let config = production_config();
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    seed_project(&db).await;
    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"before".to_vec(),
        value: b"1".to_vec(),
    })
    .await
    .expect("before");
    let seq_before = db.checkpoint_now().await.expect("checkpoint");

    fs::write(
        dir.path().join("checkpoint_9999999999999999.aedb.zst.tmp"),
        b"partial-checkpoint",
    )
    .expect("write tmp checkpoint");

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"after".to_vec(),
        value: b"2".to_vec(),
    })
    .await
    .expect("after");
    drop(db);

    let recovered = recover_with_config(dir.path(), &config).expect("recover");
    assert_eq!(recovered.current_seq, seq_before);
    assert!(recovered.keyspace.kv_get("p", "app", b"before").is_some());
    assert!(recovered.keyspace.kv_get("p", "app", b"after").is_none());
}

#[tokio::test]
async fn crash_matrix_mid_manifest_primary_corruption_falls_back_to_prev() {
    let dir = tempdir().expect("temp dir");
    let config = production_config();
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    seed_project(&db).await;
    db.checkpoint_now().await.expect("checkpoint 1");
    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"x".to_vec(),
        value: b"1".to_vec(),
    })
    .await
    .expect("write");
    db.checkpoint_now().await.expect("checkpoint 2");
    drop(db);

    fs::write(dir.path().join("manifest.json"), b"{corrupt").expect("corrupt primary manifest");
    fs::write(dir.path().join("manifest.hmac"), "deadbeef").expect("corrupt primary hmac");

    let reopened = AedbInstance::open(config.clone(), dir.path()).expect("reopen");
    let seq = reopened
        .snapshot_probe(ConsistencyMode::AtLatest)
        .await
        .expect("snapshot seq");
    assert!(seq >= 1);
}

#[tokio::test]
async fn crash_matrix_after_checkpoint_before_manifest_respects_manifest_lower_bound() {
    let dir = tempdir().expect("temp dir");
    let config = production_config();
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    seed_project(&db).await;
    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"base".to_vec(),
        value: b"v1".to_vec(),
    })
    .await
    .expect("base");
    let _ = db.checkpoint_now().await.expect("manifested checkpoint");
    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"tail".to_vec(),
        value: b"v2".to_vec(),
    })
    .await
    .expect("tail");

    drop(db);
    let recovered_now = recover_with_config(dir.path(), &config).expect("recover for snapshot");
    let _unreferenced = write_checkpoint_with_key(
        &recovered_now.keyspace.snapshot(),
        &recovered_now.catalog,
        recovered_now.current_seq,
        dir.path(),
        config.checkpoint_key(),
        config.checkpoint_key_id.clone(),
        HashMap::new(),
    )
    .expect("write unreferenced checkpoint");

    let recovered = recover_with_config(dir.path(), &config).expect("recover");
    assert!(recovered.keyspace.kv_get("p", "app", b"base").is_some());
    assert!(recovered.keyspace.kv_get("p", "app", b"tail").is_none());
}

#[tokio::test]
async fn crash_matrix_corrupt_wal_frame_fails_closed() {
    let dir = tempdir().expect("temp dir");
    let config = production_config();
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    seed_project(&db).await;
    for i in 0..20u64 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("w:{i}").into_bytes(),
            value: vec![9u8],
        })
        .await
        .expect("write");
    }
    db.checkpoint_now().await.expect("checkpoint");
    drop(db);

    let seg = segment_paths(dir.path()).pop().expect("segment");
    corrupt_file_byte(&seg, 96);

    let reopen = AedbInstance::open(config, dir.path());
    assert!(reopen.is_err());
}

#[tokio::test]
async fn crash_matrix_corrupt_wal_frame_fails_closed_in_permissive_mode() {
    let dir = tempdir().expect("temp dir");
    let mut config = AedbConfig::development();
    config.durability_mode = DurabilityMode::Full;
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    seed_project(&db).await;
    for i in 0..20u64 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("w:{i}").into_bytes(),
            value: vec![9u8],
        })
        .await
        .expect("write");
    }
    db.checkpoint_now().await.expect("checkpoint");
    drop(db);

    let seg = segment_paths(dir.path()).pop().expect("segment");
    corrupt_file_byte(&seg, 96);

    let reopen = AedbInstance::open(config, dir.path());
    assert!(reopen.is_err());
}

#[tokio::test]
async fn crash_matrix_corrupt_manifest_hmac_fails_closed() {
    let dir = tempdir().expect("temp dir");
    let config = production_config();
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    seed_project(&db).await;
    db.checkpoint_now().await.expect("checkpoint");
    drop(db);

    fs::write(dir.path().join("manifest.hmac"), "00").expect("tamper hmac");
    let reopen = AedbInstance::open(config, dir.path());
    assert!(reopen.is_err());
}

#[tokio::test]
async fn crash_matrix_segment_deletion_breaks_hash_chain() {
    let dir = tempdir().expect("temp dir");
    let mut config = production_config();
    config.max_segment_bytes = 512;
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    seed_project(&db).await;
    for i in 0..200u64 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("seg:{i}").into_bytes(),
            value: vec![u8::try_from(i % 251).unwrap_or(0); 64],
        })
        .await
        .expect("write");
    }
    db.checkpoint_now().await.expect("checkpoint");
    drop(db);

    let segments = segment_paths(dir.path());
    assert!(segments.len() >= 3, "need >=3 segments for deletion test");
    let middle = segments[segments.len() / 2].clone();
    fs::remove_file(middle).expect("delete middle segment");

    let reopen = AedbInstance::open(config, dir.path());
    assert!(reopen.is_err());
}

#[tokio::test]
async fn crash_matrix_idempotency_survives_restart() {
    let dir = tempdir().expect("temp dir");
    let config = production_config();
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    seed_project(&db).await;

    let envelope = TransactionEnvelope {
        caller: None,
        idempotency_key: Some(IdempotencyKey([9u8; 16])),
        write_class: WriteClass::Economic,
        assertions: Vec::new(),
        read_set: ReadSet::default(),
        write_intent: WriteIntent {
            mutations: vec![Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"idem:key".to_vec(),
                value: b"first".to_vec(),
            }],
        },
        base_seq: 0,
    };
    let first = db
        .commit_envelope(envelope.clone())
        .await
        .expect("first submit");
    db.checkpoint_now().await.expect("manifest before restart");
    drop(db);

    let reopened = AedbInstance::open(config.clone(), dir.path()).expect("reopen");
    let second = reopened
        .commit_envelope(envelope)
        .await
        .expect("second submit");
    assert_eq!(first.commit_seq, second.commit_seq);
    assert!(second.durable_head_seq >= second.commit_seq);
}

#[tokio::test]
async fn crash_matrix_coordinator_timeout_recovery_has_no_partial_writes() {
    let dir = tempdir().expect("temp dir");
    let config = timeout_config();
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    seed_project(&db).await;
    db.commit(Mutation::Ddl(DdlOperation::CreateScope {
        owner_id: None,
        if_not_exists: true,
        project_id: "p".into(),
        scope_id: "other".into(),
    }))
    .await
    .expect("create scope");

    let base_seq = db.head_state().await.visible_head_seq;
    let mut mutations = Vec::new();
    for i in 0..10_000u64 {
        mutations.push(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: if i % 2 == 0 {
                "app".into()
            } else {
                "other".into()
            },
            key: format!("coord-timeout:{i}").into_bytes(),
            value: vec![1u8; 8],
        });
    }
    let err = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: ReadSet::default(),
            write_intent: WriteIntent { mutations },
            base_seq,
        })
        .await
        .expect_err("coordinator timeout must reject epoch");
    assert!(matches!(err, AedbError::PartitionLockTimeout));
    drop(db);

    let recovered = recover_with_config(dir.path(), &config).expect("recover");
    assert_eq!(recovered.current_seq, 2);
    assert!(
        recovered
            .keyspace
            .kv_get("p", "app", b"coord-timeout:0")
            .is_none()
    );
    assert!(
        recovered
            .keyspace
            .kv_get("p", "other", b"coord-timeout:1")
            .is_none()
    );
}

#[tokio::test]
async fn crash_matrix_parallel_epoch_timeout_recovery_has_no_partial_writes() {
    let dir = tempdir().expect("temp dir");
    let config = timeout_config();
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    seed_project(&db).await;

    let base_seq = db.head_state().await.visible_head_seq;
    let mut mutations = Vec::new();
    for i in 0..25_000u64 {
        mutations.push(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("parallel-timeout:{i}").into_bytes(),
            value: vec![2u8; 16],
        });
    }
    let err = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: ReadSet::default(),
            write_intent: WriteIntent { mutations },
            base_seq,
        })
        .await
        .expect_err("parallel apply timeout must reject epoch");
    assert!(matches!(
        err,
        AedbError::EpochApplyTimeout | AedbError::ParallelApplyCancelled
    ));
    drop(db);

    let recovered = recover_with_config(dir.path(), &config).expect("recover");
    assert_eq!(recovered.current_seq, 1);
    assert!(
        recovered
            .keyspace
            .kv_get("p", "app", b"parallel-timeout:0")
            .is_none()
    );
}

#[tokio::test]
async fn crash_matrix_manifest_tamper_detected_with_prev_missing() {
    let dir = tempdir().expect("temp dir");
    let config = production_config();
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    seed_project(&db).await;
    db.checkpoint_now().await.expect("checkpoint");
    drop(db);

    fs::write(dir.path().join("manifest.json"), b"{tamper").expect("tamper primary");
    fs::write(dir.path().join("manifest.hmac"), "beef").expect("tamper hmac");
    let _ = fs::remove_file(dir.path().join("manifest.json.prev"));
    let _ = fs::remove_file(dir.path().join("manifest.hmac.prev"));

    let reopen = AedbInstance::open(config.clone(), dir.path());
    assert!(reopen.is_err());

    let manifest = load_manifest_signed(dir.path(), config.hmac_key());
    assert!(manifest.is_err());
}

#[tokio::test]
#[ignore = "long-running crash cycle test (A17a)"]
async fn crash_matrix_a17a_strict_restarts_fail_closed() {
    let dir = tempdir().expect("temp dir");
    let strict_config = production_config();
    let mut writer_config = strict_config.clone();
    writer_config.recovery_mode = RecoveryMode::Permissive;
    writer_config.hash_chain_required = false;
    let cycles = env_or_u64(1_000, "AEDB_A17_CYCLES");

    setup_a17_cycle_table(dir.path(), &writer_config).await;

    for cycle in 1..=cycles {
        a17_write_cycle(dir.path(), &writer_config, cycle).await;

        match AedbInstance::open(strict_config.clone(), dir.path()) {
            Ok(strict) => {
                strict.shutdown().await.expect("strict shutdown");
                panic!("strict open unexpectedly succeeded at cycle {cycle}");
            }
            Err(AedbError::Validation(_)) => {
                // Expected strict fail-closed behavior.
            }
            Err(e) => panic!("unexpected strict reopen error at cycle {cycle}: {e}"),
        }
    }
}

#[tokio::test]
#[ignore = "long-running crash cycle test (A17b)"]
async fn crash_matrix_a17b_thousand_crash_cycles_preserve_state() {
    let dir = tempdir().expect("temp dir");
    let strict_config = production_config();
    let mut writer_config = strict_config.clone();
    writer_config.recovery_mode = RecoveryMode::Permissive;
    writer_config.hash_chain_required = false;
    let cycles = env_or_u64(1_000, "AEDB_A17_CYCLES");

    setup_a17_cycle_table(dir.path(), &writer_config).await;

    for cycle in 1..=cycles {
        a17_write_cycle(dir.path(), &writer_config, cycle).await;

        match AedbInstance::open(strict_config.clone(), dir.path()) {
            Ok(strict) => {
                strict.shutdown().await.expect("strict shutdown");
            }
            Err(AedbError::Validation(_)) => {}
            Err(e) => panic!("unexpected strict reopen error at cycle {cycle}: {e}"),
        }
        assert_a17_cycle_row(dir.path(), &writer_config, cycle).await;
    }

    let final_db = AedbInstance::open(writer_config.clone(), dir.path()).expect("final open");
    let final_rows = final_db
        .query(
            "p",
            "app",
            Query::select(&["id"])
                .from("a17_cycles")
                .limit(usize::try_from(cycles).expect("cycles fit usize")),
        )
        .await
        .expect("final query");
    assert_eq!(
        final_rows.rows.len(),
        usize::try_from(cycles).expect("cycles fit usize"),
        "all cycle rows must exist after repeated crashes"
    );
    final_db.shutdown().await.expect("final shutdown");
    drop(final_db);
}
