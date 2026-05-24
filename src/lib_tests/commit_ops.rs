use super::{
    ActionCommitOutcome, ActionEnvelopeRequest, AedbConfig, AedbError, AedbInstance, CallerContext,
    ColumnDef, ColumnType, CommitFinality, ConsistencyMode, DdlOperation, DurabilityMode,
    ErrorResourceType, Expr, IdempotencyKey, KvU256MissingPolicy, Mutation, Permission, Query,
    ReadAssertion, RecoveryMode, Row, StorageMode, TransactionEnvelope, Value, WriteClass,
    WriteIntent, u256_be_test,
};
use crate::commit::tx::ReadSet;
use crate::commit::validation::{CompareOp, KvU256OverflowPolicy};
use crate::preflight::PreflightResult;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;

#[tokio::test]
async fn production_profile_requires_hmac() {
    let dir = tempdir().expect("temp");
    let invalid = AedbConfig {
        recovery_mode: RecoveryMode::Strict,
        durability_mode: DurabilityMode::Full,
        hash_chain_required: true,
        manifest_hmac_key: None,
        ..AedbConfig::default()
    };
    let err = AedbInstance::open_production(invalid, dir.path())
        .err()
        .expect("must reject missing hmac");
    assert!(matches!(err, AedbError::InvalidConfig { .. }));

    let valid = AedbConfig::production([7u8; 32]);
    AedbInstance::open_production(valid, dir.path()).expect("open production");
}

#[tokio::test]
async fn production_profile_rejects_batch_durability() {
    let dir = tempdir().expect("temp");
    let mut weak = AedbConfig::production([7u8; 32]);
    weak.durability_mode = DurabilityMode::Batch;
    let err = AedbInstance::open_production(weak, dir.path())
        .err()
        .expect("production profile must reject batch durability");
    assert!(matches!(err, AedbError::InvalidConfig { .. }));
}

#[test]
fn arcana_profile_rejects_short_hmac_key() {
    let weak = AedbConfig::default().with_hmac_key(vec![9u8; 16]);
    let err = crate::lib_helpers::validate_arcana_config(&weak)
        .expect_err("short hmac key must be rejected");
    assert!(matches!(err, AedbError::InvalidConfig { .. }));
}

#[test]
fn low_latency_profile_uses_batch_durability_with_strict_recovery() {
    let cfg = AedbConfig::low_latency([5u8; 32]);
    assert_eq!(cfg.durability_mode, DurabilityMode::Batch);
    assert_eq!(cfg.recovery_mode, RecoveryMode::Strict);
    assert!(cfg.hash_chain_required);
    assert!(cfg.batch_interval_ms > 0);
    assert!(cfg.batch_max_bytes > 0);
    assert!(cfg.manifest_hmac_key.is_some());
    assert_eq!(cfg.storage_mode, StorageMode::DiskBacked);
    assert_eq!(cfg.persistent_value_inline_threshold_bytes, 0);
}

#[tokio::test]
async fn envelope_read_bytes_budget_passes_for_tiny_envelope() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        max_read_bytes_per_envelope: 1024,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open(config, dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    db.commit_envelope(TransactionEnvelope {
        caller: None,
        idempotency_key: None,
        write_class: WriteClass::Standard,
        assertions: vec![ReadAssertion::KeyExists {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"absent".to_vec(),
            expected: false,
        }],
        read_set: ReadSet::default(),
        write_intent: WriteIntent {
            mutations: vec![Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            }],
        },
        base_seq: 0,
    })
    .await
    .expect("tiny envelope must fit budget");
}

#[tokio::test]
async fn envelope_read_bytes_budget_rejects_large_scan() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        max_scan_rows: 1_000_000,
        max_read_bytes_per_envelope: 512,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open(config, dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "items".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "blob".into(),
                col_type: ColumnType::Blob,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("create table");

    for i in 0..64i64 {
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "items".into(),
            primary_key: vec![Value::Integer(i)],
            row: Row::from_values(vec![Value::Integer(i), Value::Blob(vec![0xAB; 128])]),
        })
        .await
        .expect("seed row");
    }

    let err = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: vec![ReadAssertion::CountCompare {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "items".into(),
                filter: None,
                op: CompareOp::Gte,
                threshold: 0,
            }],
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"_".to_vec(),
                    value: b"_".to_vec(),
                }],
            },
            base_seq: 0,
        })
        .await
        .expect_err("aggregate read byte budget must reject large scan");
    match err {
        AedbError::Validation(msg) => {
            assert!(
                msg.contains("max_read_bytes_per_envelope"),
                "informative limit message expected, got: {msg}"
            );
        }
        other => panic!("expected Validation error, got: {other:?}"),
    }
}

#[tokio::test]
async fn commit_ddl_batch_is_atomic_and_reports_per_op_results() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("arcana").await.expect("project");

    let failed = db
        .commit_ddl_batch(vec![
            DdlOperation::CreateTable {
                owner_id: None,
                if_not_exists: false,
                project_id: "arcana".into(),
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
            },
            DdlOperation::CreateTable {
                owner_id: None,
                if_not_exists: false,
                project_id: "arcana".into(),
                scope_id: "missing".into(),
                table_name: "invalid".into(),
                columns: vec![ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                }],
                primary_key: vec!["id".into()],
            },
        ])
        .await
        .expect_err("invalid second ddl should fail entire batch");
    assert!(matches!(
        failed,
        AedbError::NotFound {
            resource_type: ErrorResourceType::Scope,
            ..
        }
    ));
    assert!(
        !db.table_exists("arcana", "app", "users")
            .await
            .expect("users table exists check")
    );

    let first = db
        .commit_ddl_batch(vec![DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: true,
            project_id: "arcana".into(),
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
        }])
        .await
        .expect("first batch");
    assert_eq!(first.results.len(), 1);
    assert!(first.results[0].applied);
    assert!(
        db.table_exists("arcana", "app", "users")
            .await
            .expect("table exists")
    );

    let second = db
        .commit_ddl_batch(vec![DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: true,
            project_id: "arcana".into(),
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
        }])
        .await
        .expect("second batch");
    assert_eq!(second.results.len(), 1);
    assert!(!second.results[0].applied);
}

#[tokio::test]
async fn commit_ddl_batch_supports_dependent_ddl_ordering() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    let batch = db
        .commit_ddl_batch(vec![
            DdlOperation::CreateProject {
                owner_id: None,
                project_id: "arcana".into(),
                if_not_exists: true,
            },
            DdlOperation::CreateScope {
                owner_id: None,
                project_id: "arcana".into(),
                scope_id: "ops".into(),
                if_not_exists: true,
            },
            DdlOperation::CreateTable {
                owner_id: None,
                if_not_exists: true,
                project_id: "arcana".into(),
                scope_id: "ops".into(),
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
            },
        ])
        .await
        .expect("dependent batch");

    assert_eq!(batch.results.len(), 3);
    assert!(batch.results.iter().all(|r| r.applied));
    assert!(
        db.project_exists("arcana")
            .await
            .expect("project exists after batch")
    );
    assert!(
        db.scope_exists("arcana", "ops")
            .await
            .expect("scope exists after batch")
    );
    assert!(
        db.table_exists("arcana", "ops", "users")
            .await
            .expect("table exists after batch")
    );
}

#[tokio::test]
async fn idempotency_prunes_by_commit_window() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(
        AedbConfig {
            idempotency_window_commits: 1,
            ..AedbConfig::default()
        },
        dir.path(),
    )
    .expect("open");
    db.create_project("p").await.expect("project");

    let first = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: Some(IdempotencyKey([1u8; 16])),
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"k".to_vec(),
                    value: b"v1".to_vec(),
                }],
            },
            base_seq: 0,
        })
        .await
        .expect("first commit");

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"other".to_vec(),
        value: b"v2".to_vec(),
    })
    .await
    .expect("advance seq");

    let retried = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: Some(IdempotencyKey([1u8; 16])),
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"k".to_vec(),
                    value: b"v3".to_vec(),
                }],
            },
            base_seq: 0,
        })
        .await
        .expect("retried commit");
    assert!(
        retried.commit_seq > first.commit_seq,
        "idempotency record should expire by sequence window"
    );
}

#[tokio::test]
async fn idempotency_prunes_by_time_window() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(
        AedbConfig {
            idempotency_window_commits: 100,
            idempotency_window_seconds: 1,
            ..AedbConfig::default()
        },
        dir.path(),
    )
    .expect("open");
    db.create_project("p").await.expect("project");

    let first = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: Some(IdempotencyKey([9u8; 16])),
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"time".to_vec(),
                    value: b"v1".to_vec(),
                }],
            },
            base_seq: 0,
        })
        .await
        .expect("first commit");

    tokio::time::sleep(Duration::from_millis(1_100)).await;

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"tick".to_vec(),
        value: b"v2".to_vec(),
    })
    .await
    .expect("advance and prune");

    let retried = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: Some(IdempotencyKey([9u8; 16])),
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"time".to_vec(),
                    value: b"v3".to_vec(),
                }],
            },
            base_seq: 0,
        })
        .await
        .expect("retried commit");

    assert!(
        retried.commit_seq > first.commit_seq,
        "idempotency record should expire by time window"
    );
}

#[test]
fn open_rejects_invalid_config() {
    let dir = tempdir().expect("temp");
    let bad = AedbConfig {
        durability_mode: DurabilityMode::Batch,
        batch_interval_ms: 0,
        ..AedbConfig::default()
    };
    let err = AedbInstance::open(bad, dir.path())
        .err()
        .expect("invalid config");
    assert!(matches!(err, crate::error::AedbError::InvalidConfig { .. }));

    let too_large_txn = AedbConfig {
        max_transaction_bytes: crate::wal::frame::MAX_FRAME_BODY_BYTES + 1,
        ..AedbConfig::default()
    };
    let err = AedbInstance::open(too_large_txn, dir.path())
        .err()
        .expect("oversized transaction bound");
    assert!(matches!(err, crate::error::AedbError::InvalidConfig { .. }));

    let deadlock_unsafe = AedbConfig {
        recovery_mode: RecoveryMode::Strict,
        coordinator_locking_enabled: false,
        ..AedbConfig::default()
    };
    let err = AedbInstance::open(deadlock_unsafe, dir.path())
        .err()
        .expect("strict mode must require coordinator locking");
    assert!(matches!(err, crate::error::AedbError::InvalidConfig { .. }));
}

#[tokio::test]
async fn metrics_surface_reflects_commits() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    let m = db.metrics();
    assert!(m.commits_total >= 1);
    let op = db.operational_metrics().await;
    assert!(op.commits_total >= 1);
    assert!(op.read_set_conflicts <= op.conflict_rejections);
    assert!(op.queue_depth >= op.inflight_commits);
    assert!(op.snapshot_age_micros <= u64::MAX / 2);
}

#[tokio::test]
async fn commit_with_visible_finality_can_return_before_durable_head_in_batch_mode() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        durability_mode: DurabilityMode::Batch,
        batch_interval_ms: 60_000,
        batch_max_bytes: usize::MAX,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open(config, dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let result = db
        .commit_with_finality(
            Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"fast-visible".to_vec(),
                value: b"v".to_vec(),
            },
            CommitFinality::Visible,
        )
        .await
        .expect("commit");

    assert!(
        result.durable_head_seq < result.commit_seq,
        "visible finality should not require durable head in batch mode"
    );
}

#[tokio::test]
async fn commit_with_durable_finality_waits_until_durable_head_catches_up() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        durability_mode: DurabilityMode::Batch,
        batch_interval_ms: 60_000,
        batch_max_bytes: usize::MAX,
        ..AedbConfig::default()
    };
    let db = Arc::new(AedbInstance::open(config, dir.path()).expect("open"));
    db.create_project("p").await.expect("project");

    let fsync_db = Arc::clone(&db);
    let fsync_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        fsync_db.force_fsync().await.expect("force fsync");
    });

    let started = Instant::now();
    let result = db
        .commit_with_finality(
            Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"fast-durable".to_vec(),
                value: b"v".to_vec(),
            },
            CommitFinality::Durable,
        )
        .await
        .expect("commit");
    fsync_task.await.expect("join fsync");

    assert!(
        started.elapsed() >= Duration::from_millis(15),
        "durable finality should wait for WAL durability in batch mode"
    );
    assert!(
        result.durable_head_seq >= result.commit_seq,
        "durable finality must report durable head at or beyond commit sequence"
    );
}

#[tokio::test]
#[ignore = "long-running finality latency profile"]
async fn finality_profile_visible_vs_durable_low_latency_mode() {
    async fn run_profile(
        config: AedbConfig,
        finality: CommitFinality,
        ops: usize,
    ) -> (u64, u64, u64, crate::OperationalMetrics) {
        let dir = tempdir().expect("temp");
        let db = AedbInstance::open(config, dir.path()).expect("open");
        db.create_project("p").await.expect("project");
        let started = Instant::now();
        let mut lat_sum = 0u128;
        let mut lat_max = 0u64;
        for i in 0..ops {
            let op_started = Instant::now();
            db.commit_with_finality(
                Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: format!("finality:{finality:?}:{i}").into_bytes(),
                    value: i.to_be_bytes().to_vec(),
                },
                finality,
            )
            .await
            .expect("commit with finality");
            let us = op_started.elapsed().as_micros() as u64;
            lat_sum = lat_sum.saturating_add(us as u128);
            lat_max = lat_max.max(us);
        }
        db.force_fsync().await.expect("flush");
        let elapsed = started.elapsed().as_secs_f64().max(0.001);
        let tps = (ops as f64 / elapsed) as u64;
        let avg_us = (lat_sum / ops.max(1) as u128) as u64;
        let op = db.operational_metrics().await;
        (tps, avg_us, lat_max, op)
    }

    let ops = std::env::var("AEDB_FINALITY_PROFILE_OPS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(600)
        .max(200);

    let mut low_latency_no_coalesce = AedbConfig::low_latency([1u8; 32]);
    low_latency_no_coalesce.durable_ack_coalescing_enabled = false;
    low_latency_no_coalesce.durable_ack_coalesce_window_us = 0;
    let low_latency_coalesce = AedbConfig::low_latency([1u8; 32]);

    let (visible_tps, visible_avg_us, visible_max_us, visible_op) = run_profile(
        low_latency_no_coalesce.clone(),
        CommitFinality::Visible,
        ops,
    )
    .await;
    let (durable_base_tps, durable_base_avg_us, durable_base_max_us, durable_base_op) =
        run_profile(low_latency_no_coalesce, CommitFinality::Durable, ops).await;
    let (durable_tps, durable_avg_us, durable_max_us, durable_op) =
        run_profile(low_latency_coalesce, CommitFinality::Durable, ops).await;

    eprintln!(
        "finality_profile: ops={} visible_tps={} durable_base_tps={} durable_coalesced_tps={} visible_avg_us={} durable_base_avg_us={} durable_coalesced_avg_us={} visible_max_us={} durable_base_max_us={} durable_coalesced_max_us={} visible_durable_wait_ops={} durable_base_wait_ops={} durable_coalesced_wait_ops={} visible_avg_durable_wait_us={} durable_base_avg_durable_wait_us={} durable_coalesced_avg_durable_wait_us={} visible_wal_sync_ops={} durable_base_wal_sync_ops={} durable_coalesced_wal_sync_ops={} visible_avg_wal_sync_us={} durable_base_avg_wal_sync_us={} durable_coalesced_avg_wal_sync_us={} visible_avg_wal_append_us={} durable_base_avg_wal_append_us={} durable_coalesced_avg_wal_append_us={}",
        ops,
        visible_tps,
        durable_base_tps,
        durable_tps,
        visible_avg_us,
        durable_base_avg_us,
        durable_avg_us,
        visible_max_us,
        durable_base_max_us,
        durable_max_us,
        visible_op.durable_wait_ops,
        durable_base_op.durable_wait_ops,
        durable_op.durable_wait_ops,
        visible_op.avg_durable_wait_micros,
        durable_base_op.avg_durable_wait_micros,
        durable_op.avg_durable_wait_micros,
        visible_op.wal_sync_ops,
        durable_base_op.wal_sync_ops,
        durable_op.wal_sync_ops,
        visible_op.avg_wal_sync_micros,
        durable_base_op.avg_wal_sync_micros,
        durable_op.avg_wal_sync_micros,
        visible_op.avg_wal_append_micros,
        durable_base_op.avg_wal_append_micros,
        durable_op.avg_wal_append_micros
    );

    assert_eq!(
        visible_op.queue_full_rejections, 0,
        "visible finality profile should not saturate queue"
    );
    assert_eq!(
        durable_base_op.queue_full_rejections, 0,
        "durable baseline profile should not saturate queue"
    );
    assert_eq!(
        durable_op.queue_full_rejections, 0,
        "durable coalesced profile should not saturate queue"
    );
    assert_eq!(
        visible_op.timeout_rejections, 0,
        "visible finality profile should not timeout"
    );
    assert_eq!(
        durable_base_op.timeout_rejections, 0,
        "durable baseline profile should not timeout"
    );
    assert_eq!(
        durable_op.timeout_rejections, 0,
        "durable coalesced profile should not timeout"
    );
    assert_eq!(
        visible_op.durable_wait_ops, 0,
        "visible finality profile should not accumulate durable wait operations"
    );
    assert!(
        durable_op.durable_wait_ops > 0,
        "durable finality profile should accumulate durable wait operations"
    );
    assert!(
        durable_tps >= durable_base_tps.saturating_div(2),
        "coalesced durable finality regressed severely: base={durable_base_tps} coalesced={durable_tps}"
    );
    assert!(
        durable_tps <= visible_tps.saturating_mul(2),
        "durable finality profile produced implausible TPS vs visible: visible={visible_tps} durable={durable_tps}"
    );
}

#[tokio::test]
async fn commit_success_is_observable_at_its_commit_seq() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let result = db
        .commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"inclusion-proof".to_vec(),
            value: b"ok".to_vec(),
        })
        .await
        .expect("commit");

    let at_seq = db
        .kv_get_no_auth(
            "p",
            "app",
            b"inclusion-proof",
            ConsistencyMode::AtSeq(result.commit_seq),
        )
        .await
        .expect("kv_get at seq")
        .expect("value present at commit seq");
    assert_eq!(at_seq.value, b"ok".to_vec());
}

#[tokio::test]
async fn failed_multi_mutation_envelope_is_atomic_and_has_no_partial_effects() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let before = db.head_state().await.visible_head_seq;
    let err = db
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
                        key: b"atomic-a".to_vec(),
                        value: b"1".to_vec(),
                    },
                    Mutation::KvDecU256 {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        key: b"missing-counter".to_vec(),
                        amount_be: {
                            let mut out = [0u8; 32];
                            out[31] = 1;
                            out
                        },
                    },
                ],
            },
            base_seq: before,
        })
        .await
        .expect_err("envelope should fail");
    assert!(
        matches!(
            err,
            AedbError::Underflow | AedbError::Validation(_) | AedbError::Conflict(_)
        ),
        "expected semantic failure, got: {err:?}"
    );

    let after = db.head_state().await.visible_head_seq;
    assert_eq!(after, before, "failed envelope must not advance head");
    let leaked = db
        .kv_get_no_auth("p", "app", b"atomic-a", ConsistencyMode::AtLatest)
        .await
        .expect("kv_get")
        .is_some();
    assert!(
        !leaked,
        "failed envelope must not partially apply mutations"
    );
}

#[tokio::test]
async fn action_envelope_applied_once() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let result = db
        .commit_action_envelope(ActionEnvelopeRequest {
            caller: None,
            idempotency_key: IdempotencyKey([1u8; 16]),
            write_class: WriteClass::Standard,
            base_seq: 0,
            assertions: Vec::new(),
            mutations: vec![
                Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"action:flag".to_vec(),
                    value: b"ok".to_vec(),
                },
                Mutation::KvAddU256Ex {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"action:counter".to_vec(),
                    amount_be: u256_be_test(3),
                    on_missing: KvU256MissingPolicy::TreatAsZero,
                    on_overflow: KvU256OverflowPolicy::Reject,
                },
            ],
        })
        .await
        .expect("action commit");

    assert_eq!(result.outcome, ActionCommitOutcome::Applied);
    let flag = db
        .kv_get_no_auth("p", "app", b"action:flag", ConsistencyMode::AtLatest)
        .await
        .expect("flag read")
        .expect("flag exists");
    assert_eq!(flag.value, b"ok".to_vec());
}

#[tokio::test]
async fn action_envelope_duplicate_returns_duplicate_outcome() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let req = ActionEnvelopeRequest {
        caller: None,
        idempotency_key: IdempotencyKey([2u8; 16]),
        write_class: WriteClass::Standard,
        base_seq: 0,
        assertions: Vec::new(),
        mutations: vec![Mutation::KvAddU256Ex {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"action:dup-counter".to_vec(),
            amount_be: u256_be_test(1),
            on_missing: KvU256MissingPolicy::TreatAsZero,
            on_overflow: KvU256OverflowPolicy::Reject,
        }],
    };

    let first = db
        .commit_action_envelope(req.clone())
        .await
        .expect("first action");
    let second = db
        .commit_action_envelope(req)
        .await
        .expect("duplicate action");

    assert_eq!(first.outcome, ActionCommitOutcome::Applied);
    assert_eq!(second.outcome, ActionCommitOutcome::Duplicate);
    assert_eq!(second.commit_seq, first.commit_seq);

    let counter = db
        .kv_get_no_auth("p", "app", b"action:dup-counter", ConsistencyMode::AtLatest)
        .await
        .expect("counter read")
        .expect("counter exists");
    assert_eq!(
        primitive_types::U256::from_big_endian(&counter.value),
        primitive_types::U256::one(),
    );
}

#[tokio::test]
async fn single_envelope_atomicity() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"guard".to_vec(),
        value: b"1".to_vec(),
    })
    .await
    .expect("guard seed");

    let before = db.head_state().await.visible_head_seq;
    let err = db
        .commit_action_envelope(ActionEnvelopeRequest {
            caller: None,
            idempotency_key: IdempotencyKey([3u8; 16]),
            write_class: WriteClass::Standard,
            base_seq: before,
            assertions: vec![ReadAssertion::KeyExists {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"guard".to_vec(),
                expected: false,
            }],
            mutations: vec![
                Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"atomic:x".to_vec(),
                    value: b"x".to_vec(),
                },
                Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"atomic:y".to_vec(),
                    value: b"y".to_vec(),
                },
            ],
        })
        .await
        .expect_err("assertion failure");
    assert!(matches!(err, AedbError::AssertionFailed { .. }));
    assert!(
        db.head_state().await.visible_head_seq >= before,
        "assertion failure may emit an audit commit, but data mutations must not apply"
    );
    assert!(
        db.kv_get_no_auth("p", "app", b"atomic:x", ConsistencyMode::AtLatest)
            .await
            .expect("read x")
            .is_none()
    );
    assert!(
        db.kv_get_no_auth("p", "app", b"atomic:y", ConsistencyMode::AtLatest)
            .await
            .expect("read y")
            .is_none()
    );
}

#[tokio::test]
async fn retry_idempotency_is_exactly_once_under_commit_pressure() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        commit_timeout_ms: 1,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open(config, dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let key = IdempotencyKey([91u8; 16]);
    let envelope = TransactionEnvelope {
        caller: None,
        idempotency_key: Some(key),
        write_class: WriteClass::Standard,
        assertions: Vec::new(),
        read_set: Default::default(),
        write_intent: WriteIntent {
            mutations: vec![
                Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"__slow_parallel_worker__".to_vec(),
                    value: b"slow".to_vec(),
                },
                Mutation::KvIncU256 {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"timeout-idem-counter".to_vec(),
                    amount_be: {
                        let mut out = [0u8; 32];
                        out[31] = 1;
                        out
                    },
                },
            ],
        },
        base_seq: 0,
    };

    let noisy_db = Arc::new(db);
    let mut noise_tasks = Vec::new();
    for worker in 0..8 {
        let db_clone = Arc::clone(&noisy_db);
        noise_tasks.push(tokio::spawn(async move {
            for i in 0..200usize {
                let _ = db_clone
                    .commit(Mutation::KvSet {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        key: format!("noise:{worker}:{i}").into_bytes(),
                        value: b"n".to_vec(),
                    })
                    .await;
            }
        }));
    }

    let mut saw_timeout = false;
    let second = loop {
        match noisy_db.commit_envelope(envelope.clone()).await {
            Ok(result) => break result,
            Err(AedbError::Timeout) => {
                saw_timeout = true;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err(other) => panic!("unexpected error during retry loop: {other:?}"),
        }
    };
    for t in noise_tasks {
        t.await.expect("join noise worker");
    }
    noisy_db
        .wait_for_durable(second.commit_seq)
        .await
        .expect("durable ack");

    let third = noisy_db
        .commit_envelope(envelope)
        .await
        .expect("repeat idempotent retry");
    assert_eq!(
        third.commit_seq, second.commit_seq,
        "all retries must resolve to one commit sequence"
    );

    let counter = noisy_db
        .kv_get_no_auth(
            "p",
            "app",
            b"timeout-idem-counter",
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("kv counter")
        .expect("counter exists");
    assert_eq!(
        primitive_types::U256::from_big_endian(&counter.value),
        primitive_types::U256::one(),
        "counter must be applied once"
    );

    let op = noisy_db.operational_metrics().await;
    if saw_timeout {
        assert!(
            op.timeout_rejections >= 1,
            "timeout path should be observable in operational metrics"
        );
    }
}

#[tokio::test]
async fn multi_update_transaction_envelope_updates_table_and_kv() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        durability_mode: DurabilityMode::Batch,
        batch_interval_ms: 60_000,
        batch_max_bytes: usize::MAX,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open(config, dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "balance".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("accounts table");

    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Integer(100)],
        },
    })
    .await
    .expect("seed account 1");
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        primary_key: vec![Value::Integer(2)],
        row: Row {
            values: vec![Value::Integer(2), Value::Integer(80)],
        },
    })
    .await
    .expect("seed account 2");

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "reader".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: None,
        },
    }))
    .await
    .expect("grant kv read");
    let caller = CallerContext::new("reader");

    let result = db
        .commit_envelope_with_finality(
            TransactionEnvelope {
                caller: None,
                idempotency_key: None,
                write_class: WriteClass::Standard,
                assertions: Vec::new(),
                read_set: Default::default(),
                write_intent: WriteIntent {
                    mutations: vec![
                        Mutation::Upsert {
                            project_id: "p".into(),
                            scope_id: "app".into(),
                            table_name: "accounts".into(),
                            primary_key: vec![Value::Integer(1)],
                            row: Row {
                                values: vec![Value::Integer(1), Value::Integer(90)],
                            },
                        },
                        Mutation::Upsert {
                            project_id: "p".into(),
                            scope_id: "app".into(),
                            table_name: "accounts".into(),
                            primary_key: vec![Value::Integer(2)],
                            row: Row {
                                values: vec![Value::Integer(2), Value::Integer(90)],
                            },
                        },
                        Mutation::KvSet {
                            project_id: "p".into(),
                            scope_id: "app".into(),
                            key: b"tx:last".to_vec(),
                            value: b"t1".to_vec(),
                        },
                        Mutation::KvSet {
                            project_id: "p".into(),
                            scope_id: "app".into(),
                            key: b"ledger:1->2".to_vec(),
                            value: b"10".to_vec(),
                        },
                    ],
                },
                base_seq: db.head_state().await.visible_head_seq,
            },
            CommitFinality::Visible,
        )
        .await
        .expect("multi-update tx");

    assert!(
        result.durable_head_seq < result.commit_seq,
        "visible finality should return before durable head in batch mode"
    );

    let acct1 = db
        .query(
            "p",
            "app",
            Query::select(&["balance"])
                .from("accounts")
                .where_(Expr::Eq("id".into(), Value::Integer(1)))
                .limit(1),
        )
        .await
        .expect("query account1");
    let acct2 = db
        .query(
            "p",
            "app",
            Query::select(&["balance"])
                .from("accounts")
                .where_(Expr::Eq("id".into(), Value::Integer(2)))
                .limit(1),
        )
        .await
        .expect("query account2");
    assert_eq!(acct1.rows.len(), 1);
    assert_eq!(acct2.rows.len(), 1);
    assert_eq!(acct1.rows[0].values[0], Value::Integer(90));
    assert_eq!(acct2.rows[0].values[0], Value::Integer(90));

    let tx_last = db
        .kv_get("p", "app", b"tx:last", ConsistencyMode::AtLatest, &caller)
        .await
        .expect("tx:last");
    let ledger = db
        .kv_get(
            "p",
            "app",
            b"ledger:1->2",
            ConsistencyMode::AtLatest,
            &caller,
        )
        .await
        .expect("ledger entry");
    assert_eq!(
        tx_last.as_ref().map(|v| v.value.clone()),
        Some(b"t1".to_vec())
    );
    assert_eq!(
        ledger.as_ref().map(|v| v.value.clone()),
        Some(b"10".to_vec())
    );
}

#[tokio::test]
#[ignore = "manual profiling: end-to-end pipeline breakdown (commit/checkpoint/recovery)"]
async fn profile_end_to_end_pipeline_breakdown() {
    fn percentile(sorted: &[u128], p: f64) -> u128 {
        if sorted.is_empty() {
            return 0;
        }
        let percentile_index = ((sorted.len() as f64 - 1.0) * p).round() as usize;
        sorted[percentile_index.min(sorted.len() - 1)]
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
    let db = Arc::new(AedbInstance::open(config.clone(), dir.path()).expect("open"));
    db.create_project("p").await.expect("project");

    for i in 0..20_000usize {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("seed:{i:06}").into_bytes(),
            value: vec![b's'; 256],
        })
        .await
        .expect("seed");
    }

    let workers = 8usize;
    let commits_per_worker = 500usize;
    let commit_started = Instant::now();
    let mut tasks = Vec::with_capacity(workers);
    for worker in 0..workers {
        let db = Arc::clone(&db);
        tasks.push(tokio::spawn(async move {
            let mut latencies_us = Vec::with_capacity(commits_per_worker);
            for i in 0..commits_per_worker {
                let started = Instant::now();
                db.commit(Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: format!("profile:commit:{worker:02}:{i:06}").into_bytes(),
                    value: vec![b'c'; 256],
                })
                .await
                .expect("commit");
                latencies_us.push(started.elapsed().as_micros());
            }
            latencies_us
        }));
    }
    let mut all_commit_latencies = Vec::with_capacity(workers * commits_per_worker);
    for task in tasks {
        let mut worker_latencies = task.await.expect("worker join");
        all_commit_latencies.append(&mut worker_latencies);
    }
    let commit_elapsed = commit_started.elapsed();
    all_commit_latencies.sort_unstable();
    let commit_tps = (workers * commits_per_worker) as f64 / commit_elapsed.as_secs_f64();
    let commit_p50 = percentile(&all_commit_latencies, 0.50);
    let commit_p99 = percentile(&all_commit_latencies, 0.99);
    let op_after_commits = db.operational_metrics().await;

    let checkpoint_total_started = Instant::now();
    let checkpoint_lock_started = Instant::now();
    let _checkpoint_guard = db.checkpoint_lock.lock().await;
    let checkpoint_lock_wait = checkpoint_lock_started.elapsed();

    let snapshot_started = Instant::now();
    let checkpoint_seq = db.executor.durable_head_seq_now();
    let lease = db
        .acquire_snapshot(ConsistencyMode::AtSeq(checkpoint_seq))
        .await
        .expect("checkpoint snapshot");
    let snapshot_elapsed = snapshot_started.elapsed();

    let idempotency_started = Instant::now();
    let mut idempotency = db.executor.idempotency_snapshot().await;
    idempotency.retain(|_, record| record.commit_seq <= checkpoint_seq);
    let idempotency_elapsed = idempotency_started.elapsed();

    let write_checkpoint_started = Instant::now();
    let checkpoint = crate::checkpoint::writer::write_checkpoint_with_key(
        lease.view.keyspace.as_ref(),
        lease.view.catalog.as_ref(),
        checkpoint_seq,
        &db.dir,
        db._config.checkpoint_key(),
        db._config.checkpoint_key_id.clone(),
        idempotency,
        db._config.checkpoint_compression_level,
    )
    .expect("write checkpoint");
    let write_checkpoint_elapsed = write_checkpoint_started.elapsed();

    let segments_started = Instant::now();
    let segments = crate::lib_helpers::read_segments_for_checkpoint(&db.dir, checkpoint_seq)
        .expect("read segments for checkpoint");
    let active_segment_seq = segments
        .last()
        .map(|segment| segment.segment_seq)
        .unwrap_or(checkpoint_seq.saturating_add(1));
    let segments_elapsed = segments_started.elapsed();

    let manifest_started = Instant::now();
    let manifest = crate::manifest::schema::Manifest {
        durable_seq: checkpoint_seq,
        visible_seq: checkpoint_seq,
        active_segment_seq,
        checkpoints: vec![checkpoint.clone()],
        segments: segments.clone(),
    };
    crate::manifest::atomic::write_manifest_atomic_signed(
        &manifest,
        &db.dir,
        db._config.hmac_key(),
    )
    .expect("write manifest");
    let manifest_elapsed = manifest_started.elapsed();
    let checkpoint_total_elapsed = checkpoint_total_started.elapsed();
    drop(_checkpoint_guard);

    let checkpoint_bytes = std::fs::metadata(db.dir.join(&checkpoint.filename))
        .expect("checkpoint stat")
        .len();

    drop(db);
    let reopen_started = Instant::now();
    let reopened = AedbInstance::open(config, dir.path()).expect("reopen");
    let reopen_elapsed = reopen_started.elapsed();
    let reopen_metrics = reopened.operational_metrics().await;

    eprintln!(
        "pipeline_profile commit_phase: workers={} commits_per_worker={} commits={} elapsed_ms={} tps={:.2} p50_us={} p99_us={} | prestage_validate_ops={} avg_prestage_validate_us={} epoch_process_ops={} avg_epoch_process_us={} avg_wal_append_us={} avg_wal_sync_us={} avg_coordinator_apply_us={}",
        workers,
        commits_per_worker,
        workers * commits_per_worker,
        commit_elapsed.as_millis(),
        commit_tps,
        commit_p50,
        commit_p99,
        op_after_commits.prestage_validate_ops,
        op_after_commits.avg_prestage_validate_micros,
        op_after_commits.epoch_process_ops,
        op_after_commits.avg_epoch_process_micros,
        op_after_commits.avg_wal_append_micros,
        op_after_commits.avg_wal_sync_micros,
        op_after_commits.avg_coordinator_apply_micros
    );
    eprintln!(
        "pipeline_profile checkpoint_phase: seq={} total_ms={} lock_wait_ms={} snapshot_ms={} idempotency_ms={} write_checkpoint_ms={} segment_scan_ms={} manifest_ms={} checkpoint_bytes={} retained_segments={}",
        checkpoint_seq,
        checkpoint_total_elapsed.as_millis(),
        checkpoint_lock_wait.as_millis(),
        snapshot_elapsed.as_millis(),
        idempotency_elapsed.as_millis(),
        write_checkpoint_elapsed.as_millis(),
        segments_elapsed.as_millis(),
        manifest_elapsed.as_millis(),
        checkpoint_bytes,
        segments.len()
    );
    eprintln!(
        "pipeline_profile recovery_phase: reopen_ms={} startup_recovery_micros={} startup_recovered_seq={} durable_head_seq={} visible_head_seq={}",
        reopen_elapsed.as_millis(),
        reopen_metrics.startup_recovery_micros,
        reopen_metrics.startup_recovered_seq,
        reopen_metrics.durable_head_seq,
        reopen_metrics.visible_head_seq
    );
}

#[tokio::test]
async fn preflight_uses_instance_config_limits() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        max_kv_value_bytes: 4,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open(config, dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let result = db
        .preflight(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"k".to_vec(),
            value: b"too-large".to_vec(),
        })
        .await;
    assert!(
        matches!(result, PreflightResult::Err { reason } if reason.contains("value too large"))
    );
}

#[tokio::test]
async fn subscribe_commits_delivers_delta_after_commit() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    db.create_project("p").await.expect("project");
    // Subscribe AFTER setup so we observe only the deltas under test.
    let mut rx = db.subscribe_commits();

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"hello".to_vec(),
        value: b"world".to_vec(),
    })
    .await
    .expect("commit");

    // Drain until we find our KvSet — internal subsystems may also produce
    // bookkeeping deltas, but ours must arrive within the timeout window.
    let mut saw_kv_set = false;
    let mut last_seq = 0u64;
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
            Ok(Ok(delta)) => {
                last_seq = last_seq.max(delta.seq);
                if delta
                    .mutations
                    .iter()
                    .any(|m| matches!(m, Mutation::KvSet { key, .. } if key == b"hello"))
                {
                    saw_kv_set = true;
                    break;
                }
            }
            Ok(Err(_)) => break,
            Err(_) => break,
        }
    }
    assert!(last_seq > 0, "broadcast delivered at least one delta");
    assert!(
        saw_kv_set,
        "broadcast delta must include the committed KvSet mutation"
    );

    db.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn subscribe_commits_lagged_subscriber_can_resume() {
    let config = AedbConfig {
        commit_broadcast_capacity: 2,
        ..AedbConfig::default()
    };
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(config, dir.path()).expect("open");

    let mut rx = db.subscribe_commits();
    db.create_project("p").await.expect("project");

    for i in 0..8u8 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: vec![i],
            value: vec![i],
        })
        .await
        .expect("commit");
    }

    let mut saw_lagged = false;
    let mut delivered = 0usize;
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
            Ok(Ok(_delta)) => {
                delivered += 1;
                if saw_lagged && delivered >= 2 {
                    break;
                }
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => {
                saw_lagged = true;
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => break,
            Err(_) => break,
        }
    }
    assert!(
        saw_lagged,
        "slow subscriber must observe RecvError::Lagged with capacity=2 + burst"
    );

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: vec![99],
        value: vec![99],
    })
    .await
    .expect("post-burst commit");

    let post = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("post-burst delivery within timeout")
        .expect("post-burst delta");
    let resumed = post.mutations.iter().any(|m| {
        matches!(
            m,
            Mutation::KvSet { key, .. } if key == &[99u8]
        )
    });
    assert!(resumed, "subscriber must resume after Lagged error");

    db.shutdown().await.expect("shutdown");
}

#[test]
fn mutation_write_keys_table_row_variants() {
    use crate::commit::tx::WriteKey;

    let row = Row {
        values: vec![Value::Integer(7), Value::Text("a".into())],
    };
    let pk = vec![Value::Integer(7)];

    let insert = Mutation::Insert {
        project_id: "p".into(),
        scope_id: "s".into(),
        table_name: "t".into(),
        primary_key: pk.clone(),
        row: row.clone(),
    };
    let keys = insert.write_keys();
    assert_eq!(keys.len(), 1);
    assert!(matches!(
        &keys[0],
        WriteKey::TableRow { project_id, scope_id, table_name, primary_key }
            if project_id == "p" && scope_id == "s" && table_name == "t" && primary_key == &pk
    ));

    let delete = Mutation::Delete {
        project_id: "p".into(),
        scope_id: "s".into(),
        table_name: "t".into(),
        primary_key: pk.clone(),
    };
    let keys = delete.write_keys();
    assert!(matches!(&keys[0], WriteKey::TableRow { primary_key, .. } if primary_key == &pk));

    let batch = Mutation::InsertBatch {
        project_id: "p".into(),
        scope_id: "s".into(),
        table_name: "t".into(),
        rows: vec![row.clone()],
    };
    let keys = batch.write_keys();
    assert!(matches!(
        &keys[0],
        WriteKey::TableRange { table_name, .. } if table_name == "t"
    ));
}

#[test]
fn read_set_intersects_positive_and_negative_cases() {
    use crate::commit::tx::{
        ReadBound, ReadKey, ReadRange, ReadRangeEntry, ReadSet, ReadSetEntry, WriteKey,
    };

    let mut rs = ReadSet::default();
    rs.points.push(ReadSetEntry {
        key: ReadKey::TableRow {
            project_id: "p".into(),
            scope_id: "s".into(),
            table_name: "t".into(),
            primary_key: vec![Value::Integer(1)],
        },
        version_at_read: 0,
    });
    rs.ranges.push(ReadRangeEntry {
        range: ReadRange::KvRange {
            project_id: "p".into(),
            scope_id: "s".into(),
            start: ReadBound::Included(b"a".to_vec()),
            end: ReadBound::Excluded(b"c".to_vec()),
        },
        max_version_at_read: 0,
        structural_version_at_read: 0,
    });

    // positive: same TableRow
    assert!(rs.intersects(&[WriteKey::TableRow {
        project_id: "p".into(),
        scope_id: "s".into(),
        table_name: "t".into(),
        primary_key: vec![Value::Integer(1)],
    }]));
    // positive: KvRange covers KvKey "b"
    assert!(rs.intersects(&[WriteKey::KvKey {
        project_id: "p".into(),
        scope_id: "s".into(),
        key: b"b".to_vec(),
    }]));
    // positive: ScopeAll matches any read in the scope
    assert!(rs.intersects(&[WriteKey::ScopeAll {
        project_id: "p".into(),
        scope_id: "s".into(),
    }]));
    // negative: disjoint scope
    assert!(!rs.intersects(&[WriteKey::TableRow {
        project_id: "p".into(),
        scope_id: "other".into(),
        table_name: "t".into(),
        primary_key: vec![Value::Integer(1)],
    }]));
    // negative: disjoint table
    assert!(!rs.intersects(&[WriteKey::TableRow {
        project_id: "p".into(),
        scope_id: "s".into(),
        table_name: "other".into(),
        primary_key: vec![Value::Integer(1)],
    }]));
    // negative: KvKey outside range ("c" is excluded end)
    assert!(!rs.intersects(&[WriteKey::KvKey {
        project_id: "p".into(),
        scope_id: "s".into(),
        key: b"c".to_vec(),
    }]));
    // negative: empty write set
    assert!(!rs.intersects(&[]));
    // negative: mismatching kinds (TableRow read vs KvKey write at same proj/scope)
    let rs_table_only = ReadSet {
        points: vec![ReadSetEntry {
            key: ReadKey::TableRow {
                project_id: "p".into(),
                scope_id: "s".into(),
                table_name: "t".into(),
                primary_key: vec![Value::Integer(1)],
            },
            version_at_read: 0,
        }],
        ranges: Vec::new(),
    };
    assert!(!rs_table_only.intersects(&[WriteKey::KvKey {
        project_id: "p".into(),
        scope_id: "s".into(),
        key: b"k".to_vec(),
    }]));
}
