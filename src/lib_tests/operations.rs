use super::{
    AedbConfig, AedbError, AedbErrorCode, AedbInstance, CallerContext, ColumnDef, ColumnType,
    ConsistencyMode, DdlOperation, DurabilityMode, Expr, IdempotencyKey, Mutation, Permission,
    Query, QueryError, QueryOptions, RECOVERY_CACHE_TTL, RecoveryCache, RecoveryMode, Row,
    TransactionEnvelope, Value, WriteClass, WriteIntent, create_table, mk_recovery_view,
};
use std::fs;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;

#[test]
fn recovery_cache_refresh_keeps_recent_entry() {
    let mut cache = RecoveryCache::default();
    for seq in 1..=16 {
        cache.put(seq, mk_recovery_view(seq));
    }
    assert!(
        cache.get(1).is_some(),
        "first entry should exist before refresh"
    );

    cache.put(17, mk_recovery_view(17));

    assert!(
        cache.get(1).is_some(),
        "refreshing an entry must protect it from immediate eviction"
    );
    assert!(
        cache.get(2).is_none(),
        "oldest non-refreshed entry should be evicted first"
    );
}

#[test]
fn recovery_cache_prunes_expired_entries() {
    let mut cache = RecoveryCache::default();
    cache.put(7, mk_recovery_view(7));
    {
        let entry = cache.entries.get_mut(&7).expect("entry");
        entry.created = Instant::now() - RECOVERY_CACHE_TTL - Duration::from_secs(1);
    }
    cache.prune_expired();
    assert!(cache.get(7).is_none(), "expired entry must be removed");
}

#[tokio::test]
async fn existence_and_introspection_apis_report_catalog_state() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");

    db.create_project("p").await.expect("project");
    db.create_scope("p", "s1").await.expect("scope");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "p".into(),
        scope_id: "s1".into(),
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
    db.commit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: "p".into(),
        scope_id: "s1".into(),
        table_name: "users".into(),
        index_name: "idx_users_name".into(),
        if_not_exists: false,
        columns: vec!["name".into()],
        index_type: crate::catalog::schema::IndexType::BTree,
        partial_filter: None,
    }))
    .await
    .expect("index");

    assert!(db.project_exists("p").await.expect("project_exists"));
    assert!(db.scope_exists("p", "s1").await.expect("scope_exists"));
    assert!(
        db.table_exists("p", "s1", "users")
            .await
            .expect("table_exists")
    );
    assert!(
        db.index_exists("p", "s1", "users", "idx_users_name")
            .await
            .expect("index_exists")
    );

    let projects = db.list_projects().await.expect("list projects");
    assert!(projects.iter().any(|p| p.project_id == "p"));

    let scopes = db.list_scopes_info("p").await.expect("list scopes");
    let scope = scopes
        .iter()
        .find(|s| s.scope_id == "s1")
        .expect("scope info");
    assert_eq!(scope.table_count, 1);

    let tables = db.list_tables_info("p", "s1").await.expect("list tables");
    let table = tables
        .iter()
        .find(|t| t.table_name == "users")
        .expect("table info");
    assert_eq!(table.column_count, 2);
    assert_eq!(table.index_count, 1);
}

#[tokio::test]
async fn dependency_aware_ddl_batch_reorders_create_dependencies() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");

    let batch = db
        .commit_ddl_batch_dependency_aware(vec![
            DdlOperation::CreateIndex {
                project_id: "arcana".into(),
                scope_id: "ops".into(),
                table_name: "users".into(),
                index_name: "idx_users_name".into(),
                if_not_exists: true,
                columns: vec!["name".into()],
                index_type: crate::catalog::schema::IndexType::BTree,
                partial_filter: None,
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
            DdlOperation::CreateScope {
                owner_id: None,
                project_id: "arcana".into(),
                scope_id: "ops".into(),
                if_not_exists: true,
            },
            DdlOperation::CreateProject {
                owner_id: None,
                project_id: "arcana".into(),
                if_not_exists: true,
            },
        ])
        .await
        .expect("dependency-aware batch");

    assert_eq!(batch.results.len(), 4);
    assert!(
        db.index_exists("arcana", "ops", "users", "idx_users_name")
            .await
            .expect("index exists")
    );
}

#[tokio::test]
async fn ddl_errors_expose_stable_error_codes() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("arcana").await.expect("project");

    let err = db
        .commit(Mutation::Ddl(DdlOperation::CreateIndex {
            project_id: "arcana".into(),
            scope_id: "app".into(),
            table_name: "missing_users".into(),
            index_name: "idx_missing".into(),
            if_not_exists: false,
            columns: vec!["name".into()],
            index_type: crate::catalog::schema::IndexType::BTree,
            partial_filter: None,
        }))
        .await
        .expect_err("create index on missing table should fail");

    assert_eq!(err.code(), AedbErrorCode::TableNotFound);
}

#[tokio::test]
async fn snapshot_limit_enforced_on_read_path() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(
        AedbConfig {
            max_concurrent_snapshots: 1,
            ..AedbConfig::default()
        },
        dir.path(),
    )
    .expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "alice".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: None,
        },
    }))
    .await
    .expect("grant");
    let caller = CallerContext::new("alice");

    let handle = {
        let mut mgr = db.snapshot_manager.lock();
        mgr.acquire_bounded(
            crate::snapshot::reader::SnapshotReadView {
                keyspace: Arc::new(crate::storage::keyspace::Keyspace::default().snapshot()),
                catalog: Arc::new(crate::catalog::Catalog::default()),
                seq: 0,
            },
            1,
        )
        .expect("occupy")
    };

    let err = db
        .kv_get("p", "app", b"k", ConsistencyMode::AtLatest, &caller)
        .await
        .expect_err("snapshot cap");
    assert!(matches!(
        err,
        crate::query::error::QueryError::SnapshotLimitReached
    ));

    let mut mgr = db.snapshot_manager.lock();
    mgr.release(handle);
    let _ = mgr.gc();
}

#[tokio::test]
async fn idempotent_retry_does_not_double_apply_non_idempotent_mutation() {
    let dir = tempdir().expect("temp");
    let db =
        Arc::new(AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open"));
    db.create_project("p").await.expect("project");

    let key = IdempotencyKey([42u8; 16]);
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let db = Arc::clone(&db);
        let key = key.clone();
        tasks.push(tokio::spawn(async move {
            db.commit_envelope(TransactionEnvelope {
                caller: None,
                idempotency_key: Some(key),
                write_class: WriteClass::Standard,
                assertions: Vec::new(),
                read_set: Default::default(),
                write_intent: WriteIntent {
                    mutations: vec![Mutation::KvIncU256 {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        key: b"idem-counter".to_vec(),
                        amount_be: {
                            let mut out = [0u8; 32];
                            out[31] = 1;
                            out
                        },
                    }],
                },
                base_seq: 0,
            })
            .await
            .expect("idempotent commit")
        }));
    }

    let mut seqs = std::collections::BTreeSet::new();
    let mut outcomes = Vec::new();
    for t in tasks {
        let res = t.await.expect("join");
        seqs.insert(res.commit_seq);
        outcomes.push(res.idempotency);
    }
    assert_eq!(seqs.len(), 1, "all retries must resolve to one commit_seq");
    assert!(
        outcomes
            .iter()
            .any(|o| matches!(o, crate::commit::executor::IdempotencyOutcome::Duplicate)),
        "at least one retry should report duplicate outcome"
    );

    let entry = db
        .kv_get_no_auth("p", "app", b"idem-counter", ConsistencyMode::AtLatest)
        .await
        .expect("kv_get")
        .expect("counter exists");
    assert_eq!(
        primitive_types::U256::from_big_endian(&entry.value),
        primitive_types::U256::one(),
        "idempotent retries must apply mutation exactly once"
    );
}

#[tokio::test]
#[ignore = "manual perf probe: durability knob sweep (batch/coalescing)"]
async fn benchmark_durability_knob_sweep() {
    fn percentile(sorted: &[u128], p: f64) -> u128 {
        if sorted.is_empty() {
            return 0;
        }
        let percentile_index = ((sorted.len() as f64 - 1.0) * p).round() as usize;
        sorted[percentile_index.min(sorted.len() - 1)]
    }

    #[derive(Clone)]
    struct Profile {
        name: &'static str,
        batch_interval_ms: u64,
        batch_max_bytes: usize,
        coalesce_enabled: bool,
        coalesce_window_us: u64,
    }

    async fn run_profile(profile: &Profile) -> (f64, u128, u128, u64, u64) {
        let dir = tempdir().expect("temp");
        let mut config = AedbConfig {
            durability_mode: DurabilityMode::Batch,
            batch_interval_ms: profile.batch_interval_ms,
            batch_max_bytes: profile.batch_max_bytes,
            recovery_mode: RecoveryMode::Permissive,
            hash_chain_required: false,
            durable_ack_coalescing_enabled: profile.coalesce_enabled,
            durable_ack_coalesce_window_us: profile.coalesce_window_us,
            ..AedbConfig::default()
        };
        config.manifest_hmac_key = None;
        let db = Arc::new(AedbInstance::open_anonymous(config, dir.path()).expect("open"));
        db.create_project("p").await.expect("project");

        for i in 0..8_000usize {
            db.commit(Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: format!("sweep-seed:{i:05}").into_bytes(),
                value: vec![b's'; 128],
            })
            .await
            .expect("seed");
        }

        let workers = 8usize;
        let commits_per_worker = 500usize;
        let started = Instant::now();
        let mut tasks = Vec::with_capacity(workers);
        for worker in 0..workers {
            let db = Arc::clone(&db);
            tasks.push(tokio::spawn(async move {
                let mut lats = Vec::with_capacity(commits_per_worker);
                for i in 0..commits_per_worker {
                    let t0 = Instant::now();
                    db.commit(Mutation::KvSet {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        key: format!("sweep:{worker:02}:{i:06}").into_bytes(),
                        value: vec![b'x'; 256],
                    })
                    .await
                    .expect("commit");
                    lats.push(t0.elapsed().as_micros());
                }
                lats
            }));
        }

        let mut all_lat = Vec::with_capacity(workers * commits_per_worker);
        for task in tasks {
            let mut lats = task.await.expect("worker join");
            all_lat.append(&mut lats);
        }
        all_lat.sort_unstable();

        let elapsed = started.elapsed().as_secs_f64().max(0.000_001);
        let tps = (workers * commits_per_worker) as f64 / elapsed;
        let p50 = percentile(&all_lat, 0.50);
        let p99 = percentile(&all_lat, 0.99);
        let op = db.operational_metrics().await;
        (tps, p50, p99, op.wal_sync_ops, op.avg_wal_sync_micros)
    }

    let profiles = vec![
        Profile {
            name: "baseline_10ms_1mb_no_coalesce",
            batch_interval_ms: 10,
            batch_max_bytes: 1024 * 1024,
            coalesce_enabled: false,
            coalesce_window_us: 0,
        },
        Profile {
            name: "trial_20ms_4mb_coalesce_1000us",
            batch_interval_ms: 20,
            batch_max_bytes: 4 * 1024 * 1024,
            coalesce_enabled: true,
            coalesce_window_us: 1000,
        },
        Profile {
            name: "trial_20ms_8mb_coalesce_1500us",
            batch_interval_ms: 20,
            batch_max_bytes: 8 * 1024 * 1024,
            coalesce_enabled: true,
            coalesce_window_us: 1500,
        },
        Profile {
            name: "trial_40ms_8mb_coalesce_1500us",
            batch_interval_ms: 40,
            batch_max_bytes: 8 * 1024 * 1024,
            coalesce_enabled: true,
            coalesce_window_us: 1500,
        },
    ];

    for profile in &profiles {
        let (tps, p50, p99, wal_sync_ops, avg_wal_sync_us) = run_profile(profile).await;
        eprintln!(
            "durability_sweep: profile={} tps={:.2} p50_us={} p99_us={} wal_sync_ops={} avg_wal_sync_us={}",
            profile.name, tps, p50, p99, wal_sync_ops, avg_wal_sync_us
        );
    }
}

#[tokio::test]
async fn strict_open_rejects_directory_previously_opened_in_non_strict_mode() {
    let dir = tempdir().expect("temp");
    let mut permissive = AedbConfig::production([7u8; 32]);
    permissive.recovery_mode = RecoveryMode::Permissive;
    permissive.hash_chain_required = false;

    let db = AedbInstance::open_anonymous(permissive, dir.path()).expect("open permissive");
    db.shutdown().await.expect("shutdown permissive");
    drop(db); // release the data-directory lock before reopening

    let strict = AedbConfig::production([7u8; 32]);
    let err = match AedbInstance::open_anonymous(strict, dir.path()) {
        Ok(db) => {
            db.shutdown().await.expect("shutdown unexpected strict db");
            panic!("strict open should fail closed");
        }
        Err(err) => err,
    };
    assert!(
        matches!(err, AedbError::Validation(ref msg) if msg.contains("strict open denied")),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn strict_open_rejects_tampered_trust_mode_marker() {
    let dir = tempdir().expect("temp");
    let mut permissive = AedbConfig::production([9u8; 32]);
    permissive.recovery_mode = RecoveryMode::Permissive;
    permissive.hash_chain_required = false;

    let db = AedbInstance::open_anonymous(permissive, dir.path()).expect("open permissive");
    db.shutdown().await.expect("shutdown permissive");
    drop(db); // release the data-directory lock before reopening

    fs::write(
        dir.path().join("trust_mode.json"),
        r#"{"ever_non_strict_recovery":false,"ever_hash_chain_disabled":false}"#,
    )
    .expect("tamper trust mode marker");

    let err = match AedbInstance::open_anonymous(AedbConfig::production([9u8; 32]), dir.path()) {
        Ok(db) => {
            db.shutdown().await.expect("shutdown unexpected strict db");
            panic!("tampered trust marker should fail closed");
        }
        Err(err) => err,
    };
    assert!(
        matches!(err, AedbError::IntegrityError { ref message } if message.contains("trust mode marker hmac mismatch")),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn queries_reject_oversized_in_lists_and_like_patterns() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");
    create_table(
        &db,
        "p",
        "app",
        "items",
        vec![
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
        vec!["id"],
    )
    .await;

    let oversized_in = db
        .query_with_options(
            "p",
            "app",
            Query::select(&["id"]).from("items").where_(Expr::In(
                "id".into(),
                (0..10_001).map(Value::Integer).collect(),
            )),
            QueryOptions::default(),
        )
        .await
        .expect_err("oversized IN list should be rejected");
    assert!(
        matches!(oversized_in, QueryError::InvalidQuery { reason } if reason.contains("IN list"))
    );

    let oversized_like = db
        .query_with_options(
            "p",
            "app",
            Query::select(&["name"])
                .from("items")
                .where_(Expr::Like("name".into(), "a".repeat(257))),
            QueryOptions::default(),
        )
        .await
        .expect_err("oversized LIKE should be rejected");
    assert!(
        matches!(oversized_like, QueryError::InvalidQuery { reason } if reason.contains("LIKE pattern"))
    );
}

#[tokio::test]
async fn managed_system_tables_reject_direct_user_mutations() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");

    let err = db
        .commit(Mutation::Upsert {
            project_id: crate::catalog::SYSTEM_PROJECT_ID.into(),
            scope_id: "app".into(),
            table_name: "reactive_processor_checkpoints".into(),
            primary_key: vec![Value::Text("processor".into())],
            row: Row {
                values: vec![
                    Value::Text("processor".into()),
                    Value::Integer(42),
                    Value::Timestamp(1),
                ],
            },
        })
        .await
        .expect_err("managed system tables must reject direct writes");

    assert!(
        matches!(err, AedbError::Validation(message) if message.contains("managed and read-only"))
    );
}
