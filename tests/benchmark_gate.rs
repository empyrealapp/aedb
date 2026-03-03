use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::tx::{ReadSet, TransactionEnvelope, WriteClass, WriteIntent};
use aedb::commit::validation::Mutation;
use aedb::config::{AedbConfig, DurabilityMode, RecoveryMode};
use aedb::error::AedbError;
use aedb::permission::{CallerContext, Permission};
use aedb::query::plan::{ConsistencyMode, Query, col, lit};
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

const PROJECT_ID: &str = "bench";
const SCOPE_ID: &str = "app";
const TABLE_NAME: &str = "users";

#[derive(Debug, Clone, Copy)]
struct BenchThresholds {
    kv_get_p50_us: u64,
    kv_get_p99_us: u64,
    kv_scan_100_p50_us: u64,
    kv_scan_100_p99_us: u64,
    mixed_commit_p50_us: u64,
    mixed_commit_p99_us: u64,
    batch_throughput_cps: u64,
}

fn doc_thresholds() -> BenchThresholds {
    BenchThresholds {
        kv_get_p50_us: 5,
        kv_get_p99_us: 50,
        kv_scan_100_p50_us: 100,
        kv_scan_100_p99_us: 500,
        mixed_commit_p50_us: 200,
        mixed_commit_p99_us: 2_000,
        batch_throughput_cps: 5_000,
    }
}

fn percentile(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let percentile_index = ((sorted.len().saturating_sub(1)) as f64 * p).round() as usize;
    sorted[percentile_index.min(sorted.len() - 1)]
}

async fn setup(config: AedbConfig, rows: i64) -> (tempfile::TempDir, AedbInstance) {
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open(config, dir.path()).expect("open");
    db.create_project(PROJECT_ID).await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: PROJECT_ID.into(),
        scope_id: SCOPE_ID.into(),
        table_name: TABLE_NAME.into(),
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
            ColumnDef {
                name: "age".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "bench".into(),
        permission: Permission::KvRead {
            project_id: PROJECT_ID.into(),
            scope_id: Some(SCOPE_ID.into()),
            prefix: None,
        },
    }))
    .await
    .expect("grant read");

    for i in 0..rows {
        db.commit(Mutation::Upsert {
            project_id: PROJECT_ID.into(),
            scope_id: SCOPE_ID.into(),
            table_name: TABLE_NAME.into(),
            primary_key: vec![Value::Integer(i)],
            row: Row {
                values: vec![
                    Value::Integer(i),
                    Value::Text(format!("user-{i}").into()),
                    Value::Integer(18 + (i % 50)),
                ],
            },
        })
        .await
        .expect("seed upsert");
        db.commit(Mutation::KvSet {
            project_id: PROJECT_ID.into(),
            scope_id: SCOPE_ID.into(),
            key: format!("bank:user:{i}:balance").into_bytes(),
            value: vec![0u8; 32],
        })
        .await
        .expect("seed kv");
    }
    (dir, db)
}

#[tokio::test]
#[ignore = "benchmark gate; run explicitly in CI or perf environment"]
async fn benchmark_gate_doc_matrix() {
    let enforce = std::env::var("AEDB_ENFORCE_BENCH_GATES")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let thresholds = doc_thresholds();

    let config = AedbConfig {
        recovery_mode: RecoveryMode::Strict,
        ..AedbConfig::default()
    }
    .with_hmac_key(vec![7u8; 32]);
    let (_dir, db) = setup(config, 2_000).await;

    let caller = CallerContext::new("bench");

    let mut kv_get_lat = Vec::new();
    for i in 0..300 {
        let key = format!("bank:user:{}:balance", i % 2_000);
        let t0 = Instant::now();
        let _ = db
            .kv_get(
                PROJECT_ID,
                SCOPE_ID,
                key.as_bytes(),
                ConsistencyMode::AtLatest,
                &caller,
            )
            .await
            .expect("kv get");
        kv_get_lat.push(t0.elapsed().as_micros());
    }
    kv_get_lat.sort_unstable();
    let kv_get_p50 = percentile(&kv_get_lat, 0.50) as u64;
    let kv_get_p99 = percentile(&kv_get_lat, 0.99) as u64;

    let mut kv_scan_lat = Vec::new();
    for _ in 0..120 {
        let t0 = Instant::now();
        let _ = db
            .kv_scan_prefix(
                PROJECT_ID,
                SCOPE_ID,
                b"bank:user:",
                100,
                None,
                ConsistencyMode::AtLatest,
                &caller,
            )
            .await
            .expect("kv scan");
        kv_scan_lat.push(t0.elapsed().as_micros());
    }
    kv_scan_lat.sort_unstable();
    let kv_scan_p50 = percentile(&kv_scan_lat, 0.50) as u64;
    let kv_scan_p99 = percentile(&kv_scan_lat, 0.99) as u64;

    let mut mixed_commit_lat = Vec::new();
    for i in 0..200 {
        let t0 = Instant::now();
        db.commit(Mutation::Upsert {
            project_id: PROJECT_ID.into(),
            scope_id: SCOPE_ID.into(),
            table_name: TABLE_NAME.into(),
            primary_key: vec![Value::Integer(10_000 + i)],
            row: Row {
                values: vec![
                    Value::Integer(10_000 + i),
                    Value::Text("bench".into()),
                    Value::Integer(25),
                ],
            },
        })
        .await
        .expect("mixed commit upsert");
        db.commit(Mutation::KvSet {
            project_id: PROJECT_ID.into(),
            scope_id: SCOPE_ID.into(),
            key: format!("mix:{i}:a").into_bytes(),
            value: vec![1u8; 32],
        })
        .await
        .expect("mixed commit kv set");
        db.commit(Mutation::KvIncU256 {
            project_id: PROJECT_ID.into(),
            scope_id: SCOPE_ID.into(),
            key: format!("mix:{i}:b").into_bytes(),
            amount_be: [0u8; 32],
        })
        .await
        .expect("mixed commit kv inc");
        db.commit(Mutation::KvSet {
            project_id: PROJECT_ID.into(),
            scope_id: SCOPE_ID.into(),
            key: format!("mix:{i}:c").into_bytes(),
            value: vec![2u8; 32],
        })
        .await
        .expect("mixed commit kv set2");
        let _ = db
            .query(
                PROJECT_ID,
                SCOPE_ID,
                Query::select(&["id"])
                    .from(TABLE_NAME)
                    .where_(col("id").eq(lit(10_000 + i)))
                    .limit(1),
            )
            .await
            .expect("mixed commit point query");
        mixed_commit_lat.push(t0.elapsed().as_micros());
    }
    mixed_commit_lat.sort_unstable();
    let mixed_p50 = percentile(&mixed_commit_lat, 0.50) as u64;
    let mixed_p99 = percentile(&mixed_commit_lat, 0.99) as u64;

    let mut batch_cfg = AedbConfig {
        durability_mode: DurabilityMode::Batch,
        batch_interval_ms: 10,
        batch_max_bytes: usize::MAX,
        recovery_mode: RecoveryMode::Permissive,
        hash_chain_required: false,
        ..AedbConfig::default()
    };
    batch_cfg.manifest_hmac_key = None;
    let (_batch_dir, batch_db) = setup(batch_cfg, 200).await;
    let start = Instant::now();
    let mut submitted = 0u64;
    while start.elapsed().as_millis() < 1_500 {
        batch_db
            .commit(Mutation::KvSet {
                project_id: PROJECT_ID.into(),
                scope_id: SCOPE_ID.into(),
                key: format!("throughput:{submitted}").into_bytes(),
                value: vec![1u8; 32],
            })
            .await
            .expect("throughput write");
        submitted += 1;
    }
    let elapsed_secs = start.elapsed().as_secs_f64().max(0.001);
    let throughput = (submitted as f64 / elapsed_secs) as u64;

    eprintln!(
        "benchmark_gate: kv_get p50={}us p99={}us; kv_scan100 p50={}us p99={}us; mixed_commit p50={}us p99={}us; batch_throughput={} cps",
        kv_get_p50, kv_get_p99, kv_scan_p50, kv_scan_p99, mixed_p50, mixed_p99, throughput
    );

    if enforce {
        assert!(kv_get_p50 <= thresholds.kv_get_p50_us);
        assert!(kv_get_p99 <= thresholds.kv_get_p99_us);
        assert!(kv_scan_p50 <= thresholds.kv_scan_100_p50_us);
        assert!(kv_scan_p99 <= thresholds.kv_scan_100_p99_us);
        assert!(mixed_p50 <= thresholds.mixed_commit_p50_us);
        assert!(mixed_p99 <= thresholds.mixed_commit_p99_us);
        assert!(throughput >= thresholds.batch_throughput_cps);
    } else {
        assert!(kv_get_p99 > 0);
        assert!(kv_scan_p99 > 0);
        assert!(mixed_p99 > 0);
        assert!(throughput > 0);
    }
}

#[tokio::test]
#[ignore = "benchmark gate; run explicitly in CI or perf environment"]
async fn benchmark_coordinator_vs_parallel_lanes() {
    let config = AedbConfig {
        durability_mode: DurabilityMode::Batch,
        batch_interval_ms: 10,
        batch_max_bytes: usize::MAX,
        recovery_mode: RecoveryMode::Permissive,
        hash_chain_required: false,
        ..AedbConfig::default()
    };
    let (_dir, db) = setup(config, 200).await;

    db.commit(Mutation::Ddl(DdlOperation::CreateScope {
        owner_id: None,
        if_not_exists: true,
        project_id: PROJECT_ID.into(),
        scope_id: "other".into(),
    }))
    .await
    .expect("create second scope");

    let parallel_start = Instant::now();
    let mut parallel_count = 0u64;
    while parallel_start.elapsed().as_millis() < 1_500 {
        db.commit(Mutation::KvSet {
            project_id: PROJECT_ID.into(),
            scope_id: SCOPE_ID.into(),
            key: format!("lane:parallel:{parallel_count}").into_bytes(),
            value: vec![1u8; 32],
        })
        .await
        .expect("parallel lane write");
        parallel_count += 1;
    }
    let parallel_tps =
        (parallel_count as f64 / parallel_start.elapsed().as_secs_f64().max(0.001)) as u64;

    let coordinator_start = Instant::now();
    let mut coordinator_count = 0u64;
    while coordinator_start.elapsed().as_millis() < 1_500 {
        let base_seq = db.head_state().await.visible_head_seq;
        db.commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![
                    Mutation::KvSet {
                        project_id: PROJECT_ID.into(),
                        scope_id: SCOPE_ID.into(),
                        key: format!("lane:coord:a:{coordinator_count}").into_bytes(),
                        value: vec![1u8; 16],
                    },
                    Mutation::KvSet {
                        project_id: PROJECT_ID.into(),
                        scope_id: "other".into(),
                        key: format!("lane:coord:b:{coordinator_count}").into_bytes(),
                        value: vec![2u8; 16],
                    },
                ],
            },
            base_seq,
        })
        .await
        .expect("coordinator write");
        coordinator_count += 1;
    }
    let coordinator_tps =
        (coordinator_count as f64 / coordinator_start.elapsed().as_secs_f64().max(0.001)) as u64;

    eprintln!(
        "benchmark_coordinator_vs_parallel_lanes: parallel_tps={} coordinator_tps={} (parallel_commits={} coordinator_commits={})",
        parallel_tps, coordinator_tps, parallel_count, coordinator_count
    );
}

#[tokio::test]
#[ignore = "benchmark gate; run explicitly in CI or perf environment"]
async fn benchmark_accumulator_expose_throughput() {
    let enforce = std::env::var("AEDB_ENFORCE_BENCH_GATES")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let target_tps = 10_000u64;
    let enforce_percentile = std::env::var("AEDB_BENCH_GATE_PERCENTILE")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .map(|p| p.clamp(0.0, 1.0))
        .unwrap_or(0.25);

    let mode = std::env::var("AEDB_BENCH_DURABILITY").unwrap_or_else(|_| "batch".to_string());
    let config = if mode.eq_ignore_ascii_case("full") {
        AedbConfig {
            durability_mode: DurabilityMode::Full,
            recovery_mode: RecoveryMode::Strict,
            ..AedbConfig::default()
        }
        .with_hmac_key(vec![9u8; 32])
    } else {
        AedbConfig {
            durability_mode: DurabilityMode::Batch,
            batch_interval_ms: 10,
            batch_max_bytes: usize::MAX,
            recovery_mode: RecoveryMode::Permissive,
            hash_chain_required: false,
            ..AedbConfig::default()
        }
    };
    let workers = std::env::var("AEDB_BENCH_WORKERS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(32);
    let run_secs = std::env::var("AEDB_BENCH_SECONDS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(2);
    let runs = std::env::var("AEDB_BENCH_RUNS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(if enforce { 5 } else { 1 })
        .max(1);
    let mut run_tps = Vec::with_capacity(runs);
    let mut rejected_total = 0u64;
    for run_idx in 0..runs {
        let dir = tempdir().expect("temp dir");
        let db = Arc::new(AedbInstance::open(config.clone(), dir.path()).expect("open"));
        db.create_project(PROJECT_ID).await.expect("project");
        db.create_accumulator_with_options(
            PROJECT_ID,
            SCOPE_ID,
            "house_balance",
            Some(200_000),
            20_000,
            0,
            None,
        )
        .await
        .expect("create accumulator");
        db.accumulate(
            PROJECT_ID,
            SCOPE_ID,
            "house_balance",
            1_000_000_000,
            "seed".into(),
            1,
        )
        .await
        .expect("seed balance");
        let deadline = Instant::now() + std::time::Duration::from_secs(run_secs);
        let mut handles = Vec::with_capacity(workers);
        for worker in 0..workers {
            let db = Arc::clone(&db);
            handles.push(tokio::spawn(async move {
                let mut ok = 0u64;
                let mut rejected = 0u64;
                let mut seq = 0u64;
                while Instant::now() < deadline {
                    let exposure_id = format!("bench:{run_idx}:{worker}:{seq}");
                    match db
                        .expose_accumulator(PROJECT_ID, SCOPE_ID, "house_balance", 1, exposure_id)
                        .await
                    {
                        Ok(_) => ok += 1,
                        Err(AedbError::Validation(_)) => rejected += 1,
                        Err(e) => panic!("unexpected expose error: {e}"),
                    }
                    seq += 1;
                }
                (ok, rejected)
            }));
        }
        let mut ok_total = 0u64;
        let mut rejected_run = 0u64;
        for handle in handles {
            let (ok, rejected) = handle.await.expect("worker join");
            ok_total += ok;
            rejected_run += rejected;
        }
        rejected_total += rejected_run;
        let tps = (ok_total as f64 / run_secs as f64).round() as u64;
        run_tps.push(tps);
        eprintln!(
            "benchmark_accumulator_expose_throughput: durability={} run={} workers={} seconds={} ok={} rejected={} tps={}",
            mode,
            run_idx + 1,
            workers,
            run_secs,
            ok_total,
            rejected_run,
            tps
        );
    }
    run_tps.sort_unstable();
    let run_tps_u128: Vec<u128> = run_tps.iter().map(|v| *v as u128).collect();
    let p50 = percentile(&run_tps_u128, 0.50) as u64;
    let p10 = percentile(&run_tps_u128, 0.10) as u64;
    let gate_tps = percentile(&run_tps_u128, enforce_percentile) as u64;
    eprintln!(
        "benchmark_accumulator_expose_throughput summary: durability={} runs={} gate_percentile={} gate_tps={} p10_tps={} p50_tps={} min_tps={} max_tps={} rejected_total={}",
        mode,
        runs,
        enforce_percentile,
        gate_tps,
        p10,
        p50,
        run_tps.first().copied().unwrap_or_default(),
        run_tps.last().copied().unwrap_or_default(),
        rejected_total
    );

    if enforce {
        assert_eq!(rejected_total, 0, "unexpected exposure rejections");
        assert!(
            gate_tps >= target_tps,
            "accumulator expose throughput p{} below target: {} < {}",
            (enforce_percentile * 100.0).round() as u64,
            gate_tps,
            target_tps
        );
    } else {
        assert!(p50 > 0);
    }
}

#[tokio::test]
#[ignore = "benchmark gate; run explicitly in CI or perf environment"]
async fn benchmark_accumulator_expose_batched_throughput() {
    let mode = std::env::var("AEDB_BENCH_DURABILITY").unwrap_or_else(|_| "batch".to_string());
    let config = if mode.eq_ignore_ascii_case("full") {
        AedbConfig {
            durability_mode: DurabilityMode::Full,
            recovery_mode: RecoveryMode::Strict,
            ..AedbConfig::default()
        }
        .with_hmac_key(vec![9u8; 32])
    } else {
        AedbConfig {
            durability_mode: DurabilityMode::Batch,
            batch_interval_ms: 10,
            batch_max_bytes: usize::MAX,
            recovery_mode: RecoveryMode::Permissive,
            hash_chain_required: false,
            ..AedbConfig::default()
        }
    };
    let dir = tempdir().expect("temp dir");
    let db = Arc::new(AedbInstance::open(config, dir.path()).expect("open"));
    db.create_project(PROJECT_ID).await.expect("project");
    db.create_accumulator_with_options(
        PROJECT_ID,
        SCOPE_ID,
        "house_balance",
        Some(200_000),
        20_000,
        0,
        None,
    )
    .await
    .expect("create accumulator");
    db.accumulate(
        PROJECT_ID,
        SCOPE_ID,
        "house_balance",
        1_000_000_000,
        "seed".into(),
        1,
    )
    .await
    .expect("seed balance");

    let batch_size = std::env::var("AEDB_BENCH_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(16)
        .max(1);
    let workers = std::env::var("AEDB_BENCH_WORKERS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(16)
        .max(1);
    let run_secs = std::env::var("AEDB_BENCH_SECONDS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(2);
    let deadline = Instant::now() + std::time::Duration::from_secs(run_secs);

    let mut handles = Vec::with_capacity(workers);
    for worker in 0..workers {
        let db = Arc::clone(&db);
        handles.push(tokio::spawn(async move {
            let mut batches = 0u64;
            let mut seq = 0u64;
            while Instant::now() < deadline {
                let mut exposures = Vec::with_capacity(batch_size);
                for _ in 0..batch_size {
                    exposures.push((1, format!("bench-batch:{worker}:{seq}")));
                    seq += 1;
                }
                db.expose_accumulator_many_atomic(PROJECT_ID, SCOPE_ID, "house_balance", exposures)
                    .await
                    .expect("batch expose");
                batches += 1;
            }
            batches
        }));
    }
    let mut batches = 0u64;
    for handle in handles {
        batches += handle.await.expect("worker join");
    }
    let elapsed = run_secs as f64;
    let total_ops = batches * batch_size as u64;
    let ops_tps = (total_ops as f64 / elapsed).round() as u64;
    let commit_tps = (batches as f64 / elapsed).round() as u64;
    eprintln!(
        "benchmark_accumulator_expose_batched_throughput: durability={} workers={} batch_size={} seconds={} commits={} commit_tps={} ops_tps={}",
        mode, workers, batch_size, run_secs, batches, commit_tps, ops_tps
    );
    assert!(ops_tps > 0);
}
