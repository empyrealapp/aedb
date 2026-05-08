use aedb::AedbInstance;
use aedb::backup::{extract_backup_archive, sha256_file_hex, write_backup_archive};
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::tx::{ReadSet, TransactionEnvelope, WriteClass, WriteIntent};
use aedb::commit::validation::Mutation;
use aedb::config::{AedbConfig, DurabilityMode, RecoveryMode, StorageMode};
use aedb::permission::{CallerContext, Permission};
use aedb::query::plan::{ConsistencyMode, Query, col, lit};
use aedb::storage::page_store::PagedStore;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
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

fn benchmark_thresholds() -> BenchThresholds {
    if cfg!(debug_assertions) {
        return BenchThresholds {
            kv_get_p50_us: 25,
            kv_get_p99_us: 100,
            kv_scan_100_p50_us: 350,
            kv_scan_100_p99_us: 800,
            mixed_commit_p50_us: 60_000,
            mixed_commit_p99_us: 100_000,
            batch_throughput_cps: 1_500,
        };
    }

    BenchThresholds {
        kv_get_p50_us: 5,
        kv_get_p99_us: 50,
        kv_scan_100_p50_us: 150,
        kv_scan_100_p99_us: 500,
        mixed_commit_p50_us: 35_000,
        mixed_commit_p99_us: 60_000,
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

fn latency_us(latencies: &mut [u128]) -> (u64, u64) {
    latencies.sort_unstable();
    (
        percentile(latencies, 0.50) as u64,
        percentile(latencies, 0.99) as u64,
    )
}

fn high_throughput_config() -> AedbConfig {
    AedbConfig {
        durability_mode: DurabilityMode::Batch,
        batch_interval_ms: 10,
        batch_max_bytes: usize::MAX,
        recovery_mode: RecoveryMode::Permissive,
        hash_chain_required: false,
        ..AedbConfig::default()
    }
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
    let thresholds = benchmark_thresholds();

    let config = AedbConfig {
        recovery_mode: RecoveryMode::Strict,
        ..AedbConfig::default()
    }
    .with_hmac_key(vec![7u8; 32]);
    let (_dir, db) = setup(config, 2_000).await;

    let caller = CallerContext::new("bench");

    let mut kv_get_lat_ns = Vec::new();
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
        kv_get_lat_ns.push(t0.elapsed().as_nanos());
    }
    kv_get_lat_ns.sort_unstable();
    let kv_get_p50_ns = percentile(&kv_get_lat_ns, 0.50) as u64;
    let kv_get_p99_ns = percentile(&kv_get_lat_ns, 0.99) as u64;
    let kv_get_p50 = kv_get_p50_ns / 1_000;
    let kv_get_p99 = kv_get_p99_ns / 1_000;

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
        "benchmark_gate: kv_get p50={}ns p99={}ns ({}us/{}us); kv_scan100 p50={}us p99={}us; mixed_commit p50={}us p99={}us; batch_throughput={} cps",
        kv_get_p50_ns,
        kv_get_p99_ns,
        kv_get_p50,
        kv_get_p99,
        kv_scan_p50,
        kv_scan_p99,
        mixed_p50,
        mixed_p99,
        throughput
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
        assert!(kv_get_p99_ns > 0);
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
async fn benchmark_persistent_large_kv_values() {
    const VALUE_COUNT: usize = 48;
    const VALUE_BYTES: usize = 128 * 1024;

    fn disk_config(hot_cache_bytes: usize) -> AedbConfig {
        AedbConfig {
            durability_mode: DurabilityMode::OsBuffered,
            recovery_mode: RecoveryMode::Permissive,
            hash_chain_required: false,
            persistent_value_inline_threshold_bytes: 1024,
            persistent_value_hot_cache_bytes: hot_cache_bytes,
            max_kv_value_bytes: 256 * 1024,
            max_transaction_bytes: 512 * 1024,
            ..AedbConfig::default()
        }
    }

    async fn setup_large_values(hot_cache_bytes: usize) -> (tempfile::TempDir, AedbInstance) {
        let dir = tempdir().expect("temp dir");
        let db = AedbInstance::open(disk_config(hot_cache_bytes), dir.path()).expect("open");
        db.create_project(PROJECT_ID).await.expect("project");
        for i in 0..VALUE_COUNT {
            let value = vec![u8::try_from(i % 251).expect("byte"); VALUE_BYTES];
            db.commit(Mutation::KvSet {
                project_id: PROJECT_ID.into(),
                scope_id: SCOPE_ID.into(),
                key: format!("blob:{i}").into_bytes(),
                value,
            })
            .await
            .expect("seed blob");
        }
        (dir, db)
    }

    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open(disk_config(0), dir.path()).expect("open");
    db.create_project(PROJECT_ID).await.expect("project");
    let mut large_set_lat = Vec::new();
    for i in 0..VALUE_COUNT {
        let value = vec![u8::try_from(i % 251).expect("byte"); VALUE_BYTES];
        let t0 = Instant::now();
        db.commit(Mutation::KvSet {
            project_id: PROJECT_ID.into(),
            scope_id: SCOPE_ID.into(),
            key: format!("write:{i}").into_bytes(),
            value,
        })
        .await
        .expect("large kv set");
        large_set_lat.push(t0.elapsed().as_micros());
    }
    large_set_lat.sort_unstable();
    let large_set_p50 = percentile(&large_set_lat, 0.50) as u64;
    let large_set_p99 = percentile(&large_set_lat, 0.99) as u64;

    let (_hot_dir, hot_db) = setup_large_values(VALUE_BYTES * VALUE_COUNT).await;
    let mut hot_get_lat = Vec::new();
    for i in 0..VALUE_COUNT {
        let key = format!("blob:{i}");
        let t0 = Instant::now();
        let got = hot_db
            .kv_get_no_auth(
                PROJECT_ID,
                SCOPE_ID,
                key.as_bytes(),
                ConsistencyMode::AtLatest,
            )
            .await
            .expect("hot blob get")
            .expect("hot blob");
        assert_eq!(got.value.len(), VALUE_BYTES);
        hot_get_lat.push(t0.elapsed().as_micros());
    }
    hot_get_lat.sort_unstable();
    let hot_get_p50 = percentile(&hot_get_lat, 0.50) as u64;
    let hot_get_p99 = percentile(&hot_get_lat, 0.99) as u64;

    let (_cold_dir, cold_db) = setup_large_values(0).await;
    let mut cold_get_lat = Vec::new();
    for i in 0..VALUE_COUNT {
        let key = format!("blob:{i}");
        let t0 = Instant::now();
        let got = cold_db
            .kv_get_no_auth(
                PROJECT_ID,
                SCOPE_ID,
                key.as_bytes(),
                ConsistencyMode::AtLatest,
            )
            .await
            .expect("cold blob get")
            .expect("cold blob");
        assert_eq!(got.value.len(), VALUE_BYTES);
        cold_get_lat.push(t0.elapsed().as_micros());
    }
    cold_get_lat.sort_unstable();
    let cold_get_p50 = percentile(&cold_get_lat, 0.50) as u64;
    let cold_get_p99 = percentile(&cold_get_lat, 0.99) as u64;

    eprintln!(
        "persistent_large_kv: value_bytes={} count={} set_p50={}us set_p99={}us hot_get_p50={}us hot_get_p99={}us cold_get_p50={}us cold_get_p99={}us",
        VALUE_BYTES,
        VALUE_COUNT,
        large_set_p50,
        large_set_p99,
        hot_get_p50,
        hot_get_p99,
        cold_get_p50,
        cold_get_p99
    );

    assert!(large_set_p99 > 0);
    assert!(hot_get_p99 > 0);
    assert!(cold_get_p99 > 0);
}

#[test]
#[ignore = "benchmark gate; run explicitly in CI or perf environment"]
fn benchmark_page_store_batch_append_and_hot_cache() {
    const PAGE_SIZE: usize = 4096;
    const CACHE_PAGES: usize = 128;
    const PAGE_COUNT: usize = 1024;
    const PAYLOAD_BYTES: usize = 2048;

    let payloads = (0..PAGE_COUNT)
        .map(|i| vec![u8::try_from(i % 251).expect("byte"); PAYLOAD_BYTES])
        .collect::<Vec<_>>();

    let individual_dir = tempdir().expect("individual dir");
    let individual_store =
        PagedStore::open(individual_dir.path(), "rows.aedbpg", PAGE_SIZE, CACHE_PAGES)
            .expect("open individual page store");
    let individual_start = Instant::now();
    for payload in &payloads {
        individual_store
            .append_page(payload)
            .expect("append individual page");
    }
    let individual_append_ns = individual_start.elapsed().as_nanos();

    let batch_dir = tempdir().expect("batch dir");
    let batch_store = PagedStore::open(batch_dir.path(), "rows.aedbpg", PAGE_SIZE, CACHE_PAGES)
        .expect("open batch page store");
    let slices = payloads.iter().map(Vec::as_slice).collect::<Vec<&[u8]>>();
    let batch_start = Instant::now();
    let refs = batch_store.append_pages(&slices).expect("append batch");
    let batch_append_ns = batch_start.elapsed().as_nanos();

    assert_eq!(refs.len(), PAGE_COUNT);
    assert_eq!(batch_store.page_count(), PAGE_COUNT as u64);
    assert!(batch_store.cache_resident_pages() <= CACHE_PAGES);
    for (expected_page_id, page_ref) in refs.iter().enumerate() {
        assert_eq!(page_ref.page_id.0, expected_page_id as u64);
    }

    let mut hot_read_lat_ns = Vec::new();
    for page_ref in refs.iter().rev().take(CACHE_PAGES) {
        let t0 = Instant::now();
        let page = batch_store.read_page(page_ref).expect("hot page read");
        assert_eq!(page.len(), PAYLOAD_BYTES);
        hot_read_lat_ns.push(t0.elapsed().as_nanos());
    }
    hot_read_lat_ns.sort_unstable();
    let hot_p50_ns = percentile(&hot_read_lat_ns, 0.50) as u64;
    let hot_p99_ns = percentile(&hot_read_lat_ns, 0.99) as u64;
    let batch_per_page_ns = batch_append_ns / PAGE_COUNT as u128;
    let individual_per_page_ns = individual_append_ns / PAGE_COUNT as u128;

    eprintln!(
        "page_store_batch: pages={} payload_bytes={} individual_append={}ns batch_append={}ns individual_per_page={}ns batch_per_page={}ns hot_read_p50={}ns hot_read_p99={}ns",
        PAGE_COUNT,
        PAYLOAD_BYTES,
        individual_append_ns,
        batch_append_ns,
        individual_per_page_ns,
        batch_per_page_ns,
        hot_p50_ns,
        hot_p99_ns
    );

    assert!(batch_append_ns > 0);
    assert!(hot_p99_ns > 0);
    assert!(
        batch_append_ns < individual_append_ns,
        "batched append should avoid per-page lock and flush overhead"
    );
}

#[test]
#[ignore = "benchmark gate; run explicitly in CI or perf environment"]
fn benchmark_single_file_archive_large_page_file_streaming() {
    const LARGE_PAGE_BYTES: usize = 32 * 1024 * 1024;

    let src = tempdir().expect("src");
    let dst = tempdir().expect("dst");
    let archive = src.path().join("backup_large.aedbarc");
    let page_dir = src.path().join("pages");
    std::fs::create_dir_all(&page_dir).expect("page dir");
    std::fs::write(src.path().join("backup_manifest.json"), b"{\"x\":1}").expect("manifest");

    let page_file = page_dir.join("rows.aedbpg");
    let mut page = Vec::with_capacity(LARGE_PAGE_BYTES);
    for i in 0..LARGE_PAGE_BYTES {
        page.push(((i.wrapping_mul(17) ^ (i >> 11)) & 0xff) as u8);
    }
    std::fs::write(&page_file, &page).expect("page file");

    let key = [13u8; 32];
    let write_start = Instant::now();
    write_backup_archive(src.path(), &archive, Some(&key)).expect("write archive");
    let write_ms = write_start.elapsed().as_millis();

    let extract_start = Instant::now();
    extract_backup_archive(&archive, dst.path(), Some(&key)).expect("extract archive");
    let extract_ms = extract_start.elapsed().as_millis();

    let source_hash = sha256_file_hex(&page_file).expect("source hash");
    let restored_hash =
        sha256_file_hex(&dst.path().join("pages/rows.aedbpg")).expect("restored hash");
    assert_eq!(source_hash, restored_hash);

    let archive_bytes = std::fs::metadata(&archive).expect("archive metadata").len();
    let throughput_mib_s = |bytes: usize, millis: u128| -> u128 {
        let millis = millis.max(1);
        (bytes as u128 * 1000) / (millis * 1024 * 1024)
    };
    eprintln!(
        "single_file_archive_large_page_streaming: source_bytes={} archive_bytes={} write={}ms ({}MiB/s) extract={}ms ({}MiB/s)",
        LARGE_PAGE_BYTES,
        archive_bytes,
        write_ms,
        throughput_mib_s(LARGE_PAGE_BYTES, write_ms),
        extract_ms,
        throughput_mib_s(LARGE_PAGE_BYTES, extract_ms)
    );

    assert!(archive_bytes > 0);
    assert!(write_ms > 0);
    assert!(extract_ms > 0);
}

#[tokio::test]
#[ignore = "benchmark gate; run explicitly in CI or perf environment"]
async fn benchmark_small_kv_memory_pressure_spill() {
    const VALUE_COUNT: usize = 256;
    const VALUE_BYTES: usize = 256;
    let config = AedbConfig {
        durability_mode: DurabilityMode::OsBuffered,
        recovery_mode: RecoveryMode::Permissive,
        hash_chain_required: false,
        persistent_value_inline_threshold_bytes: 1024,
        persistent_value_hot_cache_bytes: VALUE_COUNT * VALUE_BYTES,
        max_kv_value_bytes: 1024,
        max_transaction_bytes: 4096,
        max_memory_estimate_bytes: 16 * 1024,
        ..AedbConfig::default()
    };
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open(config, dir.path()).expect("open");
    db.create_project(PROJECT_ID).await.expect("project");

    let mut set_lat = Vec::new();
    for i in 0..VALUE_COUNT {
        let value = vec![u8::try_from(i % 251).expect("byte"); VALUE_BYTES];
        let t0 = Instant::now();
        db.commit(Mutation::KvSet {
            project_id: PROJECT_ID.into(),
            scope_id: SCOPE_ID.into(),
            key: format!("small-pressure:{i:04}").into_bytes(),
            value,
        })
        .await
        .expect("small kv set");
        set_lat.push(t0.elapsed().as_micros());
    }
    set_lat.sort_unstable();
    let set_p50 = percentile(&set_lat, 0.50) as u64;
    let set_p99 = percentile(&set_lat, 0.99) as u64;

    let mut cold_get_lat = Vec::new();
    for i in 0..VALUE_COUNT {
        let key = format!("small-pressure:{i:04}");
        let t0 = Instant::now();
        let got = db
            .kv_get_no_auth(
                PROJECT_ID,
                SCOPE_ID,
                key.as_bytes(),
                ConsistencyMode::AtLatest,
            )
            .await
            .expect("cold small get")
            .expect("cold small value");
        assert_eq!(got.value.len(), VALUE_BYTES);
        cold_get_lat.push(t0.elapsed().as_micros());
    }
    cold_get_lat.sort_unstable();
    let cold_get_p50 = percentile(&cold_get_lat, 0.50) as u64;
    let cold_get_p99 = percentile(&cold_get_lat, 0.99) as u64;

    let mut hot_get_lat = Vec::new();
    for i in 0..VALUE_COUNT {
        let key = format!("small-pressure:{i:04}");
        let t0 = Instant::now();
        let got = db
            .kv_get_no_auth(
                PROJECT_ID,
                SCOPE_ID,
                key.as_bytes(),
                ConsistencyMode::AtLatest,
            )
            .await
            .expect("hot small get")
            .expect("hot small value");
        assert_eq!(got.value.len(), VALUE_BYTES);
        hot_get_lat.push(t0.elapsed().as_micros());
    }
    hot_get_lat.sort_unstable();
    let hot_get_p50 = percentile(&hot_get_lat, 0.50) as u64;
    let hot_get_p99 = percentile(&hot_get_lat, 0.99) as u64;

    let memory_estimate = db.estimated_memory_bytes().await;
    let metrics = db.operational_metrics().await;
    eprintln!(
        "small_kv_pressure: value_bytes={} count={} set_p50={}us set_p99={}us cold_get_p50={}us cold_get_p99={}us hot_get_p50={}us hot_get_p99={}us memory_estimate={} value_store_bytes={}",
        VALUE_BYTES,
        VALUE_COUNT,
        set_p50,
        set_p99,
        cold_get_p50,
        cold_get_p99,
        hot_get_p50,
        hot_get_p99,
        memory_estimate,
        metrics.persistent_value_store_bytes
    );

    assert!(memory_estimate <= 16 * 1024);
    assert!(metrics.persistent_value_store_bytes > 8 + VALUE_BYTES as u64);
    assert!(set_p99 > 0);
    assert!(cold_get_p99 > 0);
    assert!(hot_get_p99 <= cold_get_p99);
}

#[tokio::test]
#[ignore = "benchmark gate; run explicitly in CI or perf environment"]
async fn benchmark_many_small_kv_no_pressure() {
    const SEED_COUNT: usize = 4_096;
    const OPS: usize = 512;
    const VALUE_BYTES: usize = 64;
    let config = AedbConfig {
        durability_mode: DurabilityMode::OsBuffered,
        recovery_mode: RecoveryMode::Permissive,
        hash_chain_required: false,
        persistent_value_inline_threshold_bytes: 1024,
        max_kv_value_bytes: 1024,
        max_transaction_bytes: 4096,
        max_memory_estimate_bytes: 512 * 1024 * 1024,
        ..AedbConfig::default()
    };
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open(config, dir.path()).expect("open");
    db.create_project(PROJECT_ID).await.expect("project");

    for i in 0..SEED_COUNT {
        db.commit(Mutation::KvSet {
            project_id: PROJECT_ID.into(),
            scope_id: SCOPE_ID.into(),
            key: format!("many-small:{i:04}").into_bytes(),
            value: vec![u8::try_from(i % 251).expect("byte"); VALUE_BYTES],
        })
        .await
        .expect("seed small kv");
    }

    let mut update_lat = Vec::new();
    for i in 0..OPS {
        let t0 = Instant::now();
        db.commit(Mutation::KvSet {
            project_id: PROJECT_ID.into(),
            scope_id: SCOPE_ID.into(),
            key: format!("many-small:{:04}", i % SEED_COUNT).into_bytes(),
            value: vec![u8::try_from((i + 1) % 251).expect("byte"); VALUE_BYTES],
        })
        .await
        .expect("update small kv");
        update_lat.push(t0.elapsed().as_micros());
    }
    update_lat.sort_unstable();
    let update_p50 = percentile(&update_lat, 0.50) as u64;
    let update_p99 = percentile(&update_lat, 0.99) as u64;

    let mut get_lat = Vec::new();
    for i in 0..OPS {
        let key = format!("many-small:{:04}", i % SEED_COUNT);
        let t0 = Instant::now();
        let got = db
            .kv_get_no_auth(
                PROJECT_ID,
                SCOPE_ID,
                key.as_bytes(),
                ConsistencyMode::AtLatest,
            )
            .await
            .expect("get small kv")
            .expect("small kv");
        assert_eq!(got.value.len(), VALUE_BYTES);
        get_lat.push(t0.elapsed().as_micros());
    }
    get_lat.sort_unstable();
    let get_p50 = percentile(&get_lat, 0.50) as u64;
    let get_p99 = percentile(&get_lat, 0.99) as u64;

    let memory_estimate = db.estimated_memory_bytes().await;
    eprintln!(
        "many_small_kv_no_pressure: seed_count={} ops={} value_bytes={} update_p50={}us update_p99={}us get_p50={}us get_p99={}us memory_estimate={}",
        SEED_COUNT, OPS, VALUE_BYTES, update_p50, update_p99, get_p50, get_p99, memory_estimate
    );

    assert!(update_p99 > 0);
    assert!(get_p99 <= update_p99);
    assert!(memory_estimate > SEED_COUNT * VALUE_BYTES);
}

#[tokio::test]
#[ignore = "benchmark gate; run explicitly in CI or perf environment"]
async fn benchmark_durability_profile_write_matrix() {
    const OPS: usize = 160;
    let profiles = [
        (
            "full_strict",
            AedbConfig {
                durability_mode: DurabilityMode::Full,
                recovery_mode: RecoveryMode::Strict,
                ..AedbConfig::default()
            }
            .with_hmac_key(vec![3u8; 32]),
        ),
        (
            "batch_strict",
            AedbConfig {
                durability_mode: DurabilityMode::Batch,
                batch_interval_ms: 10,
                batch_max_bytes: usize::MAX,
                recovery_mode: RecoveryMode::Strict,
                ..AedbConfig::default()
            }
            .with_hmac_key(vec![4u8; 32]),
        ),
        (
            "osbuffered_permissive",
            AedbConfig {
                durability_mode: DurabilityMode::OsBuffered,
                recovery_mode: RecoveryMode::Permissive,
                hash_chain_required: false,
                ..AedbConfig::default()
            },
        ),
    ];

    for (name, config) in profiles {
        let dir = tempdir().expect("temp dir");
        let db = AedbInstance::open(config, dir.path()).expect("open");
        db.create_project(PROJECT_ID).await.expect("project");

        let mut latencies = Vec::with_capacity(OPS);
        for i in 0..OPS {
            let t0 = Instant::now();
            db.commit(Mutation::KvSet {
                project_id: PROJECT_ID.into(),
                scope_id: SCOPE_ID.into(),
                key: format!("profile:{name}:{i:04}").into_bytes(),
                value: vec![u8::try_from(i % 251).expect("byte"); 64],
            })
            .await
            .expect("profile write");
            latencies.push(t0.elapsed().as_micros());
        }
        let (p50, p99) = latency_us(&mut latencies);
        let metrics = db.operational_metrics().await;
        eprintln!(
            "durability_profile: name={} ops={} p50={}us p99={}us avg_commit={}us wal_append_ops={} avg_wal_append={}us wal_sync_ops={} avg_wal_sync={}us",
            name,
            OPS,
            p50,
            p99,
            metrics.avg_commit_latency_micros,
            metrics.wal_append_ops,
            metrics.avg_wal_append_micros,
            metrics.wal_sync_ops,
            metrics.avg_wal_sync_micros
        );
        assert!(p99 > 0);
    }
}

#[tokio::test]
#[ignore = "benchmark gate; run explicitly in CI or perf environment"]
async fn benchmark_many_small_kv_batching() {
    const INDIVIDUAL_OPS: usize = 512;
    const BATCHES: usize = 64;
    const BATCH_SIZE: usize = 16;
    let config = high_throughput_config();

    let individual_dir = tempdir().expect("individual temp dir");
    let individual_db = AedbInstance::open(config.clone(), individual_dir.path()).expect("open");
    individual_db
        .create_project(PROJECT_ID)
        .await
        .expect("project");
    let individual_start = Instant::now();
    for i in 0..INDIVIDUAL_OPS {
        individual_db
            .commit(Mutation::KvSet {
                project_id: PROJECT_ID.into(),
                scope_id: SCOPE_ID.into(),
                key: format!("individual:{i:04}").into_bytes(),
                value: vec![1u8; 32],
            })
            .await
            .expect("individual write");
    }
    let individual_secs = individual_start.elapsed().as_secs_f64().max(0.001);
    let individual_ops_per_sec = (INDIVIDUAL_OPS as f64 / individual_secs) as u64;

    let batch_dir = tempdir().expect("batch temp dir");
    let batch_db = AedbInstance::open(config, batch_dir.path()).expect("open");
    batch_db.create_project(PROJECT_ID).await.expect("project");
    let batch_start = Instant::now();
    for batch in 0..BATCHES {
        let mut entries = Vec::with_capacity(BATCH_SIZE);
        for item in 0..BATCH_SIZE {
            let key_index = batch * BATCH_SIZE + item;
            entries.push((
                format!("batched:{key_index:04}").into_bytes(),
                vec![2u8; 32],
            ));
        }
        batch_db
            .kv_set_many_atomic(PROJECT_ID, SCOPE_ID, entries)
            .await
            .expect("batched write");
    }
    let batch_secs = batch_start.elapsed().as_secs_f64().max(0.001);
    let batched_ops = BATCHES * BATCH_SIZE;
    let batch_ops_per_sec = (batched_ops as f64 / batch_secs) as u64;
    let batch_commits_per_sec = (BATCHES as f64 / batch_secs) as u64;

    eprintln!(
        "many_small_kv_batching: individual_ops={} individual_ops_per_sec={} batched_ops={} batch_size={} batched_ops_per_sec={} batched_commits_per_sec={}",
        INDIVIDUAL_OPS,
        individual_ops_per_sec,
        batched_ops,
        BATCH_SIZE,
        batch_ops_per_sec,
        batch_commits_per_sec
    );
    assert!(batch_ops_per_sec > individual_ops_per_sec);
}

#[tokio::test]
#[ignore = "benchmark gate; run explicitly in CI or perf environment"]
async fn benchmark_spilled_kv_hot_cold_read_mix() {
    const KEYS: usize = 2_048;
    const HOT_KEYS: usize = 64;
    const OPS: usize = 1_024;
    const VALUE_BYTES: usize = 128;
    let config = AedbConfig {
        durability_mode: DurabilityMode::OsBuffered,
        recovery_mode: RecoveryMode::Permissive,
        hash_chain_required: false,
        persistent_value_inline_threshold_bytes: 32,
        persistent_value_hot_cache_bytes: HOT_KEYS * VALUE_BYTES,
        max_kv_value_bytes: 1024,
        max_transaction_bytes: 4096,
        ..AedbConfig::default()
    };
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open(config, dir.path()).expect("open");
    db.create_project(PROJECT_ID).await.expect("project");
    for i in 0..KEYS {
        db.commit(Mutation::KvSet {
            project_id: PROJECT_ID.into(),
            scope_id: SCOPE_ID.into(),
            key: format!("zipf:{i:04}").into_bytes(),
            value: vec![u8::try_from(i % 251).expect("byte"); VALUE_BYTES],
        })
        .await
        .expect("seed spilled kv");
    }

    let mut latencies = Vec::with_capacity(OPS);
    for i in 0..OPS {
        let key_index = if i % 10 < 8 {
            (i * 17) % HOT_KEYS
        } else {
            HOT_KEYS + ((i * 131) % (KEYS - HOT_KEYS))
        };
        let key = format!("zipf:{key_index:04}");
        let t0 = Instant::now();
        let got = db
            .kv_get_no_auth(
                PROJECT_ID,
                SCOPE_ID,
                key.as_bytes(),
                ConsistencyMode::AtLatest,
            )
            .await
            .expect("get spilled kv")
            .expect("value");
        assert_eq!(got.value.len(), VALUE_BYTES);
        latencies.push(t0.elapsed().as_micros());
    }
    let (p50, p99) = latency_us(&mut latencies);
    let metrics = db.operational_metrics().await;
    eprintln!(
        "spilled_kv_hot_cold_read_mix: keys={} hot_keys={} ops={} p50={}us p99={}us value_store_bytes={} hot_cache_bytes={} hot_cache_capacity={}",
        KEYS,
        HOT_KEYS,
        OPS,
        p50,
        p99,
        metrics.persistent_value_store_bytes,
        metrics.persistent_value_hot_cache_bytes,
        metrics.persistent_value_hot_cache_capacity_bytes
    );
    assert!(p99 > 0);
    assert!(
        metrics.persistent_value_hot_cache_bytes
            <= metrics.persistent_value_hot_cache_capacity_bytes
    );
}

#[tokio::test]
#[ignore = "benchmark gate; run explicitly in CI or perf environment"]
async fn benchmark_checkpoint_and_backup_under_spilled_write_load() {
    const VALUE_BYTES: usize = 64 * 1024;
    const SEED_VALUES: usize = 24;
    let config = AedbConfig {
        storage_mode: StorageMode::DiskBacked,
        durability_mode: DurabilityMode::Batch,
        batch_interval_ms: 10,
        batch_max_bytes: usize::MAX,
        recovery_mode: RecoveryMode::Permissive,
        hash_chain_required: false,
        persistent_value_inline_threshold_bytes: 1024,
        persistent_value_hot_cache_bytes: 4 * VALUE_BYTES,
        max_kv_value_bytes: 128 * 1024,
        max_transaction_bytes: 256 * 1024,
        ..AedbConfig::default()
    };
    let dir = tempdir().expect("temp dir");
    let backup_dir = tempdir().expect("backup dir");
    let db = Arc::new(AedbInstance::open(config, dir.path()).expect("open"));
    db.create_project(PROJECT_ID).await.expect("project");
    for i in 0..SEED_VALUES {
        db.commit(Mutation::KvSet {
            project_id: PROJECT_ID.into(),
            scope_id: SCOPE_ID.into(),
            key: format!("seed-large:{i:04}").into_bytes(),
            value: vec![u8::try_from(i % 251).expect("byte"); VALUE_BYTES],
        })
        .await
        .expect("seed large value");
    }

    let stop = Arc::new(AtomicBool::new(false));
    let writer_stop = Arc::clone(&stop);
    let writer_db = Arc::clone(&db);
    let writer = tokio::spawn(async move {
        let mut writes = 0usize;
        while !writer_stop.load(Ordering::Relaxed) {
            writer_db
                .commit(Mutation::KvSet {
                    project_id: PROJECT_ID.into(),
                    scope_id: SCOPE_ID.into(),
                    key: format!("live-large:{writes:04}").into_bytes(),
                    value: vec![u8::try_from(writes % 251).expect("byte"); VALUE_BYTES],
                })
                .await
                .expect("live large write");
            writes += 1;
            if writes >= 32 {
                break;
            }
        }
        writes
    });

    tokio::time::sleep(Duration::from_millis(25)).await;
    let checkpoint_started = Instant::now();
    let checkpoint_seq = db.checkpoint_now().await.expect("checkpoint");
    let checkpoint_micros = checkpoint_started.elapsed().as_micros() as u64;
    let backup_started = Instant::now();
    let backup = db
        .backup_full(backup_dir.path())
        .await
        .expect("backup full");
    let backup_micros = backup_started.elapsed().as_micros() as u64;
    stop.store(true, Ordering::Relaxed);
    let writes = writer.await.expect("writer join");

    let metrics = db.operational_metrics().await;
    eprintln!(
        "checkpoint_backup_spilled_write_load: checkpoint_seq={} checkpoint={}us backup_seq={} backup={}us concurrent_writes={} value_store_bytes={} hot_cache_bytes={}",
        checkpoint_seq,
        checkpoint_micros,
        backup.wal_head_seq,
        backup_micros,
        writes,
        metrics.persistent_value_store_bytes,
        metrics.persistent_value_hot_cache_bytes
    );
    assert!(checkpoint_seq > 0);
    assert!(backup.wal_head_seq >= checkpoint_seq);
    assert!(writes > 0);
}
