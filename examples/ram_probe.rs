//! RAM shape probe — measure how memory grows with dataset size.
//!
//!   cargo run --release --example ram_probe -- [seed] [shape]
//!
//! shape: "rows" (default) | "kv" | "kv-large"
//!
//! Reports:
//!   - aedb's running counter (`estimated_memory_bytes`)
//!   - process RSS delta (actual resident bytes)
//!   - ratio (overhead from OrdMap nodes, redundant caches, version_store, etc.)
//!   - per-row resident cost
//!
//! Run at multiple seeds to see the scaling slope.

use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::validation::Mutation;
use aedb::config::AedbConfig;
use std::fs;
use tempfile::tempdir;

const PROJECT: &str = "p";
const SCOPE: &str = "app";

fn rss_bytes() -> u64 {
    let s = fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let pages: u64 = s.split_whitespace().nth(1).and_then(|v| v.parse().ok()).unwrap_or(0);
    pages * 4096
}

fn human(b: u64) -> String {
    if b > 1 << 30 {
        format!("{:.2} GiB", b as f64 / (1u64 << 30) as f64)
    } else if b > 1 << 20 {
        format!("{:.2} MiB", b as f64 / (1u64 << 20) as f64)
    } else if b > 1 << 10 {
        format!("{:.2} KiB", b as f64 / (1u64 << 10) as f64)
    } else {
        format!("{b} B")
    }
}

async fn seed_rows(db: &AedbInstance, count: i64) {
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: PROJECT.into(),
        scope_id: SCOPE.into(),
        table_name: "users".into(),
        owner_id: None,
        if_not_exists: false,
        columns: vec![
            ColumnDef { name: "id".into(), col_type: ColumnType::Integer, nullable: false },
            ColumnDef { name: "name".into(), col_type: ColumnType::Text, nullable: false },
            ColumnDef { name: "age".into(), col_type: ColumnType::Integer, nullable: false },
        ],
        primary_key: vec!["id".into()],
    })).await.expect("table");
    let batch = 256_i64;
    let mut id = 0_i64;
    while id < count {
        let n = batch.min(count - id);
        let mut rows = Vec::with_capacity(n as usize);
        for _ in 0..n {
            rows.push(Row {
                values: vec![
                    Value::Integer(id),
                    Value::Text(format!("user_{id}").into()),
                    Value::Integer(20 + (id % 60)),
                ],
            });
            id += 1;
        }
        db.commit(Mutation::UpsertBatch {
            project_id: PROJECT.into(),
            scope_id: SCOPE.into(),
            table_name: "users".into(),
            rows,
        }).await.expect("seed");
    }
}

async fn seed_kv(db: &AedbInstance, count: i64, value_size: usize) {
    let payload = vec![0xABu8; value_size];
    for i in 0..count {
        db.commit(Mutation::KvSet {
            project_id: PROJECT.into(),
            scope_id: SCOPE.into(),
            key: format!("k:{i:08}").into_bytes(),
            value: payload.clone(),
        }).await.expect("kv set");
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let mut args = std::env::args().skip(1);
    let seed: i64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(100_000);
    let shape: String = args.next().unwrap_or_else(|| "rows".into());

    let rss_baseline = rss_bytes();
    let dir = tempdir().expect("tempdir");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project(PROJECT).await.expect("project");
    db.create_scope(PROJECT, SCOPE).await.expect("scope");

    let label = match shape.as_str() {
        "rows" => { seed_rows(&db, seed).await; "rows-3col".to_string() }
        "kv" => { seed_kv(&db, seed, 64).await; "kv-64B".to_string() }
        "kv-large" => { seed_kv(&db, seed, 16 * 1024).await; "kv-16KB".to_string() }
        other => panic!("unknown shape: {other}"),
    };

    let counter = db.estimated_memory_bytes().await;
    let rss_now = rss_bytes();
    let rss_delta = rss_now.saturating_sub(rss_baseline);
    let m = db.operational_metrics().await;

    println!("=== aedb ram_probe — shape={label}, seed={seed} ===");
    println!();
    println!("aedb counter (estimated_memory_bytes): {} ({})", counter, human(counter as u64));
    println!("process RSS baseline                 : {} ({})", rss_baseline, human(rss_baseline));
    println!("process RSS now                      : {} ({})", rss_now, human(rss_now));
    println!("process RSS delta                    : {} ({})", rss_delta, human(rss_delta));
    if counter > 0 {
        println!("RSS-delta / counter ratio            : {:.2}x", rss_delta as f64 / counter as f64);
        println!("  (>1 = redundant caches, OrdMap node overhead, version_store, secondary indexes,");
        println!("   accumulator deltas, async projections, allocator fragmentation, etc.)");
    }
    if seed > 0 {
        let counter_per = counter / seed as usize;
        let rss_per = rss_delta / seed as u64;
        println!();
        println!("per-record cost:");
        println!("  counter / record : {} bytes", counter_per);
        println!("  RSS-delta / record: {} bytes", rss_per);
    }
    println!();
    println!("operational metrics:");
    println!("  commits_total       : {}", m.commits_total);
    println!("  avg_commit_us       : {}", m.avg_commit_latency_micros);
    println!("  avg_wal_sync_us     : {}", m.avg_wal_sync_micros);
    println!("  visible_head_seq    : {}", m.visible_head_seq);
    println!("  durable_head_lag    : {}", m.durable_head_lag);
}
