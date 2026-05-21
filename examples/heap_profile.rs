//! Heap profile of aedb under sustained insert + query load.
//!
//! Two phases:
//! - **Seed**: batch-load N rows (alloc traffic discounted — dhat starts after).
//! - **Measure**: M single-row upserts under dhat. This is the window we care about.
//!
//!   cargo run --release --example heap_profile -- [seed] [measure]
//!
//! Defaults: seed=10_000, measure=10_000. Output: `dhat-heap.json`.

use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::validation::Mutation;
use aedb::config::AedbConfig;
use aedb::query::plan::{Query, QueryOptions, col, lit};
use tempfile::tempdir;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const PROJECT: &str = "p";
const SCOPE: &str = "app";
const TABLE: &str = "users";

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let mut args = std::env::args().skip(1);
    let seed: i64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(10_000);
    let measure: i64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(10_000);

    let dir = tempdir().expect("tempdir");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project(PROJECT).await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: PROJECT.into(),
        scope_id: SCOPE.into(),
        table_name: TABLE.into(),
        owner_id: None,
        if_not_exists: false,
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
    .expect("create table");

    // Seed via batch (fast, untracked).
    let batch = 256_i64;
    let mut id = 0_i64;
    while id < seed {
        let n = batch.min(seed - id);
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
            table_name: TABLE.into(),
            rows,
        })
        .await
        .expect("seed batch");
    }

    let pre = db.operational_metrics().await;
    eprintln!(
        "seeded={seed} pre_commits={} pre_visible_seq={}",
        pre.commits_total, pre.visible_head_seq
    );

    // dhat profiler captures only the measurement window.
    let _profiler = dhat::Profiler::new_heap();
    for i in 0..measure {
        let pk = seed + i;
        db.commit(Mutation::Upsert {
            project_id: PROJECT.into(),
            scope_id: SCOPE.into(),
            table_name: TABLE.into(),
            primary_key: vec![Value::Integer(pk)],
            row: Row {
                values: vec![
                    Value::Integer(pk),
                    Value::Text("measured".into()),
                    Value::Integer(33),
                ],
            },
        })
        .await
        .expect("measured upsert");

        if i % 1_000 == 0 {
            let _ = db
                .query_no_auth(
                    PROJECT,
                    SCOPE,
                    Query::select(&["id", "name"])
                        .from(TABLE)
                        .where_(col("id").eq(lit(pk)))
                        .limit(1),
                    QueryOptions::default(),
                )
                .await
                .expect("point query");
        }
    }

    let m = db.operational_metrics().await;
    eprintln!(
        "seed={seed} measure={measure} commits_total={} avg_commit_us={} avg_wal_sync_us={} queue_depth={} durable_lag={}",
        m.commits_total - pre.commits_total,
        m.avg_commit_latency_micros,
        m.avg_wal_sync_micros,
        m.queue_depth,
        m.durable_head_lag,
    );
}
