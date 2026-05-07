//! CPU flamegraph via pprof-rs (signal-based, no perf permissions needed).
//!
//!   cargo run --release --example cpu_profile -- [seed] [measure]
//!
//! Output: `flamegraph.svg` in cwd. Open in browser.

use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::validation::Mutation;
use aedb::config::AedbConfig;
use std::fs::File;
use tempfile::tempdir;

const PROJECT: &str = "p";
const SCOPE: &str = "app";
const TABLE: &str = "users";

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let mut args = std::env::args().skip(1);
    let seed: i64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(500_000);
    let measure: i64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(2_000);

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

    eprintln!("seeded={seed}; starting profiler for {measure} commits");

    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(999)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .expect("profiler");

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
    }

    let report = guard.report().build().expect("report");
    let file = File::create("flamegraph.svg").expect("create svg");
    report.flamegraph(file).expect("write svg");

    let m = db.operational_metrics().await;
    eprintln!(
        "measure={measure} avg_commit_us={} avg_wal_sync_us={} -> flamegraph.svg",
        m.avg_commit_latency_micros, m.avg_wal_sync_micros,
    );
}
