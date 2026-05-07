//! Scaling sweeps: how do upsert/KV/index costs change with size?
//!
//!   cargo bench --bench scale -- --save-baseline scale_main

use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::{ColumnDef, IndexType};
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::validation::Mutation;
use aedb::config::AedbConfig;
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use tempfile::tempdir;
use tokio::runtime::Runtime;

const PROJECT_ID: &str = "p";
const SCOPE_ID: &str = "app";
const TABLE_NAME: &str = "users";

async fn setup_table(db: &AedbInstance) {
    db.create_project(PROJECT_ID).await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: PROJECT_ID.into(),
        scope_id: SCOPE_ID.into(),
        table_name: TABLE_NAME.into(),
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
}

async fn seed_rows(db: &AedbInstance, count: i64, batch: i64) {
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
            project_id: PROJECT_ID.into(),
            scope_id: SCOPE_ID.into(),
            table_name: TABLE_NAME.into(),
            rows,
        })
        .await
        .expect("seed batch");
    }
}

/// How does single-row upsert latency change as the table grows?
/// Tests `im::OrdMap` log-N clone cost on the primary index.
fn bench_upsert_at_table_size(c: &mut Criterion) {
    let rt = Runtime::new().expect("rt");
    for &size in &[10_000_i64, 100_000, 500_000] {
        let dir = tempdir().expect("tempdir");
        let db = rt.block_on(async {
            AedbInstance::open(AedbConfig::default(), dir.path()).expect("open")
        });
        rt.block_on(async {
            setup_table(&db).await;
            seed_rows(&db, size, 256).await;
        });

        let mut next_id = size;
        c.bench_function(&format!("upsert_at_table_size_{size}"), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let id = black_box(next_id);
                    next_id += 1;
                    db.commit(Mutation::Upsert {
                        project_id: PROJECT_ID.into(),
                        scope_id: SCOPE_ID.into(),
                        table_name: TABLE_NAME.into(),
                        primary_key: vec![Value::Integer(id)],
                        row: Row {
                            values: vec![
                                Value::Integer(id),
                                Value::Text("scale-bench".into()),
                                Value::Integer(30),
                            ],
                        },
                    })
                    .await
                    .expect("upsert");
                });
            })
        });
        // tempdir dropped at end of loop iter
        drop(db);
        drop(dir);
    }
}

/// How does KV set/get latency change with value size?
fn bench_kv_value_size(c: &mut Criterion) {
    let rt = Runtime::new().expect("rt");
    for &vsize in &[64_usize, 1024, 16_384, 131_072] {
        let dir = tempdir().expect("tempdir");
        let db = rt.block_on(async {
            AedbInstance::open(AedbConfig::default(), dir.path()).expect("open")
        });
        rt.block_on(async {
            db.create_project(PROJECT_ID).await.expect("project");
            db.create_scope(PROJECT_ID, SCOPE_ID).await.expect("scope");
        });

        let payload = vec![0xABu8; vsize];
        let mut next_idx = 0_u64;
        c.bench_function(&format!("kv_set_value_{vsize}b"), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let i = black_box(next_idx);
                    next_idx += 1;
                    let key = format!("k:{i:08}").into_bytes();
                    db.commit(Mutation::KvSet {
                        project_id: PROJECT_ID.into(),
                        scope_id: SCOPE_ID.into(),
                        key,
                        value: payload.clone(),
                    })
                    .await
                    .expect("kv set");
                });
            })
        });
        drop(db);
        drop(dir);
    }
}

/// How does upsert cost scale with the number of secondary indexes?
fn bench_upsert_with_n_indexes(c: &mut Criterion) {
    let rt = Runtime::new().expect("rt");
    for &nidx in &[0_usize, 1, 4, 8] {
        let dir = tempdir().expect("tempdir");
        let db = rt.block_on(async {
            AedbInstance::open(AedbConfig::default(), dir.path()).expect("open")
        });
        rt.block_on(async {
            db.create_project(PROJECT_ID).await.expect("project");
            db.commit(Mutation::Ddl(DdlOperation::CreateTable {
                project_id: PROJECT_ID.into(),
                scope_id: SCOPE_ID.into(),
                table_name: TABLE_NAME.into(),
                owner_id: None,
                if_not_exists: false,
                columns: {
                    let mut cols = vec![ColumnDef {
                        name: "id".into(),
                        col_type: ColumnType::Integer,
                        nullable: false,
                    }];
                    for i in 0..8 {
                        cols.push(ColumnDef {
                            name: format!("c{i}"),
                            col_type: ColumnType::Integer,
                            nullable: false,
                        });
                    }
                    cols
                },
                primary_key: vec!["id".into()],
            }))
            .await
            .expect("create table");

            for i in 0..nidx {
                db.commit(Mutation::Ddl(DdlOperation::CreateIndex {
                    project_id: PROJECT_ID.into(),
                    scope_id: SCOPE_ID.into(),
                    table_name: TABLE_NAME.into(),
                    index_name: format!("idx_c{i}"),
                    columns: vec![format!("c{i}")],
                    index_type: IndexType::BTree,
                    partial_filter: None,
                    if_not_exists: false,
                }))
                .await
                .expect("create index");
            }
        });

        let mut next_id = 0_i64;
        c.bench_function(&format!("upsert_with_{nidx}_indexes"), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let id = black_box(next_id);
                    next_id += 1;
                    let mut values = vec![Value::Integer(id)];
                    for i in 0..8 {
                        values.push(Value::Integer((id + i as i64) % 1024));
                    }
                    db.commit(Mutation::Upsert {
                        project_id: PROJECT_ID.into(),
                        scope_id: SCOPE_ID.into(),
                        table_name: TABLE_NAME.into(),
                        primary_key: vec![Value::Integer(id)],
                        row: Row { values },
                    })
                    .await
                    .expect("upsert");
                });
            })
        });
        drop(db);
        drop(dir);
    }
}

criterion_group!(
    scale,
    bench_upsert_at_table_size,
    bench_kv_value_size,
    bench_upsert_with_n_indexes
);
criterion_main!(scale);
