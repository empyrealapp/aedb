use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::validation::Mutation;
use aedb::config::{AedbConfig, DurabilityMode, RecoveryMode};
use aedb::query::plan::{ConsistencyMode, Order, Query, QueryOptions, col, lit};
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use tempfile::tempdir;
use tokio::runtime::Runtime;

const PROJECT_ID: &str = "p";
const SCOPE_ID: &str = "app";
const TABLE_NAME: &str = "users";
const SEEDED_ROWS: i64 = 10_000;
const BATCH_INSERT_ROWS: i64 = 64;

async fn setup_db(config: AedbConfig, seed_rows: i64) -> (tempfile::TempDir, AedbInstance) {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(config, dir.path()).expect("open");
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
    .expect("table");
    db.commit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: PROJECT_ID.into(),
        scope_id: SCOPE_ID.into(),
        table_name: TABLE_NAME.into(),
        index_name: "by_age".into(),
        if_not_exists: false,
        columns: vec!["age".into()],
        index_type: aedb::catalog::schema::IndexType::BTree,
        partial_filter: None,
    }))
    .await
    .expect("index");

    for id in 1..=seed_rows {
        db.commit(Mutation::Upsert {
            project_id: PROJECT_ID.into(),
            scope_id: SCOPE_ID.into(),
            table_name: TABLE_NAME.into(),
            primary_key: vec![Value::Integer(id)],
            row: Row {
                values: vec![
                    Value::Integer(id),
                    Value::Text(format!("user-{id}").into()),
                    Value::Integer(18 + (id % 50)),
                ],
            },
        })
        .await
        .expect("seed row");
        db.commit(Mutation::KvSet {
            project_id: PROJECT_ID.into(),
            scope_id: SCOPE_ID.into(),
            key: format!("bank:user:{id}:balance").into_bytes(),
            value: vec![0u8; 32],
        })
        .await
        .expect("seed kv");
    }

    (dir, db)
}

fn bench_aedb_hot_paths(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");

    let (_seed_dir, seed_db) = rt.block_on(setup_db(AedbConfig::default(), SEEDED_ROWS));

    let mut next_upsert_id = 1_i64;
    c.bench_function("hot_upsert_single_row", |b| {
        b.iter(|| {
            rt.block_on(async {
                let id = black_box(next_upsert_id);
                next_upsert_id += 1;
                if next_upsert_id > SEEDED_ROWS {
                    next_upsert_id = 1;
                }
                seed_db
                    .commit(Mutation::Upsert {
                        project_id: PROJECT_ID.into(),
                        scope_id: SCOPE_ID.into(),
                        table_name: TABLE_NAME.into(),
                        primary_key: vec![Value::Integer(id)],
                        row: Row {
                            values: vec![
                                Value::Integer(id),
                                Value::Text("bench-user".into()),
                                Value::Integer(25),
                            ],
                        },
                    })
                    .await
                    .expect("upsert");
            });
        })
    });

    let mut next_multi_commit_base = 1_i64;
    c.bench_function("insert_64_rows_as_64_commits", |b| {
        b.iter(|| {
            rt.block_on(async {
                let base = black_box(next_multi_commit_base);
                next_multi_commit_base += BATCH_INSERT_ROWS;
                if next_multi_commit_base > SEEDED_ROWS {
                    next_multi_commit_base = 1;
                }
                for offset in 0..BATCH_INSERT_ROWS {
                    let id = ((base + offset - 1) % SEEDED_ROWS) + 1;
                    seed_db
                        .commit(Mutation::Upsert {
                            project_id: PROJECT_ID.into(),
                            scope_id: SCOPE_ID.into(),
                            table_name: TABLE_NAME.into(),
                            primary_key: vec![Value::Integer(id)],
                            row: Row {
                                values: vec![
                                    Value::Integer(id),
                                    Value::Text(format!("bench-bulk-{id}").into()),
                                    Value::Integer(30),
                                ],
                            },
                        })
                        .await
                        .expect("multi-commit upsert");
                }
            });
        })
    });

    let mut next_batch_commit_base = 1_i64;
    c.bench_function("insert_64_rows_as_1_batch_commit", |b| {
        b.iter(|| {
            rt.block_on(async {
                let base = black_box(next_batch_commit_base);
                next_batch_commit_base += BATCH_INSERT_ROWS;
                if next_batch_commit_base > SEEDED_ROWS {
                    next_batch_commit_base = 1;
                }
                let mut rows = Vec::with_capacity(BATCH_INSERT_ROWS as usize);
                for offset in 0..BATCH_INSERT_ROWS {
                    let id = ((base + offset - 1) % SEEDED_ROWS) + 1;
                    rows.push(Row {
                        values: vec![
                            Value::Integer(id),
                            Value::Text(format!("bench-bulk-{id}").into()),
                            Value::Integer(30),
                        ],
                    });
                }
                seed_db
                    .commit(Mutation::UpsertBatch {
                        project_id: PROJECT_ID.into(),
                        scope_id: SCOPE_ID.into(),
                        table_name: TABLE_NAME.into(),
                        rows,
                    })
                    .await
                    .expect("batch upsert");
            });
        })
    });

    let mut next_query_id = 1;
    c.bench_function("point_query_by_primary_key", |b| {
        b.iter(|| {
            rt.block_on(async {
                let id = black_box(next_query_id);
                next_query_id += 1;
                if next_query_id > SEEDED_ROWS {
                    next_query_id = 1;
                }
                let _ = seed_db
                    .query(
                        PROJECT_ID,
                        SCOPE_ID,
                        Query::select(&["id", "name"])
                            .from(TABLE_NAME)
                            .where_(col("id").eq(lit(id)))
                            .limit(1),
                    )
                    .await
                    .expect("point query");
            });
        })
    });

    c.bench_function("index_equality_by_age", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = seed_db
                    .query(
                        PROJECT_ID,
                        SCOPE_ID,
                        Query::select(&["id", "name"])
                            .from(TABLE_NAME)
                            .where_(col("age").eq(lit(25_i64)))
                            .limit(20),
                    )
                    .await
                    .expect("index query");
            });
        })
    });

    let caller = aedb::permission::CallerContext::new("bench");
    rt.block_on(async {
        seed_db
            .commit(Mutation::Ddl(DdlOperation::GrantPermission {
                caller_id: "bench".into(),
                permission: aedb::permission::Permission::KvRead {
                    project_id: PROJECT_ID.into(),
                    scope_id: Some(SCOPE_ID.into()),
                    prefix: None,
                },
                actor_id: None,
                delegable: false,
            }))
            .await
            .expect("grant");
    });
    c.bench_function("kv_point_get_balance", |b| {
        let mut next_kv_id = 1_i64;
        b.iter(|| {
            rt.block_on(async {
                let id = black_box(next_kv_id);
                next_kv_id += 1;
                if next_kv_id > SEEDED_ROWS {
                    next_kv_id = 1;
                }
                let key = format!("bank:user:{id}:balance");
                let _ = seed_db
                    .kv_get(
                        PROJECT_ID,
                        SCOPE_ID,
                        key.as_bytes(),
                        ConsistencyMode::AtLatest,
                        &caller,
                    )
                    .await
                    .expect("kv get");
            });
        })
    });

    c.bench_function("kv_prefix_scan_10", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = seed_db
                    .kv_scan_prefix(
                        PROJECT_ID,
                        SCOPE_ID,
                        b"bank:user:",
                        10,
                        None,
                        ConsistencyMode::AtLatest,
                        &caller,
                    )
                    .await
                    .expect("kv scan");
            });
        })
    });

    c.bench_function("scan_limit_100", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = seed_db
                    .query(
                        PROJECT_ID,
                        SCOPE_ID,
                        Query::select(&["id", "name"]).from(TABLE_NAME).limit(100),
                    )
                    .await
                    .expect("scan query");
            });
        })
    });

    c.bench_function("order_by_age_limit_100", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = seed_db
                    .query(
                        PROJECT_ID,
                        SCOPE_ID,
                        Query::select(&["id", "age"])
                            .from(TABLE_NAME)
                            .order_by("age", Order::Asc)
                            .limit(100),
                    )
                    .await
                    .expect("ordered query");
            });
        })
    });

    rt.block_on(async {
        seed_db
            .commit(Mutation::Ddl(DdlOperation::CreateTable {
                project_id: PROJECT_ID.into(),
                scope_id: SCOPE_ID.into(),
                table_name: "profiles".into(),
                owner_id: None,
                if_not_exists: false,
                columns: vec![
                    ColumnDef {
                        name: "user_id".into(),
                        col_type: ColumnType::Integer,
                        nullable: false,
                    },
                    ColumnDef {
                        name: "country".into(),
                        col_type: ColumnType::Text,
                        nullable: false,
                    },
                ],
                primary_key: vec!["user_id".into()],
            }))
            .await
            .expect("profiles table");
        for i in 1..=SEEDED_ROWS {
            seed_db
                .commit(Mutation::Upsert {
                    project_id: PROJECT_ID.into(),
                    scope_id: SCOPE_ID.into(),
                    table_name: "profiles".into(),
                    primary_key: vec![Value::Integer(i)],
                    row: Row {
                        values: vec![Value::Integer(i), Value::Text("US".into())],
                    },
                })
                .await
                .expect("seed profile");
        }
    });

    c.bench_function("inner_join_users_profiles_limit_100", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = seed_db
                    .query_with_options(
                        PROJECT_ID,
                        SCOPE_ID,
                        Query::select(&["u.id", "p.country"])
                            .from(TABLE_NAME)
                            .alias("u")
                            .inner_join("profiles", "u.id", "user_id")
                            .with_last_join_alias("p")
                            .order_by("u.id", Order::Asc)
                            .limit(100),
                        QueryOptions {
                            allow_full_scan: true,
                            ..QueryOptions::default()
                        },
                    )
                    .await
                    .expect("join query");
            });
        })
    });

    c.bench_function("join_cursor_page_100", |b| {
        b.iter(|| {
            rt.block_on(async {
                let first = seed_db
                    .query_with_options(
                        PROJECT_ID,
                        SCOPE_ID,
                        Query::select(&["u.id", "p.country"])
                            .from(TABLE_NAME)
                            .alias("u")
                            .inner_join("profiles", "u.id", "user_id")
                            .with_last_join_alias("p")
                            .order_by("u.id", Order::Asc)
                            .limit(100),
                        QueryOptions {
                            allow_full_scan: true,
                            ..QueryOptions::default()
                        },
                    )
                    .await
                    .expect("first page");
                let _ = seed_db
                    .query_with_options(
                        PROJECT_ID,
                        SCOPE_ID,
                        Query::select(&["u.id", "p.country"])
                            .from(TABLE_NAME)
                            .alias("u")
                            .inner_join("profiles", "u.id", "user_id")
                            .with_last_join_alias("p")
                            .order_by("u.id", Order::Asc)
                            .limit(100),
                        QueryOptions {
                            cursor: first.cursor,
                            allow_full_scan: true,
                            ..QueryOptions::default()
                        },
                    )
                    .await
                    .expect("second page");
            });
        })
    });

    let durability_modes = [
        ("full", DurabilityMode::Full),
        ("batch", DurabilityMode::Batch),
        ("os_buffered", DurabilityMode::OsBuffered),
    ];
    for (mode_name, mode) in durability_modes {
        let mut config = AedbConfig {
            durability_mode: mode,
            ..AedbConfig::default()
        };
        if mode == DurabilityMode::OsBuffered {
            config.recovery_mode = RecoveryMode::Permissive;
        }
        let (_dir, db) = rt.block_on(setup_db(config, SEEDED_ROWS));
        let mut next_id = 1_i64;
        c.bench_function(&format!("hot_upsert_single_row_{mode_name}"), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let id = black_box(next_id);
                    next_id += 1;
                    if next_id > SEEDED_ROWS {
                        next_id = 1;
                    }
                    db.commit(Mutation::Upsert {
                        project_id: PROJECT_ID.into(),
                        scope_id: SCOPE_ID.into(),
                        table_name: TABLE_NAME.into(),
                        primary_key: vec![Value::Integer(id)],
                        row: Row {
                            values: vec![
                                Value::Integer(id),
                                Value::Text("bench-user".into()),
                                Value::Integer(25),
                            ],
                        },
                    })
                    .await
                    .expect("upsert");
                });
            })
        });
    }
}

fn bench_end_to_end_bootstrap(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    c.bench_function("e2e_bootstrap_commit_and_query", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (_dir, db) = setup_db(AedbConfig::default(), 0).await;
                db.commit(Mutation::Upsert {
                    project_id: PROJECT_ID.into(),
                    scope_id: SCOPE_ID.into(),
                    table_name: TABLE_NAME.into(),
                    primary_key: vec![Value::Integer(1)],
                    row: Row {
                        values: vec![
                            Value::Integer(1),
                            Value::Text("alice".into()),
                            Value::Integer(25),
                        ],
                    },
                })
                .await
                .expect("upsert");
                let _ = db
                    .query(
                        PROJECT_ID,
                        SCOPE_ID,
                        Query::select(&["id", "name"])
                            .from(TABLE_NAME)
                            .where_(col("id").eq(lit(1_i64)))
                            .limit(1),
                    )
                    .await
                    .expect("query");
            });
        })
    });
}

criterion_group!(benches, bench_aedb_hot_paths, bench_end_to_end_bootstrap);
criterion_main!(benches);
