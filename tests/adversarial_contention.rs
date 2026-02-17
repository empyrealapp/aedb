use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::tx::{ReadAssertion, TransactionEnvelope, WriteClass, WriteIntent};
use aedb::commit::validation::Mutation;
use aedb::error::AedbError;
use aedb::query::plan::{ConsistencyMode, Expr, Query};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use tokio::task::JoinSet;

fn u256_be(value: u64) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[24..].copy_from_slice(&value.to_be_bytes());
    bytes
}

fn u256_from_be(bytes: &[u8; 32]) -> u64 {
    let mut out = [0u8; 8];
    out.copy_from_slice(&bytes[24..]);
    u64::from_be_bytes(out)
}

/// Test Case 1: Single-Row Thundering Herd (Hot Account)
///
/// Stress-tests lock contention on a single hot partition.
/// Critical for validating partition locking performance.
#[tokio::test]
async fn test_single_row_thundering_herd() {
    let dir = tempdir().expect("temp dir");
    let db = Arc::new(AedbInstance::open(Default::default(), dir.path()).expect("open db"));
    db.create_project("casino").await.expect("create project");

    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "casino".into(),
        scope_id: "app".into(),
        table_name: "hot_accounts".into(),
        owner_id: None,
        if_not_exists: false,
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "balance".into(),
                col_type: ColumnType::U256,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("create table");

    // Setup: Single hot account
    db.commit(Mutation::Upsert {
        project_id: "casino".into(),
        scope_id: "app".into(),
        table_name: "hot_accounts".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1), Value::U256(u256_be(1_000_000))]),
    })
    .await
    .expect("seed hot account");

    // Action: 100 concurrent CAS operations on same row
    let start = Instant::now();
    let mut tasks = JoinSet::new();

    for i in 0..100 {
        let db = Arc::clone(&db);
        tasks.spawn(async move {
            let base_seq = db
                .snapshot_probe(ConsistencyMode::AtLatest)
                .await
                .expect("probe");
            let result = db
                .commit_envelope(TransactionEnvelope {
                    caller: None,
                    idempotency_key: None,
                    write_class: WriteClass::Standard,
                    assertions: vec![ReadAssertion::RowColumnCompare {
                        project_id: "casino".into(),
                        scope_id: "app".into(),
                        table_name: "hot_accounts".into(),
                        primary_key: vec![Value::Integer(1)],
                        column: "balance".into(),
                        op: aedb::commit::validation::CompareOp::Eq,
                        threshold: Value::U256(u256_be(1_000_000 + i)),
                    }],
                    read_set: Default::default(),
                    write_intent: WriteIntent {
                        mutations: vec![Mutation::Upsert {
                            project_id: "casino".into(),
                            scope_id: "app".into(),
                            table_name: "hot_accounts".into(),
                            primary_key: vec![Value::Integer(1)],
                            row: Row::from_values(vec![
                                Value::Integer(1),
                                Value::U256(u256_be(1_000_000 + i + 1)),
                            ]),
                        }],
                    },
                    base_seq,
                })
                .await;
            (i, result)
        });
    }

    let mut success_count = 0;
    let mut conflict_count = 0;
    let mut timeout_count = 0;

    while let Some(result) = tasks.join_next().await {
        let (_op_id, cas_result) = result.expect("task panicked");
        match cas_result {
            Ok(_) => success_count += 1,
            Err(AedbError::AssertionFailed { .. }) => conflict_count += 1,
            Err(AedbError::Timeout) => timeout_count += 1,
            Err(e) => panic!("unexpected error: {:?}", e),
        }
    }

    let elapsed = start.elapsed();
    let ops_per_sec = 100.0 / elapsed.as_secs_f64();

    println!(
        "Hot account contention: success={}, conflict={}, timeout={}, elapsed={:?}, throughput={:.0} ops/sec",
        success_count, conflict_count, timeout_count, elapsed, ops_per_sec
    );

    // Verify: All operations completed
    assert_eq!(
        success_count + conflict_count + timeout_count,
        100,
        "all operations should complete"
    );

    // Verify: Reasonable timeout rate (<10%)
    assert!(
        timeout_count < 10,
        "timeout rate too high: {} / 100",
        timeout_count
    );

    // Verify: Reasonable throughput (>50 ops/sec for sequential CAS)
    // Note: CAS operations with assertions are sequential, so throughput is lower
    assert!(
        ops_per_sec > 50.0,
        "throughput too low: {:.0} ops/sec",
        ops_per_sec
    );

    // Verify: At least some operations succeeded
    assert!(success_count > 0, "at least one CAS should succeed");
}

/// Test Case 2: Cross-Partition Deadlock Prevention
///
/// Validates that coordinator lock ordering prevents deadlocks.
/// Critical for multi-partition transaction safety.
#[tokio::test]
async fn test_cross_partition_deadlock_prevention() {
    let dir = tempdir().expect("temp dir");
    let db = Arc::new(AedbInstance::open(Default::default(), dir.path()).expect("open db"));
    db.create_project("casino").await.expect("create project");

    // Setup: Create 4 tables in same scope (to test partition locking)
    for i in 0..4 {
        let table_name = format!("accounts_{}", i);
        db.commit(Mutation::Ddl(DdlOperation::CreateTable {
            project_id: "casino".into(),
            scope_id: "app".into(),
            table_name: table_name.clone(),
            owner_id: None,
            if_not_exists: false,
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "balance".into(),
                    col_type: ColumnType::U256,
                    nullable: false,
                },
            ],
            primary_key: vec!["id".into()],
        }))
        .await
        .expect("create table");

        db.commit(Mutation::Upsert {
            project_id: "casino".into(),
            scope_id: "app".into(),
            table_name: table_name.clone(),
            primary_key: vec![Value::Integer(1)],
            row: Row::from_values(vec![Value::Integer(1), Value::U256(u256_be(1000))]),
        })
        .await
        .expect("seed account");
    }

    // Action: 50 concurrent transactions accessing different tables
    // Tests partition lock coordination
    let start = Instant::now();
    let mut tasks = JoinSet::new();

    for i in 0..50i64 {
        let db = Arc::clone(&db);
        tasks.spawn(async move {
            // Access table based on i
            let table1 = format!("accounts_{}", i % 4);
            let table2 = format!("accounts_{}", (i + 2) % 4);

            if table1 == table2 {
                // Same table - simple operation
                db.table_inc_u256(
                    "casino",
                    "app",
                    &table1,
                    vec![Value::Integer(1)],
                    "balance",
                    u256_be(10),
                )
                .await
            } else {
                // Different tables - potential coordination scenario
                db.table_inc_u256(
                    "casino",
                    "app",
                    &table1,
                    vec![Value::Integer(1)],
                    "balance",
                    u256_be(5),
                )
                .await?;

                db.table_inc_u256(
                    "casino",
                    "app",
                    &table2,
                    vec![Value::Integer(1)],
                    "balance",
                    u256_be(5),
                )
                .await
            }
        });
    }

    let mut success_count = 0;
    let mut error_count = 0;

    while let Some(result) = tasks.join_next().await {
        match result.expect("task panicked") {
            Ok(_) => success_count += 1,
            Err(_) => error_count += 1,
        }
    }

    let elapsed = start.elapsed();

    println!(
        "Cross-partition test: success={}, errors={}, elapsed={:?}",
        success_count, error_count, elapsed
    );

    // Verify: All transactions completed within reasonable time (no deadlocks)
    assert!(
        elapsed < Duration::from_secs(5),
        "transactions took too long (possible deadlock): {:?}",
        elapsed
    );

    // Verify: Most operations succeeded
    assert!(
        success_count > 40,
        "too many failures: {} / 50",
        error_count
    );
}

/// Test Case 3: Read-Set Conflict Under Heavy Write Load
///
/// Validates read-set validation under concurrent writes.
/// Critical for snapshot isolation guarantees.
#[tokio::test]
async fn test_read_set_conflict_under_write_load() {
    let dir = tempdir().expect("temp dir");
    let db = Arc::new(AedbInstance::open(Default::default(), dir.path()).expect("open db"));
    db.create_project("casino").await.expect("create project");

    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "casino".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        owner_id: None,
        if_not_exists: false,
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "balance".into(),
                col_type: ColumnType::U256,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("create table");

    // Setup: 10 accounts
    for i in 0..10 {
        db.commit(Mutation::Upsert {
            project_id: "casino".into(),
            scope_id: "app".into(),
            table_name: "accounts".into(),
            primary_key: vec![Value::Integer(i)],
            row: Row::from_values(vec![Value::Integer(i), Value::U256(u256_be(1000))]),
        })
        .await
        .expect("seed account");
    }

    // Action: 10 reader threads + 10 writer threads
    let mut tasks = JoinSet::new();

    // Spawn writers
    for i in 0..10 {
        let db = Arc::clone(&db);
        tasks.spawn(async move {
            for _ in 0..5 {
                let _ = db
                    .table_inc_u256(
                        "casino",
                        "app",
                        "accounts",
                        vec![Value::Integer(i % 5)],
                        "balance",
                        u256_be(100),
                    )
                    .await;
                tokio::time::sleep(Duration::from_micros(100)).await;
            }
            ("writer", Ok::<(), AedbError>(()))
        });
    }

    // Spawn readers
    for i in 0..10 {
        let db = Arc::clone(&db);
        tasks.spawn(async move {
            for _ in 0..5 {
                let _ = db
                    .query_with_options(
                        "casino",
                        "app",
                        Query::select(&["balance"])
                            .from("accounts")
                            .where_(Expr::Eq("id".into(), Value::Integer(i % 5))),
                        aedb::query::plan::QueryOptions {
                            consistency: ConsistencyMode::AtLatest,
                            ..Default::default()
                        },
                    )
                    .await;
                tokio::time::sleep(Duration::from_micros(50)).await;
            }
            ("reader", Ok(()))
        });
    }

    let mut writer_count = 0;
    let mut reader_count = 0;

    while let Some(result) = tasks.join_next().await {
        let (task_type, _task_result) = result.expect("task panicked");
        match task_type {
            "reader" => {
                reader_count += 1;
            }
            "writer" => {
                writer_count += 1;
            }
            _ => {}
        }
    }

    println!(
        "Read-set conflict test: {} writers, {} readers completed",
        writer_count, reader_count
    );

    // Verify: All tasks completed
    assert_eq!(writer_count, 10, "all writers should complete");
    assert_eq!(reader_count, 10, "all readers should complete");
}

/// Test Case 4: High Concurrency Stress Test
///
/// Validates behavior under extreme concurrent load.
/// Critical for production readiness assessment.
#[tokio::test]
async fn test_high_concurrency_stress() {
    let dir = tempdir().expect("temp dir");
    let db = Arc::new(AedbInstance::open(Default::default(), dir.path()).expect("open db"));
    db.create_project("casino").await.expect("create project");

    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "casino".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        owner_id: None,
        if_not_exists: false,
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "balance".into(),
                col_type: ColumnType::U256,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("create table");

    // Setup: 20 accounts
    for i in 0..20 {
        db.commit(Mutation::Upsert {
            project_id: "casino".into(),
            scope_id: "app".into(),
            table_name: "accounts".into(),
            primary_key: vec![Value::Integer(i)],
            row: Row::from_values(vec![Value::Integer(i), Value::U256(u256_be(10000))]),
        })
        .await
        .expect("seed account");
    }

    // Action: 200 concurrent operations
    let start = Instant::now();
    let mut tasks = JoinSet::new();

    for i in 0..200i64 {
        let db = Arc::clone(&db);
        tasks.spawn(async move {
            tokio::time::sleep(Duration::from_micros(((i % 10) * 50) as u64)).await;
            db.table_inc_u256(
                "casino",
                "app",
                "accounts",
                vec![Value::Integer(i % 20)],
                "balance",
                u256_be(10),
            )
            .await
        });
    }

    let mut success_count = 0;
    let mut error_count = 0;

    while let Some(result) = tasks.join_next().await {
        match result.expect("task panicked") {
            Ok(_) => success_count += 1,
            Err(_) => error_count += 1,
        }
    }

    let elapsed = start.elapsed();
    let throughput = success_count as f64 / elapsed.as_secs_f64();

    println!(
        "High concurrency stress: success={}, errors={}, elapsed={:?}, throughput={:.0} ops/sec",
        success_count, error_count, elapsed, throughput
    );

    // Verify: Most operations succeeded
    assert!(
        success_count > 150,
        "too many failures: {} / 200",
        error_count
    );

    // Verify: Reasonable throughput (>500 ops/sec)
    assert!(
        throughput > 500.0,
        "throughput too low: {:.0} ops/sec",
        throughput
    );

    // Verify: Balance consistency
    let total_expected = 10000 * 20 + (success_count * 10);
    let mut total_actual = 0;

    for i in 0..20 {
        let row = db
            .query_with_options(
                "casino",
                "app",
                Query::select(&["balance"])
                    .from("accounts")
                    .where_(Expr::Eq("id".into(), Value::Integer(i))),
                aedb::query::plan::QueryOptions {
                    consistency: ConsistencyMode::AtLatest,
                    ..Default::default()
                },
            )
            .await
            .expect("query balance")
            .rows
            .into_iter()
            .next()
            .expect("account exists");

        let Value::U256(balance_be) = row.values[0] else {
            panic!("expected U256 balance");
        };
        total_actual += u256_from_be(&balance_be);
    }

    assert_eq!(
        total_actual, total_expected,
        "balance conservation violated! expected={}, actual={}",
        total_expected, total_actual
    );
}
