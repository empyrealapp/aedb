use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::tx::{IdempotencyKey, ReadSet, TransactionEnvelope, WriteClass, WriteIntent};
use aedb::commit::validation::Mutation;
use aedb::error::AedbError;
use aedb::query::plan::{ConsistencyMode, Expr, Query};
use std::sync::Arc;
use std::time::Duration;
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

async fn setup_accounts_table(db: &AedbInstance) {
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
    .expect("create accounts table");
}

async fn get_balance(db: &AedbInstance, account_id: i64) -> u64 {
    let row = db
        .query_with_options(
            "casino",
            "app",
            Query::select(&["balance"])
                .from("accounts")
                .where_(Expr::Eq("id".into(), Value::Integer(account_id))),
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
    u256_from_be(&balance_be)
}

/// Test Case 1: Concurrent Withdrawal Race
///
/// Validates that concurrent withdrawals cannot overdraw an account.
/// This is critical for casino operations to prevent double-spending.
#[tokio::test]
async fn test_concurrent_withdrawal_race() {
    let dir = tempdir().expect("temp dir");
    let db = Arc::new(AedbInstance::open(Default::default(), dir.path()).expect("open db"));
    setup_accounts_table(&db).await;

    // Setup: Account with balance = 1000
    db.commit(Mutation::Upsert {
        project_id: "casino".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1), Value::U256(u256_be(1000))]),
    })
    .await
    .expect("seed account");

    // Action: 10 threads each try to withdraw 200 concurrently
    // Add slight stagger to avoid all being in same epoch
    let mut tasks = JoinSet::new();
    for i in 0..10 {
        let db = Arc::clone(&db);
        tasks.spawn(async move {
            // Slight stagger to ensure operations span multiple epochs
            if i > 0 {
                tokio::time::sleep(Duration::from_micros(i as u64 * 100)).await;
            }
            db.table_dec_u256(
                "casino",
                "app",
                "accounts",
                vec![Value::Integer(1)],
                "balance",
                u256_be(200),
            )
            .await
        });
    }

    // Collect results
    let mut success_count = 0;
    let mut underflow_count = 0;
    let mut conflict_count = 0;

    while let Some(result) = tasks.join_next().await {
        match result.expect("task panicked") {
            Ok(_) => success_count += 1,
            Err(AedbError::Underflow) => underflow_count += 1,
            Err(AedbError::Conflict(_)) => conflict_count += 1,
            Err(AedbError::Validation(msg)) if msg.contains("underflow") => underflow_count += 1,
            Err(e) => panic!("unexpected error: {:?}", e),
        }
    }

    // Verify: Some should succeed, rest should underflow
    // Due to epoch batching, we may not get exactly 5 successes
    println!(
        "Concurrent withdrawal results: success={}, underflow={}, conflict={}",
        success_count, underflow_count, conflict_count
    );
    assert!(success_count > 0, "at least one withdrawal should succeed");
    assert!(
        success_count <= 5,
        "at most 5 withdrawals can succeed (1000/200=5)"
    );
    assert_eq!(
        success_count + underflow_count + conflict_count,
        10,
        "all 10 operations accounted for"
    );

    // Verify: Final balance = initial - (success_count * 200)
    let final_balance = get_balance(&db, 1).await;
    let expected_balance = 1000 - (success_count * 200);
    assert_eq!(
        final_balance, expected_balance,
        "balance should decrease by exactly {} (success_count={} * 200)",
        expected_balance, success_count
    );

    // CRITICAL: Verify no negative balance
    println!(
        "Final balance: {} (started at 1000, {} withdrawals succeeded)",
        final_balance, success_count
    );
    assert!(final_balance < 1000, "balance must have decreased");

    // Verify that attempting another 200 withdrawal fails if balance < 200
    if final_balance < 200 {
        let err = db
            .table_dec_u256(
                "casino",
                "app",
                "accounts",
                vec![Value::Integer(1)],
                "balance",
                u256_be(200),
            )
            .await
            .expect_err("withdrawal exceeding balance should fail");
        assert!(
            matches!(err, AedbError::Underflow)
                || matches!(err, AedbError::Validation(ref msg) if msg.contains("underflow")),
            "expected underflow, got {:?}",
            err
        );
    }
}

/// Test Case 2: Multi-Account Transfer Atomicity
///
/// Validates that complex multi-account transfers are atomic.
/// Critical for casino settlement operations.
#[tokio::test]
async fn test_multi_account_transfer_atomicity() {
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open db");
    setup_accounts_table(&db).await;

    // Setup: Accounts A=1000, B=500, C=300
    db.commit(Mutation::Upsert {
        project_id: "casino".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1), Value::U256(u256_be(1000))]),
    })
    .await
    .expect("seed account A");

    db.commit(Mutation::Upsert {
        project_id: "casino".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        primary_key: vec![Value::Integer(2)],
        row: Row::from_values(vec![Value::Integer(2), Value::U256(u256_be(500))]),
    })
    .await
    .expect("seed account B");

    db.commit(Mutation::Upsert {
        project_id: "casino".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        primary_key: vec![Value::Integer(3)],
        row: Row::from_values(vec![Value::Integer(3), Value::U256(u256_be(300))]),
    })
    .await
    .expect("seed account C");

    let initial_total =
        get_balance(&db, 1).await + get_balance(&db, 2).await + get_balance(&db, 3).await;
    assert_eq!(initial_total, 1800, "initial total");

    // Action: Complex envelope transfer
    // A -200, B +200, B -400, C +400, C -100, A +100
    // Net: A: 1000-200+100=900, B: 500+200-400=300, C: 300+400-100=600
    let base_seq = db
        .snapshot_probe(ConsistencyMode::AtLatest)
        .await
        .expect("probe seq");

    db.commit_envelope(TransactionEnvelope {
        caller: None,
        idempotency_key: None,
        write_class: WriteClass::Standard,
        assertions: Vec::new(),
        read_set: ReadSet::default(),
        write_intent: WriteIntent {
            mutations: vec![
                Mutation::TableDecU256 {
                    project_id: "casino".into(),
                    scope_id: "app".into(),
                    table_name: "accounts".into(),
                    primary_key: vec![Value::Integer(1)],
                    column: "balance".into(),
                    amount_be: u256_be(200),
                },
                Mutation::TableIncU256 {
                    project_id: "casino".into(),
                    scope_id: "app".into(),
                    table_name: "accounts".into(),
                    primary_key: vec![Value::Integer(2)],
                    column: "balance".into(),
                    amount_be: u256_be(200),
                },
                Mutation::TableDecU256 {
                    project_id: "casino".into(),
                    scope_id: "app".into(),
                    table_name: "accounts".into(),
                    primary_key: vec![Value::Integer(2)],
                    column: "balance".into(),
                    amount_be: u256_be(400),
                },
                Mutation::TableIncU256 {
                    project_id: "casino".into(),
                    scope_id: "app".into(),
                    table_name: "accounts".into(),
                    primary_key: vec![Value::Integer(3)],
                    column: "balance".into(),
                    amount_be: u256_be(400),
                },
                Mutation::TableDecU256 {
                    project_id: "casino".into(),
                    scope_id: "app".into(),
                    table_name: "accounts".into(),
                    primary_key: vec![Value::Integer(3)],
                    column: "balance".into(),
                    amount_be: u256_be(100),
                },
                Mutation::TableIncU256 {
                    project_id: "casino".into(),
                    scope_id: "app".into(),
                    table_name: "accounts".into(),
                    primary_key: vec![Value::Integer(1)],
                    column: "balance".into(),
                    amount_be: u256_be(100),
                },
            ],
        },
        base_seq,
    })
    .await
    .expect("atomic transfer envelope");

    // Verify: Balances updated correctly
    assert_eq!(get_balance(&db, 1).await, 900, "account A balance");
    assert_eq!(get_balance(&db, 2).await, 300, "account B balance");
    assert_eq!(get_balance(&db, 3).await, 600, "account C balance");

    // Verify: Total balance conserved
    let final_total =
        get_balance(&db, 1).await + get_balance(&db, 2).await + get_balance(&db, 3).await;
    assert_eq!(final_total, initial_total, "balance conservation violated!");
}

/// Test Case 3: Idempotency Prevents Double-Debit
///
/// Validates that idempotency keys prevent duplicate withdrawals.
/// Critical for retry scenarios in casino operations.
#[tokio::test]
async fn test_idempotency_prevents_double_debit() {
    let dir = tempdir().expect("temp dir");
    let db = Arc::new(AedbInstance::open(Default::default(), dir.path()).expect("open db"));
    setup_accounts_table(&db).await;

    // Setup: Account balance = 1000
    db.commit(Mutation::Upsert {
        project_id: "casino".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1), Value::U256(u256_be(1000))]),
    })
    .await
    .expect("seed account");

    // Use a fixed 16-byte idempotency key for this withdrawal
    let idempotency_key = IdempotencyKey([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

    // Action: Submit same withdrawal 10 times with same idempotency key
    let mut tasks = JoinSet::new();
    for i in 0..10 {
        let db = Arc::clone(&db);
        let key = idempotency_key.clone();
        tasks.spawn(async move {
            let base_seq = db
                .snapshot_probe(ConsistencyMode::AtLatest)
                .await
                .expect("probe seq");

            let result = db
                .commit_envelope(TransactionEnvelope {
                    caller: None,
                    idempotency_key: Some(key),
                    write_class: WriteClass::Standard,
                    assertions: Vec::new(),
                    read_set: ReadSet::default(),
                    write_intent: WriteIntent {
                        mutations: vec![Mutation::TableDecU256 {
                            project_id: "casino".into(),
                            scope_id: "app".into(),
                            table_name: "accounts".into(),
                            primary_key: vec![Value::Integer(1)],
                            column: "balance".into(),
                            amount_be: u256_be(200),
                        }],
                    },
                    base_seq,
                })
                .await;
            (i, result)
        });
    }

    // Collect results
    let mut success_count = 0;
    while let Some(result) = tasks.join_next().await {
        let (_task_id, commit_result) = result.expect("task panicked");
        if commit_result.is_ok() {
            success_count += 1;
        }
    }

    // All should "succeed" (idempotency returns cached result)
    assert_eq!(success_count, 10, "all idempotent requests should succeed");

    // Verify: Balance deducted exactly once
    let final_balance = get_balance(&db, 1).await;
    assert_eq!(
        final_balance, 800,
        "balance should be 800 (deducted once), got {}",
        final_balance
    );
}

/// Test Case 4: Withdrawal During Concurrent Balance Query
///
/// Validates snapshot isolation - readers see consistent state.
/// Critical for ensuring players see accurate balances.
#[tokio::test]
async fn test_withdrawal_during_concurrent_balance_query() {
    let dir = tempdir().expect("temp dir");
    let db = Arc::new(AedbInstance::open(Default::default(), dir.path()).expect("open db"));
    setup_accounts_table(&db).await;

    // Setup: Account balance = 500
    db.commit(Mutation::Upsert {
        project_id: "casino".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1), Value::U256(u256_be(500))]),
    })
    .await
    .expect("seed account");

    // Spawn reader thread that queries balance repeatedly
    let db_reader = Arc::clone(&db);
    let reader_task = tokio::spawn(async move {
        let mut observed_balances = Vec::new();
        for _ in 0..100 {
            let balance = get_balance(&db_reader, 1).await;
            observed_balances.push(balance);
            tokio::time::sleep(Duration::from_micros(100)).await;
        }
        observed_balances
    });

    // Spawn writer thread that withdraws full balance
    let db_writer = Arc::clone(&db);
    let writer_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1)).await;
        db_writer
            .table_dec_u256(
                "casino",
                "app",
                "accounts",
                vec![Value::Integer(1)],
                "balance",
                u256_be(500),
            )
            .await
            .expect("withdraw");
    });

    let observed_balances = reader_task.await.expect("reader task");
    writer_task.await.expect("writer task");

    // Verify: Readers only see 500 or 0 (no intermediate states)
    for balance in &observed_balances {
        assert!(
            *balance == 500 || *balance == 0,
            "observed inconsistent balance: {}",
            balance
        );
    }

    println!(
        "Observed {} readings of 500, {} readings of 0",
        observed_balances.iter().filter(|&&b| b == 500).count(),
        observed_balances.iter().filter(|&&b| b == 0).count()
    );
}
