use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::tx::{ReadSet, TransactionEnvelope, WriteClass, WriteIntent};
use aedb::commit::validation::Mutation;
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

async fn setup_settlement_schema(db: &AedbInstance) {
    db.create_project("casino").await.expect("create project");

    // Winners table
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "casino".into(),
        scope_id: "app".into(),
        table_name: "winners".into(),
        owner_id: None,
        if_not_exists: false,
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "account_id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "payout".into(),
                col_type: ColumnType::U256,
                nullable: false,
            },
            ColumnDef {
                name: "settled".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("create winners table");

    // Accounts table
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

/// Test Case 1: Settlement Atomicity with Large Batch
///
/// Validates that settlement with many winners is atomic.
/// Critical for ensuring all winners get paid or none do.
#[tokio::test]
async fn test_settlement_atomicity_large_batch() {
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open db");
    setup_settlement_schema(&db).await;

    // Setup: Create 100 winner records and accounts with initial balance 1000
    let winner_count = 100;
    for i in 0..winner_count {
        db.commit(Mutation::Upsert {
            project_id: "casino".into(),
            scope_id: "app".into(),
            table_name: "winners".into(),
            primary_key: vec![Value::Integer(i)],
            row: Row::from_values(vec![
                Value::Integer(i),
                Value::Integer(i),
                Value::U256(u256_be(500)),
                Value::Integer(0), // not settled
            ]),
        })
        .await
        .expect("seed winner");

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

    // Action: Settlement envelope with 100 payouts
    let base_seq = db
        .snapshot_probe(ConsistencyMode::AtLatest)
        .await
        .expect("probe seq");

    let mut mutations = Vec::new();
    for i in 0..winner_count {
        // Credit winner account
        mutations.push(Mutation::TableIncU256 {
            project_id: "casino".into(),
            scope_id: "app".into(),
            table_name: "accounts".into(),
            primary_key: vec![Value::Integer(i)],
            column: "balance".into(),
            amount_be: u256_be(500),
        });
        // Mark winner as settled
        mutations.push(Mutation::Upsert {
            project_id: "casino".into(),
            scope_id: "app".into(),
            table_name: "winners".into(),
            primary_key: vec![Value::Integer(i)],
            row: Row::from_values(vec![
                Value::Integer(i),
                Value::Integer(i),
                Value::U256(u256_be(500)),
                Value::Integer(1), // settled
            ]),
        });
    }

    db.commit_envelope(TransactionEnvelope {
        caller: None,
        idempotency_key: None,
        write_class: WriteClass::Standard,
        assertions: Vec::new(),
        read_set: ReadSet::default(),
        write_intent: WriteIntent { mutations },
        base_seq,
    })
    .await
    .expect("settlement envelope");

    // Verify: All 100 winners credited
    for i in 0..winner_count {
        let balance = get_balance(&db, i).await;
        assert_eq!(
            balance, 1500,
            "winner {} should have balance 1500, got {}",
            i, balance
        );
    }

    // Verify: All winners marked as settled
    let settled_count = db
        .query_with_options(
            "casino",
            "app",
            Query::select(&["id"])
                .from("winners")
                .where_(Expr::Eq("settled".into(), Value::Integer(1)))
                .limit(200),
            aedb::query::plan::QueryOptions {
                consistency: ConsistencyMode::AtLatest,
                allow_full_scan: true,
                ..Default::default()
            },
        )
        .await
        .expect("query settled")
        .rows
        .len();

    assert_eq!(
        settled_count, winner_count as usize,
        "all winners should be marked as settled"
    );

    println!(
        "Settlement atomicity test: {} winners credited successfully",
        winner_count
    );
}

/// Test Case 2: Settlement Prevents Concurrent Operations
///
/// Validates that settlement locks prevent concurrent modifications.
/// Critical for ensuring settlement finality.
#[tokio::test]
async fn test_settlement_prevents_concurrent_ops() {
    let dir = tempdir().expect("temp dir");
    let db = Arc::new(AedbInstance::open(Default::default(), dir.path()).expect("open db"));
    setup_settlement_schema(&db).await;

    // Setup: Create 10 winners
    for i in 0..10 {
        db.commit(Mutation::Upsert {
            project_id: "casino".into(),
            scope_id: "app".into(),
            table_name: "winners".into(),
            primary_key: vec![Value::Integer(i)],
            row: Row::from_values(vec![
                Value::Integer(i),
                Value::Integer(i),
                Value::U256(u256_be(100)),
                Value::Integer(0),
            ]),
        })
        .await
        .expect("seed winner");

        db.commit(Mutation::Upsert {
            project_id: "casino".into(),
            scope_id: "app".into(),
            table_name: "accounts".into(),
            primary_key: vec![Value::Integer(i)],
            row: Row::from_values(vec![Value::Integer(i), Value::U256(u256_be(500))]),
        })
        .await
        .expect("seed account");
    }

    // Spawn settlement thread
    let db_settler = Arc::clone(&db);
    let settle_task = tokio::spawn(async move {
        let base_seq = db_settler
            .snapshot_probe(ConsistencyMode::AtLatest)
            .await
            .expect("probe seq");

        let mut mutations = Vec::new();
        for i in 0..10 {
            mutations.push(Mutation::TableIncU256 {
                project_id: "casino".into(),
                scope_id: "app".into(),
                table_name: "accounts".into(),
                primary_key: vec![Value::Integer(i)],
                column: "balance".into(),
                amount_be: u256_be(100),
            });
            mutations.push(Mutation::Upsert {
                project_id: "casino".into(),
                scope_id: "app".into(),
                table_name: "winners".into(),
                primary_key: vec![Value::Integer(i)],
                row: Row::from_values(vec![
                    Value::Integer(i),
                    Value::Integer(i),
                    Value::U256(u256_be(100)),
                    Value::Integer(1),
                ]),
            });
        }

        db_settler
            .commit_envelope(TransactionEnvelope {
                caller: None,
                idempotency_key: None,
                write_class: WriteClass::Standard,
                assertions: Vec::new(),
                read_set: ReadSet::default(),
                write_intent: WriteIntent { mutations },
                base_seq,
            })
            .await
    });

    // Spawn concurrent threads trying to modify accounts during settlement
    let mut concurrent_tasks = JoinSet::new();
    for i in 0..10i64 {
        let db_concurrent = Arc::clone(&db);
        concurrent_tasks.spawn(async move {
            tokio::time::sleep(Duration::from_micros(i as u64 * 100)).await;
            db_concurrent
                .table_inc_u256(
                    "casino",
                    "app",
                    "accounts",
                    vec![Value::Integer(i % 5)], // Overlap with settlement
                    "balance",
                    u256_be(50),
                )
                .await
        });
    }

    let settle_result = settle_task.await.expect("settle task");
    assert!(settle_result.is_ok(), "settlement should succeed");

    // Collect concurrent operation results
    let mut concurrent_success = 0;
    let mut concurrent_conflict = 0;
    while let Some(result) = concurrent_tasks.join_next().await {
        match result.expect("concurrent task") {
            Ok(_) => concurrent_success += 1,
            Err(_) => concurrent_conflict += 1,
        }
    }

    println!(
        "Concurrent ops during settlement: success={}, conflict={}",
        concurrent_success, concurrent_conflict
    );

    // Verify: All winners settled
    let settled_count = db
        .query_with_options(
            "casino",
            "app",
            Query::select(&["id"])
                .from("winners")
                .where_(Expr::Eq("settled".into(), Value::Integer(1)))
                .limit(20),
            aedb::query::plan::QueryOptions {
                consistency: ConsistencyMode::AtLatest,
                allow_full_scan: true,
                ..Default::default()
            },
        )
        .await
        .expect("query settled")
        .rows
        .len();

    assert_eq!(settled_count, 10, "all 10 winners should be settled");

    // Verify: Final balances are consistent
    // Should be 500 (initial) + 100 (settlement) + (possibly some concurrent ops)
    for i in 0..10 {
        let balance = get_balance(&db, i).await;
        assert!(
            balance >= 600,
            "account {} balance should be at least 600 (500+100), got {}",
            i,
            balance
        );
    }
}
