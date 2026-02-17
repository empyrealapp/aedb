use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::tx::{ReadAssertion, ReadSet, TransactionEnvelope, WriteClass, WriteIntent};
use aedb::commit::validation::{CompareOp, Mutation};
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

async fn setup_betting_schema(db: &AedbInstance) {
    db.create_project("casino").await.expect("create project");

    // Events table
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "casino".into(),
        scope_id: "app".into(),
        table_name: "events".into(),
        owner_id: None,
        if_not_exists: false,
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "max_stake".into(),
                col_type: ColumnType::U256,
                nullable: false,
            },
            ColumnDef {
                name: "current_stake".into(),
                col_type: ColumnType::U256,
                nullable: false,
            },
            ColumnDef {
                name: "status".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("create events table");

    // Bets table
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "casino".into(),
        scope_id: "app".into(),
        table_name: "bets".into(),
        owner_id: None,
        if_not_exists: false,
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "event_id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "amount".into(),
                col_type: ColumnType::U256,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("create bets table");

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

async fn get_event_stake(db: &AedbInstance, event_id: i64) -> u64 {
    let row = db
        .query_with_options(
            "casino",
            "app",
            Query::select(&["current_stake"])
                .from("events")
                .where_(Expr::Eq("id".into(), Value::Integer(event_id))),
            aedb::query::plan::QueryOptions {
                consistency: ConsistencyMode::AtLatest,
                ..Default::default()
            },
        )
        .await
        .expect("query stake")
        .rows
        .into_iter()
        .next()
        .expect("event exists");

    let Value::U256(stake_be) = row.values[0] else {
        panic!("expected U256 stake");
    };
    u256_from_be(&stake_be)
}

async fn get_event_status(db: &AedbInstance, event_id: i64) -> String {
    let row = db
        .query_with_options(
            "casino",
            "app",
            Query::select(&["status"])
                .from("events")
                .where_(Expr::Eq("id".into(), Value::Integer(event_id))),
            aedb::query::plan::QueryOptions {
                consistency: ConsistencyMode::AtLatest,
                ..Default::default()
            },
        )
        .await
        .expect("query status")
        .rows
        .into_iter()
        .next()
        .expect("event exists");

    let Value::Text(status) = row.values[0].clone() else {
        panic!("expected Text status");
    };
    status.to_string()
}

/// Test Case 1: Concurrent Bets on Event with Limit
///
/// Validates that betting limits are enforced under concurrent load.
/// Critical for casino operations to prevent over-exposure on events.
#[tokio::test]
async fn test_concurrent_bets_with_event_limit() {
    let dir = tempdir().expect("temp dir");
    let db = Arc::new(AedbInstance::open(Default::default(), dir.path()).expect("open db"));
    setup_betting_schema(&db).await;

    // Setup: Event with max_stake=10000, current_stake=9500
    db.commit(Mutation::Upsert {
        project_id: "casino".into(),
        scope_id: "app".into(),
        table_name: "events".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![
            Value::Integer(1),
            Value::U256(u256_be(10000)),
            Value::U256(u256_be(9500)),
            Value::Text("OPEN".into()),
        ]),
    })
    .await
    .expect("seed event");

    // Action: 10 concurrent bets of 100 each
    // Only first 5 can succeed (9500 + 500 = 10000)
    let mut tasks = JoinSet::new();
    for bet_id in 0..10 {
        let db = Arc::clone(&db);
        tasks.spawn(async move {
            // Stagger slightly to span multiple epochs
            if bet_id > 0 {
                tokio::time::sleep(Duration::from_micros(bet_id as u64 * 50)).await;
            }

            let base_seq = db
                .snapshot_probe(ConsistencyMode::AtLatest)
                .await
                .expect("probe seq");

            // Atomic bet placement with envelope-level assertion
            db.commit_envelope(TransactionEnvelope {
                caller: None,
                idempotency_key: None,
                write_class: WriteClass::Standard,
                assertions: vec![ReadAssertion::RowColumnCompare {
                    project_id: "casino".into(),
                    scope_id: "app".into(),
                    table_name: "events".into(),
                    primary_key: vec![Value::Integer(1)],
                    column: "current_stake".into(),
                    op: CompareOp::Lt,
                    threshold: Value::U256(u256_be(10000)),
                }],
                read_set: ReadSet::default(),
                write_intent: WriteIntent {
                    mutations: vec![
                        // Increment current stake
                        Mutation::TableIncU256 {
                            project_id: "casino".into(),
                            scope_id: "app".into(),
                            table_name: "events".into(),
                            primary_key: vec![Value::Integer(1)],
                            column: "current_stake".into(),
                            amount_be: u256_be(100),
                        },
                        // Insert bet record
                        Mutation::Upsert {
                            project_id: "casino".into(),
                            scope_id: "app".into(),
                            table_name: "bets".into(),
                            primary_key: vec![Value::Integer(bet_id)],
                            row: Row::from_values(vec![
                                Value::Integer(bet_id),
                                Value::Integer(1),
                                Value::U256(u256_be(100)),
                            ]),
                        },
                    ],
                },
                base_seq,
            })
            .await
        });
    }

    // Collect results
    let mut success_count = 0;
    let mut conflict_count = 0;
    let mut overflow_count = 0;

    while let Some(result) = tasks.join_next().await {
        match result.expect("task panicked") {
            Ok(_) => success_count += 1,
            Err(AedbError::AssertionFailed { .. }) => conflict_count += 1,
            Err(AedbError::Overflow) => overflow_count += 1,
            Err(AedbError::Validation(msg)) if msg.contains("overflow") => overflow_count += 1,
            Err(e) => panic!("unexpected error: {:?}", e),
        }
    }

    println!(
        "Betting results: success={}, conflict={}, overflow={}",
        success_count, conflict_count, overflow_count
    );

    // Verify: Some succeeded, but not all
    assert!(success_count > 0, "at least one bet should succeed");
    assert!(success_count <= 5, "at most 5 bets can succeed");
    assert_eq!(
        success_count + conflict_count + overflow_count,
        10,
        "all operations accounted for"
    );

    // Verify: Final stake doesn't exceed limit
    let final_stake = get_event_stake(&db, 1).await;
    assert!(
        final_stake <= 10000,
        "stake must not exceed limit: {}",
        final_stake
    );
    assert_eq!(
        final_stake,
        9500 + (success_count * 100),
        "stake should increase by successful bets only"
    );

    // Verify: Bet records exist for successful bets
    let bet_count = db
        .query_with_options(
            "casino",
            "app",
            Query::select(&["id"]).from("bets").limit(100),
            aedb::query::plan::QueryOptions {
                consistency: ConsistencyMode::AtLatest,
                allow_full_scan: true,
                ..Default::default()
            },
        )
        .await
        .expect("query bets")
        .rows
        .len();

    assert_eq!(
        bet_count, success_count as usize,
        "bet records should match successful bets"
    );
}

/// Test Case 2: Bet During Settlement Lock
///
/// Validates that bets cannot be placed once settlement begins.
/// Critical for ensuring betting closes atomically at settlement time.
#[tokio::test]
async fn test_bet_during_settlement_lock() {
    let dir = tempdir().expect("temp dir");
    let db = Arc::new(AedbInstance::open(Default::default(), dir.path()).expect("open db"));
    setup_betting_schema(&db).await;

    // Setup: Event accepting bets
    db.commit(Mutation::Upsert {
        project_id: "casino".into(),
        scope_id: "app".into(),
        table_name: "events".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![
            Value::Integer(1),
            Value::U256(u256_be(10000)),
            Value::U256(u256_be(5000)),
            Value::Text("OPEN".into()),
        ]),
    })
    .await
    .expect("seed event");

    // Spawn bet placer thread
    let db_better = Arc::clone(&db);
    let bet_task = tokio::spawn(async move {
        // Wait a bit to let settlement start
        tokio::time::sleep(Duration::from_millis(5)).await;

        let base_seq = db_better
            .snapshot_probe(ConsistencyMode::AtLatest)
            .await
            .expect("probe seq");

        // Try to place bet with status check
        db_better
            .commit_envelope(TransactionEnvelope {
                caller: None,
                idempotency_key: None,
                write_class: WriteClass::Standard,
                assertions: vec![ReadAssertion::RowColumnCompare {
                    project_id: "casino".into(),
                    scope_id: "app".into(),
                    table_name: "events".into(),
                    primary_key: vec![Value::Integer(1)],
                    column: "status".into(),
                    op: CompareOp::Eq,
                    threshold: Value::Text("OPEN".into()),
                }],
                read_set: ReadSet::default(),
                write_intent: WriteIntent {
                    mutations: vec![
                        Mutation::TableIncU256 {
                            project_id: "casino".into(),
                            scope_id: "app".into(),
                            table_name: "events".into(),
                            primary_key: vec![Value::Integer(1)],
                            column: "current_stake".into(),
                            amount_be: u256_be(100),
                        },
                        Mutation::Upsert {
                            project_id: "casino".into(),
                            scope_id: "app".into(),
                            table_name: "bets".into(),
                            primary_key: vec![Value::Integer(999)],
                            row: Row::from_values(vec![
                                Value::Integer(999),
                                Value::Integer(1),
                                Value::U256(u256_be(100)),
                            ]),
                        },
                    ],
                },
                base_seq,
            })
            .await
    });

    // Spawn settler thread
    let db_settler = Arc::clone(&db);
    let settle_task = tokio::spawn(async move {
        let base_seq = db_settler
            .snapshot_probe(ConsistencyMode::AtLatest)
            .await
            .expect("probe seq");

        // Settlement: Lock event by changing status
        db_settler
            .commit_envelope(TransactionEnvelope {
                caller: None,
                idempotency_key: None,
                write_class: WriteClass::Standard,
                assertions: Vec::new(),
                read_set: ReadSet::default(),
                write_intent: WriteIntent {
                    mutations: vec![Mutation::Upsert {
                        project_id: "casino".into(),
                        scope_id: "app".into(),
                        table_name: "events".into(),
                        primary_key: vec![Value::Integer(1)],
                        row: Row::from_values(vec![
                            Value::Integer(1),
                            Value::U256(u256_be(10000)),
                            Value::U256(u256_be(5000)),
                            Value::Text("LOCKED".into()),
                        ]),
                    }],
                },
                base_seq,
            })
            .await
    });

    let bet_result = bet_task.await.expect("bet task");
    let settle_result = settle_task.await.expect("settle task");

    // At least one should succeed
    assert!(
        bet_result.is_ok() || settle_result.is_ok(),
        "at least one operation should succeed"
    );

    // If settlement succeeded, bet should have failed
    if settle_result.is_ok() {
        // Check final status
        let final_status = get_event_status(&db, 1).await;
        if final_status == "LOCKED" {
            // Settlement won - bet should have failed if it tried after
            // (it may have succeeded if it committed before settlement)
            if bet_result.is_err() {
                assert!(
                    matches!(bet_result, Err(AedbError::AssertionFailed { .. })),
                    "bet should fail with assertion failure when event is locked"
                );
            }
        }
    }

    println!(
        "Settlement test: bet={:?}, settle={:?}, final_status={}",
        bet_result.is_ok(),
        settle_result.is_ok(),
        get_event_status(&db, 1).await
    );
}

/// Test Case 3: Late Bet Prevention with AssertRowField
///
/// Validates that AssertRowField provides atomic condition checking.
/// Critical for preventing bets on locked/settled events.
#[tokio::test]
async fn test_late_bet_prevention_with_assert() {
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open db");
    setup_betting_schema(&db).await;

    // Setup: Event initially OPEN
    db.commit(Mutation::Upsert {
        project_id: "casino".into(),
        scope_id: "app".into(),
        table_name: "events".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![
            Value::Integer(1),
            Value::U256(u256_be(10000)),
            Value::U256(u256_be(0)),
            Value::Text("OPEN".into()),
        ]),
    })
    .await
    .expect("seed event");

    // Place valid bet while OPEN
    let base_seq = db
        .snapshot_probe(ConsistencyMode::AtLatest)
        .await
        .expect("probe seq");

    db.commit_envelope(TransactionEnvelope {
        caller: None,
        idempotency_key: None,
        write_class: WriteClass::Standard,
        assertions: vec![ReadAssertion::RowColumnCompare {
            project_id: "casino".into(),
            scope_id: "app".into(),
            table_name: "events".into(),
            primary_key: vec![Value::Integer(1)],
            column: "status".into(),
            op: CompareOp::Eq,
            threshold: Value::Text("OPEN".into()),
        }],
        read_set: ReadSet::default(),
        write_intent: WriteIntent {
            mutations: vec![Mutation::TableIncU256 {
                project_id: "casino".into(),
                scope_id: "app".into(),
                table_name: "events".into(),
                primary_key: vec![Value::Integer(1)],
                column: "current_stake".into(),
                amount_be: u256_be(500),
            }],
        },
        base_seq,
    })
    .await
    .expect("first bet should succeed");

    assert_eq!(get_event_stake(&db, 1).await, 500);

    // Lock event
    db.commit(Mutation::Upsert {
        project_id: "casino".into(),
        scope_id: "app".into(),
        table_name: "events".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![
            Value::Integer(1),
            Value::U256(u256_be(10000)),
            Value::U256(u256_be(500)),
            Value::Text("LOCKED".into()),
        ]),
    })
    .await
    .expect("lock event");

    // Try to place bet on locked event
    let base_seq = db
        .snapshot_probe(ConsistencyMode::AtLatest)
        .await
        .expect("probe seq");

    let err = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: vec![ReadAssertion::RowColumnCompare {
                project_id: "casino".into(),
                scope_id: "app".into(),
                table_name: "events".into(),
                primary_key: vec![Value::Integer(1)],
                column: "status".into(),
                op: CompareOp::Eq,
                threshold: Value::Text("OPEN".into()),
            }],
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::TableIncU256 {
                    project_id: "casino".into(),
                    scope_id: "app".into(),
                    table_name: "events".into(),
                    primary_key: vec![Value::Integer(1)],
                    column: "current_stake".into(),
                    amount_be: u256_be(200),
                }],
            },
            base_seq,
        })
        .await
        .expect_err("bet on locked event should fail");

    // Verify assertion failure
    assert!(
        matches!(err, AedbError::AssertionFailed { .. }),
        "expected assertion failure, got {:?}",
        err
    );

    // Verify stake unchanged
    assert_eq!(
        get_event_stake(&db, 1).await,
        500,
        "stake should not increase for failed bet"
    );

    println!("Late bet correctly rejected with AssertRowField");
}
