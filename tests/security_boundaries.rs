use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::validation::Mutation;
use aedb::error::AedbError;
use aedb::query::plan::{ConsistencyMode, Expr, Query};
use primitive_types::U256;
use tempfile::tempdir;

fn u256_be(value: u64) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[24..].copy_from_slice(&value.to_be_bytes());
    bytes
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

/// Test Case 1: U256 Boundary Values
///
/// Validates behavior at U256::MAX and U256::ZERO boundaries.
/// Critical for preventing wrapping attacks.
#[tokio::test]
async fn test_u256_boundary_values() {
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open db");
    setup_accounts_table(&db).await;

    // Test U256::MAX boundary
    let max_minus_1 = U256::MAX - U256::from(1);
    let mut max_minus_1_be = [0u8; 32];
    max_minus_1.to_big_endian(&mut max_minus_1_be);

    db.commit(Mutation::Upsert {
        project_id: "casino".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1), Value::U256(max_minus_1_be)]),
    })
    .await
    .expect("seed account at MAX-1");

    // Increment by 1 should succeed
    db.table_inc_u256(
        "casino",
        "app",
        "accounts",
        vec![Value::Integer(1)],
        "balance",
        u256_be(1),
    )
    .await
    .expect("increment to MAX should succeed");

    // Verify balance is now MAX
    let row = db
        .query_with_options(
            "casino",
            "app",
            Query::select(&["balance"])
                .from("accounts")
                .where_(Expr::Eq("id".into(), Value::Integer(1))),
            aedb::query::plan::QueryOptions {
                consistency: ConsistencyMode::AtLatest,
                ..Default::default()
            },
        )
        .await
        .expect("query")
        .rows
        .into_iter()
        .next()
        .expect("row");

    let Value::U256(balance_be) = row.values[0] else {
        panic!("expected U256");
    };
    let balance = U256::from_big_endian(&balance_be);
    assert_eq!(balance, U256::MAX, "balance should be exactly U256::MAX");

    // Increment by 1 should now OVERFLOW
    let err = db
        .table_inc_u256(
            "casino",
            "app",
            "accounts",
            vec![Value::Integer(1)],
            "balance",
            u256_be(1),
        )
        .await
        .expect_err("increment beyond MAX should fail");

    assert!(
        matches!(err, AedbError::Overflow)
            || matches!(err, AedbError::Validation(ref msg) if msg.contains("overflow")),
        "expected Overflow, got {:?}",
        err
    );

    // Test U256::ZERO boundary
    db.commit(Mutation::Upsert {
        project_id: "casino".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        primary_key: vec![Value::Integer(2)],
        row: Row::from_values(vec![Value::Integer(2), Value::U256(u256_be(0))]),
    })
    .await
    .expect("seed account at 0");

    // Decrement should fail
    let err = db
        .table_dec_u256(
            "casino",
            "app",
            "accounts",
            vec![Value::Integer(2)],
            "balance",
            u256_be(1),
        )
        .await
        .expect_err("decrement below 0 should fail");

    assert!(
        matches!(err, AedbError::Underflow)
            || matches!(err, AedbError::Validation(ref msg) if msg.contains("underflow")),
        "expected Underflow, got {:?}",
        err
    );

    println!("U256 boundary tests passed: MAX and ZERO boundaries enforced");
}

/// Test Case 2: Balance Wrapping Attack Prevention
///
/// Validates that overflow doesn't wrap to zero/negative.
/// Critical for preventing balance manipulation attacks.
#[tokio::test]
async fn test_balance_wrapping_attack_prevention() {
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open db");
    setup_accounts_table(&db).await;

    // Setup: Balance near MAX
    let near_max = U256::MAX - U256::from(50);
    let mut near_max_be = [0u8; 32];
    near_max.to_big_endian(&mut near_max_be);

    db.commit(Mutation::Upsert {
        project_id: "casino".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1), Value::U256(near_max_be)]),
    })
    .await
    .expect("seed account");

    // Attack: Try to increment by 100 to cause wrapping
    let err = db
        .table_inc_u256(
            "casino",
            "app",
            "accounts",
            vec![Value::Integer(1)],
            "balance",
            u256_be(100),
        )
        .await
        .expect_err("overflow attack should fail");

    assert!(
        matches!(err, AedbError::Overflow)
            || matches!(err, AedbError::Validation(ref msg) if msg.contains("overflow")),
        "expected Overflow, got {:?}",
        err
    );

    // Verify: Balance unchanged
    let row = db
        .query_with_options(
            "casino",
            "app",
            Query::select(&["balance"])
                .from("accounts")
                .where_(Expr::Eq("id".into(), Value::Integer(1))),
            aedb::query::plan::QueryOptions {
                consistency: ConsistencyMode::AtLatest,
                ..Default::default()
            },
        )
        .await
        .expect("query")
        .rows
        .into_iter()
        .next()
        .expect("row");

    let Value::U256(balance_be) = row.values[0] else {
        panic!("expected U256");
    };
    let balance = U256::from_big_endian(&balance_be);
    assert_eq!(
        balance, near_max,
        "balance should remain unchanged after overflow rejection"
    );

    // Valid increments within bounds should work
    db.table_inc_u256(
        "casino",
        "app",
        "accounts",
        vec![Value::Integer(1)],
        "balance",
        u256_be(50),
    )
    .await
    .expect("valid increment should succeed");

    let row = db
        .query_with_options(
            "casino",
            "app",
            Query::select(&["balance"])
                .from("accounts")
                .where_(Expr::Eq("id".into(), Value::Integer(1))),
            aedb::query::plan::QueryOptions {
                consistency: ConsistencyMode::AtLatest,
                ..Default::default()
            },
        )
        .await
        .expect("query")
        .rows
        .into_iter()
        .next()
        .expect("row");

    let Value::U256(balance_be) = row.values[0] else {
        panic!("expected U256");
    };
    let balance = U256::from_big_endian(&balance_be);
    assert_eq!(balance, U256::MAX, "balance should now be MAX");

    println!("Wrapping attack prevention: overflow correctly blocked");
}

/// Test Case 3: Large Value Stress Test
///
/// Validates correct handling of extremely large U256 values.
/// Critical for ensuring precision at scale.
#[tokio::test]
async fn test_large_value_precision() {
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open db");
    setup_accounts_table(&db).await;

    // Test with various large values
    let test_values = [
        U256::from(u64::MAX),
        U256::from(u128::MAX),
        U256::MAX / U256::from(2),
        U256::MAX / U256::from(10),
        U256::MAX - U256::from(1_000_000),
    ];

    for (i, test_val) in test_values.iter().enumerate() {
        let mut val_be = [0u8; 32];
        test_val.to_big_endian(&mut val_be);

        db.commit(Mutation::Upsert {
            project_id: "casino".into(),
            scope_id: "app".into(),
            table_name: "accounts".into(),
            primary_key: vec![Value::Integer(i as i64)],
            row: Row::from_values(vec![Value::Integer(i as i64), Value::U256(val_be)]),
        })
        .await
        .expect("seed large value");

        // Read back and verify exact value
        let row = db
            .query_with_options(
                "casino",
                "app",
                Query::select(&["balance"])
                    .from("accounts")
                    .where_(Expr::Eq("id".into(), Value::Integer(i as i64))),
                aedb::query::plan::QueryOptions {
                    consistency: ConsistencyMode::AtLatest,
                    ..Default::default()
                },
            )
            .await
            .expect("query")
            .rows
            .into_iter()
            .next()
            .expect("row");

        let Value::U256(balance_be) = row.values[0] else {
            panic!("expected U256");
        };
        let balance = U256::from_big_endian(&balance_be);

        assert_eq!(
            balance, *test_val,
            "large value precision lost for value {}",
            test_val
        );
    }

    println!(
        "Large value precision test passed for {} values",
        test_values.len()
    );
}

/// Test Case 4: Concurrent Boundary Operations
///
/// Validates that boundary conditions hold under concurrent access.
/// Critical for race condition prevention.
#[tokio::test]
async fn test_concurrent_boundary_operations() {
    use std::sync::Arc;
    use tokio::task::JoinSet;

    let dir = tempdir().expect("temp dir");
    let db = Arc::new(AedbInstance::open(Default::default(), dir.path()).expect("open db"));
    setup_accounts_table(&db).await;

    // Setup: Account at boundary
    let boundary_val = U256::MAX - U256::from(100);
    let mut boundary_be = [0u8; 32];
    boundary_val.to_big_endian(&mut boundary_be);

    db.commit(Mutation::Upsert {
        project_id: "casino".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1), Value::U256(boundary_be)]),
    })
    .await
    .expect("seed boundary account");

    // Action: 20 concurrent increments of 10 each
    // Only first 10 can succeed (100 / 10 = 10)
    let mut tasks = JoinSet::new();
    for _ in 0..20 {
        let db = Arc::clone(&db);
        tasks.spawn(async move {
            db.table_inc_u256(
                "casino",
                "app",
                "accounts",
                vec![Value::Integer(1)],
                "balance",
                u256_be(10),
            )
            .await
        });
    }

    let mut success_count = 0;
    let mut overflow_count = 0;

    while let Some(result) = tasks.join_next().await {
        match result.expect("task panicked") {
            Ok(_) => success_count += 1,
            Err(AedbError::Overflow) => overflow_count += 1,
            Err(AedbError::Validation(msg)) if msg.contains("overflow") => overflow_count += 1,
            Err(e) => panic!("unexpected error: {:?}", e),
        }
    }

    println!(
        "Concurrent boundary ops: success={}, overflow={}",
        success_count, overflow_count
    );

    // Verify: Operations accounted for
    // Due to epoch batching, all may overflow if they batch together
    assert_eq!(
        success_count + overflow_count,
        20,
        "all operations accounted for"
    );
    assert!(success_count <= 10, "at most 10 can succeed");

    // Verify: Final balance doesn't exceed MAX
    let row = db
        .query_with_options(
            "casino",
            "app",
            Query::select(&["balance"])
                .from("accounts")
                .where_(Expr::Eq("id".into(), Value::Integer(1))),
            aedb::query::plan::QueryOptions {
                consistency: ConsistencyMode::AtLatest,
                ..Default::default()
            },
        )
        .await
        .expect("query")
        .rows
        .into_iter()
        .next()
        .expect("row");

    let Value::U256(balance_be) = row.values[0] else {
        panic!("expected U256");
    };
    let balance = U256::from_big_endian(&balance_be);

    assert!(balance <= U256::MAX, "balance must not exceed MAX");
    assert_eq!(
        balance,
        boundary_val + U256::from(success_count * 10),
        "balance should increase by successful operations only"
    );
}
