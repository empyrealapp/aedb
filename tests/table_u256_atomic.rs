use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::tx::{ReadAssertion, ReadSet, TransactionEnvelope, WriteClass, WriteIntent};
use aedb::commit::validation::{CompareOp, Mutation};
use aedb::error::AedbError;
use aedb::preflight::PreflightResult;
use aedb::query::plan::{ConsistencyMode, Expr, Query};
use primitive_types::U256;
use tempfile::tempdir;

fn u256_be(value: u64) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[24..].copy_from_slice(&value.to_be_bytes());
    bytes
}

fn decode_u64_u256(bytes: &[u8; 32]) -> u64 {
    let mut out = [0u8; 8];
    out.copy_from_slice(&bytes[24..]);
    u64::from_be_bytes(out)
}

#[tokio::test]
async fn integration_table_u256_atomic_inc_dec_and_underflow() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "p".into(),
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
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1), Value::U256(u256_be(99))]),
    })
    .await
    .expect("seed row");

    db.table_inc_u256(
        "p",
        "app",
        "accounts",
        vec![Value::Integer(1)],
        "balance",
        u256_be(11),
    )
    .await
    .expect("inc");

    db.table_dec_u256(
        "p",
        "app",
        "accounts",
        vec![Value::Integer(1)],
        "balance",
        u256_be(10),
    )
    .await
    .expect("dec");

    let row = db
        .query_with_options(
            "p",
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
    let Value::U256(current) = row.values[0] else {
        panic!("expected u256");
    };
    assert_eq!(decode_u64_u256(&current), 100);

    let preflight = db
        .preflight(Mutation::TableDecU256 {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "accounts".into(),
            primary_key: vec![Value::Integer(1)],
            column: "balance".into(),
            amount_be: u256_be(101),
        })
        .await;
    assert!(matches!(
        preflight,
        PreflightResult::Err { ref reason } if reason == "underflow"
    ));

    let err = db
        .commit_with_preflight(Mutation::TableDecU256 {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "accounts".into(),
            primary_key: vec![Value::Integer(1)],
            column: "balance".into(),
            amount_be: u256_be(101),
        })
        .await
        .expect_err("underflow should fail");
    assert!(matches!(err, AedbError::Underflow));

    db.commit_envelope(TransactionEnvelope {
        caller: None,
        idempotency_key: None,
        write_class: WriteClass::Standard,
        assertions: vec![ReadAssertion::RowColumnCompare {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "accounts".into(),
            primary_key: vec![Value::Integer(1)],
            column: "balance".into(),
            op: CompareOp::Eq,
            threshold: Value::U256(u256_be(100)),
        }],
        read_set: ReadSet::default(),
        write_intent: WriteIntent {
            mutations: vec![Mutation::Upsert {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "accounts".into(),
                primary_key: vec![Value::Integer(1)],
                row: Row::from_values(vec![Value::Integer(1), Value::U256(u256_be(80))]),
            }],
        },
        base_seq: db
            .snapshot_probe(ConsistencyMode::AtLatest)
            .await
            .expect("probe"),
    })
    .await
    .expect("cas via assertion+upsert");

    let cas_fail = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: vec![ReadAssertion::RowColumnCompare {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "accounts".into(),
                primary_key: vec![Value::Integer(1)],
                column: "balance".into(),
                op: CompareOp::Eq,
                threshold: Value::U256(u256_be(100)),
            }],
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::Upsert {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    table_name: "accounts".into(),
                    primary_key: vec![Value::Integer(1)],
                    row: Row::from_values(vec![Value::Integer(1), Value::U256(u256_be(70))]),
                }],
            },
            base_seq: db
                .snapshot_probe(ConsistencyMode::AtLatest)
                .await
                .expect("probe"),
        })
        .await
        .expect_err("stale cas expected to fail");
    assert!(
        matches!(cas_fail, AedbError::AssertionFailed { .. }),
        "expected conflict, got {cas_fail:?}"
    );

    db.commit_envelope(TransactionEnvelope {
        caller: None,
        idempotency_key: None,
        write_class: WriteClass::Standard,
        assertions: vec![ReadAssertion::RowColumnCompare {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "accounts".into(),
            primary_key: vec![Value::Integer(1)],
            column: "balance".into(),
            op: CompareOp::Eq,
            threshold: Value::U256(u256_be(80)),
        }],
        read_set: ReadSet::default(),
        write_intent: WriteIntent {
            mutations: vec![Mutation::TableDecU256 {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "accounts".into(),
                primary_key: vec![Value::Integer(1)],
                column: "balance".into(),
                amount_be: u256_be(5),
            }],
        },
        base_seq: db
            .snapshot_probe(ConsistencyMode::AtLatest)
            .await
            .expect("probe"),
    })
    .await
    .expect("compare-and-dec via assertion+dec");

    let compare_fail = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: vec![ReadAssertion::RowColumnCompare {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "accounts".into(),
                primary_key: vec![Value::Integer(1)],
                column: "balance".into(),
                op: CompareOp::Eq,
                threshold: Value::U256(u256_be(80)),
            }],
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::TableDecU256 {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    table_name: "accounts".into(),
                    primary_key: vec![Value::Integer(1)],
                    column: "balance".into(),
                    amount_be: u256_be(1),
                }],
            },
            base_seq: db
                .snapshot_probe(ConsistencyMode::AtLatest)
                .await
                .expect("probe"),
        })
        .await
        .expect_err("stale compare-and-dec should fail");
    assert!(
        matches!(compare_fail, AedbError::AssertionFailed { .. }),
        "expected conflict, got {compare_fail:?}"
    );

    db.commit_envelope(TransactionEnvelope {
        caller: None,
        idempotency_key: None,
        write_class: WriteClass::Standard,
        assertions: vec![ReadAssertion::RowColumnCompare {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "accounts".into(),
            primary_key: vec![Value::Integer(1)],
            column: "balance".into(),
            op: CompareOp::Gte,
            threshold: Value::U256(u256_be(75)),
        }],
        read_set: ReadSet::default(),
        write_intent: WriteIntent {
            mutations: vec![Mutation::Upsert {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "accounts".into(),
                primary_key: vec![Value::Integer(1)],
                row: Row::from_values(vec![Value::Integer(1), Value::U256(u256_be(75))]),
            }],
        },
        base_seq: db
            .snapshot_probe(ConsistencyMode::AtLatest)
            .await
            .expect("probe"),
    })
    .await
    .expect("assert gte");

    let assert_fail = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: vec![ReadAssertion::RowColumnCompare {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "accounts".into(),
                primary_key: vec![Value::Integer(1)],
                column: "balance".into(),
                op: CompareOp::Lt,
                threshold: Value::U256(u256_be(70)),
            }],
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::Upsert {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    table_name: "accounts".into(),
                    primary_key: vec![Value::Integer(1)],
                    row: Row::from_values(vec![Value::Integer(1), Value::U256(u256_be(75))]),
                }],
            },
            base_seq: db
                .snapshot_probe(ConsistencyMode::AtLatest)
                .await
                .expect("probe"),
        })
        .await
        .expect_err("assert should fail");
    assert!(
        matches!(assert_fail, AedbError::AssertionFailed { .. }),
        "expected conflict, got {assert_fail:?}"
    );
}

#[tokio::test]
async fn integration_envelope_reverts_when_condition_fails() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "p".into(),
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
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1), Value::U256(u256_be(50))]),
    })
    .await
    .expect("seed row");
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        primary_key: vec![Value::Integer(2)],
        row: Row::from_values(vec![Value::Integer(2), Value::U256(u256_be(25))]),
    })
    .await
    .expect("seed row 2");

    let base_seq = db
        .snapshot_probe(ConsistencyMode::AtLatest)
        .await
        .expect("snapshot seq");
    let err = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: vec![
                ReadAssertion::RowColumnCompare {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    table_name: "accounts".into(),
                    primary_key: vec![Value::Integer(1)],
                    column: "balance".into(),
                    op: CompareOp::Lt,
                    threshold: Value::U256(u256_be(10)),
                },
                ReadAssertion::RowColumnCompare {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    table_name: "accounts".into(),
                    primary_key: vec![Value::Integer(1)],
                    column: "balance".into(),
                    op: CompareOp::Eq,
                    threshold: Value::U256(u256_be(50)),
                },
            ],
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![
                    Mutation::TableIncU256 {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        table_name: "accounts".into(),
                        primary_key: vec![Value::Integer(1)],
                        column: "balance".into(),
                        amount_be: u256_be(10),
                    },
                    Mutation::TableIncU256 {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        table_name: "accounts".into(),
                        primary_key: vec![Value::Integer(2)],
                        column: "balance".into(),
                        amount_be: u256_be(100),
                    },
                ],
            },
            base_seq,
        })
        .await
        .expect_err("failed condition should revert entire envelope");
    assert!(matches!(err, AedbError::AssertionFailed { .. }));

    let row = db
        .query_with_options(
            "p",
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
    let Value::U256(current) = row.values[0] else {
        panic!("expected u256");
    };
    assert_eq!(
        decode_u64_u256(&current),
        50,
        "failed envelope must not partially apply mutations"
    );

    let row2 = db
        .query_with_options(
            "p",
            "app",
            Query::select(&["balance"])
                .from("accounts")
                .where_(Expr::Eq("id".into(), Value::Integer(2))),
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
        .expect("row2");
    let Value::U256(current2) = row2.values[0] else {
        panic!("expected u256");
    };
    assert_eq!(
        decode_u64_u256(&current2),
        25,
        "failed envelope must not apply later mutations either"
    );
}

#[tokio::test]
async fn integration_table_u256_overflow_rejected() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "p".into(),
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

    // Setup: balance = U256::MAX - 50
    let max_minus_50 = U256::MAX - U256::from(50);
    let mut balance_be = [0u8; 32];
    max_minus_50.to_big_endian(&mut balance_be);

    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1), Value::U256(balance_be)]),
    })
    .await
    .expect("seed row with near-max balance");

    // Test preflight detects overflow
    let preflight = db
        .preflight(Mutation::TableIncU256 {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "accounts".into(),
            primary_key: vec![Value::Integer(1)],
            column: "balance".into(),
            amount_be: u256_be(100),
        })
        .await;
    assert!(
        matches!(
            preflight,
            PreflightResult::Err { ref reason } if reason == "overflow"
        ),
        "preflight should detect overflow, got {:?}",
        preflight
    );

    // Test actual commit rejects overflow
    let err = db
        .table_inc_u256(
            "p",
            "app",
            "accounts",
            vec![Value::Integer(1)],
            "balance",
            u256_be(100),
        )
        .await
        .expect_err("overflow should fail");
    // In parallel apply, errors are wrapped in Validation with context
    assert!(
        matches!(&err, AedbError::Overflow)
            || matches!(&err, AedbError::Validation(msg) if msg.contains("overflow")),
        "expected Overflow or Validation containing 'overflow', got {:?}",
        err
    );

    // Verify balance unchanged after overflow rejection
    let row = db
        .query_with_options(
            "p",
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
    let Value::U256(current) = row.values[0] else {
        panic!("expected u256");
    };
    let current_val = U256::from_big_endian(&current);
    assert_eq!(
        current_val, max_minus_50,
        "balance must remain unchanged after overflow rejection"
    );

    // Test that increment within bounds still works
    db.table_inc_u256(
        "p",
        "app",
        "accounts",
        vec![Value::Integer(1)],
        "balance",
        u256_be(10),
    )
    .await
    .expect("increment within bounds should succeed");

    let row = db
        .query_with_options(
            "p",
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
    let Value::U256(current) = row.values[0] else {
        panic!("expected u256");
    };
    let current_val = U256::from_big_endian(&current);
    assert_eq!(
        current_val,
        max_minus_50 + U256::from(10),
        "valid increment should apply correctly"
    );
}
