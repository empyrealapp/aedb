use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::tx::{
    IdempotencyKey, ReadAssertion, TransactionEnvelope, WriteClass, WriteIntent,
};
use aedb::commit::validation::{CompareOp, Mutation};
use aedb::error::AedbError;
use aedb::query::plan::{ConsistencyMode, Expr, Query};
use tempfile::tempdir;

fn u256_be(value: u64) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[24..].copy_from_slice(&value.to_be_bytes());
    bytes
}

#[tokio::test]
async fn integration_kv_keycompare_assertion_guards_debit() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.kv_set("p", "app", b"balance".to_vec(), u256_be(100).to_vec())
        .await
        .expect("seed");

    db.commit_envelope(TransactionEnvelope {
        caller: None,
        idempotency_key: None,
        write_class: WriteClass::Standard,
        assertions: vec![ReadAssertion::KeyCompare {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"balance".to_vec(),
            op: CompareOp::Gte,
            threshold: u256_be(80).to_vec(),
        }],
        read_set: Default::default(),
        write_intent: WriteIntent {
            mutations: vec![Mutation::KvDecU256 {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"balance".to_vec(),
                amount_be: u256_be(80),
            }],
        },
        base_seq: db
            .snapshot_probe(ConsistencyMode::AtLatest)
            .await
            .expect("probe"),
    })
    .await
    .expect("debit with sufficient balance");

    let err = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: vec![ReadAssertion::KeyCompare {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"balance".to_vec(),
                op: CompareOp::Gte,
                threshold: u256_be(80).to_vec(),
            }],
            read_set: Default::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::KvDecU256 {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"balance".to_vec(),
                    amount_be: u256_be(80),
                }],
            },
            base_seq: db
                .snapshot_probe(ConsistencyMode::AtLatest)
                .await
                .expect("probe"),
        })
        .await
        .expect_err("insufficient balance should fail assertion");
    assert!(matches!(err, AedbError::AssertionFailed { .. }));
}

#[tokio::test]
async fn integration_rowversion_assertion_detects_stale_state() {
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
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("create");
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1), Value::Integer(100)]),
    })
    .await
    .expect("seed row");

    let plan = db
        .preflight_plan(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "accounts".into(),
            primary_key: vec![Value::Integer(1)],
            row: Row::from_values(vec![Value::Integer(1), Value::Integer(90)]),
        })
        .await;
    let original_version = plan.read_set.points[0].version_at_read;

    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1), Value::Integer(80)]),
    })
    .await
    .expect("concurrent update");

    let err = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: vec![ReadAssertion::RowVersion {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "accounts".into(),
                primary_key: vec![Value::Integer(1)],
                expected_seq: original_version,
            }],
            read_set: Default::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::Upsert {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    table_name: "accounts".into(),
                    primary_key: vec![Value::Integer(1)],
                    row: Row::from_values(vec![Value::Integer(1), Value::Integer(70)]),
                }],
            },
            base_seq: db
                .snapshot_probe(ConsistencyMode::AtLatest)
                .await
                .expect("probe"),
        })
        .await
        .expect_err("stale version must fail");
    assert!(matches!(err, AedbError::AssertionFailed { .. }));
}

#[tokio::test]
async fn integration_countcompare_assertion_enforces_capacity() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "lobby_players".into(),
        owner_id: None,
        if_not_exists: false,
        columns: vec![
            ColumnDef {
                name: "lobby_id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "player_id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        primary_key: vec!["lobby_id".into(), "player_id".into()],
    }))
    .await
    .expect("create");
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "lobby_players".into(),
        primary_key: vec![Value::Integer(42), Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(42), Value::Integer(1)]),
    })
    .await
    .expect("seed 1");

    for player_id in [2i64, 3i64] {
        let result = db
            .commit_envelope(TransactionEnvelope {
                caller: None,
                idempotency_key: None,
                write_class: WriteClass::Standard,
                assertions: vec![ReadAssertion::CountCompare {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    table_name: "lobby_players".into(),
                    filter: Some(Expr::Eq("lobby_id".into(), Value::Integer(42))),
                    op: CompareOp::Lt,
                    threshold: 2,
                }],
                read_set: Default::default(),
                write_intent: WriteIntent {
                    mutations: vec![Mutation::Upsert {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        table_name: "lobby_players".into(),
                        primary_key: vec![Value::Integer(42), Value::Integer(player_id)],
                        row: Row::from_values(vec![Value::Integer(42), Value::Integer(player_id)]),
                    }],
                },
                base_seq: db
                    .snapshot_probe(ConsistencyMode::AtLatest)
                    .await
                    .expect("probe"),
            })
            .await;
        if player_id == 2 {
            result.expect("second join should pass");
        } else {
            assert!(matches!(result, Err(AedbError::AssertionFailed { .. })));
        }
    }
}

#[tokio::test]
async fn integration_sumcompare_assertion_blocks_over_budget() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "p".into(),
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
                name: "player_id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "amount".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("create");

    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "bets".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![
            Value::Integer(1),
            Value::Integer(7),
            Value::Integer(60),
        ]),
    })
    .await
    .expect("seed");

    let err = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: vec![ReadAssertion::SumCompare {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "bets".into(),
                column: "amount".into(),
                filter: Some(Expr::Eq("player_id".into(), Value::Integer(7))),
                op: CompareOp::Lt,
                threshold: Value::Integer(50),
            }],
            read_set: Default::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::Upsert {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    table_name: "bets".into(),
                    primary_key: vec![Value::Integer(2)],
                    row: Row::from_values(vec![
                        Value::Integer(2),
                        Value::Integer(7),
                        Value::Integer(5),
                    ]),
                }],
            },
            base_seq: db
                .snapshot_probe(ConsistencyMode::AtLatest)
                .await
                .expect("probe"),
        })
        .await
        .expect_err("sum assertion should fail");
    assert!(matches!(err, AedbError::AssertionFailed { .. }));
}

#[tokio::test]
async fn integration_composite_any_assertion_short_circuit() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.kv_set("p", "app", b"a".to_vec(), b"1".to_vec())
        .await
        .expect("set a");

    db.commit_envelope(TransactionEnvelope {
        caller: None,
        idempotency_key: None,
        write_class: WriteClass::Standard,
        assertions: vec![ReadAssertion::Any(vec![
            ReadAssertion::KeyEquals {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"a".to_vec(),
                expected: b"1".to_vec(),
            },
            ReadAssertion::KeyEquals {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"b".to_vec(),
                expected: b"x".to_vec(),
            },
        ])],
        read_set: Default::default(),
        write_intent: WriteIntent {
            mutations: vec![Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"ok".to_vec(),
                value: b"yes".to_vec(),
            }],
        },
        base_seq: db
            .snapshot_probe(ConsistencyMode::AtLatest)
            .await
            .expect("probe"),
    })
    .await
    .expect("any assertion should pass");
}

#[tokio::test]
async fn integration_failed_assertion_is_logged_to_system_audit_table() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.kv_set("p", "app", b"balance".to_vec(), u256_be(50).to_vec())
        .await
        .expect("seed balance");

    let err = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: vec![ReadAssertion::KeyCompare {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"balance".to_vec(),
                op: CompareOp::Gte,
                threshold: u256_be(80).to_vec(),
            }],
            read_set: Default::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"should_not_write".to_vec(),
                    value: b"x".to_vec(),
                }],
            },
            base_seq: db
                .snapshot_probe(ConsistencyMode::AtLatest)
                .await
                .expect("probe"),
        })
        .await
        .expect_err("assertion should fail");
    assert!(matches!(err, AedbError::AssertionFailed { .. }));

    db.commit_envelope(TransactionEnvelope {
        caller: None,
        idempotency_key: None,
        write_class: WriteClass::Standard,
        assertions: vec![ReadAssertion::KeyExists {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"should_not_write".to_vec(),
            expected: false,
        }],
        read_set: Default::default(),
        write_intent: WriteIntent {
            mutations: vec![Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"proof_of_no_side_effect".to_vec(),
                value: b"ok".to_vec(),
            }],
        },
        base_seq: db
            .snapshot_probe(ConsistencyMode::AtLatest)
            .await
            .expect("probe"),
    })
    .await
    .expect("failed assertion must block all mutations");

    let audit = db
        .query(
            "_system",
            "app",
            Query::select(&["*"]).from("assertion_audit").limit(20),
        )
        .await
        .expect("query assertion audit");
    assert!(
        !audit.rows.is_empty(),
        "failed assertion must emit an assertion_audit row"
    );

    let row = &audit.rows[0];
    assert_eq!(row.values.get(3), Some(&Value::Integer(0)));
    assert_eq!(
        row.values.get(6),
        Some(&Value::Text("assertion_failed".into()))
    );
}

#[tokio::test]
async fn integration_idempotent_retry_skips_assertion_re_evaluation() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.kv_set("p", "app", b"balance".to_vec(), u256_be(100).to_vec())
        .await
        .expect("seed");

    let envelope = TransactionEnvelope {
        caller: None,
        idempotency_key: Some(IdempotencyKey([7u8; 16])),
        write_class: WriteClass::Standard,
        assertions: vec![ReadAssertion::KeyCompare {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"balance".to_vec(),
            op: CompareOp::Gte,
            threshold: u256_be(80).to_vec(),
        }],
        read_set: Default::default(),
        write_intent: WriteIntent {
            mutations: vec![Mutation::KvDecU256 {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"balance".to_vec(),
                amount_be: u256_be(80),
            }],
        },
        base_seq: db
            .snapshot_probe(ConsistencyMode::AtLatest)
            .await
            .expect("probe"),
    };

    let first = db
        .commit_envelope(envelope.clone())
        .await
        .expect("first commit should pass");
    db.kv_set("p", "app", b"balance".to_vec(), u256_be(0).to_vec())
        .await
        .expect("force assertion to fail if re-evaluated");

    let retry = db
        .commit_envelope(envelope)
        .await
        .expect("idempotent retry must return cached success");
    assert_eq!(retry.commit_seq, first.commit_seq);

    db.commit_envelope(TransactionEnvelope {
        caller: None,
        idempotency_key: None,
        write_class: WriteClass::Standard,
        assertions: vec![ReadAssertion::KeyEquals {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"balance".to_vec(),
            expected: u256_be(0).to_vec(),
        }],
        read_set: Default::default(),
        write_intent: WriteIntent {
            mutations: vec![Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"idempotency_proof".to_vec(),
                value: b"ok".to_vec(),
            }],
        },
        base_seq: db
            .snapshot_probe(ConsistencyMode::AtLatest)
            .await
            .expect("probe"),
    })
    .await
    .expect("retry must not re-apply debit");
}
