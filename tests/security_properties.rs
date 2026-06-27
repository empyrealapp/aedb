#![allow(deprecated)]
use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::tx::{
    IdempotencyKey, ReadAssertion, ReadKey, ReadSet, ReadSetEntry, TransactionEnvelope, WriteClass,
    WriteIntent,
};
use aedb::commit::validation::{
    CompareOp, KvU64MissingPolicy, KvU64OverflowPolicy, KvU64UnderflowPolicy, Mutation,
};
use aedb::config::AedbConfig;
use aedb::error::AedbError;
use aedb::offline;
use aedb::permission::{CallerContext, Permission};
use aedb::query::plan::{ConsistencyMode, Query};
use std::sync::Arc;
use tempfile::tempdir;

fn one_u256() -> [u8; 32] {
    let mut out = [0u8; 32];
    out[31] = 1;
    out
}

fn u64_be(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

fn decode_u64(bytes: &[u8]) -> u64 {
    assert_eq!(bytes.len(), 8, "u64 values must use 8 bytes");
    let mut out = [0u8; 8];
    out.copy_from_slice(bytes);
    u64::from_be_bytes(out)
}

#[tokio::test]
async fn security_atomicity_no_partial_apply_on_envelope_failure() {
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let err = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![
                    Mutation::KvSet {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        key: b"must_not_persist".to_vec(),
                        value: b"x".to_vec(),
                    },
                    Mutation::KvDecU256 {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        key: b"missing-counter".to_vec(),
                        amount_be: one_u256(),
                    },
                ],
            },
            base_seq: 0,
        })
        .await
        .expect_err("envelope should fail atomically");
    assert!(matches!(
        err,
        AedbError::Underflow | AedbError::Validation(_)
    ));

    let entry = db
        .kv_get_no_auth("p", "app", b"must_not_persist", ConsistencyMode::AtLatest)
        .await
        .expect("kv read");
    assert!(entry.is_none(), "failing envelope must not partially apply");
}

#[tokio::test]
async fn security_stale_kv_read_set_cannot_overwrite_key() {
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let seed = db
        .commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"guarded".to_vec(),
            value: b"v1".to_vec(),
        })
        .await
        .expect("seed");

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"guarded".to_vec(),
        value: b"v2".to_vec(),
    })
    .await
    .expect("concurrent update");

    let err = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: ReadSet {
                points: vec![ReadSetEntry {
                    key: ReadKey::KvKey {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        key: b"guarded".to_vec(),
                    },
                    version_at_read: seed.commit_seq,
                }],
                ranges: Vec::new(),
            },
            write_intent: WriteIntent {
                mutations: vec![Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"guarded".to_vec(),
                    value: b"stale-overwrite".to_vec(),
                }],
            },
            base_seq: db
                .snapshot_probe(ConsistencyMode::AtLatest)
                .await
                .expect("probe"),
        })
        .await
        .expect_err("stale read-set overwrite must conflict");
    assert!(
        matches!(err, AedbError::Conflict(ref msg) if msg.contains("read set conflict")),
        "expected read set conflict, got {err:?}"
    );

    let current = db
        .kv_get_no_auth("p", "app", b"guarded", ConsistencyMode::AtLatest)
        .await
        .expect("read guarded")
        .expect("guarded value");
    assert_eq!(current.value, b"v2".to_vec());
}

#[tokio::test]
async fn security_stale_table_preflight_plan_cannot_overwrite_row() {
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "state".into(),
        owner_id: None,
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "value".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
        if_not_exists: false,
    }))
    .await
    .expect("create state table");
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "state".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1), Value::Integer(10)]),
    })
    .await
    .expect("seed row");

    let plan = db
        .preflight_plan(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "state".into(),
            primary_key: vec![Value::Integer(1)],
            row: Row::from_values(vec![Value::Integer(1), Value::Integer(20)]),
        })
        .await;
    assert!(plan.valid, "preflight plan should be valid");
    assert_eq!(plan.read_set.points.len(), 1);

    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "state".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1), Value::Integer(30)]),
    })
    .await
    .expect("concurrent state transition");

    let err = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: plan.read_set,
            write_intent: plan.write_intent,
            base_seq: plan.base_seq,
        })
        .await
        .expect_err("stale table plan must conflict");
    assert!(
        matches!(err, AedbError::Conflict(ref msg) if msg.contains("read set conflict")),
        "expected read set conflict, got {err:?}"
    );

    let rows = db
        .query("p", "app", Query::select(&["*"]).from("state").limit(10))
        .await
        .expect("query state");
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0].values.get(1), Some(&Value::Integer(30)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn security_parallel_asserted_state_transition_has_single_winner() {
    let dir = tempdir().expect("temp dir");
    let db = Arc::new(AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open"));
    db.create_project("p").await.expect("project");
    db.kv_set("p", "app", b"machine:1".to_vec(), b"open".to_vec())
        .await
        .expect("seed machine state");
    let base_seq = db
        .snapshot_probe(ConsistencyMode::AtLatest)
        .await
        .expect("probe");

    let mut tasks = Vec::new();
    for _ in 0..32 {
        let db = Arc::clone(&db);
        tasks.push(tokio::spawn(async move {
            db.commit_envelope(TransactionEnvelope {
                caller: None,
                idempotency_key: None,
                write_class: WriteClass::Standard,
                assertions: vec![ReadAssertion::KeyEquals {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"machine:1".to_vec(),
                    expected: b"open".to_vec(),
                }],
                read_set: ReadSet::default(),
                write_intent: WriteIntent {
                    mutations: vec![Mutation::KvSet {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        key: b"machine:1".to_vec(),
                        value: b"closed".to_vec(),
                    }],
                },
                base_seq,
            })
            .await
        }));
    }

    let mut applied = 0usize;
    let mut rejected = 0usize;
    for task in tasks {
        match task.await.expect("transition task") {
            Ok(_) => applied += 1,
            Err(AedbError::AssertionFailed { .. }) | Err(AedbError::Conflict(_)) => rejected += 1,
            Err(err) => panic!("unexpected transition error: {err:?}"),
        }
    }
    assert_eq!(applied, 1, "only one asserted transition may win");
    assert_eq!(rejected, 31, "all stale transitions must be rejected");

    let current = db
        .kv_get_no_auth("p", "app", b"machine:1", ConsistencyMode::AtLatest)
        .await
        .expect("read machine")
        .expect("machine state");
    assert_eq!(current.value, b"closed".to_vec());
}

#[tokio::test]
async fn security_multi_key_atomic_update_reverts_all_on_failure() {
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.kv_set("p", "app", b"from".to_vec(), u64_be(5).to_vec())
        .await
        .expect("seed from");
    db.kv_set("p", "app", b"to".to_vec(), u64_be(7).to_vec())
        .await
        .expect("seed to");

    let err = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![
                    Mutation::KvAddU64Ex {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        key: b"to".to_vec(),
                        amount_be: u64_be(10),
                        on_missing: KvU64MissingPolicy::Reject,
                        on_overflow: KvU64OverflowPolicy::Reject,
                    },
                    Mutation::KvSubU64Ex {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        key: b"from".to_vec(),
                        amount_be: u64_be(10),
                        on_missing: KvU64MissingPolicy::Reject,
                        on_underflow: KvU64UnderflowPolicy::Reject,
                    },
                ],
            },
            base_seq: db
                .snapshot_probe(ConsistencyMode::AtLatest)
                .await
                .expect("probe"),
        })
        .await
        .expect_err("underflow must abort the whole transaction");
    assert!(matches!(err, AedbError::Underflow));

    let from = db
        .kv_get_no_auth("p", "app", b"from", ConsistencyMode::AtLatest)
        .await
        .expect("read from")
        .expect("from key");
    let to = db
        .kv_get_no_auth("p", "app", b"to", ConsistencyMode::AtLatest)
        .await
        .expect("read to")
        .expect("to key");
    assert_eq!(decode_u64(&from.value), 5);
    assert_eq!(decode_u64(&to.value), 7);
}

#[tokio::test]
async fn security_postflight_check_reverts_prior_atomic_updates() {
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.kv_set("p", "app", b"balance".to_vec(), u64_be(5).to_vec())
        .await
        .expect("seed balance");
    db.kv_set("p", "app", b"ledger".to_vec(), u64_be(7).to_vec())
        .await
        .expect("seed ledger");

    let err = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![
                    Mutation::KvAddU64Ex {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        key: b"ledger".to_vec(),
                        amount_be: u64_be(1),
                        on_missing: KvU64MissingPolicy::Reject,
                        on_overflow: KvU64OverflowPolicy::Reject,
                    },
                    Mutation::KvSubU64Ex {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        key: b"balance".to_vec(),
                        amount_be: u64_be(5),
                        on_missing: KvU64MissingPolicy::Reject,
                        on_underflow: KvU64UnderflowPolicy::Reject,
                    },
                    Mutation::PostflightCheck {
                        assertions: vec![ReadAssertion::KeyCompare {
                            project_id: "p".into(),
                            scope_id: "app".into(),
                            key: b"balance".to_vec(),
                            op: CompareOp::Gt,
                            threshold: u64_be(0).to_vec(),
                        }],
                    },
                ],
            },
            base_seq: db
                .snapshot_probe(ConsistencyMode::AtLatest)
                .await
                .expect("probe"),
        })
        .await
        .expect_err("postflight check must abort the whole transaction");
    assert!(matches!(err, AedbError::AssertionFailed { .. }));

    let balance = db
        .kv_get_no_auth("p", "app", b"balance", ConsistencyMode::AtLatest)
        .await
        .expect("read balance")
        .expect("balance key");
    let ledger = db
        .kv_get_no_auth("p", "app", b"ledger", ConsistencyMode::AtLatest)
        .await
        .expect("read ledger")
        .expect("ledger key");
    assert_eq!(decode_u64(&balance.value), 5);
    assert_eq!(decode_u64(&ledger.value), 7);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn security_parallel_atomic_adds_do_not_lose_updates() {
    let dir = tempdir().expect("temp dir");
    let db = Arc::new(AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open"));
    db.create_project("p").await.expect("project");
    db.kv_set("p", "app", b"counter".to_vec(), u64_be(0).to_vec())
        .await
        .expect("seed counter");

    let mut tasks = Vec::new();
    for _ in 0..64 {
        let db = Arc::clone(&db);
        tasks.push(tokio::spawn(async move {
            db.commit(Mutation::KvAddU64Ex {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"counter".to_vec(),
                amount_be: u64_be(1),
                on_missing: KvU64MissingPolicy::Reject,
                on_overflow: KvU64OverflowPolicy::Reject,
            })
            .await
        }));
    }

    for task in tasks {
        task.await
            .expect("atomic add task")
            .expect("atomic add should commit");
    }
    let counter = db
        .kv_get_no_auth("p", "app", b"counter", ConsistencyMode::AtLatest)
        .await
        .expect("read counter")
        .expect("counter key");
    assert_eq!(decode_u64(&counter.value), 64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn security_hot_key_atomic_postflight_does_not_use_stale_precondition() {
    let dir = tempdir().expect("temp dir");
    let db = Arc::new(AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open"));
    db.create_project("p").await.expect("project");
    db.kv_set("p", "app", b"balance".to_vec(), u64_be(32).to_vec())
        .await
        .expect("seed balance");

    let mut tasks = Vec::new();
    for _ in 0..64 {
        let db = Arc::clone(&db);
        tasks.push(tokio::spawn(async move {
            db.commit_envelope(TransactionEnvelope {
                caller: None,
                idempotency_key: None,
                write_class: WriteClass::Standard,
                assertions: Vec::new(),
                read_set: ReadSet::default(),
                write_intent: WriteIntent {
                    mutations: vec![
                        Mutation::KvSubU64Ex {
                            project_id: "p".into(),
                            scope_id: "app".into(),
                            key: b"balance".to_vec(),
                            amount_be: u64_be(1),
                            on_missing: KvU64MissingPolicy::Reject,
                            on_underflow: KvU64UnderflowPolicy::NoOp,
                        },
                        Mutation::PostflightCheck {
                            assertions: vec![ReadAssertion::KeyCompare {
                                project_id: "p".into(),
                                scope_id: "app".into(),
                                key: b"balance".to_vec(),
                                op: CompareOp::Gt,
                                threshold: u64_be(0).to_vec(),
                            }],
                        },
                    ],
                },
                base_seq: db
                    .snapshot_probe(ConsistencyMode::AtLatest)
                    .await
                    .expect("probe"),
            })
            .await
        }));
    }

    let mut applied = 0usize;
    let mut rejected = 0usize;
    for task in tasks {
        match task.await.expect("hot-key debit task") {
            Ok(_) => applied += 1,
            Err(AedbError::AssertionFailed { .. }) => rejected += 1,
            Err(err) => panic!("unexpected hot-key debit error: {err:?}"),
        }
    }
    assert_eq!(applied, 31);
    assert_eq!(rejected, 33);

    let balance = db
        .kv_get_no_auth("p", "app", b"balance", ConsistencyMode::AtLatest)
        .await
        .expect("read balance")
        .expect("balance key");
    assert_eq!(decode_u64(&balance.value), 1);
}

#[tokio::test]
async fn security_idempotency_survives_restart_exactly_once() {
    let dir = tempdir().expect("temp dir");
    let config = AedbConfig::production([7u8; 32]);
    let db = AedbInstance::open_anonymous(config.clone(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let key = IdempotencyKey([4u8; 16]);
    let envelope = TransactionEnvelope {
        caller: None,
        idempotency_key: Some(key.clone()),
        write_class: WriteClass::Economic,
        assertions: Vec::new(),
        read_set: ReadSet::default(),
        write_intent: WriteIntent {
            mutations: vec![Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"idem-restart".to_vec(),
                value: b"v1".to_vec(),
            }],
        },
        base_seq: 0,
    };

    let first = db
        .commit_envelope(envelope.clone())
        .await
        .expect("first commit");
    let second = db
        .commit_envelope(envelope.clone())
        .await
        .expect("idempotent retry");
    assert_eq!(second.commit_seq, first.commit_seq);
    db.shutdown().await.expect("shutdown");
    drop(db);

    let reopened = AedbInstance::open_anonymous(config, dir.path()).expect("reopen");
    let third = reopened
        .commit_envelope(envelope)
        .await
        .expect("idempotent retry after restart");
    assert_eq!(third.commit_seq, first.commit_seq);
}

#[tokio::test]
async fn security_idempotency_rejects_same_key_for_different_request() {
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let key = IdempotencyKey([6u8; 16]);
    let first = TransactionEnvelope {
        caller: None,
        idempotency_key: Some(key.clone()),
        write_class: WriteClass::Standard,
        assertions: Vec::new(),
        read_set: ReadSet::default(),
        write_intent: WriteIntent {
            mutations: vec![Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"idem-mismatch".to_vec(),
                value: b"v1".to_vec(),
            }],
        },
        base_seq: 0,
    };
    db.commit_envelope(first).await.expect("first commit");

    let err = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: Some(key),
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"idem-mismatch".to_vec(),
                    value: b"v2".to_vec(),
                }],
            },
            base_seq: 0,
        })
        .await
        .expect_err("reusing key for different payload must fail");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[tokio::test]
async fn security_idempotency_is_scoped_to_caller() {
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    for caller_id in ["alice", "bob"] {
        db.commit(Mutation::Ddl(
            aedb::catalog::DdlOperation::GrantPermission {
                actor_id: None,
                delegable: false,
                caller_id: caller_id.into(),
                permission: Permission::KvWrite {
                    project_id: "p".into(),
                    scope_id: Some("app".into()),
                    prefix: None,
                },
            },
        ))
        .await
        .expect("grant kv write");
    }

    let key = IdempotencyKey([7u8; 16]);
    let first = TransactionEnvelope {
        caller: Some(CallerContext::new("alice")),
        idempotency_key: Some(key.clone()),
        write_class: WriteClass::Standard,
        assertions: Vec::new(),
        read_set: ReadSet::default(),
        write_intent: WriteIntent {
            mutations: vec![Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"idem-caller".to_vec(),
                value: b"v1".to_vec(),
            }],
        },
        base_seq: 0,
    };
    let first = db.commit_envelope(first).await.expect("first commit");

    let second = db
        .commit_envelope(TransactionEnvelope {
            caller: Some(CallerContext::new("bob")),
            idempotency_key: Some(key),
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"idem-caller".to_vec(),
                    value: b"v1".to_vec(),
                }],
            },
            base_seq: 0,
        })
        .await
        .expect("same key under different caller should apply independently");
    assert!(matches!(
        first.idempotency,
        aedb::commit::executor::IdempotencyOutcome::Applied
    ));
    assert!(matches!(
        second.idempotency,
        aedb::commit::executor::IdempotencyOutcome::Applied
    ));
    assert_ne!(first.commit_seq, second.commit_seq);
}

#[tokio::test]
async fn security_replay_is_deterministic_via_snapshot_parity() {
    let dir = tempdir().expect("temp dir");
    let dump_a = tempdir().expect("dump a");
    let dump_b = tempdir().expect("dump b");
    let dump_a_file = dump_a.path().join("state-a.aedbdump");
    let dump_b_file = dump_b.path().join("state-b.aedbdump");
    let config = AedbConfig::production([8u8; 32]);

    let db = AedbInstance::open_anonymous(config.clone(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    for i in 0..500u64 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("replay:{i}").into_bytes(),
            value: i.to_be_bytes().to_vec(),
        })
        .await
        .expect("commit");
    }
    db.shutdown().await.expect("shutdown");

    let report_a =
        offline::export_snapshot_dump(dir.path(), &config, &dump_a_file).expect("export a");
    let report_b =
        offline::export_snapshot_dump(dir.path(), &config, &dump_b_file).expect("export b");
    assert_eq!(report_a.current_seq, report_b.current_seq);
    assert_eq!(
        report_a.parity_checksum_hex, report_b.parity_checksum_hex,
        "replay parity must be deterministic"
    );
}

#[tokio::test]
async fn security_secure_mode_enforces_authenticated_commit_calls() {
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open_secure(AedbConfig::production([9u8; 32]), dir.path())
        .expect("open secure");

    let err = db
        .commit(Mutation::Ddl(aedb::catalog::DdlOperation::CreateProject {
            owner_id: None,
            if_not_exists: true,
            project_id: "p".into(),
        }))
        .await
        .expect_err("anonymous commit should be rejected in secure mode");
    assert!(matches!(err, AedbError::PermissionDenied(_)));
}
