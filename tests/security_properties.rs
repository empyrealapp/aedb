use aedb::AedbInstance;
use aedb::commit::tx::{IdempotencyKey, ReadSet, TransactionEnvelope, WriteClass, WriteIntent};
use aedb::commit::validation::Mutation;
use aedb::config::AedbConfig;
use aedb::error::AedbError;
use aedb::offline;
use aedb::permission::{CallerContext, Permission};
use aedb::query::plan::ConsistencyMode;
use tempfile::tempdir;

fn one_u256() -> [u8; 32] {
    let mut out = [0u8; 32];
    out[31] = 1;
    out
}

#[tokio::test]
async fn security_atomicity_no_partial_apply_on_envelope_failure() {
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
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
async fn security_idempotency_survives_restart_exactly_once() {
    let dir = tempdir().expect("temp dir");
    let config = AedbConfig::production([7u8; 32]);
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
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

    let reopened = AedbInstance::open(config, dir.path()).expect("reopen");
    let third = reopened
        .commit_envelope(envelope)
        .await
        .expect("idempotent retry after restart");
    assert_eq!(third.commit_seq, first.commit_seq);
}

#[tokio::test]
async fn security_idempotency_rejects_same_key_for_different_request() {
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
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
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
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

    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
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
