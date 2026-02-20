use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::commit::tx::{IdempotencyKey, ReadSet, TransactionEnvelope, WriteClass, WriteIntent};
use aedb::commit::validation::Mutation;
use aedb::config::AedbConfig;
use aedb::error::AedbError;
use aedb::offline;
use aedb::query::plan::ConsistencyMode;
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use tempfile::tempdir;

fn one_u256() -> [u8; 32] {
    let mut out = [0u8; 32];
    out[31] = 1;
    out
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 8,
        max_local_rejects: 0,
        .. ProptestConfig::default()
    })]

    #[test]
    fn prop_atomicity_no_partial_apply(
        suffix in prop::collection::vec(any::<u8>(), 4..16),
        value in prop::collection::vec(any::<u8>(), 1..64),
    ) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let outcome: Result<(), TestCaseError> = rt.block_on(async move {
            let dir = tempdir().expect("temp dir");
            let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
            db.create_project("p").await.expect("project");

            let mut key = b"atomic-prop:".to_vec();
            key.extend_from_slice(&suffix);
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
                                key: key.clone(),
                                value,
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
            assert!(matches!(err, AedbError::Underflow | AedbError::Validation(_)));

            let entry = db
                .kv_get_no_auth("p", "app", &key, ConsistencyMode::AtLatest)
                .await
                .expect("kv read");
            prop_assert!(entry.is_none());
            Ok(())
        });
        outcome?;
    }

    #[test]
    fn prop_idempotency_exactly_once_across_retries(
        key_seed in any::<u128>(),
        retries in 2u8..6,
        payload in prop::collection::vec(any::<u8>(), 1..48),
    ) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let outcome: Result<(), TestCaseError> = rt.block_on(async move {
            let dir = tempdir().expect("temp dir");
            let db = AedbInstance::open(AedbConfig::production([7u8; 32]), dir.path()).expect("open");
            db.create_project("p").await.expect("project");

            let idem = IdempotencyKey(key_seed.to_be_bytes());
            let envelope = TransactionEnvelope {
                caller: None,
                idempotency_key: Some(idem),
                write_class: WriteClass::Economic,
                assertions: Vec::new(),
                read_set: ReadSet::default(),
                write_intent: WriteIntent {
                    mutations: vec![Mutation::KvSet {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        key: b"idem-prop".to_vec(),
                        value: payload,
                    }],
                },
                base_seq: 0,
            };
            let first = db
                .commit_envelope(envelope.clone())
                .await
                .expect("first commit");
            for _ in 0..retries {
                let again = db
                    .commit_envelope(envelope.clone())
                    .await
                    .expect("idempotent retry");
                prop_assert_eq!(again.commit_seq, first.commit_seq);
            }
            Ok(())
        });
        outcome?;
    }

    #[test]
    fn prop_replay_determinism_snapshot_parity(
        seed in any::<u64>(),
        writes in 16usize..96,
    ) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let outcome: Result<(), TestCaseError> = rt.block_on(async move {
            let dir = tempdir().expect("temp dir");
            let dump_a_dir = tempdir().expect("dump a");
            let dump_b_dir = tempdir().expect("dump b");
            let dump_a = dump_a_dir.path().join("a.aedbdump");
            let dump_b = dump_b_dir.path().join("b.aedbdump");
            let config = AedbConfig::production([8u8; 32]);

            let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
            db.create_project("p").await.expect("project");
            for i in 0..writes {
                let key = format!("replay-prop:{seed}:{i}").into_bytes();
                let value = ((seed as usize) ^ i).to_be_bytes().to_vec();
                db.commit(Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key,
                    value,
                })
                .await
                .expect("commit");
            }
            db.shutdown().await.expect("shutdown");

            let report_a =
                offline::export_snapshot_dump(dir.path(), &config, &dump_a).expect("export a");
            let report_b =
                offline::export_snapshot_dump(dir.path(), &config, &dump_b).expect("export b");
            prop_assert_eq!(report_a.current_seq, report_b.current_seq);
            prop_assert_eq!(report_a.parity_checksum_hex, report_b.parity_checksum_hex);
            Ok(())
        });
        outcome?;
    }

    #[test]
    fn prop_secure_mode_rejects_unauthenticated_commits(project_suffix in 0u32..10_000) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let outcome: Result<(), TestCaseError> = rt.block_on(async move {
            let dir = tempdir().expect("temp dir");
            let db = AedbInstance::open_secure(AedbConfig::production([9u8; 32]), dir.path())
                .expect("open secure");
            let err = db
                .commit(Mutation::Ddl(DdlOperation::CreateProject {
                    owner_id: None,
                    if_not_exists: true,
                    project_id: format!("p-{project_suffix}"),
                }))
                .await
                .expect_err("secure mode must require authenticated caller");
            prop_assert!(matches!(err, AedbError::PermissionDenied(_)));
            Ok(())
        });
        outcome?;
    }
}
