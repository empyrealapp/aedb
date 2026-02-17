use super::{CommitExecutor, CommitRequest};
use crate::catalog::Catalog;
use crate::catalog::DdlOperation;
use crate::catalog::namespace_key;
use crate::catalog::schema::{
    ColumnDef, Constraint, ForeignKey, ForeignKeyAction, TableAlteration,
};
use crate::catalog::types::{ColumnType, Row, Value};
use crate::commit::tx::{
    ReadAssertion, ReadBound, ReadKey, ReadRange, ReadRangeEntry, ReadSet, ReadSetEntry,
    TransactionEnvelope, WriteClass, WriteIntent,
};
use crate::commit::validation::{ConflictAction, ConflictTarget, Mutation, UpdateExpr};
use crate::config::AedbConfig;
use crate::error::AedbError;
use crate::query::plan::Expr;
use crate::storage::encoded_key::EncodedKey;
use crate::storage::keyspace::{Keyspace, NamespaceId, SecondaryIndexStore};
use crate::wal::frame::FrameReader;
use primitive_types::U256;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fs;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use tokio::sync::{mpsc, oneshot};

fn base_row(id: i64, name: &str) -> Row {
    Row {
        values: vec![Value::Integer(id), Value::Text(name.to_string().into())],
    }
}

fn u256_value(v: u64) -> Value {
    let mut out = [0u8; 32];
    U256::from(v).to_big_endian(&mut out);
    Value::U256(out)
}

fn kv_request(scope_id: &str, key: &[u8]) -> CommitRequest {
    request_with_mutations(vec![Mutation::KvSet {
        project_id: "p".into(),
        scope_id: scope_id.into(),
        key: key.to_vec(),
        value: b"v".to_vec(),
    }])
}

fn request_with_mutations(mutations: Vec<Mutation>) -> CommitRequest {
    let (result_tx, _result_rx) = oneshot::channel();
    let envelope = TransactionEnvelope {
        caller: None,
        idempotency_key: None,
        write_class: WriteClass::Standard,
        assertions: Vec::new(),
        read_set: ReadSet::default(),
        write_intent: WriteIntent { mutations },
        base_seq: 0,
    };
    let write_partitions = super::derive_write_partitions(&envelope.write_intent.mutations);
    CommitRequest {
        envelope,
        encoded_len: 0,
        enqueue_micros: 0,
        prevalidated: false,
        assertions_engine_verified: false,
        write_partitions,
        read_partitions: HashSet::new(),
        defer_count: 0,
        result_tx,
    }
}

fn scope_of(req: &CommitRequest) -> &str {
    let mutation = req
        .envelope
        .write_intent
        .mutations
        .first()
        .expect("mutation");
    match mutation {
        Mutation::KvSet { scope_id, .. } => scope_id.as_str(),
        _ => panic!("expected kv mutation"),
    }
}

#[test]
fn canonical_partition_order_is_sorted() {
    let mut set = HashSet::new();
    set.insert("p::z".to_string());
    set.insert("p::a".to_string());
    set.insert("p::m".to_string());
    let ordered = super::canonical_partition_order(&set);
    assert_eq!(
        ordered,
        vec!["p::a".to_string(), "p::m".to_string(), "p::z".to_string()]
    );
}

#[test]
fn adaptive_epoch_state_scales_under_backlog() {
    let config = AedbConfig {
        adaptive_epoch_enabled: true,
        adaptive_epoch_min_commits_floor: 1,
        adaptive_epoch_min_commits_ceiling: 8,
        adaptive_epoch_wait_us_floor: 10,
        adaptive_epoch_wait_us_ceiling: 200,
        adaptive_epoch_target_latency_us: 5_000,
        epoch_min_commits: 1,
        epoch_max_commits: 16,
        epoch_max_wait_us: 100,
        ..AedbConfig::default()
    };
    let mut state = super::adaptive::AdaptiveEpochState::from_config(&config);
    for _ in 0..8 {
        state.observe_epoch(&config, 4, Duration::from_micros(2_000), false, 64);
    }
    let (_, min_commits, _) = state.epoch_params(&config, 64);
    assert!(min_commits > 1);
}

#[test]
fn adaptive_epoch_state_backs_off_on_error() {
    let config = AedbConfig {
        adaptive_epoch_enabled: true,
        adaptive_epoch_min_commits_floor: 1,
        adaptive_epoch_min_commits_ceiling: 16,
        adaptive_epoch_wait_us_floor: 10,
        adaptive_epoch_wait_us_ceiling: 5_000,
        adaptive_epoch_target_latency_us: 500,
        epoch_min_commits: 8,
        epoch_max_commits: 16,
        epoch_max_wait_us: 20,
        ..AedbConfig::default()
    };
    let mut state = super::adaptive::AdaptiveEpochState::from_config(&config);
    state.observe_epoch(&config, 8, Duration::from_micros(40_000), true, 0);
    let (wait_us, min_commits, _) = state.epoch_params(&config, 0);
    assert!(min_commits < 8);
    assert!(wait_us >= 20);
}

#[test]
fn coordinator_lock_manager_times_out_when_held() {
    let mgr = super::coordinator::CoordinatorLockManager::default();
    let held = vec!["p::app".to_string()];
    let guard = mgr
        .acquire_all(&held, Duration::from_millis(25))
        .expect("initial lock");
    let err = match mgr.acquire_all(&held, Duration::from_millis(2)) {
        Ok(_) => panic!("second lock should time out"),
        Err(err) => err,
    };
    drop(guard);
    assert!(matches!(err, AedbError::PartitionLockTimeout));
}

#[test]
fn coordinator_lock_manager_handles_inverse_partition_order_without_deadlock() {
    let mgr = Arc::new(super::coordinator::CoordinatorLockManager::default());
    let tx1_set = HashSet::from(["p::a".to_string(), "p::b".to_string()]);
    let tx2_set = HashSet::from(["p::b".to_string(), "p::a".to_string()]);
    let tx1_order = super::canonical_partition_order(&tx1_set);
    let tx2_order = super::canonical_partition_order(&tx2_set);
    assert_eq!(tx1_order, tx2_order);

    let first = Arc::clone(&mgr);
    let t1 = std::thread::spawn(move || {
        let _guard = first
            .acquire_all(&tx1_order, Duration::from_millis(200))
            .expect("first lock");
        std::thread::sleep(Duration::from_millis(20));
    });

    std::thread::sleep(Duration::from_millis(5));
    let second = Arc::clone(&mgr);
    let t2 = std::thread::spawn(move || {
        second
            .acquire_all(&tx2_order, Duration::from_millis(200))
            .expect("second lock");
    });

    t1.join().expect("first join");
    t2.join().expect("second join");
}

#[tokio::test]
async fn cross_partition_commit_uses_coordinator_and_applies_all_mutations() {
    let dir = tempdir().expect("temp");
    let exec = CommitExecutor::new(dir.path()).expect("executor");
    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "p".into(),
    }))
    .await
    .expect("project");
    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "q".into(),
    }))
    .await
    .expect("project q");
    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "q".into(),
    }))
    .await
    .expect("project q");
    exec.submit(Mutation::Ddl(DdlOperation::CreateScope {
        owner_id: None,
        if_not_exists: true,
        project_id: "p".into(),
        scope_id: "other".into(),
    }))
    .await
    .expect("scope");

    let base_seq = exec.current_seq().await;
    let result = exec
        .submit_envelope(TransactionEnvelope {
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
                        key: b"x:a".to_vec(),
                        value: b"1".to_vec(),
                    },
                    Mutation::KvSet {
                        project_id: "p".into(),
                        scope_id: "other".into(),
                        key: b"x:b".to_vec(),
                        value: b"2".to_vec(),
                    },
                ],
            },
            base_seq,
        })
        .await
        .expect("coordinator commit");
    assert!(result.commit_seq > base_seq);

    let (snapshot, _, _) = exec.snapshot_state().await;
    assert_eq!(
        snapshot.kv_get("p", "app", b"x:a").map(|e| e.value.clone()),
        Some(b"1".to_vec())
    );
    assert_eq!(
        snapshot
            .kv_get("p", "other", b"x:b")
            .map(|e| e.value.clone()),
        Some(b"2".to_vec())
    );
}

#[tokio::test]
async fn coordinator_timeout_rejects_cross_partition_without_side_effects() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        partition_lock_timeout_ms: 2,
        ..AedbConfig::default()
    };
    let exec = CommitExecutor::with_state(
        dir.path(),
        Keyspace::default(),
        Catalog::default(),
        0,
        1,
        config,
        HashMap::new(),
    )
    .expect("executor");
    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "p".into(),
    }))
    .await
    .expect("project");
    exec.submit(Mutation::Ddl(DdlOperation::CreateScope {
        owner_id: None,
        if_not_exists: true,
        project_id: "p".into(),
        scope_id: "other".into(),
    }))
    .await
    .expect("scope");

    let before_seq = exec.current_seq().await;
    super::COORDINATOR_TEST_DELAY_MS.store(5, Ordering::Relaxed);
    let result = exec
        .submit_envelope(TransactionEnvelope {
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
                        key: b"t:a".to_vec(),
                        value: b"1".to_vec(),
                    },
                    Mutation::KvSet {
                        project_id: "p".into(),
                        scope_id: "other".into(),
                        key: b"t:b".to_vec(),
                        value: b"2".to_vec(),
                    },
                ],
            },
            base_seq: before_seq,
        })
        .await;
    super::COORDINATOR_TEST_DELAY_MS.store(0, Ordering::Relaxed);

    let err = result.expect_err("must timeout");
    assert!(matches!(err, AedbError::PartitionLockTimeout));
    assert_eq!(exec.current_seq().await, before_seq);
    let (snapshot, _, _) = exec.snapshot_state().await;
    assert!(snapshot.kv_get("p", "app", b"t:a").is_none());
    assert!(snapshot.kv_get("p", "other", b"t:b").is_none());
}

#[tokio::test]
async fn global_unique_hash_rejects_duplicate_across_scopes_via_coordinator() {
    let dir = tempdir().expect("temp");
    let exec = CommitExecutor::new(dir.path()).expect("executor");

    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "_global".into(),
    }))
    .await
    .expect("global project");
    exec.submit(Mutation::Ddl(DdlOperation::CreateScope {
        owner_id: None,
        if_not_exists: true,
        project_id: "_global".into(),
        scope_id: "a".into(),
    }))
    .await
    .expect("scope a");
    exec.submit(Mutation::Ddl(DdlOperation::CreateScope {
        owner_id: None,
        if_not_exists: true,
        project_id: "_global".into(),
        scope_id: "b".into(),
    }))
    .await
    .expect("scope b");

    for scope in ["a", "b"] {
        exec.submit(Mutation::Ddl(DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: false,
            project_id: "_global".into(),
            scope_id: scope.into(),
            table_name: "users".into(),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "email".into(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
            ],
            primary_key: vec!["id".into()],
        }))
        .await
        .expect("users table");
        exec.submit(Mutation::Ddl(DdlOperation::CreateIndex {
            project_id: "_global".into(),
            scope_id: scope.into(),
            table_name: "users".into(),
            index_name: "uq_email".into(),
            if_not_exists: false,
            columns: vec!["email".into()],
            index_type: crate::catalog::schema::IndexType::UniqueHash,
            partial_filter: None,
        }))
        .await
        .expect("unique index");
    }

    exec.submit(Mutation::Upsert {
        project_id: "_global".into(),
        scope_id: "a".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Text("alice@x.com".into())],
        },
    })
    .await
    .expect("insert scope a");

    let err = exec
        .submit(Mutation::Upsert {
            project_id: "_global".into(),
            scope_id: "b".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(2)],
            row: Row {
                values: vec![Value::Integer(2), Value::Text("alice@x.com".into())],
            },
        })
        .await
        .expect_err("cross-scope duplicate must fail");
    assert!(
        matches!(err, AedbError::Validation(msg) if msg.contains("global unique constraint violation"))
    );
}

#[tokio::test]
async fn parallel_worker_timeout_rejects_entire_epoch_without_state_publish() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        epoch_apply_timeout_ms: 5,
        ..AedbConfig::default()
    };
    let exec = CommitExecutor::with_state(
        dir.path(),
        Keyspace::default(),
        Catalog::default(),
        0,
        1,
        config,
        HashMap::new(),
    )
    .expect("executor");

    let req_a = kv_request("app", b"__slow_parallel_worker__");
    let req_b = kv_request("other", b"pw:b");
    let mut state = exec.state.lock().await;
    let before_seq = state.current_seq;
    let epoch = super::process_commit_epoch(&mut state, vec![req_a, req_b]);
    let outcomes = epoch.outcomes;

    assert_eq!(outcomes.len(), 2);
    assert!(outcomes.iter().all(|o| matches!(
        &o.result,
        Err(AedbError::EpochApplyTimeout | AedbError::ParallelApplyCancelled)
    )));
    assert_eq!(state.current_seq, before_seq);
    assert!(state.keyspace.kv_get("p", "app", b"pw:a").is_none());
    assert!(state.keyspace.kv_get("p", "other", b"pw:b").is_none());
}

#[tokio::test]
async fn parallel_worker_panic_rejects_entire_epoch_without_state_publish() {
    let dir = tempdir().expect("temp");
    let exec = CommitExecutor::with_state(
        dir.path(),
        Keyspace::default(),
        Catalog::default(),
        0,
        1,
        AedbConfig::default(),
        HashMap::new(),
    )
    .expect("executor");

    let req_a = kv_request("app", b"__panic_parallel_worker__");
    let req_b = kv_request("other", b"pp:b");
    let mut state = exec.state.lock().await;
    let before_seq = state.current_seq;
    let epoch = super::process_commit_epoch(&mut state, vec![req_a, req_b]);
    let outcomes = epoch.outcomes;

    assert_eq!(outcomes.len(), 2);
    assert!(outcomes.iter().all(|o| matches!(
        &o.result,
        Err(AedbError::ParallelApplyWorkerPanicked | AedbError::ParallelApplyCancelled)
    )));
    assert_eq!(state.current_seq, before_seq);
    assert!(state.keyspace.kv_get("p", "app", b"pp:a").is_none());
    assert!(state.keyspace.kv_get("p", "other", b"pp:b").is_none());
}

#[test]
fn derive_write_partitions_uses_collision_free_namespace_key() {
    let partitions = super::derive_write_partitions(&[
        Mutation::KvSet {
            project_id: "a:b".into(),
            scope_id: "c".into(),
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
        },
        Mutation::KvSet {
            project_id: "a".into(),
            scope_id: "b:c".into(),
            key: b"k2".to_vec(),
            value: b"v2".to_vec(),
        },
    ]);
    assert_eq!(partitions.len(), 2);
    assert!(partitions.contains(&format!("k:{}:6b31", namespace_key("a:b", "c"))));
    assert!(partitions.contains(&format!("k:{}:6b32", namespace_key("a", "b:c"))));
}

#[test]
fn derive_write_partitions_is_table_granular_within_scope() {
    let partitions = super::derive_write_partitions(&[
        Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(1)],
            row: base_row(1, "a"),
        },
        Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "orders".into(),
            primary_key: vec![Value::Integer(1)],
            row: Row {
                values: vec![Value::Integer(1), Value::Text("o".into())],
            },
        },
    ]);
    assert_eq!(partitions.len(), 2);
    let ns = namespace_key("p", "app");
    assert!(partitions.contains(&format!("t:{ns}:users")));
    assert!(partitions.contains(&format!("t:{ns}:orders")));
}

#[test]
fn derive_read_partitions_uses_collision_free_namespace_key() {
    let envelope = TransactionEnvelope {
        caller: None,
        idempotency_key: None,
        write_class: WriteClass::Standard,
        assertions: Vec::new(),
        read_set: ReadSet {
            points: vec![
                ReadSetEntry {
                    key: ReadKey::KvKey {
                        project_id: "a:b".into(),
                        scope_id: "c".into(),
                        key: b"k1".to_vec(),
                    },
                    version_at_read: 1,
                },
                ReadSetEntry {
                    key: ReadKey::KvKey {
                        project_id: "a".into(),
                        scope_id: "b:c".into(),
                        key: b"k2".to_vec(),
                    },
                    version_at_read: 1,
                },
            ],
            ranges: vec![
                ReadRangeEntry {
                    range: ReadRange::KvRange {
                        project_id: "a:b".into(),
                        scope_id: "c".into(),
                        start: ReadBound::Unbounded,
                        end: ReadBound::Unbounded,
                    },
                    max_version_at_read: 1,
                    structural_version_at_read: 1,
                },
                ReadRangeEntry {
                    range: ReadRange::KvRange {
                        project_id: "a".into(),
                        scope_id: "b:c".into(),
                        start: ReadBound::Unbounded,
                        end: ReadBound::Unbounded,
                    },
                    max_version_at_read: 1,
                    structural_version_at_read: 1,
                },
            ],
        },
        write_intent: WriteIntent { mutations: vec![] },
        base_seq: 1,
    };
    let partitions = super::derive_read_partitions(&envelope);
    assert_eq!(partitions.len(), 4);
    assert!(partitions.contains(&format!("k:{}:6b31", namespace_key("a:b", "c"))));
    assert!(partitions.contains(&format!("k:{}:6b32", namespace_key("a", "b:c"))));
    assert!(partitions.contains(&format!("kns:{}", namespace_key("a:b", "c"))));
    assert!(partitions.contains(&format!("kns:{}", namespace_key("a", "b:c"))));
}

#[test]
fn assertion_read_dependency_conflicts_with_write_token_in_epoch_selection() {
    let (result_tx, _result_rx) = oneshot::channel();
    let envelope = TransactionEnvelope {
        caller: None,
        idempotency_key: None,
        write_class: WriteClass::Standard,
        assertions: vec![ReadAssertion::KeyExists {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"balance".to_vec(),
            expected: true,
        }],
        read_set: ReadSet::default(),
        write_intent: WriteIntent {
            mutations: vec![Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"marker".to_vec(),
                value: b"v".to_vec(),
            }],
        },
        base_seq: 0,
    };
    let write_partitions = super::derive_write_partitions(&envelope.write_intent.mutations);
    let read_partitions = super::derive_read_partitions(&envelope);
    let candidate = CommitRequest {
        envelope,
        encoded_len: 0,
        enqueue_micros: 0,
        prevalidated: false,
        assertions_engine_verified: false,
        write_partitions,
        read_partitions,
        defer_count: 0,
        result_tx,
    };

    let mut pending = VecDeque::new();
    pending.push_back(candidate);
    let mut epoch_writes = HashSet::new();
    epoch_writes.insert(format!("k:{}:62616c616e6365", namespace_key("p", "app")));
    let idx = super::find_compatible_candidate_index(
        &pending,
        &epoch_writes,
        &HashSet::new(),
        false,
        false,
    );
    assert!(
        idx.is_none(),
        "assertion-derived read token must conflict with same-key write token"
    );
}

#[tokio::test]
async fn epoch_selection_can_coalesce_same_partition_writes() {
    let mut pending = VecDeque::new();
    pending.push_back(kv_request("s1", b"a"));
    pending.push_back(kv_request("s1", b"b"));
    pending.push_back(kv_request("s2", b"c"));

    let (tx, mut rx) = mpsc::channel(1);
    drop(tx);

    let selected = super::build_epoch_requests(
        &mut pending,
        1,
        3,
        Instant::now() + Duration::from_millis(50),
        &mut rx,
        true,
    )
    .await;

    assert_eq!(selected.len(), 3);
    assert_eq!(scope_of(&selected[0]), "s1");
    assert_eq!(scope_of(&selected[1]), "s1");
    assert_eq!(scope_of(&selected[2]), "s2");
    assert!(pending.is_empty());
}

#[tokio::test]
async fn epoch_selection_does_not_treat_same_namespace_multi_key_as_cross_partition() {
    let mut pending = VecDeque::new();
    pending.push_back(request_with_mutations(vec![
        Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "s1".into(),
            key: b"a".to_vec(),
            value: b"v".to_vec(),
        },
        Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "s1".into(),
            key: b"b".to_vec(),
            value: b"v".to_vec(),
        },
    ]));
    pending.push_back(kv_request("s1", b"c"));

    let (tx, mut rx) = mpsc::channel(1);
    drop(tx);

    let selected = super::build_epoch_requests(
        &mut pending,
        1,
        3,
        Instant::now() + Duration::from_millis(50),
        &mut rx,
        true,
    )
    .await;

    assert_eq!(selected.len(), 2);
    assert_eq!(scope_of(&selected[0]), "s1");
    assert_eq!(scope_of(&selected[1]), "s1");
    assert!(pending.is_empty());
}

#[test]
fn parallel_single_partition_candidate_allows_multi_key_same_namespace() {
    let request = request_with_mutations(vec![
        Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"k1".to_vec(),
            value: b"v".to_vec(),
        },
        Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"k2".to_vec(),
            value: b"v".to_vec(),
        },
    ]);
    assert!(super::is_parallel_single_partition_apply_candidate(
        &request,
        &request.envelope.write_intent.mutations,
        &Catalog::default(),
    ));
}

#[tokio::test]
async fn trusted_submit_requires_system_caller() {
    let dir = tempdir().expect("temp");
    let exec = CommitExecutor::new(dir.path()).expect("executor");
    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "p".into(),
    }))
    .await
    .expect("project");
    exec.submit(Mutation::Ddl(DdlOperation::CreateScope {
        owner_id: None,
        if_not_exists: true,
        project_id: "p".into(),
        scope_id: "app".into(),
    }))
    .await
    .expect("scope");

    let base_seq = exec.current_seq().await;
    let non_system = TransactionEnvelope {
        caller: Some(crate::permission::CallerContext::new("u1")),
        idempotency_key: None,
        write_class: WriteClass::Standard,
        assertions: Vec::new(),
        read_set: ReadSet::default(),
        write_intent: WriteIntent {
            mutations: vec![Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            }],
        },
        base_seq,
    };
    let err = exec
        .submit_envelope_trusted(non_system)
        .await
        .expect_err("non-system trusted submit must fail");
    assert!(matches!(err, AedbError::PermissionDenied(_)));

    let system = TransactionEnvelope {
        caller: Some(crate::permission::CallerContext::system_internal()),
        idempotency_key: None,
        write_class: WriteClass::Standard,
        assertions: Vec::new(),
        read_set: ReadSet::default(),
        write_intent: WriteIntent {
            mutations: vec![Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            }],
        },
        base_seq,
    };
    exec.submit_envelope_trusted(system)
        .await
        .expect("system trusted submit");
}

#[tokio::test]
async fn trusted_submit_falls_back_to_full_apply_for_constrained_tables() {
    let dir = tempdir().expect("temp");
    let exec = CommitExecutor::new(dir.path()).expect("executor");
    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "ct".into(),
    }))
    .await
    .expect("project");
    exec.submit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "ct".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "age".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");
    exec.submit(Mutation::Ddl(DdlOperation::AlterTable {
        project_id: "ct".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        alteration: TableAlteration::AddConstraint(Constraint::Check {
            name: "age_positive".into(),
            expr: Expr::Gt("age".into(), Value::Integer(0)),
        }),
    }))
    .await
    .expect("check");

    let base_seq = exec.current_seq().await;
    let trusted = TransactionEnvelope {
        caller: Some(crate::permission::CallerContext::system_internal()),
        idempotency_key: None,
        write_class: WriteClass::Standard,
        assertions: Vec::new(),
        read_set: ReadSet::default(),
        write_intent: WriteIntent {
            mutations: vec![Mutation::Upsert {
                project_id: "ct".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                primary_key: vec![Value::Integer(1)],
                row: Row {
                    values: vec![Value::Integer(1), Value::Integer(-1)],
                },
            }],
        },
        base_seq,
    };
    let err = exec
        .submit_envelope_trusted(trusted)
        .await
        .expect_err("constraint must still be enforced");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[tokio::test]
async fn trusted_engine_verified_assertions_can_skip_evaluation() {
    let dir = tempdir().expect("temp");
    let exec = CommitExecutor::new(dir.path()).expect("executor");
    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "p".into(),
    }))
    .await
    .expect("project");
    exec.submit(Mutation::Ddl(DdlOperation::CreateScope {
        owner_id: None,
        if_not_exists: true,
        project_id: "p".into(),
        scope_id: "app".into(),
    }))
    .await
    .expect("scope");

    let base_seq = exec.current_seq().await;
    let trusted = TransactionEnvelope {
        caller: Some(crate::permission::CallerContext::system_internal()),
        idempotency_key: None,
        write_class: WriteClass::Standard,
        assertions: vec![ReadAssertion::KeyExists {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"missing".to_vec(),
            expected: true,
        }],
        read_set: ReadSet::default(),
        write_intent: WriteIntent {
            mutations: vec![Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"marker".to_vec(),
                value: b"ok".to_vec(),
            }],
        },
        base_seq,
    };
    exec.submit_envelope_trusted_engine_verified(trusted)
        .await
        .expect("engine verified trusted commit should skip assertion evaluation");

    let (snapshot, _, _) = exec.snapshot_state().await;
    assert!(
        snapshot.kv_get("p", "app", b"marker").is_some(),
        "trusted mutation should apply when assertions are marked engine verified"
    );
}

#[tokio::test]
async fn trusted_submit_still_evaluates_assertions_without_engine_verified_marker() {
    let dir = tempdir().expect("temp");
    let exec = CommitExecutor::new(dir.path()).expect("executor");
    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "p".into(),
    }))
    .await
    .expect("project");
    exec.submit(Mutation::Ddl(DdlOperation::CreateScope {
        owner_id: None,
        if_not_exists: true,
        project_id: "p".into(),
        scope_id: "app".into(),
    }))
    .await
    .expect("scope");

    let base_seq = exec.current_seq().await;
    let trusted = TransactionEnvelope {
        caller: Some(crate::permission::CallerContext::system_internal()),
        idempotency_key: None,
        write_class: WriteClass::Standard,
        assertions: vec![ReadAssertion::KeyExists {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"missing".to_vec(),
            expected: true,
        }],
        read_set: ReadSet::default(),
        write_intent: WriteIntent {
            mutations: vec![Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"marker".to_vec(),
                value: b"ok".to_vec(),
            }],
        },
        base_seq,
    };
    let err = exec
        .submit_envelope_trusted(trusted)
        .await
        .expect_err("trusted commit without engine-verified marker must still evaluate assertions");
    assert!(matches!(err, AedbError::AssertionFailed { .. }));
}

#[tokio::test]
async fn serialization_concurrent_submissions_produce_ordered_wal() {
    let dir = tempdir().expect("temp");
    let exec = CommitExecutor::new(dir.path()).expect("executor");
    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "p1".into(),
    }))
    .await
    .expect("project");
    exec.submit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "p1".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");

    let mut tasks = Vec::new();
    for t in 0..10 {
        let exec_clone = exec.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..100 {
                let id = (t * 100 + i) as i64;
                exec_clone
                    .submit(Mutation::Upsert {
                        project_id: "p1".into(),
                        scope_id: "app".into(),
                        table_name: "users".into(),
                        primary_key: vec![Value::Integer(id)],
                        row: base_row(id, "n"),
                    })
                    .await
                    .expect("commit");
            }
        }));
    }
    for task in tasks {
        task.await.expect("join");
    }

    let mut files: Vec<_> = fs::read_dir(dir.path())
        .expect("read dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".aedbwal"))
        .collect();
    files.sort_by_key(|e| e.file_name());

    let mut seqs = Vec::new();
    for entry in files {
        let bytes = fs::read(entry.path()).expect("segment bytes");
        if bytes.len() <= 64 {
            continue;
        }
        let mut reader = FrameReader::new(Cursor::new(bytes[64..].to_vec()));
        while let Some(frame) = reader.next_frame().expect("frame decode") {
            seqs.push(frame.commit_seq);
        }
    }

    assert_eq!(seqs.len(), 1002);
    let mut seen = HashSet::new();
    for seq in &seqs {
        assert!(seen.insert(*seq), "duplicate sequence");
    }
    seqs.sort_unstable();
    for (i, seq) in seqs.iter().enumerate() {
        assert_eq!(*seq, (i as u64) + 1);
    }
}

#[tokio::test]
async fn invalid_type_is_rejected_without_side_effects() {
    let dir = tempdir().expect("temp");
    let exec = CommitExecutor::new(dir.path()).expect("executor");
    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "p1".into(),
    }))
    .await
    .expect("project");
    exec.submit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "p1".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");

    let before = exec.current_seq().await;
    let bad = exec
        .submit(Mutation::Upsert {
            project_id: "p1".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(1)],
            row: Row {
                values: vec![Value::Text("wrong".into()), Value::Text("ok".into())],
            },
        })
        .await;
    assert!(bad.is_err());
    assert_eq!(exec.current_seq().await, before);
}

#[tokio::test]
async fn ddl_then_insert_succeeds_and_cross_project_isolation_rejects() {
    let dir = tempdir().expect("temp");
    let exec = CommitExecutor::new(dir.path()).expect("executor");

    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "a".into(),
    }))
    .await
    .expect("project a");
    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "b".into(),
    }))
    .await
    .expect("project b");

    exec.submit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "a".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table a");

    exec.submit(Mutation::Upsert {
        project_id: "a".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(1)],
        row: base_row(1, "ok"),
    })
    .await
    .expect("insert a");

    let bad = exec
        .submit(Mutation::Upsert {
            project_id: "b".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(1)],
            row: base_row(1, "bad"),
        })
        .await;
    assert!(bad.is_err());
}

#[tokio::test]
async fn create_index_builds_existing_rows_synchronously() {
    let dir = tempdir().expect("temp");
    let exec = CommitExecutor::new(dir.path()).expect("executor");
    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "a".into(),
    }))
    .await
    .expect("project");
    exec.submit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "a".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "age".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");

    for i in 0..100 {
        exec.submit(Mutation::Upsert {
            project_id: "a".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(i)],
            row: Row {
                values: vec![Value::Integer(i), Value::Integer(20 + (i % 10))],
            },
        })
        .await
        .expect("insert");
    }

    exec.submit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: "a".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        index_name: "by_age".into(),
        if_not_exists: false,
        columns: vec!["age".into()],
        index_type: crate::catalog::schema::IndexType::BTree,
        partial_filter: None,
    }))
    .await
    .expect("index");

    let (snapshot, _, _) = exec.snapshot_state().await;
    let table = snapshot
        .table_by_namespace_key(&namespace_key("a", "app"), "users")
        .expect("table snapshot");
    let idx = table.indexes.get("by_age").expect("index materialized");
    let at_25 = idx.scan_eq(&EncodedKey::from_values(&[Value::Integer(25)]));
    assert!(!at_25.is_empty());
}

#[tokio::test]
async fn range_read_set_conflicts_on_kv_insert_in_range() {
    let dir = tempdir().expect("temp");
    let exec = CommitExecutor::new(dir.path()).expect("executor");
    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "r".into(),
    }))
    .await
    .expect("project");

    let base_seq = exec.current_seq().await;
    let envelope = TransactionEnvelope {
        caller: None,
        idempotency_key: None,
        write_class: WriteClass::Standard,
        assertions: Vec::new(),
        read_set: ReadSet {
            points: Vec::new(),
            ranges: vec![ReadRangeEntry {
                range: ReadRange::KvRange {
                    project_id: "r".into(),
                    scope_id: "app".into(),
                    start: ReadBound::Included(b"acct:".to_vec()),
                    end: ReadBound::Excluded(b"acct;".to_vec()),
                },
                max_version_at_read: base_seq,
                structural_version_at_read: base_seq,
            }],
        },
        write_intent: WriteIntent {
            mutations: vec![Mutation::KvSet {
                project_id: "r".into(),
                scope_id: "app".into(),
                key: b"outside".to_vec(),
                value: b"noop".to_vec(),
            }],
        },
        base_seq,
    };

    exec.submit(Mutation::KvSet {
        project_id: "r".into(),
        scope_id: "app".into(),
        key: b"acct:1".to_vec(),
        value: b"hot".to_vec(),
    })
    .await
    .expect("concurrent insert");

    let err = exec
        .submit_envelope(envelope)
        .await
        .expect_err("must conflict");
    assert!(matches!(err, AedbError::Conflict(_)));
}

#[tokio::test]
async fn range_read_set_conflicts_on_kv_delete_in_range() {
    let dir = tempdir().expect("temp");
    let exec = CommitExecutor::new(dir.path()).expect("executor");
    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "rdel".into(),
    }))
    .await
    .expect("project");
    exec.submit(Mutation::KvSet {
        project_id: "rdel".into(),
        scope_id: "app".into(),
        key: b"acct:dead".to_vec(),
        value: b"v".to_vec(),
    })
    .await
    .expect("seed");

    let base_seq = exec.current_seq().await;
    let envelope = TransactionEnvelope {
        caller: None,
        idempotency_key: None,
        write_class: WriteClass::Standard,
        assertions: Vec::new(),
        read_set: ReadSet {
            points: Vec::new(),
            ranges: vec![ReadRangeEntry {
                range: ReadRange::KvRange {
                    project_id: "rdel".into(),
                    scope_id: "app".into(),
                    start: ReadBound::Included(b"acct:".to_vec()),
                    end: ReadBound::Excluded(b"acct;".to_vec()),
                },
                max_version_at_read: base_seq,
                structural_version_at_read: base_seq,
            }],
        },
        write_intent: WriteIntent {
            mutations: vec![Mutation::KvSet {
                project_id: "rdel".into(),
                scope_id: "app".into(),
                key: b"outside".to_vec(),
                value: b"noop".to_vec(),
            }],
        },
        base_seq,
    };

    exec.submit(Mutation::KvDel {
        project_id: "rdel".into(),
        scope_id: "app".into(),
        key: b"acct:dead".to_vec(),
    })
    .await
    .expect("concurrent delete");

    let err = exec
        .submit_envelope(envelope)
        .await
        .expect_err("must conflict");
    assert!(matches!(err, AedbError::Conflict(_)));
}

#[tokio::test]
async fn unique_hash_index_rejects_duplicate_key() {
    let dir = tempdir().expect("temp");
    let exec = CommitExecutor::new(dir.path()).expect("executor");
    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "u".into(),
    }))
    .await
    .expect("project");
    exec.submit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "u".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "email".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");
    exec.submit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: "u".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        index_name: "uniq_email".into(),
        if_not_exists: false,
        columns: vec!["email".into()],
        index_type: crate::catalog::schema::IndexType::UniqueHash,
        partial_filter: None,
    }))
    .await
    .expect("index");
    exec.submit(Mutation::Upsert {
        project_id: "u".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Text("a@x.com".into())],
        },
    })
    .await
    .expect("first");
    let err = exec
        .submit(Mutation::Upsert {
            project_id: "u".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(2)],
            row: Row {
                values: vec![Value::Integer(2), Value::Text("a@x.com".into())],
            },
        })
        .await
        .expect_err("duplicate should fail");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[tokio::test]
async fn upsert_on_conflict_pk_do_nothing_preserves_existing_row() {
    let dir = tempdir().expect("temp");
    let exec = CommitExecutor::new(dir.path()).expect("executor");
    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "u".into(),
    }))
    .await
    .expect("project");
    exec.submit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "u".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");
    exec.submit(Mutation::Upsert {
        project_id: "u".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Text("alice".into())],
        },
    })
    .await
    .expect("insert");

    exec.submit(Mutation::UpsertOnConflict {
        project_id: "u".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        row: Row {
            values: vec![Value::Integer(1), Value::Text("changed".into())],
        },
        conflict_target: ConflictTarget::PrimaryKey,
        conflict_action: ConflictAction::DoNothing,
    })
    .await
    .expect("upsert on conflict");

    let (snapshot, _, _) = exec.snapshot_state().await;
    let row = snapshot
        .table("u", "app", "users")
        .and_then(|t| t.rows.get(&EncodedKey::from_values(&[Value::Integer(1)])))
        .expect("row");
    assert_eq!(row.values[1], Value::Text("alice".into()));
}

#[tokio::test]
async fn upsert_on_conflict_index_do_update_updates_existing_row() {
    let dir = tempdir().expect("temp");
    let exec = CommitExecutor::new(dir.path()).expect("executor");
    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "u".into(),
    }))
    .await
    .expect("project");
    exec.submit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "u".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "email".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");
    exec.submit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: "u".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        index_name: "uniq_email".into(),
        if_not_exists: false,
        columns: vec!["email".into()],
        index_type: crate::catalog::schema::IndexType::UniqueHash,
        partial_filter: None,
    }))
    .await
    .expect("index");

    exec.submit(Mutation::Upsert {
        project_id: "u".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![
                Value::Integer(1),
                Value::Text("alice@example.com".into()),
                Value::Text("Alice".into()),
            ],
        },
    })
    .await
    .expect("insert");

    exec.submit(Mutation::UpsertOnConflict {
        project_id: "u".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        row: Row {
            values: vec![
                Value::Integer(999),
                Value::Text("alice@example.com".into()),
                Value::Text("ignored".into()),
            ],
        },
        conflict_target: ConflictTarget::Index("uniq_email".into()),
        conflict_action: ConflictAction::DoUpdate(vec![(
            "name".into(),
            Value::Text("Alice Updated".into()),
        )]),
    })
    .await
    .expect("upsert on conflict");

    let (snapshot, _, _) = exec.snapshot_state().await;
    let row = snapshot
        .table("u", "app", "users")
        .and_then(|t| t.rows.get(&EncodedKey::from_values(&[Value::Integer(1)])))
        .expect("existing row by pk");
    assert_eq!(row.values[2], Value::Text("Alice Updated".into()));
    assert!(
        snapshot
            .table("u", "app", "users")
            .and_then(|t| t.rows.get(&EncodedKey::from_values(&[Value::Integer(999)])))
            .is_none(),
        "conflict update must not insert proposed PK"
    );
}

#[tokio::test]
async fn upsert_on_conflict_do_update_with_adds_existing_and_proposed() {
    let dir = tempdir().expect("temp");
    let exec = CommitExecutor::new(dir.path()).expect("executor");
    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "sum".into(),
    }))
    .await
    .expect("project");
    exec.submit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "sum".into(),
        scope_id: "app".into(),
        table_name: "event_counts".into(),
        columns: vec![
            ColumnDef {
                name: "event_type".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "count".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        primary_key: vec!["event_type".into()],
    }))
    .await
    .expect("table");
    exec.submit(Mutation::Upsert {
        project_id: "sum".into(),
        scope_id: "app".into(),
        table_name: "event_counts".into(),
        primary_key: vec![Value::Text("login".into())],
        row: Row {
            values: vec![Value::Text("login".into()), Value::Integer(5)],
        },
    })
    .await
    .expect("seed");
    exec.submit(Mutation::UpsertOnConflict {
        project_id: "sum".into(),
        scope_id: "app".into(),
        table_name: "event_counts".into(),
        row: Row {
            values: vec![Value::Text("login".into()), Value::Integer(2)],
        },
        conflict_target: ConflictTarget::PrimaryKey,
        conflict_action: ConflictAction::DoUpdateWith(vec![(
            "count".into(),
            UpdateExpr::AddI64 {
                existing_column: "count".into(),
                proposed_column: "count".into(),
            },
        )]),
    })
    .await
    .expect("merge");
    let (snapshot, _, _) = exec.snapshot_state().await;
    let row = snapshot
        .table("sum", "app", "event_counts")
        .and_then(|t| {
            t.rows
                .get(&EncodedKey::from_values(&[Value::Text("login".into())]))
        })
        .expect("row");
    assert_eq!(row.values[1], Value::Integer(7));
}

#[tokio::test]
async fn upsert_batch_on_conflict_is_sequential_within_commit() {
    let dir = tempdir().expect("temp");
    let exec = CommitExecutor::new(dir.path()).expect("executor");
    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "b".into(),
    }))
    .await
    .expect("project");
    exec.submit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "b".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");
    exec.submit(Mutation::UpsertBatchOnConflict {
        project_id: "b".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        rows: vec![
            Row {
                values: vec![Value::Integer(1), Value::Text("first".into())],
            },
            Row {
                values: vec![Value::Integer(1), Value::Text("second".into())],
            },
        ],
        conflict_target: ConflictTarget::PrimaryKey,
        conflict_action: ConflictAction::DoMerge,
    })
    .await
    .expect("batch upsert");
    let (snapshot, _, _) = exec.snapshot_state().await;
    let row = snapshot
        .table("b", "app", "users")
        .and_then(|t| t.rows.get(&EncodedKey::from_values(&[Value::Integer(1)])))
        .expect("row");
    assert_eq!(row.values[1], Value::Text("second".into()));
}

#[tokio::test]
async fn upsert_batch_applies_all_rows_in_single_commit() {
    let dir = tempdir().expect("temp");
    let exec = CommitExecutor::new(dir.path()).expect("executor");
    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "ub".into(),
    }))
    .await
    .expect("project");
    exec.submit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "ub".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");

    exec.submit(Mutation::UpsertBatch {
        project_id: "ub".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        rows: vec![
            Row {
                values: vec![Value::Integer(1), Value::Text("one".into())],
            },
            Row {
                values: vec![Value::Integer(2), Value::Text("two".into())],
            },
            Row {
                values: vec![Value::Integer(3), Value::Text("three".into())],
            },
        ],
    })
    .await
    .expect("upsert batch");

    let (snapshot, _, _) = exec.snapshot_state().await;
    let table = snapshot.table("ub", "app", "users").expect("table");
    assert_eq!(table.rows.len(), 3);
    assert_eq!(
        table
            .rows
            .get(&EncodedKey::from_values(&[Value::Integer(1)]))
            .expect("row 1")
            .values[1],
        Value::Text("one".into())
    );
    assert_eq!(
        table
            .rows
            .get(&EncodedKey::from_values(&[Value::Integer(3)]))
            .expect("row 3")
            .values[1],
        Value::Text("three".into())
    );
}

#[tokio::test]
async fn check_and_default_constraints_are_enforced() {
    let dir = tempdir().expect("temp");
    let exec = CommitExecutor::new(dir.path()).expect("executor");
    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "c".into(),
    }))
    .await
    .expect("project");
    exec.submit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "c".into(),
        scope_id: "app".into(),
        table_name: "players".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "status".into(),
                col_type: ColumnType::Text,
                nullable: true,
            },
            ColumnDef {
                name: "score".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");
    exec.submit(Mutation::Ddl(DdlOperation::AlterTable {
        project_id: "c".into(),
        scope_id: "app".into(),
        table_name: "players".into(),
        alteration: TableAlteration::AddConstraint(Constraint::Default {
            column: "status".into(),
            value: Value::Text("active".into()),
        }),
    }))
    .await
    .expect("default");
    exec.submit(Mutation::Ddl(DdlOperation::AlterTable {
        project_id: "c".into(),
        scope_id: "app".into(),
        table_name: "players".into(),
        alteration: TableAlteration::AddConstraint(Constraint::Check {
            name: "score_non_negative".into(),
            expr: Expr::Gte("score".into(), Value::Integer(0)),
        }),
    }))
    .await
    .expect("check");
    exec.submit(Mutation::Upsert {
        project_id: "c".into(),
        scope_id: "app".into(),
        table_name: "players".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Null, Value::Integer(10)],
        },
    })
    .await
    .expect("insert");

    let err = exec
        .submit(Mutation::Upsert {
            project_id: "c".into(),
            scope_id: "app".into(),
            table_name: "players".into(),
            primary_key: vec![Value::Integer(2)],
            row: Row {
                values: vec![Value::Integer(2), Value::Null, Value::Integer(-1)],
            },
        })
        .await
        .expect_err("check must fail");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[tokio::test]
async fn foreign_key_restrict_cascade_and_update_actions() {
    let dir = tempdir().expect("temp");
    let exec = CommitExecutor::new(dir.path()).expect("executor");
    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "f".into(),
    }))
    .await
    .expect("project");
    exec.submit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "f".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("users");
    exec.submit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "f".into(),
        scope_id: "app".into(),
        table_name: "settlements".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "user_id".into(),
                col_type: ColumnType::Integer,
                nullable: true,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("settlements");
    exec.submit(Mutation::Ddl(DdlOperation::AlterTable {
        project_id: "f".into(),
        scope_id: "app".into(),
        table_name: "settlements".into(),
        alteration: TableAlteration::AddForeignKey(ForeignKey {
            name: "fk_user".into(),
            columns: vec!["user_id".into()],
            references_project_id: "f".into(),
            references_scope_id: "app".into(),
            references_table: "users".into(),
            references_columns: vec!["id".into()],
            on_delete: ForeignKeyAction::Cascade,
            on_update: ForeignKeyAction::SetNull,
        }),
    }))
    .await
    .expect("fk");

    exec.submit(Mutation::Upsert {
        project_id: "f".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Text("alice".into())],
        },
    })
    .await
    .expect("user");
    exec.submit(Mutation::Upsert {
        project_id: "f".into(),
        scope_id: "app".into(),
        table_name: "settlements".into(),
        primary_key: vec![Value::Integer(100)],
        row: Row {
            values: vec![Value::Integer(100), Value::Integer(1)],
        },
    })
    .await
    .expect("settlement");

    exec.submit(Mutation::Upsert {
        project_id: "f".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Text("alice2".into())],
        },
    })
    .await
    .expect("non-fk update");

    exec.submit(Mutation::Upsert {
        project_id: "f".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(2)],
        row: Row {
            values: vec![Value::Integer(2), Value::Text("alice-new".into())],
        },
    })
    .await
    .expect("new user");

    exec.submit(Mutation::Delete {
        project_id: "f".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(2)],
    })
    .await
    .expect("delete no refs");

    exec.submit(Mutation::Delete {
        project_id: "f".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(1)],
    })
    .await
    .expect("cascade delete");

    let (snapshot, _, _) = exec.snapshot_state().await;
    let settlements = snapshot
        .table("f", "app", "settlements")
        .expect("settlements table");
    assert!(
        settlements.rows.is_empty(),
        "cascade delete should clear child rows"
    );
}

#[tokio::test]
async fn settlements_unique_and_fk_constraints_compose() {
    let dir = tempdir().expect("temp");
    let exec = CommitExecutor::new(dir.path()).expect("executor");

    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "_global".into(),
    }))
    .await
    .expect("global project");
    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "settle".into(),
    }))
    .await
    .expect("project");

    for (table_name, key_col) in [("users", "user_id"), ("assets", "asset_id")] {
        exec.submit(Mutation::Ddl(DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: false,
            project_id: "_global".into(),
            scope_id: "app".into(),
            table_name: table_name.into(),
            columns: vec![ColumnDef {
                name: key_col.into(),
                col_type: ColumnType::Text,
                nullable: false,
            }],
            primary_key: vec![key_col.into()],
        }))
        .await
        .expect("global table");
    }

    exec.submit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "settle".into(),
        scope_id: "app".into(),
        table_name: "settlements".into(),
        columns: vec![
            ColumnDef {
                name: "settlement_id".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "user_id".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "email".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "scope".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "asset_id".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "amount".into(),
                col_type: ColumnType::U256,
                nullable: false,
            },
            ColumnDef {
                name: "tx_hash".into(),
                col_type: ColumnType::Text,
                nullable: true,
            },
        ],
        primary_key: vec!["settlement_id".into()],
    }))
    .await
    .expect("settlements");

    exec.submit(Mutation::Ddl(DdlOperation::AlterTable {
        project_id: "settle".into(),
        scope_id: "app".into(),
        table_name: "settlements".into(),
        alteration: TableAlteration::AddConstraint(Constraint::Unique {
            name: "uq_email".into(),
            columns: vec!["email".into()],
        }),
    }))
    .await
    .expect("unique email");
    exec.submit(Mutation::Ddl(DdlOperation::AlterTable {
        project_id: "settle".into(),
        scope_id: "app".into(),
        table_name: "settlements".into(),
        alteration: TableAlteration::AddConstraint(Constraint::Unique {
            name: "uq_scope_user_asset".into(),
            columns: vec!["scope".into(), "user_id".into(), "asset_id".into()],
        }),
    }))
    .await
    .expect("unique composite");
    exec.submit(Mutation::Ddl(DdlOperation::AlterTable {
        project_id: "settle".into(),
        scope_id: "app".into(),
        table_name: "settlements".into(),
        alteration: TableAlteration::AddForeignKey(ForeignKey {
            name: "fk_user".into(),
            columns: vec!["user_id".into()],
            references_project_id: "_global".into(),
            references_scope_id: "app".into(),
            references_table: "users".into(),
            references_columns: vec!["user_id".into()],
            on_delete: ForeignKeyAction::Restrict,
            on_update: ForeignKeyAction::Cascade,
        }),
    }))
    .await
    .expect("fk user");
    exec.submit(Mutation::Ddl(DdlOperation::AlterTable {
        project_id: "settle".into(),
        scope_id: "app".into(),
        table_name: "settlements".into(),
        alteration: TableAlteration::AddForeignKey(ForeignKey {
            name: "fk_asset".into(),
            columns: vec!["asset_id".into()],
            references_project_id: "_global".into(),
            references_scope_id: "app".into(),
            references_table: "assets".into(),
            references_columns: vec!["asset_id".into()],
            on_delete: ForeignKeyAction::Restrict,
            on_update: ForeignKeyAction::Cascade,
        }),
    }))
    .await
    .expect("fk asset");

    exec.submit(Mutation::Upsert {
        project_id: "_global".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Text("u1".into())],
        row: Row {
            values: vec![Value::Text("u1".into())],
        },
    })
    .await
    .expect("user");
    exec.submit(Mutation::Upsert {
        project_id: "_global".into(),
        scope_id: "app".into(),
        table_name: "assets".into(),
        primary_key: vec![Value::Text("a1".into())],
        row: Row {
            values: vec![Value::Text("a1".into())],
        },
    })
    .await
    .expect("asset");

    exec.submit(Mutation::Upsert {
        project_id: "settle".into(),
        scope_id: "app".into(),
        table_name: "settlements".into(),
        primary_key: vec![Value::Text("s1".into())],
        row: Row {
            values: vec![
                Value::Text("s1".into()),
                Value::Text("u1".into()),
                Value::Text("a@example.com".into()),
                Value::Text("game".into()),
                Value::Text("a1".into()),
                u256_value(10),
                Value::Text("tx1".into()),
            ],
        },
    })
    .await
    .expect("settlement");

    let duplicate_email = exec
        .submit(Mutation::Upsert {
            project_id: "settle".into(),
            scope_id: "app".into(),
            table_name: "settlements".into(),
            primary_key: vec![Value::Text("s2".into())],
            row: Row {
                values: vec![
                    Value::Text("s2".into()),
                    Value::Text("u1".into()),
                    Value::Text("a@example.com".into()),
                    Value::Text("game2".into()),
                    Value::Text("a1".into()),
                    u256_value(11),
                    Value::Text("tx2".into()),
                ],
            },
        })
        .await
        .expect_err("duplicate email should fail");
    assert!(matches!(duplicate_email, AedbError::UniqueViolation { .. }));

    let duplicate_composite = exec
        .submit(Mutation::Upsert {
            project_id: "settle".into(),
            scope_id: "app".into(),
            table_name: "settlements".into(),
            primary_key: vec![Value::Text("s3".into())],
            row: Row {
                values: vec![
                    Value::Text("s3".into()),
                    Value::Text("u1".into()),
                    Value::Text("b@example.com".into()),
                    Value::Text("game".into()),
                    Value::Text("a1".into()),
                    u256_value(12),
                    Value::Text("tx3".into()),
                ],
            },
        })
        .await
        .expect_err("duplicate composite should fail");
    assert!(matches!(
        duplicate_composite,
        AedbError::UniqueViolation { .. }
    ));

    let missing_fk = exec
        .submit(Mutation::Upsert {
            project_id: "settle".into(),
            scope_id: "app".into(),
            table_name: "settlements".into(),
            primary_key: vec![Value::Text("s4".into())],
            row: Row {
                values: vec![
                    Value::Text("s4".into()),
                    Value::Text("u1".into()),
                    Value::Text("c@example.com".into()),
                    Value::Text("game2".into()),
                    Value::Text("missing_asset".into()),
                    u256_value(13),
                    Value::Text("tx4".into()),
                ],
            },
        })
        .await
        .expect_err("missing fk should fail");
    assert!(matches!(missing_fk, AedbError::ForeignKeyViolation { .. }));

    let delete_referenced_user = exec
        .submit(Mutation::Delete {
            project_id: "_global".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Text("u1".into())],
        })
        .await
        .expect_err("restrict should fail");
    assert!(matches!(
        delete_referenced_user,
        AedbError::ForeignKeyViolation { .. }
    ));

    let (snapshot, catalog, _) = exec.snapshot_state().await;
    let ns = namespace_key("settle", "app");
    assert!(
        catalog
            .indexes
            .contains_key(&(ns.clone(), "settlements".into(), "uq_email".into()))
    );
    assert!(catalog.indexes.contains_key(&(
        ns.clone(),
        "settlements".into(),
        "uq_scope_user_asset".into()
    )));

    let table = snapshot
        .table_by_namespace_key(&ns, "settlements")
        .expect("table");
    assert!(table.indexes.contains_key("uq_email"));
    assert!(table.indexes.contains_key("uq_scope_user_asset"));
}

#[tokio::test]
async fn rejects_when_commit_queue_bytes_exceeded() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        max_commit_queue_bytes: 1,
        ..AedbConfig::default()
    };
    let exec = CommitExecutor::with_state(
        dir.path(),
        Keyspace::default(),
        Catalog::default(),
        0,
        1,
        config,
        HashMap::new(),
    )
    .expect("executor");
    let err = exec
        .submit(Mutation::Ddl(DdlOperation::CreateProject {
            owner_id: None,
            if_not_exists: true,
            project_id: "p".into(),
        }))
        .await
        .expect_err("queue full");
    assert!(matches!(err, AedbError::QueueFull));
    let m = exec.metrics();
    assert!(m.queue_full_rejections >= 1);
}

#[tokio::test]
async fn metrics_track_coordinator_and_read_set_contention() {
    let dir = tempdir().expect("temp");
    let exec = CommitExecutor::with_state(
        dir.path(),
        Keyspace::default(),
        Catalog::default(),
        0,
        1,
        AedbConfig::default(),
        HashMap::new(),
    )
    .expect("executor");

    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "p".into(),
    }))
    .await
    .expect("project");

    let seed = exec
        .submit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"tracked".to_vec(),
            value: b"v1".to_vec(),
        })
        .await
        .expect("seed");

    exec.submit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"tracked".to_vec(),
        value: b"v2".to_vec(),
    })
    .await
    .expect("bump tracked");

    let stale_read_envelope = TransactionEnvelope {
        caller: None,
        idempotency_key: None,
        write_class: WriteClass::Standard,
        assertions: Vec::new(),
        read_set: ReadSet {
            points: vec![ReadSetEntry {
                key: ReadKey::KvKey {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"tracked".to_vec(),
                },
                version_at_read: seed.commit_seq,
            }],
            ranges: vec![],
        },
        write_intent: WriteIntent {
            mutations: vec![Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"stale-write".to_vec(),
                value: b"x".to_vec(),
            }],
        },
        base_seq: exec.current_seq().await,
    };
    let err = exec
        .submit_envelope(stale_read_envelope)
        .await
        .expect_err("stale read set must conflict");
    match &err {
        AedbError::Conflict(msg) => assert!(
            msg.contains("read set conflict"),
            "expected read-set conflict, got: {msg}"
        ),
        other => panic!("expected validation error, got: {other:?}"),
    }

    let m = exec.metrics();
    assert!(m.read_set_conflicts >= 1);
    assert!(m.coordinator_apply_attempts >= 1);
}

#[tokio::test]
async fn batch_mode_heads_diverge_and_converge() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        durability_mode: crate::config::DurabilityMode::Batch,
        batch_interval_ms: 30,
        batch_max_bytes: usize::MAX,
        ..AedbConfig::default()
    };
    let exec = CommitExecutor::with_state(
        dir.path(),
        Keyspace::default(),
        Catalog::default(),
        0,
        1,
        config,
        HashMap::new(),
    )
    .expect("executor");
    let committed = exec
        .submit(Mutation::Ddl(DdlOperation::CreateProject {
            owner_id: None,
            if_not_exists: true,
            project_id: "p".into(),
        }))
        .await
        .expect("commit");
    let visible = exec.visible_head_seq().await;
    let durable = exec.durable_head_seq().await;
    assert_eq!(visible, committed.commit_seq);
    assert!(durable < visible);

    tokio::time::timeout(
        std::time::Duration::from_millis(750),
        exec.wait_for_durable(committed.commit_seq),
    )
    .await
    .expect("wait timeout")
    .expect("wait ok");
    assert_eq!(exec.durable_head_seq().await, committed.commit_seq);
}

#[tokio::test]
async fn batch_mode_groups_commits_without_per_commit_fsync() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        durability_mode: crate::config::DurabilityMode::Batch,
        batch_interval_ms: 60_000,
        batch_max_bytes: usize::MAX,
        ..AedbConfig::default()
    };
    let exec = CommitExecutor::with_state(
        dir.path(),
        Keyspace::default(),
        Catalog::default(),
        0,
        1,
        config,
        HashMap::new(),
    )
    .expect("executor");
    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "p".into(),
    }))
    .await
    .expect("project");
    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "q".into(),
    }))
    .await
    .expect("project q");
    exec.submit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");
    let _ = exec.force_fsync().await.expect("fsync setup");
    let durable_before = exec.durable_head_seq().await;

    for i in 0..3 {
        let _ = exec
            .submit(Mutation::Upsert {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                primary_key: vec![Value::Integer(i)],
                row: Row {
                    values: vec![Value::Integer(i), Value::Text(format!("u{i}").into())],
                },
            })
            .await
            .expect("upsert");
    }
    assert_eq!(exec.durable_head_seq().await, durable_before);
    assert!(exec.visible_head_seq().await > durable_before);
}

#[tokio::test]
async fn async_projection_refresh_applies_row_deltas() {
    let dir = tempdir().expect("temp");
    let exec = CommitExecutor::new(dir.path()).expect("executor");
    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "p".into(),
    }))
    .await
    .expect("project");
    exec.submit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");
    exec.submit(Mutation::Ddl(DdlOperation::CreateAsyncIndex {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        index_name: "users_view".into(),
        if_not_exists: false,
        projected_columns: vec!["name".into()],
    }))
    .await
    .expect("async index");

    exec.submit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Text("alice".into())],
        },
    })
    .await
    .expect("first upsert");
    exec.submit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Text("alice-updated".into())],
        },
    })
    .await
    .expect("second upsert");

    let (snapshot, _, seq) = exec.snapshot_state().await;
    let projection = snapshot
        .async_indexes
        .get(&(
            NamespaceId::Project(namespace_key("p", "app")),
            "users".into(),
            "users_view".into(),
        ))
        .expect("projection");
    assert_eq!(projection.rows.len(), 1);
    let projected = projection
        .rows
        .get(&EncodedKey::from_values(&[Value::Integer(1)]))
        .expect("projected row");
    assert_eq!(projected.values, vec![Value::Text("alice-updated".into())]);
    assert_eq!(projection.materialized_seq, seq);
}

#[tokio::test]
async fn creating_unique_hash_index_fails_on_existing_duplicates() {
    let dir = tempdir().expect("temp");
    let exec = CommitExecutor::new(dir.path()).expect("executor");
    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "dups".into(),
    }))
    .await
    .expect("project");
    exec.submit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "dups".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "email".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");
    for id in [1_i64, 2_i64] {
        exec.submit(Mutation::Upsert {
            project_id: "dups".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row {
                values: vec![Value::Integer(id), Value::Text("dup@example.com".into())],
            },
        })
        .await
        .expect("insert");
    }
    let err = exec
        .submit(Mutation::Ddl(DdlOperation::CreateIndex {
            project_id: "dups".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            index_name: "uniq_email".into(),
            if_not_exists: false,
            columns: vec!["email".into()],
            index_type: crate::catalog::schema::IndexType::UniqueHash,
            partial_filter: None,
        }))
        .await
        .expect_err("duplicate unique build should fail");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[tokio::test]
async fn partial_index_membership_updates_with_row_changes() {
    let dir = tempdir().expect("temp");
    let exec = CommitExecutor::new(dir.path()).expect("executor");
    exec.submit(Mutation::Ddl(DdlOperation::CreateProject {
        owner_id: None,
        if_not_exists: true,
        project_id: "pidx".into(),
    }))
    .await
    .expect("project");
    exec.submit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "pidx".into(),
        scope_id: "app".into(),
        table_name: "lobbies".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "status".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "created_at".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");
    exec.submit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: "pidx".into(),
        scope_id: "app".into(),
        table_name: "lobbies".into(),
        index_name: "open_by_created".into(),
        if_not_exists: false,
        columns: vec!["created_at".into()],
        index_type: crate::catalog::schema::IndexType::BTree,
        partial_filter: Some(Expr::Eq("status".into(), Value::Text("open".into()))),
    }))
    .await
    .expect("partial index");

    exec.submit(Mutation::Upsert {
        project_id: "pidx".into(),
        scope_id: "app".into(),
        table_name: "lobbies".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![
                Value::Integer(1),
                Value::Text("closed".into()),
                Value::Integer(1000),
            ],
        },
    })
    .await
    .expect("insert closed");

    let (snapshot1, _, _) = exec.snapshot_state().await;
    let table1 = snapshot1
        .table_by_namespace_key(&namespace_key("pidx", "app"), "lobbies")
        .expect("table");
    let idx1 = table1.indexes.get("open_by_created").expect("idx");
    match &idx1.store {
        SecondaryIndexStore::BTree(entries) => assert_eq!(entries.len(), 0),
        _ => panic!("expected btree index"),
    }

    exec.submit(Mutation::Upsert {
        project_id: "pidx".into(),
        scope_id: "app".into(),
        table_name: "lobbies".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![
                Value::Integer(1),
                Value::Text("open".into()),
                Value::Integer(1000),
            ],
        },
    })
    .await
    .expect("update open");

    let (snapshot2, _, _) = exec.snapshot_state().await;
    let table2 = snapshot2
        .table_by_namespace_key(&namespace_key("pidx", "app"), "lobbies")
        .expect("table");
    let idx2 = table2.indexes.get("open_by_created").expect("idx");
    let key = EncodedKey::from_values(&[Value::Integer(1000)]);
    assert_eq!(idx2.scan_eq(&key).len(), 1);

    exec.submit(Mutation::Upsert {
        project_id: "pidx".into(),
        scope_id: "app".into(),
        table_name: "lobbies".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![
                Value::Integer(1),
                Value::Text("closed".into()),
                Value::Integer(1000),
            ],
        },
    })
    .await
    .expect("update closed");

    let (snapshot3, _, _) = exec.snapshot_state().await;
    let table3 = snapshot3
        .table_by_namespace_key(&namespace_key("pidx", "app"), "lobbies")
        .expect("table");
    let idx3 = table3.indexes.get("open_by_created").expect("idx");
    assert_eq!(idx3.scan_eq(&key).len(), 0);
}
