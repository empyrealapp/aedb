use super::{
    AedbConfig, AedbError, AedbInstance, ColumnDef, ColumnType, ConsistencyMode, DdlOperation,
    EchoSqlAdapter, ErrorResourceType, ExecutionStage, Expr, IndexType, LocalRemoteAdapter,
    Mutation, PredicateEvaluationPath, Query, QueryBatchItem, QueryCommitTelemetryHook, QueryError,
    QueryOptions, ReadAssertion, RecordingTelemetryHook, Row, TransactionEnvelope, Value,
    WriteClass, WriteIntent, create_table,
};
use crate::commit::tx::ReadSet;
use crate::commit::validation::CompareOp;
use crate::query::plan::Order;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

#[tokio::test]
async fn api_open_commit_query_shutdown() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");

    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
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
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Text("reader".into())],
        },
    })
    .await
    .expect("insert");

    let result = db
        .query_no_auth(
            "p",
            "app",
            Query::select(&["*"])
                .from("users")
                .where_(Expr::Eq("id".into(), Value::Integer(1))),
            QueryOptions::default(),
        )
        .await
        .expect("query");
    assert_eq!(result.rows.len(), 1);

    db.create_scope("p", "private").await.expect("create scope");
    let scopes = db.list_scopes("p").await.expect("list scopes");
    assert!(scopes.contains(&"app".to_string()));
    assert!(scopes.contains(&"private".to_string()));
    db.drop_scope("p", "private").await.expect("drop scope");
    db.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn insert_rejects_duplicate_primary_key() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
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

    db.insert(
        "p",
        "app",
        "users",
        vec![Value::Integer(1)],
        Row {
            values: vec![Value::Integer(1), Value::Text("alice".into())],
        },
    )
    .await
    .expect("first insert");

    let duplicate = db
        .insert(
            "p",
            "app",
            "users",
            vec![Value::Integer(1)],
            Row {
                values: vec![Value::Integer(1), Value::Text("alice-2".into())],
            },
        )
        .await
        .expect_err("duplicate insert must fail");
    let duplicate_text = duplicate.to_string();
    assert!(
        duplicate_text.contains("duplicate primary key"),
        "unexpected duplicate insert error: {duplicate:?}"
    );
}

#[tokio::test]
async fn insert_batch_rejects_duplicate_primary_key() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
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

    db.insert_batch(
        "p",
        "app",
        "users",
        vec![
            Row {
                values: vec![Value::Integer(1), Value::Text("alice".into())],
            },
            Row {
                values: vec![Value::Integer(2), Value::Text("bob".into())],
            },
        ],
    )
    .await
    .expect("seed batch");

    let duplicate = db
        .insert_batch(
            "p",
            "app",
            "users",
            vec![
                Row {
                    values: vec![Value::Integer(3), Value::Text("charlie".into())],
                },
                Row {
                    values: vec![Value::Integer(1), Value::Text("alice-2".into())],
                },
            ],
        )
        .await
        .expect_err("duplicate insert_batch must fail");
    let duplicate_text = duplicate.to_string();
    assert!(
        duplicate_text.contains("duplicate primary key"),
        "unexpected duplicate insert_batch error: {duplicate:?}"
    );
}

#[tokio::test]
async fn async_projection_index_reports_materialized_seq() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
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

    let r = db
        .commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(1)],
            row: Row {
                values: vec![Value::Integer(1), Value::Text("alice".into())],
            },
        })
        .await
        .expect("insert");
    db.commit(Mutation::Ddl(DdlOperation::CreateAsyncIndex {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        index_name: "users_view".into(),
        if_not_exists: false,
        projected_columns: vec!["id".into(), "name".into()],
    }))
    .await
    .expect("create async index");

    let opts = QueryOptions {
        async_index: Some("users_view".into()),
        allow_full_scan: true,
        ..QueryOptions::default()
    };

    let mut observed = None;
    for _ in 0..20 {
        let q = db
            .query_no_auth(
                "p",
                "app",
                Query::select(&["*"]).from("users"),
                opts.clone(),
            )
            .await
            .expect("query async");
        observed = q.materialized_seq;
        if let Some(m) = q.materialized_seq
            && m >= r.commit_seq
        {
            assert_eq!(q.rows.len(), 1);
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("async index did not catch up, last materialized_seq={observed:?}");
}

#[tokio::test]
async fn envelope_mutation_count_cap_rejects_oversized_envelope() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        max_mutations_per_envelope: 4,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open_anonymous(config, dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let too_many: Vec<Mutation> = (0..5u8)
        .map(|i| Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: vec![b'k', i],
            value: vec![i],
        })
        .collect();
    let err = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: too_many,
            },
            base_seq: 0,
        })
        .await
        .expect_err("envelope mutation count cap must reject");
    match err {
        AedbError::Validation(msg) => {
            assert!(
                msg.contains("max_mutations_per_envelope") && msg.contains("mutations=5"),
                "informative limit message expected, got: {msg}"
            );
        }
        other => panic!("expected Validation error, got: {other:?}"),
    }

    let ok_batch: Vec<Mutation> = (0..4u8)
        .map(|i| Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: vec![b'k', i],
            value: vec![i],
        })
        .collect();
    db.commit_envelope(TransactionEnvelope {
        caller: None,
        idempotency_key: None,
        write_class: WriteClass::Standard,
        assertions: Vec::new(),
        read_set: ReadSet::default(),
        write_intent: WriteIntent {
            mutations: ok_batch,
        },
        base_seq: 0,
    })
    .await
    .expect("envelope within mutation cap commits");
}

#[tokio::test]
async fn envelope_assertion_count_cap_rejects_oversized_envelope() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        max_read_assertions_per_envelope: 2,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open_anonymous(config, dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let too_many: Vec<ReadAssertion> = (0..3u8)
        .map(|i| ReadAssertion::KeyExists {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: vec![b'k', i],
            expected: false,
        })
        .collect();
    let err = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: too_many,
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                }],
            },
            base_seq: 0,
        })
        .await
        .expect_err("envelope assertion count cap must reject");
    match err {
        AedbError::Validation(msg) => {
            assert!(
                msg.contains("max_read_assertions_per_envelope") && msg.contains("assertions=3"),
                "informative limit message expected, got: {msg}"
            );
        }
        other => panic!("expected Validation error, got: {other:?}"),
    }
}

#[tokio::test]
async fn ddl_if_not_exists_is_idempotent_and_reports_applied() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");

    let first = db
        .commit_ddl(DdlOperation::CreateProject {
            owner_id: None,
            project_id: "arcana".into(),
            if_not_exists: true,
        })
        .await
        .expect("create project first");
    assert!(first.applied);

    let second = db
        .commit_ddl(DdlOperation::CreateProject {
            owner_id: None,
            project_id: "arcana".into(),
            if_not_exists: true,
        })
        .await
        .expect("create project second");
    assert!(!second.applied);

    let scope_first = db
        .commit_ddl(DdlOperation::CreateScope {
            owner_id: None,
            project_id: "arcana".into(),
            scope_id: "app".into(),
            if_not_exists: true,
        })
        .await
        .expect("create scope first");
    assert!(!scope_first.applied, "default app scope already exists");

    let table_first = db
        .commit_ddl(DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: true,
            project_id: "arcana".into(),
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
        })
        .await
        .expect("create table first");
    assert!(table_first.applied);

    let table_second = db
        .commit_ddl(DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: true,
            project_id: "arcana".into(),
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
        })
        .await
        .expect("create table second");
    assert!(!table_second.applied);

    let err = db
        .commit(Mutation::Ddl(DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: false,
            project_id: "arcana".into(),
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
        .expect_err("duplicate create should error without if_not_exists");
    assert!(matches!(
        err,
        AedbError::AlreadyExists {
            resource_type: ErrorResourceType::Table,
            ..
        }
    ));

    let drop_noop = db
        .commit_ddl(DdlOperation::DropTable {
            project_id: "arcana".into(),
            scope_id: "app".into(),
            table_name: "does_not_exist".into(),
            if_exists: true,
        })
        .await
        .expect("drop noop with if_exists");
    assert!(!drop_noop.applied);

    let drop_err = db
        .commit(Mutation::Ddl(DdlOperation::DropTable {
            project_id: "arcana".into(),
            scope_id: "app".into(),
            table_name: "does_not_exist".into(),
            if_exists: false,
        }))
        .await
        .expect_err("drop missing should error when if_exists=false");
    assert!(matches!(
        drop_err,
        AedbError::NotFound {
            resource_type: ErrorResourceType::Table,
            ..
        }
    ));
}

#[tokio::test]
async fn index_introspection_apis_return_index_definitions() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("arcana").await.expect("project");
    db.commit_ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: true,
        project_id: "arcana".into(),
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
    })
    .await
    .expect("create table");
    db.commit_ddl(DdlOperation::CreateIndex {
        project_id: "arcana".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        index_name: "idx_users_name".into(),
        if_not_exists: false,
        columns: vec!["name".into()],
        index_type: IndexType::BTree,
        partial_filter: None,
    })
    .await
    .expect("create index");

    let listed = db
        .list_indexes("arcana", "app", "users")
        .await
        .expect("list indexes");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].index_name, "idx_users_name");

    let described = db
        .describe_index("arcana", "app", "users", "idx_users_name")
        .await
        .expect("describe index");
    assert_eq!(described.columns, vec!["name".to_string()]);
}

#[tokio::test]
async fn read_tx_keeps_snapshot_consistency_across_queries() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");
    create_table(
        &db,
        "p",
        "app",
        "accounts",
        vec![
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
        vec!["id"],
    )
    .await;
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1), Value::Integer(10)]),
    })
    .await
    .expect("seed");

    let tx = db
        .begin_read_tx(ConsistencyMode::AtLatest)
        .await
        .expect("read tx");
    let before = tx
        .query(
            "p",
            "app",
            Query::select(&["balance"])
                .from("accounts")
                .where_(Expr::Eq("id".into(), Value::Integer(1))),
        )
        .await
        .expect("before");
    assert_eq!(before.rows[0].values[0], Value::Integer(10));

    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1), Value::Integer(99)]),
    })
    .await
    .expect("update");

    let still_before = tx
        .query(
            "p",
            "app",
            Query::select(&["balance"])
                .from("accounts")
                .where_(Expr::Eq("id".into(), Value::Integer(1))),
        )
        .await
        .expect("stable read");
    assert_eq!(still_before.rows[0].values[0], Value::Integer(10));
}

/// Build an ECS-shaped `entities(instance_id, entity_id, component_name, data)`
/// table with composite PK, seeded across several instances/entities. Returns
/// the open db in a temp dir kept alive by the returned guard.
async fn seed_entities() -> (AedbInstance, tempfile::TempDir) {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "app").await.expect("scope");
    create_table(
        &db,
        "arcana",
        "app",
        "entities",
        vec![
            ColumnDef {
                name: "instance_id".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "entity_id".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "component_name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "data".into(),
                col_type: ColumnType::Json,
                nullable: false,
            },
        ],
        vec!["instance_id", "entity_id", "component_name"],
    )
    .await;
    // Two instances; instance "a" has two entities with two components each,
    // instance "b" has one entity — enough to prove the prefix band isolates a
    // single instance and, within it, a single entity.
    let rows = [
        ("a", "e1", "Pos", r#"{"x":1}"#),
        ("a", "e1", "Vel", r#"{"dx":2}"#),
        ("a", "e2", "Pos", r#"{"x":3}"#),
        ("a", "e2", "Vel", r#"{"dx":4}"#),
        ("b", "e1", "Pos", r#"{"x":9}"#),
    ];
    for (inst, eid, comp, data) in rows {
        db.commit(Mutation::Upsert {
            project_id: "arcana".into(),
            scope_id: "app".into(),
            table_name: "entities".into(),
            primary_key: vec![
                Value::Text(inst.into()),
                Value::Text(eid.into()),
                Value::Text(comp.into()),
            ],
            row: Row::from_values(vec![
                Value::Text(inst.into()),
                Value::Text(eid.into()),
                Value::Text(comp.into()),
                Value::Json(data.into()),
            ]),
        })
        .await
        .expect("seed row");
    }
    (db, dir)
}

/// #1: a synchronous `ReadTx` handle serves lazy, partial reads via a leading
/// primary-key prefix, returning rows in deterministic PK order and matching the
/// async path — without eagerly loading the whole table.
#[tokio::test]
async fn read_tx_sync_prefix_query_loads_single_instance() {
    let (db, _dir) = seed_entities().await;
    let tx = db
        .begin_read_tx(ConsistencyMode::AtLatest)
        .await
        .expect("read tx");

    // Leading-prefix predicate on `instance_id` → only instance "a" (4 rows).
    let q = || {
        Query::select(&["entity_id", "component_name"])
            .from("entities")
            .where_(Expr::Eq("instance_id".into(), Value::Text("a".into())))
    };
    let sync_res = tx.query_sync("arcana", "app", q()).expect("sync query");
    assert_eq!(sync_res.rows.len(), 4, "only instance a's rows");
    // Deterministic PK order: entity_id ascending, then component_name.
    let ids: Vec<(String, String)> = sync_res
        .rows
        .iter()
        .map(|r| {
            let Value::Text(e) = &r.values[0] else {
                panic!("entity_id text")
            };
            let Value::Text(c) = &r.values[1] else {
                panic!("component text")
            };
            (e.to_string(), c.to_string())
        })
        .collect();
    assert_eq!(
        ids,
        vec![
            ("e1".into(), "Pos".into()),
            ("e1".into(), "Vel".into()),
            ("e2".into(), "Pos".into()),
            ("e2".into(), "Vel".into()),
        ]
    );

    // The sync path matches the async path exactly.
    let async_res = tx.query("arcana", "app", q()).await.expect("async query");
    assert_eq!(sync_res.rows, async_res.rows);

    // Two-column prefix isolates a single entity's components.
    let one_entity = tx
        .query_sync(
            "arcana",
            "app",
            Query::select(&["component_name"]).from("entities").where_(
                Expr::Eq("instance_id".into(), Value::Text("a".into()))
                    .and(Expr::Eq("entity_id".into(), Value::Text("e1".into()))),
            ),
        )
        .expect("entity query");
    assert_eq!(one_entity.rows.len(), 2);

    // exists_sync short-circuits.
    assert!(
        tx.exists_sync(
            "arcana",
            "app",
            Query::select(&["component_name"]).from("entities").where_(
                Expr::Eq("instance_id".into(), Value::Text("b".into()))
                    .and(Expr::Eq("entity_id".into(), Value::Text("e1".into()))),
            ),
        )
        .expect("exists")
    );
}

/// #1: EXPLAIN reports the bounded key-range access path for a leading-prefix
/// predicate (exact prefix ⇒ no residual filter), instead of a full scan.
#[tokio::test]
async fn explain_reports_primary_key_prefix_scan() {
    let (db, _dir) = seed_entities().await;
    let tx = db
        .begin_read_tx(ConsistencyMode::AtLatest)
        .await
        .expect("read tx");
    let diag = tx
        .explain(
            "arcana",
            "app",
            Query::select(&["*"])
                .from("entities")
                .where_(Expr::Eq("instance_id".into(), Value::Text("a".into()))),
            QueryOptions::default(),
        )
        .expect("explain");
    assert_eq!(
        diag.predicate_evaluation_path,
        PredicateEvaluationPath::PrimaryKeyPrefixScan
    );
    // Exact prefix: no residual Filter stage.
    assert!(!diag.stages.contains(&ExecutionStage::Filter));

    // A gap predicate (`instance_id = a AND component_name = Pos`) still uses the
    // prefix band on `instance_id`, but keeps a residual filter for the gap.
    let diag_gap = tx
        .explain(
            "arcana",
            "app",
            Query::select(&["*"]).from("entities").where_(
                Expr::Eq("instance_id".into(), Value::Text("a".into()))
                    .and(Expr::Eq("component_name".into(), Value::Text("Pos".into()))),
            ),
            QueryOptions::default(),
        )
        .expect("explain gap");
    assert_eq!(
        diag_gap.predicate_evaluation_path,
        PredicateEvaluationPath::PrimaryKeyPrefixScan
    );
    assert!(diag_gap.stages.contains(&ExecutionStage::Filter));
}

/// #1 corollary: a `DeleteWhere` pinning a `(instance_id, entity_id)` PK prefix
/// (Arcana's entity despawn) removes exactly that entity's component rows.
#[tokio::test]
async fn delete_where_primary_key_prefix_despawns_one_entity() {
    let (db, _dir) = seed_entities().await;
    db.commit(Mutation::DeleteWhere {
        project_id: "arcana".into(),
        scope_id: "app".into(),
        table_name: "entities".into(),
        predicate: Expr::Eq("instance_id".into(), Value::Text("a".into()))
            .and(Expr::Eq("entity_id".into(), Value::Text("e1".into()))),
        limit: None,
    })
    .await
    .expect("despawn");

    let tx = db
        .begin_read_tx(ConsistencyMode::AtLatest)
        .await
        .expect("read tx");
    // a/e1 gone (2 rows), a/e2 intact (2 rows), b untouched (1 row).
    let remaining_a = tx
        .query_sync(
            "arcana",
            "app",
            Query::select(&["entity_id"])
                .from("entities")
                .where_(Expr::Eq("instance_id".into(), Value::Text("a".into()))),
        )
        .expect("remaining a");
    assert_eq!(remaining_a.rows.len(), 2);
    assert!(
        remaining_a
            .rows
            .iter()
            .all(|r| r.values[0] == Value::Text("e2".into()))
    );
    let all = tx
        .query_sync(
            "arcana",
            "app",
            Query::select(&["*"]).from("entities").limit(100),
        )
        .expect("all");
    assert_eq!(all.rows.len(), 3, "5 seeded − 2 despawned");
}

#[tokio::test]
async fn list_batch_and_lookup_helpers_work() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");
    create_table(
        &db,
        "p",
        "app",
        "items",
        vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "user_id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        vec!["id"],
    )
    .await;
    create_table(
        &db,
        "p",
        "app",
        "users",
        vec![
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
        vec!["id"],
    )
    .await;
    for i in 1..=5 {
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "items".into(),
            primary_key: vec![Value::Integer(i)],
            row: Row::from_values(vec![Value::Integer(i), Value::Integer(i)]),
        })
        .await
        .expect("seed item");
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(i)],
            row: Row::from_values(vec![Value::Integer(i), Value::Text(format!("u{i}").into())]),
        })
        .await
        .expect("seed user");
    }

    let batched = db
        .query_batch(
            "p",
            "app",
            vec![
                QueryBatchItem {
                    query: Query::select(&["id"]).from("items").limit(2),
                    options: QueryOptions::default(),
                },
                QueryBatchItem {
                    query: Query::select(&["id"]).from("items").limit(3),
                    options: QueryOptions::default(),
                },
            ],
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("query batch");
    assert_eq!(batched.len(), 2);
    assert_eq!(batched[0].snapshot_seq, batched[1].snapshot_seq);

    let first = db
        .list_with_total(
            "p",
            "app",
            Query::select(&["id"]).from("items"),
            None,
            None,
            2,
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("list");
    assert_eq!(first.total_count, 5);
    assert_eq!(first.rows.len(), 2);
    assert!(first.next_cursor.is_some());

    let via_offset = db
        .list_with_total(
            "p",
            "app",
            Query::select(&["id"]).from("items"),
            None,
            Some(3),
            2,
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("offset");
    assert_eq!(via_offset.total_count, 5);
    assert_eq!(via_offset.rows.len(), 2);

    let (source, hydrated) = db
        .lookup_then_hydrate(
            "p",
            "app",
            Query::select(&["user_id"]).from("items").limit(3),
            0,
            Query::select(&["id", "name"]).from("users"),
            "id",
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("lookup/hydrate");
    assert_eq!(source.rows.len(), 3);
    assert!(!source.truncated);
    assert_eq!(hydrated.rows.len(), 3);
    assert!(!hydrated.truncated);
}

#[tokio::test]
async fn lookup_then_hydrate_fetches_all_pages_for_large_key_sets() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");

    create_table(
        &db,
        "p",
        "app",
        "items",
        vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "user_id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        vec!["id"],
    )
    .await;
    create_table(
        &db,
        "p",
        "app",
        "users",
        vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "username".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        vec!["id"],
    )
    .await;

    for id in 1_i64..=1_000_i64 {
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "items".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row::from_values(vec![Value::Integer(id), Value::Integer(id)]),
        })
        .await
        .expect("insert item");
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row::from_values(vec![
                Value::Integer(id),
                Value::Text(format!("u{id}").into()),
            ]),
        })
        .await
        .expect("insert user");
    }

    let (source, hydrated) = db
        .lookup_then_hydrate(
            "p",
            "app",
            Query::select(&["user_id"]).from("items").limit(1_000),
            0,
            Query::select(&["id", "username"]).from("users"),
            "id",
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("lookup/hydrate");
    assert_eq!(source.rows.len(), 1_000);
    assert!(!source.truncated);
    assert_eq!(hydrated.rows.len(), 1_000);
    assert!(!hydrated.truncated);
}

#[tokio::test]
async fn telemetry_sql_and_remote_adapter_paths_work() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");
    create_table(
        &db,
        "p",
        "app",
        "items",
        vec![ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Integer,
            nullable: false,
        }],
        vec!["id"],
    )
    .await;
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "items".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1)]),
    })
    .await
    .expect("seed");

    let hook = Arc::new(RecordingTelemetryHook {
        queries: Arc::new(std::sync::Mutex::new(Vec::new())),
        commits: Arc::new(std::sync::Mutex::new(Vec::new())),
    });
    db.add_telemetry_hook(hook.clone());

    let sql_result = db
        .query_sql_read_only(
            &EchoSqlAdapter,
            "p",
            "app",
            "select id from items",
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("sql adapter");
    assert_eq!(sql_result.rows.len(), 1);

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"remote:test".to_vec(),
        value: b"ok".to_vec(),
    })
    .await
    .expect("kv set");

    let remote_dir = tempdir().expect("remote");
    let uri = remote_dir.path().join("snapshot");
    let adapter = LocalRemoteAdapter;
    let uri_str = uri.to_string_lossy().to_string();
    db.backup_full_to_remote(&adapter, &uri_str)
        .await
        .expect("backup remote");

    let restored_dir = tempdir().expect("restored");
    AedbInstance::restore_from_remote(
        &adapter,
        &uri_str,
        restored_dir.path(),
        &AedbConfig::default(),
    )
    .expect("restore remote");
    let restored = AedbInstance::open_anonymous(AedbConfig::default(), restored_dir.path())
        .expect("open restored");
    let restored_val = restored
        .kv_get_no_auth("p", "app", b"remote:test", ConsistencyMode::AtLatest)
        .await
        .expect("kv get");
    assert_eq!(restored_val.expect("entry").value, b"ok".to_vec());

    let query_count = hook.queries.lock().expect("query telemetry").len();
    let commit_count = hook.commits.lock().expect("commit telemetry").len();
    assert!(query_count >= 1);
    assert!(commit_count >= 1);
}

#[tokio::test]
async fn removing_last_telemetry_hook_disables_commit_and_query_callbacks() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");
    create_table(
        &db,
        "p",
        "app",
        "items",
        vec![ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Integer,
            nullable: false,
        }],
        vec!["id"],
    )
    .await;

    let hook = Arc::new(RecordingTelemetryHook {
        queries: Arc::new(std::sync::Mutex::new(Vec::new())),
        commits: Arc::new(std::sync::Mutex::new(Vec::new())),
    });
    let hook_trait: Arc<dyn QueryCommitTelemetryHook> = hook.clone();
    db.add_telemetry_hook(hook_trait.clone());
    db.remove_telemetry_hook(&hook_trait);

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"telemetry:disabled".to_vec(),
        value: b"ok".to_vec(),
    })
    .await
    .expect("commit");
    let _ = db
        .query_no_auth(
            "p",
            "app",
            Query::select(&["id"]).from("items").limit(1),
            QueryOptions::default(),
        )
        .await
        .expect("query");

    assert!(hook.queries.lock().expect("query telemetry").is_empty());
    assert!(hook.commits.lock().expect("commit telemetry").is_empty());
}

#[tokio::test]
async fn exists_and_explain_diagnostics_work() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");
    create_table(
        &db,
        "p",
        "app",
        "items",
        vec![
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
        vec!["id"],
    )
    .await;
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "items".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1), Value::Text("a".into())]),
    })
    .await
    .expect("seed");

    let tx = db
        .begin_read_tx(ConsistencyMode::AtLatest)
        .await
        .expect("read tx");
    let exists = tx
        .exists(
            "p",
            "app",
            Query::select(&["id"])
                .from("items")
                .where_(Expr::Eq("id".into(), Value::Integer(1))),
        )
        .await
        .expect("exists");
    assert!(exists);

    let explain = tx
        .explain(
            "p",
            "app",
            Query::select(&["id"]).from("items"),
            QueryOptions::default(),
        )
        .expect("explain");
    assert!(explain.estimated_scan_rows >= 1);
    assert!(explain.stages.contains(&ExecutionStage::Scan));
    assert!(
        !explain.plan_trace.is_empty(),
        "explain should include access-path trace"
    );

    let with_diag = tx
        .query_with_diagnostics(
            "p",
            "app",
            Query::select(&["id"]).from("items"),
            QueryOptions {
                allow_full_scan: true,
                ..QueryOptions::default()
            },
        )
        .await
        .expect("diag query");
    assert_eq!(with_diag.result.rows.len(), 1);
    assert_eq!(with_diag.diagnostics.snapshot_seq, tx.snapshot_seq());
}

#[tokio::test]
async fn explain_reports_ordered_index_row_source() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");
    create_table(
        &db,
        "p",
        "app",
        "items",
        vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "score".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        vec!["id"],
    )
    .await;
    db.commit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "items".into(),
        index_name: "by_score".into(),
        if_not_exists: false,
        columns: vec!["score".into()],
        index_type: IndexType::BTree,
        partial_filter: None,
    }))
    .await
    .expect("score index");

    for (id, score) in [(1_i64, 30_i64), (2, 10), (3, 20)] {
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "items".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row::from_values(vec![Value::Integer(id), Value::Integer(score)]),
        })
        .await
        .expect("seed item");
    }

    let explain = db
        .explain_query(
            "p",
            "app",
            Query::select(&["id", "score"])
                .from("items")
                .order_by("score", Order::Asc)
                .limit(2),
            QueryOptions::default(),
        )
        .await
        .expect("explain ordered index");

    assert_eq!(explain.index_used, Some("by_score".to_string()));
    assert_eq!(explain.selected_indexes, vec!["by_score".to_string()]);
    assert_eq!(
        explain.predicate_evaluation_path,
        PredicateEvaluationPath::None
    );
    assert!(
        !explain.stages.contains(&ExecutionStage::Sort),
        "ordered row source already satisfies ORDER BY"
    );
    assert!(
        explain
            .plan_trace
            .iter()
            .any(|line| line.contains("ordered row source")),
        "plan trace should report ordered index row source"
    );
}

#[tokio::test]
async fn non_pk_text_eq_regression_in_project_scope_indexed_and_non_indexed_paths() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    let project_scope_id = "__project__";
    db.create_project("p").await.expect("project");
    db.create_scope("p", project_scope_id)
        .await
        .expect("project scope");
    create_table(
        &db,
        "p",
        project_scope_id,
        "sessions",
        vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "user_id".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "status".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        vec!["id"],
    )
    .await;

    for (id, user_id, status) in [
        (1_i64, "8a25f1bc-ea96-48d0-8535-47b784a2df1d", "open"),
        (2, "8a25f1bc-ea96-48d0-8535-47b784a2df1d", "closed"),
        (3, "2cf2434c-ed95-4f35-b786-4853592e6f25", "open"),
    ] {
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: project_scope_id.into(),
            table_name: "sessions".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row::from_values(vec![
                Value::Integer(id),
                Value::Text(user_id.into()),
                Value::Text(status.into()),
            ]),
        })
        .await
        .expect("seed session");
    }

    let query = Query::select(&["id", "user_id"])
        .from("sessions")
        .where_(Expr::Eq(
            "user_id".into(),
            Value::Text("8a25f1bc-ea96-48d0-8535-47b784a2df1d".into()),
        ));
    let pre_index_result = db
        .query_no_auth(
            "p",
            project_scope_id,
            query.clone(),
            QueryOptions::default(),
        )
        .await
        .expect("eq query without index");
    assert_eq!(pre_index_result.rows.len(), 2);

    let pre_index_explain = db
        .explain_query(
            "p",
            project_scope_id,
            query.clone(),
            QueryOptions::default(),
        )
        .await
        .expect("explain without index");
    assert_eq!(
        pre_index_explain.predicate_evaluation_path,
        PredicateEvaluationPath::FullScanFilter
    );
    assert!(
        pre_index_explain.selected_indexes.is_empty(),
        "non-index path should not report selected indexes"
    );

    db.commit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: "p".into(),
        scope_id: project_scope_id.into(),
        table_name: "sessions".into(),
        index_name: "by_user_id".into(),
        if_not_exists: false,
        columns: vec!["user_id".into()],
        index_type: IndexType::BTree,
        partial_filter: None,
    }))
    .await
    .expect("user_id index");

    let indexed_result = db
        .query_no_auth(
            "p",
            project_scope_id,
            query.clone(),
            QueryOptions::default(),
        )
        .await
        .expect("eq query with index");
    assert_eq!(indexed_result.rows.len(), 2);
    assert!(
        indexed_result.rows_examined <= pre_index_result.rows_examined,
        "indexed equality should not examine more rows"
    );

    let indexed_explain = db
        .explain_query("p", project_scope_id, query, QueryOptions::default())
        .await
        .expect("explain with index");
    assert_eq!(
        indexed_explain.predicate_evaluation_path,
        PredicateEvaluationPath::SecondaryIndexLookup
    );
    assert!(
        indexed_explain
            .selected_indexes
            .contains(&"by_user_id".to_string()),
        "explain should report selected secondary index"
    );
    assert!(
        indexed_explain
            .plan_trace
            .iter()
            .any(|line| line.contains("by_user_id")),
        "plan trace should include selected index name"
    );
    assert!(
        !indexed_explain.stages.contains(&ExecutionStage::Filter),
        "exact indexed equality should not report a residual filter stage"
    );
}

#[tokio::test]
async fn uuid_text_equality_parity_between_primary_key_and_secondary_index_paths() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");

    create_table(
        &db,
        "p",
        "app",
        "users_pk_uuid",
        vec![
            ColumnDef {
                name: "user_uuid".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "display_name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        vec!["user_uuid"],
    )
    .await;
    create_table(
        &db,
        "p",
        "app",
        "users_secondary_uuid",
        vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "user_uuid".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "display_name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        vec!["id"],
    )
    .await;
    db.commit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users_secondary_uuid".into(),
        index_name: "by_user_uuid".into(),
        if_not_exists: false,
        columns: vec!["user_uuid".into()],
        index_type: IndexType::BTree,
        partial_filter: None,
    }))
    .await
    .expect("secondary uuid index");

    let rows = [
        (1_i64, "8e1e917f-f8f8-4f06-bf38-3f2c37dcd857", "alice"),
        (2, "6e0df6fd-5095-4e37-bf9d-1f6b5d6dfcb8", "bob"),
        (3, "1b631635-766d-4207-aa53-f5367b9bf13a", "carol"),
    ];
    for (_id, user_uuid, display_name) in rows {
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users_pk_uuid".into(),
            primary_key: vec![Value::Text(user_uuid.to_string().into())],
            row: Row::from_values(vec![
                Value::Text(user_uuid.to_string().into()),
                Value::Text(display_name.into()),
            ]),
        })
        .await
        .expect("seed pk uuid");
    }

    for (id, user_uuid, display_name) in rows {
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users_secondary_uuid".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row::from_values(vec![
                Value::Integer(id),
                Value::Text(user_uuid.into()),
                Value::Text(display_name.into()),
            ]),
        })
        .await
        .expect("seed secondary uuid");
    }

    for (_id, user_uuid, _display_name) in rows {
        let pk_query = Query::select(&["display_name"])
            .from("users_pk_uuid")
            .where_(Expr::Eq(
                "user_uuid".into(),
                Value::Text(user_uuid.to_string().into()),
            ));
        let secondary_query = Query::select(&["display_name"])
            .from("users_secondary_uuid")
            .where_(Expr::Eq(
                "user_uuid".into(),
                Value::Text(user_uuid.to_string().into()),
            ));

        let pk_result = db
            .query_no_auth("p", "app", pk_query.clone(), QueryOptions::default())
            .await
            .expect("pk query");
        let secondary_result = db
            .query_no_auth("p", "app", secondary_query.clone(), QueryOptions::default())
            .await
            .expect("secondary query");
        assert_eq!(pk_result.rows.len(), 1);
        assert_eq!(secondary_result.rows.len(), 1);
        assert_eq!(pk_result.rows[0].values, secondary_result.rows[0].values);
    }

    let pk_query = Query::select(&["display_name"])
        .from("users_pk_uuid")
        .where_(Expr::Eq(
            "user_uuid".into(),
            Value::Text("8e1e917f-f8f8-4f06-bf38-3f2c37dcd857".into()),
        ));
    let secondary_query = Query::select(&["display_name"])
        .from("users_secondary_uuid")
        .where_(Expr::Eq(
            "user_uuid".into(),
            Value::Text("8e1e917f-f8f8-4f06-bf38-3f2c37dcd857".into()),
        ));

    let pk_explain = db
        .explain_query("p", "app", pk_query, QueryOptions::default())
        .await
        .expect("pk explain");
    assert_eq!(
        pk_explain.predicate_evaluation_path,
        PredicateEvaluationPath::PrimaryKeyEqLookup
    );
    assert!(
        !pk_explain.stages.contains(&ExecutionStage::Filter),
        "primary-key equality should not report a residual filter stage"
    );

    let secondary_explain = db
        .explain_query("p", "app", secondary_query, QueryOptions::default())
        .await
        .expect("secondary explain");
    assert_eq!(
        secondary_explain.predicate_evaluation_path,
        PredicateEvaluationPath::SecondaryIndexLookup
    );
    assert!(
        secondary_explain
            .selected_indexes
            .contains(&"by_user_uuid".to_string())
    );
    assert!(
        !secondary_explain.stages.contains(&ExecutionStage::Filter),
        "exact secondary-index equality should not report a residual filter stage"
    );
}

#[tokio::test]
async fn u8_column_type_supports_write_read_and_indexed_equality() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");

    create_table(
        &db,
        "p",
        "app",
        "levels",
        vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "level".into(),
                col_type: ColumnType::U8,
                nullable: false,
            },
        ],
        vec!["id"],
    )
    .await;

    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "levels".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1), Value::U8(7)]),
    })
    .await
    .expect("seed u8 row");

    let without_index = db
        .query_no_auth(
            "p",
            "app",
            Query::select(&["id", "level"])
                .from("levels")
                .where_(Expr::Eq("level".into(), Value::Integer(7))),
            QueryOptions::default(),
        )
        .await
        .expect("u8 equality via integer literal");
    assert_eq!(without_index.rows.len(), 1);
    assert_eq!(without_index.rows[0].values[1], Value::U8(7));

    db.commit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "levels".into(),
        index_name: "by_level".into(),
        if_not_exists: false,
        columns: vec!["level".into()],
        index_type: IndexType::BTree,
        partial_filter: None,
    }))
    .await
    .expect("create u8 index");

    let with_index = db
        .query_no_auth(
            "p",
            "app",
            Query::select(&["id", "level"])
                .from("levels")
                .where_(Expr::Eq("level".into(), Value::U8(7))),
            QueryOptions::default(),
        )
        .await
        .expect("u8 equality with index");
    assert_eq!(with_index.rows.len(), 1);
    assert!(with_index.rows_examined <= without_index.rows_examined);

    let err = db
        .commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "levels".into(),
            primary_key: vec![Value::Integer(2)],
            row: Row::from_values(vec![Value::Integer(2), Value::Integer(8)]),
        })
        .await
        .expect_err("integer value should not satisfy U8 column type");
    assert!(matches!(err, AedbError::TypeMismatch { .. }));
}

#[tokio::test]
async fn u64_column_type_supports_write_read_and_indexed_equality() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");

    create_table(
        &db,
        "p",
        "app",
        "balances_u64",
        vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "balance".into(),
                col_type: ColumnType::U64,
                nullable: false,
            },
        ],
        vec!["id"],
    )
    .await;

    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "balances_u64".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1), Value::U64(7)]),
    })
    .await
    .expect("seed u64 row");

    let without_index = db
        .query_no_auth(
            "p",
            "app",
            Query::select(&["id", "balance"])
                .from("balances_u64")
                .where_(Expr::Eq("balance".into(), Value::Integer(7))),
            QueryOptions::default(),
        )
        .await
        .expect("u64 equality via integer literal");
    assert_eq!(without_index.rows.len(), 1);
    assert_eq!(without_index.rows[0].values[1], Value::U64(7));

    db.commit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "balances_u64".into(),
        index_name: "by_balance_u64".into(),
        if_not_exists: false,
        columns: vec!["balance".into()],
        index_type: IndexType::BTree,
        partial_filter: None,
    }))
    .await
    .expect("create u64 index");

    let with_index = db
        .query_no_auth(
            "p",
            "app",
            Query::select(&["id", "balance"])
                .from("balances_u64")
                .where_(Expr::Eq("balance".into(), Value::U64(7))),
            QueryOptions::default(),
        )
        .await
        .expect("u64 equality with index");
    assert_eq!(with_index.rows.len(), 1);
    assert!(with_index.rows_examined <= without_index.rows_examined);

    let err = db
        .commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "balances_u64".into(),
            primary_key: vec![Value::Integer(2)],
            row: Row::from_values(vec![Value::Integer(2), Value::Integer(8)]),
        })
        .await
        .expect_err("integer value should not satisfy U64 column type");
    assert!(matches!(err, AedbError::TypeMismatch { .. }));
}

#[tokio::test]
async fn sql_transaction_plan_helpers_commit() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");
    create_table(
        &db,
        "p",
        "app",
        "items",
        vec![ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Integer,
            nullable: false,
        }],
        vec!["id"],
    )
    .await;

    let plan = db
        .plan_sql_transaction(
            ConsistencyMode::AtLatest,
            vec![Mutation::Upsert {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "items".into(),
                primary_key: vec![Value::Integer(10)],
                row: Row::from_values(vec![Value::Integer(10)]),
            }],
        )
        .await
        .expect("plan");
    db.commit_sql_transaction_plan(plan)
        .await
        .expect("commit plan");

    let exists = db
        .exists(
            "p",
            "app",
            Query::select(&["id"])
                .from("items")
                .where_(Expr::Eq("id".into(), Value::Integer(10))),
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("exists");
    assert!(exists);
}

#[tokio::test]
async fn list_with_total_respects_scan_bounds_for_count_queries() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        max_scan_rows: 3,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open_anonymous(config, dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");
    create_table(
        &db,
        "p",
        "app",
        "items",
        vec![
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
        vec!["id"],
    )
    .await;

    for id in 1_i64..=5_i64 {
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "items".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row::from_values(vec![
                Value::Integer(id),
                Value::Text(format!("item-{id}").into()),
            ]),
        })
        .await
        .expect("insert item");
    }

    let err = db
        .list_with_total(
            "p",
            "app",
            Query::select(&["id"]).from("items"),
            None,
            None,
            2,
            ConsistencyMode::AtLatest,
        )
        .await
        .expect_err("count query should respect scan bounds");
    assert!(matches!(
        err,
        QueryError::ScanBoundExceeded {
            estimated_rows: 5,
            max_scan_rows: 3
        }
    ));
}

#[tokio::test]
async fn delete_where_rejects_deep_predicate_trees() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");
    create_table(
        &db,
        "p",
        "app",
        "items",
        vec![ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Integer,
            nullable: false,
        }],
        vec!["id"],
    )
    .await;

    let mut predicate = Expr::IsNotNull("id".into());
    for _ in 0..40 {
        predicate = predicate.and(Expr::IsNotNull("id".into()));
    }

    let err = db
        .commit(Mutation::DeleteWhere {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "items".into(),
            predicate,
            limit: None,
        })
        .await
        .expect_err("deep predicate should be rejected");
    assert!(matches!(err, AedbError::Validation(ref msg) if msg.contains("expression depth")));
}

#[tokio::test]
async fn insert_batch_rejects_duplicate_primary_keys_within_same_batch() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");
    create_table(
        &db,
        "p",
        "app",
        "users",
        vec![
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
        vec!["id"],
    )
    .await;

    let err = db
        .insert_batch(
            "p",
            "app",
            "users",
            vec![
                Row::from_values(vec![Value::Integer(1), Value::Text("alice".into())]),
                Row::from_values(vec![Value::Integer(1), Value::Text("alice-dup".into())]),
            ],
        )
        .await
        .expect_err("duplicate PKs in one batch should fail before apply");
    assert!(matches!(err, AedbError::DuplicatePK { .. }));

    let rows = db
        .query_no_auth(
            "p",
            "app",
            Query::select(&["id", "name"]).from("users").limit(10),
            QueryOptions::default(),
        )
        .await
        .expect("query users");
    assert!(
        rows.rows.is_empty(),
        "failed batch must not partially apply"
    );
}

#[tokio::test]
async fn delete_where_respects_scan_budget() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        max_scan_rows: 3,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open_anonymous(config, dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");
    create_table(
        &db,
        "p",
        "app",
        "items",
        vec![ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Integer,
            nullable: false,
        }],
        vec!["id"],
    )
    .await;

    for id in 1_i64..=5_i64 {
        db.commit(Mutation::Insert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "items".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row::from_values(vec![Value::Integer(id)]),
        })
        .await
        .expect("insert item");
    }

    let err = db
        .commit(Mutation::DeleteWhere {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "items".into(),
            predicate: Expr::IsNotNull("id".into()),
            limit: None,
        })
        .await
        .expect_err("delete_where should honor scan budget");
    assert!(
        matches!(err, AedbError::Validation(ref msg) if msg.contains("mutation scan bound exceeded"))
    );

    for id in 1_i64..=5_i64 {
        let row = db
            .query_no_auth(
                "p",
                "app",
                Query::select(&["id"])
                    .from("items")
                    .where_(Expr::Eq("id".into(), Value::Integer(id)))
                    .limit(1),
                QueryOptions::default(),
            )
            .await
            .expect("query item by id");
        assert_eq!(row.rows.len(), 1, "row {id} should still exist");
    }
}

#[tokio::test]
async fn count_compare_assertions_respect_scan_budget() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        max_scan_rows: 3,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open_anonymous(config, dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");
    create_table(
        &db,
        "p",
        "app",
        "items",
        vec![ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Integer,
            nullable: false,
        }],
        vec!["id"],
    )
    .await;

    for id in 1_i64..=5_i64 {
        db.commit(Mutation::Insert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "items".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row::from_values(vec![Value::Integer(id)]),
        })
        .await
        .expect("insert item");
    }

    let err = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: vec![ReadAssertion::CountCompare {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "items".into(),
                filter: None,
                op: CompareOp::Eq,
                threshold: 5,
            }],
            read_set: Default::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"guard".to_vec(),
                    value: b"1".to_vec(),
                }],
            },
            base_seq: 0,
        })
        .await
        .expect_err("assertion scan should be bounded");
    assert!(
        matches!(err, AedbError::Validation(ref msg) if msg.contains("assertion scan bound exceeded"))
    );
}

#[tokio::test]
async fn float_columns_reject_non_finite_values() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    create_table(
        &db,
        "p",
        "app",
        "metrics",
        vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "value".into(),
                col_type: ColumnType::Float,
                nullable: false,
            },
        ],
        vec!["id"],
    )
    .await;

    let err = db
        .commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "metrics".into(),
            primary_key: vec![Value::Integer(1)],
            row: Row::from_values(vec![Value::Integer(1), Value::Float(f64::NAN)]),
        })
        .await
        .expect_err("nan should be rejected");
    assert!(matches!(err, AedbError::Validation(ref msg) if msg.contains("non-finite float")));
}

#[tokio::test]
async fn table_values_respect_max_table_value_bytes() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        max_table_value_bytes: 8,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open_anonymous(config, dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    create_table(
        &db,
        "p",
        "app",
        "docs",
        vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "body".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        vec!["id"],
    )
    .await;

    let err = db
        .commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "docs".into(),
            primary_key: vec![Value::Integer(1)],
            row: Row::from_values(vec![
                Value::Integer(1),
                Value::Text("too-large-body".into()),
            ]),
        })
        .await
        .expect_err("oversized cell should be rejected");
    assert!(matches!(err, AedbError::Validation(ref msg) if msg.contains("max_table_value_bytes")));
}

#[tokio::test]
async fn cascade_delete_respects_max_depth() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");

    for depth in 0..=9 {
        let table = format!("t{depth}");
        create_table(
            &db,
            "p",
            "app",
            &table,
            vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "parent_id".into(),
                    col_type: ColumnType::Integer,
                    nullable: true,
                },
            ],
            vec!["id"],
        )
        .await;
        if depth > 0 {
            db.commit_ddl(DdlOperation::AlterTable {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: table.clone(),
                alteration: crate::catalog::schema::TableAlteration::AddForeignKey(
                    crate::catalog::schema::ForeignKey {
                        name: format!("fk_t{depth}_to_t{}", depth - 1),
                        columns: vec!["parent_id".into()],
                        references_project_id: "p".into(),
                        references_scope_id: "app".into(),
                        references_table: format!("t{}", depth - 1),
                        references_columns: vec!["id".into()],
                        on_delete: crate::catalog::schema::ForeignKeyAction::Cascade,
                        on_update: crate::catalog::schema::ForeignKeyAction::Cascade,
                    },
                ),
            })
            .await
            .expect("add fk");
        }
        db.commit(Mutation::Insert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: table,
            primary_key: vec![Value::Integer(depth as i64)],
            row: Row::from_values(vec![
                Value::Integer(depth as i64),
                if depth == 0 {
                    Value::Null
                } else {
                    Value::Integer((depth - 1) as i64)
                },
            ]),
        })
        .await
        .expect("insert row");
    }

    let err = db
        .commit(Mutation::Delete {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "t0".into(),
            primary_key: vec![Value::Integer(0)],
        })
        .await
        .expect_err("cascade depth should be bounded");
    assert!(
        matches!(err, AedbError::Validation(ref msg) if msg.contains("cascade delete depth exceeded"))
    );
}
