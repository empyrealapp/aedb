use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::{AccumulatorValueType, ColumnDef};
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::validation::Mutation;
use aedb::engine_interface::{
    EffectBatch, EffectBatchCommitResult, EffectEvent, EffectOperation, EffectPrecondition,
    KeyedStateQueryRequest,
};
use aedb::error::AedbError;
use aedb::query::plan::ConsistencyMode;
use std::sync::{Arc, mpsc};

fn user_state_columns() -> Vec<ColumnDef> {
    vec![
        ColumnDef {
            name: "user_id".to_string(),
            col_type: ColumnType::Text,
            nullable: false,
        },
        ColumnDef {
            name: "points".to_string(),
            col_type: ColumnType::Integer,
            nullable: false,
        },
    ]
}

#[tokio::test(flavor = "current_thread")]
async fn effect_batch_precondition_rejects_without_side_effects() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open db");

    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "game").await.expect("scope");
    db.commit_ddl(DdlOperation::CreateTable {
        project_id: "arcana".into(),
        scope_id: "game".into(),
        table_name: "user_state".into(),
        owner_id: None,
        columns: user_state_columns(),
        primary_key: vec!["user_id".into()],
        if_not_exists: false,
    })
    .await
    .expect("create table");
    db.commit_ddl(DdlOperation::CreateAccumulator {
        project_id: "arcana".into(),
        scope_id: "game".into(),
        accumulator_name: "house_balance".into(),
        if_not_exists: false,
        value_type: AccumulatorValueType::BigInt,
        dedupe_retain_commits: Some(10_000),
        snapshot_every: 1_000,
        exposure_margin_bps: 1_000,
        exposure_ttl_commits: Some(10_000),
    })
    .await
    .expect("create accumulator");

    let batch = EffectBatch {
        preconditions: vec![EffectPrecondition::RequireAvailable {
            accumulator: "house_balance".into(),
            min_amount: 1,
        }],
        effects: vec![EffectOperation::Write {
            keyed_state: "user_state".into(),
            key: Value::Text("u1".into()),
            value: Row::from_values(vec![Value::Text("u1".into()), Value::Integer(10)]),
        }],
        events: vec![EffectEvent {
            event_name: "hand_settled".into(),
            event_key: "e1".into(),
            data_json: "{\"user_id\":\"u1\"}".into(),
        }],
    };

    let outcome = db
        .commit_effect_batch("arcana", "game", batch)
        .await
        .expect("effect batch result");
    match outcome {
        EffectBatchCommitResult::Rejected(rejected) => {
            assert_eq!(rejected.error_code, "available_below_min");
        }
        EffectBatchCommitResult::Applied(_) => panic!("expected rejection"),
    }

    let row = db
        .keyed_state_read(
            "arcana",
            "game",
            "user_state",
            Value::Text("u1".into()),
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("read keyed state");
    assert!(row.is_none(), "rejected batch must not write keyed state");

    let page = db
        .read_event_stream(Some("hand_settled"), 0, 10, ConsistencyMode::AtLatest)
        .await
        .expect("read events");
    assert!(
        page.events.is_empty(),
        "rejected batch must not emit events"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn release_accumulator_exposure_is_first_class_operation() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open db");

    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "game").await.expect("scope");
    db.create_accumulator("arcana", "game", "house_balance", Some(10_000), 1_000)
        .await
        .expect("create accumulator");

    db.accumulate("arcana", "game", "house_balance", 1_000, "seed".into(), 1)
        .await
        .expect("seed accumulator");
    db.expose_accumulator("arcana", "game", "house_balance", 200, "exp-1".into())
        .await
        .expect("expose");

    let before = db
        .accumulator_exposure_metrics("arcana", "game", "house_balance", ConsistencyMode::AtLatest)
        .await
        .expect("metrics before release");
    assert_eq!(before.total_exposure, 200);

    db.release_accumulator_exposure("arcana", "game", "house_balance", "exp-1".into())
        .await
        .expect("release exposure");

    let after = db
        .accumulator_exposure_metrics("arcana", "game", "house_balance", ConsistencyMode::AtLatest)
        .await
        .expect("metrics after release");
    assert_eq!(after.total_exposure, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn processor_pull_commit_and_context_commit_work() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open db");

    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "game").await.expect("scope");
    db.commit_ddl(DdlOperation::CreateTable {
        project_id: "arcana".into(),
        scope_id: "game".into(),
        table_name: "user_state".into(),
        owner_id: None,
        columns: user_state_columns(),
        primary_key: vec!["user_id".into()],
        if_not_exists: false,
    })
    .await
    .expect("create table");

    db.emit_event(
        "arcana",
        "game",
        "hand_settled",
        "evt-1".into(),
        "{\"user_id\":\"u1\",\"pnl\":5}".into(),
    )
    .await
    .expect("emit event");

    let pulled = db
        .processor_pull("hand_settled", "points_processor", 32)
        .await
        .expect("pull events");
    assert_eq!(pulled.events.len(), 1);
    assert!(pulled.last_commit_seq > 0);

    db.processor_commit(
        "points_processor",
        pulled.last_commit_seq,
        vec![Mutation::Upsert {
            project_id: "arcana".into(),
            scope_id: "game".into(),
            table_name: "user_state".into(),
            primary_key: vec![Value::Text("u1".into())],
            row: Row::from_values(vec![Value::Text("u1".into()), Value::Integer(5)]),
        }],
    )
    .await
    .expect("processor commit");

    let lag = db
        .reactive_processor_lag("points_processor", ConsistencyMode::AtLatest)
        .await
        .expect("lag");
    assert_eq!(lag.checkpoint_seq, pulled.last_commit_seq);
    assert!(lag.head_seq >= lag.checkpoint_seq);

    db.emit_event(
        "arcana",
        "game",
        "hand_settled",
        "evt-2".into(),
        "{\"user_id\":\"u1\",\"pnl\":7}".into(),
    )
    .await
    .expect("emit second event");

    let mut ctx = db.processor_context("arcana", "game", "points_processor", "hand_settled");
    let events = ctx.pull(32).await.expect("context pull");
    assert_eq!(events.len(), 1);
    ctx.write(
        "user_state",
        Value::Text("u1".into()),
        Row::from_values(vec![Value::Text("u1".into()), Value::Integer(12)]),
    );
    ctx.commit().await.expect("context commit");

    let row = db
        .keyed_state_read(
            "arcana",
            "game",
            "user_state",
            Value::Text("u1".into()),
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("read row")
        .expect("row must exist");
    assert_eq!(row.values.get(1), Some(&Value::Integer(12)));

    let lag_after = db
        .reactive_processor_lag("points_processor", ConsistencyMode::AtLatest)
        .await
        .expect("lag after");
    assert!(lag_after.checkpoint_seq >= lag.checkpoint_seq);
    assert!(lag_after.head_seq >= lag_after.checkpoint_seq);
}

#[tokio::test(flavor = "current_thread")]
async fn keyed_state_operations_reject_non_single_pk_tables() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open db");

    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "game").await.expect("scope");
    db.commit_ddl(DdlOperation::CreateTable {
        project_id: "arcana".into(),
        scope_id: "game".into(),
        table_name: "composite_state".into(),
        owner_id: None,
        columns: vec![
            ColumnDef {
                name: "shard".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "user_id".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "points".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        primary_key: vec!["shard".into(), "user_id".into()],
        if_not_exists: false,
    })
    .await
    .expect("create composite table");

    let write_err = db
        .keyed_state_write(
            "arcana",
            "game",
            "composite_state",
            Value::Text("u1".into()),
            Row::from_values(vec![
                Value::Text("s1".into()),
                Value::Text("u1".into()),
                Value::Integer(10),
            ]),
        )
        .await
        .expect_err("write should reject non-keyed table");
    assert!(matches!(write_err, AedbError::Validation(_)));

    let query_err = db
        .keyed_state_query_index(
            "arcana",
            "game",
            "composite_state",
            KeyedStateQueryRequest {
                index_name: "missing".into(),
                prefix: Vec::new(),
                offset: 0,
                limit: 10,
            },
            ConsistencyMode::AtLatest,
        )
        .await
        .expect_err("query should reject non-keyed table");
    assert!(matches!(query_err, AedbError::Validation(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keyed_state_update_rejects_concurrent_overwrite() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Arc::new(AedbInstance::open(Default::default(), dir.path()).expect("open db"));

    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "game").await.expect("scope");
    db.commit_ddl(DdlOperation::CreateTable {
        project_id: "arcana".into(),
        scope_id: "game".into(),
        table_name: "user_state".into(),
        owner_id: None,
        columns: user_state_columns(),
        primary_key: vec!["user_id".into()],
        if_not_exists: false,
    })
    .await
    .expect("create table");
    db.keyed_state_write(
        "arcana",
        "game",
        "user_state",
        Value::Text("u1".into()),
        Row::from_values(vec![Value::Text("u1".into()), Value::Integer(5)]),
    )
    .await
    .expect("seed row");

    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let db_for_task = Arc::clone(&db);
    let update_task = tokio::spawn(async move {
        db_for_task
            .keyed_state_update(
                "arcana",
                "game",
                "user_state",
                Value::Text("u1".into()),
                move |current| {
                    assert_eq!(
                        current,
                        Some(Row::from_values(vec![
                            Value::Text("u1".into()),
                            Value::Integer(5),
                        ]))
                    );
                    started_tx.send(()).expect("signal start");
                    release_rx.recv().expect("wait for concurrent write");
                    Ok(Some(Row::from_values(vec![
                        Value::Text("u1".into()),
                        Value::Integer(12),
                    ])))
                },
            )
            .await
    });

    started_rx.recv().expect("update has read the row");
    db.keyed_state_write(
        "arcana",
        "game",
        "user_state",
        Value::Text("u1".into()),
        Row::from_values(vec![Value::Text("u1".into()), Value::Integer(9)]),
    )
    .await
    .expect("concurrent overwrite");
    release_tx.send(()).expect("release update");

    let err = update_task
        .await
        .expect("join update task")
        .expect_err("stale update must fail");
    assert!(matches!(err, AedbError::AssertionFailed { .. }));

    let current = db
        .keyed_state_read(
            "arcana",
            "game",
            "user_state",
            Value::Text("u1".into()),
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("read current row")
        .expect("row exists");
    assert_eq!(current.values.get(1), Some(&Value::Integer(9)));
}

#[tokio::test(flavor = "current_thread")]
async fn processor_context_update_uses_pending_row_state() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open db");

    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "game").await.expect("scope");
    db.commit_ddl(DdlOperation::CreateTable {
        project_id: "arcana".into(),
        scope_id: "game".into(),
        table_name: "user_state".into(),
        owner_id: None,
        columns: user_state_columns(),
        primary_key: vec!["user_id".into()],
        if_not_exists: false,
    })
    .await
    .expect("create table");

    let mut ctx = db.processor_context("arcana", "game", "points_processor", "hand_settled");
    ctx.write(
        "user_state",
        Value::Text("u1".into()),
        Row::from_values(vec![Value::Text("u1".into()), Value::Integer(5)]),
    );
    ctx.update("user_state", Value::Text("u1".into()), |current| {
        let row = current.expect("pending row should be visible");
        assert_eq!(row.values.get(1), Some(&Value::Integer(5)));
        Ok(Some(Row::from_values(vec![
            Value::Text("u1".into()),
            Value::Integer(12),
        ])))
    })
    .await
    .expect("update pending row");
    ctx.commit().await.expect("commit context");

    let row = db
        .keyed_state_read(
            "arcana",
            "game",
            "user_state",
            Value::Text("u1".into()),
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("read current row")
        .expect("row exists");
    assert_eq!(row.values.get(1), Some(&Value::Integer(12)));
}

#[tokio::test(flavor = "current_thread")]
async fn keyed_state_write_rejects_row_key_mismatch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open db");

    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "game").await.expect("scope");
    db.commit_ddl(DdlOperation::CreateTable {
        project_id: "arcana".into(),
        scope_id: "game".into(),
        table_name: "user_state".into(),
        owner_id: None,
        columns: user_state_columns(),
        primary_key: vec!["user_id".into()],
        if_not_exists: false,
    })
    .await
    .expect("create table");

    let err = db
        .keyed_state_write(
            "arcana",
            "game",
            "user_state",
            Value::Text("u1".into()),
            Row::from_values(vec![Value::Text("u2".into()), Value::Integer(5)]),
        )
        .await
        .expect_err("mismatched keyed row must fail");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[tokio::test(flavor = "current_thread")]
async fn processor_context_commit_rejects_row_key_mismatch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open db");

    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "game").await.expect("scope");
    db.commit_ddl(DdlOperation::CreateTable {
        project_id: "arcana".into(),
        scope_id: "game".into(),
        table_name: "user_state".into(),
        owner_id: None,
        columns: user_state_columns(),
        primary_key: vec!["user_id".into()],
        if_not_exists: false,
    })
    .await
    .expect("create table");

    let mut ctx = db.processor_context("arcana", "game", "points_processor", "hand_settled");
    ctx.write(
        "user_state",
        Value::Text("u1".into()),
        Row::from_values(vec![Value::Text("u2".into()), Value::Integer(5)]),
    );
    let err = ctx
        .commit()
        .await
        .expect_err("processor context must reject mismatched keyed row");
    assert!(matches!(err, AedbError::Validation(_)));
}
