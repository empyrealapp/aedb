use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::{AccumulatorValueType, ColumnDef};
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::validation::Mutation;
use aedb::engine_interface::{
    EffectBatch, EffectBatchCommitResult, EffectEvent, EffectOperation, EffectPrecondition,
};
use aedb::query::plan::ConsistencyMode;

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
