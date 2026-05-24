use crate::AedbInstance;
use crate::catalog::DdlOperation;
use crate::catalog::schema::ColumnDef;
use crate::catalog::types::{ColumnType, Row, Value};
use crate::commit::validation::Mutation;
use crate::engine_interface::{REACTIVE_PROCESSOR_CHECKPOINTS_TABLE, SYSTEM_SCOPE_ID};
use crate::error::AedbError;
use crate::query::plan::ConsistencyMode;
use tempfile::tempdir;

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
async fn processor_commit_rejects_stale_checkpoint_row_version() {
    let dir = tempdir().expect("tempdir");
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
    db.commit_ddl(DdlOperation::CreateScope {
        project_id: crate::catalog::SYSTEM_PROJECT_ID.into(),
        scope_id: SYSTEM_SCOPE_ID.into(),
        owner_id: None,
        if_not_exists: true,
    })
    .await
    .expect("create system scope");
    db.commit_ddl(DdlOperation::CreateTable {
        project_id: crate::catalog::SYSTEM_PROJECT_ID.into(),
        scope_id: SYSTEM_SCOPE_ID.into(),
        table_name: REACTIVE_PROCESSOR_CHECKPOINTS_TABLE.into(),
        owner_id: None,
        columns: vec![
            ColumnDef {
                name: "processor_name".to_string(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "checkpoint_seq".to_string(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "updated_at".to_string(),
                col_type: ColumnType::Timestamp,
                nullable: false,
            },
        ],
        primary_key: vec!["processor_name".into()],
        if_not_exists: true,
    })
    .await
    .expect("create checkpoint table");

    let stale = db
        .build_processor_commit_envelope(
            "points_processor",
            0,
            vec![Mutation::Upsert {
                project_id: "arcana".into(),
                scope_id: "game".into(),
                table_name: "user_state".into(),
                primary_key: vec![Value::Text("u1".into())],
                row: Row::from_values(vec![Value::Text("u1".into()), Value::Integer(5)]),
            }],
        )
        .await
        .expect("stale envelope");

    db.processor_commit("points_processor", 1, Vec::new())
        .await
        .expect("advance checkpoint");

    let err = db
        .commit_envelope_prevalidated_internal("processor_commit_test", stale)
        .await
        .expect_err("stale processor commit must fail");
    assert!(matches!(err, AedbError::Conflict(_)));

    let lag = db
        .reactive_processor_lag("points_processor", ConsistencyMode::AtLatest)
        .await
        .expect("lag");
    assert_eq!(lag.checkpoint_seq, 1);

    let row = db
        .keyed_state_read(
            "arcana",
            "game",
            "user_state",
            Value::Text("u1".into()),
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("read row");
    assert!(
        row.is_none(),
        "stale processor commit must not apply its user-state mutation"
    );
}
