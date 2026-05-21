#![allow(deprecated)]
mod common;

use aedb::AedbInstance;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::catalog::{DdlOperation, KV_INDEX_TABLE};
use aedb::commit::tx::{IdempotencyKey, ReadSet, TransactionEnvelope, WriteClass, WriteIntent};
use aedb::commit::validation::Mutation;
use aedb::config::AedbConfig;
use aedb::query::executor::QueryResult;
use aedb::query::plan::{Aggregate, ConsistencyMode, Order, Query, QueryOptions};

use common::{
    PROJECT, SCOPE, TestDb, create_async_index, create_project_scope, create_table, open_db,
    seed_kv,
};

fn rows_signature(result: &QueryResult) -> Vec<Vec<Value>> {
    let mut rows: Vec<Vec<Value>> = result.rows.iter().map(|row| row.values.clone()).collect();
    rows.sort_by(|left, right| format!("{left:?}").cmp(&format!("{right:?}")));
    rows
}

async fn projected_kv_signature(db: &AedbInstance) -> Vec<Vec<Value>> {
    for _ in 0..25 {
        let result = db
            .query(
                PROJECT,
                SCOPE,
                Query::select(&["project_id", "scope_id", "key", "value"])
                    .from(KV_INDEX_TABLE)
                    .order_by("key", Order::Asc)
                    .limit(10),
            )
            .await
            .expect("query kv projection");
        if result.rows.len() == 3 {
            return rows_signature(&result);
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("kv projection did not materialize expected rows");
}

async fn build_feature_rich_db(config: AedbConfig) -> TestDb {
    let fixture = open_db(config);
    let db = &fixture.db;
    create_project_scope(db, PROJECT, SCOPE).await;
    create_table(
        db,
        PROJECT,
        SCOPE,
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
            ColumnDef {
                name: "points".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
    )
    .await;
    create_async_index(
        db,
        PROJECT,
        SCOPE,
        "users",
        "users_projection",
        vec!["id", "name", "points"],
    )
    .await;
    db.enable_kv_projection(PROJECT, SCOPE)
        .await
        .expect("enable kv projection");
    seed_kv(
        db,
        PROJECT,
        SCOPE,
        &[(b"feature:1", b"alpha"), (b"feature:2", b"beta")],
    )
    .await;
    for (id, name, points) in [(1_i64, "alice", 10_i64), (2, "bob", 20), (3, "carol", 30)] {
        db.commit(Mutation::Upsert {
            project_id: PROJECT.into(),
            scope_id: SCOPE.into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row::from_values(vec![
                Value::Integer(id),
                Value::Text(name.into()),
                Value::Integer(points),
            ]),
        })
        .await
        .expect("seed user");
    }
    db.emit_event(
        PROJECT,
        SCOPE,
        "user_points",
        "evt-1".into(),
        r#"{"user_id":1,"points":10}"#.into(),
    )
    .await
    .expect("emit event");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: Some("system".into()),
        if_not_exists: true,
        project_id: aedb::catalog::SYSTEM_PROJECT_ID.into(),
        scope_id: SCOPE.into(),
        table_name: "reactive_processor_checkpoints".into(),
        columns: vec![
            ColumnDef {
                name: "processor_name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "checkpoint_seq".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "updated_at_micros".into(),
                col_type: ColumnType::Timestamp,
                nullable: false,
            },
        ],
        primary_key: vec!["processor_name".into()],
    }))
    .await
    .expect("create processor checkpoint table");
    db.ack_reactive_processor_checkpoint("points_processor", 1)
        .await
        .expect("ack reactive checkpoint");
    fixture
}

#[tokio::test]
async fn replay_parity_covers_async_indexes_kv_projection_outbox_reactive_and_idempotency() {
    let config = AedbConfig::production([42u8; 32]);
    let fixture = build_feature_rich_db(config.clone()).await;
    let TestDb { db, dir } = fixture;

    let idem_key = IdempotencyKey([9u8; 16]);
    let envelope = TransactionEnvelope {
        caller: None,
        idempotency_key: Some(idem_key.clone()),
        write_class: WriteClass::Economic,
        assertions: Vec::new(),
        read_set: ReadSet::default(),
        write_intent: WriteIntent {
            mutations: vec![Mutation::KvSet {
                project_id: PROJECT.into(),
                scope_id: SCOPE.into(),
                key: b"idempotent:feature".to_vec(),
                value: b"once".to_vec(),
            }],
        },
        base_seq: 0,
    };
    let first_idem = db
        .commit_envelope(envelope.clone())
        .await
        .expect("first idempotent commit");

    let live_users = db
        .query(
            PROJECT,
            SCOPE,
            Query::select(&["id", "name", "points"])
                .from("users")
                .order_by("id", Order::Asc)
                .limit(10),
        )
        .await
        .expect("live users");
    let live_async = db
        .query_with_options(
            PROJECT,
            SCOPE,
            Query::select(&["id", "name", "points"])
                .from("users")
                .order_by("id", Order::Asc)
                .limit(10),
            QueryOptions {
                async_index: Some("users_projection".into()),
                ..QueryOptions::default()
            },
        )
        .await
        .expect("live async projection");
    let live_kv_projection = projected_kv_signature(&db).await;
    let live_event_stream = db
        .read_event_stream(Some("user_points"), 0, 10, ConsistencyMode::AtLatest)
        .await
        .expect("live event stream");
    let live_lifecycle_count = db
        .query(
            "_system",
            SCOPE,
            Query::select(&[])
                .from("lifecycle_outbox")
                .aggregate(Aggregate::Count),
        )
        .await
        .expect("live lifecycle count");
    let live_lag = db
        .reactive_processor_lag("points_processor", ConsistencyMode::AtLatest)
        .await
        .expect("live processor lag");

    let data_dir = dir.path().to_path_buf();
    db.shutdown().await.expect("shutdown");
    drop(db);

    let recovered = AedbInstance::open(config, &data_dir).expect("reopen");
    let recovered_users = recovered
        .query(
            PROJECT,
            SCOPE,
            Query::select(&["id", "name", "points"])
                .from("users")
                .order_by("id", Order::Asc)
                .limit(10),
        )
        .await
        .expect("recovered users");
    let recovered_async = recovered
        .query_with_options(
            PROJECT,
            SCOPE,
            Query::select(&["id", "name", "points"])
                .from("users")
                .order_by("id", Order::Asc)
                .limit(10),
            QueryOptions {
                async_index: Some("users_projection".into()),
                ..QueryOptions::default()
            },
        )
        .await
        .expect("recovered async projection");
    let recovered_kv_projection = projected_kv_signature(&recovered).await;
    let recovered_event_stream = recovered
        .read_event_stream(Some("user_points"), 0, 10, ConsistencyMode::AtLatest)
        .await
        .expect("recovered event stream");
    let recovered_lifecycle_count = recovered
        .query(
            "_system",
            SCOPE,
            Query::select(&[])
                .from("lifecycle_outbox")
                .aggregate(Aggregate::Count),
        )
        .await
        .expect("recovered lifecycle count");
    let recovered_lag = recovered
        .reactive_processor_lag("points_processor", ConsistencyMode::AtLatest)
        .await
        .expect("recovered processor lag");
    let idem_retry = recovered
        .commit_envelope(envelope)
        .await
        .expect("idempotent retry after recovery");

    assert_eq!(
        rows_signature(&recovered_users),
        rows_signature(&live_users)
    );
    assert_eq!(
        rows_signature(&recovered_async),
        rows_signature(&live_async)
    );
    assert_eq!(recovered_kv_projection, live_kv_projection);
    assert_eq!(
        recovered_event_stream
            .events
            .iter()
            .map(|event| (&event.topic, &event.event_key, &event.payload_json))
            .collect::<Vec<_>>(),
        live_event_stream
            .events
            .iter()
            .map(|event| (&event.topic, &event.event_key, &event.payload_json))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        rows_signature(&recovered_lifecycle_count),
        rows_signature(&live_lifecycle_count)
    );
    assert_eq!(recovered_lag.checkpoint_seq, live_lag.checkpoint_seq);
    assert_eq!(idem_retry.commit_seq, first_idem.commit_seq);
}
