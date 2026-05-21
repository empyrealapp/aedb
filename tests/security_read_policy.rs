mod common;

use aedb::catalog::DEFAULT_SCOPE_ID;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::validation::Mutation;
use aedb::config::AedbConfig;
use aedb::permission::CallerContext;
use aedb::query::error::QueryError;
use aedb::query::plan::{Aggregate, ConsistencyMode, Order, Query, QueryOptions};

use common::{
    PROJECT, SCOPE, TestDb, create_async_index, create_owner_index, create_project_scope,
    create_table, grant_index_read, grant_table_read, id_owner_amount_columns, open_db,
    seed_owned_rows, set_owner_read_policy,
};

async fn setup_policy_db(max_scan_rows: usize) -> TestDb {
    let fixture = open_db(AedbConfig {
        max_scan_rows,
        ..AedbConfig::default()
    });
    let db = &fixture.db;
    create_project_scope(db, PROJECT, SCOPE).await;
    create_table(db, PROJECT, SCOPE, "docs", id_owner_amount_columns()).await;
    create_owner_index(db, PROJECT, SCOPE, "docs").await;
    create_async_index(
        db,
        PROJECT,
        SCOPE,
        "docs",
        "docs_projection",
        vec!["id", "owner", "amount"],
    )
    .await;
    seed_owned_rows(
        db,
        PROJECT,
        SCOPE,
        "docs",
        &[
            (1, "alice", 10),
            (2, "bob", 20),
            (3, "alice", 30),
            (4, "bob", 40),
            (5, "alice", 50),
            (6, "carol", 60),
        ],
    )
    .await;
    set_owner_read_policy(db, PROJECT, SCOPE, "docs").await;
    grant_table_read(db, "alice", PROJECT, SCOPE, "docs").await;
    grant_index_read(db, "alice", PROJECT, SCOPE, "docs", "docs_projection").await;
    fixture
}

#[tokio::test]
async fn read_policy_filters_aggregate_order_limit_cursor_and_async_index_paths() {
    let fixture = setup_policy_db(4).await;
    let db = &fixture.db;
    let alice = CallerContext::new("alice");

    let aggregate = db
        .query_with_options_as(
            Some(&alice),
            PROJECT,
            SCOPE,
            Query::select(&[])
                .from("docs")
                .aggregate(Aggregate::Count)
                .aggregate(Aggregate::Sum("amount".into())),
            QueryOptions::default(),
        )
        .await
        .expect("aggregate query");
    assert_eq!(aggregate.rows.len(), 1);
    assert_eq!(
        aggregate.rows[0].values,
        vec![Value::Integer(3), Value::Integer(90)]
    );

    let first_page = db
        .query_with_options_as(
            Some(&alice),
            PROJECT,
            SCOPE,
            Query::select(&["id", "owner", "amount"])
                .from("docs")
                .order_by("amount", Order::Desc)
                .limit(2),
            QueryOptions::default(),
        )
        .await
        .expect("ordered first page");
    assert_eq!(first_page.rows.len(), 2);
    assert_eq!(first_page.rows[0].values[0], Value::Integer(5));
    assert_eq!(first_page.rows[1].values[0], Value::Integer(3));
    assert!(
        first_page
            .rows
            .iter()
            .all(|row| row.values[1] == Value::Text("alice".into()))
    );
    let cursor = first_page.cursor.expect("cursor");

    let second_page = db
        .query_with_options_as(
            Some(&alice),
            PROJECT,
            SCOPE,
            Query::select(&["id", "owner", "amount"])
                .from("docs")
                .order_by("amount", Order::Desc)
                .limit(2),
            QueryOptions {
                cursor: Some(cursor),
                ..QueryOptions::default()
            },
        )
        .await
        .expect("cursor page");
    assert_eq!(second_page.rows.len(), 1);
    assert_eq!(second_page.rows[0].values[0], Value::Integer(1));
    assert_eq!(second_page.rows[0].values[1], Value::Text("alice".into()));

    let async_index = db
        .query_with_options_as(
            Some(&alice),
            PROJECT,
            SCOPE,
            Query::select(&["id", "owner", "amount"])
                .from("docs")
                .order_by("id", Order::Asc)
                .limit(10),
            QueryOptions {
                async_index: Some("docs_projection".into()),
                ..QueryOptions::default()
            },
        )
        .await
        .expect("async index query");
    assert_eq!(async_index.rows.len(), 3);
    assert!(
        async_index
            .rows
            .iter()
            .all(|row| row.values[1] == Value::Text("alice".into()))
    );
}

#[tokio::test]
async fn read_policy_filters_join_paths_and_rejects_ambiguous_aliases() {
    let fixture = setup_policy_db(32).await;
    let db = &fixture.db;
    create_table(db, PROJECT, SCOPE, "labels", id_owner_amount_columns()).await;
    seed_owned_rows(
        db,
        PROJECT,
        SCOPE,
        "labels",
        &[(1, "alice", 100), (2, "bob", 200), (3, "alice", 300)],
    )
    .await;
    set_owner_read_policy(db, PROJECT, SCOPE, "labels").await;
    grant_table_read(db, "alice", PROJECT, SCOPE, "labels").await;
    let alice = CallerContext::new("alice");

    let joined = db
        .query_with_options_as(
            Some(&alice),
            PROJECT,
            SCOPE,
            Query::select(&["d.id", "d.owner", "l.owner", "l.amount"])
                .from("docs")
                .alias("d")
                .inner_join("labels", "d.id", "id")
                .with_last_join_alias("l")
                .order_by("d.id", Order::Asc)
                .limit(10),
            QueryOptions::default(),
        )
        .await
        .expect("join query");
    assert_eq!(joined.rows.len(), 2);
    assert!(joined.rows.iter().all(|row| {
        row.values[1] == Value::Text("alice".into()) && row.values[2] == Value::Text("alice".into())
    }));

    let err = db
        .query_with_options_as(
            Some(&alice),
            PROJECT,
            SCOPE,
            Query::select(&["*"])
                .from("docs")
                .alias("dup")
                .inner_join("labels", "dup.id", "id")
                .with_last_join_alias("dup")
                .limit(10),
            QueryOptions::default(),
        )
        .await
        .expect_err("duplicate aliases must be rejected before planning");
    assert!(
        matches!(err, QueryError::InvalidQuery { reason } if reason.contains("duplicate table alias"))
    );
}

#[tokio::test]
async fn read_policy_filters_global_table_references() {
    let fixture = open_db(AedbConfig::default());
    let db = fixture.db;
    db.create_project("_global")
        .await
        .expect("create global project");
    db.create_scope("_global", DEFAULT_SCOPE_ID)
        .await
        .expect("create global scope");
    create_table(
        &db,
        "_global",
        DEFAULT_SCOPE_ID,
        "announcements",
        vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "owner".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "amount".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
    )
    .await;
    db.commit(Mutation::Upsert {
        project_id: "_global".into(),
        scope_id: DEFAULT_SCOPE_ID.into(),
        table_name: "announcements".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![
            Value::Integer(1),
            Value::Text("alice".into()),
            Value::Integer(10),
        ]),
    })
    .await
    .expect("seed alice global row");
    db.commit(Mutation::Upsert {
        project_id: "_global".into(),
        scope_id: DEFAULT_SCOPE_ID.into(),
        table_name: "announcements".into(),
        primary_key: vec![Value::Integer(2)],
        row: Row::from_values(vec![
            Value::Integer(2),
            Value::Text("bob".into()),
            Value::Integer(20),
        ]),
    })
    .await
    .expect("seed bob global row");
    set_owner_read_policy(&db, "_global", DEFAULT_SCOPE_ID, "announcements").await;
    grant_table_read(&db, "alice", "_global", DEFAULT_SCOPE_ID, "announcements").await;

    let rows = db
        .query_with_options_as(
            Some(&CallerContext::new("alice")),
            PROJECT,
            SCOPE,
            Query::select(&["id", "owner", "amount"])
                .from("_global.announcements")
                .order_by("id", Order::Asc)
                .limit(10),
            QueryOptions {
                consistency: ConsistencyMode::AtLatest,
                ..QueryOptions::default()
            },
        )
        .await
        .expect("global query");
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0].values[0], Value::Integer(1));
    assert_eq!(rows.rows[0].values[1], Value::Text("alice".into()));
}
