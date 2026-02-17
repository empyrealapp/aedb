use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::validation::Mutation;
use aedb::config::AedbConfig;
use aedb::query::plan::{ConsistencyMode, Expr, Query};
use tempfile::tempdir;

async fn setup_users(db: &AedbInstance) {
    db.create_project("p").await.expect("create project");
    db.create_scope("p", "app").await.expect("create scope");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        if_not_exists: false,
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
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("create table");
}

#[tokio::test]
async fn delete_where_deletes_matching_rows_with_limit() {
    let dir = tempdir().expect("tempdir");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    setup_users(&db).await;

    for (id, status) in [
        (1, "revoked"),
        (2, "revoked"),
        (3, "revoked"),
        (4, "active"),
    ] {
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row::from_values(vec![Value::Integer(id), Value::Text(status.into())]),
        })
        .await
        .expect("seed");
    }

    let result = db
        .delete_where(
            "p",
            "app",
            "users",
            Expr::Eq("status".into(), Value::Text("revoked".into())),
            Some(2),
        )
        .await
        .expect("delete_where");
    assert!(result.is_some());

    let remaining = db
        .query(
            "p",
            "app",
            Query::select(&["id"])
                .from("users")
                .where_(Expr::Eq("status".into(), Value::Text("revoked".into())))
                .order_by("id", aedb::query::plan::Order::Asc)
                .limit(10),
        )
        .await
        .expect("query");
    assert_eq!(remaining.rows.len(), 1);
    assert_eq!(remaining.rows[0].values[0], Value::Integer(3));
}

#[tokio::test]
async fn update_where_updates_matching_rows() {
    let dir = tempdir().expect("tempdir");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    setup_users(&db).await;

    for (id, status) in [(1, "expired"), (2, "expired"), (3, "active")] {
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row::from_values(vec![Value::Integer(id), Value::Text(status.into())]),
        })
        .await
        .expect("seed");
    }

    let result = db
        .update_where(
            "p",
            "app",
            "users",
            Expr::Eq("status".into(), Value::Text("expired".into())),
            vec![("status".into(), Value::Text("revoked".into()))],
            None,
        )
        .await
        .expect("update_where");
    assert!(result.is_some());

    let revoked = db
        .query(
            "p",
            "app",
            Query::select(&["id"])
                .from("users")
                .where_(Expr::Eq("status".into(), Value::Text("revoked".into())))
                .order_by("id", aedb::query::plan::Order::Asc)
                .limit(10),
        )
        .await
        .expect("query");
    assert_eq!(revoked.rows.len(), 2);
}

#[tokio::test]
async fn commit_many_atomic_applies_mutation_set() {
    let dir = tempdir().expect("tempdir");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    setup_users(&db).await;

    let result = db
        .commit_many_atomic(vec![
            Mutation::Upsert {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                primary_key: vec![Value::Integer(10)],
                row: Row::from_values(vec![Value::Integer(10), Value::Text("active".into())]),
            },
            Mutation::Upsert {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                primary_key: vec![Value::Integer(11)],
                row: Row::from_values(vec![Value::Integer(11), Value::Text("active".into())]),
            },
        ])
        .await
        .expect("commit_many_atomic");
    assert!(result.commit_seq > 0);

    let all = db
        .query(
            "p",
            "app",
            Query::select(&["id"])
                .from("users")
                .order_by("id", aedb::query::plan::Order::Asc)
                .limit(10),
        )
        .await
        .expect("query");
    assert_eq!(all.rows.len(), 2);
}

#[tokio::test]
async fn query_page_stable_uses_pk_order_for_cursor_paging() {
    let dir = tempdir().expect("tempdir");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    setup_users(&db).await;

    for id in [3, 1, 2] {
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row::from_values(vec![Value::Integer(id), Value::Text("active".into())]),
        })
        .await
        .expect("seed");
    }

    let page1 = db
        .query_page_stable(
            "p",
            "app",
            Query::select(&["id"]).from("users"),
            None,
            2,
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("page1");
    assert_eq!(page1.rows.len(), 2);
    assert_eq!(page1.rows[0].values[0], Value::Integer(1));
    assert_eq!(page1.rows[1].values[0], Value::Integer(2));
    assert!(page1.cursor.is_some());
}
