use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::{ColumnDef, IndexType};
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::validation::Mutation;
use aedb::query::plan::{Expr, Order, Query};
use tempfile::tempdir;

#[tokio::test]
async fn integration_multi_index_queries_with_sort_directions_use_index_paths() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("int").await.expect("project");

    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "int".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        columns: vec![
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
            ColumnDef {
                name: "rank".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("users table");

    db.commit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: "int".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        index_name: "by_rank".into(),
        if_not_exists: false,
        columns: vec!["rank".into()],
        index_type: IndexType::BTree,
        partial_filter: None,
    }))
    .await
    .expect("index by_rank");
    db.commit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: "int".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        index_name: "by_username".into(),
        if_not_exists: false,
        columns: vec!["username".into()],
        index_type: IndexType::BTree,
        partial_filter: None,
    }))
    .await
    .expect("index by_username");

    for i in 0..200_i64 {
        db.commit(Mutation::Upsert {
            project_id: "int".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(i)],
            row: Row::from_values(vec![
                Value::Integer(i),
                Value::Text(format!("u{i}").into()),
                Value::Integer(i % 20),
            ]),
        })
        .await
        .expect("seed user");
    }

    let asc = db
        .query(
            "int",
            "app",
            Query::select(&["id", "rank"])
                .from("users")
                .where_(Expr::Eq("rank".into(), Value::Integer(7)))
                .order_by("id", Order::Asc),
        )
        .await
        .expect("asc query");
    assert_eq!(asc.rows.len(), 10);
    assert!(
        asc.rows_examined < 200,
        "expected selective index path, examined={}",
        asc.rows_examined
    );
    for pair in asc.rows.windows(2) {
        assert!(
            pair[0].values[0] < pair[1].values[0],
            "ids must be ascending"
        );
    }

    let desc = db
        .query(
            "int",
            "app",
            Query::select(&["id", "rank"])
                .from("users")
                .where_(Expr::Eq("rank".into(), Value::Integer(7)))
                .order_by("id", Order::Desc),
        )
        .await
        .expect("desc query");
    assert_eq!(desc.rows.len(), 10);
    assert!(
        desc.rows_examined < 200,
        "expected selective index path, examined={}",
        desc.rows_examined
    );
    for pair in desc.rows.windows(2) {
        assert!(
            pair[0].values[0] > pair[1].values[0],
            "ids must be descending"
        );
    }
}

#[tokio::test]
async fn integration_join_and_upsert_with_multiple_indexes() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("int").await.expect("project");

    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "int".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        columns: vec![
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
            ColumnDef {
                name: "rank".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("users table");
    db.commit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: "int".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        index_name: "uniq_username".into(),
        if_not_exists: false,
        columns: vec!["username".into()],
        index_type: IndexType::UniqueHash,
        partial_filter: None,
    }))
    .await
    .expect("uniq index");

    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "int".into(),
        scope_id: "app".into(),
        table_name: "orders".into(),
        columns: vec![
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
            ColumnDef {
                name: "amount".into(),
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
    .expect("orders table");
    db.commit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: "int".into(),
        scope_id: "app".into(),
        table_name: "orders".into(),
        index_name: "by_user_id".into(),
        if_not_exists: false,
        columns: vec!["user_id".into()],
        index_type: IndexType::BTree,
        partial_filter: None,
    }))
    .await
    .expect("orders by_user_id");
    db.commit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: "int".into(),
        scope_id: "app".into(),
        table_name: "orders".into(),
        index_name: "by_status".into(),
        if_not_exists: false,
        columns: vec!["status".into()],
        index_type: IndexType::BTree,
        partial_filter: None,
    }))
    .await
    .expect("orders by_status");

    for (id, username, rank) in [(1_i64, "alice", 10_i64), (2, "bob", 20), (3, "carol", 30)] {
        db.commit(Mutation::Upsert {
            project_id: "int".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row::from_values(vec![
                Value::Integer(id),
                Value::Text(username.into()),
                Value::Integer(rank),
            ]),
        })
        .await
        .expect("seed user");
    }
    for (id, user_id, amount, status) in [
        (1_i64, 1_i64, 100_i64, "open"),
        (2, 1, 80, "closed"),
        (3, 2, 50, "open"),
        (4, 3, 75, "open"),
    ] {
        db.commit(Mutation::Upsert {
            project_id: "int".into(),
            scope_id: "app".into(),
            table_name: "orders".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row::from_values(vec![
                Value::Integer(id),
                Value::Integer(user_id),
                Value::Integer(amount),
                Value::Text(status.into()),
            ]),
        })
        .await
        .expect("seed order");
    }

    let joined = db
        .query(
            "int",
            "app",
            Query::select(&["u.username", "o.amount"])
                .from("users")
                .alias("u")
                .inner_join("orders", "u.id", "user_id")
                .with_last_join_alias("o")
                .where_(Expr::Eq("o.status".into(), Value::Text("open".into())))
                .order_by("o.amount", Order::Desc),
        )
        .await
        .expect("join query");
    assert_eq!(joined.rows.len(), 3);
    for pair in joined.rows.windows(2) {
        assert!(
            pair[0].values[1] >= pair[1].values[1],
            "amount should be sorted desc"
        );
    }

    db.commit(Mutation::Upsert {
        project_id: "int".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(2)],
        row: Row::from_values(vec![
            Value::Integer(2),
            Value::Text("bob".into()),
            Value::Integer(99),
        ]),
    })
    .await
    .expect("pk upsert");

    db.commit(Mutation::Upsert {
        project_id: "int".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![
            Value::Integer(1),
            Value::Text("alice".into()),
            Value::Integer(77),
        ]),
    })
    .await
    .expect("pk upsert alice");

    let alice_by_pk = db
        .query(
            "int",
            "app",
            Query::select(&["id", "username", "rank"])
                .from("users")
                .where_(Expr::Eq("id".into(), Value::Integer(1)))
                .limit(1),
        )
        .await
        .expect("alice by pk");
    assert_eq!(alice_by_pk.rows.len(), 1);
    assert_eq!(alice_by_pk.rows[0].values[0], Value::Integer(1));
    assert_eq!(alice_by_pk.rows[0].values[1], Value::Text("alice".into()));
    assert_eq!(alice_by_pk.rows[0].values[2], Value::Integer(77));

    let bob_by_pk = db
        .query(
            "int",
            "app",
            Query::select(&["id", "username", "rank"])
                .from("users")
                .where_(Expr::Eq("id".into(), Value::Integer(2)))
                .limit(1),
        )
        .await
        .expect("bob by pk");
    assert_eq!(bob_by_pk.rows.len(), 1);
    assert_eq!(bob_by_pk.rows[0].values[0], Value::Integer(2));
    assert_eq!(bob_by_pk.rows[0].values[1], Value::Text("bob".into()));
    assert_eq!(bob_by_pk.rows[0].values[2], Value::Integer(99));

    db.commit(Mutation::Upsert {
        project_id: "int".into(),
        scope_id: "app".into(),
        table_name: "orders".into(),
        primary_key: vec![Value::Integer(3)],
        row: Row::from_values(vec![
            Value::Integer(3),
            Value::Integer(2),
            Value::Integer(50),
            Value::Text("closed".into()),
        ]),
    })
    .await
    .expect("close order 3");

    let joined_after = db
        .query(
            "int",
            "app",
            Query::select(&["u.username", "o.amount"])
                .from("users")
                .alias("u")
                .inner_join("orders", "u.id", "user_id")
                .with_last_join_alias("o")
                .where_(Expr::Eq("o.status".into(), Value::Text("open".into())))
                .order_by("o.amount", Order::Desc),
        )
        .await
        .expect("join after update");
    assert_eq!(joined_after.rows.len(), 2);
}
