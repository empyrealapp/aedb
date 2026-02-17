use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::validation::Mutation;
use aedb::query::plan::{Aggregate, Expr, Order, Query};
use tempfile::tempdir;

#[tokio::test]
async fn integration_query_core_operations_filter_order_limit_aggregate_and_delete() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("qcore").await.expect("project");

    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "qcore".into(),
        scope_id: "app".into(),
        table_name: "orders".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "category".into(),
                col_type: ColumnType::Text,
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

    for (id, category, amount, status) in [
        (1_i64, "books", 30_i64, "open"),
        (2, "books", 90, "open"),
        (3, "books", 60, "closed"),
        (4, "games", 110, "open"),
        (5, "games", 20, "open"),
        (6, "music", 55, "open"),
    ] {
        db.commit(Mutation::Upsert {
            project_id: "qcore".into(),
            scope_id: "app".into(),
            table_name: "orders".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row::from_values(vec![
                Value::Integer(id),
                Value::Text(category.into()),
                Value::Integer(amount),
                Value::Text(status.into()),
            ]),
        })
        .await
        .expect("seed order");
    }

    let filtered = db
        .query(
            "qcore",
            "app",
            Query::select(&["id", "amount"])
                .from("orders")
                .where_(
                    Expr::Eq("status".into(), Value::Text("open".into()))
                        .and(Expr::Gte("amount".into(), Value::Integer(50))),
                )
                .order_by("amount", Order::Desc)
                .limit(2),
        )
        .await
        .expect("filtered query");
    assert_eq!(filtered.rows.len(), 2);
    assert_eq!(filtered.rows[0].values[0], Value::Integer(4));
    assert_eq!(filtered.rows[0].values[1], Value::Integer(110));
    assert_eq!(filtered.rows[1].values[0], Value::Integer(2));
    assert_eq!(filtered.rows[1].values[1], Value::Integer(90));

    let grouped = db
        .query(
            "qcore",
            "app",
            Query::select(&["category", "sum_amount", "count_star"])
                .from("orders")
                .where_(Expr::Eq("status".into(), Value::Text("open".into())))
                .group_by(&["category"])
                .aggregate(Aggregate::Sum("amount".into()))
                .aggregate(Aggregate::Count)
                .having(Expr::Eq("category".into(), Value::Text("books".into())))
                .order_by("category", Order::Asc),
        )
        .await
        .expect("grouped query");
    assert_eq!(grouped.rows.len(), 1);
    assert_eq!(grouped.rows[0].values[0], Value::Text("books".into()));
    assert_eq!(grouped.rows[0].values[1], Value::Integer(120));
    assert_eq!(grouped.rows[0].values[2], Value::Integer(2));

    db.commit(Mutation::Delete {
        project_id: "qcore".into(),
        scope_id: "app".into(),
        table_name: "orders".into(),
        primary_key: vec![Value::Integer(2)],
    })
    .await
    .expect("delete order");

    let deleted = db
        .query(
            "qcore",
            "app",
            Query::select(&["id"])
                .from("orders")
                .where_(Expr::Eq("id".into(), Value::Integer(2)))
                .limit(1),
        )
        .await
        .expect("post delete query");
    assert!(deleted.rows.is_empty(), "deleted row must not be visible");
}
