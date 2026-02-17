use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::validation::{Mutation, TableUpdateExpr};
use aedb::config::AedbConfig;
use aedb::query::plan::{Expr, Query};
use tempfile::tempdir;

async fn setup_jobs(db: &AedbInstance) {
    db.create_project("p").await.expect("create project");
    db.create_scope("p", "app").await.expect("create scope");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "jobs".into(),
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
            ColumnDef {
                name: "attempts".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "lease_owner".into(),
                col_type: ColumnType::Text,
                nullable: true,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("create jobs table");
}

#[tokio::test]
async fn update_where_expr_supports_add_and_coalesce() {
    let dir = tempdir().expect("tempdir");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    setup_jobs(&db).await;

    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "jobs".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![
            Value::Integer(1),
            Value::Text("queued".into()),
            Value::Integer(0),
            Value::Null,
        ]),
    })
    .await
    .expect("seed");

    db.update_where_expr(
        "p",
        "app",
        "jobs",
        Expr::Eq("id".into(), Value::Integer(1)),
        vec![
            ("attempts".into(), TableUpdateExpr::AddI64(1)),
            (
                "lease_owner".into(),
                TableUpdateExpr::Coalesce(Value::Text("worker-a".into())),
            ),
        ],
        Some(1),
    )
    .await
    .expect("update_where_expr");

    db.update_where_expr(
        "p",
        "app",
        "jobs",
        Expr::Eq("id".into(), Value::Integer(1)),
        vec![(
            "lease_owner".into(),
            TableUpdateExpr::Coalesce(Value::Text("worker-b".into())),
        )],
        Some(1),
    )
    .await
    .expect("second coalesce");

    let row = db
        .query(
            "p",
            "app",
            Query::select(&["attempts", "lease_owner"])
                .from("jobs")
                .where_(Expr::Eq("id".into(), Value::Integer(1))),
        )
        .await
        .expect("query");
    assert_eq!(row.rows.len(), 1);
    assert_eq!(row.rows[0].values[0], Value::Integer(1));
    assert_eq!(row.rows[0].values[1], Value::Text("worker-a".into()));
}

#[tokio::test]
async fn mutate_where_returning_returns_before_and_after() {
    let dir = tempdir().expect("tempdir");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    setup_jobs(&db).await;

    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "jobs".into(),
        primary_key: vec![Value::Integer(7)],
        row: Row::from_values(vec![
            Value::Integer(7),
            Value::Text("queued".into()),
            Value::Integer(0),
            Value::Null,
        ]),
    })
    .await
    .expect("seed");

    let claimed = db
        .mutate_where_returning(
            "p",
            "app",
            "jobs",
            Expr::Eq("status".into(), Value::Text("queued".into())),
            vec![
                (
                    "status".into(),
                    TableUpdateExpr::Value(Value::Text("running".into())),
                ),
                ("attempts".into(), TableUpdateExpr::AddI64(1)),
                (
                    "lease_owner".into(),
                    TableUpdateExpr::Value(Value::Text("worker-1".into())),
                ),
            ],
        )
        .await
        .expect("mutate_where_returning")
        .expect("should claim");

    assert_eq!(claimed.primary_key, vec![Value::Integer(7)]);
    assert_eq!(claimed.before.values[1], Value::Text("queued".into()));
    assert_eq!(claimed.after.values[1], Value::Text("running".into()));
    assert_eq!(claimed.after.values[2], Value::Integer(1));
}

#[tokio::test]
async fn claim_one_is_atomic_under_contention() {
    let dir = tempdir().expect("tempdir");
    let db =
        std::sync::Arc::new(AedbInstance::open(AedbConfig::default(), dir.path()).expect("open"));
    setup_jobs(db.as_ref()).await;

    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "jobs".into(),
        primary_key: vec![Value::Integer(99)],
        row: Row::from_values(vec![
            Value::Integer(99),
            Value::Text("queued".into()),
            Value::Integer(0),
            Value::Null,
        ]),
    })
    .await
    .expect("seed");

    let left = {
        let db = db.clone();
        tokio::spawn(async move {
            db.claim_one(
                "p",
                "app",
                "jobs",
                Expr::Eq("status".into(), Value::Text("queued".into())),
                vec![(
                    "status".into(),
                    TableUpdateExpr::Value(Value::Text("running".into())),
                )],
            )
            .await
            .expect("claim left")
            .is_some()
        })
    };
    let right = {
        let db = db.clone();
        tokio::spawn(async move {
            db.claim_one(
                "p",
                "app",
                "jobs",
                Expr::Eq("status".into(), Value::Text("queued".into())),
                vec![(
                    "status".into(),
                    TableUpdateExpr::Value(Value::Text("running".into())),
                )],
            )
            .await
            .expect("claim right")
            .is_some()
        })
    };

    let left_claimed = left.await.expect("left join");
    let right_claimed = right.await.expect("right join");
    assert_ne!(
        left_claimed, right_claimed,
        "exactly one claimant should win"
    );
}
