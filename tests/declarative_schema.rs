use aedb::AedbInstance;
use aedb::catalog::schema::IndexType;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::validation::Mutation;
use aedb::config::AedbConfig;
use aedb::declarative::TableMigrationBuilder;
use aedb::declarative::{AsyncIndexSpec, IndexSpec, MigrationSpec, SchemaMigrationPlan, TableSpec};
use aedb::query::plan::{Expr, Query};
use tempfile::tempdir;

#[test]
fn table_spec_builds_table_and_index_ddl() {
    let table = TableSpec::new("sessions")
        .column("id", ColumnType::Text, false)
        .column("user_id", ColumnType::Text, false)
        .column("expires_at", ColumnType::Timestamp, false)
        .primary_key(&["id"])
        .add_index(IndexSpec::new(
            "idx_sessions_user",
            &["user_id"],
            IndexType::BTree,
        ))
        .add_async_index(AsyncIndexSpec::new(
            "aidx_sessions_list",
            &["id", "user_id", "expires_at"],
        ));

    let ddl = table.to_ddl("p", "auth").expect("to ddl");
    assert_eq!(ddl.len(), 3);
}

#[tokio::test]
async fn declarative_migration_plan_runs_end_to_end() {
    let dir = tempdir().expect("tempdir");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    let sessions = TableSpec::new("sessions")
        .column("id", ColumnType::Text, false)
        .column("user_id", ColumnType::Text, false)
        .column("expires_at", ColumnType::Timestamp, false)
        .column("revoked", ColumnType::Boolean, false)
        .primary_key(&["id"])
        .add_index(IndexSpec::new(
            "idx_sessions_user",
            &["user_id"],
            IndexType::BTree,
        ));

    let migration = MigrationSpec::new(1, "create sessions")
        .up_table("p", "auth", &sessions)
        .expect("up table")
        .down_drop_table("p", "auth", &sessions);

    let plan = SchemaMigrationPlan::new("p", "auth").add(migration);

    let report = plan.run(&db).await.expect("run plan");
    assert_eq!(report.applied.len(), 1);
    assert_eq!(report.current_version, 1);

    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "auth".into(),
        table_name: "sessions".into(),
        primary_key: vec![Value::Text("s1".into())],
        row: Row::from_values(vec![
            Value::Text("s1".into()),
            Value::Text("u1".into()),
            Value::Timestamp(200),
            Value::Boolean(false),
        ]),
    })
    .await
    .expect("insert session");

    let q = Query::select(&["id"])
        .from("sessions")
        .where_(Expr::Eq("user_id".into(), Value::Text("u1".into())));
    let result = db.query("p", "auth", q).await.expect("query");
    assert_eq!(result.rows.len(), 1);
}

#[tokio::test]
async fn table_migration_builder_applies_and_rolls_back_schema_changes() {
    let dir = tempdir().expect("tempdir");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    let sessions = TableSpec::new("sessions")
        .column("id", ColumnType::Text, false)
        .column("user_id", ColumnType::Text, false)
        .column("expires_at", ColumnType::Timestamp, false)
        .column("revoked", ColumnType::Boolean, false)
        .primary_key(&["id"])
        .add_index(IndexSpec::new(
            "idx_sessions_user",
            &["user_id"],
            IndexType::BTree,
        ));

    let create_schema = MigrationSpec::new(1, "create sessions")
        .up_table("p", "auth", &sessions)
        .expect("up table")
        .down_drop_table("p", "auth", &sessions);

    let alter_schema = TableMigrationBuilder::new(2, "alter sessions", "p", "auth", "sessions")
        .add_column(aedb::catalog::schema::ColumnDef {
            name: "device_id".into(),
            col_type: ColumnType::Text,
            nullable: true,
        })
        .rename_column("revoked", "is_revoked")
        .drop_index(
            "idx_sessions_user",
            IndexSpec::new("idx_sessions_user", &["user_id"], IndexType::BTree),
        )
        .add_index(IndexSpec::new(
            "idx_sessions_device",
            &["device_id"],
            IndexType::BTree,
        ))
        .build()
        .expect("build alter migration");

    let plan = SchemaMigrationPlan::new("p", "auth")
        .add(create_schema)
        .add(alter_schema);
    let migrations = plan.migrations().expect("migrations");

    db.run_migrations(migrations.clone())
        .await
        .expect("run migrations");

    let table = db
        .describe_table("p", "auth", "sessions")
        .await
        .expect("describe table");
    assert!(table.columns.iter().any(|c| c.name == "device_id"));
    assert!(table.columns.iter().any(|c| c.name == "is_revoked"));
    assert!(!table.columns.iter().any(|c| c.name == "revoked"));
    assert!(
        db.index_exists("p", "auth", "sessions", "idx_sessions_device")
            .await
            .expect("idx_sessions_device")
    );
    assert!(
        !db.index_exists("p", "auth", "sessions", "idx_sessions_user")
            .await
            .expect("idx_sessions_user")
    );

    db.rollback_to_migration("p", "auth", 1, migrations)
        .await
        .expect("rollback to version 1");

    let rolled_back = db
        .describe_table("p", "auth", "sessions")
        .await
        .expect("describe rolled back table");
    assert!(rolled_back.columns.iter().any(|c| c.name == "revoked"));
    assert!(!rolled_back.columns.iter().any(|c| c.name == "is_revoked"));
    assert!(
        db.index_exists("p", "auth", "sessions", "idx_sessions_user")
            .await
            .expect("idx_sessions_user after rollback")
    );
    assert!(
        !db.index_exists("p", "auth", "sessions", "idx_sessions_device")
            .await
            .expect("idx_sessions_device after rollback")
    );
}
