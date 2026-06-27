use super::{AedbConfig, AedbError, AedbInstance, ColumnDef, ColumnType, DdlOperation, Mutation};
use crate::migration::Migration;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn migrations_are_idempotent_and_checksum_guarded() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    let migration = Migration {
        version: 1,
        name: "create-users".into(),
        project_id: "p".into(),
        scope_id: "app".into(),
        mutations: vec![Mutation::Ddl(DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: false,
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            columns: vec![
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
            ],
            primary_key: vec!["id".into()],
        })],
        down_mutations: Some(vec![Mutation::Ddl(DdlOperation::DropTable {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            if_exists: true,
        })]),
    };
    db.apply_migration(migration.clone())
        .await
        .expect("apply 1");
    db.apply_migration(migration).await.expect("apply 2");

    let changed = Migration {
        version: 1,
        name: "create-users-v2".into(),
        project_id: "p".into(),
        scope_id: "app".into(),
        mutations: vec![],
        down_mutations: None,
    };
    let err = db
        .apply_migration(changed)
        .await
        .expect_err("checksum guard");
    assert!(matches!(err, AedbError::IntegrityError { .. }));
}

#[tokio::test]
async fn run_migrations_reports_applied_and_skipped_versions() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let migration = Migration {
        version: 1,
        name: "create-users".into(),
        project_id: "p".into(),
        scope_id: "app".into(),
        mutations: vec![Mutation::Ddl(DdlOperation::CreateTable {
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
                    name: "name".into(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
            ],
            primary_key: vec!["id".into()],
        })],
        down_mutations: None,
    };

    let first = db
        .run_migrations(vec![migration.clone()])
        .await
        .expect("run first");
    assert_eq!(first.applied.len(), 1);
    assert!(first.skipped.is_empty());
    assert_eq!(first.current_version, 1);

    let second = db
        .run_migrations(vec![migration])
        .await
        .expect("run second");
    assert!(second.applied.is_empty());
    assert_eq!(second.skipped, vec![1]);
    assert_eq!(second.current_version, 1);
    assert_eq!(db.current_version("p", "app").await.expect("current"), 1);
}

#[tokio::test]
async fn concurrent_apply_migration_converges_idempotently() {
    let dir = tempdir().expect("temp");
    let db =
        Arc::new(AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open"));
    db.create_project("p").await.expect("project");

    let migration = Migration {
        version: 1,
        name: "create-users".into(),
        project_id: "p".into(),
        scope_id: "app".into(),
        mutations: vec![Mutation::Ddl(DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: false,
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            columns: vec![
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
            ],
            primary_key: vec!["id".into()],
        })],
        down_mutations: None,
    };

    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let db_a = Arc::clone(&db);
    let db_b = Arc::clone(&db);
    let migration_a = migration.clone();
    let migration_b = migration.clone();
    let barrier_a = Arc::clone(&barrier);
    let barrier_b = Arc::clone(&barrier);

    let task_a = tokio::spawn(async move {
        barrier_a.wait().await;
        db_a.apply_migration(migration_a).await
    });
    let task_b = tokio::spawn(async move {
        barrier_b.wait().await;
        db_b.apply_migration(migration_b).await
    });

    task_a
        .await
        .expect("task a join")
        .expect("task a apply migration");
    task_b
        .await
        .expect("task b join")
        .expect("task b apply migration");

    let applied = db
        .list_applied_migrations("p", "app")
        .await
        .expect("list applied");
    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].version, 1);
    assert!(
        db.table_exists("p", "app", "users")
            .await
            .expect("users table exists")
    );
}
