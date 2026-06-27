//! Memory-model observability: the keyspace resident-skeleton estimate and its
//! budget are surfaced in operational metrics and track row-count growth.

use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::validation::Mutation;
use aedb::config::AedbConfig;
use tempfile::tempdir;

async fn seed_table(db: &AedbInstance) {
    db.commit(Mutation::Ddl(DdlOperation::CreateProject {
        project_id: "p".into(),
        owner_id: None,
        if_not_exists: true,
    }))
    .await
    .expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "items".into(),
        owner_id: None,
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
    }))
    .await
    .expect("table");
}

async fn insert_rows(db: &AedbInstance, range: std::ops::Range<i64>) {
    for i in range {
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "items".into(),
            primary_key: vec![Value::Integer(i)],
            row: Row {
                values: vec![Value::Integer(i), Value::Text(format!("item-{i}").into())],
            },
        })
        .await
        .expect("insert");
    }
}

#[tokio::test]
async fn keyspace_memory_metrics_are_populated_and_grow_with_rows() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig::default();
    let db = AedbInstance::open_anonymous(config.clone(), dir.path()).expect("open");
    seed_table(&db).await;

    insert_rows(&db, 0..100).await;
    let m1 = db.operational_metrics().await;
    assert!(
        m1.keyspace_resident_bytes > 0,
        "resident skeleton estimate must be populated"
    );
    assert_eq!(
        m1.keyspace_memory_budget_bytes,
        config.max_memory_estimate_bytes as u64
    );
    assert!(m1.keyspace_memory_used_fraction > 0.0);
    assert!(
        m1.keyspace_memory_used_fraction < 1.0,
        "100 small rows should be far under the default 2GB budget"
    );

    insert_rows(&db, 100..1_000).await;
    let m2 = db.operational_metrics().await;
    assert!(
        m2.keyspace_resident_bytes > m1.keyspace_resident_bytes,
        "resident bytes must grow with row count ({} -> {})",
        m1.keyspace_resident_bytes,
        m2.keyspace_resident_bytes
    );
}
