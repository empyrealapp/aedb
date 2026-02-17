use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::validation::Mutation;
use aedb::config::AedbConfig;
use aedb::offline;
use tempfile::tempdir;

#[tokio::test]
async fn snapshot_dump_restore_parity_and_invariants_are_deterministic() {
    let data_dir = tempdir().expect("data");
    let restore_dir = tempdir().expect("restore");
    let dump_dir = tempdir().expect("dump");
    let dump_path = dump_dir.path().join("snapshot_dump.json");
    let config = AedbConfig::default();

    let db = AedbInstance::open(config.clone(), data_dir.path()).expect("open");
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
        table_name: "users".into(),
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
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Text("alice".into())],
        },
    })
    .await
    .expect("row");
    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"user:1".to_vec(),
        value: b"alice".to_vec(),
    })
    .await
    .expect("kv");
    db.shutdown().await.expect("shutdown");

    let export =
        offline::export_snapshot_dump(data_dir.path(), &config, &dump_path).expect("export");
    assert!(export.current_seq >= 4);
    assert!(!export.parity_checksum_hex.is_empty());

    let restored =
        offline::restore_snapshot_dump(&dump_path, restore_dir.path(), &config).expect("restore");
    assert_eq!(restored.parity_checksum_hex, export.parity_checksum_hex);

    let parity = offline::parity_report_against_data_dir(&dump_path, restore_dir.path(), &config)
        .expect("parity");
    assert!(parity.matches);

    let invariants = offline::invariant_report(restore_dir.path(), &config).expect("invariants");
    assert!(
        invariants.ok,
        "invariants violations: {:?}",
        invariants.violations
    );
}
