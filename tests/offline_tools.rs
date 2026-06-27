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

    let db = AedbInstance::open_anonymous(config.clone(), data_dir.path()).expect("open");
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

#[tokio::test]
async fn verify_database_passes_on_clean_store_and_detects_corruption() {
    let data_dir = tempdir().expect("data");
    let config = AedbConfig::default();

    let db = AedbInstance::open_anonymous(config.clone(), data_dir.path()).expect("open");
    db.commit(Mutation::Ddl(DdlOperation::CreateProject {
        project_id: "p".into(),
        owner_id: None,
        if_not_exists: true,
    }))
    .await
    .expect("project");
    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"k".to_vec(),
        value: b"v".to_vec(),
    })
    .await
    .expect("kv");
    db.shutdown().await.expect("shutdown");
    drop(db);

    // Clean store: every check passes.
    let report = offline::verify_database(data_dir.path(), &config);
    assert!(report.ok, "expected clean store to verify: {report:?}");
    assert!(report.checks.iter().all(|c| c.ok));
    assert!(
        report
            .checks
            .iter()
            .any(|c| c.name == "wal_checkpoint_manifest_integrity"),
        "durability check must be present"
    );
    assert!(report.current_seq >= 2);

    // Corrupt a WAL segment so the durability layer fails closed.
    let seg = std::fs::read_dir(data_dir.path())
        .expect("read dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.is_file()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("segment_") && n.ends_with(".aedbwal"))
        })
        .max()
        .expect("a wal segment file");
    let mut bytes = std::fs::read(&seg).expect("read segment");
    assert!(!bytes.is_empty());
    // Flip bytes in the tail payload region to break a frame CRC / hash chain.
    let flip_from = bytes.len().saturating_sub(16);
    for b in &mut bytes[flip_from..] {
        *b ^= 0xFF;
    }
    std::fs::write(&seg, &bytes).expect("write corrupted segment");

    let corrupt = offline::verify_database(data_dir.path(), &config);
    assert!(!corrupt.ok, "corrupted store must fail verification");
    assert!(
        corrupt.checks.iter().any(|c| !c.ok),
        "at least one check must fail: {corrupt:?}"
    );
    assert!(!corrupt.violations.is_empty());
}

#[tokio::test]
async fn verify_database_handles_empty_table_without_false_violation() {
    let data_dir = tempdir().expect("data");
    let config = AedbConfig::default();
    let db = AedbInstance::open_anonymous(config.clone(), data_dir.path()).expect("open");
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
        table_name: "empty".into(),
        owner_id: None,
        if_not_exists: false,
        columns: vec![ColumnDef { name: "id".into(), col_type: ColumnType::Integer, nullable: false }],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");
    db.shutdown().await.expect("shutdown");
    drop(db);
    let report = offline::verify_database(data_dir.path(), &config);
    assert!(
        report.ok,
        "empty table must not produce false integrity violations: {:?}",
        report.violations
    );
}
