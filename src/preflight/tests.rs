use super::{PreflightResult, preflight};
use crate::catalog::DdlOperation;
use crate::catalog::schema::ColumnDef;
use crate::catalog::types::{ColumnType, Row, Value};
use crate::commit::validation::Mutation;
use crate::storage::keyspace::Keyspace;
fn u256_be(value: u64) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[24..].copy_from_slice(&value.to_be_bytes());
    bytes
}

#[test]
fn preflight_valid_and_invalid_paths() {
    let mut catalog = crate::catalog::Catalog::default();
    let snapshot = Keyspace::default().snapshot();

    let missing = preflight(
        &snapshot,
        &catalog,
        &Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(1)],
            row: Row {
                values: vec![Value::Integer(1)],
            },
        },
    );
    assert!(matches!(missing, PreflightResult::Err { .. }));

    catalog.create_project("p").expect("project");
    catalog
        .create_table(
            "p",
            "app",
            "users",
            vec![
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
            vec!["id".into()],
        )
        .expect("table");

    let ok = preflight(
        &snapshot,
        &catalog,
        &Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(1)],
            row: Row {
                values: vec![Value::Integer(1), Value::Text("ok".into())],
            },
        },
    );
    assert_eq!(ok, PreflightResult::Ok { affected_rows: 1 });

    let mismatch = preflight(
        &snapshot,
        &catalog,
        &Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(1)],
            row: Row {
                values: vec![Value::Text("bad".into()), Value::Text("ok".into())],
            },
        },
    );
    assert!(matches!(mismatch, PreflightResult::Err { .. }));

    let ddl_existing = preflight(
        &snapshot,
        &catalog,
        &Mutation::Ddl(DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: false,
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            columns: vec![],
            primary_key: vec![],
        }),
    );
    assert!(matches!(ddl_existing, PreflightResult::Err { .. }));
}

#[test]
fn preflight_is_advisory_not_authoritative() {
    let mut catalog = crate::catalog::Catalog::default();
    let snapshot = Keyspace::default().snapshot();
    catalog.create_project("p").expect("project");
    catalog
        .create_table(
            "p",
            "app",
            "users",
            vec![
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
            vec!["id".into()],
        )
        .expect("table");
    let mutation = Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Text("ok".into())],
        },
    };
    let pre = preflight(&snapshot, &catalog, &mutation);
    assert!(matches!(pre, PreflightResult::Ok { .. }));

    catalog
        .alter_table(
            "p",
            "app",
            "users",
            crate::catalog::schema::TableAlteration::DropColumn {
                name: "name".into(),
            },
        )
        .expect("alter");
    let after = preflight(&snapshot, &catalog, &mutation);
    assert!(matches!(after, PreflightResult::Err { .. }));
}

#[test]
fn preflight_checks_kv_dec_balance_and_existing_u256_encoding() {
    let mut catalog = crate::catalog::Catalog::default();
    catalog.create_project("p").expect("project");

    let mut keyspace = Keyspace::default();
    keyspace
        .kv_set("p", "app", b"balance".to_vec(), u256_be(125).to_vec(), 1)
        .expect("set balance");
    let snapshot = keyspace.snapshot();

    let ok = preflight(
        &snapshot,
        &catalog,
        &Mutation::KvDecU256 {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"balance".to_vec(),
            amount_be: u256_be(100),
        },
    );
    assert_eq!(ok, PreflightResult::Ok { affected_rows: 1 });

    let underflow = preflight(
        &snapshot,
        &catalog,
        &Mutation::KvDecU256 {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"balance".to_vec(),
            amount_be: u256_be(130),
        },
    );
    assert!(matches!(
        underflow,
        PreflightResult::Err { ref reason } if reason == "underflow"
    ));

    let mut bad_keyspace = Keyspace::default();
    bad_keyspace
        .kv_set("p", "app", b"balance".to_vec(), vec![1, 2, 3], 1)
        .expect("set malformed balance");
    let bad_snapshot = bad_keyspace.snapshot();
    let bad = preflight(
        &bad_snapshot,
        &catalog,
        &Mutation::KvIncU256 {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"balance".to_vec(),
            amount_be: u256_be(1),
        },
    );
    assert!(matches!(
        bad,
        PreflightResult::Err { ref reason } if reason.contains("invalid u256 bytes length")
    ));
}

#[test]
fn preflight_insert_reports_duplicate_primary_key() {
    let mut catalog = crate::catalog::Catalog::default();
    catalog.create_project("p").expect("project");
    catalog
        .create_table(
            "p",
            "app",
            "users",
            vec![
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
            vec!["id".into()],
        )
        .expect("table");

    let mut keyspace = Keyspace::default();
    keyspace.upsert_row(
        "p",
        "app",
        "users",
        vec![Value::Integer(1)],
        Row {
            values: vec![Value::Integer(1), Value::Text("alice".into())],
        },
        1,
    );
    let snapshot = keyspace.snapshot();

    let duplicate = preflight(
        &snapshot,
        &catalog,
        &Mutation::Insert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(1)],
            row: Row {
                values: vec![Value::Integer(1), Value::Text("second".into())],
            },
        },
    );
    assert!(matches!(duplicate, PreflightResult::Err { .. }));
}

#[test]
fn preflight_insert_batch_reports_duplicate_primary_key() {
    let mut catalog = crate::catalog::Catalog::default();
    catalog.create_project("p").expect("project");
    catalog
        .create_table(
            "p",
            "app",
            "users",
            vec![
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
            vec!["id".into()],
        )
        .expect("table");

    let snapshot = Keyspace::default().snapshot();
    let duplicate = preflight(
        &snapshot,
        &catalog,
        &Mutation::InsertBatch {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            rows: vec![
                Row {
                    values: vec![Value::Integer(1), Value::Text("alice".into())],
                },
                Row {
                    values: vec![Value::Integer(1), Value::Text("alice-dup".into())],
                },
            ],
        },
    );
    assert!(matches!(duplicate, PreflightResult::Err { .. }));
}
