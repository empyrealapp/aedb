use super::check_invariants;
use crate::catalog::schema::{ColumnDef, IndexDef, IndexType, TableSchema};
use crate::catalog::types::{ColumnType, Row, Value};
use crate::recovery::RecoveredState;
use crate::storage::encoded_key::EncodedKey;
use crate::storage::keyspace::{
    Keyspace, Namespace, NamespaceId, SecondaryIndex, SecondaryIndexStore, TableData,
};
use im::{HashMap as ImHashMap, OrdMap};
use std::collections::HashMap;

#[test]
fn check_invariants_detects_secondary_index_mismatch() {
    let namespace = "p::app".to_string();
    let pk = EncodedKey::from_values(&[Value::Integer(1)]);
    let row = Row::from_values(vec![Value::Integer(1), Value::Text("alice".into())]);

    let mut table = TableData {
        rows: OrdMap::new(),
        row_versions: OrdMap::new(),
        structural_version: 0,
        indexes: ImHashMap::new(),
    };
    table.rows.insert(pk.clone(), row.clone());
    table.row_versions.insert(pk.clone(), 1);
    table.indexes.insert(
        "by_owner".into(),
        SecondaryIndex {
            store: SecondaryIndexStore::BTree(OrdMap::new()),
            columns_bitmask: 0,
            partial_filter: None,
        },
    );

    let mut ns = Namespace {
        id: NamespaceId::Project(namespace.clone()),
        ..Namespace::default()
    };
    ns.tables.insert("users".into(), table);

    let mut keyspace = Keyspace::default();
    keyspace.insert_namespace(NamespaceId::Project(namespace.clone()), ns);

    let mut catalog = crate::catalog::Catalog::default();
    catalog.tables.insert(
        (namespace.clone(), "users".into()),
        TableSchema {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            owner_id: None,
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "owner".into(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
            ],
            primary_key: vec!["id".into()],
            constraints: Vec::new(),
            foreign_keys: Vec::new(),
        },
    );
    catalog.indexes.insert(
        (namespace, "users".into(), "by_owner".into()),
        IndexDef {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            index_name: "by_owner".into(),
            columns: vec!["owner".into()],
            index_type: IndexType::BTree,
            columns_bitmask: 0,
            partial_filter: None,
        },
    );

    let report = check_invariants(&RecoveredState {
        keyspace,
        catalog,
        current_seq: 1,
        idempotency: HashMap::new(),
    });

    assert!(!report.ok);
    assert!(
        report
            .violations
            .iter()
            .any(|violation| violation.contains("secondary index mismatch"))
    );
}
