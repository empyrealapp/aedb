use crate::AedbInstance;
use crate::catalog::DdlOperation;
use crate::catalog::schema::ColumnDef;
use crate::catalog::types::{ColumnType, Row, Value};
use crate::commit::tx::{ReadAssertion, ReadSet, TransactionEnvelope, WriteClass, WriteIntent};
use crate::commit::validation::Mutation;
use crate::config::AedbConfig;
use crate::error::AedbError;
use crate::sync_bridge::AedbSync;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::runtime::Runtime;

#[test]
fn sync_bridge_supports_envelope_assertions() {
    let rt = Runtime::new().expect("runtime");
    let dir = tempdir().expect("tempdir");
    let db = {
        let _guard = rt.enter();
        Arc::new(AedbInstance::open(AedbConfig::default(), dir.path()).expect("open"))
    };
    let sync = AedbSync::new(Arc::clone(&db), rt.handle().clone());

    sync.commit_ddl(DdlOperation::CreateProject {
        owner_id: None,
        project_id: "p".into(),
        if_not_exists: true,
    })
    .expect("create project");
    sync.commit_ddl(DdlOperation::CreateScope {
        owner_id: None,
        project_id: "p".into(),
        scope_id: "app".into(),
        if_not_exists: true,
    })
    .expect("create scope");
    sync.commit_ddl(DdlOperation::CreateTable {
        owner_id: None,
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
        if_not_exists: false,
    })
    .expect("create table");

    let seed = sync
        .commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Text("u1".into())],
            row: Row {
                values: vec![Value::Text("u1".into()), Value::Text("alice".into())],
            },
        })
        .expect("seed");

    let base_seq = seed.commit_seq;
    let ok = sync
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: vec![ReadAssertion::RowVersion {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                primary_key: vec![Value::Text("u1".into())],
                expected_seq: base_seq,
            }],
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::Upsert {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    table_name: "users".into(),
                    primary_key: vec![Value::Text("u1".into())],
                    row: Row {
                        values: vec![Value::Text("u1".into()), Value::Text("alice2".into())],
                    },
                }],
            },
            base_seq,
        })
        .expect("envelope commit");
    assert!(ok.commit_seq > base_seq);

    let stale = sync.commit_envelope(TransactionEnvelope {
        caller: None,
        idempotency_key: None,
        write_class: WriteClass::Standard,
        assertions: vec![ReadAssertion::RowVersion {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Text("u1".into())],
            expected_seq: base_seq,
        }],
        read_set: ReadSet::default(),
        write_intent: WriteIntent {
            mutations: vec![Mutation::Upsert {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                primary_key: vec![Value::Text("u1".into())],
                row: Row {
                    values: vec![Value::Text("u1".into()), Value::Text("alice3".into())],
                },
            }],
        },
        base_seq,
    });
    assert!(matches!(stale, Err(AedbError::AssertionFailed { .. })));
}
