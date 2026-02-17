use aedb::AedbInstance;
use aedb::catalog::KV_INDEX_TABLE;
use aedb::catalog::types::Value;
use aedb::commit::validation::Mutation;
use aedb::config::AedbConfig;
use aedb::error::AedbError;
use aedb::query::plan::Query;
use tempfile::tempdir;

#[tokio::test]
async fn kv_projection_table_materializes_kv_state() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.enable_kv_projection("p", "app")
        .await
        .expect("enable kv projection");

    db.kv_set("p", "app", b"user:1".to_vec(), b"alice".to_vec())
        .await
        .expect("kv set 1");
    db.kv_set("p", "app", b"user:2".to_vec(), b"bob".to_vec())
        .await
        .expect("kv set 2");

    let mut projected = None;
    for _ in 0..25 {
        let result = db
            .query(
                "p",
                "app",
                Query::select(&["*"]).from(KV_INDEX_TABLE).limit(10),
            )
            .await
            .expect("query projection table");
        if result.rows.len() == 2 {
            projected = Some(result.rows);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    let rows = projected.expect("projection rows");
    let mut saw_user1 = false;
    let mut saw_user2 = false;
    for row in rows {
        let project = row.values.first().expect("project_id");
        let scope = row.values.get(1).expect("scope_id");
        let key = row.values.get(2).expect("key");
        let value = row.values.get(3).expect("value");
        let commit_seq = row.values.get(4).expect("commit_seq");
        assert_eq!(project, &Value::Text("p".into()));
        assert_eq!(scope, &Value::Text("app".into()));
        assert!(matches!(commit_seq, Value::Integer(v) if *v > 0));
        match (key, value) {
            (Value::Blob(k), Value::Blob(v)) if k == b"user:1" && v == b"alice" => {
                saw_user1 = true;
            }
            (Value::Blob(k), Value::Blob(v)) if k == b"user:2" && v == b"bob" => {
                saw_user2 = true;
            }
            other => panic!("unexpected kv projection row: {other:?}"),
        }
    }
    assert!(saw_user1, "missing user:1 projection row");
    assert!(saw_user2, "missing user:2 projection row");
}

#[tokio::test]
async fn kv_projection_table_is_managed_and_read_only() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.enable_kv_projection("p", "app")
        .await
        .expect("enable projection");

    let err = db
        .commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: KV_INDEX_TABLE.into(),
            primary_key: vec![Value::Blob(b"k".to_vec())],
            row: aedb::catalog::types::Row {
                values: vec![
                    Value::Text("p".into()),
                    Value::Text("app".into()),
                    Value::Blob(b"k".to_vec()),
                    Value::Blob(b"v".to_vec()),
                    Value::Integer(1),
                    Value::Timestamp(1),
                ],
            },
        })
        .await
        .expect_err("managed table writes must fail");
    assert!(matches!(err, AedbError::Validation(_)));
}
