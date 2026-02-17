use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::validation::Mutation;
use aedb::config::AedbConfig;
use aedb::error::AedbError;
use aedb::query::plan::{ConsistencyMode, Query};
use tempfile::tempdir;

fn u256_from_u64(v: u64) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[24..32].copy_from_slice(&v.to_be_bytes());
    out
}

#[tokio::test]
async fn compare_and_swap_enforces_expected_seq() {
    let dir = tempdir().expect("tempdir");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    db.create_project("p").await.expect("create project");
    db.create_scope("p", "app").await.expect("create scope");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        if_not_exists: false,
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
    }))
    .await
    .expect("create table");

    let seed = db
        .commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Text("u1".into())],
            row: Row::from_values(vec![Value::Text("u1".into()), Value::Text("alice".into())]),
        })
        .await
        .expect("seed row");

    db.compare_and_swap(
        "p",
        "app",
        "users",
        vec![Value::Text("u1".into())],
        Row::from_values(vec![Value::Text("u1".into()), Value::Text("alice2".into())]),
        seed.commit_seq,
    )
    .await
    .expect("cas update should succeed");

    let stale = db
        .compare_and_swap(
            "p",
            "app",
            "users",
            vec![Value::Text("u1".into())],
            Row::from_values(vec![Value::Text("u1".into()), Value::Text("alice3".into())]),
            seed.commit_seq,
        )
        .await
        .expect_err("stale expected seq should fail");
    assert!(matches!(stale, AedbError::AssertionFailed { .. }));
}

#[tokio::test]
async fn compare_and_dec_u256_enforces_expected_seq_and_updates_value() {
    let dir = tempdir().expect("tempdir");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    db.create_project("p").await.expect("create project");
    db.create_scope("p", "app").await.expect("create scope");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "balances".into(),
        if_not_exists: false,
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "amount".into(),
                col_type: ColumnType::U256,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("create table");

    let seed = db
        .commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "balances".into(),
            primary_key: vec![Value::Text("acct".into())],
            row: Row::from_values(vec![
                Value::Text("acct".into()),
                Value::U256(u256_from_u64(5)),
            ]),
        })
        .await
        .expect("seed row");

    db.compare_and_dec_u256(
        "p",
        "app",
        "balances",
        vec![Value::Text("acct".into())],
        "amount",
        u256_from_u64(3),
        seed.commit_seq,
    )
    .await
    .expect("guarded decrement should succeed");

    let stale = db
        .compare_and_dec_u256(
            "p",
            "app",
            "balances",
            vec![Value::Text("acct".into())],
            "amount",
            u256_from_u64(1),
            seed.commit_seq,
        )
        .await
        .expect_err("stale expected seq should fail");
    assert!(matches!(stale, AedbError::AssertionFailed { .. }));

    let result = db
        .query(
            "p",
            "app",
            Query::select(&["amount"])
                .from("balances")
                .where_(aedb::query::plan::Expr::Eq(
                    "id".into(),
                    Value::Text("acct".into()),
                )),
        )
        .await
        .expect("query balance");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], Value::U256(u256_from_u64(2)));

    let latest = db
        .snapshot_probe(ConsistencyMode::AtLatest)
        .await
        .expect("latest seq");
    assert!(latest > seed.commit_seq);
}

#[tokio::test]
async fn kv_compare_and_swap_enforces_expected_seq() {
    let dir = tempdir().expect("tempdir");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    db.create_project("p").await.expect("create project");
    db.create_scope("p", "app").await.expect("create scope");

    let seed = db
        .kv_set("p", "app", b"token".to_vec(), b"v1".to_vec())
        .await
        .expect("seed kv");

    db.kv_compare_and_swap(
        "p",
        "app",
        b"token".to_vec(),
        b"v2".to_vec(),
        seed.commit_seq,
    )
    .await
    .expect("kv cas update should succeed");

    let stale = db
        .kv_compare_and_swap(
            "p",
            "app",
            b"token".to_vec(),
            b"v3".to_vec(),
            seed.commit_seq,
        )
        .await
        .expect_err("stale expected seq should fail");
    assert!(matches!(stale, AedbError::AssertionFailed { .. }));
}

#[tokio::test]
async fn kv_compare_and_u256_ops_enforce_expected_seq_and_value() {
    let dir = tempdir().expect("tempdir");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    db.create_project("p").await.expect("create project");
    db.create_scope("p", "app").await.expect("create scope");

    let seed = db
        .kv_set("p", "app", b"nonce".to_vec(), u256_from_u64(5).to_vec())
        .await
        .expect("seed kv u256");

    db.kv_compare_and_inc_u256(
        "p",
        "app",
        b"nonce".to_vec(),
        u256_from_u64(2),
        seed.commit_seq,
    )
    .await
    .expect("guarded kv inc");

    let stale = db
        .kv_compare_and_inc_u256(
            "p",
            "app",
            b"nonce".to_vec(),
            u256_from_u64(1),
            seed.commit_seq,
        )
        .await
        .expect_err("stale expected seq should fail");
    assert!(matches!(stale, AedbError::AssertionFailed { .. }));

    let entry = db
        .kv_get_no_auth("p", "app", b"nonce", ConsistencyMode::AtLatest)
        .await
        .expect("kv get")
        .expect("kv exists");
    assert_eq!(entry.value, u256_from_u64(7).to_vec());

    db.kv_compare_and_dec_u256(
        "p",
        "app",
        b"nonce".to_vec(),
        u256_from_u64(3),
        entry.version,
    )
    .await
    .expect("guarded kv dec");

    let final_entry = db
        .kv_get_no_auth("p", "app", b"nonce", ConsistencyMode::AtLatest)
        .await
        .expect("kv get")
        .expect("kv exists");
    assert_eq!(final_entry.value, u256_from_u64(4).to_vec());
}
