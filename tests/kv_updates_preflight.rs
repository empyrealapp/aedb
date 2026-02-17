use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::commit::tx::{TransactionEnvelope, WriteClass};
use aedb::commit::validation::Mutation;
use aedb::error::AedbError;
use aedb::permission::{CallerContext, Permission};
use aedb::preflight::PreflightResult;
use aedb::query::plan::ConsistencyMode;
use tempfile::tempdir;

fn u256_be(value: u64) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[24..].copy_from_slice(&value.to_be_bytes());
    bytes
}

fn decode_u64_u256(bytes: &[u8]) -> u64 {
    assert_eq!(bytes.len(), 32, "u256 values must use 32 bytes");
    let mut out = [0u8; 8];
    out.copy_from_slice(&bytes[24..]);
    u64::from_be_bytes(out)
}

#[tokio::test]
async fn integration_kv_updates_and_preflight_balance_checks() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("kv").await.expect("project");

    db.kv_set("kv", "app", b"greeting".to_vec(), b"hello".to_vec())
        .await
        .expect("set greeting");
    db.kv_set("kv", "app", b"greeting".to_vec(), b"hello-v2".to_vec())
        .await
        .expect("update greeting");
    db.kv_del("kv", "app", b"does-not-exist".to_vec())
        .await
        .expect("delete missing key is noop");

    db.kv_set("kv", "app", b"balance".to_vec(), u256_be(200).to_vec())
        .await
        .expect("seed balance");

    let ok_preflight = db
        .preflight(Mutation::KvDecU256 {
            project_id: "kv".into(),
            scope_id: "app".into(),
            key: b"balance".to_vec(),
            amount_be: u256_be(100),
        })
        .await;
    assert_eq!(ok_preflight, PreflightResult::Ok { affected_rows: 1 });

    let bad_preflight = db
        .preflight(Mutation::KvDecU256 {
            project_id: "kv".into(),
            scope_id: "app".into(),
            key: b"balance".to_vec(),
            amount_be: u256_be(300),
        })
        .await;
    assert!(matches!(
        bad_preflight,
        PreflightResult::Err { ref reason } if reason == "underflow"
    ));

    let plan = db
        .preflight_plan(Mutation::KvDecU256 {
            project_id: "kv".into(),
            scope_id: "app".into(),
            key: b"balance".to_vec(),
            amount_be: u256_be(1),
        })
        .await;
    assert!(plan.valid);
    assert_eq!(plan.read_set.points.len(), 1);

    db.commit_with_preflight(Mutation::KvDecU256 {
        project_id: "kv".into(),
        scope_id: "app".into(),
        key: b"balance".to_vec(),
        amount_be: u256_be(100),
    })
    .await
    .expect("preflighted decrement should succeed");

    let underflow = db
        .commit_with_preflight(Mutation::KvDecU256 {
            project_id: "kv".into(),
            scope_id: "app".into(),
            key: b"balance".to_vec(),
            amount_be: u256_be(101),
        })
        .await
        .expect_err("preflight should block underflowing decrement");
    assert!(matches!(underflow, AedbError::Underflow));

    db.commit_with_preflight(Mutation::KvIncU256 {
        project_id: "kv".into(),
        scope_id: "app".into(),
        key: b"balance".to_vec(),
        amount_be: u256_be(25),
    })
    .await
    .expect("increment still works with preflight");

    let reader = CallerContext::new("kv-reader");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "kv-reader".into(),
        permission: Permission::KvRead {
            project_id: "kv".into(),
            scope_id: Some("app".into()),
            prefix: None,
        },
    }))
    .await
    .expect("grant kv read");

    let balance_entry = db
        .kv_get("kv", "app", b"balance", ConsistencyMode::AtLatest, &reader)
        .await
        .expect("kv get")
        .expect("balance key exists");
    assert_eq!(decode_u64_u256(&balance_entry.value), 125);

    let greeting_entry = db
        .kv_get("kv", "app", b"greeting", ConsistencyMode::AtLatest, &reader)
        .await
        .expect("kv get")
        .expect("greeting key exists");
    assert_eq!(greeting_entry.value, b"hello-v2".to_vec());
}

#[tokio::test]
async fn integration_preflight_plan_rejects_stale_kv_assertion() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("kv").await.expect("project");
    db.kv_set("kv", "app", b"balance".to_vec(), u256_be(100).to_vec())
        .await
        .expect("seed balance");

    let plan = db
        .preflight_plan(Mutation::KvDecU256 {
            project_id: "kv".into(),
            scope_id: "app".into(),
            key: b"balance".to_vec(),
            amount_be: u256_be(10),
        })
        .await;
    assert!(plan.valid, "preflight plan should be valid");
    assert_eq!(plan.read_set.points.len(), 1);

    db.kv_set("kv", "app", b"balance".to_vec(), u256_be(200).to_vec())
        .await
        .expect("concurrent update");

    let err = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: plan.read_set,
            write_intent: plan.write_intent,
            base_seq: plan.base_seq,
        })
        .await
        .expect_err("stale plan must conflict");
    assert!(
        matches!(err, AedbError::Conflict(ref msg) if msg.contains("read set conflict")),
        "expected read set conflict, got {err:?}"
    );
}
