use super::{
    AedbConfig, AedbError, AedbInstance, CallerContext, CommitFinality, ConsistencyMode,
    DdlOperation, DurabilityMode, Mutation, Permission, QueryError,
};
use crate::order_book::{
    ExecInstruction, OrderRequest, OrderSide, OrderStatus, OrderType, SelfTradePrevention,
    TimeInForce, key_client_id,
};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;

#[tokio::test]
async fn order_book_new_with_durable_finality_waits_until_durable_head_catches_up() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        durability_mode: DurabilityMode::Batch,
        batch_interval_ms: 60_000,
        batch_max_bytes: usize::MAX,
        ..AedbConfig::default()
    };
    let db = Arc::new(AedbInstance::open_anonymous(config, dir.path()).expect("open"));
    db.create_project("p").await.expect("project");

    let fsync_db = Arc::clone(&db);
    let fsync_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        fsync_db.force_fsync().await.expect("force fsync");
    });

    let started = Instant::now();
    let result = db
        .order_book_new_with_finality(
            "p",
            "app",
            OrderRequest {
                instrument: "BTC-USD".into(),
                client_order_id: "oid-1".into(),
                side: OrderSide::Bid,
                order_type: OrderType::Limit,
                time_in_force: TimeInForce::Gtc,
                exec_instructions: ExecInstruction(0),
                self_trade_prevention: SelfTradePrevention::None,
                price_ticks: 100,
                qty_be: {
                    let mut out = [0u8; 32];
                    out[31] = 1;
                    out
                },
                owner: "alice".into(),
                account: None,
                nonce: 1,
                price_limit_ticks: None,
            },
            CommitFinality::Durable,
        )
        .await
        .expect("order");
    fsync_task.await.expect("join fsync");

    assert!(
        started.elapsed() >= Duration::from_millis(15),
        "durable finality should wait for WAL durability in batch mode"
    );
    assert!(
        result.durable_head_seq >= result.commit_seq,
        "durable finality must report durable head at or beyond commit sequence"
    );
}

#[tokio::test]
async fn order_book_new_fok_reject_is_dropped_before_wal_append() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    let before = db.operational_metrics().await;

    let err = db
        .order_book_new(
            "p",
            "app",
            OrderRequest {
                instrument: "BTC-USD".into(),
                client_order_id: "fok-no-liq-1".into(),
                side: OrderSide::Bid,
                order_type: OrderType::Limit,
                time_in_force: TimeInForce::Fok,
                exec_instructions: ExecInstruction(0),
                self_trade_prevention: SelfTradePrevention::None,
                price_ticks: 100,
                qty_be: {
                    let mut out = [0u8; 32];
                    out[31] = 1;
                    out
                },
                owner: "alice".into(),
                account: None,
                nonce: 1,
                price_limit_ticks: None,
            },
        )
        .await
        .expect_err("unfillable FOK should reject upstream");
    let after = db.operational_metrics().await;

    match err {
        AedbError::Validation(msg) => assert!(
            msg.contains("fok cannot fill"),
            "unexpected validation message: {msg}"
        ),
        other => panic!("unexpected error variant: {other:?}"),
    }
    assert_eq!(
        after.wal_append_ops, before.wal_append_ops,
        "upstream dropped rejects should not append WAL frames"
    );
    assert_eq!(
        after.wal_append_bytes, before.wal_append_bytes,
        "upstream dropped rejects should not increase WAL append bytes"
    );
}

#[tokio::test]
async fn order_book_write_requires_authenticated_caller_in_secure_mode() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_secure(AedbConfig::production([9u8; 32]), dir.path())
        .expect("open secure");
    let err = db
        .order_book_new(
            "p",
            "app",
            OrderRequest {
                instrument: "BTC-USD".into(),
                client_order_id: "oid-secure".into(),
                side: OrderSide::Bid,
                order_type: OrderType::Limit,
                time_in_force: TimeInForce::Gtc,
                exec_instructions: ExecInstruction(0),
                self_trade_prevention: SelfTradePrevention::None,
                price_ticks: 100,
                qty_be: {
                    let mut out = [0u8; 32];
                    out[31] = 1;
                    out
                },
                owner: "alice".into(),
                account: None,
                nonce: 1,
                price_limit_ticks: None,
            },
        )
        .await
        .expect_err("secure mode should require authenticated caller");
    assert!(matches!(err, AedbError::PermissionDenied(_)));
}

#[tokio::test]
async fn secure_mode_supports_order_book_writes_via_authenticated_as_apis() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_secure(AedbConfig::production([5u8; 32]), dir.path())
        .expect("open secure");
    db.commit_as(
        CallerContext::system_internal(),
        Mutation::Ddl(DdlOperation::CreateProject {
            owner_id: None,
            if_not_exists: true,
            project_id: "p".into(),
        }),
    )
    .await
    .expect("create project");
    db.commit_as(
        CallerContext::system_internal(),
        Mutation::Ddl(DdlOperation::GrantPermission {
            caller_id: "alice".into(),
            permission: Permission::ProjectAdmin {
                project_id: "p".into(),
            },
            actor_id: Some("system".into()),
            delegable: false,
        }),
    )
    .await
    .expect("grant project admin");

    let alice = CallerContext::new("alice");
    db.order_book_new_as(
        alice.clone(),
        "p",
        "app",
        OrderRequest {
            instrument: "BTC-USD".into(),
            client_order_id: "oid-secure-as".into(),
            side: OrderSide::Bid,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
            exec_instructions: ExecInstruction(0),
            self_trade_prevention: SelfTradePrevention::None,
            price_ticks: 100,
            qty_be: {
                let mut out = [0u8; 32];
                out[31] = 2;
                out
            },
            owner: "alice".into(),
            account: None,
            nonce: 1,
            price_limit_ticks: None,
        },
    )
    .await
    .expect("place order");
    db.order_book_cancel_as(alice.clone(), "p", "app", "BTC-USD", 1, "alice")
        .await
        .expect("cancel order");
    let status = db
        .order_status("p", "app", "BTC-USD", 1, ConsistencyMode::AtLatest, &alice)
        .await
        .expect("status query")
        .expect("order exists");
    assert_eq!(status.status, OrderStatus::Cancelled);
}

#[tokio::test]
async fn open_orders_requires_kv_read_permission() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    db.order_book_new(
        "p",
        "app",
        OrderRequest {
            instrument: "BTC-USD".into(),
            client_order_id: "cid-open-orders-1".into(),
            side: OrderSide::Bid,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
            exec_instructions: ExecInstruction(0),
            self_trade_prevention: SelfTradePrevention::None,
            price_ticks: 100,
            qty_be: {
                let mut out = [0u8; 32];
                out[31] = 1;
                out
            },
            owner: "alice".into(),
            account: None,
            nonce: 1,
            price_limit_ticks: None,
        },
    )
    .await
    .expect("place order");

    let alice = CallerContext::new("alice");
    let denied = db
        .open_orders(
            "p",
            "app",
            "BTC-USD",
            "alice",
            ConsistencyMode::AtLatest,
            &alice,
        )
        .await
        .expect_err("missing KvRead should be denied");
    assert!(matches!(denied, QueryError::PermissionDenied { .. }));

    db.commit_ddl(DdlOperation::GrantPermission {
        caller_id: "alice".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: Some(b"ob:BTC-USD:".to_vec()),
        },
        actor_id: None,
        delegable: false,
    })
    .await
    .expect("grant kv read");

    let open = db
        .open_orders(
            "p",
            "app",
            "BTC-USD",
            "alice",
            ConsistencyMode::AtLatest,
            &alice,
        )
        .await
        .expect("open orders");
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].owner, "alice");
}

#[tokio::test]
async fn strict_cancel_rejects_missing_order() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let err = db
        .order_book_cancel_strict(
            "p",
            "app",
            "BTC-USD",
            999_999,
            "alice",
            CommitFinality::Visible,
        )
        .await
        .expect_err("strict cancel should fail when target is missing");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[tokio::test]
async fn strict_cancel_rejects_already_final_order() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    db.order_book_new(
        "p",
        "app",
        OrderRequest {
            instrument: "BTC-USD".into(),
            client_order_id: "strict-final".into(),
            side: OrderSide::Bid,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
            exec_instructions: ExecInstruction(0),
            self_trade_prevention: SelfTradePrevention::None,
            price_ticks: 100,
            qty_be: {
                let mut out = [0u8; 32];
                out[31] = 1;
                out
            },
            owner: "alice".into(),
            account: None,
            nonce: 1,
            price_limit_ticks: None,
        },
    )
    .await
    .expect("place order");

    db.order_book_cancel_strict("p", "app", "BTC-USD", 1, "alice", CommitFinality::Visible)
        .await
        .expect("first strict cancel");

    let err = db
        .order_book_cancel_strict("p", "app", "BTC-USD", 1, "alice", CommitFinality::Visible)
        .await
        .expect_err("second strict cancel should fail on already-cancelled order");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[tokio::test]
async fn strict_cancel_by_client_id_rejects_missing_mapping() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let err = db
        .order_book_cancel_by_client_id_strict(
            "p",
            "app",
            "BTC-USD",
            "missing-client-order-id",
            "alice",
            CommitFinality::Visible,
        )
        .await
        .expect_err("strict cancel by client id should fail when mapping is missing");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[tokio::test]
async fn strict_cancel_by_client_id_rejects_already_final_order() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    db.order_book_new(
        "p",
        "app",
        OrderRequest {
            instrument: "BTC-USD".into(),
            client_order_id: "strict-client-final".into(),
            side: OrderSide::Bid,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
            exec_instructions: ExecInstruction(0),
            self_trade_prevention: SelfTradePrevention::None,
            price_ticks: 100,
            qty_be: {
                let mut out = [0u8; 32];
                out[31] = 1;
                out
            },
            owner: "alice".into(),
            account: None,
            nonce: 1,
            price_limit_ticks: None,
        },
    )
    .await
    .expect("place order");

    db.order_book_cancel_by_client_id_strict(
        "p",
        "app",
        "BTC-USD",
        "strict-client-final",
        "alice",
        CommitFinality::Visible,
    )
    .await
    .expect("first strict cancel by client id");

    let err = db
        .order_book_cancel_by_client_id_strict(
            "p",
            "app",
            "BTC-USD",
            "strict-client-final",
            "alice",
            CommitFinality::Visible,
        )
        .await
        .expect_err("second strict cancel by client id should fail on finalized order");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[tokio::test]
async fn strict_cancel_by_client_id_rejects_invalid_mapping_encoding() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: key_client_id("BTC-USD", "alice", "cid-corrupt"),
        value: vec![1, 2, 3, 4],
    })
    .await
    .expect("inject corrupted mapping");

    let err = db
        .order_book_cancel_by_client_id_strict(
            "p",
            "app",
            "BTC-USD",
            "cid-corrupt",
            "alice",
            CommitFinality::Visible,
        )
        .await
        .expect_err("strict cancel by client id should reject malformed mapping");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[tokio::test]
async fn strict_cancel_by_client_id_detects_owner_mismatch_under_tampered_mapping() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    db.order_book_new(
        "p",
        "app",
        OrderRequest {
            instrument: "BTC-USD".into(),
            client_order_id: "cid-owner-a".into(),
            side: OrderSide::Bid,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
            exec_instructions: ExecInstruction(0),
            self_trade_prevention: SelfTradePrevention::None,
            price_ticks: 100,
            qty_be: {
                let mut out = [0u8; 32];
                out[31] = 1;
                out
            },
            owner: "alice".into(),
            account: None,
            nonce: 1,
            price_limit_ticks: None,
        },
    )
    .await
    .expect("place order");

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: key_client_id("BTC-USD", "bob", "cid-owner-b"),
        value: 1u64.to_be_bytes().to_vec(),
    })
    .await
    .expect("inject tampered mapping");

    let err = db
        .order_book_cancel_by_client_id_strict(
            "p",
            "app",
            "BTC-USD",
            "cid-owner-b",
            "bob",
            CommitFinality::Visible,
        )
        .await
        .expect_err("strict cancel by client id should reject owner mismatch");
    assert!(matches!(err, AedbError::PermissionDenied(_)));
}

#[tokio::test]
async fn strict_reduce_rejects_missing_order() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    let mut one = [0u8; 32];
    one[31] = 1;
    let err = db
        .order_book_reduce_strict(
            "p",
            "app",
            "BTC-USD",
            777,
            "alice",
            one,
            CommitFinality::Visible,
        )
        .await
        .expect_err("strict reduce should fail on missing order");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[tokio::test]
async fn strict_cancel_replace_rejects_already_final_order() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    db.order_book_new(
        "p",
        "app",
        OrderRequest {
            instrument: "BTC-USD".into(),
            client_order_id: "strict-cr-final".into(),
            side: OrderSide::Bid,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
            exec_instructions: ExecInstruction(0),
            self_trade_prevention: SelfTradePrevention::None,
            price_ticks: 100,
            qty_be: {
                let mut out = [0u8; 32];
                out[31] = 1;
                out
            },
            owner: "alice".into(),
            account: None,
            nonce: 1,
            price_limit_ticks: None,
        },
    )
    .await
    .expect("place order");
    db.order_book_cancel_strict("p", "app", "BTC-USD", 1, "alice", CommitFinality::Visible)
        .await
        .expect("cancel");

    let err = db
        .order_book_cancel_replace_strict(
            "p",
            "app",
            "BTC-USD",
            1,
            "alice",
            Some(101),
            None,
            None,
            None,
            CommitFinality::Visible,
        )
        .await
        .expect_err("strict cancel-replace should fail on finalized order");
    assert!(matches!(err, AedbError::Validation(_)));
}
