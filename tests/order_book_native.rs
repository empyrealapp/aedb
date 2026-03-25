use aedb::AedbInstance;
use aedb::error::AedbError;
use aedb::order_book::{
    ExecInstruction, InstrumentConfig, OrderBookTableMode, OrderRecord, OrderRequest, OrderSide,
    OrderStatus, OrderType, SelfTradePrevention, TimeInForce, key_client_id, key_execution_report,
    key_order, scoped_instrument,
};
use aedb::query::plan::ConsistencyMode;
use tempfile::tempdir;

fn u256_be(v: u64) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[24..].copy_from_slice(&v.to_be_bytes());
    out
}

fn decode_u64_u256(be: [u8; 32]) -> u64 {
    let mut out = [0u8; 8];
    out.copy_from_slice(&be[24..]);
    u64::from_be_bytes(out)
}

#[allow(clippy::too_many_arguments)]
fn order_req(
    instrument: &str,
    owner: &str,
    client_id: &str,
    side: OrderSide,
    tif: TimeInForce,
    post_only: bool,
    price: i64,
    qty: u64,
) -> OrderRequest {
    OrderRequest {
        instrument: instrument.to_string(),
        client_order_id: client_id.to_string(),
        side,
        order_type: OrderType::Limit,
        time_in_force: tif,
        exec_instructions: ExecInstruction(if post_only {
            ExecInstruction::POST_ONLY
        } else {
            0
        }),
        self_trade_prevention: aedb::order_book::SelfTradePrevention::None,
        price_ticks: price,
        qty_be: u256_be(qty),
        owner: owner.to_string(),
        account: None,
        nonce: 1,
        price_limit_ticks: None,
    }
}

fn order_req_with_stp(
    instrument: &str,
    owner: &str,
    client_id: &str,
    side: OrderSide,
    price: i64,
    qty: u64,
    stp: SelfTradePrevention,
) -> OrderRequest {
    let mut req = order_req(
        instrument,
        owner,
        client_id,
        side,
        TimeInForce::Gtc,
        false,
        price,
        qty,
    );
    req.self_trade_prevention = stp;
    req
}

async fn load_order(db: &AedbInstance, instrument: &str, order_id: u64) -> OrderRecord {
    let key = key_order(instrument, order_id);
    let entry = db
        .kv_get_no_auth("p", "app", &key, ConsistencyMode::AtLatest)
        .await
        .expect("kv_get")
        .expect("order entry");
    rmp_serde::from_slice(&entry.value).expect("decode order")
}

async fn load_order_id(
    db: &AedbInstance,
    instrument: &str,
    owner: &str,
    client_order_id: &str,
) -> u64 {
    let key = key_client_id(instrument, owner, client_order_id);
    let entry = db
        .kv_get_no_auth("p", "app", &key, ConsistencyMode::AtLatest)
        .await
        .expect("kv_get")
        .expect("client id mapping");
    let mut out = [0u8; 8];
    out.copy_from_slice(&entry.value);
    u64::from_be_bytes(out)
}

#[tokio::test]
async fn post_only_rejects_crossing() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    db.order_book_new(
        "p",
        "app",
        order_req(
            "BTC-USD",
            "maker",
            "maker-1",
            OrderSide::Ask,
            TimeInForce::Gtc,
            false,
            100,
            5,
        ),
    )
    .await
    .expect("seed ask");

    let err = db
        .order_book_new(
            "p",
            "app",
            order_req(
                "BTC-USD",
                "taker",
                "taker-post",
                OrderSide::Bid,
                TimeInForce::Gtc,
                true,
                100,
                4,
            ),
        )
        .await
        .expect_err("post only should reject");
    assert!(
        matches!(err, AedbError::Validation(ref msg) if msg.contains("post_only")),
        "unexpected error: {err:?}"
    );

    let trades = db
        .kv_scan_prefix_no_auth(
            "p",
            "app",
            b"ob:BTC-USD:trade:",
            100,
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("trade scan");
    assert!(trades.is_empty());
}

#[tokio::test]
async fn ioc_partial_fill_and_fok_rejection() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    db.order_book_new(
        "p",
        "app",
        order_req(
            "BTC-USD",
            "maker",
            "maker-a",
            OrderSide::Ask,
            TimeInForce::Gtc,
            false,
            100,
            5,
        ),
    )
    .await
    .expect("seed ask");

    db.order_book_new(
        "p",
        "app",
        order_req(
            "BTC-USD",
            "ioc-user",
            "ioc-1",
            OrderSide::Bid,
            TimeInForce::Ioc,
            false,
            100,
            8,
        ),
    )
    .await
    .expect("ioc commit");

    let ioc_order_id = load_order_id(&db, "BTC-USD", "ioc-user", "ioc-1").await;
    let ioc = load_order(&db, "BTC-USD", ioc_order_id).await;
    assert_eq!(decode_u64_u256(ioc.remaining_qty_be), 0);
    assert_eq!(decode_u64_u256(ioc.filled_qty_be), 5);
    assert_eq!(ioc.status, OrderStatus::PartiallyFilled);

    db.order_book_new(
        "p",
        "app",
        order_req(
            "BTC-USD",
            "maker2",
            "maker-b",
            OrderSide::Ask,
            TimeInForce::Gtc,
            false,
            101,
            3,
        ),
    )
    .await
    .expect("seed ask 2");

    let err = db
        .order_book_new(
            "p",
            "app",
            order_req(
                "BTC-USD",
                "fok-user",
                "fok-1",
                OrderSide::Bid,
                TimeInForce::Fok,
                false,
                101,
                5,
            ),
        )
        .await
        .expect_err("fok should reject");
    assert!(
        matches!(err, AedbError::Validation(ref msg) if msg.contains("fok")),
        "unexpected error: {err:?}"
    );
}

#[tokio::test]
async fn cancel_replace_reduce_and_mass_cancel() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    db.order_book_new(
        "p",
        "app",
        order_req(
            "BTC-USD",
            "maker",
            "maker-cr",
            OrderSide::Bid,
            TimeInForce::Gtc,
            false,
            90,
            10,
        ),
    )
    .await
    .expect("new");

    let order_id = load_order_id(&db, "BTC-USD", "maker", "maker-cr").await;

    db.order_book_cancel_replace(
        "p",
        "app",
        "BTC-USD",
        order_id,
        "maker",
        Some(91),
        Some(u256_be(12)),
        None,
        None,
    )
    .await
    .expect("cancel replace");

    let updated = load_order(&db, "BTC-USD", order_id).await;
    assert_eq!(updated.price_ticks, 91);
    assert_eq!(decode_u64_u256(updated.remaining_qty_be), 12);

    db.order_book_reduce("p", "app", "BTC-USD", order_id, "maker", u256_be(3))
        .await
        .expect("reduce");
    let reduced = load_order(&db, "BTC-USD", order_id).await;
    assert_eq!(decode_u64_u256(reduced.remaining_qty_be), 9);

    db.order_book_mass_cancel(
        "p",
        "app",
        "BTC-USD",
        "maker",
        None,
        Some("maker".to_string()),
        None,
    )
    .await
    .expect("mass cancel");
    let cancelled = load_order(&db, "BTC-USD", order_id).await;
    assert_eq!(cancelled.status, OrderStatus::Cancelled);
    assert_eq!(decode_u64_u256(cancelled.remaining_qty_be), 0);
}

#[tokio::test]
async fn table_scoped_books_support_multi_asset_and_cancel_by_client_id() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.order_book_define_table("p", "app", "markets", OrderBookTableMode::MultiAsset)
        .await
        .expect("define table");

    db.order_book_new_in_table(
        "p",
        "app",
        "markets",
        "BTC-USD",
        order_req(
            "placeholder",
            "alice",
            "btc-1",
            OrderSide::Bid,
            TimeInForce::Gtc,
            false,
            100,
            2,
        ),
    )
    .await
    .expect("new btc");
    db.order_book_new_in_table(
        "p",
        "app",
        "markets",
        "ETH-USD",
        order_req(
            "placeholder",
            "alice",
            "eth-1",
            OrderSide::Bid,
            TimeInForce::Gtc,
            false,
            50,
            3,
        ),
    )
    .await
    .expect("new eth");

    let btc_scoped = scoped_instrument("markets", "BTC-USD");
    let btc_id = load_order_id(&db, &btc_scoped, "alice", "btc-1").await;
    db.order_book_cancel_by_client_id("p", "app", &btc_scoped, "btc-1", "alice")
        .await
        .expect("cancel by client id");
    let btc_order = load_order(&db, &btc_scoped, btc_id).await;
    assert_eq!(btc_order.status, OrderStatus::Cancelled);

    db.order_book_cancel_by_client_id("p", "app", &btc_scoped, "btc-1", "alice")
        .await
        .expect("idempotent cancel by client id");

    let eth_scoped = scoped_instrument("markets", "ETH-USD");
    let eth_id = load_order_id(&db, &eth_scoped, "alice", "eth-1").await;
    let eth_order = load_order(&db, &eth_scoped, eth_id).await;
    assert_eq!(eth_order.status, OrderStatus::Open);
    assert_eq!(decode_u64_u256(eth_order.remaining_qty_be), 3);
}

#[tokio::test]
async fn per_asset_table_mode_is_enforced() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.order_book_define_table("p", "app", "BTC-USD", OrderBookTableMode::PerAsset)
        .await
        .expect("define table");

    let ok = db
        .order_book_new_in_table(
            "p",
            "app",
            "BTC-USD",
            "BTC-USD",
            order_req(
                "placeholder",
                "alice",
                "btc-pa",
                OrderSide::Bid,
                TimeInForce::Gtc,
                false,
                100,
                1,
            ),
        )
        .await;
    assert!(ok.is_ok());

    let err = db
        .order_book_new_in_table(
            "p",
            "app",
            "BTC-USD",
            "ETH-USD",
            order_req(
                "placeholder",
                "alice",
                "eth-pa",
                OrderSide::Bid,
                TimeInForce::Gtc,
                false,
                50,
                1,
            ),
        )
        .await
        .expect_err("per asset should reject different asset id");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[tokio::test]
async fn market_order_without_liquidity_rejects() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let mut req = order_req(
        "BTC-USD",
        "alice",
        "mkt-1",
        OrderSide::Bid,
        TimeInForce::Ioc,
        false,
        0,
        1,
    );
    req.order_type = OrderType::Market;
    let err = db
        .order_book_new("p", "app", req)
        .await
        .expect_err("market with no liquidity should reject");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[tokio::test]
async fn self_trade_prevention_modes_apply() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    db.order_book_new("p", "app", {
        let mut req = order_req(
            "BTC-USD",
            "alice",
            "resting",
            OrderSide::Ask,
            TimeInForce::Gtc,
            false,
            100,
            5,
        );
        req.nonce = 1;
        req
    })
    .await
    .expect("seed");
    let resting_id = load_order_id(&db, "BTC-USD", "alice", "resting").await;

    db.order_book_new("p", "app", {
        let mut req = order_req_with_stp(
            "BTC-USD",
            "alice",
            "aggr-cancel-resting",
            OrderSide::Bid,
            100,
            2,
            SelfTradePrevention::CancelResting,
        );
        req.nonce = 2;
        req
    })
    .await
    .expect("stp cancel resting");
    let resting = load_order(&db, "BTC-USD", resting_id).await;
    assert_eq!(resting.status, OrderStatus::Cancelled);

    db.order_book_new("p", "app", {
        let mut req = order_req(
            "BTC-USD",
            "alice",
            "resting-2",
            OrderSide::Ask,
            TimeInForce::Gtc,
            false,
            101,
            5,
        );
        req.nonce = 3;
        req
    })
    .await
    .expect("seed2");
    let resting2_id = load_order_id(&db, "BTC-USD", "alice", "resting-2").await;

    db.order_book_new("p", "app", {
        let mut req = order_req_with_stp(
            "BTC-USD",
            "alice",
            "aggr-cancel-both",
            OrderSide::Bid,
            101,
            2,
            SelfTradePrevention::CancelBoth,
        );
        req.nonce = 4;
        req
    })
    .await
    .expect("stp cancel both");
    let resting2 = load_order(&db, "BTC-USD", resting2_id).await;
    assert_eq!(resting2.status, OrderStatus::Cancelled);

    db.order_book_new("p", "app", {
        let mut req = order_req(
            "BTC-USD",
            "alice",
            "resting-3",
            OrderSide::Ask,
            TimeInForce::Gtc,
            false,
            102,
            5,
        );
        req.nonce = 5;
        req
    })
    .await
    .expect("seed3");
    let resting3_id = load_order_id(&db, "BTC-USD", "alice", "resting-3").await;

    db.order_book_new("p", "app", {
        let mut req = order_req_with_stp(
            "BTC-USD",
            "alice",
            "aggr-cancel-aggressor",
            OrderSide::Bid,
            102,
            2,
            SelfTradePrevention::CancelAggressor,
        );
        req.nonce = 6;
        req
    })
    .await
    .expect("stp cancel aggressor");
    let resting3 = load_order(&db, "BTC-USD", resting3_id).await;
    assert_eq!(resting3.status, OrderStatus::Open);
}

#[tokio::test]
async fn instrument_config_and_halt_are_enforced() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    db.order_book_set_instrument_config(
        "p",
        "app",
        "BTC-USD",
        InstrumentConfig {
            instrument: "BTC-USD".to_string(),
            tick_size: 1,
            lot_size_be: u256_be(2),
            min_price_ticks: 10,
            max_price_ticks: 200,
            market_order_price_band: Some(20),
            halted: false,
            balance_config: None,
        },
    )
    .await
    .expect("set config");

    let err = db
        .order_book_new(
            "p",
            "app",
            order_req(
                "BTC-USD",
                "alice",
                "bad-lot",
                OrderSide::Bid,
                TimeInForce::Gtc,
                false,
                100,
                3,
            ),
        )
        .await
        .expect_err("lot size should reject");
    assert!(matches!(err, AedbError::Validation(_)));

    db.order_book_set_instrument_halted("p", "app", "BTC-USD", true)
        .await
        .expect("halt");
    let err = db
        .order_book_new(
            "p",
            "app",
            order_req(
                "BTC-USD",
                "alice",
                "halted",
                OrderSide::Bid,
                TimeInForce::Gtc,
                false,
                100,
                2,
            ),
        )
        .await
        .expect_err("halted should reject");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[tokio::test]
async fn execution_report_is_persisted() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.order_book_new(
        "p",
        "app",
        order_req(
            "BTC-USD",
            "maker",
            "mk-rpt",
            OrderSide::Ask,
            TimeInForce::Gtc,
            false,
            100,
            2,
        ),
    )
    .await
    .expect("seed");
    db.order_book_new(
        "p",
        "app",
        order_req(
            "BTC-USD",
            "taker",
            "tk-rpt",
            OrderSide::Bid,
            TimeInForce::Ioc,
            false,
            100,
            2,
        ),
    )
    .await
    .expect("cross");

    let report_key = aedb::order_book::key_execution_report_last("BTC-USD");
    let report = db
        .kv_get_no_auth("p", "app", &report_key, ConsistencyMode::AtLatest)
        .await
        .expect("report query")
        .expect("report exists");
    let decoded: aedb::order_book::ExecutionReport = if report.value.len() == 16 {
        let commit_seq = u64::from_be_bytes(report.value[0..8].try_into().expect("seq bytes"));
        let order_id = u64::from_be_bytes(report.value[8..16].try_into().expect("id bytes"));
        let full = db
            .kv_get_no_auth(
                "p",
                "app",
                &key_execution_report("BTC-USD", commit_seq, order_id),
                ConsistencyMode::AtLatest,
            )
            .await
            .expect("report by pointer query")
            .expect("report by pointer exists");
        rmp_serde::from_slice(&full.value).expect("decode pointed report")
    } else {
        rmp_serde::from_slice(&report.value).expect("decode inline report")
    };
    assert_eq!(decoded.client_order_id, "tk-rpt");
    assert!(decoded.seq > 0);
}
