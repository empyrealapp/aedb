use aedb::AedbInstance;
use aedb::error::AedbError;
use aedb::order_book::{
    ExecInstruction, InstrumentConfig, OrderRequest, OrderSide, OrderStatus, OrderType,
    TimeInForce, parse_plqty_price,
};
use aedb::query::plan::ConsistencyMode;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::BTreeMap;
use std::sync::Arc;
use tempfile::tempdir;

fn u256_be(v: u64) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[24..].copy_from_slice(&v.to_be_bytes());
    out
}

fn decode_u256_u64(bytes: [u8; 32]) -> u64 {
    let mut out = [0u8; 8];
    out.copy_from_slice(&bytes[24..]);
    u64::from_be_bytes(out)
}

fn decode_u256_bytes_to_u64(bytes: &[u8]) -> u64 {
    assert_eq!(bytes.len(), 32);
    let mut out = [0u8; 8];
    out.copy_from_slice(&bytes[24..]);
    u64::from_be_bytes(out)
}

fn request(
    instrument: &str,
    owner: &str,
    client_order_id: String,
    side: OrderSide,
    order_type: OrderType,
    tif: TimeInForce,
    post_only: bool,
    price_ticks: i64,
    qty: u64,
    nonce: u64,
) -> OrderRequest {
    OrderRequest {
        instrument: instrument.to_string(),
        client_order_id,
        side,
        order_type,
        time_in_force: tif,
        exec_instructions: ExecInstruction(if post_only {
            ExecInstruction::POST_ONLY
        } else {
            0
        }),
        self_trade_prevention: aedb::order_book::SelfTradePrevention::None,
        price_ticks,
        qty_be: u256_be(qty),
        owner: owner.to_string(),
        account: None,
        nonce,
        price_limit_ticks: None,
    }
}

async fn setup_books(db: &AedbInstance, assets: &[String]) {
    db.create_project("p").await.expect("project");
    for asset in assets {
        db.order_book_set_instrument_config(
            "p",
            "app",
            asset,
            InstrumentConfig {
                instrument: asset.clone(),
                tick_size: 1,
                lot_size_be: u256_be(1),
                min_price_ticks: 1,
                max_price_ticks: 1_000_000,
                market_order_price_band: Some(50),
                halted: false,
                balance_config: None,
            },
        )
        .await
        .expect("config");

        // Seed symmetric depth around 1_000 ticks.
        for i in 0..20_u64 {
            let ask_owner = format!("seed_ask_{}_{}", asset, i);
            db.order_book_new(
                "p",
                "app",
                request(
                    asset,
                    &ask_owner,
                    format!("seed-a-{i}"),
                    OrderSide::Ask,
                    OrderType::Limit,
                    TimeInForce::Gtc,
                    false,
                    1_000 + i as i64,
                    10,
                    1,
                ),
            )
            .await
            .expect("seed ask");

            let bid_owner = format!("seed_bid_{}_{}", asset, i);
            db.order_book_new(
                "p",
                "app",
                request(
                    asset,
                    &bid_owner,
                    format!("seed-b-{i}"),
                    OrderSide::Bid,
                    OrderType::Limit,
                    TimeInForce::Gtc,
                    false,
                    999 - i as i64,
                    10,
                    1,
                ),
            )
            .await
            .expect("seed bid");
        }
    }
}

async fn run_simulation(
    assets: Vec<String>,
    traders: usize,
    ops_per_trader: usize,
) -> Arc<AedbInstance> {
    let dir = tempdir().expect("temp");
    let db = Arc::new(AedbInstance::open(Default::default(), dir.path()).expect("open"));
    setup_books(&db, &assets).await;

    let mut tasks = Vec::with_capacity(traders);
    for t in 0..traders {
        let db_clone = Arc::clone(&db);
        let assets_clone = assets.clone();
        tasks.push(tokio::spawn(async move {
            let owner = format!("trader_{t}");
            let mut nonces: BTreeMap<String, u64> = BTreeMap::new();
            let mut rng = StdRng::seed_from_u64(42 + t as u64);

            for op in 0..ops_per_trader {
                let asset = &assets_clone[rng.gen_range(0..assets_clone.len())];
                let nonce = nonces.entry(asset.clone()).or_insert(0);
                *nonce += 1;

                let side = if rng.gen_bool(0.5) {
                    OrderSide::Bid
                } else {
                    OrderSide::Ask
                };
                let price = 995 + rng.gen_range(0..12) as i64;
                let qty = 1 + rng.gen_range(0..5) as u64;
                let tif = if rng.gen_bool(0.7) {
                    TimeInForce::Ioc
                } else {
                    TimeInForce::Fok
                };
                let order_type = if rng.gen_bool(0.1) {
                    OrderType::Market
                } else {
                    OrderType::Limit
                };
                let post_only = order_type == OrderType::Limit && rng.gen_bool(0.05);

                let res = db_clone
                    .order_book_new(
                        "p",
                        "app",
                        request(
                            asset,
                            &owner,
                            format!("{owner}-{op}"),
                            side,
                            order_type,
                            tif,
                            post_only,
                            price,
                            qty,
                            *nonce,
                        ),
                    )
                    .await;

                if let Err(err) = res {
                    // Expected rejects under stress: FOK, market no liquidity, post-only crossing.
                    match err {
                        AedbError::Validation(_) => {}
                        other => panic!("unexpected simulation error: {other:?}"),
                    }
                }

                // Periodically exercise lifecycle primitives.
                if op % 100 == 0 {
                    *nonce += 1;
                    let cid = format!("gtc-{owner}-{op}");
                    let _ = db_clone
                        .order_book_new(
                            "p",
                            "app",
                            request(
                                asset,
                                &owner,
                                cid.clone(),
                                side,
                                OrderType::Limit,
                                TimeInForce::Gtc,
                                false,
                                price,
                                qty,
                                *nonce,
                            ),
                        )
                        .await;
                    let _ = db_clone
                        .order_book_cancel_by_client_id("p", "app", asset, &cid, &owner)
                        .await;
                }
            }
        }));
    }

    for task in tasks {
        task.await.expect("task join");
    }

    db
}

async fn assert_book_invariants(db: &AedbInstance, assets: &[String]) {
    for asset in assets {
        let mut from_orders: BTreeMap<(u8, i64), u64> = BTreeMap::new();

        let rows = db
            .kv_scan_prefix_no_auth(
                "p",
                "app",
                format!("ob:{asset}:ord:").as_bytes(),
                1_000_000,
                ConsistencyMode::AtLatest,
            )
            .await
            .expect("scan orders");

        for (_, entry) in rows {
            let order: aedb::order_book::OrderRecord =
                rmp_serde::from_slice(&entry.value).expect("decode order");
            let original = decode_u256_u64(order.original_qty_be);
            let remaining = decode_u256_u64(order.remaining_qty_be);
            let filled = decode_u256_u64(order.filled_qty_be);
            assert!(
                remaining + filled <= original,
                "quantity accounting invariant"
            );
            if remaining > 0
                && matches!(
                    order.status,
                    OrderStatus::Open | OrderStatus::PartiallyFilled
                )
            {
                *from_orders
                    .entry((order.side as u8, order.price_ticks))
                    .or_insert(0) += remaining;
            }
        }

        let mut from_levels: BTreeMap<(u8, i64), u64> = BTreeMap::new();
        for side in [OrderSide::Bid, OrderSide::Ask] {
            let levels = db
                .kv_scan_prefix_no_auth(
                    "p",
                    "app",
                    format!("ob:{asset}:plqty:{}:", side as u8).as_bytes(),
                    1_000_000,
                    ConsistencyMode::AtLatest,
                )
                .await
                .expect("scan levels");
            for (k, v) in levels {
                let qty = decode_u256_bytes_to_u64(&v.value);
                if qty == 0 {
                    continue;
                }
                let price = parse_plqty_price(side, &k).expect("parse level price");
                from_levels.insert((side as u8, price), qty);
            }
        }

        assert_eq!(
            from_orders, from_levels,
            "price-level aggregates must match open orders for {asset}"
        );
    }
}

#[tokio::test]
async fn order_book_simulation_smoke() {
    let assets = vec!["BTC-USD".to_string(), "ETH-USD".to_string()];
    let db = run_simulation(assets.clone(), 6, 250).await;
    assert_book_invariants(&db, &assets).await;
}

#[tokio::test]
#[ignore = "long-running high-frequency simulation"]
async fn order_book_simulation_hft_soak() {
    let assets = vec![
        "BTC-USD".to_string(),
        "ETH-USD".to_string(),
        "SOL-USD".to_string(),
        "DOGE-USD".to_string(),
    ];
    let db = run_simulation(assets.clone(), 24, 2_000).await;
    assert_book_invariants(&db, &assets).await;
}
