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

fn decode_u256_bytes_to_u64(bytes: &[u8]) -> Result<u64, AedbError> {
    if bytes.len() != 32 {
        return Err(AedbError::Validation(format!(
            "invalid u256 byte length: {}",
            bytes.len()
        )));
    }
    let mut out = [0u8; 8];
    out.copy_from_slice(&bytes[24..]);
    Ok(u64::from_be_bytes(out))
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

async fn setup_books(db: &AedbInstance, assets: &[String]) -> Result<(), AedbError> {
    db.create_project("p").await?;
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
        .await?;

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
            .await?;

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
            .await?;
        }
    }
    Ok(())
}

#[derive(Debug, Default)]
struct ChaosMetrics {
    primary_attempted: usize,
    primary_accepted: usize,
    primary_rejected: usize,
    lifecycle_attempted: usize,
    lifecycle_accepted: usize,
    lifecycle_rejected: usize,
    reader_checks: usize,
}

async fn validate_asset_read_consistency(db: &AedbInstance, asset: &str) -> Result<(), AedbError> {
    let rows = db
        .kv_scan_prefix_no_auth(
            "p",
            "app",
            format!("ob:{asset}:ord:").as_bytes(),
            2_000_000,
            ConsistencyMode::AtLatest,
        )
        .await
        .map_err(|e| AedbError::Validation(e.to_string()))?;
    for (_, entry) in rows {
        let order: aedb::order_book::OrderRecord =
            rmp_serde::from_slice(&entry.value).map_err(|e| AedbError::Decode(e.to_string()))?;
        let original = decode_u256_u64(order.original_qty_be);
        let remaining = decode_u256_u64(order.remaining_qty_be);
        let filled = decode_u256_u64(order.filled_qty_be);
        if remaining + filled > original {
            return Err(AedbError::Validation(format!(
                "quantity accounting violated in live read for {asset}"
            )));
        }
    }

    for side in [OrderSide::Bid, OrderSide::Ask] {
        let levels = db
            .kv_scan_prefix_no_auth(
                "p",
                "app",
                format!("ob:{asset}:plqty:{}:", side as u8).as_bytes(),
                2_000_000,
                ConsistencyMode::AtLatest,
            )
            .await
            .map_err(|e| AedbError::Validation(e.to_string()))?;
        for (k, v) in levels {
            let qty = decode_u256_bytes_to_u64(&v.value)?;
            if qty == 0 {
                continue;
            }
            parse_plqty_price(side, &k).ok_or_else(|| {
                AedbError::Validation(format!("failed to parse level price for {asset}"))
            })?;
        }
    }
    Ok(())
}

async fn run_simulation(
    assets: Vec<String>,
    traders: usize,
    ops_per_trader: usize,
) -> Result<(Arc<AedbInstance>, ChaosMetrics), AedbError> {
    let dir = tempdir().map_err(AedbError::Io)?;
    let db = Arc::new(AedbInstance::open(Default::default(), dir.path())?);
    setup_books(&db, &assets).await?;

    let mut tasks = Vec::with_capacity(traders);
    for t in 0..traders {
        let db_clone = Arc::clone(&db);
        let assets_clone = assets.clone();
        tasks.push(tokio::spawn(async move {
            let owner = format!("trader_{t}");
            let mut nonces: BTreeMap<String, u64> = BTreeMap::new();
            let mut rng = StdRng::seed_from_u64(42 + t as u64);
            let mut primary_attempted = 0usize;
            let mut primary_accepted = 0usize;
            let mut primary_rejected = 0usize;
            let mut lifecycle_attempted = 0usize;
            let mut lifecycle_accepted = 0usize;
            let mut lifecycle_rejected = 0usize;

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

                primary_attempted += 1;
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

                match res {
                    Ok(_) => primary_accepted += 1,
                    Err(err) => {
                        // Expected rejects under stress: FOK, market no liquidity, post-only crossing.
                        match err {
                            AedbError::Validation(_) => primary_rejected += 1,
                            other => return Err(other),
                        }
                    }
                }

                // Periodically exercise lifecycle primitives.
                if op % 100 == 0 {
                    *nonce += 1;
                    let cid = format!("gtc-{owner}-{op}");
                    lifecycle_attempted += 1;
                    match db_clone
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
                        .await
                    {
                        Ok(_) => lifecycle_accepted += 1,
                        Err(AedbError::Validation(_)) => lifecycle_rejected += 1,
                        Err(other) => return Err(other),
                    }

                    lifecycle_attempted += 1;
                    match db_clone
                        .order_book_cancel_by_client_id("p", "app", asset, &cid, &owner)
                        .await
                    {
                        Ok(_) => lifecycle_accepted += 1,
                        Err(AedbError::Validation(_)) => lifecycle_rejected += 1,
                        Err(other) => return Err(other),
                    }
                }
            }
            Ok(ChaosMetrics {
                primary_attempted,
                primary_accepted,
                primary_rejected,
                lifecycle_attempted,
                lifecycle_accepted,
                lifecycle_rejected,
                reader_checks: 0,
            })
        }));
    }

    let reader_workers = (traders / 2).max(4);
    let reader_loops = (ops_per_trader / 2).max(200);
    for r in 0..reader_workers {
        let db_clone = Arc::clone(&db);
        let assets_clone = assets.clone();
        tasks.push(tokio::spawn(async move {
            let mut rng = StdRng::seed_from_u64(9_000 + r as u64);
            let mut checks = 0usize;
            for _ in 0..reader_loops {
                let asset = &assets_clone[rng.gen_range(0..assets_clone.len())];
                validate_asset_read_consistency(db_clone.as_ref(), asset).await?;
                checks += 1;
            }
            Ok(ChaosMetrics {
                reader_checks: checks,
                ..Default::default()
            })
        }));
    }

    let mut metrics = ChaosMetrics::default();
    for task in tasks {
        let worker = task
            .await
            .map_err(|e| AedbError::Validation(format!("simulation task join failure: {e}")))?;
        let worker = worker?;
        metrics.primary_attempted += worker.primary_attempted;
        metrics.primary_accepted += worker.primary_accepted;
        metrics.primary_rejected += worker.primary_rejected;
        metrics.lifecycle_attempted += worker.lifecycle_attempted;
        metrics.lifecycle_accepted += worker.lifecycle_accepted;
        metrics.lifecycle_rejected += worker.lifecycle_rejected;
        metrics.reader_checks += worker.reader_checks;
    }

    if metrics.primary_accepted + metrics.primary_rejected != metrics.primary_attempted {
        return Err(AedbError::Validation(
            "primary flow accounting mismatch".into(),
        ));
    }
    if metrics.lifecycle_accepted + metrics.lifecycle_rejected != metrics.lifecycle_attempted {
        return Err(AedbError::Validation(
            "lifecycle flow accounting mismatch".into(),
        ));
    }

    Ok((db, metrics))
}

async fn assert_book_invariants(db: &AedbInstance, assets: &[String]) -> Result<(), AedbError> {
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
            .map_err(|e| AedbError::Validation(e.to_string()))?;

        for (_, entry) in rows {
            let order: aedb::order_book::OrderRecord = rmp_serde::from_slice(&entry.value)
                .map_err(|e| AedbError::Decode(e.to_string()))?;
            let original = decode_u256_u64(order.original_qty_be);
            let remaining = decode_u256_u64(order.remaining_qty_be);
            let filled = decode_u256_u64(order.filled_qty_be);
            if remaining + filled > original {
                return Err(AedbError::Validation(
                    "quantity accounting invariant violated".into(),
                ));
            }
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
                .map_err(|e| AedbError::Validation(e.to_string()))?;
            for (k, v) in levels {
                let qty = decode_u256_bytes_to_u64(&v.value)?;
                if qty == 0 {
                    continue;
                }
                let price = parse_plqty_price(side, &k)
                    .ok_or_else(|| AedbError::Validation("failed to parse level price".into()))?;
                from_levels.insert((side as u8, price), qty);
            }
        }

        if from_orders != from_levels {
            return Err(AedbError::Validation(format!(
                "price-level aggregates mismatch for {asset}"
            )));
        }
    }
    Ok(())
}

#[tokio::test]
async fn order_book_simulation_smoke() {
    let assets = vec!["BTC-USD".to_string(), "ETH-USD".to_string()];
    let (db, metrics) = run_simulation(assets.clone(), 6, 250)
        .await
        .expect("run simulation");
    assert!(metrics.reader_checks > 0, "reader workers should execute");
    assert_book_invariants(&db, &assets)
        .await
        .expect("final invariants");
}

#[tokio::test]
async fn order_book_chaos_read_write_accuracy() {
    let assets = vec![
        "BTC-USD".to_string(),
        "ETH-USD".to_string(),
        "SOL-USD".to_string(),
        "DOGE-USD".to_string(),
    ];
    let (db, metrics) = run_simulation(assets.clone(), 16, 800)
        .await
        .expect("chaos run");
    assert!(
        metrics.primary_attempted >= 16 * 800,
        "writers should execute full primary load"
    );
    assert!(
        metrics.reader_checks >= 1_000,
        "read-side chaos checks should be substantial"
    );
    assert_book_invariants(&db, &assets)
        .await
        .expect("final invariants");
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
    let (db, _metrics) = run_simulation(assets.clone(), 24, 2_000)
        .await
        .expect("hft soak");
    assert_book_invariants(&db, &assets)
        .await
        .expect("final invariants");
}
