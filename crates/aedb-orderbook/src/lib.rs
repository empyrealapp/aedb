use aedb::AedbInstance;
use aedb::commit::validation::Mutation;
use aedb::config::{AedbConfig, DurabilityMode};
use aedb::error::AedbError;
use aedb::order_book::{
    ExecInstruction, InstrumentConfig, OrderBookTableMode, OrderRequest, OrderSide, OrderStatus,
    OrderType, TimeInForce, parse_plqty_price, scoped_instrument,
};
use aedb::query::plan::ConsistencyMode;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationConfig {
    pub assets: Vec<String>,
    pub traders: usize,
    pub ops_per_trader: usize,
    pub seed: u64,
    pub flow_profile: OrderFlowProfile,
    pub table_profile: TableProfile,
    pub collect_latency: bool,
    pub lifecycle_every_ops: usize,
    pub orders_per_commit: usize,
    pub match_workload: MatchWorkload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum OrderFlowProfile {
    LimitOnlyIoc,
    MixedMarketAndLimit,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TableProfile {
    NativeInstrument,
    PerAssetTable,
    MultiAssetTable { table_id: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MatchWorkload {
    CrossingNearTouch,
    NoCrossIoc,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SimulationReport {
    pub assets: Vec<String>,
    pub instruments: Vec<String>,
    pub traders: usize,
    pub ops_per_trader: usize,
    pub attempted_orders: usize,
    pub accepted_orders: usize,
    pub rejected_orders: usize,
    pub max_commit_seq: u64,
    pub visible_head_seq: u64,
    pub durable_head_seq: u64,
    pub flow_profile: OrderFlowProfile,
    pub table_profile: TableProfile,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LatencyStats {
    pub samples: usize,
    pub avg_us: u64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProfiledSimulationReport {
    pub simulation: SimulationReport,
    pub elapsed_ms: u64,
    pub attempted_ops_per_sec: u64,
    pub accepted_ops_per_sec: u64,
    pub rejected_ops_per_sec: u64,
    pub max_commit_finality_gap: u64,
    pub zero_dropped_orders: bool,
    pub latency: LatencyStats,
    pub durability_mode: String,
}

pub fn high_throughput_simulation_config() -> AedbConfig {
    AedbConfig {
        durability_mode: DurabilityMode::Batch,
        batch_interval_ms: 20,
        batch_max_bytes: 2 * 1024 * 1024,
        max_inflight_commits: 4096,
        max_commit_queue_bytes: 256 * 1024 * 1024,
        epoch_max_wait_us: 250,
        epoch_min_commits: 16,
        epoch_max_commits: 1024,
        adaptive_epoch_min_commits_floor: 8,
        adaptive_epoch_min_commits_ceiling: 1024,
        adaptive_epoch_wait_us_floor: 25,
        adaptive_epoch_wait_us_ceiling: 5_000,
        adaptive_epoch_target_latency_us: 5_000,
        prestage_shards: 16,
        ..AedbConfig::default()
    }
}

pub fn tuned_simulation_config_with_durability(durability_mode: DurabilityMode) -> AedbConfig {
    AedbConfig {
        durability_mode,
        ..high_throughput_simulation_config()
    }
}

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

async fn setup_books(
    db: &AedbInstance,
    assets: &[String],
    table_profile: &TableProfile,
) -> Result<Vec<String>, AedbError> {
    db.create_project("p").await.expect("project");
    let mut instruments = Vec::with_capacity(assets.len());
    let multi_table_id = match table_profile {
        TableProfile::MultiAssetTable { table_id } => Some(table_id.clone()),
        _ => None,
    };
    if let Some(table_id) = &multi_table_id {
        db.order_book_define_table("p", "app", table_id, OrderBookTableMode::MultiAsset)
            .await?;
    }
    for asset in assets {
        let instrument = match table_profile {
            TableProfile::NativeInstrument => asset.clone(),
            TableProfile::PerAssetTable => {
                db.order_book_define_table("p", "app", asset, OrderBookTableMode::PerAsset)
                    .await?;
                scoped_instrument(asset, asset)
            }
            TableProfile::MultiAssetTable { table_id } => scoped_instrument(table_id, asset),
        };
        instruments.push(instrument.clone());
        db.order_book_set_instrument_config(
            "p",
            "app",
            &instrument,
            InstrumentConfig {
                instrument: instrument.clone(),
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

        for i in 0..20_u64 {
            let ask_owner = format!("seed_ask_{}_{}", instrument, i);
            db.order_book_new(
                "p",
                "app",
                request(
                    &instrument,
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

            let bid_owner = format!("seed_bid_{}_{}", instrument, i);
            db.order_book_new(
                "p",
                "app",
                request(
                    &instrument,
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
    Ok(instruments)
}

async fn assert_book_invariants(db: &AedbInstance, instruments: &[String]) {
    for instrument in instruments {
        let mut from_orders: BTreeMap<(u8, i64), u64> = BTreeMap::new();

        let rows = db
            .kv_scan_prefix_no_auth(
                "p",
                "app",
                format!("ob:{instrument}:ord:").as_bytes(),
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
                    format!("ob:{instrument}:plqty:{}:", side as u8).as_bytes(),
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
            "price-level aggregates must match open orders for {instrument}"
        );
    }
}

pub async fn run_hft_simulation(cfg: SimulationConfig) -> Result<SimulationReport, AedbError> {
    run_hft_simulation_with_config(cfg, high_throughput_simulation_config())
        .await
        .map(|r| r.simulation)
}

pub async fn run_hft_simulation_with_config(
    cfg: SimulationConfig,
    db_cfg: AedbConfig,
) -> Result<ProfiledSimulationReport, AedbError> {
    let durability_mode_name = match db_cfg.durability_mode {
        DurabilityMode::Full => "full",
        DurabilityMode::Batch => "batch",
        DurabilityMode::OsBuffered => "os_buffered",
    }
    .to_string();
    let dir = tempdir().expect("temp");
    let db = Arc::new(AedbInstance::open(db_cfg, dir.path()).expect("open"));
    let instruments = setup_books(&db, &cfg.assets, &cfg.table_profile).await?;
    let run_started = Instant::now();

    let mut tasks = Vec::with_capacity(cfg.traders);
    for t in 0..cfg.traders {
        let db_clone = Arc::clone(&db);
        let instruments_clone = instruments.clone();
        let flow_profile = cfg.flow_profile.clone();
        let match_workload = cfg.match_workload.clone();
        let seed = cfg.seed;
        let ops_per_trader = cfg.ops_per_trader;
        let orders_per_commit = cfg.orders_per_commit.max(1);
        tasks.push(tokio::spawn(async move {
            let owner = format!("trader_{t}");
            let mut nonces: BTreeMap<String, u64> = BTreeMap::new();
            let mut rng = StdRng::seed_from_u64(seed + t as u64);
            let mut accepted = 0usize;
            let mut rejected = 0usize;
            let mut max_commit_seq = 0u64;
            let mut max_finality_gap = 0u64;
            let mut latencies_us = if cfg.collect_latency {
                Vec::with_capacity(ops_per_trader)
            } else {
                Vec::new()
            };
            let mut pending_mutations = Vec::with_capacity(orders_per_commit);
            let mut pending_orders = 0usize;
            let mut pending_started: Option<Instant> = None;

            for op in 0..ops_per_trader {
                let instrument = &instruments_clone[rng.gen_range(0..instruments_clone.len())];
                let nonce = nonces.entry(instrument.clone()).or_insert(0);
                *nonce += 1;

                let side = if rng.gen_bool(0.5) {
                    OrderSide::Bid
                } else {
                    OrderSide::Ask
                };
                let mut price = match match_workload {
                    MatchWorkload::CrossingNearTouch => 995 + rng.gen_range(0..12) as i64,
                    MatchWorkload::NoCrossIoc => {
                        if side == OrderSide::Bid {
                            900
                        } else {
                            1_100
                        }
                    }
                };
                let qty = 1 + rng.gen_range(0..5) as u64;
                let (order_type, tif) = match flow_profile {
                    OrderFlowProfile::LimitOnlyIoc => (OrderType::Limit, TimeInForce::Ioc),
                    OrderFlowProfile::MixedMarketAndLimit => {
                        let tif = if rng.gen_bool(0.7) {
                            TimeInForce::Ioc
                        } else {
                            TimeInForce::Fok
                        };
                        if rng.gen_bool(0.15) {
                            (OrderType::Market, tif)
                        } else {
                            (OrderType::Limit, tif)
                        }
                    }
                };
                let post_only = matches!(match_workload, MatchWorkload::CrossingNearTouch)
                    && order_type == OrderType::Limit
                    && rng.gen_bool(0.05);
                if order_type == OrderType::Market {
                    price = 0;
                }

                if pending_mutations.is_empty() {
                    pending_started = Some(Instant::now());
                }
                pending_mutations.push(Mutation::OrderBookNew {
                    project_id: "p".to_string(),
                    scope_id: "app".to_string(),
                    request: request(
                        instrument,
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
                });
                pending_orders += 1;
                if pending_orders >= orders_per_commit {
                    if let Err(other) = flush_pending_orders(
                        db_clone.as_ref(),
                        &mut pending_mutations,
                        &mut pending_orders,
                        &mut pending_started,
                        cfg.collect_latency,
                        &mut latencies_us,
                        &mut accepted,
                        &mut rejected,
                        &mut max_commit_seq,
                        &mut max_finality_gap,
                    )
                    .await
                    {
                        panic!("unexpected simulation error: {other:?}");
                    }
                }

                if cfg.lifecycle_every_ops > 0 && op % cfg.lifecycle_every_ops == 0 {
                    *nonce += 1;
                    let cid = format!("gtc-{owner}-{op}");
                    let _ = db_clone
                        .order_book_new(
                            "p",
                            "app",
                            request(
                                instrument,
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
                        .order_book_cancel_by_client_id("p", "app", instrument, &cid, &owner)
                        .await;
                }
            }
            if let Err(other) = flush_pending_orders(
                db_clone.as_ref(),
                &mut pending_mutations,
                &mut pending_orders,
                &mut pending_started,
                cfg.collect_latency,
                &mut latencies_us,
                &mut accepted,
                &mut rejected,
                &mut max_commit_seq,
                &mut max_finality_gap,
            )
            .await
            {
                panic!("unexpected simulation error: {other:?}");
            }
            (
                accepted,
                rejected,
                max_commit_seq,
                max_finality_gap,
                latencies_us,
            )
        }));
    }

    let mut accepted_orders = 0usize;
    let mut rejected_orders = 0usize;
    let mut max_commit_seq = 0u64;
    let mut max_commit_finality_gap = 0u64;
    let mut all_latencies_us = Vec::new();
    for task in tasks {
        let (a, r, max_seq, max_gap, mut latencies) = task.await.expect("task join");
        accepted_orders += a;
        rejected_orders += r;
        max_commit_seq = max_commit_seq.max(max_seq);
        max_commit_finality_gap = max_commit_finality_gap.max(max_gap);
        all_latencies_us.append(&mut latencies);
    }

    assert_book_invariants(&db, &instruments).await;
    let elapsed_ms = run_started.elapsed().as_millis().max(1) as u64;
    db.force_fsync().await?;
    let heads = db.head_state().await;
    assert!(
        heads.visible_head_seq >= max_commit_seq,
        "visible head must include all accepted commits"
    );
    assert!(
        heads.durable_head_seq >= max_commit_seq,
        "durable head must include all accepted commits after final fsync"
    );

    all_latencies_us.sort_unstable();
    let latency = summarize_latency(&all_latencies_us);
    let attempted_orders = cfg.traders * cfg.ops_per_trader;
    let attempted_ops_per_sec = (attempted_orders as u64).saturating_mul(1000) / elapsed_ms;
    let accepted_ops_per_sec = (accepted_orders as u64).saturating_mul(1000) / elapsed_ms;
    let rejected_ops_per_sec = (rejected_orders as u64).saturating_mul(1000) / elapsed_ms;

    let simulation = SimulationReport {
        assets: cfg.assets,
        instruments,
        traders: cfg.traders,
        ops_per_trader: cfg.ops_per_trader,
        attempted_orders,
        accepted_orders,
        rejected_orders,
        max_commit_seq,
        visible_head_seq: heads.visible_head_seq,
        durable_head_seq: heads.durable_head_seq,
        flow_profile: cfg.flow_profile,
        table_profile: cfg.table_profile,
    };
    Ok(ProfiledSimulationReport {
        zero_dropped_orders: simulation.accepted_orders + simulation.rejected_orders
            == simulation.attempted_orders,
        simulation,
        elapsed_ms,
        attempted_ops_per_sec,
        accepted_ops_per_sec,
        rejected_ops_per_sec,
        max_commit_finality_gap,
        latency,
        durability_mode: durability_mode_name,
    })
}

fn summarize_latency(sorted_latencies_us: &[u64]) -> LatencyStats {
    if sorted_latencies_us.is_empty() {
        return LatencyStats {
            samples: 0,
            avg_us: 0,
            p50_us: 0,
            p95_us: 0,
            p99_us: 0,
            max_us: 0,
        };
    }
    let samples = sorted_latencies_us.len();
    let sum: u128 = sorted_latencies_us.iter().map(|v| *v as u128).sum();
    let percentile = |p: f64| -> u64 {
        let idx = ((samples as f64 - 1.0) * p).round() as usize;
        sorted_latencies_us[idx]
    };
    LatencyStats {
        samples,
        avg_us: (sum / samples as u128) as u64,
        p50_us: percentile(0.50),
        p95_us: percentile(0.95),
        p99_us: percentile(0.99),
        max_us: *sorted_latencies_us.last().expect("non-empty"),
    }
}

async fn flush_pending_orders(
    db: &AedbInstance,
    pending_mutations: &mut Vec<Mutation>,
    pending_orders: &mut usize,
    pending_started: &mut Option<Instant>,
    collect_latency: bool,
    latencies_us: &mut Vec<u64>,
    accepted: &mut usize,
    rejected: &mut usize,
    max_commit_seq: &mut u64,
    max_finality_gap: &mut u64,
) -> Result<(), AedbError> {
    if pending_mutations.is_empty() {
        return Ok(());
    }
    let started = pending_started.take().unwrap_or_else(Instant::now);
    let batch_len = *pending_orders;
    let res = db
        .commit_many_atomic(std::mem::take(pending_mutations))
        .await;
    let elapsed = started.elapsed().as_micros() as u64;
    if collect_latency && batch_len > 0 {
        let per_order = (elapsed / batch_len as u64).max(1);
        latencies_us.extend(std::iter::repeat_n(per_order, batch_len));
    }
    *pending_orders = 0;
    match res {
        Ok(commit) => {
            let gap = commit.commit_seq.saturating_sub(commit.durable_head_seq);
            *max_finality_gap = (*max_finality_gap).max(gap);
            *max_commit_seq = (*max_commit_seq).max(commit.commit_seq);
            *accepted += batch_len;
        }
        Err(err) => match err {
            AedbError::Validation(_) => *rejected += batch_len,
            other => return Err(other),
        },
    }
    Ok(())
}
