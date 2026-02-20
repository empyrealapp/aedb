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
    pub lifecycle_attempted_ops: usize,
    pub lifecycle_accepted_ops: usize,
    pub lifecycle_rejected_ops: usize,
    pub lifecycle_rejection_breakdown: RejectionBreakdown,
    pub rejection_breakdown: RejectionBreakdown,
    pub max_commit_seq: u64,
    pub visible_head_seq: u64,
    pub durable_head_seq: u64,
    pub flow_profile: OrderFlowProfile,
    pub table_profile: TableProfile,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RejectionBreakdown {
    pub conflict: usize,
    pub post_only_would_cross: usize,
    pub fok_cannot_fill: usize,
    pub market_no_liquidity: usize,
    pub instrument_halted: usize,
    pub nonce_too_low: usize,
    pub duplicate_client_order_id: usize,
    pub lot_size_violation: usize,
    pub qty_non_positive: usize,
    pub price_out_of_bounds: usize,
    pub other_validation: usize,
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

async fn setup_books(
    db: &AedbInstance,
    assets: &[String],
    table_profile: &TableProfile,
) -> Result<Vec<String>, AedbError> {
    db.create_project("p").await?;
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

async fn assert_book_invariants(
    db: &AedbInstance,
    instruments: &[String],
) -> Result<(), AedbError> {
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
            .map_err(|e| AedbError::Validation(e.to_string()))?;

        for (_, entry) in rows {
            let order: aedb::order_book::OrderRecord = rmp_serde::from_slice(&entry.value)
                .map_err(|e| AedbError::Decode(e.to_string()))?;
            let original = decode_u256_u64(order.original_qty_be);
            let remaining = decode_u256_u64(order.remaining_qty_be);
            let filled = decode_u256_u64(order.filled_qty_be);
            if remaining + filled > original {
                return Err(AedbError::Validation(format!(
                    "quantity accounting invariant violated for {instrument}"
                )));
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
                    format!("ob:{instrument}:plqty:{}:", side as u8).as_bytes(),
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
                let price = parse_plqty_price(side, &k).ok_or_else(|| {
                    AedbError::Validation(format!(
                        "failed to parse plqty price for instrument {instrument}"
                    ))
                })?;
                from_levels.insert((side as u8, price), qty);
            }
        }

        if from_orders != from_levels {
            return Err(AedbError::Validation(format!(
                "price-level aggregates mismatch for {instrument}"
            )));
        }
    }
    Ok(())
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
    let dir = tempdir().map_err(AedbError::Io)?;
    let db = Arc::new(AedbInstance::open(db_cfg, dir.path())?);
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
            let mut lifecycle_attempted = 0usize;
            let mut lifecycle_accepted = 0usize;
            let mut lifecycle_rejected = 0usize;
            let mut rejection_breakdown = RejectionBreakdown::default();
            let mut lifecycle_rejection_breakdown = RejectionBreakdown::default();
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
                        &mut rejection_breakdown,
                        &mut max_commit_seq,
                        &mut max_finality_gap,
                    )
                    .await
                    {
                        return Err(other);
                    }
                }

                if cfg.lifecycle_every_ops > 0 && op % cfg.lifecycle_every_ops == 0 {
                    *nonce += 1;
                    let cid = format!("gtc-{owner}-{op}");
                    lifecycle_attempted += 1;
                    match db_clone
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
                        .await
                    {
                        Ok(_) => lifecycle_accepted += 1,
                        Err(err) => {
                            lifecycle_rejected += 1;
                            record_rejection_error(&mut lifecycle_rejection_breakdown, err, 1)?;
                        }
                    }
                    lifecycle_attempted += 1;
                    match db_clone
                        .order_book_cancel_by_client_id("p", "app", instrument, &cid, &owner)
                        .await
                    {
                        Ok(_) => lifecycle_accepted += 1,
                        Err(err) => {
                            lifecycle_rejected += 1;
                            record_rejection_error(&mut lifecycle_rejection_breakdown, err, 1)?;
                        }
                    }
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
                &mut rejection_breakdown,
                &mut max_commit_seq,
                &mut max_finality_gap,
            )
            .await
            {
                return Err(other);
            }
            Ok((
                accepted,
                rejected,
                lifecycle_attempted,
                lifecycle_accepted,
                lifecycle_rejected,
                lifecycle_rejection_breakdown,
                rejection_breakdown,
                max_commit_seq,
                max_finality_gap,
                latencies_us,
            ))
        }));
    }

    let mut accepted_orders = 0usize;
    let mut rejected_orders = 0usize;
    let mut lifecycle_attempted_ops = 0usize;
    let mut lifecycle_accepted_ops = 0usize;
    let mut lifecycle_rejected_ops = 0usize;
    let mut lifecycle_rejection_breakdown = RejectionBreakdown::default();
    let mut rejection_breakdown = RejectionBreakdown::default();
    let mut max_commit_seq = 0u64;
    let mut max_commit_finality_gap = 0u64;
    let mut all_latencies_us = Vec::new();
    for task in tasks {
        let task_output = task
            .await
            .map_err(|e| AedbError::Validation(format!("simulation task join failure: {e}")))?;
        let (a, r, la, lac, lr, lbreakdown, breakdown, max_seq, max_gap, mut latencies) =
            task_output?;
        accepted_orders += a;
        rejected_orders += r;
        lifecycle_attempted_ops += la;
        lifecycle_accepted_ops += lac;
        lifecycle_rejected_ops += lr;
        merge_rejection_breakdown(&mut lifecycle_rejection_breakdown, &lbreakdown);
        merge_rejection_breakdown(&mut rejection_breakdown, &breakdown);
        max_commit_seq = max_commit_seq.max(max_seq);
        max_commit_finality_gap = max_commit_finality_gap.max(max_gap);
        all_latencies_us.append(&mut latencies);
    }

    assert_book_invariants(&db, &instruments).await?;
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
    assert_eq!(
        total_rejections(&rejection_breakdown),
        rejected_orders,
        "primary rejection accounting mismatch"
    );
    assert_eq!(
        total_rejections(&lifecycle_rejection_breakdown),
        lifecycle_rejected_ops,
        "lifecycle rejection accounting mismatch"
    );
    assert_eq!(
        accepted_orders + rejected_orders,
        attempted_orders,
        "primary flow accounting mismatch"
    );
    assert_eq!(
        lifecycle_accepted_ops + lifecycle_rejected_ops,
        lifecycle_attempted_ops,
        "lifecycle flow accounting mismatch"
    );

    let simulation = SimulationReport {
        assets: cfg.assets,
        instruments,
        traders: cfg.traders,
        ops_per_trader: cfg.ops_per_trader,
        attempted_orders,
        accepted_orders,
        rejected_orders,
        lifecycle_attempted_ops,
        lifecycle_accepted_ops,
        lifecycle_rejected_ops,
        lifecycle_rejection_breakdown,
        rejection_breakdown,
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
    rejection_breakdown: &mut RejectionBreakdown,
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
        Err(err) => {
            *rejected += batch_len;
            record_rejection_error(rejection_breakdown, err, batch_len)?;
        }
    }
    Ok(())
}

fn record_rejection_error(
    rejection_breakdown: &mut RejectionBreakdown,
    err: AedbError,
    count: usize,
) -> Result<(), AedbError> {
    match err {
        AedbError::Validation(msg) => {
            record_validation_rejection(rejection_breakdown, &msg, count);
            Ok(())
        }
        AedbError::Conflict(_) => {
            rejection_breakdown.conflict += count;
            Ok(())
        }
        other => Err(other),
    }
}

fn record_validation_rejection(
    rejection_breakdown: &mut RejectionBreakdown,
    msg: &str,
    count: usize,
) {
    if msg.contains("conflict") {
        rejection_breakdown.conflict += count;
    } else if msg.contains("post_only would cross") {
        rejection_breakdown.post_only_would_cross += count;
    } else if msg.contains("fok cannot fill") {
        rejection_breakdown.fok_cannot_fill += count;
    } else if msg.contains("market order has no liquidity") {
        rejection_breakdown.market_no_liquidity += count;
    } else if msg.contains("instrument halted") {
        rejection_breakdown.instrument_halted += count;
    } else if msg.contains("nonce too low") {
        rejection_breakdown.nonce_too_low += count;
    } else if msg.contains("duplicate client_order_id") {
        rejection_breakdown.duplicate_client_order_id += count;
    } else if msg.contains("quantity violates lot size") {
        rejection_breakdown.lot_size_violation += count;
    } else if msg.contains("qty must be > 0") {
        rejection_breakdown.qty_non_positive += count;
    } else if msg.contains("price outside instrument bounds") {
        rejection_breakdown.price_out_of_bounds += count;
    } else {
        rejection_breakdown.other_validation += count;
    }
}

fn merge_rejection_breakdown(dst: &mut RejectionBreakdown, src: &RejectionBreakdown) {
    dst.conflict += src.conflict;
    dst.post_only_would_cross += src.post_only_would_cross;
    dst.fok_cannot_fill += src.fok_cannot_fill;
    dst.market_no_liquidity += src.market_no_liquidity;
    dst.instrument_halted += src.instrument_halted;
    dst.nonce_too_low += src.nonce_too_low;
    dst.duplicate_client_order_id += src.duplicate_client_order_id;
    dst.lot_size_violation += src.lot_size_violation;
    dst.qty_non_positive += src.qty_non_positive;
    dst.price_out_of_bounds += src.price_out_of_bounds;
    dst.other_validation += src.other_validation;
}

fn total_rejections(b: &RejectionBreakdown) -> usize {
    b.conflict
        + b.post_only_would_cross
        + b.fok_cannot_fill
        + b.market_no_liquidity
        + b.instrument_halted
        + b.nonce_too_low
        + b.duplicate_client_order_id
        + b.lot_size_violation
        + b.qty_non_positive
        + b.price_out_of_bounds
        + b.other_validation
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_validation_rejection_reasons() {
        let mut breakdown = RejectionBreakdown::default();
        record_validation_rejection(&mut breakdown, "post_only would cross", 2);
        record_validation_rejection(&mut breakdown, "fok cannot fill", 3);
        record_validation_rejection(&mut breakdown, "market order has no liquidity", 4);
        record_validation_rejection(&mut breakdown, "instrument halted", 5);
        record_validation_rejection(&mut breakdown, "nonce too low", 6);
        record_validation_rejection(&mut breakdown, "duplicate client_order_id", 7);
        record_validation_rejection(&mut breakdown, "quantity violates lot size", 8);
        record_validation_rejection(&mut breakdown, "qty must be > 0", 9);
        record_validation_rejection(&mut breakdown, "price outside instrument bounds", 10);
        record_validation_rejection(&mut breakdown, "some other validation", 11);

        assert_eq!(breakdown.post_only_would_cross, 2);
        assert_eq!(breakdown.fok_cannot_fill, 3);
        assert_eq!(breakdown.market_no_liquidity, 4);
        assert_eq!(breakdown.instrument_halted, 5);
        assert_eq!(breakdown.nonce_too_low, 6);
        assert_eq!(breakdown.duplicate_client_order_id, 7);
        assert_eq!(breakdown.lot_size_violation, 8);
        assert_eq!(breakdown.qty_non_positive, 9);
        assert_eq!(breakdown.price_out_of_bounds, 10);
        assert_eq!(breakdown.other_validation, 11);
    }

    #[test]
    fn merge_rejection_breakdowns() {
        let mut left = RejectionBreakdown {
            conflict: 1,
            post_only_would_cross: 2,
            fok_cannot_fill: 3,
            market_no_liquidity: 4,
            instrument_halted: 5,
            nonce_too_low: 6,
            duplicate_client_order_id: 7,
            lot_size_violation: 8,
            qty_non_positive: 9,
            price_out_of_bounds: 10,
            other_validation: 11,
        };
        let right = RejectionBreakdown {
            conflict: 10,
            post_only_would_cross: 20,
            fok_cannot_fill: 30,
            market_no_liquidity: 40,
            instrument_halted: 50,
            nonce_too_low: 60,
            duplicate_client_order_id: 70,
            lot_size_violation: 80,
            qty_non_positive: 90,
            price_out_of_bounds: 100,
            other_validation: 110,
        };
        merge_rejection_breakdown(&mut left, &right);
        assert_eq!(
            left,
            RejectionBreakdown {
                conflict: 11,
                post_only_would_cross: 22,
                fok_cannot_fill: 33,
                market_no_liquidity: 44,
                instrument_halted: 55,
                nonce_too_low: 66,
                duplicate_client_order_id: 77,
                lot_size_violation: 88,
                qty_non_positive: 99,
                price_out_of_bounds: 110,
                other_validation: 121,
            }
        );
        assert_eq!(total_rejections(&left), 726);
    }

    #[test]
    fn classify_conflict_wrapped_as_validation() {
        let mut breakdown = RejectionBreakdown::default();
        record_validation_rejection(
            &mut breakdown,
            "read set conflict: key changed under snapshot",
            5,
        );
        assert_eq!(breakdown.conflict, 5);
        assert_eq!(breakdown.other_validation, 0);
    }

    #[test]
    fn classify_conflict_error_variant() {
        let mut breakdown = RejectionBreakdown::default();
        record_rejection_error(&mut breakdown, AedbError::Conflict("rw-conflict".into()), 3)
            .expect("classification must succeed");
        assert_eq!(breakdown.conflict, 3);
    }

    #[test]
    fn decode_u256_bytes_rejects_invalid_length() {
        let err = decode_u256_bytes_to_u64(&[1, 2, 3]).expect_err("must reject short u256");
        assert!(matches!(err, AedbError::Validation(_)));
    }
}
