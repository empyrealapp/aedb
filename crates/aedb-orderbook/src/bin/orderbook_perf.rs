use aedb::config::DurabilityMode;
use aedb_orderbook::{
    MatchWorkload, OrderFlowProfile, SimulationConfig, TableProfile,
    run_hft_simulation_with_config, tuned_simulation_config_with_durability,
};

#[derive(Debug, Clone)]
struct Scenario {
    name: &'static str,
    assets: Vec<String>,
    traders: usize,
    ops_per_trader: usize,
    flow: OrderFlowProfile,
    table: TableProfile,
    durability: DurabilityMode,
    collect_latency: bool,
    lifecycle_every_ops: usize,
    orders_per_commit: usize,
    match_workload: MatchWorkload,
}

fn scenario_matrix(scale: &str) -> Vec<Scenario> {
    if scale == "max" {
        let assets = vec![
            "BTC-USD".into(),
            "ETH-USD".into(),
            "SOL-USD".into(),
            "DOGE-USD".into(),
            "XRP-USD".into(),
            "ADA-USD".into(),
            "LTC-USD".into(),
            "BNB-USD".into(),
        ];
        let mut out = Vec::new();
        for traders in [8usize, 12, 16] {
            for orders_per_commit in [16usize, 32, 64] {
                out.push(Scenario {
                    name: Box::leak(
                        format!("max_tps_batch_limit_multi_asset_t{traders}_b{orders_per_commit}")
                            .into_boxed_str(),
                    ),
                    assets: assets.clone(),
                    traders,
                    ops_per_trader: 1_000,
                    flow: OrderFlowProfile::LimitOnlyIoc,
                    table: TableProfile::MultiAssetTable {
                        table_id: "markets".to_string(),
                    },
                    durability: DurabilityMode::Batch,
                    collect_latency: false,
                    lifecycle_every_ops: 0,
                    orders_per_commit,
                    match_workload: MatchWorkload::NoCrossIoc,
                });
            }
        }
        return out;
    }
    let (traders, ops) = if scale == "stress" {
        (20, 1_000)
    } else {
        (10, 400)
    };
    vec![
        Scenario {
            name: "per_asset_limit_full",
            assets: vec!["BTC-USD".into(), "ETH-USD".into(), "SOL-USD".into()],
            traders,
            ops_per_trader: ops,
            flow: OrderFlowProfile::LimitOnlyIoc,
            table: TableProfile::PerAssetTable,
            durability: DurabilityMode::Full,
            collect_latency: true,
            lifecycle_every_ops: 100,
            orders_per_commit: 1,
            match_workload: MatchWorkload::CrossingNearTouch,
        },
        Scenario {
            name: "per_asset_limit_batch",
            assets: vec!["BTC-USD".into(), "ETH-USD".into(), "SOL-USD".into()],
            traders,
            ops_per_trader: ops,
            flow: OrderFlowProfile::LimitOnlyIoc,
            table: TableProfile::PerAssetTable,
            durability: DurabilityMode::Batch,
            collect_latency: true,
            lifecycle_every_ops: 100,
            orders_per_commit: 1,
            match_workload: MatchWorkload::CrossingNearTouch,
        },
        Scenario {
            name: "per_asset_mixed_full",
            assets: vec!["BTC-USD".into(), "ETH-USD".into(), "SOL-USD".into()],
            traders,
            ops_per_trader: ops,
            flow: OrderFlowProfile::MixedMarketAndLimit,
            table: TableProfile::PerAssetTable,
            durability: DurabilityMode::Full,
            collect_latency: true,
            lifecycle_every_ops: 100,
            orders_per_commit: 1,
            match_workload: MatchWorkload::CrossingNearTouch,
        },
        Scenario {
            name: "per_asset_mixed_batch",
            assets: vec!["BTC-USD".into(), "ETH-USD".into(), "SOL-USD".into()],
            traders,
            ops_per_trader: ops,
            flow: OrderFlowProfile::MixedMarketAndLimit,
            table: TableProfile::PerAssetTable,
            durability: DurabilityMode::Batch,
            collect_latency: true,
            lifecycle_every_ops: 100,
            orders_per_commit: 1,
            match_workload: MatchWorkload::CrossingNearTouch,
        },
        Scenario {
            name: "multi_asset_mixed_full",
            assets: vec![
                "BTC-USD".into(),
                "ETH-USD".into(),
                "SOL-USD".into(),
                "DOGE-USD".into(),
            ],
            traders,
            ops_per_trader: ops,
            flow: OrderFlowProfile::MixedMarketAndLimit,
            table: TableProfile::MultiAssetTable {
                table_id: "markets".to_string(),
            },
            durability: DurabilityMode::Full,
            collect_latency: true,
            lifecycle_every_ops: 100,
            orders_per_commit: 1,
            match_workload: MatchWorkload::CrossingNearTouch,
        },
        Scenario {
            name: "multi_asset_mixed_batch",
            assets: vec![
                "BTC-USD".into(),
                "ETH-USD".into(),
                "SOL-USD".into(),
                "DOGE-USD".into(),
            ],
            traders,
            ops_per_trader: ops,
            flow: OrderFlowProfile::MixedMarketAndLimit,
            table: TableProfile::MultiAssetTable {
                table_id: "markets".to_string(),
            },
            durability: DurabilityMode::Batch,
            collect_latency: true,
            lifecycle_every_ops: 100,
            orders_per_commit: 1,
            match_workload: MatchWorkload::CrossingNearTouch,
        },
        Scenario {
            name: "single_asset_contention_mixed_full",
            assets: vec!["BTC-USD".into()],
            traders: traders.saturating_mul(2),
            ops_per_trader: ops,
            flow: OrderFlowProfile::MixedMarketAndLimit,
            table: TableProfile::PerAssetTable,
            durability: DurabilityMode::Full,
            collect_latency: true,
            lifecycle_every_ops: 100,
            orders_per_commit: 1,
            match_workload: MatchWorkload::CrossingNearTouch,
        },
        Scenario {
            name: "single_asset_contention_mixed_batch",
            assets: vec!["BTC-USD".into()],
            traders: traders.saturating_mul(2),
            ops_per_trader: ops,
            flow: OrderFlowProfile::MixedMarketAndLimit,
            table: TableProfile::PerAssetTable,
            durability: DurabilityMode::Batch,
            collect_latency: true,
            lifecycle_every_ops: 100,
            orders_per_commit: 1,
            match_workload: MatchWorkload::CrossingNearTouch,
        },
    ]
}

fn cfg_for_scenario(s: &Scenario) -> (SimulationConfig, aedb::config::AedbConfig) {
    let db_cfg = tuned_simulation_config_with_durability(s.durability);
    let sim_cfg = SimulationConfig {
        assets: s.assets.clone(),
        traders: s.traders,
        ops_per_trader: s.ops_per_trader,
        seed: 42,
        flow_profile: s.flow.clone(),
        table_profile: s.table.clone(),
        collect_latency: s.collect_latency,
        lifecycle_every_ops: s.lifecycle_every_ops,
        orders_per_commit: s.orders_per_commit,
        match_workload: s.match_workload.clone(),
    };
    (sim_cfg, db_cfg)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let scale = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "quick".to_string());
    if scale != "quick" && scale != "stress" && scale != "max" {
        eprintln!("usage: cargo run -p aedb-orderbook --bin orderbook_perf [quick|stress|max]");
        std::process::exit(2);
    }

    println!(
        "scenario,attempted,accepted,rejected,elapsed_ms,attempted_ops_s,accepted_ops_s,rejected_ops_s,lat_avg_us,lat_p50_us,lat_p95_us,lat_p99_us,lat_max_us,max_finality_gap,visible_head,durable_head,zero_dropped,durability,lifecycle_attempted,lifecycle_accepted,lifecycle_rejected,primary_reject_conflict,primary_reject_post_only,primary_reject_fok,primary_reject_no_liquidity,primary_reject_nonce,primary_reject_duplicate_cid,primary_reject_other_validation,lifecycle_reject_conflict,lifecycle_reject_post_only,lifecycle_reject_fok,lifecycle_reject_no_liquidity,lifecycle_reject_nonce,lifecycle_reject_duplicate_cid,lifecycle_reject_other_validation"
    );
    for scenario in scenario_matrix(&scale) {
        let (sim_cfg, db_cfg) = cfg_for_scenario(&scenario);
        let report = match run_hft_simulation_with_config(sim_cfg, db_cfg).await {
            Ok(report) => report,
            Err(e) => {
                eprintln!("scenario {} failed: {e}", scenario.name);
                std::process::exit(1);
            }
        };
        println!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            scenario.name,
            report.simulation.attempted_orders,
            report.simulation.accepted_orders,
            report.simulation.rejected_orders,
            report.elapsed_ms,
            report.attempted_ops_per_sec,
            report.accepted_ops_per_sec,
            report.rejected_ops_per_sec,
            report.latency.avg_us,
            report.latency.p50_us,
            report.latency.p95_us,
            report.latency.p99_us,
            report.latency.max_us,
            report.max_commit_finality_gap,
            report.simulation.visible_head_seq,
            report.simulation.durable_head_seq,
            report.zero_dropped_orders,
            report.durability_mode,
            report.simulation.lifecycle_attempted_ops,
            report.simulation.lifecycle_accepted_ops,
            report.simulation.lifecycle_rejected_ops,
            report.simulation.rejection_breakdown.conflict,
            report.simulation.rejection_breakdown.post_only_would_cross,
            report.simulation.rejection_breakdown.fok_cannot_fill,
            report.simulation.rejection_breakdown.market_no_liquidity,
            report.simulation.rejection_breakdown.nonce_too_low,
            report
                .simulation
                .rejection_breakdown
                .duplicate_client_order_id,
            report.simulation.rejection_breakdown.other_validation,
            report.simulation.lifecycle_rejection_breakdown.conflict,
            report
                .simulation
                .lifecycle_rejection_breakdown
                .post_only_would_cross,
            report
                .simulation
                .lifecycle_rejection_breakdown
                .fok_cannot_fill,
            report
                .simulation
                .lifecycle_rejection_breakdown
                .market_no_liquidity,
            report
                .simulation
                .lifecycle_rejection_breakdown
                .nonce_too_low,
            report
                .simulation
                .lifecycle_rejection_breakdown
                .duplicate_client_order_id,
            report
                .simulation
                .lifecycle_rejection_breakdown
                .other_validation,
        );
    }
}
