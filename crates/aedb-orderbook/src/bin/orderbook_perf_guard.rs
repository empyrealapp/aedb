use aedb::config::DurabilityMode;
use aedb_orderbook::{
    MatchWorkload, OrderFlowProfile, SimulationConfig, TableProfile,
    run_hft_simulation_with_config, tuned_simulation_config_with_durability,
};

#[derive(Clone)]
struct Scenario {
    name: &'static str,
    cfg: SimulationConfig,
    min_attempted_tps: u64,
}

fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "per_asset_mixed_batch",
            cfg: SimulationConfig {
                assets: vec!["BTC-USD".into(), "ETH-USD".into(), "SOL-USD".into()],
                traders: 10,
                ops_per_trader: 400,
                seed: 42,
                flow_profile: OrderFlowProfile::MixedMarketAndLimit,
                table_profile: TableProfile::PerAssetTable,
                collect_latency: false,
                lifecycle_every_ops: 100,
                orders_per_commit: 1,
                match_workload: MatchWorkload::CrossingNearTouch,
            },
            min_attempted_tps: 3_500,
        },
        Scenario {
            name: "multi_asset_mixed_batch",
            cfg: SimulationConfig {
                assets: vec![
                    "BTC-USD".into(),
                    "ETH-USD".into(),
                    "SOL-USD".into(),
                    "DOGE-USD".into(),
                ],
                traders: 10,
                ops_per_trader: 400,
                seed: 42,
                flow_profile: OrderFlowProfile::MixedMarketAndLimit,
                table_profile: TableProfile::MultiAssetTable {
                    table_id: "markets".to_string(),
                },
                collect_latency: false,
                lifecycle_every_ops: 100,
                orders_per_commit: 1,
                match_workload: MatchWorkload::CrossingNearTouch,
            },
            min_attempted_tps: 3_000,
        },
        Scenario {
            name: "single_asset_contention_mixed_batch",
            cfg: SimulationConfig {
                assets: vec!["BTC-USD".into()],
                traders: 20,
                ops_per_trader: 400,
                seed: 42,
                flow_profile: OrderFlowProfile::MixedMarketAndLimit,
                table_profile: TableProfile::PerAssetTable,
                collect_latency: false,
                lifecycle_every_ops: 100,
                orders_per_commit: 1,
                match_workload: MatchWorkload::CrossingNearTouch,
            },
            min_attempted_tps: 1_800,
        },
    ]
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let db_cfg = tuned_simulation_config_with_durability(DurabilityMode::Batch);
    let mut failures = Vec::new();

    println!(
        "scenario,attempted_tps,accepted_tps,rejected_tps,min_attempted_tps,zero_dropped,max_finality_gap,lifecycle_attempted,lifecycle_accepted,lifecycle_rejected,primary_reject_conflict,primary_reject_post_only,primary_reject_fok,primary_reject_no_liquidity,primary_reject_nonce,primary_reject_duplicate_cid,primary_reject_other_validation,lifecycle_reject_conflict,lifecycle_reject_post_only,lifecycle_reject_fok,lifecycle_reject_no_liquidity,lifecycle_reject_nonce,lifecycle_reject_duplicate_cid,lifecycle_reject_other_validation"
    );
    for s in scenarios() {
        let report = match run_hft_simulation_with_config(s.cfg.clone(), db_cfg.clone()).await {
            Ok(report) => report,
            Err(e) => {
                eprintln!("scenario {} failed to run: {e}", s.name);
                std::process::exit(1);
            }
        };
        println!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            s.name,
            report.attempted_ops_per_sec,
            report.accepted_ops_per_sec,
            report.rejected_ops_per_sec,
            s.min_attempted_tps,
            report.zero_dropped_orders,
            report.max_commit_finality_gap,
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
                .other_validation
        );
        if report.attempted_ops_per_sec < s.min_attempted_tps || !report.zero_dropped_orders {
            failures.push(format!(
                "{}: attempted_tps={} (min={}), zero_dropped={}",
                s.name,
                report.attempted_ops_per_sec,
                s.min_attempted_tps,
                report.zero_dropped_orders
            ));
        }
    }

    if !failures.is_empty() {
        eprintln!("performance/correctness guard failed:");
        for f in failures {
            eprintln!("  - {f}");
        }
        std::process::exit(1);
    }
}
