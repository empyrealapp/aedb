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
        "scenario,attempted_tps,accepted_tps,rejected_tps,min_attempted_tps,zero_dropped,max_finality_gap"
    );
    for s in scenarios() {
        let report = run_hft_simulation_with_config(s.cfg.clone(), db_cfg.clone())
            .await
            .unwrap_or_else(|e| panic!("scenario {} failed to run: {e}", s.name));
        println!(
            "{},{},{},{},{},{},{}",
            s.name,
            report.attempted_ops_per_sec,
            report.accepted_ops_per_sec,
            report.rejected_ops_per_sec,
            s.min_attempted_tps,
            report.zero_dropped_orders,
            report.max_commit_finality_gap
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
