use aedb_orderbook::{
    MatchWorkload, OrderFlowProfile, SimulationConfig, TableProfile,
    high_throughput_simulation_config, run_hft_simulation_with_config,
};
use proptest::prelude::*;

fn pick_assets(asset_count: usize) -> Vec<String> {
    let universe = ["BTC-USD", "ETH-USD", "SOL-USD", "DOGE-USD", "AVAX-USD"];
    universe
        .iter()
        .take(asset_count.max(1).min(universe.len()))
        .map(|s| s.to_string())
        .collect()
}

fn table_profile(multi_asset: bool) -> TableProfile {
    if multi_asset {
        TableProfile::MultiAssetTable {
            table_id: "markets".to_string(),
        }
    } else {
        TableProfile::PerAssetTable
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 8,
        max_local_rejects: 0,
        .. ProptestConfig::default()
    })]
    #[test]
    fn randomized_invariants_hold_under_contention(
        seed in any::<u64>(),
        traders in 4usize..10,
        ops_per_trader in 120usize..320,
        asset_count in 1usize..5,
        mixed_flow in any::<bool>(),
        multi_asset in any::<bool>(),
        crossing_workload in any::<bool>(),
        lifecycle_every_ops in 30usize..140,
        orders_per_commit in 1usize..4
    ) {
        let flow_profile = if mixed_flow {
            OrderFlowProfile::MixedMarketAndLimit
        } else {
            OrderFlowProfile::LimitOnlyIoc
        };
        let workload = if crossing_workload {
            MatchWorkload::CrossingNearTouch
        } else {
            MatchWorkload::NoCrossIoc
        };
        let cfg = SimulationConfig {
            assets: pick_assets(asset_count),
            traders,
            ops_per_trader,
            seed,
            flow_profile,
            table_profile: table_profile(multi_asset),
            collect_latency: false,
            lifecycle_every_ops,
            orders_per_commit,
            match_workload: workload,
        };

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let report = rt
            .block_on(async { run_hft_simulation_with_config(cfg, high_throughput_simulation_config()).await })
            .expect("simulation should succeed");

        let sim = report.simulation;
        prop_assert_eq!(
            sim.accepted_orders + sim.rejected_orders,
            sim.attempted_orders
        );
        prop_assert_eq!(
            sim.lifecycle_accepted_ops + sim.lifecycle_rejected_ops,
            sim.lifecycle_attempted_ops
        );
        prop_assert!(report.zero_dropped_orders);
        prop_assert!(sim.visible_head_seq >= sim.max_commit_seq);
        prop_assert!(sim.durable_head_seq >= sim.max_commit_seq);
    }
}
