use aedb_orderbook::{
    MatchWorkload, OrderFlowProfile, SimulationConfig, TableProfile,
    high_throughput_simulation_config, run_hft_simulation_with_config,
};

fn cfg(
    assets: Vec<&str>,
    traders: usize,
    ops_per_trader: usize,
    seed: u64,
    flow_profile: OrderFlowProfile,
    table_profile: TableProfile,
) -> SimulationConfig {
    SimulationConfig {
        assets: assets.into_iter().map(|s| s.to_string()).collect(),
        traders,
        ops_per_trader,
        seed,
        flow_profile,
        table_profile,
        collect_latency: false,
        lifecycle_every_ops: 50,
        orders_per_commit: 1,
        match_workload: MatchWorkload::CrossingNearTouch,
    }
}

#[tokio::test]
async fn correctness_matrix_multiple_seeds_and_layouts() {
    let scenarios = vec![
        (
            vec!["BTC-USD", "ETH-USD", "SOL-USD"],
            OrderFlowProfile::LimitOnlyIoc,
            TableProfile::PerAssetTable,
        ),
        (
            vec!["BTC-USD", "ETH-USD", "SOL-USD", "DOGE-USD"],
            OrderFlowProfile::MixedMarketAndLimit,
            TableProfile::MultiAssetTable {
                table_id: "markets".to_string(),
            },
        ),
    ];

    for seed in [3_u64, 7, 11, 19, 23] {
        for (assets, flow, table) in &scenarios {
            let report = run_hft_simulation_with_config(
                cfg(assets.clone(), 8, 300, seed, flow.clone(), table.clone()),
                high_throughput_simulation_config(),
            )
            .await
            .expect("simulation matrix run should succeed");

            assert!(report.zero_dropped_orders, "no dropped orders allowed");
            assert_eq!(
                report.simulation.accepted_orders + report.simulation.rejected_orders,
                report.simulation.attempted_orders
            );
            assert!(
                report.simulation.visible_head_seq >= report.simulation.max_commit_seq,
                "visible head must include all accepted commits"
            );
            assert!(
                report.simulation.durable_head_seq >= report.simulation.max_commit_seq,
                "durable head must include all accepted commits after fsync"
            );
        }
    }
}
