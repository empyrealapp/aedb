use aedb_orderbook::{
    MatchWorkload, OrderFlowProfile, SimulationConfig, TableProfile, run_hft_simulation,
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
        collect_latency: true,
        lifecycle_every_ops: 100,
        orders_per_commit: 1,
        match_workload: MatchWorkload::CrossingNearTouch,
    }
}

#[tokio::test]
async fn simulation_smoke_runs() {
    let report = run_hft_simulation(cfg(
        vec!["BTC-USD", "ETH-USD"],
        4,
        150,
        7,
        OrderFlowProfile::LimitOnlyIoc,
        TableProfile::NativeInstrument,
    ))
    .await
    .expect("simulation should run");

    assert_eq!(report.attempted_orders, 600);
    assert_eq!(
        report.accepted_orders + report.rejected_orders,
        report.attempted_orders
    );
    assert!(report.durable_head_seq >= report.max_commit_seq);
    assert!(report.visible_head_seq >= report.max_commit_seq);
}

#[tokio::test]
async fn simulation_high_throughput_per_asset_limit_only() {
    let report = run_hft_simulation(cfg(
        vec!["BTC-USD", "ETH-USD", "SOL-USD"],
        12,
        700,
        11,
        OrderFlowProfile::LimitOnlyIoc,
        TableProfile::PerAssetTable,
    ))
    .await
    .expect("high-throughput per-asset limit-only simulation should run");

    assert_eq!(report.attempted_orders, 8_400);
    assert_eq!(
        report.accepted_orders + report.rejected_orders,
        report.attempted_orders
    );
    assert!(report.accepted_orders > 0);
    assert!(report.durable_head_seq >= report.max_commit_seq);
}

#[tokio::test]
async fn simulation_high_throughput_per_asset_mixed_market_limit() {
    let report = run_hft_simulation(cfg(
        vec!["BTC-USD", "ETH-USD", "SOL-USD"],
        12,
        700,
        13,
        OrderFlowProfile::MixedMarketAndLimit,
        TableProfile::PerAssetTable,
    ))
    .await
    .expect("high-throughput per-asset mixed simulation should run");

    assert_eq!(report.attempted_orders, 8_400);
    assert_eq!(
        report.accepted_orders + report.rejected_orders,
        report.attempted_orders
    );
    assert!(report.rejected_orders > 0);
    assert!(report.durable_head_seq >= report.max_commit_seq);
}

#[tokio::test]
async fn simulation_high_throughput_multi_asset_table_limit_only() {
    let report = run_hft_simulation(cfg(
        vec!["BTC-USD", "ETH-USD", "SOL-USD", "DOGE-USD"],
        14,
        650,
        17,
        OrderFlowProfile::LimitOnlyIoc,
        TableProfile::MultiAssetTable {
            table_id: "markets".to_string(),
        },
    ))
    .await
    .expect("high-throughput multi-asset table limit-only simulation should run");

    assert_eq!(report.attempted_orders, 9_100);
    assert_eq!(
        report.accepted_orders + report.rejected_orders,
        report.attempted_orders
    );
    assert!(report.durable_head_seq >= report.max_commit_seq);
}

#[tokio::test]
async fn simulation_high_throughput_multi_asset_table_mixed_market_limit() {
    let report = run_hft_simulation(cfg(
        vec!["BTC-USD", "ETH-USD", "SOL-USD", "DOGE-USD"],
        14,
        650,
        19,
        OrderFlowProfile::MixedMarketAndLimit,
        TableProfile::MultiAssetTable {
            table_id: "markets".to_string(),
        },
    ))
    .await
    .expect("high-throughput multi-asset table mixed simulation should run");

    assert_eq!(report.attempted_orders, 9_100);
    assert_eq!(
        report.accepted_orders + report.rejected_orders,
        report.attempted_orders
    );
    assert!(report.rejected_orders > 0);
    assert!(report.durable_head_seq >= report.max_commit_seq);
}

#[tokio::test]
async fn simulation_high_contention_single_asset_mixed_market_limit() {
    let report = run_hft_simulation(cfg(
        vec!["BTC-USD"],
        20,
        1_200,
        23,
        OrderFlowProfile::MixedMarketAndLimit,
        TableProfile::PerAssetTable,
    ))
    .await
    .expect("high-contention single-asset mixed simulation should run");

    assert_eq!(report.attempted_orders, 24_000);
    assert_eq!(
        report.accepted_orders + report.rejected_orders,
        report.attempted_orders
    );
    assert!(report.rejected_orders > 0);
    assert!(report.durable_head_seq >= report.max_commit_seq);
}

#[tokio::test]
#[ignore = "extended soak test"]
async fn simulation_soak_multi_asset_mixed() {
    let report = run_hft_simulation(cfg(
        vec!["BTC-USD", "ETH-USD", "SOL-USD", "DOGE-USD"],
        24,
        2_000,
        29,
        OrderFlowProfile::MixedMarketAndLimit,
        TableProfile::MultiAssetTable {
            table_id: "markets".to_string(),
        },
    ))
    .await
    .expect("soak simulation should run");

    assert_eq!(report.attempted_orders, 48_000);
    assert_eq!(
        report.accepted_orders + report.rejected_orders,
        report.attempted_orders
    );
    assert!(report.durable_head_seq >= report.max_commit_seq);
}

#[tokio::test]
#[ignore = "extended contention soak test"]
async fn simulation_soak_single_asset_contention_limit() {
    let report = run_hft_simulation(cfg(
        vec!["BTC-USD"],
        32,
        2_000,
        31,
        OrderFlowProfile::LimitOnlyIoc,
        TableProfile::PerAssetTable,
    ))
    .await
    .expect("single-asset soak simulation should run");

    assert_eq!(report.attempted_orders, 64_000);
    assert_eq!(
        report.accepted_orders + report.rejected_orders,
        report.attempted_orders
    );
    assert!(report.durable_head_seq >= report.max_commit_seq);
}
