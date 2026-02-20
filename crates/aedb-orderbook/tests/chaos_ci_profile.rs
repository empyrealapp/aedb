use aedb_orderbook::{
    MatchWorkload, OrderFlowProfile, RejectionBreakdown, SimulationConfig, TableProfile,
    high_throughput_simulation_config, run_hft_simulation_with_config,
};

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

#[derive(Clone)]
struct ChaosScenario {
    name: &'static str,
    cfg: SimulationConfig,
    min_attempted_tps: u64,
    max_finality_gap: u64,
}

fn scenarios() -> Vec<ChaosScenario> {
    vec![
        ChaosScenario {
            name: "ci_per_asset_mixed",
            cfg: SimulationConfig {
                assets: vec!["BTC-USD".into(), "ETH-USD".into(), "SOL-USD".into()],
                traders: 8,
                ops_per_trader: 300,
                seed: 101,
                flow_profile: OrderFlowProfile::MixedMarketAndLimit,
                table_profile: TableProfile::PerAssetTable,
                collect_latency: true,
                lifecycle_every_ops: 100,
                orders_per_commit: 1,
                match_workload: MatchWorkload::CrossingNearTouch,
            },
            min_attempted_tps: 500,
            max_finality_gap: 5_000,
        },
        ChaosScenario {
            name: "ci_multi_asset_mixed",
            cfg: SimulationConfig {
                assets: vec![
                    "BTC-USD".into(),
                    "ETH-USD".into(),
                    "SOL-USD".into(),
                    "DOGE-USD".into(),
                ],
                traders: 8,
                ops_per_trader: 300,
                seed: 202,
                flow_profile: OrderFlowProfile::MixedMarketAndLimit,
                table_profile: TableProfile::MultiAssetTable {
                    table_id: "markets".to_string(),
                },
                collect_latency: true,
                lifecycle_every_ops: 100,
                orders_per_commit: 1,
                match_workload: MatchWorkload::CrossingNearTouch,
            },
            min_attempted_tps: 500,
            max_finality_gap: 5_000,
        },
    ]
}

#[tokio::test]
async fn chaos_ci_profile_invariants_and_bounds() {
    let db_cfg = high_throughput_simulation_config();
    for s in scenarios() {
        let report = run_hft_simulation_with_config(s.cfg.clone(), db_cfg.clone())
            .await
            .unwrap_or_else(|e| panic!("{} failed to run: {e}", s.name));

        let sim = &report.simulation;
        assert_eq!(
            sim.attempted_orders,
            s.cfg.traders * s.cfg.ops_per_trader,
            "{} attempted mismatch",
            s.name
        );
        assert_eq!(
            sim.accepted_orders + sim.rejected_orders,
            sim.attempted_orders,
            "{} primary accounting mismatch",
            s.name
        );
        assert_eq!(
            sim.lifecycle_accepted_ops + sim.lifecycle_rejected_ops,
            sim.lifecycle_attempted_ops,
            "{} lifecycle accounting mismatch",
            s.name
        );
        assert_eq!(
            total_rejections(&sim.rejection_breakdown),
            sim.rejected_orders,
            "{} primary rejection breakdown mismatch",
            s.name
        );
        assert_eq!(
            total_rejections(&sim.lifecycle_rejection_breakdown),
            sim.lifecycle_rejected_ops,
            "{} lifecycle rejection breakdown mismatch",
            s.name
        );
        assert!(
            report.zero_dropped_orders,
            "{} dropped orders detected",
            s.name
        );
        assert!(
            sim.visible_head_seq >= sim.max_commit_seq,
            "{} visible head below commit seq",
            s.name
        );
        assert!(
            sim.durable_head_seq >= sim.max_commit_seq,
            "{} durable head below commit seq",
            s.name
        );
        assert!(
            report.max_commit_finality_gap <= s.max_finality_gap,
            "{} finality gap too large: {}",
            s.name,
            report.max_commit_finality_gap
        );
        assert!(
            report.attempted_ops_per_sec >= s.min_attempted_tps,
            "{} attempted TPS too low: {}",
            s.name,
            report.attempted_ops_per_sec
        );
        assert!(
            report.latency.p99_us > 0 && report.latency.p99_us < 1_000_000,
            "{} p99 latency out of bounds: {}",
            s.name,
            report.latency.p99_us
        );
    }
}
