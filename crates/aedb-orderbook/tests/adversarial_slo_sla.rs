use aedb_orderbook::{
    MatchWorkload, OrderFlowProfile, ProfiledSimulationReport, SimulationConfig, TableProfile,
    high_throughput_simulation_config, run_hft_simulation_with_config,
};

#[derive(Clone)]
struct Scenario {
    name: &'static str,
    cfg: SimulationConfig,
}

#[derive(Clone)]
struct SloThresholds {
    min_attempted_tps: u64,
    max_p99_latency_us: u64,
    max_finality_gap: u64,
    max_primary_reject_ratio_ppm: u64,
}

fn env_or_u64(var: &str, default: u64) -> u64 {
    std::env::var(var)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn thresholds() -> SloThresholds {
    SloThresholds {
        min_attempted_tps: env_or_u64("AEDB_ORDERBOOK_SLA_MIN_ATTEMPTED_TPS", 600),
        max_p99_latency_us: env_or_u64("AEDB_ORDERBOOK_SLA_MAX_P99_US", 1_000_000),
        max_finality_gap: env_or_u64("AEDB_ORDERBOOK_SLA_MAX_FINALITY_GAP", 10_000),
        max_primary_reject_ratio_ppm: env_or_u64(
            "AEDB_ORDERBOOK_SLA_MAX_PRIMARY_REJECT_RATIO_PPM",
            900_000,
        ),
    }
}

fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "sla_per_asset_crossing_mixed",
            cfg: SimulationConfig {
                assets: vec!["BTC-USD".into(), "ETH-USD".into(), "SOL-USD".into()],
                traders: 12,
                ops_per_trader: 600,
                seed: 1_001,
                flow_profile: OrderFlowProfile::MixedMarketAndLimit,
                table_profile: TableProfile::PerAssetTable,
                collect_latency: true,
                lifecycle_every_ops: 80,
                orders_per_commit: 1,
                match_workload: MatchWorkload::CrossingNearTouch,
            },
        },
        Scenario {
            name: "sla_multi_asset_crossing_mixed",
            cfg: SimulationConfig {
                assets: vec![
                    "BTC-USD".into(),
                    "ETH-USD".into(),
                    "SOL-USD".into(),
                    "DOGE-USD".into(),
                    "AVAX-USD".into(),
                ],
                traders: 12,
                ops_per_trader: 600,
                seed: 2_002,
                flow_profile: OrderFlowProfile::MixedMarketAndLimit,
                table_profile: TableProfile::MultiAssetTable {
                    table_id: "markets".to_string(),
                },
                collect_latency: true,
                lifecycle_every_ops: 80,
                orders_per_commit: 1,
                match_workload: MatchWorkload::CrossingNearTouch,
            },
        },
        Scenario {
            name: "sla_multi_asset_no_cross_limit_only",
            cfg: SimulationConfig {
                assets: vec![
                    "BTC-USD".into(),
                    "ETH-USD".into(),
                    "SOL-USD".into(),
                    "DOGE-USD".into(),
                ],
                traders: 10,
                ops_per_trader: 500,
                seed: 3_003,
                flow_profile: OrderFlowProfile::LimitOnlyIoc,
                table_profile: TableProfile::MultiAssetTable {
                    table_id: "markets".to_string(),
                },
                collect_latency: true,
                lifecycle_every_ops: 100,
                orders_per_commit: 1,
                match_workload: MatchWorkload::NoCrossIoc,
            },
        },
    ]
}

fn assert_common_invariants(name: &str, report: &ProfiledSimulationReport) {
    let sim = &report.simulation;
    assert_eq!(
        sim.accepted_orders + sim.rejected_orders,
        sim.attempted_orders,
        "{name}: primary accounting mismatch"
    );
    assert_eq!(
        sim.lifecycle_accepted_ops + sim.lifecycle_rejected_ops,
        sim.lifecycle_attempted_ops,
        "{name}: lifecycle accounting mismatch"
    );
    assert!(
        report.zero_dropped_orders,
        "{name}: dropped orders detected"
    );
    assert!(
        sim.visible_head_seq >= sim.max_commit_seq,
        "{name}: visible head below max commit sequence"
    );
    assert!(
        sim.durable_head_seq >= sim.max_commit_seq,
        "{name}: durable head below max commit sequence"
    );
}

fn assert_slo(name: &str, report: &ProfiledSimulationReport, slo: &SloThresholds) {
    let sim = &report.simulation;
    let reject_ratio_ppm = if sim.attempted_orders == 0 {
        0
    } else {
        (sim.rejected_orders as u64)
            .saturating_mul(1_000_000)
            .saturating_div(sim.attempted_orders as u64)
    };
    assert!(
        report.attempted_ops_per_sec >= slo.min_attempted_tps,
        "{name}: attempted TPS below SLO ({})",
        report.attempted_ops_per_sec
    );
    assert!(
        report.latency.p99_us <= slo.max_p99_latency_us,
        "{name}: p99 latency above SLO ({}us)",
        report.latency.p99_us
    );
    assert!(
        report.max_commit_finality_gap <= slo.max_finality_gap,
        "{name}: finality gap above SLO ({})",
        report.max_commit_finality_gap
    );
    assert!(
        reject_ratio_ppm <= slo.max_primary_reject_ratio_ppm,
        "{name}: reject ratio above SLO ({} ppm)",
        reject_ratio_ppm
    );
}

#[tokio::test]
async fn adversarial_slo_sla_gates() {
    let db_cfg = high_throughput_simulation_config();
    let slo = thresholds();
    for scenario in scenarios() {
        let report = run_hft_simulation_with_config(scenario.cfg.clone(), db_cfg.clone())
            .await
            .unwrap_or_else(|e| panic!("{} failed to run: {e}", scenario.name));
        assert_common_invariants(scenario.name, &report);
        assert_slo(scenario.name, &report, &slo);
    }
}
