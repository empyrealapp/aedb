//! End-to-end tour of the event & effect primitives.
//!
//!   cargo run --example event_effect_primitives
//!
//! Walks the flow a monitor-driven app actually uses: emit events, query them,
//! perform an exactly-once external effect, maintain bounded projections, drive
//! a leased monitor checkpoint, compact old events, and inspect the result.
//! See docs/EVENT_AND_EFFECT_PRIMITIVES.md for the reference.

use aedb::commit::validation::Mutation;
use aedb::config::AedbConfig;
use aedb::query::plan::ConsistencyMode;
use aedb::{AedbInstance, EventQuery, EventRetentionPolicy, MonitorCheckpointUpdate};
use std::error::Error;
use tempfile::tempdir;

const P: &str = "arcana";
const S: &str = "app";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db = AedbInstance::open(AedbConfig::default(), dir.path())?;
    db.create_project(P).await?;
    db.create_scope(P, S).await?;

    // 1. Emit events (the canonical, append-only history).
    for i in 0..5 {
        db.emit_event(
            P,
            S,
            "deposit",
            format!("dep-{i}"),
            format!(r#"{{"recipient":"0xabc","amount":{}}}"#, (i + 1) * 100),
        )
        .await?;
    }

    // 1b. Query them: by type, by payload field, newest-first, paginated.
    let page = db
        .query_events(
            &EventQuery::new()
                .topic("deposit")
                .where_field("recipient", "0xabc")
                .limit(3),
            ConsistencyMode::AtLatest,
        )
        .await?;
    println!(
        "events page: {} returned, next cursor = {:?}",
        page.events.len(),
        page.next_commit_seq
    );
    let latest = db
        .latest_events_by_topic("deposit", 2, ConsistencyMode::AtLatest)
        .await?;
    println!(
        "latest 2 deposits: {:?}",
        latest.iter().map(|e| &e.event_key).collect::<Vec<_>>()
    );

    // 2. Exactly-once external effect: claim a dedupe key, do the side effect,
    //    then complete (credit + marker commit atomically). Replays are no-ops.
    let dedupe = "nado-deposit-0xabc-block-100";
    match db.begin_effect(P, S, dedupe).await? {
        aedb::EffectClaim::Fresh => {
            // ... perform the external submission here ...
            let credit = vec![Mutation::KvSet {
                project_id: P.into(),
                scope_id: S.into(),
                key: b"balance:0xabc".to_vec(),
                value: b"500".to_vec(),
            }];
            db.complete_effect(P, S, dedupe, r#"{"tx":"0xdead"}"#.into(), credit)
                .await?;
            println!("effect applied exactly once");
        }
        other => println!("effect already handled: {other:?}"),
    }
    // Replaying complete_effect does nothing.
    let replay = db
        .complete_effect(P, S, dedupe, "{}".into(), vec![])
        .await?;
    println!("replay outcome: {replay:?}");

    // 3. Bounded projections: a NAV ring (keep last 3) + latest protocol status.
    for nav in [100, 101, 102, 103] {
        db.series_append(P, S, "nav", &format!(r#"{{"nav":{nav}}}"#), 3)
            .await?;
    }
    let nav_series = db
        .series_read(P, S, "nav", 10, true, ConsistencyMode::AtLatest)
        .await?;
    println!("NAV series (newest first, bounded to 3): {nav_series:?}");
    db.projection_put(P, S, "status", "protocol", r#"{"paused":false}"#)
        .await?;
    println!(
        "latest protocol status: {:?}",
        db.projection_get(P, S, "status", "protocol", ConsistencyMode::AtLatest)
            .await?
    );

    // 4. Monitor: acquire a lease, advance the checkpoint under the fencing token.
    let lease = match db
        .monitor_acquire_lease(P, S, "erc20", "worker-1", 30_000_000)
        .await?
    {
        aedb::LeaseOutcome::Acquired(l) => l,
        aedb::LeaseOutcome::Held { owner_id, .. } => {
            println!("another worker holds the lease: {owner_id}");
            return Ok(());
        }
    };
    db.monitor_advance_checkpoint(
        P,
        S,
        "erc20",
        lease.fencing_token,
        MonitorCheckpointUpdate {
            start_block: Some(100),
            last_scanned_block: Some(150),
            last_processed_cursor: Some("page-7".into()),
            ..Default::default()
        },
        vec![],
    )
    .await?;
    let mon = db
        .monitor_status(P, S, "erc20", ConsistencyMode::AtLatest)
        .await?
        .unwrap();
    println!(
        "monitor erc20: scanned block {:?}, owner {:?}, token {}",
        mon.last_scanned_block, mon.owner_id, mon.fencing_token
    );

    // 5. Retention: keep the last 2 events (no reactive consumers here).
    let report = db
        .compact_events(&EventRetentionPolicy {
            keep_last: Some(2),
            respect_processor_checkpoints: false,
            ..Default::default()
        })
        .await?;
    println!(
        "compaction: pruned {} (archived seqs {:?}..={:?})",
        report.pruned_count, report.archived_min_seq, report.archived_max_seq
    );

    // 6. Inspection: a one-call dashboard + log summary.
    let summary = db.event_log_summary(ConsistencyMode::AtLatest).await?;
    println!(
        "event log: {} events, seqs {:?}..={:?}",
        summary.count, summary.oldest_seq, summary.newest_seq
    );
    let view = db
        .inspect_namespace(P, S, ConsistencyMode::AtLatest)
        .await?;
    println!(
        "namespace {}/{}: {} monitor(s), effects pending={} committed={}",
        view.project_id,
        view.scope_id,
        view.monitors.len(),
        view.effect_pending,
        view.effect_committed
    );

    db.shutdown().await?;
    Ok(())
}
