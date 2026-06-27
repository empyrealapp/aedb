use crate::engine_interface::{EffectBatch, EffectBatchCommitResult, EffectEvent};
use crate::{AedbConfig, AedbInstance, ConsistencyMode};
use tempfile::tempdir;

async fn open() -> (tempfile::TempDir, AedbInstance) {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "app").await.expect("scope");
    (dir, db)
}

#[tokio::test]
async fn inspect_commit_lists_events_of_that_commit() {
    let (_dir, db) = open().await;
    db.emit_event("arcana", "app", "deposit", "before".into(), "{}".into())
        .await
        .unwrap();
    let res = db
        .commit_effect_batch(
            "arcana",
            "app",
            EffectBatch {
                preconditions: Vec::new(),
                effects: Vec::new(),
                events: vec![
                    EffectEvent {
                        event_name: "trade".into(),
                        event_key: "t1".into(),
                        data_json: "{}".into(),
                    },
                    EffectEvent {
                        event_name: "nav".into(),
                        event_key: "n1".into(),
                        data_json: "{}".into(),
                    },
                ],
            },
        )
        .await
        .unwrap();
    let seq = match res {
        EffectBatchCommitResult::Applied(c) => c.commit_seq,
        EffectBatchCommitResult::Rejected(_) => panic!("rejected"),
    };

    let trace = db
        .inspect_commit(seq, ConsistencyMode::AtLatest)
        .await
        .unwrap();
    assert_eq!(trace.commit_seq, seq);
    let mut keys: Vec<_> = trace.events.iter().map(|e| e.event_key.clone()).collect();
    keys.sort();
    assert_eq!(keys, vec!["n1", "t1"]);
}

#[tokio::test]
async fn find_events_by_key_locates_indexing_commit() {
    let (_dir, db) = open().await;
    db.emit_event(
        "arcana",
        "app",
        "share_price",
        "alice".into(),
        r#"{"px":"1.00"}"#.into(),
    )
    .await
    .unwrap();
    db.emit_event(
        "arcana",
        "app",
        "share_price",
        "bob".into(),
        r#"{"px":"2.00"}"#.into(),
    )
    .await
    .unwrap();
    db.emit_event(
        "arcana",
        "app",
        "share_price",
        "alice".into(),
        r#"{"px":"1.50"}"#.into(),
    )
    .await
    .unwrap();

    let hits = db
        .find_events_by_key("share_price", "alice", 10, ConsistencyMode::AtLatest)
        .await
        .unwrap();
    assert_eq!(hits.len(), 2);
    // Newest first.
    assert_eq!(hits[0].payload_json, r#"{"px":"1.50"}"#);
    assert_eq!(hits[1].payload_json, r#"{"px":"1.00"}"#);
}

#[tokio::test]
async fn event_log_summary_reports_range() {
    let (_dir, db) = open().await;
    let mut seqs = Vec::new();
    for i in 0..5 {
        seqs.push(
            db.emit_event("arcana", "app", "deposit", format!("d{i}"), "{}".into())
                .await
                .unwrap()
                .commit_seq,
        );
    }
    let summary = db
        .event_log_summary(ConsistencyMode::AtLatest)
        .await
        .unwrap();
    assert_eq!(summary.count, 5);
    assert_eq!(summary.oldest_seq, Some(seqs[0]));
    assert_eq!(summary.newest_seq, Some(seqs[4]));
}

#[tokio::test]
async fn inspect_namespace_aggregates_monitors_and_effects() {
    let (_dir, db) = open().await;
    // One pending effect, one committed effect.
    db.begin_effect("arcana", "app", "pending-1").await.unwrap();
    db.complete_effect("arcana", "app", "done-1", "{}".into(), vec![])
        .await
        .unwrap();
    // One monitor.
    db.monitor_acquire_lease("arcana", "app", "erc20", "w1", 60_000_000)
        .await
        .unwrap();

    let view = db
        .inspect_namespace("arcana", "app", ConsistencyMode::AtLatest)
        .await
        .unwrap();
    assert_eq!(view.effect_pending, 1);
    assert_eq!(view.effect_committed, 1);
    assert_eq!(view.monitors.len(), 1);
    assert_eq!(view.monitors[0].monitor, "erc20");
    assert_eq!(view.monitors[0].owner_id.as_deref(), Some("w1"));
}

#[tokio::test]
async fn empty_log_summary() {
    let (_dir, db) = open().await;
    let summary = db
        .event_log_summary(ConsistencyMode::AtLatest)
        .await
        .unwrap();
    assert_eq!(summary.count, 0);
    assert_eq!(summary.oldest_seq, None);
}
