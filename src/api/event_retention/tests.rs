use std::time::{Duration, Instant};

use crate::{
    AedbConfig, AedbInstance, ConsistencyMode, EventQuery, EventRetentionPolicy,
    ReactiveProcessorOptions,
};
use tempfile::tempdir;

async fn open() -> (tempfile::TempDir, AedbInstance) {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "app").await.expect("scope");
    (dir, db)
}

async fn emit_n(db: &AedbInstance, n: usize) -> Vec<u64> {
    let mut seqs = Vec::new();
    for i in 0..n {
        seqs.push(
            db.emit_event("arcana", "app", "deposit", format!("d{i}"), "{}".into())
                .await
                .unwrap()
                .commit_seq,
        );
    }
    seqs
}

async fn remaining_keys(db: &AedbInstance) -> Vec<String> {
    db.query_events(
        &EventQuery::new().topic("deposit").limit(1000),
        ConsistencyMode::AtLatest,
    )
    .await
    .unwrap()
    .events
    .into_iter()
    .map(|e| e.event_key)
    .collect()
}

#[tokio::test]
async fn keep_last_prunes_oldest() {
    let (_dir, db) = open().await;
    emit_n(&db, 10).await;

    let report = db
        .compact_events(&EventRetentionPolicy {
            keep_last: Some(3),
            respect_processor_checkpoints: false,
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(report.pruned_count, 7);
    assert!(!report.more_remaining);
    assert_eq!(remaining_keys(&db).await, vec!["d7", "d8", "d9"]);
}

#[tokio::test]
async fn empty_policy_prunes_nothing() {
    let (_dir, db) = open().await;
    emit_n(&db, 5).await;
    let report = db
        .compact_events(&EventRetentionPolicy {
            respect_processor_checkpoints: false,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(report.pruned_count, 0);
    assert_eq!(remaining_keys(&db).await.len(), 5);
}

#[tokio::test]
async fn max_age_zero_prunes_all_committed() {
    let (_dir, db) = open().await;
    emit_n(&db, 5).await;
    // Ensure all events are strictly older than 'now'.
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let report = db
        .compact_events(&EventRetentionPolicy {
            max_age_micros: Some(1),
            respect_processor_checkpoints: false,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(report.pruned_count, 5);
    assert!(remaining_keys(&db).await.is_empty());
}

#[tokio::test]
async fn huge_max_age_prunes_nothing() {
    let (_dir, db) = open().await;
    emit_n(&db, 5).await;
    let report = db
        .compact_events(&EventRetentionPolicy {
            max_age_micros: Some(3_600_000_000), // 1 hour
            respect_processor_checkpoints: false,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(report.pruned_count, 0);
}

#[tokio::test]
async fn processor_checkpoint_floors_pruning() {
    let dir = tempdir().expect("temp");
    let db = std::sync::Arc::new(
        AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open"),
    );
    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "app").await.expect("scope");
    // First 4 events are consumed by a processor; the rest are not.
    let first = emit_n(&db, 4).await;
    let floor = first[3];

    db.start_reactive_processor(
        "p1",
        ReactiveProcessorOptions {
            topic_filter: Some("deposit".into()),
            checkpoint_watermark_commits: 1,
            ..ReactiveProcessorOptions::default()
        },
        |_db, _events| async move { Ok(()) },
    )
    .await
    .expect("start");

    // Wait until the processor has checkpointed past the 4th event.
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let lag = db
            .reactive_processor_lag("p1", ConsistencyMode::AtLatest)
            .await
            .unwrap();
        if lag.checkpoint_seq >= floor {
            break;
        }
        assert!(Instant::now() < deadline, "processor failed to checkpoint");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Stop it so the next events stay unconsumed.
    db.pause_reactive_processor("p1").await.expect("pause");

    // These 6 are unconsumed by the processor.
    for i in 4..10 {
        db.emit_event("arcana", "app", "deposit", format!("d{i}"), "{}".into())
            .await
            .unwrap();
    }

    // keep_last=0 would prune everything, but the floor protects unconsumed events.
    let report = db
        .compact_events(&EventRetentionPolicy {
            keep_last: Some(0),
            respect_processor_checkpoints: true,
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(report.processor_floor_seq, Some(floor));
    // Only events at or below the processor checkpoint were pruned.
    assert_eq!(report.pruned_count, 4);
    assert_eq!(
        remaining_keys(&db).await,
        vec!["d4", "d5", "d6", "d7", "d8", "d9"]
    );
}

#[tokio::test]
async fn more_remaining_supports_incremental_compaction() {
    let (_dir, db) = open().await;
    emit_n(&db, 10).await;

    let policy = EventRetentionPolicy {
        keep_last: Some(0),
        respect_processor_checkpoints: false,
        max_prune_per_run: Some(3),
        ..Default::default()
    };
    let r1 = db.compact_events(&policy).await.unwrap();
    assert_eq!(r1.pruned_count, 3);
    assert!(r1.more_remaining);
    assert!(r1.archived_min_seq.is_some());

    // Loop to drain.
    let mut total = r1.pruned_count;
    loop {
        let r = db.compact_events(&policy).await.unwrap();
        total += r.pruned_count;
        if !r.more_remaining {
            break;
        }
    }
    assert_eq!(total, 10);
    assert!(remaining_keys(&db).await.is_empty());
}

#[tokio::test]
async fn pruned_events_stay_pruned_after_restart() {
    let dir = tempdir().expect("temp");
    {
        let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
        db.create_project("arcana").await.expect("project");
        db.create_scope("arcana", "app").await.expect("scope");
        emit_n(&db, 10).await;
        db.compact_events(&EventRetentionPolicy {
            keep_last: Some(2),
            respect_processor_checkpoints: false,
            ..Default::default()
        })
        .await
        .unwrap();
        db.shutdown().await.expect("shutdown");
    }
    let db2 = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("reopen");
    assert_eq!(remaining_keys(&db2).await, vec!["d8", "d9"]);
}

#[tokio::test]
async fn secure_mode_requires_admin() {
    let dir = tempdir().expect("temp");
    let db =
        AedbInstance::open_production(AedbConfig::production([5u8; 32]), dir.path()).expect("open");
    let err = db.compact_events(&EventRetentionPolicy::default()).await;
    assert!(matches!(err, Err(crate::AedbError::PermissionDenied(_))));
}
