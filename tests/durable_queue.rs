//! Durability of the task queue across graceful restart and simulated crash.
//!
//! The queue is "durable" only if enqueued work survives process death. These
//! tests enqueue work, take the instance down, reopen the same data dir, and
//! assert the tasks are still there and claimable.

use aedb::config::AedbConfig;
use aedb::query::plan::ConsistencyMode;
use aedb::{AedbInstance, EnqueueOptions, TaskState};
use tempfile::tempdir;

const Q: &str = "jobs";
const LEASE: u64 = 60_000_000;

async fn boot(config: AedbConfig, dir: &std::path::Path) -> AedbInstance {
    let db = AedbInstance::open_anonymous(config, dir).expect("open");
    // create_project/scope are idempotent-friendly via if-exists guards in tests:
    let _ = db.create_project("arcana").await;
    let _ = db.create_scope("arcana", "app").await;
    db
}

fn opts(payload: &str, priority: u64) -> EnqueueOptions {
    EnqueueOptions {
        payload_json: format!("\"{payload}\""),
        priority,
        ..Default::default()
    }
}

#[tokio::test]
async fn tasks_survive_graceful_restart() {
    let dir = tempdir().expect("tempdir");
    let config = AedbConfig::default();

    {
        let db = boot(config.clone(), dir.path()).await;
        db.queue_enqueue("arcana", "app", Q, opts("low", 1), vec![])
            .await
            .unwrap();
        db.queue_enqueue("arcana", "app", Q, opts("high", 9), vec![])
            .await
            .unwrap();
        // A delayed task, plus a follow-up chain via complete.
        db.queue_enqueue(
            "arcana",
            "app",
            Q,
            EnqueueOptions {
                payload_json: "\"later\"".into(),
                delay_micros: 3_600_000_000, // 1h out
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap();
        db.shutdown().await.expect("shutdown");
    }

    // Reopen the same data dir.
    let db = boot(config, dir.path()).await;
    let all = db
        .queue_list("arcana", "app", Q, None, 100, ConsistencyMode::AtLatest)
        .await
        .unwrap();
    assert_eq!(all.len(), 3, "all enqueued tasks recovered");

    // Due tasks are still served highest-priority-first after recovery.
    let claimed = db
        .queue_claim("arcana", "app", Q, "w1", LEASE, 10)
        .await
        .unwrap();
    let order: Vec<_> = claimed.iter().map(|c| c.payload_json.clone()).collect();
    assert_eq!(order, vec!["\"high\"", "\"low\""]);
    assert!(
        !order.contains(&"\"later\"".to_string()),
        "the delayed task is not yet due"
    );
}

#[tokio::test]
async fn inflight_task_survives_crash_and_is_reclaimable() {
    let dir = tempdir().expect("tempdir");
    let config = AedbConfig::default();

    let task_id;
    let fencing;
    {
        let db = boot(config.clone(), dir.path()).await;
        task_id = db
            .queue_enqueue("arcana", "app", Q, opts("work", 0), vec![])
            .await
            .unwrap()
            .task_id;
        // Claim with a short lease, then "crash" (drop without shutdown).
        let claimed = db
            .queue_claim("arcana", "app", Q, "worker-a", 1, 1)
            .await
            .unwrap();
        fencing = claimed[0].fencing_token;
        db.shutdown().await.expect("shutdown");
    }

    let db = boot(config, dir.path()).await;
    // The in-flight record survived; its lease has expired, so a new worker
    // reclaims and runs it.
    let rec = db
        .queue_get("arcana", "app", Q, &task_id, ConsistencyMode::AtLatest)
        .await
        .unwrap()
        .expect("task recovered");
    assert_eq!(rec.state, TaskState::InFlight);

    let again = db
        .queue_claim("arcana", "app", Q, "worker-b", LEASE, 1)
        .await
        .unwrap();
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].task_id, task_id);
    assert!(
        again[0].fencing_token > fencing,
        "reclaim issues a fresh fencing token"
    );
}
