use crate::commit::validation::Mutation;
use crate::{
    AedbConfig, AedbInstance, ConsistencyMode, EnqueueOptions, EnqueueSpec, FencedCommit, TaskState,
};
use tempfile::tempdir;

const Q: &str = "tasks";
const LEASE: u64 = 60_000_000;

async fn open() -> (tempfile::TempDir, AedbInstance) {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "app").await.expect("scope");
    (dir, db)
}

fn opts(payload: &str) -> EnqueueOptions {
    EnqueueOptions {
        payload_json: format!("\"{payload}\""),
        ..Default::default()
    }
}

async fn enqueue(db: &AedbInstance, o: EnqueueOptions) -> String {
    db.queue_enqueue("arcana", "app", Q, o, vec![])
        .await
        .unwrap()
        .task_id
}

#[tokio::test]
async fn claim_orders_by_priority_then_fifo() {
    let (_dir, db) = open().await;
    enqueue(&db, opts("a-low")).await; // priority 0, seq 0
    db.queue_enqueue(
        "arcana",
        "app",
        Q,
        EnqueueOptions {
            payload_json: "\"b-high\"".into(),
            priority: 10,
            ..Default::default()
        },
        vec![],
    )
    .await
    .unwrap();
    enqueue(&db, opts("c-low")).await; // priority 0, seq 2

    let claimed = db
        .queue_claim("arcana", "app", Q, "w1", LEASE, 10)
        .await
        .unwrap();
    let order: Vec<_> = claimed.iter().map(|c| c.payload_json.clone()).collect();
    assert_eq!(order, vec!["\"b-high\"", "\"a-low\"", "\"c-low\""]);
}

#[tokio::test]
async fn enqueue_with_extra_mutation_is_atomic() {
    let (_dir, db) = open().await;
    let extra = Mutation::KvSet {
        project_id: "arcana".into(),
        scope_id: "app".into(),
        key: b"outbox-marker".to_vec(),
        value: b"1".to_vec(),
    };
    db.queue_enqueue("arcana", "app", Q, opts("t"), vec![extra])
        .await
        .unwrap();
    // The task and the caller's own state change committed together.
    assert!(
        db.kv_get_no_auth("arcana", "app", b"outbox-marker", ConsistencyMode::AtLatest)
            .await
            .unwrap()
            .is_some()
    );
    let claimed = db
        .queue_claim("arcana", "app", Q, "w1", LEASE, 1)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
}

#[tokio::test]
async fn delay_defers_execution() {
    let (_dir, db) = open().await;
    db.queue_enqueue(
        "arcana",
        "app",
        Q,
        EnqueueOptions {
            payload_json: "\"later\"".into(),
            delay_micros: 60_000_000,
            ..Default::default()
        },
        vec![],
    )
    .await
    .unwrap();
    enqueue(&db, opts("now")).await;

    let claimed = db
        .queue_claim("arcana", "app", Q, "w1", LEASE, 10)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1, "delayed task is not yet due");
    assert_eq!(claimed[0].payload_json, "\"now\"");
}

#[tokio::test]
async fn complete_acks_applies_mutations_and_follow_ups() {
    let (_dir, db) = open().await;
    enqueue(&db, opts("step1")).await;
    let c = db
        .queue_claim("arcana", "app", Q, "w1", LEASE, 1)
        .await
        .unwrap();
    let task = &c[0];

    let extra = Mutation::KvSet {
        project_id: "arcana".into(),
        scope_id: "app".into(),
        key: b"side-effect".to_vec(),
        value: b"done".to_vec(),
    };
    let follow = EnqueueSpec {
        queue: Q.to_string(),
        options: opts("step2"),
    };
    let out = db
        .queue_complete(
            "arcana",
            "app",
            Q,
            &task.task_id,
            task.fencing_token,
            "\"ok\"".into(),
            vec![follow],
            vec![extra],
        )
        .await
        .unwrap();
    assert!(matches!(out, FencedCommit::Applied(_)));

    // Original task is gone.
    assert!(
        db.queue_get("arcana", "app", Q, &task.task_id, ConsistencyMode::AtLatest)
            .await
            .unwrap()
            .is_none()
    );
    // Side-effect mutation committed atomically.
    assert_eq!(
        db.kv_get_no_auth("arcana", "app", b"side-effect", ConsistencyMode::AtLatest)
            .await
            .unwrap()
            .unwrap()
            .value,
        b"done"
    );
    // Follow-up is now claimable (workflow next step / scheduler re-arm).
    let next = db
        .queue_claim("arcana", "app", Q, "w1", LEASE, 1)
        .await
        .unwrap();
    assert_eq!(next[0].payload_json, "\"step2\"");
}

#[tokio::test]
async fn fail_retries_then_dead_letters() {
    let (_dir, db) = open().await;
    let id = db
        .queue_enqueue(
            "arcana",
            "app",
            Q,
            EnqueueOptions {
                payload_json: "\"flaky\"".into(),
                max_attempts: 2,
                backoff_base_micros: 1, // effectively immediate re-arm
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap()
        .task_id;

    // Attempt 1 -> fail -> requeued.
    let c1 = db.queue_claim("arcana", "app", Q, "w1", LEASE, 1).await.unwrap();
    let f1 = db
        .queue_fail("arcana", "app", Q, &id, c1[0].fencing_token, "boom".into())
        .await
        .unwrap();
    assert!(matches!(f1, FencedCommit::Applied(_)));
    let rec = db
        .queue_get("arcana", "app", Q, &id, ConsistencyMode::AtLatest)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rec.state, TaskState::Ready);
    assert_eq!(rec.attempts, 1);

    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    // Attempt 2 -> fail -> dead-lettered (attempts == max).
    let c2 = db.queue_claim("arcana", "app", Q, "w1", LEASE, 1).await.unwrap();
    assert_eq!(c2[0].attempts, 2);
    db.queue_fail("arcana", "app", Q, &id, c2[0].fencing_token, "boom2".into())
        .await
        .unwrap();
    let dead = db
        .queue_get("arcana", "app", Q, &id, ConsistencyMode::AtLatest)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(dead.state, TaskState::Dead);

    let dlq = db
        .queue_list("arcana", "app", Q, Some(TaskState::Dead), 10, ConsistencyMode::AtLatest)
        .await
        .unwrap();
    assert_eq!(dlq.len(), 1);
    assert_eq!(dlq[0].last_error.as_deref(), Some("boom2"));
}

#[tokio::test]
async fn stale_fencing_token_is_rejected() {
    let (_dir, db) = open().await;
    let id = enqueue(&db, opts("t")).await;
    let c = db.queue_claim("arcana", "app", Q, "w1", LEASE, 1).await.unwrap();
    let good = c[0].fencing_token;

    // A zombie worker presenting a wrong token cannot complete.
    let lost = db
        .queue_complete("arcana", "app", Q, &id, good + 7, "\"x\"".into(), vec![], vec![])
        .await
        .unwrap();
    assert!(matches!(lost, FencedCommit::LeaseLost));
    // The legitimate owner still can.
    let ok = db
        .queue_complete("arcana", "app", Q, &id, good, "\"x\"".into(), vec![], vec![])
        .await
        .unwrap();
    assert!(matches!(ok, FencedCommit::Applied(_)));
}

#[tokio::test]
async fn expired_lease_is_reclaimed() {
    let (_dir, db) = open().await;
    let id = db
        .queue_enqueue(
            "arcana",
            "app",
            Q,
            EnqueueOptions {
                payload_json: "\"t\"".into(),
                backoff_base_micros: 1,
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap()
        .task_id;

    // Claim with an already-expired lease; the worker "dies".
    let c = db.queue_claim("arcana", "app", Q, "w1", 0, 1).await.unwrap();
    assert_eq!(c.len(), 1);

    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    // A later claim reclaims the abandoned task and serves it again.
    let again = db.queue_claim("arcana", "app", Q, "w2", LEASE, 1).await.unwrap();
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].task_id, id);
    assert_eq!(again[0].attempts, 2, "reclaim preserved progress, new claim bumped attempts");
}

#[tokio::test]
async fn enqueue_is_idempotent_with_a_key() {
    let (_dir, db) = open().await;
    let o = EnqueueOptions {
        payload_json: "\"once\"".into(),
        idempotency_key: Some("order-42".into()),
        ..Default::default()
    };
    let first = db.queue_enqueue("arcana", "app", Q, o.clone(), vec![]).await.unwrap();
    assert!(!first.deduplicated);
    let second = db.queue_enqueue("arcana", "app", Q, o, vec![]).await.unwrap();
    assert!(second.deduplicated);
    assert_eq!(second.task_id, first.task_id);

    let all = db
        .queue_list("arcana", "app", Q, None, 10, ConsistencyMode::AtLatest)
        .await
        .unwrap();
    assert_eq!(all.len(), 1, "only one task despite two enqueues");
}

#[tokio::test]
async fn heartbeat_records_progress_and_cancel_removes() {
    let (_dir, db) = open().await;
    let id = enqueue(&db, opts("t")).await;
    let c = db.queue_claim("arcana", "app", Q, "w1", LEASE, 1).await.unwrap();
    let hb = db
        .queue_heartbeat(
            "arcana",
            "app",
            Q,
            &id,
            c[0].fencing_token,
            LEASE,
            Some("{\"step\":3}".into()),
        )
        .await
        .unwrap();
    assert!(matches!(hb, FencedCommit::Applied(_)));
    let rec = db
        .queue_get("arcana", "app", Q, &id, ConsistencyMode::AtLatest)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rec.progress_json, "{\"step\":3}");

    assert!(db.queue_cancel("arcana", "app", Q, &id).await.unwrap());
    assert!(
        db.queue_get("arcana", "app", Q, &id, ConsistencyMode::AtLatest)
            .await
            .unwrap()
            .is_none()
    );
}
