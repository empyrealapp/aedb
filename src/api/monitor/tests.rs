use crate::commit::validation::Mutation;
use crate::{
    AedbConfig, AedbInstance, ConsistencyMode, FencedCommit, LeaseOutcome, MonitorCheckpointUpdate,
    RenewOutcome,
};
use tempfile::tempdir;

const TTL: u64 = 60_000_000; // 60s — comfortably live for a test.

async fn open() -> (tempfile::TempDir, AedbInstance) {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "app").await.expect("scope");
    (dir, db)
}

fn lease(outcome: LeaseOutcome) -> crate::MonitorLease {
    match outcome {
        LeaseOutcome::Acquired(l) => l,
        LeaseOutcome::Held { .. } => panic!("expected Acquired, got Held"),
    }
}

#[tokio::test]
async fn acquire_is_exclusive_while_live() {
    let (_dir, db) = open().await;
    let l1 = db
        .monitor_acquire_lease("arcana", "app", "erc20", "w1", TTL)
        .await
        .unwrap();
    assert_eq!(lease(l1).fencing_token, 1);

    let l2 = db
        .monitor_acquire_lease("arcana", "app", "erc20", "w2", TTL)
        .await
        .unwrap();
    match l2 {
        LeaseOutcome::Held { owner_id, .. } => assert_eq!(owner_id, "w1"),
        LeaseOutcome::Acquired(_) => panic!("w2 should not acquire a live lease"),
    }
}

#[tokio::test]
async fn expired_lease_can_be_taken_over_and_old_owner_is_fenced() {
    let (_dir, db) = open().await;
    // ttl=0 -> immediately expired.
    let l1 = lease(
        db.monitor_acquire_lease("arcana", "app", "erc20", "w1", 0)
            .await
            .unwrap(),
    );
    assert_eq!(l1.fencing_token, 1);

    let l2 = lease(
        db.monitor_acquire_lease("arcana", "app", "erc20", "w2", TTL)
            .await
            .unwrap(),
    );
    assert_eq!(l2.fencing_token, 2, "takeover bumps the fencing token");

    // The old owner is fenced out.
    let fenced = db
        .monitor_advance_checkpoint(
            "arcana",
            "app",
            "erc20",
            l1.fencing_token,
            MonitorCheckpointUpdate::default(),
            vec![],
        )
        .await
        .unwrap();
    assert!(matches!(fenced, FencedCommit::LeaseLost));

    // The new owner can advance.
    let ok = db
        .monitor_advance_checkpoint(
            "arcana",
            "app",
            "erc20",
            l2.fencing_token,
            MonitorCheckpointUpdate::default(),
            vec![],
        )
        .await
        .unwrap();
    assert!(matches!(ok, FencedCommit::Applied(_)));
}

#[tokio::test]
async fn advance_records_checkpoint_and_applies_mutation_atomically() {
    let (_dir, db) = open().await;
    let l = lease(
        db.monitor_acquire_lease("arcana", "app", "erc20", "w1", TTL)
            .await
            .unwrap(),
    );

    let update = MonitorCheckpointUpdate {
        start_block: Some(100),
        last_scanned_block: Some(150),
        last_processed_cursor: Some("page-7".into()),
        retry_count: Some(0),
        last_error: None,
    };
    let credit = vec![Mutation::KvSet {
        project_id: "arcana".into(),
        scope_id: "app".into(),
        key: b"balance:alice".to_vec(),
        value: b"42".to_vec(),
    }];
    let res = db
        .monitor_advance_checkpoint("arcana", "app", "erc20", l.fencing_token, update, credit)
        .await
        .unwrap();
    assert!(matches!(res, FencedCommit::Applied(_)));

    let status = db
        .monitor_status("arcana", "app", "erc20", ConsistencyMode::AtLatest)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.start_block, Some(100));
    assert_eq!(status.last_scanned_block, Some(150));
    assert_eq!(status.last_processed_cursor.as_deref(), Some("page-7"));
    assert_eq!(status.owner_id.as_deref(), Some("w1"));
    assert!(status.last_update_micros > 0);

    let bal = db
        .kv_get_no_auth("arcana", "app", b"balance:alice", ConsistencyMode::AtLatest)
        .await
        .unwrap();
    assert_eq!(bal.map(|e| e.value), Some(b"42".to_vec()));
}

#[tokio::test]
async fn renew_extends_and_detects_loss() {
    let (_dir, db) = open().await;
    let l = lease(
        db.monitor_acquire_lease("arcana", "app", "erc20", "w1", TTL)
            .await
            .unwrap(),
    );
    let renewed = db
        .monitor_renew_lease("arcana", "app", "erc20", l.fencing_token, TTL)
        .await
        .unwrap();
    assert!(matches!(renewed, RenewOutcome::Renewed(_)));

    // A stale token cannot renew.
    let lost = db
        .monitor_renew_lease("arcana", "app", "erc20", 999, TTL)
        .await
        .unwrap();
    assert!(matches!(lost, RenewOutcome::LeaseLost));
}

#[tokio::test]
async fn release_frees_the_lease() {
    let (_dir, db) = open().await;
    let l = lease(
        db.monitor_acquire_lease("arcana", "app", "erc20", "w1", TTL)
            .await
            .unwrap(),
    );
    db.monitor_release_lease("arcana", "app", "erc20", l.fencing_token)
        .await
        .unwrap();

    let status = db
        .monitor_status("arcana", "app", "erc20", ConsistencyMode::AtLatest)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.owner_id, None);

    // After release a new owner acquires cleanly.
    let l2 = lease(
        db.monitor_acquire_lease("arcana", "app", "erc20", "w2", TTL)
            .await
            .unwrap(),
    );
    assert_eq!(l2.owner_id, "w2");
}

#[tokio::test]
async fn status_none_for_unknown_monitor() {
    let (_dir, db) = open().await;
    let s = db
        .monitor_status("arcana", "app", "never", ConsistencyMode::AtLatest)
        .await
        .unwrap();
    assert!(s.is_none());
}

#[tokio::test]
async fn rejects_empty_names() {
    let (_dir, db) = open().await;
    assert!(
        db.monitor_acquire_lease("arcana", "app", "", "w1", TTL)
            .await
            .is_err()
    );
    assert!(
        db.monitor_acquire_lease("arcana", "app", "m", "", TTL)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn checkpoint_survives_restart() {
    let dir = tempdir().expect("temp");
    let token = {
        let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
        db.create_project("arcana").await.expect("project");
        db.create_scope("arcana", "app").await.expect("scope");
        let l = lease(
            db.monitor_acquire_lease("arcana", "app", "erc20", "w1", TTL)
                .await
                .unwrap(),
        );
        db.monitor_advance_checkpoint(
            "arcana",
            "app",
            "erc20",
            l.fencing_token,
            MonitorCheckpointUpdate {
                last_scanned_block: Some(500),
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap();
        db.shutdown().await.expect("shutdown");
        l.fencing_token
    };
    let db2 = AedbInstance::open(AedbConfig::default(), dir.path()).expect("reopen");
    let status = db2
        .monitor_status("arcana", "app", "erc20", ConsistencyMode::AtLatest)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.last_scanned_block, Some(500));
    assert_eq!(status.fencing_token, token);
}
