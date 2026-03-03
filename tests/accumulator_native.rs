use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::commit::validation::Mutation;
use aedb::error::AedbError;
use aedb::permission::{CallerContext, Permission};
use aedb::query::plan::ConsistencyMode;
use tempfile::tempdir;
use tokio::time::{Duration, sleep};

async fn wait_for_projected(
    db: &AedbInstance,
    project_id: &str,
    scope_id: &str,
    accumulator_name: &str,
    expected: i64,
) {
    for _ in 0..50 {
        let value = db
            .accumulator_value(
                project_id,
                scope_id,
                accumulator_name,
                ConsistencyMode::AtLatest,
            )
            .await
            .expect("projected read");
        if value == expected {
            return;
        }
        sleep(Duration::from_millis(10)).await;
    }
    panic!("projected value did not converge to expected");
}

#[tokio::test]
async fn accumulator_exactly_once_and_projected_reads() {
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_accumulator("p", "app", "house_balance", Some(1_000), 10_000)
        .await
        .expect("create accumulator");

    db.accumulate("p", "app", "house_balance", 100, "tx-1".into(), 1)
        .await
        .expect("accumulate 1");
    db.accumulate("p", "app", "house_balance", -25, "tx-2".into(), 2)
        .await
        .expect("accumulate 2");
    db.accumulate("p", "app", "house_balance", -25, "tx-2".into(), 2)
        .await
        .expect("duplicate idempotent retry");

    let strong = db
        .accumulator_value_strong("p", "app", "house_balance", ConsistencyMode::AtLatest)
        .await
        .expect("strong value");
    assert_eq!(strong, 75);

    let mut lag = db
        .accumulator_lag("p", "app", "house_balance", ConsistencyMode::AtLatest)
        .await
        .expect("lag");
    for _ in 0..20 {
        if lag.lag_orders == 0 && lag.projector_error.is_none() {
            break;
        }
        sleep(Duration::from_millis(10)).await;
        lag = db
            .accumulator_lag("p", "app", "house_balance", ConsistencyMode::AtLatest)
            .await
            .expect("lag refresh");
    }
    assert_eq!(lag.lag_orders, 0, "projector should eventually catch up");

    let projected = db
        .accumulator_value("p", "app", "house_balance", ConsistencyMode::AtLatest)
        .await
        .expect("projected value");
    assert_eq!(projected, 75);
}

#[tokio::test]
async fn accumulator_rejects_out_of_order_and_bad_dedupe_reuse() {
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_accumulator("p", "app", "house_balance", Some(1_000), 10_000)
        .await
        .expect("create accumulator");

    db.accumulate("p", "app", "house_balance", 10, "tx-a".into(), 10)
        .await
        .expect("accumulate baseline");

    let out_of_order = db
        .accumulate("p", "app", "house_balance", 5, "tx-b".into(), 9)
        .await
        .expect_err("out of order should fail");
    assert!(matches!(out_of_order, AedbError::Validation(_)));

    db.accumulate("p", "app", "house_balance", 5, "tx-c".into(), 11)
        .await
        .expect("next in order");
    let bad_retry = db
        .accumulate("p", "app", "house_balance", 5, "tx-c".into(), 12)
        .await
        .expect_err("dedupe key reuse with changed order should fail");
    assert!(matches!(bad_retry, AedbError::Validation(_)));
}

#[tokio::test]
async fn accumulator_recovers_from_wal_and_checkpoint() {
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_accumulator("p", "app", "house_balance", Some(1_000), 2)
        .await
        .expect("create accumulator");
    db.accumulate("p", "app", "house_balance", 40, "tx-1".into(), 1)
        .await
        .expect("acc 1");
    db.accumulate("p", "app", "house_balance", 2, "tx-2".into(), 2)
        .await
        .expect("acc 2");
    db.checkpoint_now().await.expect("checkpoint");
    db.shutdown().await.expect("shutdown");

    let reopened = AedbInstance::open(Default::default(), dir.path()).expect("reopen");
    let strong = reopened
        .accumulator_value_strong("p", "app", "house_balance", ConsistencyMode::AtLatest)
        .await
        .expect("strong");
    assert_eq!(strong, 42);
}

#[tokio::test]
async fn accumulator_overflow_is_exposed_as_health_error() {
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_accumulator("p", "app", "house_balance", Some(1_000), 10_000)
        .await
        .expect("create accumulator");

    db.accumulate("p", "app", "house_balance", i64::MAX, "tx-max".into(), 1)
        .await
        .expect("max");
    db.accumulate("p", "app", "house_balance", 1, "tx-over".into(), 2)
        .await
        .expect("overflow delta append");

    let mut lag = db
        .accumulator_lag("p", "app", "house_balance", ConsistencyMode::AtLatest)
        .await
        .expect("lag");
    for _ in 0..30 {
        if lag.projector_error.is_some() {
            break;
        }
        sleep(Duration::from_millis(10)).await;
        lag = db
            .accumulator_lag("p", "app", "house_balance", ConsistencyMode::AtLatest)
            .await
            .expect("lag refresh");
    }
    assert!(
        lag.projector_error.is_some(),
        "projector error should be reported"
    );

    let strong = db
        .accumulator_value_strong("p", "app", "house_balance", ConsistencyMode::AtLatest)
        .await
        .expect_err("strong read should fail while projector unhealthy");
    assert!(matches!(strong, AedbError::Validation(_)));
}

#[tokio::test]
async fn accumulator_authenticated_apis_enforce_permissions() {
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    let alice = CallerContext::new("alice");

    let denied_create = db
        .create_accumulator_as(alice.clone(), "p", "app", "house_balance", Some(100), 1000)
        .await
        .expect_err("create should require ddl permission");
    assert!(matches!(denied_create, AedbError::PermissionDenied(_)));

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "alice".into(),
        permission: Permission::TableDdl {
            project_id: "p".into(),
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant ddl");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "alice".into(),
        permission: Permission::KvWrite {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: None,
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant kv write");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "alice".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: None,
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant kv read");

    db.create_accumulator_as(alice.clone(), "p", "app", "house_balance", Some(100), 1000)
        .await
        .expect("authorized create");
    db.accumulate_as(
        alice.clone(),
        "p",
        "app",
        "house_balance",
        7,
        "tx-1".into(),
        1,
    )
    .await
    .expect("authorized accumulate");

    let value = db
        .accumulator_value_as(
            &alice,
            "p",
            "app",
            "house_balance",
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("authorized read");
    assert_eq!(value, 7);

    db.expose_accumulator_as(alice.clone(), "p", "app", "house_balance", 2, "h-1".into())
        .await
        .expect("authorized expose");
    let exposure = db
        .accumulator_exposure_as(
            &alice,
            "p",
            "app",
            "house_balance",
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("authorized exposure read");
    assert_eq!(exposure, 2);
}

#[tokio::test]
async fn accumulator_exposure_margin_enforced_and_release_is_idempotent() {
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_accumulator_with_options(
        "p",
        "app",
        "house_balance",
        Some(1_000),
        10_000,
        1_000,
        None,
    )
    .await
    .expect("create accumulator");

    db.accumulate("p", "app", "house_balance", 1_000, "seed".into(), 1)
        .await
        .expect("seed balance");
    wait_for_projected(&db, "p", "app", "house_balance", 1_000).await;

    db.expose_accumulator("p", "app", "house_balance", 400, "hand-1".into())
        .await
        .expect("reserve hand 1");
    db.expose_accumulator("p", "app", "house_balance", 500, "hand-2".into())
        .await
        .expect("reserve hand 2");
    let over = db
        .expose_accumulator("p", "app", "house_balance", 1, "hand-3".into())
        .await
        .expect_err("excess reserve should fail");
    assert!(matches!(over, AedbError::Validation(_)));

    let exposure = db
        .accumulator_exposure("p", "app", "house_balance", ConsistencyMode::AtLatest)
        .await
        .expect("exposure");
    assert_eq!(exposure, 900);
    let available = db
        .accumulator_available("p", "app", "house_balance", ConsistencyMode::AtLatest)
        .await
        .expect("available");
    assert_eq!(available, 100);
    let metrics = db
        .accumulator_exposure_metrics("p", "app", "house_balance", ConsistencyMode::AtLatest)
        .await
        .expect("exposure metrics");
    assert_eq!(metrics.total_exposure, 900);
    assert_eq!(metrics.available, 100);
    assert_eq!(metrics.rejection_count, 0);
    assert_eq!(metrics.open_exposure_count, 2);

    db.accumulate_with_release(
        "p",
        "app",
        "house_balance",
        -50,
        "settle-1".into(),
        2,
        Some("hand-1".into()),
    )
    .await
    .expect("settle hand 1");
    db.accumulate_with_release(
        "p",
        "app",
        "house_balance",
        -50,
        "settle-1".into(),
        2,
        Some("hand-1".into()),
    )
    .await
    .expect("idempotent settle retry");
    wait_for_projected(&db, "p", "app", "house_balance", 950).await;

    let exposure_after = db
        .accumulator_exposure("p", "app", "house_balance", ConsistencyMode::AtLatest)
        .await
        .expect("exposure after release");
    assert_eq!(exposure_after, 500);
    let available_after = db
        .accumulator_available("p", "app", "house_balance", ConsistencyMode::AtLatest)
        .await
        .expect("available after release");
    assert_eq!(available_after, 450);
    let metrics_after = db
        .accumulator_exposure_metrics("p", "app", "house_balance", ConsistencyMode::AtLatest)
        .await
        .expect("metrics after release");
    assert_eq!(metrics_after.total_exposure, 500);
    assert_eq!(metrics_after.open_exposure_count, 1);
}

#[tokio::test]
async fn accumulator_release_without_exposure_is_rejected() {
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_accumulator("p", "app", "house_balance", Some(1_000), 10_000)
        .await
        .expect("create accumulator");
    db.accumulate("p", "app", "house_balance", 100, "seed".into(), 1)
        .await
        .expect("seed");
    let err = db
        .accumulate_with_release(
            "p",
            "app",
            "house_balance",
            -5,
            "settle-missing".into(),
            2,
            Some("unknown-hand".into()),
        )
        .await
        .expect_err("release without expose should fail");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[tokio::test]
async fn accumulator_expose_dedupe_is_exactly_once() {
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_accumulator("p", "app", "house_balance", Some(1_000), 10_000)
        .await
        .expect("create accumulator");
    db.accumulate("p", "app", "house_balance", 100, "seed".into(), 1)
        .await
        .expect("seed");
    wait_for_projected(&db, "p", "app", "house_balance", 100).await;

    db.expose_accumulator("p", "app", "house_balance", 20, "hand-1".into())
        .await
        .expect("first expose");
    db.expose_accumulator("p", "app", "house_balance", 20, "hand-1".into())
        .await
        .expect("idempotent expose retry");

    let wrong_retry = db
        .expose_accumulator("p", "app", "house_balance", 21, "hand-1".into())
        .await
        .expect_err("mismatched idempotent expose should fail");
    assert!(matches!(wrong_retry, AedbError::Validation(_)));

    let exposure = db
        .accumulator_exposure("p", "app", "house_balance", ConsistencyMode::AtLatest)
        .await
        .expect("exposure");
    assert_eq!(exposure, 20);
}

#[tokio::test]
async fn accumulator_exposure_recovers_after_restart() {
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_accumulator("p", "app", "house_balance", Some(1_000), 10_000)
        .await
        .expect("create accumulator");
    db.accumulate("p", "app", "house_balance", 500, "seed".into(), 1)
        .await
        .expect("seed");
    wait_for_projected(&db, "p", "app", "house_balance", 500).await;
    db.expose_accumulator("p", "app", "house_balance", 40, "hand-1".into())
        .await
        .expect("expose");
    db.checkpoint_now().await.expect("checkpoint");
    db.shutdown().await.expect("shutdown");

    let reopened = AedbInstance::open(Default::default(), dir.path()).expect("reopen");
    let exposure = reopened
        .accumulator_exposure("p", "app", "house_balance", ConsistencyMode::AtLatest)
        .await
        .expect("exposure");
    assert_eq!(exposure, 40);
    let available = reopened
        .accumulator_available("p", "app", "house_balance", ConsistencyMode::AtLatest)
        .await
        .expect("available");
    assert_eq!(available, 460);
}

#[tokio::test]
async fn accumulator_exposure_ttl_prunes_orphaned_hands() {
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_accumulator_with_options(
        "p",
        "app",
        "house_balance",
        Some(1_000),
        10_000,
        1_000,
        Some(2),
    )
    .await
    .expect("create accumulator");
    db.accumulate("p", "app", "house_balance", 200, "seed".into(), 1)
        .await
        .expect("seed");
    wait_for_projected(&db, "p", "app", "house_balance", 200).await;
    db.expose_accumulator("p", "app", "house_balance", 25, "orphan".into())
        .await
        .expect("expose");

    db.accumulate("p", "app", "house_balance", 0, "tick-1".into(), 2)
        .await
        .expect("tick 1");
    db.accumulate("p", "app", "house_balance", 0, "tick-2".into(), 3)
        .await
        .expect("tick 2");
    db.accumulate("p", "app", "house_balance", 0, "tick-3".into(), 4)
        .await
        .expect("tick 3");

    for _ in 0..50 {
        let exposure = db
            .accumulator_exposure("p", "app", "house_balance", ConsistencyMode::AtLatest)
            .await
            .expect("exposure read");
        if exposure == 0 {
            return;
        }
        sleep(Duration::from_millis(10)).await;
    }
    panic!("exposure ttl did not prune orphaned exposure");
}

#[tokio::test]
async fn accumulator_expose_many_atomic_reserves_all_or_none() {
    let dir = tempdir().expect("temp dir");
    let db = AedbInstance::open(Default::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_accumulator("p", "app", "house_balance", Some(1_000), 10_000)
        .await
        .expect("create accumulator");
    db.accumulate("p", "app", "house_balance", 100, "seed".into(), 1)
        .await
        .expect("seed");
    wait_for_projected(&db, "p", "app", "house_balance", 100).await;

    db.expose_accumulator_many_atomic(
        "p",
        "app",
        "house_balance",
        vec![(20, "h1".into()), (30, "h2".into())],
    )
    .await
    .expect("batch expose");
    let exposure = db
        .accumulator_exposure("p", "app", "house_balance", ConsistencyMode::AtLatest)
        .await
        .expect("exposure");
    assert_eq!(exposure, 50);

    let err = db
        .expose_accumulator_many_atomic(
            "p",
            "app",
            "house_balance",
            vec![(10, "h3".into()), (10, "".into())],
        )
        .await
        .expect_err("invalid batch should fail");
    assert!(matches!(err, AedbError::Validation(_)));
    let exposure_after = db
        .accumulator_exposure("p", "app", "house_balance", ConsistencyMode::AtLatest)
        .await
        .expect("exposure unchanged");
    assert_eq!(exposure_after, 50);
}
