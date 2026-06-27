//! End-to-end fault-injection tests: arm a durability-path fail point, drive a
//! real commit through it, and assert the failure is surfaced and the store
//! stays recoverable and consistent.
//!
//! These tests mutate a process-global fault registry, so they are serialized
//! with a shared mutex and always `reset()` in teardown.
//!
//! The serial guard is intentionally held across `.await` points to keep the
//! whole test body in one critical section; that is safe here because the test
//! runtime is single-threaded per test.
#![allow(clippy::await_holding_lock)]

use aedb::config::{AedbConfig, DurabilityMode, RecoveryMode};
use aedb::faults::{self, FaultPlan};
use aedb::{AedbInstance, CommitFinality};
use aedb::commit::validation::Mutation;
use aedb::offline;
use std::sync::Mutex;
use tempfile::tempdir;

static SERIAL: Mutex<()> = Mutex::new(());

fn durable_config() -> AedbConfig {
    AedbConfig {
        durability_mode: DurabilityMode::Full,
        recovery_mode: RecoveryMode::Permissive,
        hash_chain_required: false,
        ..AedbConfig::default()
    }
}

async fn seed(db: &AedbInstance) {
    db.create_project("p").await.expect("project");
    db.kv_set("p", "app", b"k0".to_vec(), b"v0".to_vec())
        .await
        .expect("seed kv");
}

#[tokio::test]
async fn injected_wal_sync_failure_aborts_commit_and_store_stays_consistent() {
    let _guard = SERIAL.lock().unwrap();
    faults::reset();

    let dir = tempdir().expect("temp");
    let config = durable_config();
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    seed(&db).await;

    // Fail every WAL fsync. The commit that needs durability must error.
    faults::arm("wal_sync", FaultPlan::Always);
    let result = db
        .commit_with_finality(
            Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
            },
            CommitFinality::Durable,
        )
        .await;
    assert!(
        result.is_err(),
        "commit must fail when the WAL sync fault fires"
    );

    // Clear the fault; subsequent commits succeed again.
    faults::reset();
    db.kv_set("p", "app", b"k2".to_vec(), b"v2".to_vec())
        .await
        .expect("commit after fault clears");

    db.shutdown().await.expect("shutdown");
    drop(db);

    // The store must verify cleanly regardless of the mid-flight failure.
    let report = offline::verify_database(dir.path(), &config);
    assert!(report.ok, "store must stay consistent: {report:?}");

    faults::reset();
}

#[tokio::test]
async fn injected_manifest_write_failure_surfaces_on_checkpoint() {
    let _guard = SERIAL.lock().unwrap();
    faults::reset();

    let dir = tempdir().expect("temp");
    let config = durable_config();
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    seed(&db).await;

    faults::arm("manifest_write", FaultPlan::Once);
    let checkpoint = db.checkpoint_now().await;
    assert!(
        checkpoint.is_err(),
        "checkpoint must fail when manifest write fault fires"
    );
    faults::reset();

    // After clearing the fault, a checkpoint and verification both succeed.
    db.checkpoint_now().await.expect("checkpoint after clear");
    db.shutdown().await.expect("shutdown");
    drop(db);
    let report = offline::verify_database(dir.path(), &config);
    assert!(report.ok, "store must stay consistent: {report:?}");

    faults::reset();
}
