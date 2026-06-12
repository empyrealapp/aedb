//! Regression tests for the exclusive data-directory lock.
//!
//! Two live `AedbInstance`s on the same directory would race two writers (and
//! two WAL-GC threads) against the same segments/manifest and corrupt the
//! store. Opening a second instance must fail fast instead.

use aedb::AedbInstance;
use aedb::config::{AedbConfig, RecoveryMode};
use aedb::error::AedbError;

fn lock_test_config() -> AedbConfig {
    // Permissive recovery so an empty, freshly-created store can be reopened in
    // the reopen test; this keeps the tests focused on the directory lock
    // rather than strict-mode manifest reconstruction.
    AedbConfig {
        recovery_mode: RecoveryMode::Permissive,
        hash_chain_required: false,
        ..AedbConfig::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_open_on_same_dir_is_rejected_while_first_is_alive() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("aedb");

    let first = AedbInstance::open(lock_test_config(), &dir).expect("first open succeeds");

    match AedbInstance::open(lock_test_config(), &dir) {
        Ok(_) => panic!("second concurrent open on the same dir must be rejected"),
        Err(AedbError::Unavailable { .. }) => {}
        Err(other) => panic!("expected Unavailable (directory locked), got: {other:?}"),
    }

    // Keep the first instance alive until after the second attempt.
    drop(first);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reopen_succeeds_after_first_instance_is_dropped() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("aedb");

    let first = AedbInstance::open(lock_test_config(), &dir).expect("first open succeeds");
    drop(first); // releases the advisory lock (and joins the GC thread)

    // A fresh open on the same dir must now succeed.
    let _second = AedbInstance::open(lock_test_config(), &dir)
        .expect("reopen after drop must succeed once the lock is released");
}
