use crate::faults::{FaultPlan, any_armed, arm, disarm, reset, trip};

// These tests mutate global registry state, so they run serially under a shared
// mutex to avoid cross-test interference.
use std::sync::Mutex as StdMutex;
static SERIAL: StdMutex<()> = StdMutex::new(());

#[test]
fn disarmed_trip_is_ok_and_not_armed() {
    let _guard = SERIAL.lock().unwrap();
    reset();
    assert!(!any_armed());
    assert!(trip("nope").is_ok());
}

#[test]
fn once_fires_exactly_once_then_disarms() {
    let _guard = SERIAL.lock().unwrap();
    reset();
    arm("wal_sync", FaultPlan::Once);
    assert!(any_armed());
    assert!(trip("wal_sync").is_err(), "first call fires");
    assert!(trip("wal_sync").is_ok(), "auto-disarmed after firing");
    assert!(!any_armed());
    reset();
}

#[test]
fn fail_after_n_skips_then_fires() {
    let _guard = SERIAL.lock().unwrap();
    reset();
    arm("manifest_write", FaultPlan::FailAfter(2));
    assert!(trip("manifest_write").is_ok(), "call 1 ok");
    assert!(trip("manifest_write").is_ok(), "call 2 ok");
    assert!(trip("manifest_write").is_err(), "call 3 fires");
    assert!(trip("manifest_write").is_err(), "and keeps firing");
    reset();
}

#[test]
fn every_nth_fires_on_multiples() {
    let _guard = SERIAL.lock().unwrap();
    reset();
    arm("checkpoint_write", FaultPlan::EveryNth(3));
    assert!(trip("checkpoint_write").is_ok());
    assert!(trip("checkpoint_write").is_ok());
    assert!(trip("checkpoint_write").is_err(), "3rd fires");
    assert!(trip("checkpoint_write").is_ok());
    assert!(trip("checkpoint_write").is_ok());
    assert!(trip("checkpoint_write").is_err(), "6th fires");
    reset();
}

#[test]
fn always_fires_until_disarmed() {
    let _guard = SERIAL.lock().unwrap();
    reset();
    arm("wal_append", FaultPlan::Always);
    assert!(trip("wal_append").is_err());
    assert!(trip("wal_append").is_err());
    disarm("wal_append");
    assert!(trip("wal_append").is_ok());
    assert!(!any_armed());
    reset();
}

#[test]
fn untargeted_names_are_unaffected_while_armed() {
    let _guard = SERIAL.lock().unwrap();
    reset();
    arm("only_this", FaultPlan::Always);
    assert!(trip("only_this").is_err());
    assert!(trip("something_else").is_ok(), "other sites unaffected");
    reset();
}
