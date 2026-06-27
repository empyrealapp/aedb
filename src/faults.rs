//! Deterministic fault injection for durability-path testing.
//!
//! A small, always-compiled registry of named *fail points*. Tests arm a fail
//! point by name (e.g. `"wal_sync"`) with a [`FaultPlan`]; the corresponding
//! call site invokes [`trip`] and receives the injected error when the plan
//! fires. This lets crash/corruption-recovery paths be exercised
//! deterministically — "fail the 3rd WAL fsync", "fail every manifest write" —
//! thousands of times, the way SQLite's fault injection does.
//!
//! ## Production cost
//!
//! The registry is compiled into every build, but [`trip`] short-circuits on a
//! single relaxed atomic load when nothing is armed (the common case), so the
//! production overhead at each site is one predictable branch. Fail points are
//! only ever armed by explicit test calls — there is no way for normal
//! operation to enable them.
//!
//! ## Determinism
//!
//! Plans count invocations, so behaviour is a function of how many times a site
//! is reached, not of wall-clock timing. A test that arms `FailAfter(2)` always
//! sees the third call fail.

use crate::error::AedbError;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// When a fail point fires. Counts are evaluated against the number of times the
/// named site has been reached since the plan was armed.
#[derive(Debug, Clone)]
pub enum FaultPlan {
    /// Fire on every call.
    Always,
    /// Fire exactly once, on the next call, then disarm.
    Once,
    /// Fire after `n` successful (non-firing) calls — i.e. on call `n + 1`,
    /// then keep firing on every subsequent call.
    FailAfter(u64),
    /// Fire on every `n`-th call (`n`, `2n`, ...). `n == 0` never fires.
    EveryNth(u64),
}

#[derive(Debug)]
struct ArmedFault {
    plan: FaultPlan,
    hits: u64,
    /// Error message used when the fault fires.
    message: String,
}

struct Registry {
    /// Number of currently-armed fail points; mirrored into [`ARMED_COUNT`] so
    /// the hot path can skip the lock entirely when nothing is armed.
    faults: HashMap<String, ArmedFault>,
}

static REGISTRY: LazyLock<Mutex<Registry>> = LazyLock::new(|| {
    Mutex::new(Registry {
        faults: HashMap::new(),
    })
});

/// Fast-path gate: number of armed fail points. `trip` returns immediately when
/// this is zero, which is always true in production.
static ARMED_COUNT: AtomicU64 = AtomicU64::new(0);

/// Arm `name` with `plan`. The injected error carries a generic message; use
/// [`arm_with_message`] to customise it.
pub fn arm(name: &str, plan: FaultPlan) {
    arm_with_message(name, plan, &format!("injected fault at '{name}'"));
}

/// Arm `name` with `plan` and a specific error message.
pub fn arm_with_message(name: &str, plan: FaultPlan, message: &str) {
    let mut registry = REGISTRY.lock();
    let existed = registry.faults.contains_key(name);
    registry.faults.insert(
        name.to_string(),
        ArmedFault {
            plan,
            hits: 0,
            message: message.to_string(),
        },
    );
    if !existed {
        ARMED_COUNT.fetch_add(1, Ordering::Release);
    }
}

/// Disarm a single fail point. No-op if it was not armed.
pub fn disarm(name: &str) {
    let mut registry = REGISTRY.lock();
    if registry.faults.remove(name).is_some() {
        ARMED_COUNT.fetch_sub(1, Ordering::Release);
    }
}

/// Disarm every fail point. Tests should call this in teardown so leaked arming
/// cannot bleed into other tests.
pub fn reset() {
    let mut registry = REGISTRY.lock();
    let n = registry.faults.len() as u64;
    registry.faults.clear();
    ARMED_COUNT.fetch_sub(n, Ordering::Release);
}

/// Whether any fail point is currently armed. Cheap; lock-free.
#[inline]
pub fn any_armed() -> bool {
    ARMED_COUNT.load(Ordering::Acquire) != 0
}

/// Consult the fail point `name`. Returns the injected [`AedbError`] when the
/// armed plan fires for this call, otherwise `Ok(())`.
///
/// At call sites this is `crate::faults::trip("name")?;` immediately before the
/// real fallible operation, so a firing fault stands in for that operation's
/// failure.
#[inline]
pub fn trip(name: &str) -> Result<(), AedbError> {
    // Hot path: nothing armed anywhere. One relaxed-ish atomic load.
    if !any_armed() {
        return Ok(());
    }
    let mut registry = REGISTRY.lock();
    let Some(fault) = registry.faults.get_mut(name) else {
        return Ok(());
    };
    fault.hits += 1;
    let fires = match fault.plan {
        FaultPlan::Always => true,
        FaultPlan::Once => true,
        FaultPlan::FailAfter(n) => fault.hits > n,
        FaultPlan::EveryNth(0) => false,
        FaultPlan::EveryNth(n) => fault.hits.is_multiple_of(n),
    };
    if !fires {
        return Ok(());
    }
    let message = fault.message.clone();
    let disarm_now = matches!(fault.plan, FaultPlan::Once);
    if disarm_now {
        registry.faults.remove(name);
        ARMED_COUNT.fetch_sub(1, Ordering::Release);
    }
    Err(AedbError::Io(std::io::Error::other(message)))
}

#[cfg(test)]
#[path = "faults_tests.rs"]
mod faults_tests;
