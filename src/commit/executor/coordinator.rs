use crate::error::AedbError;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};
use tracing::error;

#[derive(Default)]
struct LockState {
    held: HashSet<String>,
}

pub(super) struct CoordinatorLockManager {
    state: Mutex<LockState>,
    cv: Condvar,
    /// Set to true if a thread panics while holding the lock, indicating
    /// invariants may be violated. After poisoning, all new operations are rejected.
    poisoned: AtomicBool,
}

impl Default for CoordinatorLockManager {
    fn default() -> Self {
        Self {
            state: Mutex::new(LockState::default()),
            cv: Condvar::default(),
            poisoned: AtomicBool::new(false),
        }
    }
}

impl CoordinatorLockManager {
    /// Acquires locks for all specified partitions in sorted order to prevent deadlocks.
    ///
    /// CRITICAL: This function defensively sorts the partitions to ensure consistent lock
    /// ordering across all threads. This prevents deadlocks when multiple transactions
    /// attempt to acquire locks on overlapping sets of partitions.
    pub(super) fn acquire_all(
        &self,
        ordered_partitions: &[String],
        timeout: Duration,
    ) -> Result<CoordinatorLockGuard<'_>, AedbError> {
        // Check fail-safe flag: reject all operations after lock poisoning
        if self.poisoned.load(Ordering::Acquire) {
            return Err(AedbError::Validation(
                "coordinator lock poisoned - system in fail-safe mode".into(),
            ));
        }

        if ordered_partitions.is_empty() {
            return Ok(CoordinatorLockGuard {
                manager: self,
                held: Vec::new(),
            });
        }

        // Defensive sort to enforce consistent lock ordering and prevent deadlock
        let mut sorted_partitions = ordered_partitions.to_vec();
        sorted_partitions.sort();

        // Debug assertion to catch callers that don't pre-sort (will be optimized out in release)
        debug_assert!(
            ordered_partitions.windows(2).all(|w| w[0] <= w[1]),
            "acquire_all called with unsorted partitions: {:?}",
            ordered_partitions
        );

        let deadline = Instant::now() + timeout;
        let mut state = self.state.lock().map_err(|_| {
            // Lock poisoned - enter fail-safe mode and reject all future operations
            self.poisoned.store(true, Ordering::Release);
            error!(
                "coordinator lock poisoned - invariants may be violated, entering fail-safe mode"
            );
            AedbError::Validation("coordinator lock poisoned - entering fail-safe mode".into())
        })?;
        loop {
            let blocked = sorted_partitions.iter().any(|p| state.held.contains(p));
            if !blocked {
                for p in &sorted_partitions {
                    state.held.insert(p.clone());
                }
                return Ok(CoordinatorLockGuard {
                    manager: self,
                    held: sorted_partitions,
                });
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(AedbError::PartitionLockTimeout);
            }
            let remaining = deadline.saturating_duration_since(now);
            let (new_state, wait_result) =
                self.cv.wait_timeout(state, remaining).map_err(|_| {
                    // Lock poisoned during wait - enter fail-safe mode
                    self.poisoned.store(true, Ordering::Release);
                    error!("coordinator lock poisoned during wait - entering fail-safe mode");
                    AedbError::Validation(
                        "coordinator lock poisoned - entering fail-safe mode".into(),
                    )
                })?;
            state = new_state;
            if wait_result.timed_out() {
                return Err(AedbError::PartitionLockTimeout);
            }
        }
    }
}

pub(super) struct CoordinatorLockGuard<'a> {
    manager: &'a CoordinatorLockManager,
    held: Vec<String>,
}

impl Drop for CoordinatorLockGuard<'_> {
    fn drop(&mut self) {
        if self.held.is_empty() {
            return;
        }

        // Attempt to release locks, but enter fail-safe mode if poisoned
        match self.manager.state.lock() {
            Ok(mut state) => {
                for p in &self.held {
                    state.held.remove(p);
                }
                drop(state);
                self.manager.cv.notify_all();
            }
            Err(_) => {
                // Lock poisoned during drop - enter fail-safe mode
                self.manager.poisoned.store(true, Ordering::Release);
                error!("coordinator lock poisoned during guard drop - entering fail-safe mode");
                // Cannot return error from Drop, but fail-safe flag prevents future operations
            }
        }
    }
}
