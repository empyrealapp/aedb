use crate::error::AedbError;
use crate::snapshot::reader::SnapshotReadView;
use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SnapshotHandle(u64);

#[derive(Debug, Clone)]
pub struct LeakWarning {
    pub handle: SnapshotHandle,
    pub age: Duration,
}

#[derive(Debug, Clone, Default)]
pub struct GcResult {
    pub reclaimed_snapshots: usize,
}

#[derive(Debug)]
struct SnapshotRef {
    view: SnapshotReadView,
    acquired_at: Instant,
    released: bool,
}

#[derive(Debug, Default)]
pub struct SnapshotManager {
    active_snapshots: HashMap<SnapshotHandle, SnapshotRef>,
    next_id: u64,
}

impl SnapshotManager {
    pub fn acquire(&mut self, view: SnapshotReadView) -> SnapshotHandle {
        self.next_id += 1;
        let handle = SnapshotHandle(self.next_id);
        self.active_snapshots.insert(
            handle,
            SnapshotRef {
                view,
                acquired_at: Instant::now(),
                released: false,
            },
        );
        handle
    }

    pub fn acquire_bounded(
        &mut self,
        view: SnapshotReadView,
        max_concurrent_snapshots: usize,
    ) -> Result<SnapshotHandle, AedbError> {
        let active = self
            .active_snapshots
            .values()
            .filter(|s| !s.released)
            .count();
        if active >= max_concurrent_snapshots {
            return Err(AedbError::QueueFull);
        }
        Ok(self.acquire(view))
    }

    pub fn release(&mut self, handle: SnapshotHandle) {
        if let Some(snapshot) = self.active_snapshots.get_mut(&handle) {
            snapshot.released = true;
        }
    }

    pub fn gc(&mut self) -> GcResult {
        let before = self.active_snapshots.len();
        self.active_snapshots.retain(|_, s| !s.released);
        GcResult {
            reclaimed_snapshots: before.saturating_sub(self.active_snapshots.len()),
        }
    }

    pub fn check_leaks(&self, threshold_ms: u64) -> Vec<LeakWarning> {
        let threshold = Duration::from_millis(threshold_ms);
        let now = Instant::now();
        self.active_snapshots
            .iter()
            .filter_map(|(handle, snapshot)| {
                if snapshot.released {
                    return None;
                }
                let age = now.duration_since(snapshot.acquired_at);
                if age >= threshold {
                    Some(LeakWarning {
                        handle: *handle,
                        age,
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn get(&self, handle: SnapshotHandle) -> Option<&SnapshotReadView> {
        self.active_snapshots.get(&handle).map(|s| &s.view)
    }

    pub fn get_checked(
        &mut self,
        handle: SnapshotHandle,
        max_snapshot_age_ms: u64,
    ) -> Result<&SnapshotReadView, AedbError> {
        let Some(snapshot) = self.active_snapshots.get_mut(&handle) else {
            return Err(AedbError::SnapshotExpired);
        };
        if snapshot.released {
            return Err(AedbError::SnapshotExpired);
        }
        if snapshot.acquired_at.elapsed() > Duration::from_millis(max_snapshot_age_ms) {
            snapshot.released = true;
            return Err(AedbError::SnapshotExpired);
        }
        Ok(&snapshot.view)
    }

    pub fn eligible_segment_reclaims(
        &self,
        segment_seqs: &[u64],
        checkpointed_through_seq: u64,
        active_segment_seq: u64,
    ) -> Vec<u64> {
        let has_live_snapshots = self.active_snapshots.values().any(|s| !s.released);
        if has_live_snapshots {
            return Vec::new();
        }
        let mut out: Vec<u64> = segment_seqs
            .iter()
            .copied()
            .filter(|seq| *seq <= checkpointed_through_seq && *seq != active_segment_seq)
            .collect();
        out.sort_unstable();
        out
    }
}

#[cfg(test)]
mod tests {
    use super::SnapshotManager;
    use crate::catalog::Catalog;
    use crate::error::AedbError;
    use crate::snapshot::reader::SnapshotReadView;
    use crate::storage::keyspace::Keyspace;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    fn view(seq: u64) -> SnapshotReadView {
        SnapshotReadView {
            keyspace: Arc::new(Keyspace::default().snapshot()),
            catalog: Arc::new(Catalog::default()),
            seq,
        }
    }

    #[test]
    fn gc_reclaims_only_released_snapshots() {
        let mut mgr = SnapshotManager::default();
        let h1 = mgr.acquire(view(1));
        let h2 = mgr.acquire(view(2));
        let h3 = mgr.acquire(view(3));
        mgr.release(h1);
        mgr.release(h2);
        let res = mgr.gc();
        assert_eq!(res.reclaimed_snapshots, 2);
        assert!(mgr.get(h3).is_some());
        assert!(mgr.get(h1).is_none());
    }

    #[test]
    fn leak_detection_warns_for_old_snapshot() {
        let mut mgr = SnapshotManager::default();
        let _ = mgr.acquire(view(1));
        thread::sleep(Duration::from_millis(5));
        let warnings = mgr.check_leaks(1);
        assert!(!warnings.is_empty());
    }

    #[test]
    fn wal_segment_reclamation_rules() {
        let mut mgr = SnapshotManager::default();
        let h = mgr.acquire(view(4));
        let segments = vec![1, 2, 3, 4, 5];

        let none = mgr.eligible_segment_reclaims(&segments, 4, 5);
        assert!(none.is_empty());

        mgr.release(h);
        let _ = mgr.gc();
        let eligible = mgr.eligible_segment_reclaims(&segments, 4, 5);
        assert_eq!(eligible, vec![1, 2, 3, 4]);
    }

    #[test]
    fn bounded_acquire_and_expiry_guardrails() {
        let mut mgr = SnapshotManager::default();
        let _h1 = mgr.acquire_bounded(view(1), 1).expect("first");
        let err = mgr.acquire_bounded(view(2), 1).expect_err("bounded");
        assert!(matches!(err, AedbError::QueueFull));

        let mut mgr = SnapshotManager::default();
        let h = mgr.acquire(view(1));
        thread::sleep(Duration::from_millis(3));
        let expired = mgr.get_checked(h, 1).expect_err("expired");
        assert!(matches!(expired, AedbError::SnapshotExpired));
    }
}
