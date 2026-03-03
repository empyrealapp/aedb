use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GroupCommitPhase {
    Filling,
    Flushing,
    Complete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GroupCommitFlushReason {
    MaxGroupSize,
    MaxGroupDelay,
    IngressDrained,
    StructuralBarrier,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct GroupCommitPolicy {
    pub max_group_size: usize,
    pub max_group_delay: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct GroupCommitEpochSnapshot {
    pub phase: GroupCommitPhase,
    pub epoch: u64,
    pub pending_commits: usize,
    pub flush_reason: Option<GroupCommitFlushReason>,
}

#[derive(Debug)]
pub(super) struct GroupCommitStateMachine {
    phase: GroupCommitPhase,
    epoch: u64,
    policy: GroupCommitPolicy,
    filling_started: Option<Instant>,
    pending_commits: usize,
    flush_reason: Option<GroupCommitFlushReason>,
}

impl GroupCommitStateMachine {
    pub(super) fn new(policy: GroupCommitPolicy) -> Self {
        Self {
            phase: GroupCommitPhase::Complete,
            epoch: 0,
            policy: sanitize_policy(policy),
            filling_started: None,
            pending_commits: 0,
            flush_reason: None,
        }
    }

    pub(super) fn begin_filling(&mut self, now: Instant, policy: GroupCommitPolicy) {
        self.policy = sanitize_policy(policy);
        self.phase = GroupCommitPhase::Filling;
        self.filling_started = Some(now);
        self.pending_commits = 0;
        self.flush_reason = None;
    }

    pub(super) fn record_pending_commits(&mut self, commits: usize) {
        self.pending_commits = commits;
    }

    pub(super) fn begin_flushing(&mut self, reason: GroupCommitFlushReason) {
        assert!(
            matches!(self.phase, GroupCommitPhase::Filling),
            "group commit phase invariant violated: begin_flushing outside Filling"
        );
        self.phase = GroupCommitPhase::Flushing;
        self.flush_reason = Some(reason);
        self.epoch = self.epoch.saturating_add(1);
    }

    pub(super) fn complete_flush(&mut self) {
        assert!(
            matches!(self.phase, GroupCommitPhase::Flushing),
            "group commit phase invariant violated: complete_flush outside Flushing"
        );
        self.phase = GroupCommitPhase::Complete;
        self.pending_commits = 0;
        self.filling_started = None;
    }

    pub(super) fn reached_size_limit(&self) -> bool {
        self.pending_commits >= self.policy.max_group_size
    }

    pub(super) fn reached_delay_limit(&self, now: Instant) -> bool {
        self.filling_started
            .is_some_and(|started| now.duration_since(started) >= self.policy.max_group_delay)
    }

    pub(super) fn snapshot(&self) -> GroupCommitEpochSnapshot {
        GroupCommitEpochSnapshot {
            phase: self.phase,
            epoch: self.epoch,
            pending_commits: self.pending_commits,
            flush_reason: self.flush_reason,
        }
    }
}

fn sanitize_policy(policy: GroupCommitPolicy) -> GroupCommitPolicy {
    GroupCommitPolicy {
        max_group_size: policy.max_group_size.max(1),
        max_group_delay: if policy.max_group_delay.is_zero() {
            Duration::from_micros(1)
        } else {
            policy.max_group_delay
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{
        GroupCommitFlushReason, GroupCommitPhase, GroupCommitPolicy, GroupCommitStateMachine,
    };
    use std::time::{Duration, Instant};

    #[test]
    fn state_machine_filling_flushing_complete_cycle() {
        let now = Instant::now();
        let mut sm = GroupCommitStateMachine::new(GroupCommitPolicy {
            max_group_size: 4,
            max_group_delay: Duration::from_millis(2),
        });

        sm.begin_filling(
            now,
            GroupCommitPolicy {
                max_group_size: 4,
                max_group_delay: Duration::from_millis(2),
            },
        );
        assert_eq!(sm.snapshot().phase, GroupCommitPhase::Filling);
        sm.record_pending_commits(4);
        assert!(sm.reached_size_limit());
        sm.begin_flushing(GroupCommitFlushReason::MaxGroupSize);
        assert_eq!(sm.snapshot().phase, GroupCommitPhase::Flushing);
        assert_eq!(
            sm.snapshot().flush_reason,
            Some(GroupCommitFlushReason::MaxGroupSize)
        );
        sm.complete_flush();
        assert_eq!(sm.snapshot().phase, GroupCommitPhase::Complete);
    }

    #[test]
    fn state_machine_delay_limit_detection() {
        let mut sm = GroupCommitStateMachine::new(GroupCommitPolicy {
            max_group_size: 16,
            max_group_delay: Duration::from_millis(5),
        });
        let started = Instant::now();
        sm.begin_filling(
            started,
            GroupCommitPolicy {
                max_group_size: 16,
                max_group_delay: Duration::from_millis(5),
            },
        );
        sm.record_pending_commits(1);
        assert!(!sm.reached_delay_limit(started + Duration::from_millis(1)));
        assert!(sm.reached_delay_limit(started + Duration::from_millis(6)));
    }

    #[test]
    fn state_machine_sanitizes_zero_policy_values() {
        let mut sm = GroupCommitStateMachine::new(GroupCommitPolicy {
            max_group_size: 0,
            max_group_delay: Duration::ZERO,
        });
        sm.begin_filling(
            Instant::now(),
            GroupCommitPolicy {
                max_group_size: 0,
                max_group_delay: Duration::ZERO,
            },
        );
        sm.record_pending_commits(1);
        assert!(sm.reached_size_limit());
        assert!(sm.reached_delay_limit(Instant::now() + Duration::from_millis(1)));
    }
}
