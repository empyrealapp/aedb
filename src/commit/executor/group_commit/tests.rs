use crate::commit::executor::group_commit::{
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
