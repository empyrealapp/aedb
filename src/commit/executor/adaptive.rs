use crate::config::AedbConfig;
use std::time::Duration;

#[derive(Debug, Clone)]
pub(super) struct AdaptiveEpochState {
    min_commits: usize,
    max_wait_us: u64,
    ewma_latency_us: u64,
}

impl AdaptiveEpochState {
    pub(super) fn from_config(config: &AedbConfig) -> Self {
        Self {
            min_commits: config
                .epoch_min_commits
                .clamp(1, config.epoch_max_commits.max(1)),
            max_wait_us: config.epoch_max_wait_us.max(1),
            ewma_latency_us: config.adaptive_epoch_target_latency_us.max(1),
        }
    }

    pub(super) fn epoch_params(
        &self,
        config: &AedbConfig,
        pending_len: usize,
    ) -> (u64, usize, usize) {
        let max_commits = config.epoch_max_commits.max(1);
        if !config.adaptive_epoch_enabled {
            return (
                config.epoch_max_wait_us.max(1),
                config.epoch_min_commits.clamp(1, max_commits),
                max_commits,
            );
        }
        let mut min_commits = self
            .min_commits
            .clamp(config.adaptive_epoch_min_commits_floor.max(1), max_commits)
            .clamp(1, config.adaptive_epoch_min_commits_ceiling.max(1));
        if pending_len > min_commits.saturating_mul(3) {
            min_commits = min_commits.saturating_add(1).min(max_commits);
        }
        let wait = self.max_wait_us.clamp(
            config.adaptive_epoch_wait_us_floor.max(1),
            config.adaptive_epoch_wait_us_ceiling.max(1),
        );
        (wait, min_commits, max_commits)
    }

    pub(super) fn observe_epoch(
        &mut self,
        config: &AedbConfig,
        request_count: usize,
        elapsed: Duration,
        had_error: bool,
        pending_after_epoch: usize,
    ) {
        if !config.adaptive_epoch_enabled || request_count == 0 {
            return;
        }
        let per_commit = (elapsed.as_micros() as u64).max(1) / (request_count as u64).max(1);
        self.ewma_latency_us = ((self.ewma_latency_us * 7) + per_commit) / 8;

        let floor = config.adaptive_epoch_min_commits_floor.max(1);
        let ceiling = config
            .adaptive_epoch_min_commits_ceiling
            .max(floor)
            .min(config.epoch_max_commits.max(1));
        let wait_floor = config.adaptive_epoch_wait_us_floor.max(1);
        let wait_ceiling = config.adaptive_epoch_wait_us_ceiling.max(wait_floor);
        let target = config.adaptive_epoch_target_latency_us.max(1);

        if had_error || self.ewma_latency_us > target.saturating_mul(2) {
            self.min_commits = self.min_commits.saturating_sub(1).max(floor);
            self.max_wait_us = self.max_wait_us.saturating_add(25).min(wait_ceiling);
            return;
        }

        if pending_after_epoch > self.min_commits.saturating_mul(2)
            && self.ewma_latency_us <= target
        {
            self.min_commits = self.min_commits.saturating_add(1).min(ceiling);
            self.max_wait_us = self.max_wait_us.saturating_sub(10).max(wait_floor);
            return;
        }

        if pending_after_epoch <= 1 {
            self.min_commits = self.min_commits.saturating_sub(1).max(floor);
            self.max_wait_us = self.max_wait_us.saturating_add(10).min(wait_ceiling);
        }
    }

    pub(super) fn current_min_commits(&self) -> usize {
        self.min_commits
    }

    pub(super) fn current_max_wait_us(&self) -> u64 {
        self.max_wait_us
    }
}
