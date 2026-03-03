use crate::commit::tx::{IdempotencyKey, ReadAssertion, WriteClass};
use crate::commit::validation::Mutation;
use crate::permission::CallerContext;

#[derive(Debug, Clone)]
pub struct ActionEnvelopeRequest {
    pub caller: Option<CallerContext>,
    pub idempotency_key: IdempotencyKey,
    pub write_class: WriteClass,
    pub base_seq: u64,
    pub assertions: Vec<ReadAssertion>,
    pub mutations: Vec<Mutation>,
}

#[derive(Debug, Clone)]
pub struct ActionCommitResult {
    pub commit_seq: u64,
    pub durable_head_seq: u64,
    pub outcome: ActionCommitOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionCommitOutcome {
    Applied,
    Duplicate,
}
