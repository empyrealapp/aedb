use crate::AedbInstance;
use crate::commit::executor::CommitResult;
use crate::commit::tx::{PreflightPlan, TransactionEnvelope, WriteClass};
use crate::error::AedbError;
use crate::permission::CallerContext;

pub(crate) fn preflight_reason_to_error(reason: String) -> AedbError {
    if reason == AedbError::Underflow.to_string() {
        return AedbError::Underflow;
    }
    if let Some(detail) = reason.strip_prefix("conflict error: ") {
        return AedbError::Conflict(detail.to_string());
    }
    if let Some(detail) = reason.strip_prefix("validation error: ") {
        return AedbError::Validation(detail.to_string());
    }
    AedbError::Validation(reason)
}

pub(crate) async fn commit_from_preflight_plan(
    db: &AedbInstance,
    caller: Option<CallerContext>,
    plan: PreflightPlan,
) -> Result<CommitResult, AedbError> {
    if !plan.valid {
        let reason = plan
            .errors
            .first()
            .cloned()
            .unwrap_or_else(|| "preflight failed".to_string());
        return Err(preflight_reason_to_error(reason));
    }
    // Preflight is an advisory snapshot read. The read set captured in the
    // plan is what makes the eventual commit safe: if the values/ranges used
    // by preflight changed before this envelope reaches the executor, conflict
    // detection rejects the write instead of applying a stale decision.
    db.commit_envelope(TransactionEnvelope {
        caller,
        idempotency_key: None,
        write_class: WriteClass::Standard,
        assertions: Vec::new(),
        read_set: plan.read_set,
        write_intent: plan.write_intent,
        base_seq: plan.base_seq,
    })
    .await
}
