use crate::query::error::QueryError;
use crate::query::executor::cursor::{CursorToken, decode_cursor};
use crate::query::plan::{Query, QueryOptions};

pub(super) struct ExecutionSetup {
    pub(super) options: QueryOptions,
    pub(super) cursor_state: Option<CursorToken>,
}

pub(super) fn prepare_execution_setup(
    query: &Query,
    options: &QueryOptions,
    snapshot_seq: u64,
    cursor_signing_key: Option<&[u8; 32]>,
) -> Result<ExecutionSetup, QueryError> {
    let mut options = options.clone();
    if options.async_index.is_none() {
        options.async_index = query.use_index.clone();
    }

    validate_expression_depth(query)?;
    validate_full_scan_policy(query, &options)?;

    let cursor_state = match &options.cursor {
        Some(encoded) => Some(decode_cursor(encoded, cursor_signing_key)?),
        None => None,
    };
    validate_cursor_state(query, snapshot_seq, &cursor_state)?;

    Ok(ExecutionSetup {
        options,
        cursor_state,
    })
}

fn validate_expression_depth(query: &Query) -> Result<(), QueryError> {
    if let Some(pred) = &query.predicate {
        pred.validate_depth()
            .map_err(|e| QueryError::InvalidQuery {
                reason: e.to_string(),
            })?;
    }
    if let Some(having) = &query.having {
        having
            .validate_depth()
            .map_err(|e| QueryError::InvalidQuery {
                reason: e.to_string(),
            })?;
    }
    Ok(())
}

fn validate_full_scan_policy(query: &Query, options: &QueryOptions) -> Result<(), QueryError> {
    if !options.allow_full_scan
        && query.limit.is_none()
        && query.offset.is_none()
        && query.predicate.is_none()
        && query.group_by.is_empty()
        && query.aggregates.is_empty()
        && query.having.is_none()
    {
        return Err(QueryError::InvalidQuery {
            reason: "full scan requires limit/cursor or allow_full_scan".into(),
        });
    }
    Ok(())
}

fn validate_cursor_state(
    query: &Query,
    snapshot_seq: u64,
    cursor_state: &Option<CursorToken>,
) -> Result<(), QueryError> {
    if let Some(cursor) = cursor_state
        && cursor.snapshot_seq != snapshot_seq
    {
        return Err(QueryError::InvalidQuery {
            reason: "cursor snapshot_seq mismatch".into(),
        });
    }
    if cursor_state.is_some() && query.offset.unwrap_or(0) > 0 {
        return Err(QueryError::InvalidQuery {
            reason: "OFFSET cannot be combined with cursor pagination".into(),
        });
    }
    Ok(())
}
