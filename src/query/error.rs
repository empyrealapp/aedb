use crate::error::AedbError;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryError {
    TableNotFound {
        project_id: String,
        table: String,
    },
    ColumnNotFound {
        table: String,
        column: String,
    },
    TypeMismatch {
        column: String,
        expected: String,
        got: String,
    },
    ScanBoundExceeded {
        estimated_rows: u64,
        max_scan_rows: u64,
    },
    InvalidQuery {
        reason: String,
    },
    PermissionDenied {
        permission: String,
        scope: String,
    },
    SeqNotYetVisible {
        requested: u64,
        current: u64,
    },
    SeqGarbageCollected {
        requested: u64,
        oldest_available: u64,
    },
    CursorExpired {
        original_seq: u64,
    },
    SnapshotExpired,
    SnapshotLimitReached,
    InternalError(String),
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryError::TableNotFound { project_id, table } => {
                write!(f, "table '{table}' not found in project '{project_id}'")
            }
            QueryError::ColumnNotFound { table, column } => {
                write!(f, "column '{column}' not found in table '{table}'")
            }
            QueryError::TypeMismatch {
                column,
                expected,
                got,
            } => {
                write!(
                    f,
                    "type mismatch in column '{column}': expected {expected}, got {got}"
                )
            }
            QueryError::ScanBoundExceeded {
                estimated_rows,
                max_scan_rows,
            } => write!(
                f,
                "scan bound exceeded: estimated_rows={estimated_rows}, max_scan_rows={max_scan_rows}"
            ),
            QueryError::InvalidQuery { reason } => write!(f, "invalid query: {reason}"),
            QueryError::PermissionDenied { permission, scope } => {
                write!(f, "permission denied: {permission} (scope={scope})")
            }
            QueryError::SeqNotYetVisible { requested, current } => write!(
                f,
                "requested seq {requested} is not yet visible (current={current})"
            ),
            QueryError::SeqGarbageCollected {
                requested,
                oldest_available,
            } => write!(
                f,
                "requested seq {requested} was garbage collected (oldest_available={oldest_available})"
            ),
            QueryError::CursorExpired { original_seq } => {
                write!(f, "cursor expired (original_seq={original_seq})")
            }
            QueryError::SnapshotExpired => write!(f, "snapshot expired"),
            QueryError::SnapshotLimitReached => write!(f, "snapshot limit reached"),
            QueryError::InternalError(msg) => write!(f, "internal query error: {msg}"),
        }
    }
}

impl std::error::Error for QueryError {}

impl From<AedbError> for QueryError {
    fn from(value: AedbError) -> Self {
        match value {
            AedbError::PermissionDenied(msg) => QueryError::PermissionDenied {
                permission: msg,
                scope: "query".to_string(),
            },
            AedbError::SnapshotExpired => QueryError::SnapshotExpired,
            AedbError::Validation(reason) => QueryError::InvalidQuery { reason },
            AedbError::InvalidConfig { message } => QueryError::InvalidQuery { reason: message },
            AedbError::IntegrityError { message } => QueryError::InternalError(message),
            AedbError::Unavailable { message } => QueryError::InternalError(message),
            AedbError::CheckpointInProgress => {
                QueryError::InternalError("checkpoint in progress".into())
            }
            AedbError::Decode(reason) => QueryError::InvalidQuery { reason },
            AedbError::AlreadyExists {
                resource_type,
                resource_id,
            } => QueryError::InvalidQuery {
                reason: format!("{resource_type} '{resource_id}' already exists"),
            },
            AedbError::NotFound {
                resource_type,
                resource_id,
            } => QueryError::InvalidQuery {
                reason: format!("{resource_type} '{resource_id}' not found"),
            },
            AedbError::DuplicatePK { table, key } => QueryError::InvalidQuery {
                reason: format!("duplicate primary key in table '{table}': {key}"),
            },
            AedbError::UniqueViolation { table, index, .. } => QueryError::InvalidQuery {
                reason: format!(
                    "unique constraint violation on index '{index}' in table '{table}'"
                ),
            },
            AedbError::ForeignKeyViolation {
                fk_name,
                ref_table,
                ref_key,
                ..
            } => QueryError::InvalidQuery {
                reason: format!(
                    "foreign key violation: {fk_name} references {ref_table}({ref_key})"
                ),
            },
            AedbError::CheckConstraintFailed { table, constraint } => QueryError::InvalidQuery {
                reason: format!("check constraint '{constraint}' failed on table '{table}'"),
            },
            AedbError::NotNullViolation { table, column } => QueryError::InvalidQuery {
                reason: format!("NOT NULL violation: column '{column}' in table '{table}'"),
            },
            AedbError::TypeMismatch {
                table,
                column,
                expected,
                actual,
            } => QueryError::InvalidQuery {
                reason: format!(
                    "type mismatch in table '{table}' column '{column}': expected {expected}, got {actual}"
                ),
            },
            AedbError::UnknownColumn { table, column } => QueryError::InvalidQuery {
                reason: format!("unknown column '{column}' in table '{table}'"),
            },
            AedbError::Encode(reason) => QueryError::InternalError(reason),
            AedbError::Io(e) => QueryError::InternalError(e.to_string()),
            AedbError::Conflict(reason) => QueryError::InternalError(reason),
            AedbError::Underflow => QueryError::InternalError("underflow".into()),
            AedbError::Overflow => QueryError::InternalError("overflow".into()),
            AedbError::QueueFull => QueryError::SnapshotLimitReached,
            AedbError::Timeout => QueryError::InternalError("timeout".into()),
            AedbError::PartitionLockTimeout => {
                QueryError::InternalError("partition lock timeout".into())
            }
            AedbError::EpochApplyTimeout => {
                QueryError::InternalError("epoch apply timeout exceeded".into())
            }
            AedbError::ParallelApplyCancelled => {
                QueryError::InternalError("parallel apply cancelled".into())
            }
            AedbError::ParallelApplyWorkerPanicked => {
                QueryError::InternalError("parallel apply worker panicked".into())
            }
            AedbError::AssertionFailed { .. } => {
                QueryError::InternalError("assertion failed".into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::QueryError;

    #[test]
    fn query_error_display_is_human_readable() {
        let err = QueryError::ColumnNotFound {
            table: "users".into(),
            column: "name".into(),
        };
        assert_eq!(err.to_string(), "column 'name' not found in table 'users'");
    }
}
