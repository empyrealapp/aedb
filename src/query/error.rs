use crate::error::AedbError;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryErrorCode {
    TableNotFound,
    ColumnNotFound,
    TypeMismatch,
    ScanBoundExceeded,
    MaterializationBudgetExceeded,
    InvalidQuery,
    PermissionDenied,
    SeqNotYetVisible,
    SeqGarbageCollected,
    CursorExpired,
    InvalidCursor,
    SnapshotExpired,
    SnapshotLimitReached,
    InternalError,
}

/// Broad caller-action class for [`QueryError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryErrorClass {
    /// The query can be retried after backoff or snapshot renewal.
    Retryable,
    /// The caller identity lacks the required read permission.
    Permission,
    /// The query, cursor, schema reference, or requested snapshot is invalid.
    Validation,
    /// The requested scan exceeds configured limits.
    Unavailable,
    /// Internal storage/query failure. Surface for operator investigation.
    Integrity,
}

impl QueryErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            QueryErrorCode::TableNotFound => "table_not_found",
            QueryErrorCode::ColumnNotFound => "column_not_found",
            QueryErrorCode::TypeMismatch => "type_mismatch",
            QueryErrorCode::ScanBoundExceeded => "scan_bound_exceeded",
            QueryErrorCode::MaterializationBudgetExceeded => "materialization_budget_exceeded",
            QueryErrorCode::InvalidQuery => "invalid_query",
            QueryErrorCode::PermissionDenied => "permission_denied",
            QueryErrorCode::SeqNotYetVisible => "seq_not_yet_visible",
            QueryErrorCode::SeqGarbageCollected => "seq_garbage_collected",
            QueryErrorCode::CursorExpired => "cursor_expired",
            QueryErrorCode::InvalidCursor => "invalid_cursor",
            QueryErrorCode::SnapshotExpired => "snapshot_expired",
            QueryErrorCode::SnapshotLimitReached => "snapshot_limit_reached",
            QueryErrorCode::InternalError => "internal_error",
        }
    }

    pub fn class(self) -> QueryErrorClass {
        match self {
            QueryErrorCode::SeqNotYetVisible
            | QueryErrorCode::CursorExpired
            | QueryErrorCode::SnapshotExpired
            | QueryErrorCode::SnapshotLimitReached => QueryErrorClass::Retryable,
            QueryErrorCode::PermissionDenied => QueryErrorClass::Permission,
            QueryErrorCode::ScanBoundExceeded
            | QueryErrorCode::MaterializationBudgetExceeded => QueryErrorClass::Unavailable,
            QueryErrorCode::InternalError => QueryErrorClass::Integrity,
            QueryErrorCode::TableNotFound
            | QueryErrorCode::ColumnNotFound
            | QueryErrorCode::TypeMismatch
            | QueryErrorCode::InvalidQuery
            | QueryErrorCode::SeqGarbageCollected
            | QueryErrorCode::InvalidCursor => QueryErrorClass::Validation,
        }
    }

    pub fn is_retryable(self) -> bool {
        matches!(self.class(), QueryErrorClass::Retryable)
    }
}

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
    /// A query (currently joins) would buffer more bytes of materialized rows
    /// than the configured ceiling allows. Guards the host against OOM from a
    /// query that returns few but very large rows.
    MaterializationBudgetExceeded {
        estimated_bytes: u64,
        max_bytes: u64,
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
    /// Cursor was malformed, tampered with, or failed HMAC verification.
    InvalidCursor,
    SnapshotExpired,
    SnapshotLimitReached,
    InternalError(String),
}

fn parse_table_resource_id(resource_id: &str) -> (String, String) {
    let mut parts = resource_id.split('.');
    let project_id = parts.next().unwrap_or_default().to_string();
    let table = resource_id
        .rsplit('.')
        .next()
        .unwrap_or(resource_id)
        .to_string();
    (project_id, table)
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
            QueryError::MaterializationBudgetExceeded {
                estimated_bytes,
                max_bytes,
            } => write!(
                f,
                "materialization budget exceeded: estimated_bytes={estimated_bytes}, max_bytes={max_bytes}"
            ),
            QueryError::InvalidQuery { reason } => write!(f, "invalid query: {reason}"),
            QueryError::PermissionDenied { .. } => {
                write!(f, "permission denied: permission denied")
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
            QueryError::InvalidCursor => write!(f, "invalid cursor (tampered or malformed)"),
            QueryError::SnapshotExpired => write!(f, "snapshot expired"),
            QueryError::SnapshotLimitReached => write!(f, "snapshot limit reached"),
            QueryError::InternalError(msg) => write!(f, "internal query error: {msg}"),
        }
    }
}

impl std::error::Error for QueryError {}

impl QueryError {
    pub fn is_table_not_found(&self) -> bool {
        matches!(self, QueryError::TableNotFound { .. })
    }

    pub fn code(&self) -> QueryErrorCode {
        match self {
            QueryError::TableNotFound { .. } => QueryErrorCode::TableNotFound,
            QueryError::ColumnNotFound { .. } => QueryErrorCode::ColumnNotFound,
            QueryError::TypeMismatch { .. } => QueryErrorCode::TypeMismatch,
            QueryError::ScanBoundExceeded { .. } => QueryErrorCode::ScanBoundExceeded,
            QueryError::MaterializationBudgetExceeded { .. } => {
                QueryErrorCode::MaterializationBudgetExceeded
            }
            QueryError::InvalidQuery { .. } => QueryErrorCode::InvalidQuery,
            QueryError::PermissionDenied { .. } => QueryErrorCode::PermissionDenied,
            QueryError::SeqNotYetVisible { .. } => QueryErrorCode::SeqNotYetVisible,
            QueryError::SeqGarbageCollected { .. } => QueryErrorCode::SeqGarbageCollected,
            QueryError::CursorExpired { .. } => QueryErrorCode::CursorExpired,
            QueryError::InvalidCursor => QueryErrorCode::InvalidCursor,
            QueryError::SnapshotExpired => QueryErrorCode::SnapshotExpired,
            QueryError::SnapshotLimitReached => QueryErrorCode::SnapshotLimitReached,
            QueryError::InternalError(_) => QueryErrorCode::InternalError,
        }
    }

    pub fn code_str(&self) -> &'static str {
        self.code().as_str()
    }

    pub fn class(&self) -> QueryErrorClass {
        self.code().class()
    }

    pub fn is_retryable(&self) -> bool {
        self.code().is_retryable()
    }
}

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
            AedbError::RecoveryIntegrity { diagnostic } => {
                QueryError::InternalError(diagnostic.to_string())
            }
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
            } => match resource_type {
                crate::error::ResourceType::Table => {
                    let (project_id, table) = parse_table_resource_id(&resource_id);
                    QueryError::TableNotFound { project_id, table }
                }
                _ => QueryError::InvalidQuery {
                    reason: format!("{resource_type} '{resource_id}' not found"),
                },
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
mod tests;
