use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceType {
    Project,
    Scope,
    Table,
    Index,
}

impl std::fmt::Display for ResourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResourceType::Project => write!(f, "project"),
            ResourceType::Scope => write!(f, "scope"),
            ResourceType::Table => write!(f, "table"),
            ResourceType::Index => write!(f, "index"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AedbErrorCode {
    Io,
    Encode,
    Decode,
    Validation,
    InvalidConfig,
    IntegrityError,
    Unavailable,
    CheckpointInProgress,
    ProjectAlreadyExists,
    ScopeAlreadyExists,
    TableAlreadyExists,
    IndexAlreadyExists,
    ProjectNotFound,
    ScopeNotFound,
    TableNotFound,
    IndexNotFound,
    DuplicatePrimaryKey,
    UniqueViolation,
    ForeignKeyViolation,
    CheckConstraintFailed,
    NotNullViolation,
    TypeMismatch,
    UnknownColumn,
    Conflict,
    Underflow,
    Overflow,
    QueueFull,
    PermissionDenied,
    Timeout,
    PartitionLockTimeout,
    EpochApplyTimeout,
    ParallelApplyCancelled,
    ParallelApplyWorkerPanicked,
    SnapshotExpired,
    AssertionFailed,
}

impl AedbErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            AedbErrorCode::Io => "io",
            AedbErrorCode::Encode => "encode",
            AedbErrorCode::Decode => "decode",
            AedbErrorCode::Validation => "validation",
            AedbErrorCode::InvalidConfig => "invalid_config",
            AedbErrorCode::IntegrityError => "integrity_error",
            AedbErrorCode::Unavailable => "unavailable",
            AedbErrorCode::CheckpointInProgress => "checkpoint_in_progress",
            AedbErrorCode::ProjectAlreadyExists => "project_already_exists",
            AedbErrorCode::ScopeAlreadyExists => "scope_already_exists",
            AedbErrorCode::TableAlreadyExists => "table_already_exists",
            AedbErrorCode::IndexAlreadyExists => "index_already_exists",
            AedbErrorCode::ProjectNotFound => "project_not_found",
            AedbErrorCode::ScopeNotFound => "scope_not_found",
            AedbErrorCode::TableNotFound => "table_not_found",
            AedbErrorCode::IndexNotFound => "index_not_found",
            AedbErrorCode::DuplicatePrimaryKey => "duplicate_primary_key",
            AedbErrorCode::UniqueViolation => "unique_violation",
            AedbErrorCode::ForeignKeyViolation => "foreign_key_violation",
            AedbErrorCode::CheckConstraintFailed => "check_constraint_failed",
            AedbErrorCode::NotNullViolation => "not_null_violation",
            AedbErrorCode::TypeMismatch => "type_mismatch",
            AedbErrorCode::UnknownColumn => "unknown_column",
            AedbErrorCode::Conflict => "conflict",
            AedbErrorCode::Underflow => "underflow",
            AedbErrorCode::Overflow => "overflow",
            AedbErrorCode::QueueFull => "queue_full",
            AedbErrorCode::PermissionDenied => "permission_denied",
            AedbErrorCode::Timeout => "timeout",
            AedbErrorCode::PartitionLockTimeout => "partition_lock_timeout",
            AedbErrorCode::EpochApplyTimeout => "epoch_apply_timeout",
            AedbErrorCode::ParallelApplyCancelled => "parallel_apply_cancelled",
            AedbErrorCode::ParallelApplyWorkerPanicked => "parallel_apply_worker_panicked",
            AedbErrorCode::SnapshotExpired => "snapshot_expired",
            AedbErrorCode::AssertionFailed => "assertion_failed",
        }
    }
}

#[derive(Debug, Error)]
pub enum AedbError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("encode error: {0}")]
    Encode(String),
    #[error("decode error: {0}")]
    Decode(String),
    #[error("validation error: {0}")]
    Validation(String),
    #[error("invalid config: {message}")]
    InvalidConfig { message: String },
    #[error("integrity error: {message}")]
    IntegrityError { message: String },
    #[error("resource unavailable: {message}")]
    Unavailable { message: String },
    #[error("checkpoint in progress")]
    CheckpointInProgress,
    #[error("{resource_type} '{resource_id}' already exists")]
    AlreadyExists {
        resource_type: ResourceType,
        resource_id: String,
    },
    #[error("{resource_type} '{resource_id}' not found")]
    NotFound {
        resource_type: ResourceType,
        resource_id: String,
    },
    #[error("duplicate primary key in table '{table}': {key}")]
    DuplicatePK { table: String, key: String },
    #[error("unique constraint violation on index '{index}' in table '{table}'")]
    UniqueViolation {
        table: String,
        index: String,
        key: String,
    },
    #[error("foreign key violation: {fk_name} references {ref_table}({ref_key})")]
    ForeignKeyViolation {
        fk_name: String,
        table: String,
        ref_table: String,
        ref_key: String,
    },
    #[error("check constraint '{constraint}' failed on table '{table}'")]
    CheckConstraintFailed { table: String, constraint: String },
    #[error("NOT NULL violation: column '{column}' in table '{table}'")]
    NotNullViolation { table: String, column: String },
    #[error(
        "type mismatch: column '{column}' in table '{table}' expected {expected}, got {actual}"
    )]
    TypeMismatch {
        table: String,
        column: String,
        expected: String,
        actual: String,
    },
    #[error("unknown column '{column}' in table '{table}'")]
    UnknownColumn { table: String, column: String },
    #[error("conflict error: {0}")]
    Conflict(String),
    #[error("underflow")]
    Underflow,
    #[error("overflow")]
    Overflow,
    #[error("queue full")]
    QueueFull,
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    #[error("timeout")]
    Timeout,
    #[error("partition lock timeout")]
    PartitionLockTimeout,
    #[error("epoch apply timeout exceeded")]
    EpochApplyTimeout,
    #[error("parallel apply cancelled")]
    ParallelApplyCancelled,
    #[error("parallel apply worker panicked")]
    ParallelApplyWorkerPanicked,
    #[error("snapshot expired")]
    SnapshotExpired,
    #[error("assertion failed at index {index}")]
    AssertionFailed {
        index: usize,
        assertion: Box<crate::commit::tx::ReadAssertion>,
        actual: Box<crate::commit::tx::AssertionActual>,
    },
}

impl AedbError {
    pub fn code(&self) -> AedbErrorCode {
        match self {
            AedbError::Io(_) => AedbErrorCode::Io,
            AedbError::Encode(_) => AedbErrorCode::Encode,
            AedbError::Decode(_) => AedbErrorCode::Decode,
            AedbError::Validation(_) => AedbErrorCode::Validation,
            AedbError::InvalidConfig { .. } => AedbErrorCode::InvalidConfig,
            AedbError::IntegrityError { .. } => AedbErrorCode::IntegrityError,
            AedbError::Unavailable { .. } => AedbErrorCode::Unavailable,
            AedbError::CheckpointInProgress => AedbErrorCode::CheckpointInProgress,
            AedbError::AlreadyExists { resource_type, .. } => match resource_type {
                ResourceType::Project => AedbErrorCode::ProjectAlreadyExists,
                ResourceType::Scope => AedbErrorCode::ScopeAlreadyExists,
                ResourceType::Table => AedbErrorCode::TableAlreadyExists,
                ResourceType::Index => AedbErrorCode::IndexAlreadyExists,
            },
            AedbError::NotFound { resource_type, .. } => match resource_type {
                ResourceType::Project => AedbErrorCode::ProjectNotFound,
                ResourceType::Scope => AedbErrorCode::ScopeNotFound,
                ResourceType::Table => AedbErrorCode::TableNotFound,
                ResourceType::Index => AedbErrorCode::IndexNotFound,
            },
            AedbError::DuplicatePK { .. } => AedbErrorCode::DuplicatePrimaryKey,
            AedbError::UniqueViolation { .. } => AedbErrorCode::UniqueViolation,
            AedbError::ForeignKeyViolation { .. } => AedbErrorCode::ForeignKeyViolation,
            AedbError::CheckConstraintFailed { .. } => AedbErrorCode::CheckConstraintFailed,
            AedbError::NotNullViolation { .. } => AedbErrorCode::NotNullViolation,
            AedbError::TypeMismatch { .. } => AedbErrorCode::TypeMismatch,
            AedbError::UnknownColumn { .. } => AedbErrorCode::UnknownColumn,
            AedbError::Conflict(_) => AedbErrorCode::Conflict,
            AedbError::Underflow => AedbErrorCode::Underflow,
            AedbError::Overflow => AedbErrorCode::Overflow,
            AedbError::QueueFull => AedbErrorCode::QueueFull,
            AedbError::PermissionDenied(_) => AedbErrorCode::PermissionDenied,
            AedbError::Timeout => AedbErrorCode::Timeout,
            AedbError::PartitionLockTimeout => AedbErrorCode::PartitionLockTimeout,
            AedbError::EpochApplyTimeout => AedbErrorCode::EpochApplyTimeout,
            AedbError::ParallelApplyCancelled => AedbErrorCode::ParallelApplyCancelled,
            AedbError::ParallelApplyWorkerPanicked => AedbErrorCode::ParallelApplyWorkerPanicked,
            AedbError::SnapshotExpired => AedbErrorCode::SnapshotExpired,
            AedbError::AssertionFailed { .. } => AedbErrorCode::AssertionFailed,
        }
    }

    pub fn code_str(&self) -> &'static str {
        self.code().as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::{AedbError, AedbErrorCode, ResourceType};

    #[test]
    fn error_code_strings_are_stable() {
        assert_eq!(AedbErrorCode::TableNotFound.as_str(), "table_not_found");
        assert_eq!(
            AedbErrorCode::IndexAlreadyExists.as_str(),
            "index_already_exists"
        );
        assert_eq!(
            AedbErrorCode::PermissionDenied.as_str(),
            "permission_denied"
        );
    }

    #[test]
    fn error_code_str_matches_variant_mapping() {
        let err = AedbError::NotFound {
            resource_type: ResourceType::Table,
            resource_id: "p.s.users".into(),
        };
        assert_eq!(err.code(), AedbErrorCode::TableNotFound);
        assert_eq!(err.code_str(), "table_not_found");
    }
}
