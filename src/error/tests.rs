use super::{AedbError, AedbErrorClass, AedbErrorCode, ResourceType};

#[test]
fn error_code_strings_are_stable() {
    let expected = [
        (AedbErrorCode::Io, "io"),
        (AedbErrorCode::Encode, "encode"),
        (AedbErrorCode::Decode, "decode"),
        (AedbErrorCode::Validation, "validation"),
        (AedbErrorCode::InvalidConfig, "invalid_config"),
        (AedbErrorCode::IntegrityError, "integrity_error"),
        (AedbErrorCode::RecoveryIntegrity, "recovery_integrity"),
        (AedbErrorCode::Unavailable, "unavailable"),
        (
            AedbErrorCode::CheckpointInProgress,
            "checkpoint_in_progress",
        ),
        (
            AedbErrorCode::ProjectAlreadyExists,
            "project_already_exists",
        ),
        (AedbErrorCode::ScopeAlreadyExists, "scope_already_exists"),
        (AedbErrorCode::TableAlreadyExists, "table_already_exists"),
        (AedbErrorCode::IndexAlreadyExists, "index_already_exists"),
        (AedbErrorCode::ProjectNotFound, "project_not_found"),
        (AedbErrorCode::ScopeNotFound, "scope_not_found"),
        (AedbErrorCode::TableNotFound, "table_not_found"),
        (AedbErrorCode::IndexNotFound, "index_not_found"),
        (AedbErrorCode::DuplicatePrimaryKey, "duplicate_primary_key"),
        (AedbErrorCode::UniqueViolation, "unique_violation"),
        (AedbErrorCode::ForeignKeyViolation, "foreign_key_violation"),
        (
            AedbErrorCode::CheckConstraintFailed,
            "check_constraint_failed",
        ),
        (AedbErrorCode::NotNullViolation, "not_null_violation"),
        (AedbErrorCode::TypeMismatch, "type_mismatch"),
        (AedbErrorCode::UnknownColumn, "unknown_column"),
        (AedbErrorCode::Conflict, "conflict"),
        (AedbErrorCode::Underflow, "underflow"),
        (AedbErrorCode::Overflow, "overflow"),
        (AedbErrorCode::QueueFull, "queue_full"),
        (AedbErrorCode::PermissionDenied, "permission_denied"),
        (AedbErrorCode::Timeout, "timeout"),
        (
            AedbErrorCode::PartitionLockTimeout,
            "partition_lock_timeout",
        ),
        (AedbErrorCode::EpochApplyTimeout, "epoch_apply_timeout"),
        (
            AedbErrorCode::ParallelApplyCancelled,
            "parallel_apply_cancelled",
        ),
        (
            AedbErrorCode::ParallelApplyWorkerPanicked,
            "parallel_apply_worker_panicked",
        ),
        (AedbErrorCode::SnapshotExpired, "snapshot_expired"),
        (AedbErrorCode::AssertionFailed, "assertion_failed"),
    ];
    for (code, stable) in expected {
        assert_eq!(code.as_str(), stable);
    }
}

#[test]
fn error_code_str_matches_variant_mapping() {
    let err = AedbError::NotFound {
        resource_type: ResourceType::Table,
        resource_id: "p.s.users".into(),
    };
    assert_eq!(err.code(), AedbErrorCode::TableNotFound);
    assert_eq!(err.code_str(), "table_not_found");
    assert_eq!(err.class(), AedbErrorClass::Validation);
}

#[test]
fn error_code_classes_describe_caller_behavior() {
    assert_eq!(AedbErrorCode::QueueFull.class(), AedbErrorClass::Retryable);
    assert_eq!(
        AedbErrorCode::AssertionFailed.class(),
        AedbErrorClass::Conflict
    );
    assert_eq!(
        AedbErrorCode::PermissionDenied.class(),
        AedbErrorClass::Permission
    );
    assert_eq!(
        AedbErrorCode::IntegrityError.class(),
        AedbErrorClass::Integrity
    );
    assert_eq!(
        AedbErrorCode::Unavailable.class(),
        AedbErrorClass::Unavailable
    );
    assert!(AedbErrorCode::Timeout.is_retryable());
}
