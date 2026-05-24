use crate::error::{AedbError, ResourceType};
use crate::query::error::{QueryError, QueryErrorClass, QueryErrorCode};

#[test]
fn query_error_display_is_human_readable() {
    let err = QueryError::ColumnNotFound {
        table: "users".into(),
        column: "name".into(),
    };
    assert_eq!(err.to_string(), "column 'name' not found in table 'users'");
}

#[test]
fn maps_aedb_table_not_found_to_structured_query_error() {
    let err = QueryError::from(AedbError::NotFound {
        resource_type: ResourceType::Table,
        resource_id: "arcana.app.leaderboard_points".into(),
    });
    assert_eq!(
        err,
        QueryError::TableNotFound {
            project_id: "arcana".into(),
            table: "leaderboard_points".into(),
        }
    );
    assert!(err.is_table_not_found());
    assert_eq!(err.code(), QueryErrorCode::TableNotFound);
    assert_eq!(err.code_str(), "table_not_found");
    assert_eq!(err.class(), QueryErrorClass::Validation);
}

#[test]
fn query_error_code_strings_are_stable() {
    let expected = [
        (QueryErrorCode::TableNotFound, "table_not_found"),
        (QueryErrorCode::ColumnNotFound, "column_not_found"),
        (QueryErrorCode::TypeMismatch, "type_mismatch"),
        (QueryErrorCode::ScanBoundExceeded, "scan_bound_exceeded"),
        (QueryErrorCode::InvalidQuery, "invalid_query"),
        (QueryErrorCode::PermissionDenied, "permission_denied"),
        (QueryErrorCode::SeqNotYetVisible, "seq_not_yet_visible"),
        (QueryErrorCode::SeqGarbageCollected, "seq_garbage_collected"),
        (QueryErrorCode::CursorExpired, "cursor_expired"),
        (QueryErrorCode::InvalidCursor, "invalid_cursor"),
        (QueryErrorCode::SnapshotExpired, "snapshot_expired"),
        (
            QueryErrorCode::SnapshotLimitReached,
            "snapshot_limit_reached",
        ),
        (QueryErrorCode::InternalError, "internal_error"),
    ];
    for (code, stable) in expected {
        assert_eq!(code.as_str(), stable);
    }
}

#[test]
fn query_error_code_classes_describe_caller_behavior() {
    assert_eq!(
        QueryErrorCode::SnapshotExpired.class(),
        QueryErrorClass::Retryable
    );
    assert_eq!(
        QueryErrorCode::PermissionDenied.class(),
        QueryErrorClass::Permission
    );
    assert_eq!(
        QueryErrorCode::ScanBoundExceeded.class(),
        QueryErrorClass::Unavailable
    );
    assert_eq!(
        QueryErrorCode::InternalError.class(),
        QueryErrorClass::Integrity
    );
    assert!(QueryErrorCode::CursorExpired.is_retryable());
}
