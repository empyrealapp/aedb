//! Lazy bootstrap of engine-internal system tables (audit log, outbox, and
//! reactive-processor registries) within the reserved system project/scope.
//!
//! These are pure catalog mutations: each `ensure_*` call is idempotent and
//! defines a `TableSchema` the first time the corresponding internal table is
//! written. Kept separate from `apply.rs` row-mutation logic so the hot commit
//! path stays focused on rows, keys, indexes, and constraints.

use super::now_micros;
use super::{
    ASSERTION_AUDIT_TABLE, AUTHZ_AUDIT_TABLE, EVENT_OUTBOX_TABLE, LIFECYCLE_OUTBOX_TABLE,
    REACTIVE_PROCESSOR_CHECKPOINTS_TABLE, REACTIVE_PROCESSOR_DLQ_TABLE,
    REACTIVE_PROCESSOR_REGISTRY_TABLE, SYSTEM_SCOPE_ID,
};
use crate::catalog::Catalog;
use crate::catalog::namespace_key;
use crate::catalog::schema::{ColumnDef, TableSchema};
use crate::catalog::types::ColumnType;
use crate::error::AedbError;

pub(super) fn ensure_internal_audit_schema_for_upsert(
    catalog: &mut Catalog,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
) -> Result<(), AedbError> {
    if project_id == crate::catalog::SYSTEM_PROJECT_ID
        && scope_id == SYSTEM_SCOPE_ID
        && table_name == ASSERTION_AUDIT_TABLE
    {
        ensure_assertion_audit_schema(catalog)?;
    } else if project_id == crate::catalog::SYSTEM_PROJECT_ID
        && scope_id == SYSTEM_SCOPE_ID
        && table_name == LIFECYCLE_OUTBOX_TABLE
    {
        ensure_lifecycle_outbox_schema(catalog)?;
    } else if project_id == crate::catalog::SYSTEM_PROJECT_ID
        && scope_id == SYSTEM_SCOPE_ID
        && table_name == EVENT_OUTBOX_TABLE
    {
        ensure_event_outbox_schema(catalog)?;
    } else if project_id == crate::catalog::SYSTEM_PROJECT_ID
        && scope_id == SYSTEM_SCOPE_ID
        && table_name == REACTIVE_PROCESSOR_CHECKPOINTS_TABLE
    {
        ensure_reactive_processor_checkpoints_schema(catalog)?;
    } else if project_id == crate::catalog::SYSTEM_PROJECT_ID
        && scope_id == SYSTEM_SCOPE_ID
        && table_name == REACTIVE_PROCESSOR_REGISTRY_TABLE
    {
        ensure_reactive_processor_registry_schema(catalog)?;
    } else if project_id == crate::catalog::SYSTEM_PROJECT_ID
        && scope_id == SYSTEM_SCOPE_ID
        && table_name == REACTIVE_PROCESSOR_DLQ_TABLE
    {
        ensure_reactive_processor_dead_letter_schema(catalog)?;
    }
    Ok(())
}

pub(super) fn ensure_authz_audit_schema(catalog: &mut Catalog) -> Result<(), AedbError> {
    ensure_system_project_scope(catalog);

    let key = (
        namespace_key(crate::catalog::SYSTEM_PROJECT_ID, SYSTEM_SCOPE_ID),
        AUTHZ_AUDIT_TABLE.to_string(),
    );
    if catalog.tables.contains_key(&key) {
        return Ok(());
    }
    catalog.tables.insert(
        key,
        TableSchema {
            project_id: crate::catalog::SYSTEM_PROJECT_ID.to_string(),
            scope_id: SYSTEM_SCOPE_ID.to_string(),
            table_name: AUTHZ_AUDIT_TABLE.to_string(),
            owner_id: Some("system".to_string()),
            columns: vec![
                ColumnDef {
                    name: "seq".to_string(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "ts_micros".to_string(),
                    col_type: ColumnType::Timestamp,
                    nullable: false,
                },
                ColumnDef {
                    name: "action".to_string(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
                ColumnDef {
                    name: "actor_id".to_string(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
                ColumnDef {
                    name: "target_caller_id".to_string(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
                ColumnDef {
                    name: "permission".to_string(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
                ColumnDef {
                    name: "project_id".to_string(),
                    col_type: ColumnType::Text,
                    nullable: true,
                },
                ColumnDef {
                    name: "scope_id".to_string(),
                    col_type: ColumnType::Text,
                    nullable: true,
                },
                ColumnDef {
                    name: "table_name".to_string(),
                    col_type: ColumnType::Text,
                    nullable: true,
                },
                ColumnDef {
                    name: "delegable".to_string(),
                    col_type: ColumnType::Boolean,
                    nullable: false,
                },
                ColumnDef {
                    name: "resource_type".to_string(),
                    col_type: ColumnType::Text,
                    nullable: true,
                },
                ColumnDef {
                    name: "resource_id".to_string(),
                    col_type: ColumnType::Text,
                    nullable: true,
                },
                ColumnDef {
                    name: "metadata".to_string(),
                    col_type: ColumnType::Text,
                    nullable: true,
                },
            ],
            primary_key: vec!["seq".to_string()],
            constraints: Vec::new(),
            foreign_keys: Vec::new(),
        },
    );
    Ok(())
}

fn ensure_assertion_audit_schema(catalog: &mut Catalog) -> Result<(), AedbError> {
    ensure_system_project_scope(catalog);
    let key = (
        namespace_key(crate::catalog::SYSTEM_PROJECT_ID, SYSTEM_SCOPE_ID),
        ASSERTION_AUDIT_TABLE.to_string(),
    );
    if catalog.tables.contains_key(&key) {
        return Ok(());
    }
    catalog.tables.insert(
        key,
        TableSchema {
            project_id: crate::catalog::SYSTEM_PROJECT_ID.to_string(),
            scope_id: SYSTEM_SCOPE_ID.to_string(),
            table_name: ASSERTION_AUDIT_TABLE.to_string(),
            owner_id: Some("system".to_string()),
            columns: vec![
                ColumnDef {
                    name: "seq".to_string(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "ts_micros".to_string(),
                    col_type: ColumnType::Timestamp,
                    nullable: false,
                },
                ColumnDef {
                    name: "caller_id".to_string(),
                    col_type: ColumnType::Text,
                    nullable: true,
                },
                ColumnDef {
                    name: "assertion_index".to_string(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "assertion".to_string(),
                    col_type: ColumnType::Json,
                    nullable: false,
                },
                ColumnDef {
                    name: "actual".to_string(),
                    col_type: ColumnType::Json,
                    nullable: false,
                },
                ColumnDef {
                    name: "error".to_string(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
            ],
            primary_key: vec!["seq".to_string()],
            constraints: vec![],
            foreign_keys: vec![],
        },
    );
    Ok(())
}

fn ensure_lifecycle_outbox_schema(catalog: &mut Catalog) -> Result<(), AedbError> {
    ensure_system_project_scope(catalog);
    let key = (
        namespace_key(crate::catalog::SYSTEM_PROJECT_ID, SYSTEM_SCOPE_ID),
        LIFECYCLE_OUTBOX_TABLE.to_string(),
    );
    if catalog.tables.contains_key(&key) {
        return Ok(());
    }
    catalog.tables.insert(
        key,
        TableSchema {
            project_id: crate::catalog::SYSTEM_PROJECT_ID.to_string(),
            scope_id: SYSTEM_SCOPE_ID.to_string(),
            table_name: LIFECYCLE_OUTBOX_TABLE.to_string(),
            owner_id: Some("system".to_string()),
            columns: vec![
                ColumnDef {
                    name: "commit_seq".to_string(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "ts_micros".to_string(),
                    col_type: ColumnType::Timestamp,
                    nullable: false,
                },
                ColumnDef {
                    name: "event_count".to_string(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "events".to_string(),
                    col_type: ColumnType::Json,
                    nullable: false,
                },
            ],
            primary_key: vec!["commit_seq".to_string()],
            constraints: vec![],
            foreign_keys: vec![],
        },
    );
    Ok(())
}

pub(super) fn ensure_event_outbox_schema(catalog: &mut Catalog) -> Result<(), AedbError> {
    ensure_system_project_scope(catalog);
    let key = (
        namespace_key(crate::catalog::SYSTEM_PROJECT_ID, SYSTEM_SCOPE_ID),
        EVENT_OUTBOX_TABLE.to_string(),
    );
    if catalog.tables.contains_key(&key) {
        return Ok(());
    }
    catalog.tables.insert(
        key,
        TableSchema {
            project_id: crate::catalog::SYSTEM_PROJECT_ID.to_string(),
            scope_id: SYSTEM_SCOPE_ID.to_string(),
            table_name: EVENT_OUTBOX_TABLE.to_string(),
            owner_id: Some("system".to_string()),
            columns: vec![
                ColumnDef {
                    name: "commit_seq".to_string(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "ts_micros".to_string(),
                    col_type: ColumnType::Timestamp,
                    nullable: false,
                },
                ColumnDef {
                    name: "project_id".to_string(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
                ColumnDef {
                    name: "scope_id".to_string(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
                ColumnDef {
                    name: "topic".to_string(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
                ColumnDef {
                    name: "event_key".to_string(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
                ColumnDef {
                    name: "payload".to_string(),
                    col_type: ColumnType::Json,
                    nullable: false,
                },
            ],
            primary_key: vec![
                "commit_seq".to_string(),
                "topic".to_string(),
                "event_key".to_string(),
            ],
            constraints: vec![],
            foreign_keys: vec![],
        },
    );
    Ok(())
}

fn ensure_reactive_processor_checkpoints_schema(catalog: &mut Catalog) -> Result<(), AedbError> {
    ensure_system_project_scope(catalog);
    let key = (
        namespace_key(crate::catalog::SYSTEM_PROJECT_ID, SYSTEM_SCOPE_ID),
        REACTIVE_PROCESSOR_CHECKPOINTS_TABLE.to_string(),
    );
    if catalog.tables.contains_key(&key) {
        return Ok(());
    }
    catalog.tables.insert(
        key,
        TableSchema {
            project_id: crate::catalog::SYSTEM_PROJECT_ID.to_string(),
            scope_id: SYSTEM_SCOPE_ID.to_string(),
            table_name: REACTIVE_PROCESSOR_CHECKPOINTS_TABLE.to_string(),
            owner_id: Some("system".to_string()),
            columns: vec![
                ColumnDef {
                    name: "processor_name".to_string(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
                ColumnDef {
                    name: "checkpoint_seq".to_string(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "updated_at_micros".to_string(),
                    col_type: ColumnType::Timestamp,
                    nullable: false,
                },
            ],
            primary_key: vec!["processor_name".to_string()],
            constraints: vec![],
            foreign_keys: vec![],
        },
    );
    Ok(())
}

fn ensure_reactive_processor_registry_schema(catalog: &mut Catalog) -> Result<(), AedbError> {
    ensure_system_project_scope(catalog);
    let key = (
        namespace_key(crate::catalog::SYSTEM_PROJECT_ID, SYSTEM_SCOPE_ID),
        REACTIVE_PROCESSOR_REGISTRY_TABLE.to_string(),
    );
    if catalog.tables.contains_key(&key) {
        return Ok(());
    }
    catalog.tables.insert(
        key,
        TableSchema {
            project_id: crate::catalog::SYSTEM_PROJECT_ID.to_string(),
            scope_id: SYSTEM_SCOPE_ID.to_string(),
            table_name: REACTIVE_PROCESSOR_REGISTRY_TABLE.to_string(),
            owner_id: Some("system".to_string()),
            columns: vec![
                ColumnDef {
                    name: "processor_name".to_string(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
                ColumnDef {
                    name: "options_json".to_string(),
                    col_type: ColumnType::Json,
                    nullable: false,
                },
                ColumnDef {
                    name: "enabled".to_string(),
                    col_type: ColumnType::Boolean,
                    nullable: false,
                },
                ColumnDef {
                    name: "updated_at_micros".to_string(),
                    col_type: ColumnType::Timestamp,
                    nullable: false,
                },
            ],
            primary_key: vec!["processor_name".to_string()],
            constraints: vec![],
            foreign_keys: vec![],
        },
    );
    Ok(())
}

fn ensure_reactive_processor_dead_letter_schema(catalog: &mut Catalog) -> Result<(), AedbError> {
    ensure_system_project_scope(catalog);
    let key = (
        namespace_key(crate::catalog::SYSTEM_PROJECT_ID, SYSTEM_SCOPE_ID),
        REACTIVE_PROCESSOR_DLQ_TABLE.to_string(),
    );
    if catalog.tables.contains_key(&key) {
        return Ok(());
    }
    catalog.tables.insert(
        key,
        TableSchema {
            project_id: crate::catalog::SYSTEM_PROJECT_ID.to_string(),
            scope_id: SYSTEM_SCOPE_ID.to_string(),
            table_name: REACTIVE_PROCESSOR_DLQ_TABLE.to_string(),
            owner_id: Some("system".to_string()),
            columns: vec![
                ColumnDef {
                    name: "processor_name".to_string(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
                ColumnDef {
                    name: "commit_seq".to_string(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "topic".to_string(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
                ColumnDef {
                    name: "event_key".to_string(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
                ColumnDef {
                    name: "payload_json".to_string(),
                    col_type: ColumnType::Json,
                    nullable: false,
                },
                ColumnDef {
                    name: "error".to_string(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
                ColumnDef {
                    name: "attempts".to_string(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "failed_at_micros".to_string(),
                    col_type: ColumnType::Timestamp,
                    nullable: false,
                },
            ],
            primary_key: vec![
                "processor_name".to_string(),
                "commit_seq".to_string(),
                "event_key".to_string(),
            ],
            constraints: vec![],
            foreign_keys: vec![],
        },
    );
    Ok(())
}

fn ensure_system_project_scope(catalog: &mut Catalog) {
    if !catalog
        .projects
        .contains_key(crate::catalog::SYSTEM_PROJECT_ID)
    {
        catalog.projects.insert(
            crate::catalog::SYSTEM_PROJECT_ID.to_string(),
            crate::catalog::project::ProjectMeta {
                project_id: crate::catalog::SYSTEM_PROJECT_ID.to_string(),
                created_at_micros: now_micros(),
                owner_id: Some("system".to_string()),
            },
        );
    }
    if !catalog.scopes.contains_key(&(
        crate::catalog::SYSTEM_PROJECT_ID.to_string(),
        SYSTEM_SCOPE_ID.to_string(),
    )) {
        catalog.scopes.insert(
            (
                crate::catalog::SYSTEM_PROJECT_ID.to_string(),
                SYSTEM_SCOPE_ID.to_string(),
            ),
            crate::catalog::project::ScopeMeta {
                project_id: crate::catalog::SYSTEM_PROJECT_ID.to_string(),
                scope_id: SYSTEM_SCOPE_ID.to_string(),
                created_at_micros: now_micros(),
                owner_id: Some("system".to_string()),
            },
        );
    }
}
