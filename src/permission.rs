use serde::{Deserialize, Serialize};

/// Permissions v2 authorization primitives.
///
/// Semantics are intentionally strict:
/// - `GlobalAdmin` applies to every project/scope/table/KV namespace.
/// - Project-scoped admin/DDL permissions do not imply global privileges.
/// - Scope-scoped permissions do not spill into other scopes.
/// - KV permissions with `scope_id: None` are project-wide across all scopes.
/// - KV permissions with `scope_id: Some(..)` are limited to that exact scope.
/// - KV permissions with `prefix: Some(..)` only match keys starting with that prefix.
/// - KV permissions with `prefix: None` match all keys within the chosen scope domain.
///
/// Delegation is tracked separately via grant metadata; this enum only captures
/// the resource/action envelope being granted.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Permission {
    /// Read KV values. `scope_id: None` means project-wide; otherwise exact scope.
    /// `prefix` constrains access to keys with the given binary prefix.
    KvRead {
        project_id: String,
        #[serde(default)]
        scope_id: Option<String>,
        #[serde(default)]
        prefix: Option<Vec<u8>>,
    },
    /// Write KV values. Matching rules are identical to `KvRead`.
    KvWrite {
        project_id: String,
        #[serde(default)]
        scope_id: Option<String>,
        #[serde(default)]
        prefix: Option<Vec<u8>>,
    },
    /// Read rows from one concrete table.
    TableRead {
        project_id: String,
        scope_id: String,
        table_name: String,
    },
    /// Write rows in one concrete table.
    TableWrite {
        project_id: String,
        scope_id: String,
        table_name: String,
    },
    /// Read one concrete secondary index.
    IndexRead {
        project_id: String,
        scope_id: String,
        table_name: String,
        index_name: String,
    },
    /// Authorize schema/policy/projection DDL at project scope.
    TableDdl { project_id: String },
    /// Unrestricted admin over all projects and resources.
    GlobalAdmin,
    /// Admin over one project and all of its scopes/tables.
    ProjectAdmin { project_id: String },
    /// Admin over one scope within one project.
    ScopeAdmin {
        project_id: String,
        scope_id: String,
    },
    /// Bypass row-level read policy checks for a project, optionally narrowed
    /// to a specific table when `table_name` is set.
    PolicyBypass {
        project_id: String,
        #[serde(default)]
        table_name: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CallerContext {
    pub caller_id: String,
    // Reserved internal flag: never deserialize from untrusted input.
    #[serde(default, skip_deserializing)]
    internal_system: bool,
}

impl CallerContext {
    pub fn new(caller_id: impl Into<String>) -> Self {
        Self {
            caller_id: caller_id.into(),
            internal_system: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn system_internal() -> Self {
        Self {
            caller_id: "system".to_string(),
            internal_system: true,
        }
    }

    pub(crate) fn is_internal_system(&self) -> bool {
        self.internal_system && self.caller_id == "system"
    }
}
