pub mod project;
pub mod schema;
pub mod types;

use crate::catalog::project::{ProjectMeta, ScopeMeta};
use crate::catalog::schema::{
    AsyncIndexDef, ColumnDef, Constraint, IndexDef, IndexType, KvProjectionDef, TableAlteration,
    TableSchema,
};
use crate::catalog::types::ColumnType;
use crate::error::AedbError;
use crate::error::ResourceType as ErrorResourceType;
use crate::permission::Permission;
use crate::query::plan::Expr;
use im::HashMap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap as StdHashMap};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn default_true() -> bool {
    true
}

/// Cache entry for permission checks with TTL
#[derive(Debug, Clone)]
struct PermissionCacheEntry {
    allowed: bool,
    expires_at: Instant,
}

type PermissionCacheMap = StdHashMap<(String, Permission), PermissionCacheEntry>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Catalog {
    pub projects: HashMap<String, ProjectMeta>,
    pub scopes: HashMap<(String, String), ScopeMeta>,
    pub tables: HashMap<(String, String), TableSchema>,
    pub indexes: HashMap<(String, String, String), IndexDef>,
    pub async_indexes: HashMap<(String, String, String), AsyncIndexDef>,
    #[serde(default)]
    pub kv_projections: HashMap<(String, String), KvProjectionDef>,
    pub permissions: HashMap<String, BTreeSet<Permission>>,
    #[serde(default)]
    pub permission_grants: HashMap<(String, Permission), PermissionGrantMeta>,
    #[serde(default)]
    pub read_policies: HashMap<(String, String, String), Expr>,
    /// TTL cache for permission checks (caller_id, permission) -> (allowed, expiry)
    /// Skipped during serialization; initialized on deserialization.
    /// Shared across clones to maintain cache effectiveness.
    #[serde(skip)]
    permission_check_cache: Arc<RwLock<PermissionCacheMap>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PermissionGrantMeta {
    pub granted_by: String,
    #[serde(default)]
    pub delegable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ResourceType {
    Project,
    Scope,
    Table,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DdlOperation {
    CreateProject {
        project_id: String,
        #[serde(default)]
        owner_id: Option<String>,
        #[serde(default = "default_true")]
        if_not_exists: bool,
    },
    DropProject {
        project_id: String,
        #[serde(default = "default_true")]
        if_exists: bool,
    },
    CreateScope {
        project_id: String,
        scope_id: String,
        #[serde(default)]
        owner_id: Option<String>,
        #[serde(default = "default_true")]
        if_not_exists: bool,
    },
    DropScope {
        project_id: String,
        scope_id: String,
        #[serde(default = "default_true")]
        if_exists: bool,
    },
    CreateTable {
        project_id: String,
        scope_id: String,
        table_name: String,
        #[serde(default)]
        owner_id: Option<String>,
        #[serde(default)]
        if_not_exists: bool,
        columns: Vec<ColumnDef>,
        primary_key: Vec<String>,
    },
    AlterTable {
        project_id: String,
        scope_id: String,
        table_name: String,
        alteration: TableAlteration,
    },
    DropTable {
        project_id: String,
        scope_id: String,
        table_name: String,
        #[serde(default = "default_true")]
        if_exists: bool,
    },
    CreateIndex {
        project_id: String,
        scope_id: String,
        table_name: String,
        index_name: String,
        #[serde(default)]
        if_not_exists: bool,
        columns: Vec<String>,
        index_type: IndexType,
        partial_filter: Option<Expr>,
    },
    DropIndex {
        project_id: String,
        scope_id: String,
        table_name: String,
        index_name: String,
        #[serde(default = "default_true")]
        if_exists: bool,
    },
    CreateAsyncIndex {
        project_id: String,
        scope_id: String,
        table_name: String,
        index_name: String,
        #[serde(default)]
        if_not_exists: bool,
        projected_columns: Vec<String>,
    },
    DropAsyncIndex {
        project_id: String,
        scope_id: String,
        table_name: String,
        index_name: String,
        #[serde(default = "default_true")]
        if_exists: bool,
    },
    EnableKvProjection {
        project_id: String,
        scope_id: String,
    },
    DisableKvProjection {
        project_id: String,
        scope_id: String,
    },
    GrantPermission {
        caller_id: String,
        permission: Permission,
        #[serde(default)]
        actor_id: Option<String>,
        #[serde(default)]
        delegable: bool,
    },
    RevokePermission {
        caller_id: String,
        permission: Permission,
        #[serde(default)]
        actor_id: Option<String>,
    },
    TransferOwnership {
        resource_type: ResourceType,
        project_id: String,
        #[serde(default)]
        scope_id: Option<String>,
        #[serde(default)]
        table_name: Option<String>,
        new_owner_id: String,
        #[serde(default)]
        actor_id: Option<String>,
    },
    SetReadPolicy {
        project_id: String,
        scope_id: String,
        table_name: String,
        predicate: Expr,
        #[serde(default)]
        actor_id: Option<String>,
    },
    ClearReadPolicy {
        project_id: String,
        scope_id: String,
        table_name: String,
        #[serde(default)]
        actor_id: Option<String>,
    },
}

impl Default for Catalog {
    fn default() -> Self {
        Self {
            projects: HashMap::new(),
            scopes: HashMap::new(),
            tables: HashMap::new(),
            indexes: HashMap::new(),
            async_indexes: HashMap::new(),
            kv_projections: HashMap::new(),
            permissions: HashMap::new(),
            permission_grants: HashMap::new(),
            read_policies: HashMap::new(),
            permission_check_cache: Arc::new(RwLock::new(PermissionCacheMap::new())),
        }
    }
}

// Implement PartialEq and Eq manually, comparing only serializable fields
impl PartialEq for Catalog {
    fn eq(&self, other: &Self) -> bool {
        self.projects == other.projects
            && self.scopes == other.scopes
            && self.tables == other.tables
            && self.indexes == other.indexes
            && self.async_indexes == other.async_indexes
            && self.kv_projections == other.kv_projections
            && self.permissions == other.permissions
            && self.permission_grants == other.permission_grants
            && self.read_policies == other.read_policies
        // permission_check_cache is intentionally excluded - it's a performance optimization
    }
}

impl Eq for Catalog {}

impl Catalog {
    const PERMISSION_CACHE_MAX_ENTRIES: usize = 1000;
    const PERMISSION_CACHE_TTL: Duration = Duration::from_secs(30);

    /// Invalidate cached checks for one caller after grant/revoke.
    fn invalidate_permission_cache_for_caller(&self, caller_id: &str) {
        if let Ok(mut cache) = self.permission_check_cache.write() {
            cache.retain(|(cached_caller_id, _), _| cached_caller_id != caller_id);
        }
    }

    fn permission_cache_get(&self, key: &(String, Permission), now: Instant) -> Option<bool> {
        self.permission_check_cache
            .read()
            .ok()
            .and_then(|cache| cache.get(key).cloned())
            .filter(|entry| entry.expires_at > now)
            .map(|entry| entry.allowed)
    }

    fn permission_cache_put(&self, key: (String, Permission), allowed: bool, now: Instant) {
        if let Ok(mut cache) = self.permission_check_cache.write() {
            if cache.len() >= Self::PERMISSION_CACHE_MAX_ENTRIES {
                cache.clear();
            }
            cache.insert(
                key,
                PermissionCacheEntry {
                    allowed,
                    expires_at: now + Self::PERMISSION_CACHE_TTL,
                },
            );
        }
    }

    /// Checks for overlapping KV prefixes and logs warnings if found.
    ///
    /// Overlapping prefixes are technically valid (permissions are additive),
    /// but may indicate misconfiguration. For example:
    /// - User has `prefix: [0x01]` → can read all keys starting with 0x01
    /// - Granting `prefix: [0x01, 0x02]` → adds no new access (already covered)
    ///
    /// This logs a warning but does NOT prevent the grant.
    fn check_overlapping_kv_prefix(
        &self,
        caller_id: &str,
        new_permission: &Permission,
        existing_permissions: &BTreeSet<Permission>,
    ) {
        // Extract KV prefix info from new permission
        let (new_project, new_scope, new_prefix, new_is_read) = match new_permission {
            Permission::KvRead {
                project_id,
                scope_id,
                prefix,
            } => (project_id, scope_id, prefix, true),
            Permission::KvWrite {
                project_id,
                scope_id,
                prefix,
            } => (project_id, scope_id, prefix, false),
            _ => return, // Not a KV permission, no overlap possible
        };

        // Skip if new permission has no prefix (unrestricted access)
        let Some(new_prefix_bytes) = new_prefix else {
            return;
        };

        // Check against existing KV permissions with same project/scope/type
        for existing in existing_permissions {
            let (existing_project, existing_scope, existing_prefix, existing_is_read) =
                match existing {
                    Permission::KvRead {
                        project_id,
                        scope_id,
                        prefix,
                    } => (project_id, scope_id, prefix, true),
                    Permission::KvWrite {
                        project_id,
                        scope_id,
                        prefix,
                    } => (project_id, scope_id, prefix, false),
                    _ => continue,
                };

            // Only check overlaps for same project/scope/read-write type
            if existing_project != new_project
                || existing_scope != new_scope
                || existing_is_read != new_is_read
            {
                continue;
            }

            let Some(existing_prefix_bytes) = existing_prefix else {
                // Existing permission has no prefix (unrestricted), so new prefix is redundant
                tracing::warn!(
                    caller_id = caller_id,
                    project_id = %new_project,
                    scope_id = ?new_scope,
                    new_prefix = ?hex::encode(new_prefix_bytes),
                    permission_type = if new_is_read { "KvRead" } else { "KvWrite" },
                    "Granting KV permission with prefix, but user already has unrestricted \
                     access (prefix: None) for this project/scope - new permission is redundant"
                );
                return;
            };

            // Check for prefix overlap
            if new_prefix_bytes.starts_with(existing_prefix_bytes)
                && new_prefix_bytes != existing_prefix_bytes
            {
                // New prefix is more specific than existing (e.g., existing=[0x01], new=[0x01,0x02])
                tracing::warn!(
                    caller_id = caller_id,
                    project_id = %new_project,
                    scope_id = ?new_scope,
                    existing_prefix = ?hex::encode(existing_prefix_bytes),
                    new_prefix = ?hex::encode(new_prefix_bytes),
                    permission_type = if new_is_read { "KvRead" } else { "KvWrite" },
                    "Granting KV permission with prefix that is more specific than existing \
                     prefix - user already has access to these keys via existing permission"
                );
            } else if existing_prefix_bytes.starts_with(new_prefix_bytes)
                && new_prefix_bytes != existing_prefix_bytes
            {
                // New prefix is broader than existing (e.g., existing=[0x01,0x02], new=[0x01])
                tracing::warn!(
                    caller_id = caller_id,
                    project_id = %new_project,
                    scope_id = ?new_scope,
                    existing_prefix = ?hex::encode(existing_prefix_bytes),
                    new_prefix = ?hex::encode(new_prefix_bytes),
                    permission_type = if new_is_read { "KvRead" } else { "KvWrite" },
                    "Granting KV permission with prefix that is broader than existing prefix \
                     - this will grant additional access beyond existing permission"
                );
            }
        }
    }

    pub fn snapshot(&self) -> Self {
        self.clone()
    }

    pub fn ddl_payload(op: &DdlOperation) -> Result<Vec<u8>, AedbError> {
        rmp_serde::to_vec(op).map_err(|e| AedbError::Encode(e.to_string()))
    }

    pub fn ddl_from_payload(bytes: &[u8]) -> Result<DdlOperation, AedbError> {
        rmp_serde::from_slice(bytes).map_err(|e| AedbError::Decode(e.to_string()))
    }

    pub fn apply_ddl(&mut self, op: DdlOperation) -> Result<(), AedbError> {
        match op {
            DdlOperation::CreateProject {
                project_id,
                owner_id,
                if_not_exists,
            } => self.create_project_owned_with_options(&project_id, owner_id, if_not_exists),
            DdlOperation::DropProject {
                project_id,
                if_exists,
            } => self.drop_project_with_options(&project_id, if_exists),
            DdlOperation::CreateScope {
                project_id,
                scope_id,
                owner_id,
                if_not_exists,
            } => self.create_scope_owned_with_options(
                &project_id,
                &scope_id,
                owner_id,
                if_not_exists,
            ),
            DdlOperation::DropScope {
                project_id,
                scope_id,
                if_exists,
            } => self.drop_scope_with_options(&project_id, &scope_id, if_exists),
            DdlOperation::CreateTable {
                project_id,
                scope_id,
                table_name,
                owner_id,
                if_not_exists,
                columns,
                primary_key,
            } => self.create_table_owned_with_options(
                &project_id,
                &scope_id,
                &table_name,
                owner_id,
                if_not_exists,
                columns,
                primary_key,
            ),
            DdlOperation::AlterTable {
                project_id,
                scope_id,
                table_name,
                alteration,
            } => self.alter_table(&project_id, &scope_id, &table_name, alteration),
            DdlOperation::DropTable {
                project_id,
                scope_id,
                table_name,
                if_exists,
            } => self.drop_table_with_options(&project_id, &scope_id, &table_name, if_exists),
            DdlOperation::CreateIndex {
                project_id,
                scope_id,
                table_name,
                index_name,
                if_not_exists,
                columns,
                index_type,
                partial_filter,
            } => self.create_index_with_options(
                &project_id,
                &scope_id,
                &table_name,
                &index_name,
                if_not_exists,
                columns,
                index_type,
                partial_filter,
            ),
            DdlOperation::DropIndex {
                project_id,
                scope_id,
                table_name,
                index_name,
                if_exists,
            } => self.drop_index_with_options(
                &project_id,
                &scope_id,
                &table_name,
                &index_name,
                if_exists,
            ),
            DdlOperation::CreateAsyncIndex {
                project_id,
                scope_id,
                table_name,
                index_name,
                if_not_exists,
                projected_columns,
            } => self.create_async_index_with_options(
                &project_id,
                &scope_id,
                &table_name,
                &index_name,
                if_not_exists,
                projected_columns,
            ),
            DdlOperation::DropAsyncIndex {
                project_id,
                scope_id,
                table_name,
                index_name,
                if_exists,
            } => self.drop_async_index_with_options(
                &project_id,
                &scope_id,
                &table_name,
                &index_name,
                if_exists,
            ),
            DdlOperation::EnableKvProjection {
                project_id,
                scope_id,
            } => self.enable_kv_projection(&project_id, &scope_id),
            DdlOperation::DisableKvProjection {
                project_id,
                scope_id,
            } => self.disable_kv_projection(&project_id, &scope_id),
            DdlOperation::GrantPermission {
                caller_id,
                permission,
                actor_id,
                delegable,
            } => self.grant_permission(&caller_id, permission, actor_id, delegable),
            DdlOperation::RevokePermission {
                caller_id,
                permission,
                actor_id,
            } => self.revoke_permission_with_actor(&caller_id, &permission, actor_id),
            DdlOperation::TransferOwnership {
                resource_type,
                project_id,
                scope_id,
                table_name,
                new_owner_id,
                actor_id: _,
            } => self.transfer_ownership(
                &resource_type,
                &project_id,
                scope_id.as_deref(),
                table_name.as_deref(),
                &new_owner_id,
            ),
            DdlOperation::SetReadPolicy {
                project_id,
                scope_id,
                table_name,
                predicate,
                actor_id: _,
            } => self.set_read_policy(&project_id, &scope_id, &table_name, predicate),
            DdlOperation::ClearReadPolicy {
                project_id,
                scope_id,
                table_name,
                actor_id: _,
            } => self.clear_read_policy(&project_id, &scope_id, &table_name),
        }
    }

    pub fn grant_permission(
        &mut self,
        caller_id: &str,
        permission: Permission,
        actor_id: Option<String>,
        delegable: bool,
    ) -> Result<(), AedbError> {
        let mut set = self.permissions.get(caller_id).cloned().unwrap_or_default();

        // Check for overlapping KV prefixes and warn if found
        self.check_overlapping_kv_prefix(caller_id, &permission, &set);

        set.insert(permission.clone());
        self.permissions.insert(caller_id.to_string(), set);
        let granted_by = actor_id.unwrap_or_else(|| "system".to_string());
        self.permission_grants.insert(
            (caller_id.to_string(), permission),
            PermissionGrantMeta {
                granted_by,
                delegable,
            },
        );
        // Invalidate cache entries for this caller when permissions change.
        self.invalidate_permission_cache_for_caller(caller_id);
        Ok(())
    }

    pub fn revoke_permission(
        &mut self,
        caller_id: &str,
        permission: &Permission,
    ) -> Result<(), AedbError> {
        let Some(mut set) = self.permissions.get(caller_id).cloned() else {
            return Ok(());
        };
        set.remove(permission);
        self.permissions.insert(caller_id.to_string(), set);
        self.permission_grants
            .remove(&(caller_id.to_string(), permission.clone()));
        // Invalidate cache entries for this caller when permissions change.
        self.invalidate_permission_cache_for_caller(caller_id);
        Ok(())
    }

    pub fn revoke_permission_with_actor(
        &mut self,
        caller_id: &str,
        permission: &Permission,
        _actor_id: Option<String>,
    ) -> Result<(), AedbError> {
        self.revoke_permission(caller_id, permission)
    }

    pub fn has_permission(&self, caller_id: &str, required: &Permission) -> bool {
        let now = Instant::now();
        let cache_key = (caller_id.to_string(), required.clone());
        if let Some(allowed) = self.permission_cache_get(&cache_key, now) {
            return allowed;
        }

        // Cache miss or expired - check authoritative permissions
        let Some(granted) = self.permissions.get(caller_id) else {
            self.permission_cache_put(cache_key, false, now);
            return false;
        };

        let is_global_admin = granted.contains(&Permission::GlobalAdmin);
        let allowed = is_global_admin || granted.contains(required);

        // Log GlobalAdmin usage for audit trail (break-glass access)
        if is_global_admin {
            // Log at WARN level since GlobalAdmin bypasses normal authorization
            tracing::warn!(
                caller_id = caller_id,
                required_permission = ?required,
                "GlobalAdmin permission used - break-glass access"
            );
        }

        self.permission_cache_put(cache_key, allowed, now);

        allowed
    }

    pub fn has_delegable_grant(&self, caller_id: &str, permission: &Permission) -> bool {
        self.permission_grants
            .get(&(caller_id.to_string(), permission.clone()))
            .is_some_and(|m| m.delegable)
    }

    pub fn is_owner_of_project(&self, caller_id: &str, project_id: &str) -> bool {
        self.projects
            .get(project_id)
            .and_then(|p| p.owner_id.as_deref())
            == Some(caller_id)
    }

    pub fn is_owner_of_scope(&self, caller_id: &str, project_id: &str, scope_id: &str) -> bool {
        self.scopes
            .get(&(project_id.to_string(), scope_id.to_string()))
            .and_then(|s| s.owner_id.as_deref())
            == Some(caller_id)
    }

    pub fn is_owner_of_table(
        &self,
        caller_id: &str,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
    ) -> bool {
        self.tables
            .get(&(namespace_key(project_id, scope_id), table_name.to_string()))
            .and_then(|t| t.owner_id.as_deref())
            == Some(caller_id)
    }

    pub fn has_kv_read_permission(
        &self,
        caller_id: &str,
        project_id: &str,
        scope_id: &str,
        key: &[u8],
    ) -> bool {
        self.has_kv_permission(caller_id, project_id, scope_id, key, true)
    }

    pub fn has_kv_write_permission(
        &self,
        caller_id: &str,
        project_id: &str,
        scope_id: &str,
        key: &[u8],
    ) -> bool {
        self.has_kv_permission(caller_id, project_id, scope_id, key, false)
    }

    pub fn kv_read_prefixes_for_caller(
        &self,
        caller_id: &str,
        project_id: &str,
        scope_id: &str,
    ) -> Option<Vec<Vec<u8>>> {
        let granted = self.permissions.get(caller_id)?;
        collect_kv_read_prefixes(granted.iter(), project_id, scope_id)
    }

    fn has_kv_permission(
        &self,
        caller_id: &str,
        project_id: &str,
        scope_id: &str,
        key: &[u8],
        read: bool,
    ) -> bool {
        let Some(granted) = self.permissions.get(caller_id) else {
            return false;
        };
        granted
            .iter()
            .any(|p| permission_allows_kv(p, project_id, scope_id, key, read))
    }

    pub fn create_project(&mut self, project_id: &str) -> Result<(), AedbError> {
        self.create_project_owned(project_id, None)
    }

    pub fn create_project_owned(
        &mut self,
        project_id: &str,
        owner_id: Option<String>,
    ) -> Result<(), AedbError> {
        self.create_project_owned_with_options(project_id, owner_id, true)
    }

    pub fn create_project_owned_with_options(
        &mut self,
        project_id: &str,
        owner_id: Option<String>,
        if_not_exists: bool,
    ) -> Result<(), AedbError> {
        validate_project_id(project_id)?;
        if self.projects.contains_key(project_id) {
            if if_not_exists {
                return Ok(());
            }
            return Err(AedbError::AlreadyExists {
                resource_type: ErrorResourceType::Project,
                resource_id: project_id.to_string(),
            });
        }
        self.projects.insert(
            project_id.to_string(),
            ProjectMeta {
                project_id: project_id.to_string(),
                created_at_micros: now_micros(),
                owner_id: owner_id.clone(),
            },
        );
        self.create_scope_owned_with_options(project_id, DEFAULT_SCOPE_ID, owner_id, true)?;
        Ok(())
    }

    pub fn drop_project(&mut self, project_id: &str) -> Result<(), AedbError> {
        self.drop_project_with_options(project_id, true)
    }

    pub fn drop_project_with_options(
        &mut self,
        project_id: &str,
        if_exists: bool,
    ) -> Result<(), AedbError> {
        if !self.projects.contains_key(project_id) {
            if if_exists {
                return Ok(());
            }
            return Err(AedbError::NotFound {
                resource_type: ErrorResourceType::Project,
                resource_id: project_id.to_string(),
            });
        }
        self.projects.remove(project_id);
        let project = project_id.to_string();
        let scope_keys: Vec<(String, String)> = self
            .scopes
            .keys()
            .filter(|(p, _)| p == &project)
            .cloned()
            .collect();
        for key in scope_keys {
            self.scopes.remove(&key);
        }

        let table_keys: Vec<(String, String)> = self
            .tables
            .keys()
            .filter(|(p, _)| p == &project || p.starts_with(&format!("{project}::")))
            .cloned()
            .collect();
        for key in table_keys {
            self.tables.remove(&key);
        }

        let index_keys: Vec<(String, String, String)> = self
            .indexes
            .keys()
            .filter(|(p, _, _)| p == &project || p.starts_with(&format!("{project}::")))
            .cloned()
            .collect();
        for key in index_keys {
            self.indexes.remove(&key);
        }
        let async_index_keys: Vec<(String, String, String)> = self
            .async_indexes
            .keys()
            .filter(|(p, _, _)| p == &project || p.starts_with(&format!("{project}::")))
            .cloned()
            .collect();
        for key in async_index_keys {
            self.async_indexes.remove(&key);
        }
        let projection_keys: Vec<(String, String)> = self
            .kv_projections
            .keys()
            .filter(|(p, _)| p == &project)
            .cloned()
            .collect();
        for key in projection_keys {
            self.kv_projections.remove(&key);
        }
        Ok(())
    }

    pub fn create_table(
        &mut self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        columns: Vec<ColumnDef>,
        primary_key: Vec<String>,
    ) -> Result<(), AedbError> {
        self.create_table_owned(project_id, scope_id, table_name, None, columns, primary_key)
    }

    pub fn create_table_owned(
        &mut self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        owner_id: Option<String>,
        columns: Vec<ColumnDef>,
        primary_key: Vec<String>,
    ) -> Result<(), AedbError> {
        self.create_table_owned_with_options(
            project_id,
            scope_id,
            table_name,
            owner_id,
            false,
            columns,
            primary_key,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_table_owned_with_options(
        &mut self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        owner_id: Option<String>,
        if_not_exists: bool,
        columns: Vec<ColumnDef>,
        primary_key: Vec<String>,
    ) -> Result<(), AedbError> {
        if table_name == KV_INDEX_TABLE {
            return Err(AedbError::Validation(format!(
                "table name {KV_INDEX_TABLE} is reserved"
            )));
        }
        validate_identifier(table_name, "table_name")?;
        if !self.projects.contains_key(project_id) {
            return Err(AedbError::NotFound {
                resource_type: ErrorResourceType::Project,
                resource_id: project_id.to_string(),
            });
        }
        if !self
            .scopes
            .contains_key(&(project_id.to_string(), scope_id.to_string()))
        {
            return Err(AedbError::NotFound {
                resource_type: ErrorResourceType::Scope,
                resource_id: format!("{project_id}.{scope_id}"),
            });
        }
        let key = (namespace_key(project_id, scope_id), table_name.to_string());
        if self.tables.contains_key(&key) {
            if if_not_exists {
                return Ok(());
            }
            return Err(AedbError::AlreadyExists {
                resource_type: ErrorResourceType::Table,
                resource_id: format!("{project_id}.{scope_id}.{table_name}"),
            });
        }
        if columns.is_empty() {
            return Err(AedbError::Validation(
                "table needs at least one column".into(),
            ));
        }
        if primary_key.is_empty() {
            return Err(AedbError::Validation("primary key required".into()));
        }
        for pk in &primary_key {
            if !columns.iter().any(|c| c.name == *pk) {
                return Err(AedbError::Validation(format!(
                    "primary key column missing: {pk}"
                )));
            }
        }
        self.tables.insert(
            key,
            TableSchema {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: table_name.to_string(),
                owner_id,
                columns,
                primary_key,
                constraints: Vec::new(),
                foreign_keys: Vec::new(),
            },
        );
        Ok(())
    }

    pub fn alter_table(
        &mut self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        alteration: TableAlteration,
    ) -> Result<(), AedbError> {
        let ns = namespace_key(project_id, scope_id);
        let table_name_owned = table_name.to_string();
        let key = (ns.clone(), table_name_owned.clone());
        let Some(existing_table) = self.tables.get(&key) else {
            return Err(AedbError::NotFound {
                resource_type: ErrorResourceType::Table,
                resource_id: format!("{project_id}.{scope_id}.{table_name}"),
            });
        };
        match &alteration {
            TableAlteration::DropColumn { name } => {
                if existing_table.primary_key.contains(name) {
                    return Err(AedbError::Validation(
                        "cannot drop primary key column".into(),
                    ));
                }
                if column_has_schema_dependency(
                    self,
                    existing_table,
                    &ns,
                    &table_name_owned,
                    project_id,
                    scope_id,
                    table_name,
                    name,
                ) {
                    return Err(AedbError::Validation(format!(
                        "cannot drop column with active dependencies: {name}"
                    )));
                }
                if !existing_table.columns.iter().any(|c| c.name == *name) {
                    return Err(AedbError::Validation("column does not exist".into()));
                }
            }
            TableAlteration::RenameColumn { from, .. } => {
                if existing_table.primary_key.contains(from) {
                    return Err(AedbError::Validation(
                        "cannot rename primary key column".into(),
                    ));
                }
                if column_has_schema_dependency(
                    self,
                    existing_table,
                    &ns,
                    &table_name_owned,
                    project_id,
                    scope_id,
                    table_name,
                    from,
                ) {
                    return Err(AedbError::Validation(format!(
                        "cannot rename column with active dependencies: {from}"
                    )));
                }
            }
            _ => {}
        }

        if let TableAlteration::AddConstraint(Constraint::Unique { name, .. }) = &alteration {
            let index_key = (ns.clone(), table_name_owned.clone(), name.clone());
            if self.indexes.contains_key(&index_key) {
                return Err(AedbError::Validation("index already exists".into()));
            }
        }
        let mut add_unique_index: Option<(String, Vec<String>, u128)> = None;
        let mut drop_unique_index: Option<String> = None;
        let table = self
            .tables
            .get_mut(&key)
            .ok_or_else(|| AedbError::NotFound {
                resource_type: ErrorResourceType::Table,
                resource_id: format!("{project_id}.{scope_id}.{table_name}"),
            })?;
        match alteration {
            TableAlteration::AddColumn(column) => {
                if table.columns.iter().any(|c| c.name == column.name) {
                    return Err(AedbError::Validation("column already exists".into()));
                }
                table.columns.push(column);
            }
            TableAlteration::DropColumn { name } => {
                table.columns.retain(|c| c.name != name);
            }
            TableAlteration::RenameColumn { from, to } => {
                if table.columns.iter().any(|c| c.name == to) {
                    return Err(AedbError::Validation("target column already exists".into()));
                }
                let col = table
                    .columns
                    .iter_mut()
                    .find(|c| c.name == from)
                    .ok_or_else(|| AedbError::Validation("source column does not exist".into()))?;
                col.name = to.clone();
                for pk in &mut table.primary_key {
                    if *pk == from {
                        *pk = to.clone();
                    }
                }
            }
            TableAlteration::AddConstraint(constraint) => {
                if let Constraint::Unique { name, columns } = &constraint {
                    if table.constraints.iter().any(|c| {
                        matches!(c, Constraint::Unique { name: existing, .. } if existing == name)
                    }) {
                        return Err(AedbError::Validation("constraint already exists".into()));
                    }
                    for col in columns {
                        if !table.columns.iter().any(|c| c.name == *col) {
                            return Err(AedbError::Validation(format!(
                                "constraint column does not exist: {col}"
                            )));
                        }
                    }
                    let mut columns_bitmask = 0u128;
                    for col in columns {
                        if let Some(idx) = table.columns.iter().position(|c| c.name == *col)
                            && idx < 128
                        {
                            columns_bitmask |= 1u128 << idx;
                        }
                    }
                    add_unique_index = Some((name.clone(), columns.clone(), columns_bitmask));
                }
                if let Constraint::NotNull { column } = &constraint
                    && !table.columns.iter().any(|c| c.name == *column)
                {
                    return Err(AedbError::Validation(format!(
                        "constraint column does not exist: {column}"
                    )));
                }
                table.constraints.push(constraint);
            }
            TableAlteration::DropConstraint { name } => {
                if let Some(idx) = table.constraints.iter().position(|c| match c {
                    Constraint::Unique { name: n, .. } | Constraint::Check { name: n, .. } => {
                        n == &name
                    }
                    Constraint::NotNull { column } | Constraint::Default { column, .. } => {
                        column == &name
                    }
                }) {
                    let removed = table.constraints.remove(idx);
                    if matches!(removed, Constraint::Unique { .. }) {
                        drop_unique_index = Some(name);
                    }
                }
            }
            TableAlteration::AddForeignKey(fk) => {
                if table
                    .foreign_keys
                    .iter()
                    .any(|existing| existing.name == fk.name)
                {
                    return Err(AedbError::Validation("foreign key already exists".into()));
                }
                table.foreign_keys.push(fk);
            }
            TableAlteration::DropForeignKey { name } => {
                table.foreign_keys.retain(|fk| fk.name != name);
            }
        }
        if let Some((index_name, columns, columns_bitmask)) = add_unique_index {
            self.indexes.insert(
                (ns.clone(), table_name_owned.clone(), index_name.clone()),
                IndexDef {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    table_name: table_name_owned.clone(),
                    index_name,
                    columns,
                    index_type: IndexType::UniqueHash,
                    columns_bitmask,
                    partial_filter: None,
                },
            );
        }
        if let Some(index_name) = drop_unique_index {
            self.indexes.remove(&(ns, table_name_owned, index_name));
        }
        Ok(())
    }

    pub fn drop_table(
        &mut self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
    ) -> Result<(), AedbError> {
        self.drop_table_with_options(project_id, scope_id, table_name, true)
    }

    pub fn drop_table_with_options(
        &mut self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        if_exists: bool,
    ) -> Result<(), AedbError> {
        if table_name == KV_INDEX_TABLE {
            return Err(AedbError::Validation(format!(
                "table {KV_INDEX_TABLE} is managed; use DisableKvProjection"
            )));
        }
        let ns = namespace_key(project_id, scope_id);
        let table_key = (ns.clone(), table_name.to_string());
        if !self.tables.contains_key(&table_key) {
            if if_exists {
                return Ok(());
            }
            return Err(AedbError::NotFound {
                resource_type: ErrorResourceType::Table,
                resource_id: format!("{project_id}.{scope_id}.{table_name}"),
            });
        }
        self.tables.remove(&table_key);
        let index_keys: Vec<(String, String, String)> = self
            .indexes
            .keys()
            .filter(|(p, t, _)| p == &ns && t == table_name)
            .cloned()
            .collect();
        for key in index_keys {
            self.indexes.remove(&key);
        }
        let async_index_keys: Vec<(String, String, String)> = self
            .async_indexes
            .keys()
            .filter(|(p, t, _)| p == &ns && t == table_name)
            .cloned()
            .collect();
        for key in async_index_keys {
            self.async_indexes.remove(&key);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_index(
        &mut self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        index_name: &str,
        columns: Vec<String>,
        index_type: IndexType,
        partial_filter: Option<Expr>,
    ) -> Result<(), AedbError> {
        self.create_index_with_options(
            project_id,
            scope_id,
            table_name,
            index_name,
            false,
            columns,
            index_type,
            partial_filter,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_index_with_options(
        &mut self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        index_name: &str,
        if_not_exists: bool,
        columns: Vec<String>,
        index_type: IndexType,
        partial_filter: Option<Expr>,
    ) -> Result<(), AedbError> {
        validate_identifier(index_name, "index_name")?;
        let ns = namespace_key(project_id, scope_id);
        let table_key = (ns.clone(), table_name.to_string());
        let table = self
            .tables
            .get(&table_key)
            .ok_or_else(|| AedbError::NotFound {
                resource_type: ErrorResourceType::Table,
                resource_id: format!("{project_id}.{scope_id}.{table_name}"),
            })?;

        for col in &columns {
            if !table.columns.iter().any(|c| c.name == *col) {
                return Err(AedbError::Validation(format!(
                    "index column does not exist: {col}"
                )));
            }
        }

        let index_key = (ns, table_name.to_string(), index_name.to_string());
        if self.indexes.contains_key(&index_key) {
            if if_not_exists {
                return Ok(());
            }
            return Err(AedbError::AlreadyExists {
                resource_type: ErrorResourceType::Index,
                resource_id: format!("{project_id}.{scope_id}.{table_name}.{index_name}"),
            });
        }
        let mut columns_bitmask = 0u128;
        for col in &columns {
            if let Some(idx) = table.columns.iter().position(|c| c.name == *col)
                && idx < 128
            {
                columns_bitmask |= 1u128 << idx;
            }
        }
        self.indexes.insert(
            index_key,
            IndexDef {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: table_name.to_string(),
                index_name: index_name.to_string(),
                columns,
                index_type,
                columns_bitmask,
                partial_filter,
            },
        );
        Ok(())
    }

    pub fn drop_index(
        &mut self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        index_name: &str,
    ) -> Result<(), AedbError> {
        self.drop_index_with_options(project_id, scope_id, table_name, index_name, true)
    }

    pub fn drop_index_with_options(
        &mut self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        index_name: &str,
        if_exists: bool,
    ) -> Result<(), AedbError> {
        let ns = namespace_key(project_id, scope_id);
        let key = (ns, table_name.to_string(), index_name.to_string());
        if !self.indexes.contains_key(&key) {
            if if_exists {
                return Ok(());
            }
            return Err(AedbError::NotFound {
                resource_type: ErrorResourceType::Index,
                resource_id: format!("{project_id}.{scope_id}.{table_name}.{index_name}"),
            });
        }
        self.indexes.remove(&key);
        Ok(())
    }

    pub fn create_async_index(
        &mut self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        index_name: &str,
        projected_columns: Vec<String>,
    ) -> Result<(), AedbError> {
        self.create_async_index_with_options(
            project_id,
            scope_id,
            table_name,
            index_name,
            false,
            projected_columns,
        )
    }

    pub fn create_async_index_with_options(
        &mut self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        index_name: &str,
        if_not_exists: bool,
        projected_columns: Vec<String>,
    ) -> Result<(), AedbError> {
        validate_identifier(index_name, "index_name")?;
        let ns = namespace_key(project_id, scope_id);
        let table_key = (ns.clone(), table_name.to_string());
        let table = self
            .tables
            .get(&table_key)
            .ok_or_else(|| AedbError::NotFound {
                resource_type: ErrorResourceType::Table,
                resource_id: format!("{project_id}.{scope_id}.{table_name}"),
            })?;
        for col in &projected_columns {
            if !table.columns.iter().any(|c| c.name == *col) {
                return Err(AedbError::Validation(format!(
                    "projection column does not exist: {col}"
                )));
            }
        }
        let key = (ns, table_name.to_string(), index_name.to_string());
        if self.async_indexes.contains_key(&key) {
            if if_not_exists {
                return Ok(());
            }
            return Err(AedbError::AlreadyExists {
                resource_type: ErrorResourceType::Index,
                resource_id: format!("{project_id}.{scope_id}.{table_name}.{index_name}"),
            });
        }
        self.async_indexes.insert(
            key,
            AsyncIndexDef {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: table_name.to_string(),
                index_name: index_name.to_string(),
                projected_columns,
            },
        );
        Ok(())
    }

    pub fn drop_async_index(
        &mut self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        index_name: &str,
    ) -> Result<(), AedbError> {
        self.drop_async_index_with_options(project_id, scope_id, table_name, index_name, true)
    }

    pub fn drop_async_index_with_options(
        &mut self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        index_name: &str,
        if_exists: bool,
    ) -> Result<(), AedbError> {
        let ns = namespace_key(project_id, scope_id);
        let key = (ns, table_name.to_string(), index_name.to_string());
        if !self.async_indexes.contains_key(&key) {
            if if_exists {
                return Ok(());
            }
            return Err(AedbError::NotFound {
                resource_type: ErrorResourceType::Index,
                resource_id: format!("{project_id}.{scope_id}.{table_name}.{index_name}"),
            });
        }
        self.async_indexes.remove(&key);
        Ok(())
    }

    pub fn enable_kv_projection(
        &mut self,
        project_id: &str,
        scope_id: &str,
    ) -> Result<(), AedbError> {
        if !self
            .scopes
            .contains_key(&(project_id.to_string(), scope_id.to_string()))
        {
            return Err(AedbError::NotFound {
                resource_type: ErrorResourceType::Scope,
                resource_id: format!("{project_id}.{scope_id}"),
            });
        }
        let key = (project_id.to_string(), scope_id.to_string());
        if self.kv_projections.contains_key(&key) {
            return Ok(());
        }
        let ns = namespace_key(project_id, scope_id);
        self.tables.insert(
            (ns, KV_INDEX_TABLE.to_string()),
            TableSchema {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: KV_INDEX_TABLE.to_string(),
                owner_id: None,
                columns: vec![
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
                        name: "key".to_string(),
                        col_type: ColumnType::Blob,
                        nullable: false,
                    },
                    ColumnDef {
                        name: "value".to_string(),
                        col_type: ColumnType::Blob,
                        nullable: false,
                    },
                    ColumnDef {
                        name: "commit_seq".to_string(),
                        col_type: ColumnType::Integer,
                        nullable: false,
                    },
                    ColumnDef {
                        name: "updated_at".to_string(),
                        col_type: ColumnType::Timestamp,
                        nullable: false,
                    },
                ],
                primary_key: vec!["key".to_string()],
                constraints: Vec::new(),
                foreign_keys: Vec::new(),
            },
        );
        self.kv_projections.insert(
            key,
            KvProjectionDef {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: KV_INDEX_TABLE.to_string(),
            },
        );
        Ok(())
    }

    pub fn disable_kv_projection(
        &mut self,
        project_id: &str,
        scope_id: &str,
    ) -> Result<(), AedbError> {
        self.kv_projections
            .remove(&(project_id.to_string(), scope_id.to_string()));
        let ns = namespace_key(project_id, scope_id);
        self.tables.remove(&(ns, KV_INDEX_TABLE.to_string()));
        Ok(())
    }

    pub fn create_scope(&mut self, project_id: &str, scope_id: &str) -> Result<(), AedbError> {
        self.create_scope_owned(project_id, scope_id, None)
    }

    pub fn create_scope_owned(
        &mut self,
        project_id: &str,
        scope_id: &str,
        owner_id: Option<String>,
    ) -> Result<(), AedbError> {
        self.create_scope_owned_with_options(project_id, scope_id, owner_id, true)
    }

    pub fn create_scope_owned_with_options(
        &mut self,
        project_id: &str,
        scope_id: &str,
        owner_id: Option<String>,
        if_not_exists: bool,
    ) -> Result<(), AedbError> {
        validate_scope_id(scope_id)?;
        if !self.projects.contains_key(project_id) {
            return Err(AedbError::NotFound {
                resource_type: ErrorResourceType::Project,
                resource_id: project_id.to_string(),
            });
        }
        let key = (project_id.to_string(), scope_id.to_string());
        if self.scopes.contains_key(&key) {
            if if_not_exists {
                return Ok(());
            }
            return Err(AedbError::AlreadyExists {
                resource_type: ErrorResourceType::Scope,
                resource_id: format!("{project_id}.{scope_id}"),
            });
        }
        self.scopes.insert(
            key,
            ScopeMeta {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                created_at_micros: now_micros(),
                owner_id,
            },
        );
        Ok(())
    }

    pub fn drop_scope(&mut self, project_id: &str, scope_id: &str) -> Result<(), AedbError> {
        self.drop_scope_with_options(project_id, scope_id, true)
    }

    pub fn drop_scope_with_options(
        &mut self,
        project_id: &str,
        scope_id: &str,
        if_exists: bool,
    ) -> Result<(), AedbError> {
        if !self
            .scopes
            .contains_key(&(project_id.to_string(), scope_id.to_string()))
        {
            if if_exists {
                return Ok(());
            }
            return Err(AedbError::NotFound {
                resource_type: ErrorResourceType::Scope,
                resource_id: format!("{project_id}.{scope_id}"),
            });
        }
        self.scopes
            .remove(&(project_id.to_string(), scope_id.to_string()));
        let ns = namespace_key(project_id, scope_id);

        let table_keys: Vec<(String, String)> = self
            .tables
            .keys()
            .filter(|(p, _)| p == &ns)
            .cloned()
            .collect();
        for key in table_keys {
            self.tables.remove(&key);
        }

        let index_keys: Vec<(String, String, String)> = self
            .indexes
            .keys()
            .filter(|(p, _, _)| p == &ns)
            .cloned()
            .collect();
        for key in index_keys {
            self.indexes.remove(&key);
        }
        let async_index_keys: Vec<(String, String, String)> = self
            .async_indexes
            .keys()
            .filter(|(p, _, _)| p == &ns)
            .cloned()
            .collect();
        for key in async_index_keys {
            self.async_indexes.remove(&key);
        }
        self.kv_projections
            .remove(&(project_id.to_string(), scope_id.to_string()));
        Ok(())
    }

    pub fn list_scopes(&self, project_id: &str) -> Vec<String> {
        self.scopes
            .values()
            .filter(|s| s.project_id == project_id)
            .map(|s| s.scope_id.clone())
            .collect()
    }

    pub fn read_policy_for_table(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
    ) -> Option<Expr> {
        self.read_policies
            .get(&(
                project_id.to_string(),
                scope_id.to_string(),
                table_name.to_string(),
            ))
            .cloned()
    }

    pub fn set_read_policy(
        &mut self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        predicate: Expr,
    ) -> Result<(), AedbError> {
        // Validate expression depth to prevent stack overflow
        predicate.validate_depth()?;

        if !self
            .tables
            .contains_key(&(namespace_key(project_id, scope_id), table_name.to_string()))
        {
            return Err(AedbError::NotFound {
                resource_type: ErrorResourceType::Table,
                resource_id: format!("{project_id}.{scope_id}.{table_name}"),
            });
        }
        self.read_policies.insert(
            (
                project_id.to_string(),
                scope_id.to_string(),
                table_name.to_string(),
            ),
            predicate,
        );
        Ok(())
    }

    pub fn clear_read_policy(
        &mut self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
    ) -> Result<(), AedbError> {
        self.read_policies.remove(&(
            project_id.to_string(),
            scope_id.to_string(),
            table_name.to_string(),
        ));
        Ok(())
    }

    pub fn transfer_ownership(
        &mut self,
        resource_type: &ResourceType,
        project_id: &str,
        scope_id: Option<&str>,
        table_name: Option<&str>,
        new_owner_id: &str,
    ) -> Result<(), AedbError> {
        match resource_type {
            ResourceType::Project => {
                let Some(project) = self.projects.get_mut(project_id) else {
                    return Err(AedbError::NotFound {
                        resource_type: ErrorResourceType::Project,
                        resource_id: project_id.to_string(),
                    });
                };
                project.owner_id = Some(new_owner_id.to_string());
            }
            ResourceType::Scope => {
                let Some(scope_id) = scope_id else {
                    return Err(AedbError::Validation(
                        "scope transfer requires scope_id".into(),
                    ));
                };
                let Some(scope) = self
                    .scopes
                    .get_mut(&(project_id.to_string(), scope_id.to_string()))
                else {
                    return Err(AedbError::NotFound {
                        resource_type: ErrorResourceType::Scope,
                        resource_id: format!("{project_id}.{scope_id}"),
                    });
                };
                scope.owner_id = Some(new_owner_id.to_string());
            }
            ResourceType::Table => {
                let Some(scope_id) = scope_id else {
                    return Err(AedbError::Validation(
                        "table transfer requires scope_id".into(),
                    ));
                };
                let Some(table_name) = table_name else {
                    return Err(AedbError::Validation(
                        "table transfer requires table_name".into(),
                    ));
                };
                let Some(table) = self
                    .tables
                    .get_mut(&(namespace_key(project_id, scope_id), table_name.to_string()))
                else {
                    return Err(AedbError::NotFound {
                        resource_type: ErrorResourceType::Table,
                        resource_id: format!("{project_id}.{scope_id}.{table_name}"),
                    });
                };
                table.owner_id = Some(new_owner_id.to_string());
            }
        }
        Ok(())
    }
}

fn collect_kv_read_prefixes<'a>(
    permissions: impl Iterator<Item = &'a Permission>,
    project_id: &str,
    required_scope_id: &str,
) -> Option<Vec<Vec<u8>>> {
    let mut prefixes = Vec::new();
    for permission in permissions {
        match permission {
            Permission::GlobalAdmin => return Some(Vec::new()),
            Permission::ProjectAdmin { project_id: p } if p == project_id => {
                return Some(Vec::new());
            }
            Permission::ScopeAdmin {
                project_id: p,
                scope_id: s,
            } if p == project_id && s == required_scope_id => return Some(Vec::new()),
            Permission::KvRead {
                project_id: p,
                scope_id,
                prefix,
            } if p == project_id => {
                let scope_matches = scope_id
                    .as_deref()
                    .is_none_or(|candidate| candidate == required_scope_id);
                if scope_matches {
                    if let Some(prefix) = prefix {
                        prefixes.push(prefix.clone());
                    } else {
                        return Some(Vec::new());
                    }
                }
            }
            _ => {}
        }
    }
    if prefixes.is_empty() {
        None
    } else {
        Some(prefixes)
    }
}

/// Checks if a permission grants access to a specific KV operation.
///
/// # Prefix Matching Semantics
///
/// KV permissions use **byte-level prefix matching** on the key:
/// - `prefix: None` → Grants unrestricted access to all keys within the scope domain
/// - `prefix: Some(&[])` → Empty prefix matches all keys (equivalent to None)
/// - `prefix: Some(bytes)` → Only keys starting with these exact bytes are allowed
///
/// ## Important Edge Cases
///
/// ### Overlapping Prefixes
/// Multiple permissions with overlapping prefixes can coexist. If a user has both
/// `prefix: [0x01]` and `prefix: [0x01, 0x02]`, they can access:
/// - Keys starting with `[0x01]` via the first permission
/// - Keys starting with `[0x01, 0x02]` via either permission (overlap)
///
/// This is **intentional** and follows the principle that permissions are additive.
///
/// ### Exact Prefix Match
/// When `key.len() == prefix.len()`, the key is considered to "start with" the prefix
/// if they are byte-for-byte identical. For example:
/// - `prefix: [0x01, 0x02]` matches key `[0x01, 0x02]` ✓
/// - `prefix: [0x01, 0x02]` does NOT match key `[0x01]` ✗
///
/// ### Binary Safety
/// Prefixes are pure binary data and do not require valid UTF-8. Keys and prefixes
/// can contain any byte values including `0xFF`, `0x00`, etc.
///
/// ### Scope Matching
/// - `scope_id: None` → Permission applies across ALL scopes within the project
/// - `scope_id: Some(id)` → Permission only applies to the exact scope specified
///
/// ## Admin Permissions
/// - `GlobalAdmin` → Bypasses all checks, grants access to any project/scope/key
/// - `ProjectAdmin` → Bypasses prefix/scope checks within the specified project
/// - `ScopeAdmin` → Bypasses prefix checks within the specified project+scope
///
/// # Parameters
/// - `permission`: The permission to check
/// - `project_id`: The target project ID
/// - `required_scope_id`: The target scope ID
/// - `key`: The KV key being accessed (as binary bytes)
/// - `read`: true for read operations, false for write operations
///
/// # Returns
/// `true` if the permission grants access, `false` otherwise
fn permission_allows_kv(
    permission: &Permission,
    project_id: &str,
    required_scope_id: &str,
    key: &[u8],
    read: bool,
) -> bool {
    match permission {
        Permission::GlobalAdmin => true,
        Permission::ProjectAdmin { project_id: p } => p == project_id,
        Permission::ScopeAdmin {
            project_id: p,
            scope_id: s,
        } => p == project_id && s == required_scope_id,
        Permission::KvRead {
            project_id: p,
            scope_id,
            prefix,
        } if read && p == project_id => {
            let scope_matches = scope_id
                .as_deref()
                .is_none_or(|candidate| candidate == required_scope_id);
            scope_matches
                && prefix
                    .as_deref()
                    .is_none_or(|required_prefix| key.starts_with(required_prefix))
        }
        Permission::KvWrite {
            project_id: p,
            scope_id,
            prefix,
        } if !read && p == project_id => {
            let scope_matches = scope_id
                .as_deref()
                .is_none_or(|candidate| candidate == required_scope_id);
            scope_matches
                && prefix
                    .as_deref()
                    .is_none_or(|required_prefix| key.starts_with(required_prefix))
        }
        _ => false,
    }
}

#[allow(clippy::too_many_arguments)]
fn column_has_schema_dependency(
    catalog: &Catalog,
    table: &TableSchema,
    namespace: &str,
    table_name: &str,
    project_id: &str,
    scope_id: &str,
    table_name_raw: &str,
    column: &str,
) -> bool {
    let index_dep = catalog
        .indexes
        .iter()
        .any(|((index_ns, index_table, _), index)| {
            index_ns == namespace
                && index_table == table_name
                && index.columns.iter().any(|c| c == column)
        });
    let constraint_dep = table.constraints.iter().any(|constraint| match constraint {
        Constraint::Unique { columns, .. } => columns.iter().any(|c| c == column),
        Constraint::Check { .. } => false,
        Constraint::NotNull { column: c } => c == column,
        Constraint::Default { column: c, .. } => c == column,
    });
    let local_fk_dep = table.foreign_keys.iter().any(|fk| {
        fk.columns.iter().any(|c| c == column) || fk.references_columns.iter().any(|c| c == column)
    });
    let inbound_fk_dep = catalog.tables.values().any(|schema| {
        schema.foreign_keys.iter().any(|fk| {
            fk.references_project_id == project_id
                && fk.references_scope_id == scope_id
                && fk.references_table == table_name_raw
                && fk.references_columns.iter().any(|c| c == column)
        })
    });
    index_dep || constraint_dep || local_fk_dep || inbound_fk_dep
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

pub fn namespace_key(project_id: &str, scope_id: &str) -> String {
    format!("{project_id}{NAMESPACE_KEY_SEPARATOR}{scope_id}")
}

pub const DEFAULT_SCOPE_ID: &str = "app";
pub const KV_INDEX_TABLE: &str = "__kv_index";
pub const SYSTEM_PROJECT_ID: &str = "_system";
const NAMESPACE_KEY_SEPARATOR: &str = "::";
const MAX_IDENTIFIER_LEN: usize = 128;

fn validate_project_id(project_id: &str) -> Result<(), AedbError> {
    validate_namespace_component(project_id, "project_id")?;
    if project_id == SYSTEM_PROJECT_ID {
        return Err(AedbError::Validation(format!(
            "project_id {SYSTEM_PROJECT_ID} is reserved"
        )));
    }
    Ok(())
}

fn validate_scope_id(scope_id: &str) -> Result<(), AedbError> {
    validate_namespace_component(scope_id, "scope_id")
}

fn validate_namespace_component(component: &str, name: &str) -> Result<(), AedbError> {
    validate_identifier(component, name)?;
    if component.contains(NAMESPACE_KEY_SEPARATOR) {
        return Err(AedbError::Validation(format!(
            "{name} must not contain {NAMESPACE_KEY_SEPARATOR:?}"
        )));
    }
    Ok(())
}

fn validate_identifier(value: &str, name: &str) -> Result<(), AedbError> {
    if value.is_empty() {
        return Err(AedbError::Validation(format!("{name} must not be empty")));
    }
    if value.len() > MAX_IDENTIFIER_LEN {
        return Err(AedbError::Validation(format!(
            "{name} must be <= {MAX_IDENTIFIER_LEN} bytes"
        )));
    }
    if !value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(AedbError::Validation(format!(
            "{name} must contain only [A-Za-z0-9_-]"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Catalog, DdlOperation, namespace_key};
    use crate::catalog::schema::{
        ColumnDef, ForeignKey, ForeignKeyAction, IndexType, TableAlteration,
    };
    use crate::catalog::types::ColumnType;
    use crate::permission::Permission;

    fn users_columns() -> Vec<ColumnDef> {
        vec![
            ColumnDef {
                name: "id".to_string(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "name".to_string(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ]
    }

    #[test]
    fn project_isolation_allows_same_table_name() {
        let mut c = Catalog::default();
        c.create_project("A").expect("project A");
        c.create_project("B").expect("project B");

        c.create_table("A", "app", "users", users_columns(), vec!["id".into()])
            .expect("table A");
        c.create_table("B", "app", "users", users_columns(), vec!["id".into()])
            .expect("table B");

        assert!(
            c.tables
                .contains_key(&(namespace_key("A", "app"), "users".into()))
        );
        assert!(
            c.tables
                .contains_key(&(namespace_key("B", "app"), "users".into()))
        );
    }

    #[test]
    fn drop_project_cascades_tables_and_indexes() {
        let mut c = Catalog::default();
        c.create_project("A").expect("create");
        for t in ["t1", "t2", "t3"] {
            c.create_table("A", "app", t, users_columns(), vec!["id".into()])
                .expect("table");
            c.create_index(
                "A",
                "app",
                t,
                "by_name",
                vec!["name".into()],
                IndexType::BTree,
                None,
            )
            .expect("index");
        }
        c.drop_project("A").expect("drop");
        assert!(!c.projects.contains_key("A"));
        assert!(c.tables.keys().all(|(p, _)| !p.starts_with("A::")));
        assert!(c.indexes.keys().all(|(p, _, _)| !p.starts_with("A::")));
    }

    #[test]
    fn ddl_payload_roundtrip_and_apply() {
        let mut c = Catalog::default();
        let ops = vec![
            DdlOperation::CreateProject {
                owner_id: None,
                if_not_exists: true,
                project_id: "A".into(),
            },
            DdlOperation::CreateTable {
                owner_id: None,
                if_not_exists: false,
                project_id: "A".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                columns: users_columns(),
                primary_key: vec!["id".into()],
            },
            DdlOperation::AlterTable {
                project_id: "A".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                alteration: TableAlteration::AddColumn(ColumnDef {
                    name: "email".into(),
                    col_type: ColumnType::Text,
                    nullable: true,
                }),
            },
            DdlOperation::CreateIndex {
                project_id: "A".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                index_name: "by_name".into(),
                if_not_exists: false,
                columns: vec!["name".into()],
                index_type: IndexType::BTree,
                partial_filter: None,
            },
        ];

        for op in ops {
            let payload = Catalog::ddl_payload(&op).expect("encode");
            let decoded = Catalog::ddl_from_payload(&payload).expect("decode");
            c.apply_ddl(decoded).expect("apply");
        }

        assert!(
            c.tables
                .contains_key(&(namespace_key("A", "app"), "users".into()))
        );
        assert!(c.indexes.contains_key(&(
            namespace_key("A", "app"),
            "users".into(),
            "by_name".into()
        )));
    }

    #[test]
    fn test_grant_and_revoke_permissions() {
        let mut c = Catalog::default();
        c.grant_permission(
            "caller",
            Permission::KvRead {
                project_id: "A".into(),
                scope_id: Some("app".into()),
                prefix: None,
            },
            None,
            false,
        )
        .expect("grant");
        assert!(c.has_permission(
            "caller",
            &Permission::KvRead {
                project_id: "A".into(),
                scope_id: Some("app".into()),
                prefix: None,
            }
        ));
        c.revoke_permission(
            "caller",
            &Permission::KvRead {
                project_id: "A".into(),
                scope_id: Some("app".into()),
                prefix: None,
            },
        )
        .expect("revoke");
        assert!(!c.has_permission(
            "caller",
            &Permission::KvRead {
                project_id: "A".into(),
                scope_id: Some("app".into()),
                prefix: None,
            }
        ));
    }

    #[test]
    fn grant_permission_records_metadata_without_actor() {
        let mut c = Catalog::default();
        let permission = Permission::KvRead {
            project_id: "A".into(),
            scope_id: Some("app".into()),
            prefix: None,
        };
        c.grant_permission("caller", permission.clone(), None, true)
            .expect("grant");

        let meta = c
            .permission_grants
            .get(&("caller".to_string(), permission))
            .expect("metadata");
        assert!(meta.delegable);
        assert_eq!(meta.granted_by, "system");
    }

    #[test]
    fn alter_table_rejects_drop_column_with_index_dependency() {
        let mut c = Catalog::default();
        c.create_project("A").expect("project");
        c.create_table("A", "app", "users", users_columns(), vec!["id".into()])
            .expect("table");
        c.create_index(
            "A",
            "app",
            "users",
            "by_name",
            vec!["name".into()],
            IndexType::BTree,
            None,
        )
        .expect("index");

        let err = c
            .alter_table(
                "A",
                "app",
                "users",
                TableAlteration::DropColumn {
                    name: "name".into(),
                },
            )
            .expect_err("drop should fail");
        assert!(err.to_string().contains("dependencies"));
    }

    #[test]
    fn alter_table_rejects_rename_column_with_inbound_fk_dependency() {
        let mut c = Catalog::default();
        c.create_project("A").expect("project");
        c.create_table(
            "A",
            "app",
            "parents",
            vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "code".into(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
            ],
            vec!["id".into()],
        )
        .expect("parents");
        c.create_table(
            "A",
            "app",
            "children",
            vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "parent_code".into(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
            ],
            vec!["id".into()],
        )
        .expect("children");
        c.alter_table(
            "A",
            "app",
            "children",
            TableAlteration::AddForeignKey(ForeignKey {
                name: "fk_parent_code".into(),
                columns: vec!["parent_code".into()],
                references_project_id: "A".into(),
                references_scope_id: "app".into(),
                references_table: "parents".into(),
                references_columns: vec!["code".into()],
                on_delete: ForeignKeyAction::Restrict,
                on_update: ForeignKeyAction::Restrict,
            }),
        )
        .expect("fk");

        let err = c
            .alter_table(
                "A",
                "app",
                "parents",
                TableAlteration::RenameColumn {
                    from: "code".into(),
                    to: "external_code".into(),
                },
            )
            .expect_err("rename should fail");
        assert!(err.to_string().contains("dependencies"));
    }

    #[test]
    fn rejects_reserved_and_ambiguous_namespace_ids() {
        let mut c = Catalog::default();
        let reserved = c
            .create_project("_system")
            .expect_err("reserved project id must fail");
        assert!(reserved.to_string().contains("reserved"));

        let bad_project = c
            .create_project("A::B")
            .expect_err("separator in project id must fail");
        assert!(bad_project.to_string().contains("project_id"));

        c.create_project("A").expect("valid project");
        let bad_scope = c
            .create_scope("A", "x::y")
            .expect_err("separator in scope id must fail");
        assert!(bad_scope.to_string().contains("scope_id"));

        c.create_project("_global")
            .expect("global project is allowed");
    }

    #[test]
    fn rejects_invalid_table_and_index_identifiers() {
        let mut c = Catalog::default();
        c.create_project("A").expect("create project");

        let bad_table = c
            .create_table("A", "app", "users.v2", users_columns(), vec!["id".into()])
            .expect_err("table name with dot should fail");
        assert!(bad_table.to_string().contains("table_name"));

        c.create_table("A", "app", "users", users_columns(), vec!["id".into()])
            .expect("valid table");

        let bad_index = c
            .create_index(
                "A",
                "app",
                "users",
                "by name",
                vec!["name".into()],
                IndexType::BTree,
                None,
            )
            .expect_err("index name with space should fail");
        assert!(bad_index.to_string().contains("index_name"));
    }

    #[test]
    fn rejects_overlong_namespace_identifiers() {
        let mut c = Catalog::default();
        let too_long = "a".repeat(129);
        let err = c
            .create_project(&too_long)
            .expect_err("overlong project id should fail");
        assert!(err.to_string().contains("<= 128"));
    }

    /// Test suite for KV prefix permission semantics
    mod kv_prefix_tests {
        use super::*;
        use crate::catalog::permission_allows_kv;

        #[test]
        fn none_prefix_allows_all_keys() {
            // None prefix means unrestricted access within the scope
            let perm = Permission::KvRead {
                project_id: "proj1".into(),
                scope_id: Some("app".into()),
                prefix: None,
            };

            // Should allow any key
            assert!(permission_allows_kv(&perm, "proj1", "app", &[0x01], true));
            assert!(permission_allows_kv(
                &perm,
                "proj1",
                "app",
                &[0xFF, 0xFF],
                true
            ));
            assert!(permission_allows_kv(&perm, "proj1", "app", &[], true));
            assert!(permission_allows_kv(
                &perm,
                "proj1",
                "app",
                &[0x00, 0x01, 0x02, 0x03],
                true
            ));
        }

        #[test]
        fn empty_prefix_allows_all_keys() {
            // Empty prefix (Some(&[])) means every key starts with empty prefix
            let perm = Permission::KvRead {
                project_id: "proj1".into(),
                scope_id: Some("app".into()),
                prefix: Some(vec![]),
            };

            // Should allow any key (since all keys start with empty prefix)
            assert!(permission_allows_kv(&perm, "proj1", "app", &[0x01], true));
            assert!(permission_allows_kv(
                &perm,
                "proj1",
                "app",
                &[0xFF, 0xFF],
                true
            ));
            assert!(permission_allows_kv(&perm, "proj1", "app", &[], true));
            assert!(permission_allows_kv(
                &perm,
                "proj1",
                "app",
                &[0x00, 0x01, 0x02, 0x03],
                true
            ));
        }

        #[test]
        fn exact_prefix_match() {
            let perm = Permission::KvRead {
                project_id: "proj1".into(),
                scope_id: Some("app".into()),
                prefix: Some(vec![0x01, 0x02]),
            };

            // Key exactly equals prefix - should match
            assert!(permission_allows_kv(
                &perm,
                "proj1",
                "app",
                &[0x01, 0x02],
                true
            ));

            // Key starts with prefix - should match
            assert!(permission_allows_kv(
                &perm,
                "proj1",
                "app",
                &[0x01, 0x02, 0x03],
                true
            ));
            assert!(permission_allows_kv(
                &perm,
                "proj1",
                "app",
                &[0x01, 0x02, 0xFF, 0xFF],
                true
            ));

            // Key does not start with prefix - should NOT match
            assert!(!permission_allows_kv(&perm, "proj1", "app", &[0x01], true));
            assert!(!permission_allows_kv(
                &perm,
                "proj1",
                "app",
                &[0x01, 0x03],
                true
            ));
            assert!(!permission_allows_kv(
                &perm,
                "proj1",
                "app",
                &[0x02, 0x02],
                true
            ));
            assert!(!permission_allows_kv(&perm, "proj1", "app", &[], true));
        }

        #[test]
        fn overlapping_prefixes_both_grant_access() {
            // This tests the critical edge case: what happens when user has two
            // overlapping prefixes? Both should independently grant access.

            let prefix_short = Permission::KvRead {
                project_id: "proj1".into(),
                scope_id: Some("app".into()),
                prefix: Some(vec![0x01]),
            };

            let prefix_long = Permission::KvRead {
                project_id: "proj1".into(),
                scope_id: Some("app".into()),
                prefix: Some(vec![0x01, 0x02]),
            };

            // Key [0x01] - only matches short prefix
            assert!(permission_allows_kv(
                &prefix_short,
                "proj1",
                "app",
                &[0x01],
                true
            ));
            assert!(!permission_allows_kv(
                &prefix_long,
                "proj1",
                "app",
                &[0x01],
                true
            ));

            // Key [0x01, 0x02] - matches BOTH prefixes
            assert!(permission_allows_kv(
                &prefix_short,
                "proj1",
                "app",
                &[0x01, 0x02],
                true
            ));
            assert!(permission_allows_kv(
                &prefix_long,
                "proj1",
                "app",
                &[0x01, 0x02],
                true
            ));

            // Key [0x01, 0x02, 0x03] - matches BOTH prefixes
            assert!(permission_allows_kv(
                &prefix_short,
                "proj1",
                "app",
                &[0x01, 0x02, 0x03],
                true
            ));
            assert!(permission_allows_kv(
                &prefix_long,
                "proj1",
                "app",
                &[0x01, 0x02, 0x03],
                true
            ));

            // Key [0x01, 0xFF] - only matches short prefix
            assert!(permission_allows_kv(
                &prefix_short,
                "proj1",
                "app",
                &[0x01, 0xFF],
                true
            ));
            assert!(!permission_allows_kv(
                &prefix_long,
                "proj1",
                "app",
                &[0x01, 0xFF],
                true
            ));
        }

        #[test]
        fn prefix_boundary_at_key_length() {
            // Test that prefix matching works correctly when prefix length equals key length
            let perm = Permission::KvWrite {
                project_id: "proj1".into(),
                scope_id: Some("app".into()),
                prefix: Some(vec![0xAA, 0xBB, 0xCC]),
            };

            // Exact match - prefix length == key length
            assert!(permission_allows_kv(
                &perm,
                "proj1",
                "app",
                &[0xAA, 0xBB, 0xCC],
                false
            ));

            // Key shorter than prefix - should NOT match
            assert!(!permission_allows_kv(
                &perm,
                "proj1",
                "app",
                &[0xAA, 0xBB],
                false
            ));
            assert!(!permission_allows_kv(&perm, "proj1", "app", &[0xAA], false));

            // Key longer than prefix with matching prefix - should match
            assert!(permission_allows_kv(
                &perm,
                "proj1",
                "app",
                &[0xAA, 0xBB, 0xCC, 0xDD],
                false
            ));
        }

        #[test]
        fn scope_id_none_vs_some() {
            // scope_id: None means project-wide access across all scopes
            let project_wide = Permission::KvRead {
                project_id: "proj1".into(),
                scope_id: None,
                prefix: Some(vec![0x01]),
            };

            // Should match ANY scope within the project
            assert!(permission_allows_kv(
                &project_wide,
                "proj1",
                "app",
                &[0x01, 0x02],
                true
            ));
            assert!(permission_allows_kv(
                &project_wide,
                "proj1",
                "staging",
                &[0x01, 0x02],
                true
            ));
            assert!(permission_allows_kv(
                &project_wide,
                "proj1",
                "prod",
                &[0x01, 0x02],
                true
            ));

            // But not a different project
            assert!(!permission_allows_kv(
                &project_wide,
                "proj2",
                "app",
                &[0x01, 0x02],
                true
            ));

            // scope_id: Some means exact scope match required
            let scope_specific = Permission::KvRead {
                project_id: "proj1".into(),
                scope_id: Some("app".into()),
                prefix: Some(vec![0x01]),
            };

            // Should only match the specific scope
            assert!(permission_allows_kv(
                &scope_specific,
                "proj1",
                "app",
                &[0x01, 0x02],
                true
            ));
            assert!(!permission_allows_kv(
                &scope_specific,
                "proj1",
                "staging",
                &[0x01, 0x02],
                true
            ));
        }

        #[test]
        fn read_vs_write_permissions() {
            let read_perm = Permission::KvRead {
                project_id: "proj1".into(),
                scope_id: Some("app".into()),
                prefix: Some(vec![0x01]),
            };

            let write_perm = Permission::KvWrite {
                project_id: "proj1".into(),
                scope_id: Some("app".into()),
                prefix: Some(vec![0x01]),
            };

            let key = &[0x01, 0x02];

            // Read permission should only allow read operations (read=true)
            assert!(permission_allows_kv(&read_perm, "proj1", "app", key, true));
            assert!(!permission_allows_kv(
                &read_perm, "proj1", "app", key, false
            ));

            // Write permission should only allow write operations (read=false)
            assert!(!permission_allows_kv(
                &write_perm,
                "proj1",
                "app",
                key,
                true
            ));
            assert!(permission_allows_kv(
                &write_perm,
                "proj1",
                "app",
                key,
                false
            ));
        }

        #[test]
        fn global_admin_bypasses_all_checks() {
            let perm = Permission::GlobalAdmin;

            // GlobalAdmin should allow any operation on any project/scope/key
            assert!(permission_allows_kv(
                &perm,
                "any-project",
                "any-scope",
                &[],
                true
            ));
            assert!(permission_allows_kv(
                &perm,
                "any-project",
                "any-scope",
                &[0xFF],
                false
            ));
            assert!(permission_allows_kv(
                &perm,
                "proj1",
                "app",
                &[0x01, 0x02, 0x03],
                true
            ));
        }

        #[test]
        fn project_admin_bypasses_prefix_checks() {
            let perm = Permission::ProjectAdmin {
                project_id: "proj1".into(),
            };

            // ProjectAdmin should allow any operation within the project, any scope, any key
            assert!(permission_allows_kv(&perm, "proj1", "app", &[], true));
            assert!(permission_allows_kv(&perm, "proj1", "app", &[0xFF], false));
            assert!(permission_allows_kv(
                &perm,
                "proj1",
                "staging",
                &[0x01, 0x02],
                true
            ));

            // But not a different project
            assert!(!permission_allows_kv(&perm, "proj2", "app", &[], true));
        }

        #[test]
        fn scope_admin_bypasses_prefix_checks_within_scope() {
            let perm = Permission::ScopeAdmin {
                project_id: "proj1".into(),
                scope_id: "app".into(),
            };

            // ScopeAdmin should allow any operation within the specific project+scope
            assert!(permission_allows_kv(&perm, "proj1", "app", &[], true));
            assert!(permission_allows_kv(&perm, "proj1", "app", &[0xFF], false));
            assert!(permission_allows_kv(
                &perm,
                "proj1",
                "app",
                &[0x01, 0x02],
                true
            ));

            // But not a different scope or project
            assert!(!permission_allows_kv(&perm, "proj1", "staging", &[], true));
            assert!(!permission_allows_kv(&perm, "proj2", "app", &[], true));
        }

        #[test]
        fn wrong_project_denies_access() {
            let perm = Permission::KvRead {
                project_id: "proj1".into(),
                scope_id: Some("app".into()),
                prefix: Some(vec![0x01]),
            };

            // Even with matching scope and key, wrong project should deny
            assert!(!permission_allows_kv(
                &perm,
                "proj2",
                "app",
                &[0x01, 0x02],
                true
            ));
        }

        #[test]
        fn unicode_safe_binary_prefix() {
            // Prefixes are binary, not string-based, so should work with any byte values
            let perm = Permission::KvRead {
                project_id: "proj1".into(),
                scope_id: Some("app".into()),
                prefix: Some(vec![0xFF, 0xFE, 0xFD]), // Invalid UTF-8
            };

            // Should match keys starting with these bytes
            assert!(permission_allows_kv(
                &perm,
                "proj1",
                "app",
                &[0xFF, 0xFE, 0xFD],
                true
            ));
            assert!(permission_allows_kv(
                &perm,
                "proj1",
                "app",
                &[0xFF, 0xFE, 0xFD, 0x00],
                true
            ));
            assert!(!permission_allows_kv(
                &perm,
                "proj1",
                "app",
                &[0xFF, 0xFE],
                true
            ));
        }

        #[test]
        fn integration_has_kv_permission() {
            // Test the full has_kv_permission flow with multiple permissions
            let mut catalog = Catalog::default();
            catalog.create_project("proj1").unwrap();

            let caller_id = "user1";

            // Grant overlapping permissions
            catalog
                .grant_permission(
                    caller_id,
                    Permission::KvRead {
                        project_id: "proj1".into(),
                        scope_id: Some("app".into()),
                        prefix: Some(vec![0x01]),
                    },
                    None,
                    false,
                )
                .unwrap();

            catalog
                .grant_permission(
                    caller_id,
                    Permission::KvRead {
                        project_id: "proj1".into(),
                        scope_id: Some("app".into()),
                        prefix: Some(vec![0x01, 0x02]),
                    },
                    None,
                    false,
                )
                .unwrap();

            // Should be able to read keys matching either prefix
            assert!(catalog.has_kv_permission(caller_id, "proj1", "app", &[0x01], true));
            assert!(catalog.has_kv_permission(caller_id, "proj1", "app", &[0x01, 0x02], true));
            assert!(catalog.has_kv_permission(
                caller_id,
                "proj1",
                "app",
                &[0x01, 0x02, 0x03],
                true
            ));
            assert!(catalog.has_kv_permission(caller_id, "proj1", "app", &[0x01, 0xFF], true));

            // Should NOT be able to read keys not matching any prefix
            assert!(!catalog.has_kv_permission(caller_id, "proj1", "app", &[0x02], true));
            assert!(!catalog.has_kv_permission(caller_id, "proj1", "app", &[], true));

            // Should NOT be able to write (only granted read)
            assert!(!catalog.has_kv_permission(caller_id, "proj1", "app", &[0x01], false));
        }
    }
}
