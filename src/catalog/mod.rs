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
use std::collections::{BTreeSet, HashMap as StdHashMap, VecDeque};
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
    last_accessed_at: Instant,
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
    #[serde(skip)]
    permission_cache_order: Arc<RwLock<VecDeque<(String, Permission)>>>,
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
            permission_cache_order: Arc::new(RwLock::new(VecDeque::new())),
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
        if let Ok(mut order) = self.permission_cache_order.write() {
            order.retain(|(cached_caller_id, _)| cached_caller_id != caller_id);
        }
    }

    fn permission_cache_get(&self, key: &(String, Permission), now: Instant) -> Option<bool> {
        let mut cache = self.permission_check_cache.write().ok()?;
        let entry = cache.get_mut(key)?;
        if entry.expires_at <= now {
            cache.remove(key);
            drop(cache);
            if let Ok(mut order) = self.permission_cache_order.write() {
                order.retain(|candidate| candidate != key);
            }
            return None;
        }
        entry.last_accessed_at = now;
        Some(entry.allowed)
    }

    fn permission_cache_put(&self, key: (String, Permission), allowed: bool, now: Instant) {
        if let Ok(mut cache) = self.permission_check_cache.write() {
            let mut order = self.permission_cache_order.write().ok();
            if !cache.contains_key(&key) && cache.len() >= Self::PERMISSION_CACHE_MAX_ENTRIES {
                let evicted = if let Some(order) = order.as_mut() {
                    let mut evicted = None;
                    while let Some(candidate) = order.pop_front() {
                        if let Some(entry) = cache.get(&candidate) {
                            if entry.expires_at <= now {
                                cache.remove(&candidate);
                                continue;
                            }
                            evicted = Some(candidate);
                            break;
                        }
                    }
                    evicted
                } else {
                    cache
                        .iter()
                        .min_by_key(|(_, entry)| entry.last_accessed_at)
                        .map(|(candidate, _)| candidate.clone())
                };
                if let Some(evicted) = evicted {
                    cache.remove(&evicted);
                }
            }
            if let Some(order) = order.as_mut() {
                order.push_back(key.clone());
            }
            cache.insert(
                key,
                PermissionCacheEntry {
                    allowed,
                    expires_at: now + Self::PERMISSION_CACHE_TTL,
                    last_accessed_at: now,
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

        let source = resolve_permission_grant(granted, required);
        let allowed = source.is_some();

        // Log GlobalAdmin usage for audit trail (break-glass access)
        if matches!(source, Some(PermissionGrantSource::GlobalAdmin)) {
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
        let mut check_fk_cycle = false;
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
                check_fk_cycle = true;
            }
            TableAlteration::DropForeignKey { name } => {
                table.foreign_keys.retain(|fk| fk.name != name);
            }
        }
        let _ = table;
        if check_fk_cycle && foreign_key_graph_has_cycle(self) {
            let table = self
                .tables
                .get_mut(&key)
                .ok_or_else(|| AedbError::Validation("table missing after fk update".into()))?;
            table.foreign_keys.pop();
            return Err(AedbError::Validation(
                "foreign key cycle detected; cascading cycles are not allowed".into(),
            ));
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PermissionGrantSource {
    Direct,
    Hierarchical,
    GlobalAdmin,
}

fn resolve_permission_grant(
    granted: &BTreeSet<Permission>,
    required: &Permission,
) -> Option<PermissionGrantSource> {
    if granted.contains(&Permission::GlobalAdmin) {
        return Some(PermissionGrantSource::GlobalAdmin);
    }
    if granted.contains(required) {
        return Some(PermissionGrantSource::Direct);
    }
    granted
        .iter()
        .any(|permission| permission_implies(permission, required))
        .then_some(PermissionGrantSource::Hierarchical)
}

fn permission_implies(granted: &Permission, required: &Permission) -> bool {
    match granted {
        Permission::ProjectAdmin { project_id } => match required {
            Permission::ProjectAdmin {
                project_id: required_project_id,
            }
            | Permission::TableDdl {
                project_id: required_project_id,
            }
            | Permission::ScopeAdmin {
                project_id: required_project_id,
                ..
            }
            | Permission::TableRead {
                project_id: required_project_id,
                ..
            }
            | Permission::TableWrite {
                project_id: required_project_id,
                ..
            }
            | Permission::IndexRead {
                project_id: required_project_id,
                ..
            } => project_id == required_project_id,
            _ => false,
        },
        Permission::ScopeAdmin {
            project_id,
            scope_id,
        } => match required {
            Permission::ScopeAdmin {
                project_id: required_project_id,
                scope_id: required_scope_id,
            }
            | Permission::TableRead {
                project_id: required_project_id,
                scope_id: required_scope_id,
                ..
            }
            | Permission::TableWrite {
                project_id: required_project_id,
                scope_id: required_scope_id,
                ..
            }
            | Permission::IndexRead {
                project_id: required_project_id,
                scope_id: required_scope_id,
                ..
            } => project_id == required_project_id && scope_id == required_scope_id,
            _ => false,
        },
        _ => false,
    }
}

fn foreign_key_graph_has_cycle(catalog: &Catalog) -> bool {
    fn visit(
        node: &(String, String),
        catalog: &Catalog,
        visiting: &mut BTreeSet<(String, String)>,
        visited: &mut BTreeSet<(String, String)>,
    ) -> bool {
        if visited.contains(node) {
            return false;
        }
        if !visiting.insert(node.clone()) {
            return true;
        }
        if let Some(schema) = catalog.tables.get(node) {
            for fk in &schema.foreign_keys {
                let target = (
                    namespace_key(&fk.references_project_id, &fk.references_scope_id),
                    fk.references_table.clone(),
                );
                if visit(&target, catalog, visiting, visited) {
                    return true;
                }
            }
        }
        visiting.remove(node);
        visited.insert(node.clone());
        false
    }

    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    for node in catalog.tables.keys() {
        if visit(node, catalog, &mut visiting, &mut visited) {
            return true;
        }
    }
    false
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
    let mut key =
        String::with_capacity(project_id.len() + NAMESPACE_KEY_SEPARATOR.len() + scope_id.len());
    key.push_str(project_id);
    key.push_str(NAMESPACE_KEY_SEPARATOR);
    key.push_str(scope_id);
    key
}

pub const DEFAULT_SCOPE_ID: &str = "app";
pub const KV_INDEX_TABLE: &str = "__kv_index";
pub const SYSTEM_PROJECT_ID: &str = "_system";

// Canonical names for engine-internal system tables, kept here (next to
// SYSTEM_PROJECT_ID) so the commit/apply/engine/lib layers share one source of
// truth instead of each redefining them. All live under SYSTEM_PROJECT_ID /
// SYSTEM_SCOPE_ID.
/// Reserved scope id for engine-internal system tables.
pub(crate) const SYSTEM_SCOPE_ID: &str = "app";
pub(crate) const AUTHZ_AUDIT_TABLE: &str = "authz_audit";
pub(crate) const ASSERTION_AUDIT_TABLE: &str = "assertion_audit";
pub(crate) const LIFECYCLE_OUTBOX_TABLE: &str = "lifecycle_outbox";
pub(crate) const EVENT_OUTBOX_TABLE: &str = "event_outbox";
pub(crate) const REACTIVE_PROCESSOR_CHECKPOINTS_TABLE: &str = "reactive_processor_checkpoints";
pub(crate) const REACTIVE_PROCESSOR_REGISTRY_TABLE: &str = "reactive_processor_registry";
pub(crate) const REACTIVE_PROCESSOR_DLQ_TABLE: &str = "reactive_processor_dead_letters";
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
mod tests;
