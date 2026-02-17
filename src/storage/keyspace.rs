use crate::catalog::namespace_key;
use crate::catalog::types::{Row, Value};
use crate::config::PrimaryIndexBackend;
use crate::query::plan::Expr;
use crate::storage::encoded_key::EncodedKey;
use im::{HashMap, OrdMap, OrdSet};
use primitive_types::U256;
use serde::{Deserialize, Serialize};
use std::ops::Bound;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum NamespaceId {
    #[default]
    System,
    Global,
    Project(String),
}

impl NamespaceId {
    pub fn project_scope(project_id: &str, scope_id: &str) -> Self {
        Self::Project(namespace_key(project_id, scope_id))
    }

    pub fn as_project_scope_key(&self) -> Option<&str> {
        match self {
            Self::Project(v) => Some(v.as_str()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SecondaryIndexStore {
    BTree(OrdMap<EncodedKey, OrdSet<EncodedKey>>),
    Hash(HashMap<EncodedKey, OrdSet<EncodedKey>>),
    UniqueHash(HashMap<EncodedKey, EncodedKey>),
}

impl Default for SecondaryIndexStore {
    fn default() -> Self {
        Self::BTree(OrdMap::new())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SecondaryIndex {
    pub store: SecondaryIndexStore,
    pub columns_bitmask: u128,
    pub partial_filter: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TableData {
    pub rows: OrdMap<EncodedKey, Row>,
    pub row_versions: OrdMap<EncodedKey, u64>,
    #[serde(default)]
    pub structural_version: u64,
    pub pk_hash: HashMap<EncodedKey, ()>,
    pub row_cache: HashMap<EncodedKey, Row>,
    pub row_versions_cache: HashMap<EncodedKey, u64>,
    pub indexes: HashMap<String, SecondaryIndex>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvEntry {
    pub value: Vec<u8>,
    pub version: u64,
    pub created_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct KvData {
    pub entries: OrdMap<Vec<u8>, KvEntry>,
    #[serde(default)]
    pub structural_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AsyncProjectionData {
    pub rows: OrdMap<EncodedKey, Row>,
    pub materialized_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Namespace {
    pub id: NamespaceId,
    pub tables: HashMap<String, TableData>,
    pub kv: KvData,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Keyspace {
    #[serde(default = "default_primary_index_backend")]
    pub primary_index_backend: PrimaryIndexBackend,
    #[serde(
        serialize_with = "serialize_arc_hashmap",
        deserialize_with = "deserialize_arc_hashmap"
    )]
    pub namespaces: Arc<HashMap<NamespaceId, Namespace>>,
    #[serde(
        serialize_with = "serialize_arc_async_indexes",
        deserialize_with = "deserialize_arc_async_indexes"
    )]
    pub async_indexes: Arc<HashMap<(NamespaceId, String, String), AsyncProjectionData>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyspaceSnapshot {
    #[serde(default = "default_primary_index_backend")]
    pub primary_index_backend: PrimaryIndexBackend,
    #[serde(
        serialize_with = "serialize_arc_hashmap",
        deserialize_with = "deserialize_arc_hashmap"
    )]
    pub namespaces: Arc<HashMap<NamespaceId, Namespace>>,
    #[serde(
        serialize_with = "serialize_arc_async_indexes",
        deserialize_with = "deserialize_arc_async_indexes"
    )]
    pub async_indexes: Arc<HashMap<(NamespaceId, String, String), AsyncProjectionData>>,
}

// Custom serde for Arc<HashMap>
fn serialize_arc_hashmap<S>(
    value: &Arc<HashMap<NamespaceId, Namespace>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    (**value).serialize(serializer)
}

fn deserialize_arc_hashmap<'de, D>(
    deserializer: D,
) -> Result<Arc<HashMap<NamespaceId, Namespace>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    HashMap::deserialize(deserializer).map(Arc::new)
}

fn serialize_arc_async_indexes<S>(
    value: &Arc<HashMap<(NamespaceId, String, String), AsyncProjectionData>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    (**value).serialize(serializer)
}

type AsyncIndexesMap = HashMap<(NamespaceId, String, String), AsyncProjectionData>;

fn deserialize_arc_async_indexes<'de, D>(deserializer: D) -> Result<Arc<AsyncIndexesMap>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    HashMap::deserialize(deserializer).map(Arc::new)
}

impl Keyspace {
    pub fn with_backend(primary_index_backend: PrimaryIndexBackend) -> Self {
        Self {
            primary_index_backend,
            namespaces: Arc::new(HashMap::new()),
            async_indexes: Arc::new(HashMap::new()),
        }
    }

    /// Ensures the namespaces HashMap is uniquely owned, cloning if necessary (copy-on-write).
    /// Returns a mutable reference to the underlying HashMap.
    fn namespaces_mut(&mut self) -> &mut HashMap<NamespaceId, Namespace> {
        Arc::make_mut(&mut self.namespaces)
    }

    /// Ensures the async_indexes HashMap is uniquely owned, cloning if necessary (copy-on-write).
    /// Returns a mutable reference to the underlying HashMap.
    fn async_indexes_mut(
        &mut self,
    ) -> &mut HashMap<(NamespaceId, String, String), AsyncProjectionData> {
        Arc::make_mut(&mut self.async_indexes)
    }

    /// Inserts an async projection into the keyspace (copy-on-write).
    pub fn insert_async_projection(
        &mut self,
        ns_id: NamespaceId,
        table_name: String,
        index_name: String,
        data: AsyncProjectionData,
    ) {
        Arc::make_mut(&mut self.async_indexes).insert((ns_id, table_name, index_name), data);
    }

    /// Removes an async projection from the keyspace (copy-on-write).
    pub fn remove_async_projection(
        &mut self,
        ns_id: &NamespaceId,
        table_name: &str,
        index_name: &str,
    ) {
        Arc::make_mut(&mut self.async_indexes).remove(&(
            ns_id.clone(),
            table_name.to_string(),
            index_name.to_string(),
        ));
    }

    /// Inserts a namespace into the keyspace (copy-on-write).
    pub fn insert_namespace(&mut self, ns_id: NamespaceId, namespace: Namespace) {
        Arc::make_mut(&mut self.namespaces).insert(ns_id, namespace);
    }

    pub fn set_backend(&mut self, backend: PrimaryIndexBackend) {
        self.primary_index_backend = backend;
        self.rebuild_point_lookup_caches();
    }

    fn rebuild_point_lookup_caches(&mut self) {
        let namespace_ids: Vec<NamespaceId> = self.namespaces.keys().cloned().collect();
        for namespace_id in namespace_ids {
            let Some(namespace) = self.namespaces_mut().get_mut(&namespace_id) else {
                continue;
            };
            let table_names: Vec<String> = namespace.tables.keys().cloned().collect();
            for table_name in table_names {
                let Some(table) = namespace.tables.get_mut(&table_name) else {
                    continue;
                };
                table.row_cache = table.rows.clone().into_iter().collect();
                table.row_versions_cache = table.row_versions.clone().into_iter().collect();
            }
        }
    }

    pub fn namespace_mut(&mut self, namespace_id: NamespaceId) -> &mut Namespace {
        self.namespaces_mut()
            .entry(namespace_id.clone())
            .or_insert_with(|| Namespace {
                id: namespace_id,
                tables: HashMap::new(),
                kv: KvData::default(),
            })
    }

    pub fn namespace(&self, namespace_id: &NamespaceId) -> Option<&Namespace> {
        self.namespaces.get(namespace_id)
    }

    pub fn table_mut(
        &mut self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
    ) -> &mut TableData {
        self.namespace_mut(NamespaceId::project_scope(project_id, scope_id))
            .tables
            .entry(table_name.to_string())
            .or_default()
    }

    pub fn table_mut_by_namespace_key(
        &mut self,
        namespace: &str,
        table_name: &str,
    ) -> &mut TableData {
        self.namespace_mut(NamespaceId::Project(namespace.to_string()))
            .tables
            .entry(table_name.to_string())
            .or_default()
    }

    pub fn table_by_namespace_key(&self, namespace: &str, table_name: &str) -> Option<&TableData> {
        self.namespace(&NamespaceId::Project(namespace.to_string()))
            .and_then(|ns| ns.tables.get(table_name))
    }

    pub fn table_by_namespace_key_mut(
        &mut self,
        namespace: &str,
        table_name: &str,
    ) -> Option<&mut TableData> {
        self.namespace_mut(NamespaceId::Project(namespace.to_string()))
            .tables
            .get_mut(table_name)
    }

    pub fn upsert_row(
        &mut self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        pk: Vec<Value>,
        row: Row,
        commit_seq: u64,
    ) {
        let use_cache = self.primary_index_backend == PrimaryIndexBackend::ArtExperimental;
        let table = self.table_mut(project_id, scope_id, table_name);
        let encoded_pk = EncodedKey::from_values(&pk);
        if !table.rows.contains_key(&encoded_pk) {
            table.structural_version = commit_seq;
        }
        table.rows.insert(encoded_pk.clone(), row.clone());
        table.row_versions.insert(encoded_pk.clone(), commit_seq);
        table.pk_hash.insert(encoded_pk.clone(), ());
        if use_cache {
            table.row_cache.insert(encoded_pk.clone(), row);
            table.row_versions_cache.insert(encoded_pk, commit_seq);
        }
    }

    pub fn upsert_row_by_encoded_pk(
        &mut self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        encoded_pk: EncodedKey,
        row: Row,
        commit_seq: u64,
    ) {
        let use_cache = self.primary_index_backend == PrimaryIndexBackend::ArtExperimental;
        let table = self.table_mut(project_id, scope_id, table_name);
        if !table.rows.contains_key(&encoded_pk) {
            table.structural_version = commit_seq;
        }
        table.rows.insert(encoded_pk.clone(), row.clone());
        table.row_versions.insert(encoded_pk.clone(), commit_seq);
        table.pk_hash.insert(encoded_pk.clone(), ());
        if use_cache {
            table.row_cache.insert(encoded_pk.clone(), row);
            table.row_versions_cache.insert(encoded_pk, commit_seq);
        }
    }

    pub fn get_row(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        pk: &[Value],
    ) -> Option<&Row> {
        let encoded_pk = EncodedKey::from_values(pk);
        self.get_row_by_encoded(project_id, scope_id, table_name, &encoded_pk)
    }

    pub fn get_row_by_encoded(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        encoded_pk: &EncodedKey,
    ) -> Option<&Row> {
        self.namespace(&NamespaceId::project_scope(project_id, scope_id))
            .and_then(|ns| ns.tables.get(table_name))
            .and_then(|t| {
                if self.primary_index_backend == PrimaryIndexBackend::ArtExperimental {
                    t.row_cache
                        .get(encoded_pk)
                        .or_else(|| t.rows.get(encoded_pk))
                } else {
                    t.rows.get(encoded_pk)
                }
            })
    }

    pub fn delete_row(
        &mut self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        pk: &[Value],
        commit_seq: u64,
    ) -> Option<Row> {
        let encoded_pk = EncodedKey::from_values(pk);
        self.delete_row_by_encoded(project_id, scope_id, table_name, &encoded_pk, commit_seq)
    }

    pub fn delete_row_by_encoded(
        &mut self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        encoded_pk: &EncodedKey,
        commit_seq: u64,
    ) -> Option<Row> {
        let table = self
            .namespace_mut(NamespaceId::project_scope(project_id, scope_id))
            .tables
            .get_mut(table_name)?;
        table.row_versions.remove(encoded_pk);
        table.pk_hash.remove(encoded_pk);
        table.row_cache.remove(encoded_pk);
        table.row_versions_cache.remove(encoded_pk);
        let removed = table.rows.remove(encoded_pk);
        if removed.is_some() {
            table.structural_version = commit_seq;
        }
        removed
    }

    /// Creates a snapshot of the keyspace.
    ///
    /// This is now an O(1) operation that just clones Arc pointers,
    /// providing true copy-on-write semantics with zero traversal cost.
    pub fn snapshot(&self) -> KeyspaceSnapshot {
        KeyspaceSnapshot {
            primary_index_backend: self.primary_index_backend,
            namespaces: Arc::clone(&self.namespaces),
            async_indexes: Arc::clone(&self.async_indexes),
        }
    }

    pub fn drop_table(&mut self, project_id: &str, scope_id: &str, table_name: &str) {
        let ns = NamespaceId::project_scope(project_id, scope_id);
        if let Some(namespace) = self.namespaces_mut().get_mut(&ns) {
            namespace.tables.remove(table_name);
        }
        let async_keys: Vec<(NamespaceId, String, String)> = self
            .async_indexes
            .keys()
            .filter(|(p, t, _)| p == &ns && t == table_name)
            .cloned()
            .collect();
        for key in async_keys {
            self.async_indexes_mut().remove(&key);
        }
    }

    pub fn drop_project(&mut self, project_id: &str) {
        let prefix = format!("{project_id}::");
        let ns_keys: Vec<NamespaceId> = self
            .namespaces
            .keys()
            .filter(|ns| {
                ns.as_project_scope_key()
                    .map(|k| k.starts_with(&prefix))
                    .unwrap_or(false)
            })
            .cloned()
            .collect();
        for key in ns_keys {
            self.namespaces_mut().remove(&key);
        }
        let async_keys: Vec<(NamespaceId, String, String)> = self
            .async_indexes
            .keys()
            .filter(|(p, _, _)| {
                p.as_project_scope_key()
                    .map(|k| k.starts_with(&prefix))
                    .unwrap_or(false)
            })
            .cloned()
            .collect();
        for key in async_keys {
            self.async_indexes_mut().remove(&key);
        }
    }

    pub fn drop_scope(&mut self, project_id: &str, scope_id: &str) {
        let ns = NamespaceId::project_scope(project_id, scope_id);
        self.namespaces_mut().remove(&ns);
        let async_keys: Vec<(NamespaceId, String, String)> = self
            .async_indexes
            .keys()
            .filter(|(p, _, _)| p == &ns)
            .cloned()
            .collect();
        for key in async_keys {
            self.async_indexes_mut().remove(&key);
        }
    }

    pub fn get_row_version(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        pk: &[Value],
    ) -> u64 {
        let encoded_pk = EncodedKey::from_values(pk);
        self.namespace(&NamespaceId::project_scope(project_id, scope_id))
            .and_then(|ns| ns.tables.get(table_name))
            .and_then(|t| {
                if self.primary_index_backend == PrimaryIndexBackend::ArtExperimental {
                    t.row_versions_cache
                        .get(&encoded_pk)
                        .copied()
                        .or_else(|| t.row_versions.get(&encoded_pk).copied())
                } else {
                    t.row_versions.get(&encoded_pk).copied()
                }
            })
            .unwrap_or(0)
    }

    pub fn max_row_version_in_encoded_range(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        start: Bound<EncodedKey>,
        end: Bound<EncodedKey>,
    ) -> u64 {
        self.namespace(&NamespaceId::project_scope(project_id, scope_id))
            .and_then(|ns| ns.tables.get(table_name))
            .map(|t| {
                t.row_versions
                    .range((start, end))
                    .map(|(_, version)| *version)
                    .max()
                    .unwrap_or(0)
            })
            .unwrap_or(0)
    }

    pub fn table_structural_version(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
    ) -> u64 {
        self.namespace(&NamespaceId::project_scope(project_id, scope_id))
            .and_then(|ns| ns.tables.get(table_name))
            .map(|t| t.structural_version)
            .unwrap_or(0)
    }

    fn kv_data_mut(&mut self, project_id: &str, scope_id: &str) -> &mut KvData {
        &mut self
            .namespace_mut(NamespaceId::project_scope(project_id, scope_id))
            .kv
    }

    pub fn kv_get(&self, project_id: &str, scope_id: &str, key: &[u8]) -> Option<&KvEntry> {
        self.namespace(&NamespaceId::project_scope(project_id, scope_id))
            .and_then(|ns| ns.kv.entries.get(key))
    }

    pub fn kv_set(
        &mut self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        value: Vec<u8>,
        commit_seq: u64,
    ) {
        let kv = self.kv_data_mut(project_id, scope_id);
        if !kv.entries.contains_key(&key) {
            kv.structural_version = commit_seq;
        }
        let created_at = kv
            .entries
            .get(&key)
            .map(|e| e.created_at)
            .unwrap_or(commit_seq);
        kv.entries.insert(
            key,
            KvEntry {
                value,
                version: commit_seq,
                created_at,
            },
        );
    }

    pub fn kv_del(
        &mut self,
        project_id: &str,
        scope_id: &str,
        key: &[u8],
        commit_seq: u64,
    ) -> bool {
        let kv = self.kv_data_mut(project_id, scope_id);
        let removed = kv.entries.remove(key).is_some();
        if removed {
            kv.structural_version = commit_seq;
        }
        removed
    }

    pub fn kv_scan_prefix(
        &self,
        project_id: &str,
        scope_id: &str,
        prefix: &[u8],
        limit: usize,
    ) -> Vec<(Vec<u8>, KvEntry)> {
        let Some(kv) = self
            .namespace(&NamespaceId::project_scope(project_id, scope_id))
            .map(|ns| &ns.kv)
        else {
            return Vec::new();
        };
        kv.entries
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .take(limit)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn kv_scan_range(
        &self,
        project_id: &str,
        scope_id: &str,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
        limit: usize,
    ) -> Vec<(Vec<u8>, KvEntry)> {
        let Some(kv) = self
            .namespace(&NamespaceId::project_scope(project_id, scope_id))
            .map(|ns| &ns.kv)
        else {
            return Vec::new();
        };
        kv.entries
            .range((start, end))
            .take(limit)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn kv_inc_u256(
        &mut self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount: U256,
        commit_seq: u64,
    ) -> Result<U256, crate::error::AedbError> {
        let current = self
            .kv_get(project_id, scope_id, &key)
            .map(|e| decode_u256(&e.value))
            .transpose()?
            .unwrap_or(U256::zero());
        let next = current
            .checked_add(amount)
            .ok_or(crate::error::AedbError::Overflow)?;
        self.kv_set(project_id, scope_id, key, encode_u256(next), commit_seq);
        Ok(next)
    }

    pub fn kv_dec_u256(
        &mut self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount: U256,
        commit_seq: u64,
    ) -> Result<U256, crate::error::AedbError> {
        let current = self
            .kv_get(project_id, scope_id, &key)
            .map(|e| decode_u256(&e.value))
            .transpose()?
            .unwrap_or(U256::zero());
        if current < amount {
            return Err(crate::error::AedbError::Underflow);
        }
        let next = current - amount;
        self.kv_set(project_id, scope_id, key, encode_u256(next), commit_seq);
        Ok(next)
    }

    pub fn kv_version(&self, project_id: &str, scope_id: &str, key: &[u8]) -> u64 {
        self.kv_get(project_id, scope_id, key)
            .map(|e| e.version)
            .unwrap_or(0)
    }

    pub fn max_kv_version_in_range(
        &self,
        project_id: &str,
        scope_id: &str,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
    ) -> u64 {
        let Some(kv) = self
            .namespace(&NamespaceId::project_scope(project_id, scope_id))
            .map(|ns| &ns.kv)
        else {
            return 0;
        };
        kv.entries
            .range((start, end))
            .map(|(_, entry)| entry.version)
            .max()
            .unwrap_or(0)
    }

    pub fn kv_structural_version(&self, project_id: &str, scope_id: &str) -> u64 {
        self.namespace(&NamespaceId::project_scope(project_id, scope_id))
            .map(|ns| ns.kv.structural_version)
            .unwrap_or(0)
    }

    pub fn estimate_memory_bytes(&self) -> usize {
        let row_bytes = self
            .namespaces
            .values()
            .map(|ns| {
                ns.tables
                    .values()
                    .map(|t| {
                        t.rows.values().map(estimate_row_bytes).sum::<usize>() + t.rows.len() * 32
                    })
                    .sum::<usize>()
            })
            .sum::<usize>();
        let kv_bytes = self
            .namespaces
            .values()
            .map(|ns| {
                ns.kv
                    .entries
                    .iter()
                    .map(|(key, value)| key.len() + value.value.len() + 24)
                    .sum::<usize>()
            })
            .sum::<usize>();
        let projection_bytes = self
            .async_indexes
            .values()
            .map(|p| p.rows.values().map(estimate_row_bytes).sum::<usize>())
            .sum::<usize>();
        row_bytes + kv_bytes + projection_bytes
    }
}

impl KeyspaceSnapshot {
    pub fn table(&self, project_id: &str, scope_id: &str, table_name: &str) -> Option<&TableData> {
        self.namespaces
            .get(&NamespaceId::project_scope(project_id, scope_id))
            .and_then(|ns| ns.tables.get(table_name))
    }

    pub fn table_by_namespace_key(&self, namespace: &str, table_name: &str) -> Option<&TableData> {
        self.namespaces
            .get(&NamespaceId::Project(namespace.to_string()))
            .and_then(|ns| ns.tables.get(table_name))
    }

    pub fn kv_get(&self, project_id: &str, scope_id: &str, key: &[u8]) -> Option<&KvEntry> {
        self.namespaces
            .get(&NamespaceId::project_scope(project_id, scope_id))
            .and_then(|ns| ns.kv.entries.get(key))
    }

    pub fn async_index(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        index_name: &str,
    ) -> Option<&AsyncProjectionData> {
        self.async_indexes.get(&(
            NamespaceId::project_scope(project_id, scope_id),
            table_name.to_string(),
            index_name.to_string(),
        ))
    }

    pub fn async_index_by_namespace_key(
        &self,
        namespace: &str,
        table_name: &str,
        index_name: &str,
    ) -> Option<&AsyncProjectionData> {
        self.async_indexes.get(&(
            NamespaceId::Project(namespace.to_string()),
            table_name.to_string(),
            index_name.to_string(),
        ))
    }
}

fn default_primary_index_backend() -> PrimaryIndexBackend {
    PrimaryIndexBackend::OrdMap
}

fn encode_u256(v: U256) -> Vec<u8> {
    let mut bytes = [0u8; 32];
    v.to_big_endian(&mut bytes);
    bytes.to_vec()
}

fn decode_u256(bytes: &[u8]) -> Result<U256, crate::error::AedbError> {
    if bytes.len() != 32 {
        return Err(crate::error::AedbError::Validation(
            "invalid u256 bytes length".into(),
        ));
    }
    Ok(U256::from_big_endian(bytes))
}

fn estimate_row_bytes(row: &Row) -> usize {
    row.values.iter().map(estimate_value_bytes).sum::<usize>() + 16
}

fn estimate_value_bytes(v: &Value) -> usize {
    match v {
        Value::Text(s) | Value::Json(s) => s.len(),
        Value::Integer(_) | Value::Float(_) | Value::Timestamp(_) => 8,
        Value::Boolean(_) => 1,
        Value::U256(_) | Value::I256(_) => 32,
        Value::Blob(b) => b.len(),
        Value::Null => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::{Keyspace, NamespaceId};
    use crate::catalog::types::{Row, Value};
    use crate::config::PrimaryIndexBackend;
    use crate::storage::encoded_key::EncodedKey;

    fn row(values: Vec<Value>) -> Row {
        Row::from_values(values)
    }

    #[test]
    fn snapshot_isolation_works() {
        let project = "p";
        let scope = "app";
        let table = "t";

        let mut ks = Keyspace::default();
        ks.upsert_row(
            project,
            scope,
            table,
            vec![Value::Integer(1)],
            row(vec![Value::Text("A".into())]),
            1,
        );
        ks.upsert_row(
            project,
            scope,
            table,
            vec![Value::Integer(2)],
            row(vec![Value::Text("B".into())]),
            2,
        );
        ks.upsert_row(
            project,
            scope,
            table,
            vec![Value::Integer(3)],
            row(vec![Value::Text("C".into())]),
            3,
        );

        let s1 = ks.snapshot();

        ks.delete_row(project, scope, table, &[Value::Integer(2)], 4);
        ks.upsert_row(
            project,
            scope,
            table,
            vec![Value::Integer(4)],
            row(vec![Value::Text("D".into())]),
            4,
        );

        let s2 = ks.snapshot();

        let s1_rows = &s1.table(project, scope, table).expect("table").rows;
        assert!(
            s1_rows.contains_key(&EncodedKey::from_values(&[Value::Integer(1)])),
            "s1 should contain row 1"
        );
        assert!(
            s1_rows.contains_key(&EncodedKey::from_values(&[Value::Integer(2)])),
            "s1 should contain row 2"
        );
        assert!(
            s1_rows.contains_key(&EncodedKey::from_values(&[Value::Integer(3)])),
            "s1 should contain row 3"
        );
        assert!(
            !s1_rows.contains_key(&EncodedKey::from_values(&[Value::Integer(4)])),
            "s1 should not contain row 4"
        );

        let s2_rows = &s2.table(project, scope, table).expect("table").rows;
        assert!(s2_rows.contains_key(&EncodedKey::from_values(&[Value::Integer(1)])));
        assert!(!s2_rows.contains_key(&EncodedKey::from_values(&[Value::Integer(2)])));
        assert!(s2_rows.contains_key(&EncodedKey::from_values(&[Value::Integer(3)])));
        assert!(s2_rows.contains_key(&EncodedKey::from_values(&[Value::Integer(4)])));
    }

    #[test]
    fn memory_estimate_is_non_zero_for_populated_state() {
        let mut ks = Keyspace::default();
        ks.upsert_row(
            "p",
            "app",
            "t",
            vec![Value::Integer(1)],
            row(vec![Value::Text("abc".into()), Value::U256([1u8; 32])]),
            1,
        );
        ks.kv_set("p", "app", b"k".to_vec(), b"v".to_vec(), 2);
        assert!(ks.estimate_memory_bytes() > 0);
    }

    #[test]
    fn drop_project_preserves_global_and_system_namespaces() {
        let mut ks = Keyspace::default();
        ks.namespace_mut(NamespaceId::System)
            .tables
            .entry("permissions".into())
            .or_default();
        ks.namespace_mut(NamespaceId::Global)
            .tables
            .entry("users".into())
            .or_default();
        ks.upsert_row(
            "tenant",
            "app",
            "users",
            vec![Value::Integer(1)],
            row(vec![Value::Text("alice".into())]),
            1,
        );

        ks.drop_project("tenant");

        assert!(ks.namespace(&NamespaceId::System).is_some());
        assert!(ks.namespace(&NamespaceId::Global).is_some());
        assert!(
            ks.namespace(&NamespaceId::Project("tenant::app".into()))
                .is_none()
        );
    }

    #[test]
    fn art_experimental_backend_uses_point_lookup_cache() {
        let mut ks = Keyspace::with_backend(PrimaryIndexBackend::OrdMap);
        ks.upsert_row(
            "p",
            "app",
            "users",
            vec![Value::Integer(1)],
            row(vec![Value::Text("alice".into())]),
            11,
        );
        ks.set_backend(PrimaryIndexBackend::ArtExperimental);
        assert_eq!(
            ks.primary_index_backend,
            PrimaryIndexBackend::ArtExperimental
        );
        let row = ks
            .get_row_by_encoded(
                "p",
                "app",
                "users",
                &EncodedKey::from_values(&[Value::Integer(1)]),
            )
            .expect("row");
        assert_eq!(row.values[0], Value::Text("alice".into()));
        assert_eq!(
            ks.get_row_version("p", "app", "users", &[Value::Integer(1)]),
            11
        );
    }
}
