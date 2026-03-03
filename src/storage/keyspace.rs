use crate::catalog::namespace_key;
use crate::catalog::types::{Row, Value};
use crate::commit::validation::{
    KvIntegerAmount, KvIntegerMissingPolicy, KvIntegerUnderflowPolicy, KvU64MissingPolicy,
    KvU64MutatorOp, KvU64OverflowPolicy, KvU64UnderflowPolicy, KvU256MissingPolicy,
    KvU256MutatorOp, KvU256OverflowPolicy, KvU256UnderflowPolicy, counter_shard_index,
    counter_shard_storage_key,
};
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccumulatorDedupeRecord {
    pub order_key: u64,
    pub delta: i64,
    pub commit_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenExposureRecord {
    pub amount: i64,
    pub opened_at_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccumulatorDeltaRecord {
    pub order_key: u64,
    pub delta: i64,
    pub commit_seq: u64,
    pub dedupe_key: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccumulatorAppendResult {
    Applied,
    DuplicateNoop,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AccumulatorData {
    pub value: i64,
    pub last_applied_order_key: u64,
    pub latest_order_key: u64,
    pub materialized_seq: u64,
    pub latest_seq: u64,
    #[serde(default)]
    pub applied_since_snapshot: u64,
    #[serde(default)]
    pub projector_error: Option<String>,
    #[serde(default = "default_exposure_margin_bps")]
    pub exposure_margin_bps: u32,
    #[serde(default)]
    pub exposure_limit_cached: i64,
    #[serde(default)]
    pub exposure_limit_cache_valid: bool,
    #[serde(default)]
    pub total_exposure: i64,
    #[serde(default)]
    pub exposure_rejections: u64,
    #[serde(default)]
    pub dedupe: OrdMap<String, AccumulatorDedupeRecord>,
    #[serde(default)]
    pub deltas: OrdMap<u64, AccumulatorDeltaRecord>,
    #[serde(default)]
    pub open_exposures: HashMap<String, OpenExposureRecord>,
    #[serde(default = "default_exposure_rebuild_required")]
    pub exposure_rebuild_required: bool,
}

fn default_exposure_rebuild_required() -> bool {
    true
}

fn default_exposure_margin_bps() -> u32 {
    1_000
}

fn compute_exposure_limit(value: i64, exposure_margin_bps: u32) -> i64 {
    let allowed_ratio_bps = 10_000i128 - exposure_margin_bps as i128;
    ((value as i128 * allowed_ratio_bps) / 10_000).clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Namespace {
    pub id: NamespaceId,
    pub tables: HashMap<String, TableData>,
    pub kv: KvData,
    #[serde(default)]
    pub accumulators: HashMap<String, AccumulatorData>,
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
                accumulators: HashMap::new(),
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

    pub fn accumulator(
        &self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
    ) -> Option<&AccumulatorData> {
        self.namespace(&NamespaceId::project_scope(project_id, scope_id))
            .and_then(|ns| ns.accumulators.get(accumulator_name))
    }

    pub fn accumulator_mut(
        &mut self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
    ) -> &mut AccumulatorData {
        self.namespace_mut(NamespaceId::project_scope(project_id, scope_id))
            .accumulators
            .entry(accumulator_name.to_string())
            .or_default()
    }

    pub fn create_accumulator(
        &mut self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        exposure_margin_bps: u32,
    ) {
        let acc = self.accumulator_mut(project_id, scope_id, accumulator_name);
        acc.exposure_margin_bps = exposure_margin_bps;
        acc.exposure_limit_cached = compute_exposure_limit(acc.value, exposure_margin_bps);
        acc.exposure_limit_cache_valid = true;
    }

    pub fn drop_accumulator(&mut self, project_id: &str, scope_id: &str, accumulator_name: &str) {
        let namespace = self.namespace_mut(NamespaceId::project_scope(project_id, scope_id));
        namespace.accumulators.remove(accumulator_name);
    }

    pub fn append_accumulator_delta(
        &mut self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        delta: i64,
        dedupe_key: &str,
        order_key: u64,
        commit_seq: u64,
    ) -> Result<AccumulatorAppendResult, crate::error::AedbError> {
        let accumulator = self.accumulator_mut(project_id, scope_id, accumulator_name);
        if let Some(existing) = accumulator.dedupe.get(dedupe_key) {
            if existing.order_key == order_key && existing.delta == delta {
                return Ok(AccumulatorAppendResult::DuplicateNoop);
            }
            return Err(crate::error::AedbError::Validation(format!(
                "dedupe key already used with different payload: {dedupe_key}"
            )));
        }
        if order_key <= accumulator.latest_order_key {
            return Err(crate::error::AedbError::Validation(format!(
                "order key must be strictly increasing (latest={}, got={order_key})",
                accumulator.latest_order_key
            )));
        }
        accumulator.latest_order_key = order_key;
        accumulator.latest_seq = commit_seq;
        accumulator.deltas.insert(
            order_key,
            AccumulatorDeltaRecord {
                order_key,
                delta,
                commit_seq,
                dedupe_key: dedupe_key.to_string(),
            },
        );
        accumulator.dedupe.insert(
            dedupe_key.to_string(),
            AccumulatorDedupeRecord {
                order_key,
                delta,
                commit_seq,
            },
        );
        Ok(AccumulatorAppendResult::Applied)
    }

    pub fn expose_accumulator(
        &mut self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        amount: i64,
        exposure_id: &str,
        commit_seq: u64,
    ) -> Result<(), crate::error::AedbError> {
        if amount <= 0 {
            return Err(crate::error::AedbError::Validation(
                "exposure amount must be > 0".into(),
            ));
        }
        let accumulator = self.accumulator_mut(project_id, scope_id, accumulator_name);
        if !accumulator.exposure_limit_cache_valid {
            accumulator.exposure_limit_cached =
                compute_exposure_limit(accumulator.value, accumulator.exposure_margin_bps);
            accumulator.exposure_limit_cache_valid = true;
        }
        if let Some(existing) = accumulator.open_exposures.get(exposure_id) {
            if existing.amount == amount {
                return Ok(());
            }
            return Err(crate::error::AedbError::Validation(format!(
                "exposure id already exists with different amount: {exposure_id}"
            )));
        }
        let Some(new_total) = accumulator.total_exposure.checked_add(amount) else {
            accumulator.exposure_rejections = accumulator.exposure_rejections.saturating_add(1);
            return Err(crate::error::AedbError::Overflow);
        };
        let max_exposure_allowed = accumulator.exposure_limit_cached;
        if new_total > max_exposure_allowed {
            accumulator.exposure_rejections = accumulator.exposure_rejections.saturating_add(1);
            return Err(crate::error::AedbError::Validation(format!(
                "exposure margin exceeded: requested_total={new_total}, allowed={max_exposure_allowed}"
            )));
        }
        accumulator.total_exposure = new_total;
        accumulator.exposure_rebuild_required = false;
        accumulator.open_exposures.insert(
            exposure_id.to_string(),
            OpenExposureRecord {
                amount,
                opened_at_seq: commit_seq,
            },
        );
        Ok(())
    }

    pub fn expose_accumulator_batch(
        &mut self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        exposures: &[(i64, String)],
        commit_seq: u64,
    ) -> Result<(), crate::error::AedbError> {
        let accumulator = self.accumulator_mut(project_id, scope_id, accumulator_name);
        if !accumulator.exposure_limit_cache_valid {
            accumulator.exposure_limit_cached =
                compute_exposure_limit(accumulator.value, accumulator.exposure_margin_bps);
            accumulator.exposure_limit_cache_valid = true;
        }
        let max_exposure_allowed = accumulator.exposure_limit_cached;
        let mut running_total = accumulator.total_exposure;
        let mut pending_inserts: Vec<(String, OpenExposureRecord)> = Vec::new();
        let mut seen_batch: std::collections::HashMap<String, i64> =
            std::collections::HashMap::new();

        for (amount, exposure_id) in exposures {
            if *amount <= 0 {
                return Err(crate::error::AedbError::Validation(
                    "exposure amount must be > 0".into(),
                ));
            }

            if let Some(existing) = accumulator.open_exposures.get(exposure_id.as_str()) {
                if existing.amount != *amount {
                    return Err(crate::error::AedbError::Validation(format!(
                        "exposure id already exists with different amount: {exposure_id}"
                    )));
                }
                continue;
            }

            if let Some(seen_amount) = seen_batch.get(exposure_id.as_str()) {
                if *seen_amount != *amount {
                    return Err(crate::error::AedbError::Validation(format!(
                        "duplicate exposure id with different amount in batch: {exposure_id}"
                    )));
                }
                continue;
            }

            let Some(next_total) = running_total.checked_add(*amount) else {
                accumulator.exposure_rejections = accumulator.exposure_rejections.saturating_add(1);
                return Err(crate::error::AedbError::Overflow);
            };
            if next_total > max_exposure_allowed {
                accumulator.exposure_rejections = accumulator.exposure_rejections.saturating_add(1);
                return Err(crate::error::AedbError::Validation(format!(
                    "exposure margin exceeded: requested_total={next_total}, allowed={max_exposure_allowed}"
                )));
            }
            running_total = next_total;
            seen_batch.insert(exposure_id.clone(), *amount);
            pending_inserts.push((
                exposure_id.clone(),
                OpenExposureRecord {
                    amount: *amount,
                    opened_at_seq: commit_seq,
                },
            ));
        }

        accumulator.total_exposure = running_total;
        accumulator.exposure_rebuild_required = false;
        for (exposure_id, record) in pending_inserts {
            accumulator.open_exposures.insert(exposure_id, record);
        }
        Ok(())
    }

    pub fn release_accumulator_exposure(
        &mut self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        exposure_id: &str,
    ) -> Result<(), crate::error::AedbError> {
        let accumulator = self.accumulator_mut(project_id, scope_id, accumulator_name);
        let Some(record) = accumulator.open_exposures.remove(exposure_id) else {
            return Err(crate::error::AedbError::Validation(format!(
                "release requested for unknown exposure id: {exposure_id}"
            )));
        };
        if accumulator.total_exposure < record.amount {
            return Err(crate::error::AedbError::Underflow);
        }
        accumulator.total_exposure -= record.amount;
        accumulator.exposure_rebuild_required = false;
        Ok(())
    }

    pub fn rebuild_total_exposure(
        &mut self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
    ) -> Result<(), crate::error::AedbError> {
        let accumulator = self.accumulator_mut(project_id, scope_id, accumulator_name);
        let mut total = 0i64;
        for (_, rec) in &accumulator.open_exposures {
            total = total
                .checked_add(rec.amount)
                .ok_or(crate::error::AedbError::Overflow)?;
        }
        accumulator.total_exposure = total;
        accumulator.exposure_rebuild_required = false;
        Ok(())
    }

    pub fn kv_get(&self, project_id: &str, scope_id: &str, key: &[u8]) -> Option<&KvEntry> {
        self.namespace(&NamespaceId::project_scope(project_id, scope_id))
            .and_then(|ns| ns.kv.entries.get(key))
    }

    pub fn counter_read_sharded(
        &self,
        project_id: &str,
        scope_id: &str,
        key: &[u8],
        shard_count: u16,
    ) -> Result<u64, crate::error::AedbError> {
        let mut total = 0u64;
        for shard in 0..shard_count {
            let shard_key = counter_shard_storage_key(key, shard);
            if let Some(entry) = self.kv_get(project_id, scope_id, &shard_key) {
                let value = decode_u64(&entry.value)?;
                total = total
                    .checked_add(value)
                    .ok_or(crate::error::AedbError::Overflow)?;
            }
        }
        Ok(total)
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
        let created_at = match kv.entries.get(&key) {
            Some(entry) => entry.created_at,
            None => {
                kv.structural_version = commit_seq;
                commit_seq
            }
        };
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
        if prefix.is_empty() {
            return kv
                .entries
                .iter()
                .take(limit)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
        }
        let start = Bound::Included(prefix.to_vec());
        let end = prefix_range_end(prefix)
            .map(Bound::Excluded)
            .unwrap_or(Bound::Unbounded);
        kv.entries
            .range((start, end))
            .take(limit)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn kv_scan_prefix_ref<'a>(
        &'a self,
        project_id: &str,
        scope_id: &str,
        prefix: &[u8],
        limit: usize,
    ) -> Vec<(&'a [u8], &'a KvEntry)> {
        let Some(kv) = self
            .namespace(&NamespaceId::project_scope(project_id, scope_id))
            .map(|ns| &ns.kv)
        else {
            return Vec::new();
        };
        if prefix.is_empty() {
            return kv
                .entries
                .iter()
                .take(limit)
                .map(|(k, v)| (k.as_slice(), v))
                .collect();
        }
        let start = Bound::Included(prefix.to_vec());
        let end = prefix_range_end(prefix)
            .map(Bound::Excluded)
            .unwrap_or(Bound::Unbounded);
        kv.entries
            .range((start, end))
            .take(limit)
            .map(|(k, v)| (k.as_slice(), v))
            .collect()
    }

    pub fn kv_visit_prefix_ref<F>(
        &self,
        project_id: &str,
        scope_id: &str,
        prefix: &[u8],
        limit: usize,
        mut visitor: F,
    ) where
        F: FnMut(&[u8], &KvEntry) -> bool,
    {
        let Some(kv) = self
            .namespace(&NamespaceId::project_scope(project_id, scope_id))
            .map(|ns| &ns.kv)
        else {
            return;
        };
        if prefix.is_empty() {
            for (k, v) in kv.entries.iter().take(limit) {
                if !visitor(k.as_slice(), v) {
                    break;
                }
            }
            return;
        }
        let start = Bound::Included(prefix.to_vec());
        let end = prefix_range_end(prefix)
            .map(Bound::Excluded)
            .unwrap_or(Bound::Unbounded);
        for (k, v) in kv.entries.range((start, end)).take(limit) {
            if !visitor(k.as_slice(), v) {
                break;
            }
        }
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

    pub fn kv_add_u256_ex(
        &mut self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount: U256,
        on_missing: &KvU256MissingPolicy,
        on_overflow: &KvU256OverflowPolicy,
        commit_seq: u64,
    ) -> Result<(), crate::error::AedbError> {
        let current = self.kv_get(project_id, scope_id, &key);
        let current_value = match (current, on_missing) {
            (Some(entry), _) => decode_u256(&entry.value)?,
            (None, KvU256MissingPolicy::TreatAsZero) => U256::zero(),
            (None, KvU256MissingPolicy::Reject) => {
                return Err(crate::error::AedbError::Validation(
                    "u256 key missing and policy is Reject".into(),
                ));
            }
        };
        let next = match current_value.checked_add(amount) {
            Some(sum) => sum,
            None => match on_overflow {
                KvU256OverflowPolicy::Reject => return Err(crate::error::AedbError::Overflow),
                KvU256OverflowPolicy::Saturate => U256::MAX,
            },
        };
        self.kv_set(project_id, scope_id, key, encode_u256(next), commit_seq);
        Ok(())
    }

    pub fn kv_sub_u256_ex(
        &mut self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount: U256,
        on_missing: &KvU256MissingPolicy,
        on_underflow: &KvU256UnderflowPolicy,
        commit_seq: u64,
    ) -> Result<(), crate::error::AedbError> {
        let current = self.kv_get(project_id, scope_id, &key);
        let current_value = match (current, on_missing) {
            (Some(entry), _) => decode_u256(&entry.value)?,
            (None, KvU256MissingPolicy::TreatAsZero) => U256::zero(),
            (None, KvU256MissingPolicy::Reject) => {
                return Err(crate::error::AedbError::Validation(
                    "u256 key missing and policy is Reject".into(),
                ));
            }
        };
        if current_value < amount {
            return match on_underflow {
                KvU256UnderflowPolicy::Reject => Err(crate::error::AedbError::Underflow),
                KvU256UnderflowPolicy::NoOp => Ok(()),
            };
        }
        let next = current_value - amount;
        self.kv_set(project_id, scope_id, key, encode_u256(next), commit_seq);
        Ok(())
    }

    pub fn kv_max_u256(
        &mut self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        candidate: U256,
        on_missing: &KvU256MissingPolicy,
        commit_seq: u64,
    ) -> Result<(), crate::error::AedbError> {
        let current = self.kv_get(project_id, scope_id, &key);
        let current_value = match (current, on_missing) {
            (Some(entry), _) => decode_u256(&entry.value)?,
            (None, KvU256MissingPolicy::TreatAsZero) => U256::zero(),
            (None, KvU256MissingPolicy::Reject) => {
                return Err(crate::error::AedbError::Validation(
                    "u256 key missing and policy is Reject".into(),
                ));
            }
        };
        let next = current_value.max(candidate);
        if current.is_some() && next == current_value {
            return Ok(());
        }
        self.kv_set(project_id, scope_id, key, encode_u256(next), commit_seq);
        Ok(())
    }

    pub fn kv_min_u256(
        &mut self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        candidate: U256,
        on_missing: &KvU256MissingPolicy,
        commit_seq: u64,
    ) -> Result<(), crate::error::AedbError> {
        let current = self.kv_get(project_id, scope_id, &key);
        let current_value = match (current, on_missing) {
            (Some(entry), _) => decode_u256(&entry.value)?,
            (None, KvU256MissingPolicy::TreatAsZero) => U256::zero(),
            (None, KvU256MissingPolicy::Reject) => {
                return Err(crate::error::AedbError::Validation(
                    "u256 key missing and policy is Reject".into(),
                ));
            }
        };
        let next = current_value.min(candidate);
        if current.is_some() && next == current_value {
            return Ok(());
        }
        self.kv_set(project_id, scope_id, key, encode_u256(next), commit_seq);
        Ok(())
    }

    pub fn kv_mutate_u256(
        &mut self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        op: KvU256MutatorOp,
        operand: U256,
        commit_seq: u64,
    ) -> Result<(), crate::error::AedbError> {
        let current = self
            .kv_get(project_id, scope_id, &key)
            .map(|e| decode_u256(&e.value))
            .transpose()?
            .unwrap_or(U256::zero());
        let next = match op {
            KvU256MutatorOp::Set => operand,
            KvU256MutatorOp::Add => current
                .checked_add(operand)
                .ok_or(crate::error::AedbError::Overflow)?,
            KvU256MutatorOp::Sub => {
                if current < operand {
                    return Err(crate::error::AedbError::Underflow);
                }
                current - operand
            }
        };
        self.kv_set(project_id, scope_id, key, encode_u256(next), commit_seq);
        Ok(())
    }

    pub fn kv_add_u64_ex(
        &mut self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount: u64,
        on_missing: &KvU64MissingPolicy,
        on_overflow: &KvU64OverflowPolicy,
        commit_seq: u64,
    ) -> Result<(), crate::error::AedbError> {
        let current = self.kv_get(project_id, scope_id, &key);
        let current_value = match (current, on_missing) {
            (Some(entry), _) => decode_u64(&entry.value)?,
            (None, KvU64MissingPolicy::TreatAsZero) => 0u64,
            (None, KvU64MissingPolicy::Reject) => {
                return Err(crate::error::AedbError::Validation(
                    "u64 key missing and policy is Reject".into(),
                ));
            }
        };
        let next = match current_value.checked_add(amount) {
            Some(sum) => sum,
            None => match on_overflow {
                KvU64OverflowPolicy::Reject => return Err(crate::error::AedbError::Overflow),
                KvU64OverflowPolicy::Saturate => u64::MAX,
            },
        };
        self.kv_set(project_id, scope_id, key, encode_u64(next), commit_seq);
        Ok(())
    }

    pub fn kv_mutate_u64(
        &mut self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        op: KvU64MutatorOp,
        operand: u64,
        commit_seq: u64,
    ) -> Result<(), crate::error::AedbError> {
        let current = self
            .kv_get(project_id, scope_id, &key)
            .map(|e| decode_u64(&e.value))
            .transpose()?
            .unwrap_or(0u64);
        let next = match op {
            KvU64MutatorOp::Set => operand,
            KvU64MutatorOp::Add => current
                .checked_add(operand)
                .ok_or(crate::error::AedbError::Overflow)?,
            KvU64MutatorOp::Sub => {
                if current < operand {
                    return Err(crate::error::AedbError::Underflow);
                }
                current - operand
            }
        };
        self.kv_set(project_id, scope_id, key, encode_u64(next), commit_seq);
        Ok(())
    }

    pub fn kv_sub_u64_ex(
        &mut self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount: u64,
        on_missing: &KvU64MissingPolicy,
        on_underflow: &KvU64UnderflowPolicy,
        commit_seq: u64,
    ) -> Result<(), crate::error::AedbError> {
        let current = self.kv_get(project_id, scope_id, &key);
        let current_value = match (current, on_missing) {
            (Some(entry), _) => decode_u64(&entry.value)?,
            (None, KvU64MissingPolicy::TreatAsZero) => 0u64,
            (None, KvU64MissingPolicy::Reject) => {
                return Err(crate::error::AedbError::Validation(
                    "u64 key missing and policy is Reject".into(),
                ));
            }
        };
        if current_value < amount {
            return match on_underflow {
                KvU64UnderflowPolicy::Reject => Err(crate::error::AedbError::Underflow),
                KvU64UnderflowPolicy::NoOp => Ok(()),
            };
        }
        let next = current_value - amount;
        self.kv_set(project_id, scope_id, key, encode_u64(next), commit_seq);
        Ok(())
    }

    pub fn kv_sub_int_ex(
        &mut self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount: KvIntegerAmount,
        on_missing: KvIntegerMissingPolicy,
        on_underflow: KvIntegerUnderflowPolicy,
        commit_seq: u64,
    ) -> Result<(), crate::error::AedbError> {
        match amount {
            KvIntegerAmount::U64(amount_be) => self.kv_sub_u64_ex(
                project_id,
                scope_id,
                key,
                u64::from_be_bytes(amount_be),
                &match on_missing {
                    KvIntegerMissingPolicy::TreatAsZero => KvU64MissingPolicy::TreatAsZero,
                    KvIntegerMissingPolicy::Reject => KvU64MissingPolicy::Reject,
                },
                &match on_underflow {
                    KvIntegerUnderflowPolicy::Reject => KvU64UnderflowPolicy::Reject,
                    KvIntegerUnderflowPolicy::NoOp => KvU64UnderflowPolicy::NoOp,
                },
                commit_seq,
            ),
            KvIntegerAmount::U256(amount_be) => self.kv_sub_u256_ex(
                project_id,
                scope_id,
                key,
                U256::from_big_endian(&amount_be),
                &match on_missing {
                    KvIntegerMissingPolicy::TreatAsZero => KvU256MissingPolicy::TreatAsZero,
                    KvIntegerMissingPolicy::Reject => KvU256MissingPolicy::Reject,
                },
                &match on_underflow {
                    KvIntegerUnderflowPolicy::Reject => KvU256UnderflowPolicy::Reject,
                    KvIntegerUnderflowPolicy::NoOp => KvU256UnderflowPolicy::NoOp,
                },
                commit_seq,
            ),
        }
    }

    pub fn counter_add_sharded(
        &mut self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 8],
        shard_count: u16,
        shard_hint: u32,
        commit_seq: u64,
    ) -> Result<(), crate::error::AedbError> {
        let shard = counter_shard_index(shard_hint, shard_count);
        let physical_key = counter_shard_storage_key(&key, shard);
        self.kv_add_u64_ex(
            project_id,
            scope_id,
            physical_key,
            u64::from_be_bytes(amount_be),
            &KvU64MissingPolicy::TreatAsZero,
            &KvU64OverflowPolicy::Reject,
            commit_seq,
        )
    }

    pub fn kv_max_u64(
        &mut self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        candidate: u64,
        on_missing: &KvU64MissingPolicy,
        commit_seq: u64,
    ) -> Result<(), crate::error::AedbError> {
        let current = self.kv_get(project_id, scope_id, &key);
        let current_value = match (current, on_missing) {
            (Some(entry), _) => decode_u64(&entry.value)?,
            (None, KvU64MissingPolicy::TreatAsZero) => 0u64,
            (None, KvU64MissingPolicy::Reject) => {
                return Err(crate::error::AedbError::Validation(
                    "u64 key missing and policy is Reject".into(),
                ));
            }
        };
        let next = current_value.max(candidate);
        if current.is_some() && next == current_value {
            return Ok(());
        }
        self.kv_set(project_id, scope_id, key, encode_u64(next), commit_seq);
        Ok(())
    }

    pub fn kv_min_u64(
        &mut self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        candidate: u64,
        on_missing: &KvU64MissingPolicy,
        commit_seq: u64,
    ) -> Result<(), crate::error::AedbError> {
        let current = self.kv_get(project_id, scope_id, &key);
        let current_value = match (current, on_missing) {
            (Some(entry), _) => decode_u64(&entry.value)?,
            (None, KvU64MissingPolicy::TreatAsZero) => 0u64,
            (None, KvU64MissingPolicy::Reject) => {
                return Err(crate::error::AedbError::Validation(
                    "u64 key missing and policy is Reject".into(),
                ));
            }
        };
        let next = current_value.min(candidate);
        if current.is_some() && next == current_value {
            return Ok(());
        }
        self.kv_set(project_id, scope_id, key, encode_u64(next), commit_seq);
        Ok(())
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
        let accumulator_bytes = self
            .namespaces
            .values()
            .map(|ns| {
                ns.accumulators
                    .values()
                    .map(|acc| {
                        acc.dedupe.iter().map(|(k, _)| k.len() + 32).sum::<usize>()
                            + acc.deltas.len() * 80
                            + acc
                                .open_exposures
                                .iter()
                                .map(|(k, _)| k.len() + 24)
                                .sum::<usize>()
                    })
                    .sum::<usize>()
            })
            .sum::<usize>();
        row_bytes + kv_bytes + projection_bytes + accumulator_bytes
    }
}

impl KeyspaceSnapshot {
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
        let accumulator_bytes = self
            .namespaces
            .values()
            .map(|ns| {
                ns.accumulators
                    .values()
                    .map(|acc| {
                        acc.dedupe.iter().map(|(k, _)| k.len() + 32).sum::<usize>()
                            + acc.deltas.len() * 80
                            + acc
                                .open_exposures
                                .iter()
                                .map(|(k, _)| k.len() + 24)
                                .sum::<usize>()
                    })
                    .sum::<usize>()
            })
            .sum::<usize>();
        row_bytes + kv_bytes + projection_bytes + accumulator_bytes
    }

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

    pub fn counter_read_sharded(
        &self,
        project_id: &str,
        scope_id: &str,
        key: &[u8],
        shard_count: u16,
    ) -> Result<u64, crate::error::AedbError> {
        let mut total = 0u64;
        for shard in 0..shard_count {
            let shard_key = counter_shard_storage_key(key, shard);
            if let Some(entry) = self.kv_get(project_id, scope_id, &shard_key) {
                let value = decode_u64(&entry.value)?;
                total = total
                    .checked_add(value)
                    .ok_or(crate::error::AedbError::Overflow)?;
            }
        }
        Ok(total)
    }

    pub fn accumulator(
        &self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
    ) -> Option<&AccumulatorData> {
        self.namespaces
            .get(&NamespaceId::project_scope(project_id, scope_id))
            .and_then(|ns| ns.accumulators.get(accumulator_name))
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

fn prefix_range_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for byte_index in (0..end.len()).rev() {
        if end[byte_index] != u8::MAX {
            end[byte_index] = end[byte_index].saturating_add(1);
            end.truncate(byte_index + 1);
            return Some(end);
        }
    }
    None
}

fn encode_u256(v: U256) -> Vec<u8> {
    let mut bytes = [0u8; 32];
    v.to_big_endian(&mut bytes);
    bytes.to_vec()
}

fn encode_u64(v: u64) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}

fn decode_u256(bytes: &[u8]) -> Result<U256, crate::error::AedbError> {
    let value_size_bytes = bytes.len();
    if value_size_bytes != 32 {
        return Err(crate::error::AedbError::Validation(
            "invalid u256 bytes length".into(),
        ));
    }
    Ok(U256::from_big_endian(bytes))
}

fn decode_u64(bytes: &[u8]) -> Result<u64, crate::error::AedbError> {
    if bytes.len() != 8 {
        return Err(crate::error::AedbError::Validation(
            "invalid u64 bytes length".into(),
        ));
    }
    let mut out = [0u8; 8];
    out.copy_from_slice(bytes);
    Ok(u64::from_be_bytes(out))
}

fn estimate_row_bytes(row: &Row) -> usize {
    row.values.iter().map(estimate_value_bytes).sum::<usize>() + 16
}

fn estimate_value_bytes(v: &Value) -> usize {
    match v {
        Value::Text(s) | Value::Json(s) => s.len(),
        Value::U8(_) => 1,
        Value::U64(_) => 8,
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

    #[test]
    fn kv_prefix_scans_are_lexicographically_bounded() {
        let mut ks = Keyspace::default();
        ks.kv_set("p", "app", b"ob:a:1".to_vec(), b"v1".to_vec(), 1);
        ks.kv_set("p", "app", b"ob:a:2".to_vec(), b"v2".to_vec(), 2);
        ks.kv_set("p", "app", b"ob:b:1".to_vec(), b"v3".to_vec(), 3);
        ks.kv_set("p", "app", b"zz".to_vec(), b"v4".to_vec(), 4);

        let rows = ks.kv_scan_prefix("p", "app", b"ob:a:", 10);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, b"ob:a:1".to_vec());
        assert_eq!(rows[1].0, b"ob:a:2".to_vec());

        let refs = ks.kv_scan_prefix_ref("p", "app", b"ob:", 10);
        assert_eq!(refs.len(), 3);
        assert!(refs.iter().all(|(k, _)| k.starts_with(b"ob:")));
    }
}
