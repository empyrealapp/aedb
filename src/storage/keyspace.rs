pub(crate) mod memory_accounting;
pub(crate) use memory_accounting::{
    kv_entry_cost, kv_inline_entry_cost, kv_segment_meta_cost, kv_tombstone_cost,
    namespace_mem_cost, persistent_value_ref_cost, persistent_value_ref_resident_cost,
    projection_data_mem_cost, row_mem_cost, secondary_index_mem_cost, secondary_index_store_cost,
    small_kv_entry_cost, stored_row_mem_cost, table_data_mem_cost,
};

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
use crate::storage::kv_segment::{KvSegmentEntry, KvSegmentMeta, KvSegmentStore};
use crate::storage::value_store::{PersistentValueRef, PersistentValueStore};
use im::{HashMap, OrdMap, OrdSet};
use primitive_types::U256;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
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
    /// Cold tier: postings evicted from `store` to sorted on-disk segments under
    /// memory pressure. Runtime-only — re-inlined into `store` before every
    /// checkpoint (like `TableData::row_segments`), so it never persists and
    /// evicted postings are always recoverable from the checkpoint + WAL. Each
    /// segment entry has a composite key `index_value ‖ primary_key` and carries
    /// the posting's MVCC version for tombstone resolution on read.
    #[serde(skip)]
    pub segments: Vec<KvSegmentMeta>,
    /// Deletes of postings that exist only in `segments`. Maps the composite
    /// `index_value ‖ primary_key` key to the commit seq of the delete.
    /// Runtime-only, alongside `segments`.
    #[serde(skip)]
    pub segment_tombstones: OrdMap<EncodedKey, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TableData {
    pub rows: OrdMap<EncodedKey, StoredRow>,
    /// Legacy parallel map of per-row MVCC versions. New writes carry the
    /// version inline in [`StoredRow::Versioned`] and leave this empty; it is
    /// only populated when loading a pre-inline-version checkpoint, and is
    /// drained as those rows are rewritten or normalized.
    #[serde(default)]
    pub row_versions: OrdMap<EncodedKey, u64>,
    /// Cold tier: whole rows evicted from `rows` to sorted on-disk segments
    /// under memory pressure. Runtime-only — re-inlined into `rows` before every
    /// checkpoint (like KV segments and spilled payloads), so it never persists
    /// and evicted rows are always recoverable from the checkpoint + WAL. Reads
    /// page cold rows back through the segment store.
    #[serde(skip)]
    pub row_segments: Vec<KvSegmentMeta>,
    /// Deletes of keys that exist only in `row_segments`. Maps the deleted key
    /// to the commit seq of the delete. Runtime-only, alongside `row_segments`.
    #[serde(skip)]
    pub row_tombstones: OrdMap<EncodedKey, u64>,
    #[serde(default)]
    pub structural_version: u64,
    pub indexes: HashMap<String, SecondaryIndex>,
}

impl TableData {
    /// The MVCC version (commit seq) of a row: the inline version when present,
    /// otherwise the legacy parallel-map entry. Returns `None` if the key is
    /// absent.
    pub fn version_of(&self, key: &EncodedKey) -> Option<u64> {
        self.rows
            .get(key)
            .and_then(StoredRow::inline_version)
            .or_else(|| self.row_versions.get(key).copied())
    }

    /// Highest MVCC version across all rows (inline or legacy-map). `0` if empty.
    pub fn max_version(&self) -> u64 {
        self.rows
            .iter()
            .map(|(key, stored)| {
                stored
                    .inline_version()
                    .or_else(|| self.row_versions.get(key).copied())
                    .unwrap_or(0)
            })
            .max()
            .unwrap_or(0)
    }
}

/// Convenience: a row's version as a `u64`, defaulting to `0` when absent.
fn table_row_version(table: &TableData, key: &EncodedKey) -> u64 {
    table.version_of(key).unwrap_or(0)
}

/// A table row that is either resident in memory or spilled to the persistent
/// value store. Spilled rows keep only a `PersistentValueRef` (offset/len/hash)
/// in the in-memory keyspace tree; the payload bytes are materialized on demand
/// through the value store's hot cache. This mirrors the inline/spilled model
/// used by `KvEntry` so table-heavy workloads can grow beyond RAM instead of
/// having writes rejected at the memory ceiling.
/// The payload half of a [`StoredRow`]: either resident in memory or spilled to
/// the persistent value store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoredRowPayload {
    Resident(Row),
    Spilled(PersistentValueRef),
}

/// A table row that is either resident in memory or spilled to the persistent
/// value store. Spilled rows keep only a `PersistentValueRef` (offset/len/hash)
/// in the in-memory keyspace tree; the payload bytes are materialized on demand
/// through the value store's hot cache.
///
/// The MVCC version (commit seq) is carried inline by the `Versioned` variant.
/// The bare `Resident`/`Spilled` variants are **legacy**: they were written
/// before the version was inlined, when it lived in a parallel
/// `TableData::row_versions` map. They remain the first two variants so existing
/// checkpoints deserialize unchanged; their version is read from that map until
/// the row is rewritten or normalized into a `Versioned` form. New writes always
/// produce `Versioned`, eliminating the redundant second resident copy of every
/// primary key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoredRow {
    /// Legacy resident row (version in `TableData::row_versions`).
    Resident(Row),
    /// Legacy spilled row (version in `TableData::row_versions`).
    Spilled(PersistentValueRef),
    /// Current form: payload plus its inline MVCC version.
    Versioned {
        version: u64,
        payload: StoredRowPayload,
    },
}

impl Default for StoredRow {
    fn default() -> Self {
        StoredRow::Versioned {
            version: 0,
            payload: StoredRowPayload::Resident(Row { values: Vec::new() }),
        }
    }
}

impl From<Row> for StoredRow {
    fn from(row: Row) -> Self {
        StoredRow::Resident(row)
    }
}

impl StoredRow {
    /// A resident row carrying its inline version.
    pub fn resident_versioned(version: u64, row: Row) -> Self {
        StoredRow::Versioned {
            version,
            payload: StoredRowPayload::Resident(row),
        }
    }

    /// A spilled row carrying its inline version.
    pub fn spilled_versioned(version: u64, value_ref: PersistentValueRef) -> Self {
        StoredRow::Versioned {
            version,
            payload: StoredRowPayload::Spilled(value_ref),
        }
    }

    /// Returns the row payload only if it is currently resident in memory.
    pub fn resident(&self) -> Option<&Row> {
        match self {
            StoredRow::Resident(row)
            | StoredRow::Versioned {
                payload: StoredRowPayload::Resident(row),
                ..
            } => Some(row),
            _ => None,
        }
    }

    pub fn value_ref(&self) -> Option<&PersistentValueRef> {
        match self {
            StoredRow::Spilled(value_ref)
            | StoredRow::Versioned {
                payload: StoredRowPayload::Spilled(value_ref),
                ..
            } => Some(value_ref),
            _ => None,
        }
    }

    pub fn is_spilled(&self) -> bool {
        self.value_ref().is_some()
    }

    /// The inline MVCC version, if this row carries one. Legacy variants return
    /// `None` (their version lives in `TableData::row_versions`).
    pub fn inline_version(&self) -> Option<u64> {
        match self {
            StoredRow::Versioned { version, .. } => Some(*version),
            _ => None,
        }
    }

    /// Rebuild this row in `Versioned` form with the given version, preserving
    /// the payload. Used to normalize legacy rows after a checkpoint load.
    pub fn into_versioned(self, version: u64) -> Self {
        let payload = match self {
            StoredRow::Resident(row) => StoredRowPayload::Resident(row),
            StoredRow::Spilled(value_ref) => StoredRowPayload::Spilled(value_ref),
            StoredRow::Versioned { payload, .. } => payload,
        };
        StoredRow::Versioned { version, payload }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvEntry {
    pub value: Vec<u8>,
    pub version: u64,
    pub created_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_ref: Option<PersistentValueRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InlineKvValue {
    len: u8,
    bytes: [u8; INLINE_KV_VALUE_MAX_BYTES],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmallKvEntry {
    pub value: InlineKvValue,
    pub version: u64,
    pub created_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct KvData {
    pub entries: OrdMap<Vec<u8>, KvEntry>,
    #[serde(default)]
    pub small_entries: OrdMap<CompactKvKey, SmallKvEntry>,
    #[serde(default)]
    pub segment_tombstones: OrdMap<Vec<u8>, u64>,
    #[serde(default)]
    pub segments: Vec<KvSegmentMeta>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Keyspace {
    #[serde(default = "default_primary_index_backend")]
    pub primary_index_backend: PrimaryIndexBackend,
    #[serde(skip)]
    pub value_store: Option<Arc<PersistentValueStore>>,
    #[serde(skip)]
    pub kv_segment_store: Option<Arc<KvSegmentStore>>,
    #[serde(skip, default = "default_persistent_value_inline_threshold_bytes")]
    pub persistent_value_inline_threshold_bytes: usize,
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
    /// Running sum used by `estimate_memory_bytes()`. Maintained incrementally by
    /// every mutation helper. Use `recompute_memory_bytes_full()` to rebuild
    /// after constructing from external data (checkpoint load, partial merges).
    #[serde(default)]
    pub mem_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyspaceSnapshot {
    #[serde(default = "default_primary_index_backend")]
    pub primary_index_backend: PrimaryIndexBackend,
    #[serde(skip)]
    pub value_store: Option<Arc<PersistentValueStore>>,
    #[serde(skip)]
    pub kv_segment_store: Option<Arc<KvSegmentStore>>,
    #[serde(skip, default = "default_persistent_value_inline_threshold_bytes")]
    pub persistent_value_inline_threshold_bytes: usize,
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
    #[serde(default)]
    pub mem_bytes: usize,
}

const INLINE_KV_VALUE_MAX_BYTES: usize = 32;
const INLINE_KV_KEY_MAX_BYTES: usize = 64;

pub type CompactKvKey = SmallVec<[u8; INLINE_KV_KEY_MAX_BYTES]>;

pub(crate) fn compact_kv_key(key: &[u8]) -> CompactKvKey {
    SmallVec::from_slice(key)
}

pub(crate) fn bound_to_compact_key(bound: Bound<Vec<u8>>) -> Bound<CompactKvKey> {
    match bound {
        Bound::Included(key) => Bound::Included(compact_kv_key(&key)),
        Bound::Excluded(key) => Bound::Excluded(compact_kv_key(&key)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl InlineKvValue {
    fn new(value: &[u8]) -> Option<Self> {
        if value.len() > INLINE_KV_VALUE_MAX_BYTES {
            return None;
        }
        let mut bytes = [0u8; INLINE_KV_VALUE_MAX_BYTES];
        bytes[..value.len()].copy_from_slice(value);
        Some(Self {
            len: value.len() as u8,
            bytes,
        })
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }

    pub(crate) fn to_vec(&self) -> Vec<u8> {
        self.as_slice().to_vec()
    }
}

impl KvEntry {
    fn inline(value: Vec<u8>, version: u64, created_at: u64) -> Self {
        Self {
            value,
            version,
            created_at,
            value_ref: None,
        }
    }

    fn spilled(version: u64, created_at: u64, value_ref: PersistentValueRef) -> Self {
        Self {
            value: Vec::new(),
            version,
            created_at,
            value_ref: Some(value_ref),
        }
    }

    pub(crate) fn resident_memory_value_len(&self) -> usize {
        self.value.len().saturating_add(
            self.value_ref
                .as_ref()
                .map(persistent_value_ref_cost)
                .unwrap_or(0),
        )
    }

    pub(crate) fn resident_value_slice(&self) -> Option<&[u8]> {
        if self.value_ref.is_none() {
            Some(self.value.as_slice())
        } else {
            None
        }
    }
}

impl SmallKvEntry {
    fn new(value: &[u8], version: u64, created_at: u64) -> Option<Self> {
        Some(Self {
            value: InlineKvValue::new(value)?,
            version,
            created_at,
        })
    }

    pub(crate) fn materialize(&self) -> KvEntry {
        KvEntry {
            value: self.value.to_vec(),
            version: self.version,
            created_at: self.created_at,
            value_ref: None,
        }
    }

    pub(crate) fn resident_value_len(&self) -> usize {
        self.value.len as usize
    }
}

fn existing_kv_created_at_and_cost(kv: &KvData, key: &[u8]) -> Option<(u64, usize)> {
    kv.small_entries
        .get(&compact_kv_key(key))
        .map(|entry| {
            (
                entry.created_at,
                small_kv_entry_cost(key.len(), entry.resident_value_len()),
            )
        })
        .or_else(|| {
            kv.entries.get(key).map(|entry| {
                (
                    entry.created_at,
                    kv_entry_cost(key.len(), entry.resident_memory_value_len()),
                )
            })
        })
        .or_else(|| {
            kv.segment_tombstones
                .get(key)
                .map(|seq| (*seq, kv_tombstone_cost(key.len())))
        })
}

fn remove_replaced_segment_tombstone_cost(kv: &mut KvData, key: &[u8]) -> usize {
    let tombstone_cost_already_counted =
        !kv.small_entries.contains_key(&compact_kv_key(key)) && !kv.entries.contains_key(key);
    if kv.segment_tombstones.remove(key).is_some() && !tombstone_cost_already_counted {
        kv_tombstone_cost(key.len())
    } else {
        0
    }
}

fn apply_inline_kv_batch_entry(
    kv: &mut KvData,
    key: &Vec<u8>,
    value: &[u8],
    commit_seq: u64,
) -> (usize, usize) {
    let compact_key = compact_kv_key(key);
    let old_small = kv.small_entries.get(&compact_key).map(|entry| {
        (
            entry.created_at,
            small_kv_entry_cost(key.len(), entry.resident_value_len()),
        )
    });
    let old_normal = old_small
        .is_none()
        .then(|| {
            kv.entries.get(key).map(|entry| {
                (
                    entry.created_at,
                    kv_entry_cost(key.len(), entry.resident_memory_value_len()),
                )
            })
        })
        .flatten();
    let tombstone_seq = kv.segment_tombstones.get(key).copied();
    let (created_at, old_cost) = old_small.or(old_normal).unwrap_or_else(|| {
        tombstone_seq
            .map(|seq| (seq, kv_tombstone_cost(key.len())))
            .unwrap_or_else(|| {
                kv.structural_version = commit_seq;
                (commit_seq, 0)
            })
    });
    let old_has_hot_entry = old_small.is_some() || old_normal.is_some();
    let old_tombstone_cost = if old_has_hot_entry && tombstone_seq.is_some() {
        kv_tombstone_cost(key.len())
    } else {
        0
    };

    let new_cost = if let Some(entry) = SmallKvEntry::new(value, commit_seq, created_at) {
        let cost = small_kv_entry_cost(key.len(), entry.resident_value_len());
        if old_normal.is_some() {
            kv.entries.remove(key);
        }
        if tombstone_seq.is_some() {
            kv.segment_tombstones.remove(key);
        }
        kv.small_entries.insert(compact_key, entry);
        cost
    } else {
        let entry = KvEntry::inline(value.to_vec(), commit_seq, created_at);
        let cost = kv_entry_cost(key.len(), entry.resident_memory_value_len());
        if old_small.is_some() {
            kv.small_entries.remove(&compact_key);
        }
        if tombstone_seq.is_some() {
            kv.segment_tombstones.remove(key);
        }
        kv.entries.insert(key.clone(), entry);
        cost
    };

    (new_cost, old_cost.saturating_add(old_tombstone_cost))
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

fn materialize_kv_entry(
    entry: &KvEntry,
    value_store: Option<&PersistentValueStore>,
) -> Result<KvEntry, crate::error::AedbError> {
    if let Some(value_ref) = &entry.value_ref {
        let store = value_store.ok_or_else(|| crate::error::AedbError::Unavailable {
            message: "persistent value store is not attached".into(),
        })?;
        let mut out = entry.clone();
        out.value = store.read(value_ref)?;
        out.value_ref = None;
        Ok(out)
    } else {
        Ok(entry.clone())
    }
}

/// Serializes a table row payload for storage in the persistent value store.
pub(crate) fn encode_row_payload(row: &Row) -> Result<Vec<u8>, crate::error::AedbError> {
    rmp_serde::to_vec(row).map_err(|e| crate::error::AedbError::Encode(e.to_string()))
}

/// Deserializes a table row payload read back from the persistent value store.
fn decode_row_payload(bytes: &[u8]) -> Result<Row, crate::error::AedbError> {
    rmp_serde::from_slice(bytes).map_err(|e| crate::error::AedbError::Decode(e.to_string()))
}

/// Materializes a stored row, reading spilled payloads through the value store
/// (which serves resident bytes from its hot cache and pages cold bytes in from
/// the mmap-backed file). Resident rows are returned as a borrow with no copy.
fn materialize_row<'a>(
    stored: &'a StoredRow,
    value_store: Option<&PersistentValueStore>,
) -> Result<std::borrow::Cow<'a, Row>, crate::error::AedbError> {
    if let Some(row) = stored.resident() {
        Ok(std::borrow::Cow::Borrowed(row))
    } else if let Some(value_ref) = stored.value_ref() {
        let store = value_store.ok_or_else(|| crate::error::AedbError::Unavailable {
            message: "persistent value store is not attached".into(),
        })?;
        let bytes = store.read(value_ref)?;
        Ok(std::borrow::Cow::Owned(decode_row_payload(&bytes)?))
    } else {
        // Unreachable: every StoredRow is either resident or spilled.
        Ok(std::borrow::Cow::Owned(Row { values: Vec::new() }))
    }
}

fn segment_may_contain_key(meta: &KvSegmentMeta, key: &[u8]) -> bool {
    key >= meta.min_key.as_slice() && key <= meta.max_key.as_slice()
}

/// Encode one evicted table row as a [`KvSegmentEntry`] for the cold tier: the
/// key is the encoded primary key bytes, the value is the serialized row
/// payload, and the version is carried on the entry.
fn encode_row_segment_entry(
    encoded_pk: &EncodedKey,
    version: u64,
    row: &Row,
) -> Result<KvSegmentEntry, crate::error::AedbError> {
    Ok(KvSegmentEntry {
        key: encoded_pk.as_slice().to_vec(),
        entry: KvEntry {
            value: encode_row_payload(row)?,
            version,
            created_at: version,
            value_ref: None,
        },
    })
}

/// Composite segment key for a secondary-index posting: the index value bytes
/// followed by the primary key bytes. Because `EncodedKey` values are
/// self-delimiting and prefix-free (fixed-width tags or NUL-terminated text), no
/// complete index value is a prefix of a different one, so this concatenation
/// sorts first by index value and then by primary key, and all postings for a
/// given index value `V` occupy the contiguous prefix range
/// `[V, prefix_successor(V))`.
pub(crate) fn index_segment_composite_key(index_value: &EncodedKey, pk: &EncodedKey) -> Vec<u8> {
    let mut out = Vec::with_capacity(index_value.as_slice().len() + pk.as_slice().len());
    out.extend_from_slice(index_value.as_slice());
    out.extend_from_slice(pk.as_slice());
    out
}

/// Encode one evicted index posting as a [`KvSegmentEntry`]. The key is the
/// composite `index_value ‖ pk`, the value carries the primary-key bytes (so the
/// posting can be reconstructed on read-back without parsing the composite), and
/// the version is the MVCC seq used for tombstone resolution.
fn encode_index_segment_entry(
    index_value: &EncodedKey,
    pk: &EncodedKey,
    version: u64,
) -> KvSegmentEntry {
    KvSegmentEntry {
        key: index_segment_composite_key(index_value, pk),
        entry: KvEntry {
            value: pk.as_slice().to_vec(),
            version,
            created_at: version,
            value_ref: None,
        },
    }
}

/// Split a composite index-segment key back into `(index_value, pk)` given the
/// primary-key bytes recovered from the entry value.
pub(crate) fn decode_index_segment_entry(
    composite: &[u8],
    pk_bytes: &[u8],
) -> (EncodedKey, EncodedKey) {
    let split = composite.len().saturating_sub(pk_bytes.len());
    let index_value = EncodedKey::from_bytes(composite[..split].to_vec());
    let pk = EncodedKey::from_bytes(pk_bytes.to_vec());
    (index_value, pk)
}

/// Re-inline a secondary index's cold-tier segments back into its resident
/// store, then clear the cold tier. Merge rules mirror the row tier: among
/// segment copies of the same posting the highest version wins; a tombstone
/// with `seq >= version` suppresses the posting; and a posting already resident
/// (re-inserted after eviction) always wins. After this the index is fully
/// resident with empty `segments`/`segment_tombstones`, so the checkpoint is
/// self-contained and never persists the runtime-only cold tier.
fn reinline_index_segments(
    index: &mut SecondaryIndex,
    store: &KvSegmentStore,
) -> Result<(), crate::error::AedbError> {
    if index.segments.is_empty() {
        index.segment_tombstones = OrdMap::new();
        return Ok(());
    }
    // Postings already resident win and must not be overwritten by stale segment
    // copies.
    let hot: HashSet<(Vec<u8>, Vec<u8>)> = index
        .resident_postings()
        .into_iter()
        .map(|(value, pk)| (value.as_slice().to_vec(), pk.as_slice().to_vec()))
        .collect();
    // Highest-versioned live segment copy of each composite key.
    let mut best: std::collections::BTreeMap<Vec<u8>, (u64, EncodedKey, EncodedKey)> =
        std::collections::BTreeMap::new();
    for segment in &index.segments {
        for item in store.scan_range(segment, &Bound::Unbounded, &Bound::Unbounded)? {
            let version = item.entry.version;
            if best
                .get(&item.key)
                .map(|(v, _, _)| version > *v)
                .unwrap_or(true)
            {
                let (value, pk) = decode_index_segment_entry(&item.key, &item.entry.value);
                best.insert(item.key.clone(), (version, value, pk));
            }
        }
    }
    for (composite, (version, value, pk)) in best {
        let composite_key = EncodedKey::from_bytes(composite);
        if let Some(tomb) = index.segment_tombstones.get(&composite_key)
            && *tomb >= version
        {
            continue; // deleted after this segment was written
        }
        if hot.contains(&(value.as_slice().to_vec(), pk.as_slice().to_vec())) {
            continue; // resident copy already current
        }
        index.insert(value, pk);
    }
    index.segments.clear();
    index.segment_tombstones = OrdMap::new();
    Ok(())
}

fn encoded_key_bound_to_bytes(bound: &Bound<EncodedKey>) -> Bound<Vec<u8>> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(k) => Bound::Included(k.as_slice().to_vec()),
        Bound::Excluded(k) => Bound::Excluded(k.as_slice().to_vec()),
    }
}

/// Tier-aware point read: resolve a single row by primary key, returning its
/// version and materialized payload. Checks the hot `rows` map first, then the
/// cold tier (tombstones, then row segments by highest version). `None` means
/// the row does not exist.
fn tier_get_table_row(
    table: &TableData,
    encoded_pk: &EncodedKey,
    value_store: Option<&PersistentValueStore>,
    segment_store: Option<&KvSegmentStore>,
) -> Result<Option<(u64, Row)>, crate::error::AedbError> {
    if let Some(stored) = table.rows.get(encoded_pk) {
        let version = stored
            .inline_version()
            .or_else(|| table.row_versions.get(encoded_pk).copied())
            .unwrap_or(0);
        return Ok(Some((
            version,
            materialize_row(stored, value_store)?.into_owned(),
        )));
    }
    if table.row_segments.is_empty() {
        return Ok(None);
    }
    let key_bytes = encoded_pk.as_slice();
    let tombstone_version = table.row_tombstones.get(encoded_pk).copied();
    let store = segment_store.ok_or_else(|| crate::error::AedbError::Unavailable {
        message: "row segment store is not attached".into(),
    })?;
    let start = Bound::Included(key_bytes.to_vec());
    let end = Bound::Included(key_bytes.to_vec());
    let mut best: Option<(u64, Row)> = None;
    for segment in &table.row_segments {
        if !segment_may_contain_key(segment, key_bytes) {
            continue;
        }
        for item in store.scan_range(segment, &start, &end)? {
            if item.key.as_slice() != key_bytes {
                continue;
            }
            let version = item.entry.version;
            if best.as_ref().map(|(v, _)| version > *v).unwrap_or(true) {
                best = Some((version, decode_row_payload(&item.entry.value)?));
            }
        }
    }
    match (best, tombstone_version) {
        (Some((version, _)), Some(tomb)) if tomb >= version => Ok(None),
        (Some((version, row)), _) => Ok(Some((version, row))),
        (None, _) => Ok(None),
    }
}

/// Tier-aware range scan: merge the hot `rows` map with the cold row segments
/// (minus tombstones), newest version winning, returning rows in key order up to
/// `limit`.
fn tier_scan_table_rows(
    table: &TableData,
    start: Bound<EncodedKey>,
    end: Bound<EncodedKey>,
    limit: usize,
    value_store: Option<&PersistentValueStore>,
    segment_store: Option<&KvSegmentStore>,
) -> Result<Vec<(EncodedKey, Row)>, crate::error::AedbError> {
    // Fast path: nothing cold — just the hot map.
    if table.row_segments.is_empty() && table.row_tombstones.is_empty() {
        let mut out = Vec::new();
        for (key, stored) in table.rows.range((start, end)) {
            if out.len() >= limit {
                break;
            }
            out.push((
                key.clone(),
                materialize_row(stored, value_store)?.into_owned(),
            ));
        }
        return Ok(out);
    }

    // General merge. `None` payload marks a tombstoned key.
    let mut merged: std::collections::BTreeMap<EncodedKey, (u64, Option<Row>)> =
        std::collections::BTreeMap::new();
    let byte_start = encoded_key_bound_to_bytes(&start);
    let byte_end = encoded_key_bound_to_bytes(&end);
    if !table.row_segments.is_empty() {
        let store = segment_store.ok_or_else(|| crate::error::AedbError::Unavailable {
            message: "row segment store is not attached".into(),
        })?;
        for segment in &table.row_segments {
            for item in store.scan_range(segment, &byte_start, &byte_end)? {
                let key = EncodedKey::from_bytes(item.key);
                let version = item.entry.version;
                if merged.get(&key).map(|(v, _)| version > *v).unwrap_or(true) {
                    merged.insert(key, (version, Some(decode_row_payload(&item.entry.value)?)));
                }
            }
        }
    }
    for (key, tomb_version) in table.row_tombstones.range((start.clone(), end.clone())) {
        if merged
            .get(key)
            .map(|(v, _)| *tomb_version >= *v)
            .unwrap_or(true)
        {
            merged.insert(key.clone(), (*tomb_version, None));
        }
    }
    // Hot rows always win — they are the current committed values.
    for (key, stored) in table.rows.range((start, end)) {
        let version = stored
            .inline_version()
            .or_else(|| table.row_versions.get(key).copied())
            .unwrap_or(0);
        merged.insert(
            key.clone(),
            (
                version,
                Some(materialize_row(stored, value_store)?.into_owned()),
            ),
        );
    }

    let mut out = Vec::new();
    for (key, (_, payload)) in merged {
        if out.len() >= limit {
            break;
        }
        if let Some(row) = payload {
            out.push((key, row));
        }
    }
    Ok(out)
}

fn segments_are_sorted_non_overlapping(segments: &[KvSegmentMeta]) -> bool {
    segments
        .windows(2)
        .all(|pair| pair[0].min_key <= pair[1].min_key && pair[0].max_key < pair[1].min_key)
}

fn get_sorted_segment_for_key<'a>(
    segments: &'a [KvSegmentMeta],
    key: &[u8],
) -> Option<&'a KvSegmentMeta> {
    let segment_position = match segments.binary_search_by(|segment| {
        if key < segment.min_key.as_slice() {
            std::cmp::Ordering::Greater
        } else if key > segment.max_key.as_slice() {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Equal
        }
    }) {
        Ok(position) => position,
        Err(_) => return None,
    };
    segments.get(segment_position)
}

fn first_segment_position_for_start(segments: &[KvSegmentMeta], start: &Bound<Vec<u8>>) -> usize {
    match start {
        Bound::Unbounded => 0,
        Bound::Included(start) => match segments
            .binary_search_by(|segment| segment.max_key.as_slice().cmp(start.as_slice()))
        {
            Ok(position) | Err(position) => position,
        },
        Bound::Excluded(start) => match segments
            .binary_search_by(|segment| segment.max_key.as_slice().cmp(start.as_slice()))
        {
            Ok(position) => position.saturating_add(1),
            Err(position) => position,
        },
    }
}

fn segment_starts_after_end(segment: &KvSegmentMeta, end: &Bound<Vec<u8>>) -> bool {
    match end {
        Bound::Unbounded => false,
        Bound::Included(end) => segment.min_key.as_slice() > end.as_slice(),
        Bound::Excluded(end) => segment.min_key.as_slice() >= end.as_slice(),
    }
}

fn sorted_non_overlapping_segments(segments: &[KvSegmentMeta]) -> Option<Vec<&KvSegmentMeta>> {
    let mut sorted = segments.iter().collect::<Vec<_>>();
    sorted.sort_by(|left, right| {
        left.min_key
            .cmp(&right.min_key)
            .then_with(|| left.max_key.cmp(&right.max_key))
            .then_with(|| left.created_at_micros.cmp(&right.created_at_micros))
            .then_with(|| left.filename.cmp(&right.filename))
    });
    for pair in sorted.windows(2) {
        if pair[0].max_key >= pair[1].min_key {
            return None;
        }
    }
    Some(sorted)
}

fn get_segment_entry(
    segments: &[KvSegmentMeta],
    key: &[u8],
    store: &KvSegmentStore,
) -> Result<Option<KvEntry>, crate::error::AedbError> {
    if segments_are_sorted_non_overlapping(segments) {
        let Some(segment) = get_sorted_segment_for_key(segments, key) else {
            return Ok(None);
        };
        return store.get(segment, key);
    }

    if let Some(sorted_segments) = sorted_non_overlapping_segments(segments) {
        let segment_position = match sorted_segments.binary_search_by(|segment| {
            if key < segment.min_key.as_slice() {
                std::cmp::Ordering::Greater
            } else if key > segment.max_key.as_slice() {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        }) {
            Ok(position) => position,
            Err(_) => return Ok(None),
        };
        return store.get(sorted_segments[segment_position], key);
    }

    for segment in segments.iter().rev() {
        if let Some(entry) = store.get(segment, key)? {
            return Ok(Some(entry));
        }
    }
    Ok(None)
}

fn scan_kv_entries(
    kv: &KvData,
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
    limit: usize,
    value_store: Option<&PersistentValueStore>,
    segment_store: Option<&KvSegmentStore>,
    populate_segment_cache: bool,
) -> Result<Vec<(Vec<u8>, KvEntry)>, crate::error::AedbError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    if !kv.segments.is_empty() {
        let store = segment_store.ok_or_else(|| crate::error::AedbError::Unavailable {
            message: "KV segment store is not attached".into(),
        })?;
        let scan_segment_limited =
            |segment: &KvSegmentMeta,
             remaining: usize|
             -> Result<Vec<KvSegmentEntry>, crate::error::AedbError> {
                if populate_segment_cache {
                    store.scan_range_limited(segment, &start, &end, remaining)
                } else {
                    store.scan_range_limited_cold(segment, &start, &end, remaining)
                }
            };
        if kv.segments.len() == 1
            && kv.segment_tombstones.is_empty()
            && kv.small_entries.is_empty()
            && kv.entries.is_empty()
        {
            let segment_entries = scan_segment_limited(&kv.segments[0], limit)?;
            return segment_entries
                .into_iter()
                .map(|item| {
                    materialize_kv_entry(&item.entry, value_store).map(|entry| (item.key, entry))
                })
                .collect();
        }
        if kv.segment_tombstones.is_empty()
            && kv.small_entries.is_empty()
            && kv.entries.is_empty()
            && segments_are_sorted_non_overlapping(&kv.segments)
        {
            let mut out = Vec::with_capacity(limit.min(64));
            let first_segment_position = first_segment_position_for_start(&kv.segments, &start);
            for segment in &kv.segments[first_segment_position..] {
                if segment_starts_after_end(segment, &end) {
                    break;
                }
                let remaining = limit.saturating_sub(out.len());
                if remaining == 0 {
                    break;
                }
                for item in scan_segment_limited(segment, remaining)? {
                    out.push((item.key, materialize_kv_entry(&item.entry, value_store)?));
                }
            }
            return Ok(out);
        }
        if kv.segment_tombstones.is_empty()
            && kv.small_entries.is_empty()
            && kv.entries.is_empty()
            && let Some(sorted_segments) = sorted_non_overlapping_segments(&kv.segments)
        {
            let mut out = Vec::with_capacity(limit.min(64));
            for segment in sorted_segments {
                if segment_starts_after_end(segment, &end) {
                    break;
                }
                let remaining = limit.saturating_sub(out.len());
                if remaining == 0 {
                    break;
                }
                for item in scan_segment_limited(segment, remaining)? {
                    out.push((item.key, materialize_kv_entry(&item.entry, value_store)?));
                }
            }
            return Ok(out);
        }
        let mut merged = std::collections::BTreeMap::<Vec<u8>, KvEntry>::new();
        for segment in &kv.segments {
            let segment_entries = if populate_segment_cache {
                store.scan_range(segment, &start, &end)?
            } else {
                store.scan_range_cold(segment, &start, &end)?
            };
            for item in segment_entries {
                merged.insert(item.key, materialize_kv_entry(&item.entry, value_store)?);
            }
        }
        for (key, _) in kv.segment_tombstones.range((start.clone(), end.clone())) {
            merged.remove(key);
        }
        for (key, entry) in kv.small_entries.range((
            bound_to_compact_key(start.clone()),
            bound_to_compact_key(end.clone()),
        )) {
            merged.insert(key.as_slice().to_vec(), entry.materialize());
        }
        for (key, entry) in kv.entries.range((start, end)) {
            merged.insert(key.clone(), materialize_kv_entry(entry, value_store)?);
        }
        return Ok(merged.into_iter().take(limit).collect());
    }

    if kv.small_entries.is_empty() {
        let mut out = Vec::with_capacity(limit.min(64));
        for (key, entry) in kv.entries.range((start, end)).take(limit) {
            out.push((key.clone(), materialize_kv_entry(entry, value_store)?));
        }
        return Ok(out);
    }
    if kv.entries.is_empty() {
        let small_start = bound_to_compact_key(start);
        let small_end = bound_to_compact_key(end);
        let out = kv
            .small_entries
            .range((small_start, small_end))
            .take(limit)
            .map(|(key, entry)| (key.as_slice().to_vec(), entry.materialize()))
            .collect();
        return Ok(out);
    }

    let small_start = bound_to_compact_key(start.clone());
    let small_end = bound_to_compact_key(end.clone());
    let mut small = kv.small_entries.range((small_start, small_end)).peekable();
    let mut normal = kv.entries.range((start, end)).peekable();
    let mut out = Vec::with_capacity(limit.min(64));

    while out.len() < limit {
        let take_small = match (small.peek(), normal.peek()) {
            (Some((small_key, _)), Some((normal_key, _))) => {
                Some(small_key.as_slice() <= normal_key.as_slice())
            }
            (Some(_), None) => Some(true),
            (None, Some(_)) => Some(false),
            (None, None) => None,
        };
        match take_small {
            Some(true) => {
                let duplicate_normal = match (small.peek(), normal.peek()) {
                    (Some((small_key, _)), Some((normal_key, _))) => {
                        small_key.as_slice() == normal_key.as_slice()
                    }
                    _ => false,
                };
                let (key, entry) = small.next().expect("peeked small entry");
                if duplicate_normal {
                    normal.next();
                }
                out.push((key.as_slice().to_vec(), entry.materialize()));
            }
            Some(false) => {
                let (key, entry) = normal.next().expect("peeked normal entry");
                out.push((key.clone(), materialize_kv_entry(entry, value_store)?));
            }
            None => break,
        }
    }

    Ok(out)
}

fn collect_hot_kv_segment_entries(kv: &KvData) -> (Vec<KvSegmentEntry>, usize) {
    let mut small = kv.small_entries.iter().peekable();
    let mut normal = kv.entries.iter().peekable();
    let mut entries = Vec::with_capacity(kv.small_entries.len().saturating_add(kv.entries.len()));
    let mut resident_cost = 0usize;

    while small.peek().is_some() || normal.peek().is_some() {
        let take_small = match (small.peek(), normal.peek()) {
            (Some((small_key, _)), Some((normal_key, _))) => {
                small_key.as_slice() <= normal_key.as_slice()
            }
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => false,
        };
        if take_small {
            let duplicate_normal = match (small.peek(), normal.peek()) {
                (Some((small_key, _)), Some((normal_key, _))) => {
                    small_key.as_slice() == normal_key.as_slice()
                }
                _ => false,
            };
            let (key, entry) = small.next().expect("peeked small entry");
            resident_cost = resident_cost
                .saturating_add(small_kv_entry_cost(key.len(), entry.resident_value_len()));
            if duplicate_normal && let Some((normal_key, normal_entry)) = normal.next() {
                resident_cost = resident_cost.saturating_add(kv_entry_cost(
                    normal_key.len(),
                    normal_entry.resident_memory_value_len(),
                ));
            }
            entries.push(KvSegmentEntry {
                key: key.as_slice().to_vec(),
                entry: entry.materialize(),
            });
        } else {
            let (key, entry) = normal.next().expect("peeked normal entry");
            resident_cost = resident_cost
                .saturating_add(kv_entry_cost(key.len(), entry.resident_memory_value_len()));
            entries.push(KvSegmentEntry {
                key: key.clone(),
                entry: entry.clone(),
            });
        }
    }

    (entries, resident_cost)
}

fn hot_kv_resident_cost(kv: &KvData) -> usize {
    let normal = kv
        .entries
        .iter()
        .map(|(key, entry)| kv_entry_cost(key.len(), entry.resident_memory_value_len()))
        .sum::<usize>();
    let small = kv
        .small_entries
        .iter()
        .map(|(key, entry)| small_kv_entry_cost(key.len(), entry.resident_value_len()))
        .sum::<usize>();
    normal.saturating_add(small)
}

fn maybe_spill_kv_entry(
    value_store: Option<&Arc<PersistentValueStore>>,
    inline_threshold_bytes: usize,
    value: Vec<u8>,
    version: u64,
    created_at: u64,
) -> Result<KvEntry, crate::error::AedbError> {
    if let Some(store) = value_store
        && value.len() > inline_threshold_bytes
    {
        let value_ref = store.append(&value)?;
        return Ok(KvEntry::spilled(version, created_at, value_ref));
    }
    Ok(KvEntry::inline(value, version, created_at))
}

impl Default for Keyspace {
    fn default() -> Self {
        Self::with_backend(default_primary_index_backend())
    }
}

impl Keyspace {
    pub fn with_backend(primary_index_backend: PrimaryIndexBackend) -> Self {
        Self {
            primary_index_backend,
            value_store: None,
            kv_segment_store: None,
            persistent_value_inline_threshold_bytes: usize::MAX,
            namespaces: Arc::new(HashMap::new()),
            async_indexes: Arc::new(HashMap::new()),
            mem_bytes: 0,
        }
    }

    pub fn attach_persistent_value_store(
        &mut self,
        store: Arc<PersistentValueStore>,
        inline_threshold_bytes: usize,
    ) -> Result<(), crate::error::AedbError> {
        self.set_persistent_value_store(store, inline_threshold_bytes);
        self.refresh_mem_bytes();
        self.spill_kv_values()?;
        self.refresh_mem_bytes();
        Ok(())
    }

    pub fn attach_kv_segment_store(&mut self, store: Arc<KvSegmentStore>) {
        self.kv_segment_store = Some(store);
    }

    pub(crate) fn set_persistent_value_store(
        &mut self,
        store: Arc<PersistentValueStore>,
        inline_threshold_bytes: usize,
    ) {
        self.value_store = Some(store);
        self.persistent_value_inline_threshold_bytes = inline_threshold_bytes;
    }

    pub fn detach_persistent_value_store(&mut self) {
        self.value_store = None;
        self.kv_segment_store = None;
        self.persistent_value_inline_threshold_bytes = usize::MAX;
    }

    pub fn kv_segment_filenames(&self) -> HashSet<String> {
        collect_kv_segment_filenames(self.namespaces.values())
    }

    pub fn sync_persistent_value_store(&self) -> Result<(), crate::error::AedbError> {
        if let Some(store) = &self.value_store {
            store.sync_all()?;
        }
        Ok(())
    }

    pub fn spill_kv_values(&mut self) -> Result<(), crate::error::AedbError> {
        let Some(store) = self.value_store.clone() else {
            return Ok(());
        };
        let threshold = self.persistent_value_inline_threshold_bytes;
        let mut plans = Vec::new();
        let value_refs = {
            let mut values = Vec::new();
            for (namespace_id, namespace) in self.namespaces.iter() {
                for (key, entry) in namespace.kv.small_entries.iter() {
                    let value = entry.value.as_slice();
                    if value.len() > threshold {
                        plans.push((namespace_id.clone(), key.as_slice().to_vec(), value.len()));
                        values.push(value);
                    }
                }
                for (key, entry) in namespace.kv.entries.iter() {
                    if let Some(value) = entry.resident_value_slice()
                        && value.len() > threshold
                    {
                        plans.push((namespace_id.clone(), key.clone(), value.len()));
                        values.push(value);
                    }
                }
            }
            store.append_many_cold_slices(&values)?
        };
        self.apply_spill_plans(plans, value_refs);
        Ok(())
    }

    pub fn spill_kv_values_to_memory_target(
        &mut self,
        target_bytes: usize,
    ) -> Result<usize, crate::error::AedbError> {
        let mut memory_estimate = self.estimate_memory_bytes();
        if memory_estimate <= target_bytes {
            return Ok(memory_estimate);
        }

        let Some(store) = self.value_store.clone() else {
            return Ok(memory_estimate);
        };

        let mut candidates = Vec::new();
        let mut heap_entries = Vec::new();
        for (namespace_id, namespace) in self.namespaces.iter() {
            for (key, entry) in namespace.kv.small_entries.iter() {
                let value = entry.value.as_slice();
                if !value.is_empty() {
                    let payload_bytes = value.len();
                    let old_cost = small_kv_entry_cost(key.len(), payload_bytes);
                    let new_cost = kv_entry_cost(key.len(), persistent_value_ref_resident_cost());
                    if old_cost > new_cost {
                        let memory_reduction_bytes = old_cost - new_cost;
                        let candidate_index = candidates.len();
                        candidates.push((
                            namespace_id.clone(),
                            key.as_slice().to_vec(),
                            memory_reduction_bytes,
                        ));
                        heap_entries.push(Reverse((
                            entry.version,
                            memory_reduction_bytes,
                            candidate_index,
                        )));
                    }
                }
            }
            for (key, entry) in namespace.kv.entries.iter() {
                if let Some(value) = entry.resident_value_slice()
                    && !value.is_empty()
                {
                    let payload_bytes = value.len();
                    let old_cost = kv_entry_cost(key.len(), payload_bytes);
                    let new_cost = kv_entry_cost(key.len(), persistent_value_ref_resident_cost());
                    if old_cost > new_cost {
                        let memory_reduction_bytes = old_cost - new_cost;
                        let candidate_index = candidates.len();
                        candidates.push((
                            namespace_id.clone(),
                            key.clone(),
                            memory_reduction_bytes,
                        ));
                        heap_entries.push(Reverse((
                            entry.version,
                            memory_reduction_bytes,
                            candidate_index,
                        )));
                    }
                }
            }
        }
        let mut oldest_first = BinaryHeap::from(heap_entries);

        let mut plans = Vec::new();
        let value_refs = {
            let mut values = Vec::new();
            while memory_estimate > target_bytes {
                let Some(Reverse((_, _, candidate_index))) = oldest_first.pop() else {
                    break;
                };
                let Some((namespace_id, key, memory_reduction_bytes)) =
                    candidates.get(candidate_index)
                else {
                    continue;
                };
                let Some(value) = self.namespaces.get(namespace_id).and_then(|namespace| {
                    namespace
                        .kv
                        .small_entries
                        .get(&compact_kv_key(key))
                        .map(|entry| entry.value.as_slice())
                        .or_else(|| {
                            namespace
                                .kv
                                .entries
                                .get(key)
                                .and_then(KvEntry::resident_value_slice)
                        })
                }) else {
                    continue;
                };
                if value.is_empty() {
                    continue;
                }
                plans.push((namespace_id.clone(), key.clone(), value.len()));
                values.push(value);
                memory_estimate = memory_estimate.saturating_sub(*memory_reduction_bytes);
            }

            store.append_many_cold_slices(&values)?
        };
        self.apply_spill_plans(plans, value_refs);

        Ok(self.estimate_memory_bytes())
    }

    /// Spills resident table-row payloads to the persistent value store until
    /// the estimated resident memory drops to `target_bytes` (or no further
    /// reduction is possible). Coldest rows (lowest commit version) are spilled
    /// first; their payloads are paged back in on read through the value store's
    /// hot cache. Keys, indexes, and row versions remain resident.
    pub fn spill_table_rows_to_memory_target(
        &mut self,
        target_bytes: usize,
    ) -> Result<usize, crate::error::AedbError> {
        let mut memory_estimate = self.estimate_memory_bytes();
        if memory_estimate <= target_bytes {
            return Ok(memory_estimate);
        }

        let Some(store) = self.value_store.clone() else {
            return Ok(memory_estimate);
        };

        let new_cost = persistent_value_ref_resident_cost();
        let mut candidates: Vec<(NamespaceId, String, EncodedKey, usize)> = Vec::new();
        let mut heap_entries = Vec::new();
        for (namespace_id, namespace) in self.namespaces.iter() {
            for (table_name, table) in namespace.tables.iter() {
                for (key, stored) in table.rows.iter() {
                    let Some(row) = stored.resident() else {
                        continue;
                    };
                    let old_cost = row_mem_cost(row);
                    if old_cost <= new_cost {
                        continue;
                    }
                    let memory_reduction_bytes = old_cost - new_cost;
                    let version = table_row_version(table, key);
                    let candidate_index = candidates.len();
                    candidates.push((
                        namespace_id.clone(),
                        table_name.clone(),
                        key.clone(),
                        memory_reduction_bytes,
                    ));
                    heap_entries.push(Reverse((version, memory_reduction_bytes, candidate_index)));
                }
            }
        }
        let mut oldest_first = BinaryHeap::from(heap_entries);

        let mut plans: Vec<(NamespaceId, String, EncodedKey, usize)> = Vec::new();
        let mut payloads: Vec<Vec<u8>> = Vec::new();
        while memory_estimate > target_bytes {
            let Some(Reverse((_, _, candidate_index))) = oldest_first.pop() else {
                break;
            };
            let Some((namespace_id, table_name, key, memory_reduction_bytes)) =
                candidates.get(candidate_index)
            else {
                continue;
            };
            let Some(row) = self
                .namespaces
                .get(namespace_id)
                .and_then(|namespace| namespace.tables.get(table_name))
                .and_then(|table| table.rows.get(key))
                .and_then(StoredRow::resident)
            else {
                continue;
            };
            payloads.push(encode_row_payload(row)?);
            plans.push((
                namespace_id.clone(),
                table_name.clone(),
                key.clone(),
                *memory_reduction_bytes,
            ));
            memory_estimate = memory_estimate.saturating_sub(*memory_reduction_bytes);
        }

        if plans.is_empty() {
            return Ok(memory_estimate);
        }

        let payload_slices: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
        let value_refs = store.append_many_cold_slices(&payload_slices)?;
        self.apply_row_spill_plans(plans, value_refs);

        Ok(self.estimate_memory_bytes())
    }

    fn apply_row_spill_plans(
        &mut self,
        plans: Vec<(NamespaceId, String, EncodedKey, usize)>,
        value_refs: Vec<PersistentValueRef>,
    ) {
        for ((namespace_id, table_name, key, _reduction), value_ref) in
            plans.into_iter().zip(value_refs)
        {
            let Some(table) = self
                .namespaces_mut()
                .get_mut(&namespace_id)
                .and_then(|namespace| namespace.tables.get_mut(&table_name))
            else {
                continue;
            };
            let Some(stored) = table.rows.get(&key) else {
                continue;
            };
            let Some(row) = stored.resident() else {
                continue;
            };
            let old_cost = row_mem_cost(row);
            let new_cost = persistent_value_ref_cost(&value_ref);
            let version = table_row_version(table, &key);
            table
                .rows
                .insert(key, StoredRow::spilled_versioned(version, value_ref));
            self.mem_bytes = self
                .mem_bytes
                .saturating_add(new_cost)
                .saturating_sub(old_cost);
        }
    }

    /// Cold-tier eviction: move whole rows (key + value) out of the resident
    /// `rows` map into sorted on-disk segments until the estimated resident
    /// memory drops to `target_bytes`. Reads page evicted rows back through the
    /// segment store; the cold tier is re-inlined before every checkpoint, so it
    /// never persists and evicted rows stay recoverable from the WAL + last
    /// checkpoint. The largest hot tables are evicted first.
    pub fn flush_table_rows_to_segments_to_memory_target(
        &mut self,
        target_bytes: usize,
    ) -> Result<usize, crate::error::AedbError> {
        let mut memory_estimate = self.estimate_memory_bytes();
        if memory_estimate <= target_bytes {
            return Ok(memory_estimate);
        }
        let Some(store) = self.kv_segment_store.clone() else {
            return Ok(memory_estimate);
        };
        let value_store = self.value_store.clone();

        // Candidate (namespace, table) pairs ranked by resident hot-row cost.
        // Only user-project tables are evicted; system/global operational tables
        // stay hot (they are small and read on every commit).
        let mut candidates: Vec<(NamespaceId, String, usize)> = Vec::new();
        for (namespace_id, namespace) in self.namespaces.iter() {
            let NamespaceId::Project(ns_key) = namespace_id else {
                continue;
            };
            if ns_key.starts_with(crate::catalog::SYSTEM_PROJECT_ID) {
                continue;
            }
            for (table_name, table) in namespace.tables.iter() {
                if table.rows.is_empty() {
                    continue;
                }
                let cost: usize = table.rows.values().map(stored_row_mem_cost).sum();
                if cost > 0 {
                    candidates.push((namespace_id.clone(), table_name.clone(), cost));
                }
            }
        }
        candidates.sort_by_key(|c| std::cmp::Reverse(c.2)); // largest resident cost first

        for (namespace_id, table_name, _cost) in candidates {
            if memory_estimate <= target_bytes {
                break;
            }
            let Some(table) = self
                .namespaces
                .get(&namespace_id)
                .and_then(|ns| ns.tables.get(&table_name))
            else {
                continue;
            };
            // Materialize all hot rows into sorted segment entries. The `rows`
            // OrdMap iterates in encoded-key (== byte) order, which the segment
            // writer requires.
            let mut entries: Vec<KvSegmentEntry> = Vec::with_capacity(table.rows.len());
            let mut resident_cost: usize = 0;
            for (key, stored) in table.rows.iter() {
                resident_cost = resident_cost.saturating_add(stored_row_mem_cost(stored));
                let version = stored
                    .inline_version()
                    .or_else(|| table.row_versions.get(key).copied())
                    .unwrap_or(0);
                let row = materialize_row(stored, value_store.as_deref())?.into_owned();
                entries.push(encode_row_segment_entry(key, version, &row)?);
            }
            if entries.is_empty() {
                continue;
            }
            let meta =
                store.write_segment(&format!("rowseg_{namespace_id:?}_{table_name}"), entries)?;
            let filename = meta.filename.clone();
            let meta_cost = kv_segment_meta_cost(&meta);
            let Some(table) = self
                .namespaces_mut()
                .get_mut(&namespace_id)
                .and_then(|ns| ns.tables.get_mut(&table_name))
            else {
                store.mark_segment_published(&filename);
                continue;
            };
            table.rows.clear();
            table.row_versions.clear();
            table.row_segments.push(meta);
            store.mark_segment_published(&filename);
            self.mem_bytes = self
                .mem_bytes
                .saturating_sub(resident_cost)
                .saturating_add(meta_cost);
            memory_estimate = self.estimate_memory_bytes();
        }
        Ok(memory_estimate)
    }

    /// Evict cold secondary-index postings to sorted on-disk segments until the
    /// resident estimate drops to `target_bytes`, mirroring
    /// [`flush_table_rows_to_segments_to_memory_target`] for index stores.
    ///
    /// Indexes are ranked by resident posting cost (largest first); for each the
    /// whole resident store is serialized to one segment (composite key
    /// `index_value ‖ pk`, stamped with `current_seq` for tombstone resolution),
    /// then cleared. The cold tier is runtime-only: postings page back in on read
    /// and are re-inlined before every checkpoint. Only user-project indexes are
    /// evicted; small system tables stay hot.
    pub fn flush_index_postings_to_segments_to_memory_target(
        &mut self,
        target_bytes: usize,
        current_seq: u64,
    ) -> Result<usize, crate::error::AedbError> {
        let mut memory_estimate = self.estimate_memory_bytes();
        if memory_estimate <= target_bytes {
            return Ok(memory_estimate);
        }
        let Some(store) = self.kv_segment_store.clone() else {
            return Ok(memory_estimate);
        };

        let mut candidates: Vec<(NamespaceId, String, String, usize)> = Vec::new();
        for (namespace_id, namespace) in self.namespaces.iter() {
            let NamespaceId::Project(ns_key) = namespace_id else {
                continue;
            };
            if ns_key.starts_with(crate::catalog::SYSTEM_PROJECT_ID) {
                continue;
            }
            for (table_name, table) in namespace.tables.iter() {
                for (index_name, index) in table.indexes.iter() {
                    if index.resident_store_is_empty() {
                        continue;
                    }
                    let cost = secondary_index_store_cost(index);
                    if cost > 0 {
                        candidates.push((
                            namespace_id.clone(),
                            table_name.clone(),
                            index_name.clone(),
                            cost,
                        ));
                    }
                }
            }
        }
        candidates.sort_by_key(|c| std::cmp::Reverse(c.3)); // largest resident cost first

        for (namespace_id, table_name, index_name, _cost) in candidates {
            if memory_estimate <= target_bytes {
                break;
            }
            let Some(index) = self
                .namespaces
                .get(&namespace_id)
                .and_then(|ns| ns.tables.get(&table_name))
                .and_then(|t| t.indexes.get(&index_name))
            else {
                continue;
            };
            let resident_cost = secondary_index_store_cost(index);
            let mut entries: Vec<KvSegmentEntry> = index
                .resident_postings()
                .into_iter()
                .map(|(value, pk)| encode_index_segment_entry(&value, &pk, current_seq))
                .collect();
            if entries.is_empty() {
                continue;
            }
            // The segment writer requires entries in ascending key order; Hash and
            // UniqueHash stores iterate unordered, so sort the composites.
            entries.sort_by(|a, b| a.key.cmp(&b.key));
            let meta = store.write_segment(
                &format!("idxseg_{namespace_id:?}_{table_name}_{index_name}"),
                entries,
            )?;
            let filename = meta.filename.clone();
            let meta_cost = kv_segment_meta_cost(&meta);
            let Some(index) = self
                .namespaces_mut()
                .get_mut(&namespace_id)
                .and_then(|ns| ns.tables.get_mut(&table_name))
                .and_then(|t| t.indexes.get_mut(&index_name))
            else {
                store.mark_segment_published(&filename);
                continue;
            };
            index.clear_resident_store();
            index.segments.push(meta);
            store.mark_segment_published(&filename);
            self.mem_bytes = self
                .mem_bytes
                .saturating_sub(resident_cost)
                .saturating_add(meta_cost);
            memory_estimate = self.estimate_memory_bytes();
        }
        Ok(memory_estimate)
    }

    pub fn flush_kv_to_segments_to_memory_target(
        &mut self,
        target_bytes: usize,
    ) -> Result<usize, crate::error::AedbError> {
        let mut memory_estimate = self.estimate_memory_bytes();
        if memory_estimate <= target_bytes {
            return Ok(memory_estimate);
        }
        let Some(segment_store) = self.kv_segment_store.clone() else {
            return Ok(memory_estimate);
        };

        let mut namespace_ids: Vec<(NamespaceId, usize)> = self
            .namespaces
            .iter()
            .filter_map(|(namespace_id, namespace)| {
                let resident_cost = hot_kv_resident_cost(&namespace.kv);
                (resident_cost > 0).then_some((namespace_id.clone(), resident_cost))
            })
            .collect();
        namespace_ids.sort_by(|(_, left_cost), (_, right_cost)| right_cost.cmp(left_cost));

        for (namespace_id, _) in namespace_ids {
            if memory_estimate <= target_bytes {
                break;
            }
            let Some(namespace) = self.namespaces.get(&namespace_id) else {
                continue;
            };
            let (entries, resident_cost) = collect_hot_kv_segment_entries(&namespace.kv);
            if entries.is_empty() {
                continue;
            }
            let meta = segment_store.write_segment(&format!("{namespace_id:?}"), entries)?;
            let filename = meta.filename.clone();
            let meta_cost = kv_segment_meta_cost(&meta);
            let Some(namespace) = self.namespaces_mut().get_mut(&namespace_id) else {
                segment_store.mark_segment_published(&filename);
                continue;
            };
            namespace.kv.entries.clear();
            namespace.kv.small_entries.clear();
            namespace.kv.segments.push(meta);
            segment_store.mark_segment_published(&filename);
            self.mem_bytes = self
                .mem_bytes
                .saturating_sub(resident_cost)
                .saturating_add(meta_cost);
            memory_estimate = self.estimate_memory_bytes();
        }
        let compactable = self
            .namespaces
            .values()
            .any(|namespace| namespace.kv.segments.len() > 4);
        if compactable {
            memory_estimate = self.compact_kv_segments()?;
        }

        Ok(memory_estimate)
    }

    pub fn compact_kv_segments(&mut self) -> Result<usize, crate::error::AedbError> {
        let namespace_ids: Vec<NamespaceId> = self
            .namespaces
            .iter()
            .filter(|(_, namespace)| {
                namespace.kv.segments.len() > 1 || !namespace.kv.segment_tombstones.is_empty()
            })
            .map(|(namespace_id, _)| namespace_id.clone())
            .collect();
        for namespace_id in namespace_ids {
            self.compact_kv_segments_for_namespace(&namespace_id)?;
        }
        self.refresh_mem_bytes();
        Ok(self.estimate_memory_bytes())
    }

    fn compact_kv_segments_for_namespace(
        &mut self,
        namespace_id: &NamespaceId,
    ) -> Result<(), crate::error::AedbError> {
        let Some(segment_store) = self.kv_segment_store.clone() else {
            return Ok(());
        };
        let Some(namespace) = self.namespaces.get(namespace_id) else {
            return Ok(());
        };
        if namespace.kv.segments.len() <= 1 && namespace.kv.segment_tombstones.is_empty() {
            return Ok(());
        }
        let mut merged = std::collections::BTreeMap::<Vec<u8>, KvEntry>::new();
        for segment in namespace.kv.segments.iter().rev() {
            for item in segment_store.read_segment_cold(segment)? {
                if namespace.kv.segment_tombstones.contains_key(&item.key) {
                    continue;
                }
                if let std::collections::btree_map::Entry::Vacant(entry) = merged.entry(item.key) {
                    entry.insert(item.entry);
                }
            }
        }
        if merged.is_empty() {
            let Some(namespace) = self.namespaces_mut().get_mut(namespace_id) else {
                return Ok(());
            };
            namespace.kv.segments.clear();
            namespace.kv.segment_tombstones.clear();
            return Ok(());
        }
        let entries = merged
            .into_iter()
            .map(|(key, entry)| KvSegmentEntry { key, entry })
            .collect::<Vec<_>>();
        let meta = segment_store.write_segment(&format!("{namespace_id:?}"), entries)?;
        let filename = meta.filename.clone();
        let Some(namespace) = self.namespaces_mut().get_mut(namespace_id) else {
            segment_store.mark_segment_published(&filename);
            return Ok(());
        };
        namespace.kv.segments.clear();
        namespace.kv.segments.push(meta);
        namespace.kv.segment_tombstones.clear();
        segment_store.mark_segment_published(&filename);
        Ok(())
    }

    fn apply_spill_plans(
        &mut self,
        plans: Vec<(NamespaceId, Vec<u8>, usize)>,
        value_refs: Vec<PersistentValueRef>,
    ) {
        for ((namespace_id, key, _payload_bytes), value_ref) in plans.into_iter().zip(value_refs) {
            let Some(namespace) = self.namespaces_mut().get_mut(&namespace_id) else {
                continue;
            };
            if let Some(entry) = namespace.kv.small_entries.remove(&compact_kv_key(&key)) {
                let old_cost = small_kv_entry_cost(key.len(), entry.resident_value_len());
                let spilled_entry = KvEntry::spilled(entry.version, entry.created_at, value_ref);
                let new_cost = kv_entry_cost(key.len(), spilled_entry.resident_memory_value_len());
                namespace.kv.entries.insert(key, spilled_entry);
                self.mem_bytes = self
                    .mem_bytes
                    .saturating_add(new_cost)
                    .saturating_sub(old_cost);
                continue;
            }
            let Some(entry) = namespace.kv.entries.get_mut(&key) else {
                continue;
            };
            if entry
                .resident_value_slice()
                .is_none_or(|value| value.is_empty())
            {
                continue;
            }
            let old_cost = kv_entry_cost(key.len(), entry.resident_memory_value_len());
            entry.value = Vec::new();
            entry.value_ref = Some(value_ref);
            let new_cost = kv_entry_cost(key.len(), entry.resident_memory_value_len());
            self.mem_bytes = self
                .mem_bytes
                .saturating_add(new_cost)
                .saturating_sub(old_cost);
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
        let new_cost = projection_data_mem_cost(&data);
        let key = (ns_id, table_name, index_name);
        let map = Arc::make_mut(&mut self.async_indexes);
        let old_cost = map.get(&key).map(projection_data_mem_cost).unwrap_or(0);
        map.insert(key, data);
        self.mem_bytes = self
            .mem_bytes
            .saturating_add(new_cost)
            .saturating_sub(old_cost);
    }

    /// Removes an async projection from the keyspace (copy-on-write).
    pub fn remove_async_projection(
        &mut self,
        ns_id: &NamespaceId,
        table_name: &str,
        index_name: &str,
    ) {
        let key = (
            ns_id.clone(),
            table_name.to_string(),
            index_name.to_string(),
        );
        if let Some(p) = Arc::make_mut(&mut self.async_indexes).remove(&key) {
            self.mem_bytes = self.mem_bytes.saturating_sub(projection_data_mem_cost(&p));
        }
    }

    pub fn take_async_projection(
        &mut self,
        ns_id: &NamespaceId,
        table_name: &str,
        index_name: &str,
    ) -> Option<AsyncProjectionData> {
        let key = (
            ns_id.clone(),
            table_name.to_string(),
            index_name.to_string(),
        );
        let removed = Arc::make_mut(&mut self.async_indexes).remove(&key);
        if let Some(p) = &removed {
            self.mem_bytes = self.mem_bytes.saturating_sub(projection_data_mem_cost(p));
        }
        removed
    }

    /// Inserts a namespace into the keyspace (copy-on-write).
    pub fn insert_namespace(&mut self, ns_id: NamespaceId, namespace: Namespace) {
        let new_cost = namespace_mem_cost(&namespace);
        let map = Arc::make_mut(&mut self.namespaces);
        let old_cost = map.get(&ns_id).map(namespace_mem_cost).unwrap_or(0);
        map.insert(ns_id, namespace);
        self.mem_bytes = self
            .mem_bytes
            .saturating_add(new_cost)
            .saturating_sub(old_cost);
    }

    /// Inserts without updating `mem_bytes`. Use for throwaway keyspaces
    /// (e.g. per-task local keyspaces in the parallel apply runtime) where
    /// the running counter is never read. Avoids the O(N) walk.
    pub fn insert_namespace_unchecked(&mut self, ns_id: NamespaceId, namespace: Namespace) {
        Arc::make_mut(&mut self.namespaces).insert(ns_id, namespace);
    }

    pub fn set_backend(&mut self, backend: PrimaryIndexBackend) {
        self.primary_index_backend = backend;
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

    pub fn namespace(&self, namespace_id: &NamespaceId) -> Option<&Namespace> {
        self.namespaces.get(namespace_id)
    }

    pub fn kv_set_inline(
        &mut self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        value: Vec<u8>,
        commit_seq: u64,
    ) {
        let kv = self.kv_data_mut(project_id, scope_id);
        let (created_at, old_cost) = match existing_kv_created_at_and_cost(kv, &key) {
            Some((created_at, old_cost)) => (created_at, old_cost),
            None => {
                kv.structural_version = commit_seq;
                (commit_seq, 0)
            }
        };
        let extra_old_cost = remove_replaced_segment_tombstone_cost(kv, &key);
        let new_cost = if let Some(entry) = SmallKvEntry::new(&value, commit_seq, created_at) {
            let cost = small_kv_entry_cost(key.len(), entry.resident_value_len());
            kv.entries.remove(&key);
            kv.small_entries.insert(compact_kv_key(&key), entry);
            cost
        } else {
            let entry = KvEntry::inline(value, commit_seq, created_at);
            let cost = kv_entry_cost(key.len(), entry.resident_memory_value_len());
            kv.small_entries.remove(&compact_kv_key(&key));
            kv.entries.insert(key, entry);
            cost
        };
        self.mem_bytes = self
            .mem_bytes
            .saturating_add(new_cost)
            .saturating_sub(old_cost)
            .saturating_sub(extra_old_cost);
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
        let encoded_pk = EncodedKey::from_values(&pk);
        self.upsert_row_by_encoded_pk(
            project_id, scope_id, table_name, encoded_pk, row, commit_seq,
        );
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
        let new_cost = row_mem_cost(&row);
        let old_cost = {
            let table = self.table_mut(project_id, scope_id, table_name);
            let existing = table.rows.get(&encoded_pk);
            let old_cost = existing.map(stored_row_mem_cost).unwrap_or(0);
            if existing.is_none() {
                table.structural_version = commit_seq;
            }
            table.rows.insert(
                encoded_pk.clone(),
                StoredRow::resident_versioned(commit_seq, row),
            );
            // The version is now carried inline by the row. Clear any legacy
            // parallel-map entry for this key so the redundant resident copy of
            // the primary key is released as data is rewritten.
            table.row_versions.remove(&encoded_pk);
            // Re-inserting a key supersedes any tombstone from a prior delete of
            // a cold copy; the new hot row wins over the (now stale) segment.
            if !table.row_tombstones.is_empty() {
                table.row_tombstones.remove(&encoded_pk);
            }
            old_cost
        };
        self.mem_bytes = self
            .mem_bytes
            .saturating_add(new_cost)
            .saturating_sub(old_cost);
    }

    pub fn get_row(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        pk: &[Value],
    ) -> Result<Option<std::borrow::Cow<'_, Row>>, crate::error::AedbError> {
        let encoded_pk = EncodedKey::from_values(pk);
        self.get_row_by_encoded(project_id, scope_id, table_name, &encoded_pk)
    }

    pub fn get_row_by_encoded(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        encoded_pk: &EncodedKey,
    ) -> Result<Option<std::borrow::Cow<'_, Row>>, crate::error::AedbError> {
        let Some(table) = self
            .namespace(&NamespaceId::project_scope(project_id, scope_id))
            .and_then(|ns| ns.tables.get(table_name))
        else {
            return Ok(None);
        };
        if table.row_segments.is_empty() {
            return match table.rows.get(encoded_pk) {
                Some(stored) => Ok(Some(materialize_row(stored, self.value_store.as_deref())?)),
                None => Ok(None),
            };
        }
        Ok(tier_get_table_row(
            table,
            encoded_pk,
            self.value_store.as_deref(),
            self.kv_segment_store.as_deref(),
        )?
        .map(|(_, row)| std::borrow::Cow::Owned(row)))
    }

    /// Tier-aware range scan over a table (hot rows merged with the cold tier).
    pub fn tier_scan_rows(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        start: Bound<EncodedKey>,
        end: Bound<EncodedKey>,
        limit: usize,
    ) -> Result<Vec<(EncodedKey, Row)>, crate::error::AedbError> {
        let Some(table) = self
            .namespace(&NamespaceId::project_scope(project_id, scope_id))
            .and_then(|ns| ns.tables.get(table_name))
        else {
            return Ok(Vec::new());
        };
        tier_scan_table_rows(
            table,
            start,
            end,
            limit,
            self.value_store.as_deref(),
            self.kv_segment_store.as_deref(),
        )
    }

    /// Materializes a stored row using this keyspace's value store, paging in
    /// spilled payloads on demand. Resident rows are returned without a copy.
    pub fn materialize_row<'a>(
        &'a self,
        stored: &'a StoredRow,
    ) -> Result<std::borrow::Cow<'a, Row>, crate::error::AedbError> {
        materialize_row(stored, self.value_store.as_deref())
    }

    /// Returns the raw stored slot for a row without materializing spilled
    /// payloads. Use for existence and identity checks where the value is not
    /// needed.
    pub fn row_slot(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        encoded_pk: &EncodedKey,
    ) -> Option<&StoredRow> {
        self.namespace(&NamespaceId::project_scope(project_id, scope_id))
            .and_then(|ns| ns.tables.get(table_name))
            .and_then(|t| t.rows.get(encoded_pk))
    }

    pub fn delete_row(
        &mut self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        pk: &[Value],
        commit_seq: u64,
    ) -> Result<Option<Row>, crate::error::AedbError> {
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
    ) -> Result<Option<Row>, crate::error::AedbError> {
        // Resolve a cold-tier copy (if any) up front, while we only hold a
        // shared borrow, so a delete of a row that lives only in a segment is
        // not mistaken for "not found".
        let cold_row = {
            let table = self
                .namespace(&NamespaceId::project_scope(project_id, scope_id))
                .and_then(|ns| ns.tables.get(table_name));
            match table {
                None => return Ok(None),
                Some(table) if table.row_segments.is_empty() => None,
                Some(table) => {
                    if table.rows.contains_key(encoded_pk) {
                        None // hot copy wins; handled below
                    } else {
                        tier_get_table_row(
                            table,
                            encoded_pk,
                            self.value_store.as_deref(),
                            self.kv_segment_store.as_deref(),
                        )?
                        .map(|(_, row)| row)
                    }
                }
            }
        };

        let (removed_hot, had_segments) = {
            let table = match self
                .namespace_mut(NamespaceId::project_scope(project_id, scope_id))
                .tables
                .get_mut(table_name)
            {
                Some(table) => table,
                None => return Ok(None),
            };
            table.row_versions.remove(encoded_pk);
            let removed = table.rows.remove(encoded_pk);
            let had_segments = !table.row_segments.is_empty();
            if removed.is_some() || cold_row.is_some() {
                table.structural_version = commit_seq;
            }
            // Tombstone any cold copy so future reads see the delete; harmless if
            // no segment holds the key (cleared at the next checkpoint re-inline).
            if had_segments && (removed.is_some() || cold_row.is_some()) {
                table.row_tombstones.insert(encoded_pk.clone(), commit_seq);
            }
            (removed, had_segments)
        };
        let _ = had_segments;

        match removed_hot {
            Some(stored) => {
                self.mem_bytes = self.mem_bytes.saturating_sub(stored_row_mem_cost(&stored));
                let row = materialize_row(&stored, self.value_store.as_deref())?.into_owned();
                Ok(Some(row))
            }
            None => Ok(cold_row),
        }
    }

    /// Creates a snapshot of the keyspace.
    ///
    /// This is now an O(1) operation that just clones Arc pointers,
    /// providing true copy-on-write semantics with zero traversal cost.
    pub fn snapshot(&self) -> KeyspaceSnapshot {
        KeyspaceSnapshot {
            primary_index_backend: self.primary_index_backend,
            value_store: self.value_store.clone(),
            kv_segment_store: self.kv_segment_store.clone(),
            persistent_value_inline_threshold_bytes: self.persistent_value_inline_threshold_bytes,
            namespaces: Arc::clone(&self.namespaces),
            async_indexes: Arc::clone(&self.async_indexes),
            mem_bytes: self.mem_bytes,
        }
    }

    pub fn drop_table(&mut self, project_id: &str, scope_id: &str, table_name: &str) {
        let ns = NamespaceId::project_scope(project_id, scope_id);
        let mut freed: usize = 0;
        if let Some(namespace) = self.namespaces_mut().get_mut(&ns)
            && let Some(t) = namespace.tables.remove(table_name)
        {
            freed = freed.saturating_add(table_data_mem_cost(&t));
        }
        let async_keys: Vec<(NamespaceId, String, String)> = self
            .async_indexes
            .keys()
            .filter(|(p, t, _)| p == &ns && t == table_name)
            .cloned()
            .collect();
        for key in async_keys {
            if let Some(p) = self.async_indexes_mut().remove(&key) {
                freed = freed.saturating_add(projection_data_mem_cost(&p));
            }
        }
        self.mem_bytes = self.mem_bytes.saturating_sub(freed);
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
        let mut freed: usize = 0;
        for key in ns_keys {
            if let Some(ns) = self.namespaces_mut().remove(&key) {
                freed = freed.saturating_add(namespace_mem_cost(&ns));
            }
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
            if let Some(p) = self.async_indexes_mut().remove(&key) {
                freed = freed.saturating_add(projection_data_mem_cost(&p));
            }
        }
        self.mem_bytes = self.mem_bytes.saturating_sub(freed);
    }

    pub fn drop_scope(&mut self, project_id: &str, scope_id: &str) {
        let ns = NamespaceId::project_scope(project_id, scope_id);
        let mut freed: usize = 0;
        if let Some(removed_ns) = self.namespaces_mut().remove(&ns) {
            freed = freed.saturating_add(namespace_mem_cost(&removed_ns));
        }
        let async_keys: Vec<(NamespaceId, String, String)> = self
            .async_indexes
            .keys()
            .filter(|(p, _, _)| p == &ns)
            .cloned()
            .collect();
        for key in async_keys {
            if let Some(p) = self.async_indexes_mut().remove(&key) {
                freed = freed.saturating_add(projection_data_mem_cost(&p));
            }
        }
        self.mem_bytes = self.mem_bytes.saturating_sub(freed);
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
            .and_then(|t| t.version_of(&encoded_pk))
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
                // Versions are carried inline on rows (with a legacy-map
                // fallback), so range over the rows themselves.
                t.rows
                    .range((start, end))
                    .map(|(key, stored)| {
                        stored
                            .inline_version()
                            .or_else(|| t.row_versions.get(key).copied())
                            .unwrap_or(0)
                    })
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

    pub fn kv_get(&self, project_id: &str, scope_id: &str, key: &[u8]) -> Option<KvEntry> {
        self.try_kv_get(project_id, scope_id, key)
            .expect("persistent value store read failed")
    }

    pub fn try_kv_get(
        &self,
        project_id: &str,
        scope_id: &str,
        key: &[u8],
    ) -> Result<Option<KvEntry>, crate::error::AedbError> {
        self.namespace(&NamespaceId::project_scope(project_id, scope_id))
            .map(|ns| {
                if let Some(entry) = ns.kv.small_entries.get(&compact_kv_key(key)) {
                    Ok(Some(entry.materialize()))
                } else if let Some(entry) = ns.kv.entries.get(key) {
                    materialize_kv_entry(entry, self.value_store.as_deref()).map(Some)
                } else if ns.kv.segment_tombstones.contains_key(key) {
                    Ok(None)
                } else {
                    self.try_kv_segment_get(&ns.kv, key)
                }
            })
            .transpose()
            .map(|entry| entry.flatten())
    }

    fn try_kv_segment_get(
        &self,
        kv: &KvData,
        key: &[u8],
    ) -> Result<Option<KvEntry>, crate::error::AedbError> {
        let Some(store) = self.kv_segment_store.as_deref() else {
            if kv.segments.is_empty() {
                return Ok(None);
            }
            return Err(crate::error::AedbError::Unavailable {
                message: "KV segment store is not attached".into(),
            });
        };
        get_segment_entry(&kv.segments, key, store)?
            .map(|entry| materialize_kv_entry(&entry, self.value_store.as_deref()))
            .transpose()
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
            if let Some(entry) = self.try_kv_get(project_id, scope_id, &shard_key)? {
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
    ) -> Result<(), crate::error::AedbError> {
        let value_store = self.value_store.clone();
        let inline_threshold_bytes = self.persistent_value_inline_threshold_bytes;
        let kv = self.kv_data_mut(project_id, scope_id);
        let (created_at, old_cost) = match existing_kv_created_at_and_cost(kv, &key) {
            Some((created_at, old_cost)) => (created_at, old_cost),
            None => {
                kv.structural_version = commit_seq;
                (commit_seq, 0)
            }
        };
        let extra_old_cost = remove_replaced_segment_tombstone_cost(kv, &key);
        let new_cost = if value.len() <= inline_threshold_bytes
            && let Some(entry) = SmallKvEntry::new(&value, commit_seq, created_at)
        {
            let cost = small_kv_entry_cost(key.len(), entry.resident_value_len());
            kv.entries.remove(&key);
            kv.small_entries.insert(compact_kv_key(&key), entry);
            cost
        } else {
            let entry = maybe_spill_kv_entry(
                value_store.as_ref(),
                inline_threshold_bytes,
                value,
                commit_seq,
                created_at,
            )?;
            let cost = kv_entry_cost(key.len(), entry.resident_memory_value_len());
            kv.small_entries.remove(&compact_kv_key(&key));
            kv.entries.insert(key, entry);
            cost
        };
        self.mem_bytes = self
            .mem_bytes
            .saturating_add(new_cost)
            .saturating_sub(old_cost)
            .saturating_sub(extra_old_cost);
        Ok(())
    }

    pub fn kv_set_many_same_namespace<'a, I>(
        &mut self,
        project_id: &str,
        scope_id: &str,
        entries: I,
        commit_seq: u64,
    ) -> Result<(), crate::error::AedbError>
    where
        I: IntoIterator<Item = (&'a Vec<u8>, &'a Vec<u8>)>,
    {
        let value_store = self.value_store.clone();
        let inline_threshold_bytes = self.persistent_value_inline_threshold_bytes;
        let entries = entries.into_iter().collect::<Vec<_>>();
        let spilled_value_refs = if let Some(store) = value_store.as_ref() {
            let spilled_values = entries
                .iter()
                .filter_map(|(_key, value)| {
                    (value.len() > inline_threshold_bytes).then_some(value.as_slice())
                })
                .collect::<Vec<_>>();
            store.append_many_hot_slices(&spilled_values)?
        } else {
            Vec::new()
        };
        let mut spilled_value_refs = spilled_value_refs.into_iter();
        let mut new_cost_total = 0usize;
        let mut old_cost_total = 0usize;
        {
            let kv = self.kv_data_mut(project_id, scope_id);
            for (key, value) in entries {
                let (created_at, old_cost) = match existing_kv_created_at_and_cost(kv, key) {
                    Some((created_at, old_cost)) => (created_at, old_cost),
                    None => {
                        kv.structural_version = commit_seq;
                        (commit_seq, 0)
                    }
                };
                let extra_old_cost = remove_replaced_segment_tombstone_cost(kv, key);
                let new_cost = if value.len() <= inline_threshold_bytes
                    && let Some(entry) = SmallKvEntry::new(value, commit_seq, created_at)
                {
                    let cost = small_kv_entry_cost(key.len(), entry.resident_value_len());
                    kv.entries.remove(key);
                    kv.small_entries.insert(compact_kv_key(key), entry);
                    cost
                } else if value_store.is_some() {
                    let entry = KvEntry::spilled(
                        commit_seq,
                        created_at,
                        spilled_value_refs.next().ok_or_else(|| {
                            crate::error::AedbError::IntegrityError {
                                message: "missing persistent value ref for spilled KV batch".into(),
                            }
                        })?,
                    );
                    let cost = kv_entry_cost(key.len(), entry.resident_memory_value_len());
                    kv.small_entries.remove(&compact_kv_key(key));
                    kv.entries.insert(key.clone(), entry);
                    cost
                } else {
                    let entry = KvEntry::inline(value.clone(), commit_seq, created_at);
                    let cost = kv_entry_cost(key.len(), entry.resident_memory_value_len());
                    kv.small_entries.remove(&compact_kv_key(key));
                    kv.entries.insert(key.clone(), entry);
                    cost
                };
                new_cost_total = new_cost_total.saturating_add(new_cost);
                old_cost_total = old_cost_total
                    .saturating_add(old_cost)
                    .saturating_add(extra_old_cost);
            }
        }
        self.mem_bytes = self
            .mem_bytes
            .saturating_add(new_cost_total)
            .saturating_sub(old_cost_total);
        debug_assert!(
            spilled_value_refs.next().is_none(),
            "all persistent value refs should be consumed"
        );
        Ok(())
    }

    pub fn kv_set_many_inline_same_namespace<'a, I>(
        &mut self,
        project_id: &str,
        scope_id: &str,
        entries: I,
        commit_seq: u64,
    ) where
        I: IntoIterator<Item = (&'a Vec<u8>, &'a Vec<u8>)>,
    {
        let mut new_cost_total = 0usize;
        let mut old_cost_total = 0usize;
        {
            let kv = self.kv_data_mut(project_id, scope_id);
            for (key, value) in entries {
                let (new_cost, old_cost) = apply_inline_kv_batch_entry(kv, key, value, commit_seq);
                new_cost_total = new_cost_total.saturating_add(new_cost);
                old_cost_total = old_cost_total.saturating_add(old_cost);
            }
        }
        self.mem_bytes = self
            .mem_bytes
            .saturating_add(new_cost_total)
            .saturating_sub(old_cost_total);
    }

    pub fn kv_set_many_inline_same_namespace_with_seq<'a, I>(
        &mut self,
        project_id: &str,
        scope_id: &str,
        entries: I,
    ) where
        I: IntoIterator<Item = (&'a Vec<u8>, &'a Vec<u8>, u64)>,
    {
        let mut new_cost_total = 0usize;
        let mut old_cost_total = 0usize;
        {
            let kv = self.kv_data_mut(project_id, scope_id);
            for (key, value, commit_seq) in entries {
                let (new_cost, old_cost) = apply_inline_kv_batch_entry(kv, key, value, commit_seq);
                new_cost_total = new_cost_total.saturating_add(new_cost);
                old_cost_total = old_cost_total.saturating_add(old_cost);
            }
        }
        self.mem_bytes = self
            .mem_bytes
            .saturating_add(new_cost_total)
            .saturating_sub(old_cost_total);
    }

    pub fn kv_del(
        &mut self,
        project_id: &str,
        scope_id: &str,
        key: &[u8],
        commit_seq: u64,
    ) -> bool {
        let (removed, cost_freed, tombstone_added) = {
            let kv = self.kv_data_mut(project_id, scope_id);
            let had_tombstone = kv.segment_tombstones.contains_key(key);
            let small_removed = kv.small_entries.remove(&compact_kv_key(key));
            let entry_removed = kv.entries.remove(key);
            let may_remove_segment_entry = kv
                .segments
                .iter()
                .any(|segment| segment_may_contain_key(segment, key));
            let cost = small_removed
                .as_ref()
                .map(|e| small_kv_entry_cost(key.len(), e.resident_value_len()))
                .or_else(|| {
                    entry_removed
                        .as_ref()
                        .map(|e| kv_entry_cost(key.len(), e.resident_memory_value_len()))
                })
                .unwrap_or(0);
            let removed =
                small_removed.is_some() || entry_removed.is_some() || may_remove_segment_entry;
            if removed {
                let tombstone_added = if may_remove_segment_entry && !had_tombstone {
                    kv.segment_tombstones.insert(key.to_vec(), commit_seq);
                    true
                } else if may_remove_segment_entry {
                    kv.segment_tombstones.insert(key.to_vec(), commit_seq);
                    false
                } else {
                    kv.segment_tombstones.remove(key);
                    false
                };
                kv.structural_version = commit_seq;
                (removed, cost, tombstone_added)
            } else {
                (removed, 0, false)
            }
        };
        if cost_freed > 0 {
            self.mem_bytes = self.mem_bytes.saturating_sub(cost_freed);
        }
        if tombstone_added {
            self.mem_bytes = self.mem_bytes.saturating_add(kv_tombstone_cost(key.len()));
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
        self.try_kv_scan_prefix(project_id, scope_id, prefix, limit)
            .expect("persistent value store read failed")
    }

    pub fn try_kv_scan_prefix(
        &self,
        project_id: &str,
        scope_id: &str,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, KvEntry)>, crate::error::AedbError> {
        let Some(kv) = self
            .namespace(&NamespaceId::project_scope(project_id, scope_id))
            .map(|ns| &ns.kv)
        else {
            return Ok(Vec::new());
        };
        let (start, end) = if prefix.is_empty() {
            (Bound::Unbounded, Bound::Unbounded)
        } else {
            (
                Bound::Included(prefix.to_vec()),
                prefix_range_end(prefix)
                    .map(Bound::Excluded)
                    .unwrap_or(Bound::Unbounded),
            )
        };
        scan_kv_entries(
            kv,
            start,
            end,
            limit,
            self.value_store.as_deref(),
            self.kv_segment_store.as_deref(),
            true,
        )
    }

    pub fn kv_scan_prefix_ref(
        &self,
        project_id: &str,
        scope_id: &str,
        prefix: &[u8],
        limit: usize,
    ) -> Vec<(Vec<u8>, KvEntry)> {
        self.kv_scan_prefix(project_id, scope_id, prefix, limit)
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
        for (key, entry) in self.kv_scan_prefix(project_id, scope_id, prefix, limit) {
            if !visitor(key.as_slice(), &entry) {
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
        self.try_kv_scan_range(project_id, scope_id, start, end, limit)
            .expect("persistent value store read failed")
    }

    pub fn try_kv_scan_range(
        &self,
        project_id: &str,
        scope_id: &str,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, KvEntry)>, crate::error::AedbError> {
        let Some(kv) = self
            .namespace(&NamespaceId::project_scope(project_id, scope_id))
            .map(|ns| &ns.kv)
        else {
            return Ok(Vec::new());
        };
        scan_kv_entries(
            kv,
            start,
            end,
            limit,
            self.value_store.as_deref(),
            self.kv_segment_store.as_deref(),
            true,
        )
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
            .try_kv_get(project_id, scope_id, &key)?
            .map(|e| decode_u256(&e.value))
            .transpose()?
            .unwrap_or(U256::zero());
        let next = current
            .checked_add(amount)
            .ok_or(crate::error::AedbError::Overflow)?;
        self.kv_set(project_id, scope_id, key, encode_u256(next), commit_seq)?;
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
            .try_kv_get(project_id, scope_id, &key)?
            .map(|e| decode_u256(&e.value))
            .transpose()?
            .unwrap_or(U256::zero());
        if current < amount {
            return Err(crate::error::AedbError::Underflow);
        }
        let next = current - amount;
        self.kv_set(project_id, scope_id, key, encode_u256(next), commit_seq)?;
        Ok(next)
    }

    #[allow(clippy::too_many_arguments)]
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
        let current = self.try_kv_get(project_id, scope_id, &key)?;
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
        self.kv_set(project_id, scope_id, key, encode_u256(next), commit_seq)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
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
        let current = self.try_kv_get(project_id, scope_id, &key)?;
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
        self.kv_set(project_id, scope_id, key, encode_u256(next), commit_seq)?;
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
        let current = self.try_kv_get(project_id, scope_id, &key)?;
        let current_exists = current.is_some();
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
        if current_exists && next == current_value {
            return Ok(());
        }
        self.kv_set(project_id, scope_id, key, encode_u256(next), commit_seq)?;
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
        let current = self.try_kv_get(project_id, scope_id, &key)?;
        let current_exists = current.is_some();
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
        if current_exists && next == current_value {
            return Ok(());
        }
        self.kv_set(project_id, scope_id, key, encode_u256(next), commit_seq)?;
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
            .try_kv_get(project_id, scope_id, &key)?
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
        self.kv_set(project_id, scope_id, key, encode_u256(next), commit_seq)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
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
        let current = self.try_kv_get(project_id, scope_id, &key)?;
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
        self.kv_set(project_id, scope_id, key, encode_u64(next), commit_seq)?;
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
            .try_kv_get(project_id, scope_id, &key)?
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
        self.kv_set(project_id, scope_id, key, encode_u64(next), commit_seq)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
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
        let current = self.try_kv_get(project_id, scope_id, &key)?;
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
        self.kv_set(project_id, scope_id, key, encode_u64(next), commit_seq)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
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

    #[allow(clippy::too_many_arguments)]
    pub fn kv_add_i64_bounded(
        &mut self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        delta: i64,
        on_missing: &KvIntegerMissingPolicy,
        min_value: Option<i64>,
        max_value: Option<i64>,
        commit_seq: u64,
    ) -> Result<(), crate::error::AedbError> {
        let current = self.try_kv_get(project_id, scope_id, &key)?;
        let current_value = match (current, on_missing) {
            (Some(entry), _) => decode_i64(&entry.value)?,
            (None, KvIntegerMissingPolicy::TreatAsZero) => 0i64,
            (None, KvIntegerMissingPolicy::Reject) => {
                return Err(crate::error::AedbError::Validation(
                    "i64 key missing and policy is Reject".into(),
                ));
            }
        };
        let next = current_value
            .checked_add(delta)
            .ok_or(crate::error::AedbError::Overflow)?;
        if let Some(min_value) = min_value
            && next < min_value
        {
            return Err(crate::error::AedbError::Validation(format!(
                "i64 minimum would be violated: projected={next}, min={min_value}"
            )));
        }
        if let Some(max_value) = max_value
            && next > max_value
        {
            return Err(crate::error::AedbError::Validation(format!(
                "i64 maximum would be violated: projected={next}, max={max_value}"
            )));
        }
        self.kv_set(project_id, scope_id, key, encode_i64(next), commit_seq)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
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
        let current = self.try_kv_get(project_id, scope_id, &key)?;
        let current_exists = current.is_some();
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
        if current_exists && next == current_value {
            return Ok(());
        }
        self.kv_set(project_id, scope_id, key, encode_u64(next), commit_seq)?;
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
        let current = self.try_kv_get(project_id, scope_id, &key)?;
        let current_exists = current.is_some();
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
        if current_exists && next == current_value {
            return Ok(());
        }
        self.kv_set(project_id, scope_id, key, encode_u64(next), commit_seq)?;
        Ok(())
    }

    pub fn kv_version(&self, project_id: &str, scope_id: &str, key: &[u8]) -> u64 {
        self.try_kv_version(project_id, scope_id, key)
            .expect("KV segment read failed")
    }

    pub fn try_kv_version(
        &self,
        project_id: &str,
        scope_id: &str,
        key: &[u8],
    ) -> Result<u64, crate::error::AedbError> {
        let Some(namespace) = self.namespace(&NamespaceId::project_scope(project_id, scope_id))
        else {
            return Ok(0);
        };
        let version = namespace
            .kv
            .entries
            .get(key)
            .map(|entry| entry.version)
            .or_else(|| {
                namespace
                    .kv
                    .small_entries
                    .get(&compact_kv_key(key))
                    .map(|entry| entry.version)
            })
            .or_else(|| namespace.kv.segment_tombstones.get(key).copied())
            .map(Ok)
            .unwrap_or_else(|| {
                self.try_kv_segment_get(&namespace.kv, key)
                    .map(|entry| entry.map(|entry| entry.version).unwrap_or(0))
            })?;
        Ok(version)
    }

    pub fn max_kv_version_in_range(
        &self,
        project_id: &str,
        scope_id: &str,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
    ) -> u64 {
        self.try_max_kv_version_in_range(project_id, scope_id, start, end)
            .expect("KV segment scan failed")
    }

    pub fn try_max_kv_version_in_range(
        &self,
        project_id: &str,
        scope_id: &str,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
    ) -> Result<u64, crate::error::AedbError> {
        let Some(kv) = self
            .namespace(&NamespaceId::project_scope(project_id, scope_id))
            .map(|ns| &ns.kv)
        else {
            return Ok(0);
        };
        let visible_max = scan_kv_entries(
            kv,
            start.clone(),
            end.clone(),
            usize::MAX,
            self.value_store.as_deref(),
            self.kv_segment_store.as_deref(),
            true,
        )?
        .into_iter()
        .map(|(_, entry)| entry.version)
        .max()
        .unwrap_or(0);
        let tombstone_max = kv
            .segment_tombstones
            .range((start, end))
            .map(|(_, version)| *version)
            .max()
            .unwrap_or(0);
        Ok(visible_max.max(tombstone_max))
    }

    pub fn kv_structural_version(&self, project_id: &str, scope_id: &str) -> u64 {
        self.namespace(&NamespaceId::project_scope(project_id, scope_id))
            .map(|ns| ns.kv.structural_version)
            .unwrap_or(0)
    }

    pub fn estimate_memory_bytes(&self) -> usize {
        self.mem_bytes
    }

    /// Walks the entire keyspace to compute the memory estimate from scratch.
    /// Used after constructing a `Keyspace` from external data (checkpoint load,
    /// merges) to seed `mem_bytes`, and as the parity oracle in tests.
    pub fn recompute_memory_bytes_full(&self) -> usize {
        let ns_bytes: usize = self.namespaces.values().map(namespace_mem_cost).sum();
        let projection_bytes: usize = self
            .async_indexes
            .values()
            .map(projection_data_mem_cost)
            .sum();
        ns_bytes.saturating_add(projection_bytes)
    }

    /// Reseeds `mem_bytes` from a full walk. Call after building a `Keyspace`
    /// from external sources where the running counter cannot be tracked.
    pub fn refresh_mem_bytes(&mut self) {
        self.mem_bytes = self.recompute_memory_bytes_full();
    }

    /// Applies a signed delta to the running `mem_bytes` counter, saturating at
    /// zero. Index maintenance mutates posting stores directly (outside the row
    /// mutation helpers), so it reports its byte delta here to keep the running
    /// counter in lockstep with `recompute_memory_bytes_full()`.
    pub fn apply_mem_bytes_delta(&mut self, delta: i64) {
        if delta >= 0 {
            self.mem_bytes = self.mem_bytes.saturating_add(delta as usize);
        } else {
            self.mem_bytes = self.mem_bytes.saturating_sub(delta.unsigned_abs() as usize);
        }
    }
}

impl KeyspaceSnapshot {
    pub fn kv_segment_filenames(&self) -> HashSet<String> {
        collect_kv_segment_filenames(self.namespaces.values())
    }
}

fn collect_kv_segment_filenames<'a>(
    namespaces: impl Iterator<Item = &'a Namespace>,
) -> HashSet<String> {
    let mut filenames = HashSet::new();
    for namespace in namespaces {
        // KV cold tier.
        for segment in &namespace.kv.segments {
            filenames.insert(segment.filename.clone());
        }
        for table in namespace.tables.values() {
            // Cold table-row tier.
            for segment in &table.row_segments {
                filenames.insert(segment.filename.clone());
            }
            // Cold secondary-index tier. These share the `kvseg_*.aedbkv`
            // namespace and naming with KV/row segments, so they MUST be listed
            // here or `reclaim_unreferenced_segments` would delete live index
            // segment files and corrupt cold postings.
            for secondary_index in table.indexes.values() {
                for segment in &secondary_index.segments {
                    filenames.insert(segment.filename.clone());
                }
            }
        }
    }
    filenames
}

impl KeyspaceSnapshot {
    pub fn estimate_memory_bytes(&self) -> usize {
        self.mem_bytes
    }

    pub fn recompute_memory_bytes_full(&self) -> usize {
        let ns_bytes: usize = self.namespaces.values().map(namespace_mem_cost).sum();
        let projection_bytes: usize = self
            .async_indexes
            .values()
            .map(projection_data_mem_cost)
            .sum();
        ns_bytes.saturating_add(projection_bytes)
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

    /// Materializes a stored row using this snapshot's value store, paging in
    /// spilled payloads on demand. Resident rows are returned without a copy.
    pub fn materialize_row<'a>(
        &'a self,
        stored: &'a StoredRow,
    ) -> Result<std::borrow::Cow<'a, Row>, crate::error::AedbError> {
        materialize_row(stored, self.value_store.as_deref())
    }

    pub fn get_row_by_encoded(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        encoded_pk: &EncodedKey,
    ) -> Result<Option<std::borrow::Cow<'_, Row>>, crate::error::AedbError> {
        let Some(table) = self.table(project_id, scope_id, table_name) else {
            return Ok(None);
        };
        // Hot fast path returns a borrow; cold rows page in as owned.
        if table.row_segments.is_empty() {
            return match table.rows.get(encoded_pk) {
                Some(stored) => Ok(Some(self.materialize_row(stored)?)),
                None => Ok(None),
            };
        }
        Ok(tier_get_table_row(
            table,
            encoded_pk,
            self.value_store.as_deref(),
            self.kv_segment_store.as_deref(),
        )?
        .map(|(_, row)| std::borrow::Cow::Owned(row)))
    }

    /// Tier-aware MVCC version of a row by primary key (hot map or cold tier).
    /// `0` if the row does not exist.
    pub fn tier_row_version(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        encoded_pk: &EncodedKey,
    ) -> u64 {
        let Some(table) = self.table(project_id, scope_id, table_name) else {
            return 0;
        };
        if let Some(version) = table.version_of(encoded_pk) {
            return version;
        }
        if table.row_segments.is_empty() {
            return 0;
        }
        tier_get_table_row(
            table,
            encoded_pk,
            self.value_store.as_deref(),
            self.kv_segment_store.as_deref(),
        )
        .ok()
        .flatten()
        .map(|(version, _)| version)
        .unwrap_or(0)
    }

    /// Tier-aware range scan over a table, merging hot rows with the cold tier.
    pub fn tier_scan_rows(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        start: Bound<EncodedKey>,
        end: Bound<EncodedKey>,
        limit: usize,
    ) -> Result<Vec<(EncodedKey, Row)>, crate::error::AedbError> {
        let Some(table) = self.table(project_id, scope_id, table_name) else {
            return Ok(Vec::new());
        };
        tier_scan_table_rows(
            table,
            start,
            end,
            limit,
            self.value_store.as_deref(),
            self.kv_segment_store.as_deref(),
        )
    }

    pub fn kv_get(&self, project_id: &str, scope_id: &str, key: &[u8]) -> Option<KvEntry> {
        self.try_kv_get(project_id, scope_id, key)
            .expect("persistent value store read failed")
    }

    pub fn try_kv_get(
        &self,
        project_id: &str,
        scope_id: &str,
        key: &[u8],
    ) -> Result<Option<KvEntry>, crate::error::AedbError> {
        self.namespaces
            .get(&NamespaceId::project_scope(project_id, scope_id))
            .map(|ns| {
                if let Some(entry) = ns.kv.small_entries.get(&compact_kv_key(key)) {
                    Ok(Some(entry.materialize()))
                } else if let Some(entry) = ns.kv.entries.get(key) {
                    materialize_kv_entry(entry, self.value_store.as_deref()).map(Some)
                } else if ns.kv.segment_tombstones.contains_key(key) {
                    Ok(None)
                } else {
                    self.try_kv_segment_get(&ns.kv, key)
                }
            })
            .transpose()
            .map(|entry| entry.flatten())
    }

    fn try_kv_segment_get(
        &self,
        kv: &KvData,
        key: &[u8],
    ) -> Result<Option<KvEntry>, crate::error::AedbError> {
        let Some(store) = self.kv_segment_store.as_deref() else {
            if kv.segments.is_empty() {
                return Ok(None);
            }
            return Err(crate::error::AedbError::Unavailable {
                message: "KV segment store is not attached".into(),
            });
        };
        get_segment_entry(&kv.segments, key, store)?
            .map(|entry| materialize_kv_entry(&entry, self.value_store.as_deref()))
            .transpose()
    }

    pub fn kv_scan_prefix(
        &self,
        project_id: &str,
        scope_id: &str,
        prefix: &[u8],
        limit: usize,
    ) -> Vec<(Vec<u8>, KvEntry)> {
        self.try_kv_scan_prefix(project_id, scope_id, prefix, limit)
            .expect("persistent value store read failed")
    }

    pub fn try_kv_scan_prefix(
        &self,
        project_id: &str,
        scope_id: &str,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, KvEntry)>, crate::error::AedbError> {
        let Some(kv) = self
            .namespaces
            .get(&NamespaceId::project_scope(project_id, scope_id))
            .map(|ns| &ns.kv)
        else {
            return Ok(Vec::new());
        };
        let (start, end) = if prefix.is_empty() {
            (Bound::Unbounded, Bound::Unbounded)
        } else {
            (
                Bound::Included(prefix.to_vec()),
                prefix_range_end(prefix)
                    .map(Bound::Excluded)
                    .unwrap_or(Bound::Unbounded),
            )
        };
        scan_kv_entries(
            kv,
            start,
            end,
            limit,
            self.value_store.as_deref(),
            self.kv_segment_store.as_deref(),
            true,
        )
    }

    pub fn try_kv_scan_range(
        &self,
        project_id: &str,
        scope_id: &str,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, KvEntry)>, crate::error::AedbError> {
        let Some(kv) = self
            .namespaces
            .get(&NamespaceId::project_scope(project_id, scope_id))
            .map(|ns| &ns.kv)
        else {
            return Ok(Vec::new());
        };
        scan_kv_entries(
            kv,
            start,
            end,
            limit,
            self.value_store.as_deref(),
            self.kv_segment_store.as_deref(),
            true,
        )
    }

    pub fn materialized_for_checkpoint(&self) -> Result<Self, crate::error::AedbError> {
        let mut out = self.clone();
        let value_store = self.value_store.as_deref();
        let namespace_ids: Vec<NamespaceId> = self
            .namespaces
            .iter()
            .filter(|(_, namespace)| {
                !namespace.kv.segments.is_empty()
                    || !namespace.kv.small_entries.is_empty()
                    || !namespace.kv.segment_tombstones.is_empty()
                    || namespace
                        .kv
                        .entries
                        .values()
                        .any(|entry| entry.value_ref.is_some())
            })
            .map(|(namespace_id, _)| namespace_id.clone())
            .collect();
        for namespace_id in namespace_ids {
            let Some(source_namespace) = self.namespaces.get(&namespace_id) else {
                continue;
            };
            let materialized_entries = scan_kv_entries(
                &source_namespace.kv,
                Bound::Unbounded,
                Bound::Unbounded,
                usize::MAX,
                value_store,
                self.kv_segment_store.as_deref(),
                false,
            )?;
            let Some(namespace) = Arc::make_mut(&mut out.namespaces).get_mut(&namespace_id) else {
                continue;
            };
            namespace.kv.entries.clear();
            namespace.kv.small_entries.clear();
            namespace.kv.segment_tombstones.clear();
            namespace.kv.segments.clear();
            for (key, entry) in materialized_entries {
                namespace.kv.entries.insert(key, entry);
            }
        }
        // Materialize spilled table rows back inline so the checkpoint is
        // self-contained once the value store is detached below.
        let row_namespace_ids: Vec<NamespaceId> = self
            .namespaces
            .iter()
            .filter(|(_, namespace)| {
                namespace
                    .tables
                    .values()
                    .any(|table| table.rows.values().any(StoredRow::is_spilled))
            })
            .map(|(namespace_id, _)| namespace_id.clone())
            .collect();
        for namespace_id in row_namespace_ids {
            let Some(namespace) = Arc::make_mut(&mut out.namespaces).get_mut(&namespace_id) else {
                continue;
            };
            let table_names: Vec<String> = namespace.tables.keys().cloned().collect();
            for table_name in table_names {
                let Some(table) = namespace.tables.get_mut(&table_name) else {
                    continue;
                };
                let spilled_keys: Vec<EncodedKey> = table
                    .rows
                    .iter()
                    .filter(|(_, stored)| stored.is_spilled())
                    .map(|(key, _)| key.clone())
                    .collect();
                for key in spilled_keys {
                    if let Some(stored) = table.rows.get(&key) {
                        let version = stored.inline_version();
                        let row = materialize_row(stored, value_store)?.into_owned();
                        let restored = match version {
                            Some(version) => StoredRow::resident_versioned(version, row),
                            None => StoredRow::Resident(row),
                        };
                        table.rows.insert(key, restored);
                    }
                }
            }
        }

        // Re-inline the cold row-segment tier so the checkpoint is
        // self-contained (the cold tier is runtime-only and never persisted).
        let segment_store = self.kv_segment_store.as_deref();
        let cold_namespace_ids: Vec<NamespaceId> = self
            .namespaces
            .iter()
            .filter(|(_, namespace)| {
                namespace
                    .tables
                    .values()
                    .any(|table| !table.row_segments.is_empty())
            })
            .map(|(namespace_id, _)| namespace_id.clone())
            .collect();
        for namespace_id in cold_namespace_ids {
            let Some(namespace) = Arc::make_mut(&mut out.namespaces).get_mut(&namespace_id) else {
                continue;
            };
            let table_names: Vec<String> = namespace.tables.keys().cloned().collect();
            for table_name in table_names {
                let Some(table) = namespace.tables.get_mut(&table_name) else {
                    continue;
                };
                if table.row_segments.is_empty() {
                    continue;
                }
                let store = segment_store.ok_or_else(|| crate::error::AedbError::Unavailable {
                    message: "row segment store is not attached".into(),
                })?;
                let hot_keys: std::collections::HashSet<EncodedKey> =
                    table.rows.keys().cloned().collect();
                let mut new_rows = table.rows.clone();
                for segment in &table.row_segments {
                    for item in store.scan_range(segment, &Bound::Unbounded, &Bound::Unbounded)? {
                        let key = EncodedKey::from_bytes(item.key);
                        if hot_keys.contains(&key) {
                            continue; // hot copy always wins
                        }
                        let version = item.entry.version;
                        if let Some(tomb) = table.row_tombstones.get(&key)
                            && *tomb >= version
                        {
                            continue; // deleted after this segment was written
                        }
                        if let Some(existing) = new_rows.get(&key)
                            && existing
                                .inline_version()
                                .map(|v| v >= version)
                                .unwrap_or(false)
                        {
                            continue; // a newer segment already provided this key
                        }
                        let row = decode_row_payload(&item.entry.value)?;
                        new_rows.insert(key, StoredRow::resident_versioned(version, row));
                    }
                }
                table.rows = new_rows;
                table.row_segments.clear();
                table.row_tombstones.clear();
            }
        }

        // Re-inline the cold secondary-index segment tier so the checkpoint is
        // self-contained (also runtime-only, never persisted).
        let index_cold_namespace_ids: Vec<NamespaceId> = self
            .namespaces
            .iter()
            .filter(|(_, namespace)| {
                namespace.tables.values().any(|table| {
                    table
                        .indexes
                        .values()
                        .any(SecondaryIndex::has_cold_segments)
                })
            })
            .map(|(namespace_id, _)| namespace_id.clone())
            .collect();
        for namespace_id in index_cold_namespace_ids {
            let store = segment_store.ok_or_else(|| crate::error::AedbError::Unavailable {
                message: "row segment store is not attached".into(),
            })?;
            let Some(namespace) = Arc::make_mut(&mut out.namespaces).get_mut(&namespace_id) else {
                continue;
            };
            let table_names: Vec<String> = namespace.tables.keys().cloned().collect();
            for table_name in table_names {
                let Some(table) = namespace.tables.get_mut(&table_name) else {
                    continue;
                };
                let index_names: Vec<String> = table.indexes.keys().cloned().collect();
                for index_name in index_names {
                    let Some(index) = table.indexes.get_mut(&index_name) else {
                        continue;
                    };
                    if index.has_cold_segments() {
                        reinline_index_segments(index, store)?;
                    }
                }
            }
        }

        out.value_store = None;
        out.kv_segment_store = None;
        out.persistent_value_inline_threshold_bytes = usize::MAX;
        out.mem_bytes = out.recompute_memory_bytes_full();
        Ok(out)
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
            if let Some(entry) = self.try_kv_get(project_id, scope_id, &shard_key)? {
                let value = decode_u64(&entry.value)?;
                total = total
                    .checked_add(value)
                    .ok_or(crate::error::AedbError::Overflow)?;
            }
        }
        Ok(total)
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

fn default_persistent_value_inline_threshold_bytes() -> usize {
    usize::MAX
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

fn encode_i64(v: i64) -> Vec<u8> {
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

fn decode_i64(bytes: &[u8]) -> Result<i64, crate::error::AedbError> {
    if bytes.len() != 8 {
        return Err(crate::error::AedbError::Validation(
            "invalid i64 bytes length".into(),
        ));
    }
    let mut out = [0u8; 8];
    out.copy_from_slice(bytes);
    Ok(i64::from_be_bytes(out))
}

#[cfg(test)]
mod tests;
