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
    pub pending_delta_sum: i128,
    #[serde(default)]
    pub pending_delta_sum_cache_valid: bool,
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

fn unapplied_delta_sum(accumulator: &AccumulatorData) -> Result<i128, crate::error::AedbError> {
    if accumulator.pending_delta_sum_cache_valid {
        return Ok(accumulator.pending_delta_sum);
    }
    let mut pending = 0i128;
    for (_, delta) in accumulator.deltas.range((
        std::ops::Bound::Excluded(accumulator.last_applied_order_key),
        std::ops::Bound::Unbounded,
    )) {
        pending = pending
            .checked_add(delta.delta as i128)
            .ok_or(crate::error::AedbError::Overflow)?;
    }
    Ok(pending)
}

fn effective_accumulator_value(
    accumulator: &AccumulatorData,
) -> Result<i64, crate::error::AedbError> {
    if let Some(err) = &accumulator.projector_error {
        return Err(crate::error::AedbError::Validation(format!(
            "accumulator projector unhealthy: {err}"
        )));
    }
    let pending = unapplied_delta_sum(accumulator)?;
    let combined = (accumulator.value as i128)
        .checked_add(pending)
        .ok_or(crate::error::AedbError::Overflow)?;
    i64::try_from(combined).map_err(|_| crate::error::AedbError::Overflow)
}

fn refresh_exposure_limit_cache(
    accumulator: &mut AccumulatorData,
) -> Result<(), crate::error::AedbError> {
    let effective_value = effective_accumulator_value(accumulator)?;
    accumulator.exposure_limit_cached =
        compute_exposure_limit(effective_value, accumulator.exposure_margin_bps);
    accumulator.exposure_limit_cache_valid = true;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Namespace {
    pub id: NamespaceId,
    pub tables: HashMap<String, TableData>,
    pub kv: KvData,
    #[serde(default)]
    pub accumulators: HashMap<String, AccumulatorData>,
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

fn segment_may_contain_key(meta: &KvSegmentMeta, key: &[u8]) -> bool {
    key >= meta.min_key.as_slice() && key <= meta.max_key.as_slice()
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
        let use_cache = self.primary_index_backend == PrimaryIndexBackend::ArtExperimental;
        let new_cost = row_mem_cost(&row);
        let old_cost = {
            let table = self.table_mut(project_id, scope_id, table_name);
            let old_cost = table.rows.get(&encoded_pk).map(row_mem_cost).unwrap_or(0);
            if old_cost == 0 {
                table.structural_version = commit_seq;
            }
            if use_cache {
                table.rows.insert(encoded_pk.clone(), row.clone());
                table.row_cache.insert(encoded_pk.clone(), row);
            } else {
                table.rows.insert(encoded_pk.clone(), row);
            }
            table.row_versions.insert(encoded_pk.clone(), commit_seq);
            table.pk_hash.insert(encoded_pk.clone(), ());
            if use_cache {
                table.row_versions_cache.insert(encoded_pk, commit_seq);
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
        let removed = {
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
        };
        if let Some(row) = &removed {
            self.mem_bytes = self.mem_bytes.saturating_sub(row_mem_cost(row));
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

    fn accumulator_mut_existing(
        &mut self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
    ) -> Result<&mut AccumulatorData, crate::error::AedbError> {
        self.namespace_mut(NamespaceId::project_scope(project_id, scope_id))
            .accumulators
            .get_mut(accumulator_name)
            .ok_or_else(|| {
                crate::error::AedbError::Validation(format!(
                    "accumulator does not exist: {project_id}.{scope_id}.{accumulator_name}"
                ))
            })
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
        let freed = {
            let namespace = self.namespace_mut(NamespaceId::project_scope(project_id, scope_id));
            namespace
                .accumulators
                .remove(accumulator_name)
                .as_ref()
                .map(accumulator_data_mem_cost)
                .unwrap_or(0)
        };
        self.mem_bytes = self.mem_bytes.saturating_sub(freed);
    }

    #[allow(clippy::too_many_arguments)]
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
        let accumulator = self.accumulator_mut_existing(project_id, scope_id, accumulator_name)?;
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
        let next_pending_delta_sum = accumulator
            .pending_delta_sum
            .checked_add(delta as i128)
            .ok_or(crate::error::AedbError::Overflow)?;
        let next_exposure_limit = if accumulator.projector_error.is_none() {
            (accumulator.value as i128)
                .checked_add(next_pending_delta_sum)
                .and_then(|next_effective| i64::try_from(next_effective).ok())
                .map(|next_effective| {
                    compute_exposure_limit(next_effective, accumulator.exposure_margin_bps)
                })
        } else {
            None
        };
        accumulator.latest_order_key = order_key;
        accumulator.latest_seq = commit_seq;
        accumulator.pending_delta_sum = next_pending_delta_sum;
        accumulator.pending_delta_sum_cache_valid = true;
        if let Some(next_exposure_limit) = next_exposure_limit {
            accumulator.exposure_limit_cached = next_exposure_limit;
            accumulator.exposure_limit_cache_valid = true;
        } else {
            accumulator.exposure_limit_cache_valid = false;
        }
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
        self.mem_bytes = self
            .mem_bytes
            .saturating_add(ACCUMULATOR_DELTA_COST)
            .saturating_add(accumulator_dedupe_cost(dedupe_key.len()));
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
        let accumulator = self.accumulator_mut_existing(project_id, scope_id, accumulator_name)?;
        if !accumulator.exposure_limit_cache_valid || !accumulator.pending_delta_sum_cache_valid {
            refresh_exposure_limit_cache(accumulator)?;
        } else if let Some(err) = &accumulator.projector_error {
            return Err(crate::error::AedbError::Validation(format!(
                "accumulator projector unhealthy: {err}"
            )));
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
        self.mem_bytes = self
            .mem_bytes
            .saturating_add(accumulator_exposure_cost(exposure_id.len()));
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
        let accumulator = self.accumulator_mut_existing(project_id, scope_id, accumulator_name)?;
        if !accumulator.exposure_limit_cache_valid || !accumulator.pending_delta_sum_cache_valid {
            refresh_exposure_limit_cache(accumulator)?;
        } else if let Some(err) = &accumulator.projector_error {
            return Err(crate::error::AedbError::Validation(format!(
                "accumulator projector unhealthy: {err}"
            )));
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
        let mut added: usize = 0;
        for (exposure_id, record) in pending_inserts {
            added = added.saturating_add(accumulator_exposure_cost(exposure_id.len()));
            accumulator.open_exposures.insert(exposure_id, record);
        }
        self.mem_bytes = self.mem_bytes.saturating_add(added);
        Ok(())
    }

    pub fn release_accumulator_exposure(
        &mut self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
        exposure_id: &str,
    ) -> Result<(), crate::error::AedbError> {
        let accumulator = self.accumulator_mut_existing(project_id, scope_id, accumulator_name)?;
        let Some(record) = accumulator.open_exposures.get(exposure_id) else {
            return Err(crate::error::AedbError::Validation(format!(
                "release requested for unknown exposure id: {exposure_id}"
            )));
        };
        if accumulator.total_exposure < record.amount {
            return Err(crate::error::AedbError::Underflow);
        }
        let record_amount = record.amount;
        accumulator.open_exposures.remove(exposure_id);
        accumulator.total_exposure -= record_amount;
        accumulator.exposure_rebuild_required = false;
        self.mem_bytes = self
            .mem_bytes
            .saturating_sub(accumulator_exposure_cost(exposure_id.len()));
        Ok(())
    }

    pub fn rebuild_total_exposure(
        &mut self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
    ) -> Result<(), crate::error::AedbError> {
        let accumulator = self.accumulator_mut_existing(project_id, scope_id, accumulator_name)?;
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

    pub fn accumulator_effective_value(
        &self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
    ) -> Result<Option<i64>, crate::error::AedbError> {
        let Some(accumulator) = self.accumulator(project_id, scope_id, accumulator_name) else {
            return Ok(None);
        };
        effective_accumulator_value(accumulator).map(Some)
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
        let Some(namespace) = self.namespace(&NamespaceId::project_scope(project_id, scope_id))
        else {
            return 0;
        };
        namespace
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
            .or_else(|| {
                self.try_kv_segment_get(&namespace.kv, key)
                    .expect("KV segment read failed")
                    .map(|entry| entry.version)
            })
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
        let visible_max = scan_kv_entries(
            kv,
            start.clone(),
            end.clone(),
            usize::MAX,
            self.value_store.as_deref(),
            self.kv_segment_store.as_deref(),
            true,
        )
        .expect("KV segment scan failed")
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
        visible_max.max(tombstone_max)
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
        for segment in &namespace.kv.segments {
            filenames.insert(segment.filename.clone());
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

    pub fn accumulator_effective_value(
        &self,
        project_id: &str,
        scope_id: &str,
        accumulator_name: &str,
    ) -> Result<Option<i64>, crate::error::AedbError> {
        let Some(accumulator) = self.accumulator(project_id, scope_id, accumulator_name) else {
            return Ok(None);
        };
        effective_accumulator_value(accumulator).map(Some)
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
    const ROW_BASE_OVERHEAD_BYTES: usize = 48;
    const VALUE_SLOT_OVERHEAD_BYTES: usize = 16;

    row.values
        .iter()
        .map(estimate_value_bytes)
        .sum::<usize>()
        .saturating_add(ROW_BASE_OVERHEAD_BYTES)
        .saturating_add(row.values.len().saturating_mul(VALUE_SLOT_OVERHEAD_BYTES))
}

/// Cost contribution of one table row in `estimate_memory_bytes` accounting.
/// Matches the original O(N) walk: row bytes + 32 per-row overhead.
pub(crate) fn row_mem_cost(row: &Row) -> usize {
    estimate_row_bytes(row).saturating_add(32)
}

pub(crate) fn projection_row_mem_cost(row: &Row) -> usize {
    estimate_row_bytes(row)
}

pub(crate) fn kv_entry_cost(key_len: usize, value_len: usize) -> usize {
    key_len.saturating_add(value_len).saturating_add(24)
}

pub(crate) fn kv_inline_entry_cost(key_len: usize, value_len: usize) -> usize {
    if value_len <= INLINE_KV_VALUE_MAX_BYTES {
        small_kv_entry_cost(key_len, value_len)
    } else {
        kv_entry_cost(key_len, value_len)
    }
}

pub(crate) fn persistent_value_ref_resident_cost() -> usize {
    std::mem::size_of::<PersistentValueRef>().saturating_add(8)
}

pub(crate) fn persistent_value_ref_cost(_value_ref: &PersistentValueRef) -> usize {
    persistent_value_ref_resident_cost()
}

pub(crate) fn small_kv_entry_cost(key_len: usize, value_len: usize) -> usize {
    key_len.saturating_add(value_len).saturating_add(16)
}

pub(crate) fn kv_tombstone_cost(key_len: usize) -> usize {
    key_len.saturating_add(16)
}

pub(crate) fn kv_segment_meta_cost(meta: &KvSegmentMeta) -> usize {
    let block_cost = meta
        .blocks
        .iter()
        .map(|block| {
            block
                .first_key
                .len()
                .saturating_add(block.last_key.len())
                .saturating_add(block.sha256_hex.len())
                .saturating_add(64)
        })
        .sum::<usize>();
    meta.filename
        .len()
        .saturating_add(meta.min_key.len())
        .saturating_add(meta.max_key.len())
        .saturating_add(
            meta.bloom_bits
                .len()
                .saturating_mul(std::mem::size_of::<u64>()),
        )
        .saturating_add(block_cost)
        .saturating_add(96)
}

pub(crate) fn accumulator_dedupe_cost(key_len: usize) -> usize {
    key_len.saturating_add(32)
}

pub(crate) const ACCUMULATOR_DELTA_COST: usize = 80;

pub(crate) fn accumulator_exposure_cost(key_len: usize) -> usize {
    key_len.saturating_add(24)
}

pub(crate) fn table_data_mem_cost(t: &TableData) -> usize {
    t.rows.values().map(row_mem_cost).sum::<usize>()
}

pub(crate) fn kv_data_mem_cost(kv: &KvData) -> usize {
    let normal = kv
        .entries
        .iter()
        .map(|(k, v)| kv_entry_cost(k.len(), v.resident_memory_value_len()))
        .sum::<usize>();
    let small = kv
        .small_entries
        .iter()
        .map(|(k, v)| small_kv_entry_cost(k.len(), v.resident_value_len()))
        .sum::<usize>();
    let tombstones = kv
        .segment_tombstones
        .keys()
        .map(|key| kv_tombstone_cost(key.len()))
        .sum::<usize>();
    let segments = kv.segments.iter().map(kv_segment_meta_cost).sum::<usize>();
    normal
        .saturating_add(small)
        .saturating_add(tombstones)
        .saturating_add(segments)
}

pub(crate) fn projection_data_mem_cost(p: &AsyncProjectionData) -> usize {
    p.rows.values().map(projection_row_mem_cost).sum()
}

pub(crate) fn accumulator_data_mem_cost(acc: &AccumulatorData) -> usize {
    acc.dedupe
        .iter()
        .map(|(k, _)| accumulator_dedupe_cost(k.len()))
        .sum::<usize>()
        .saturating_add(acc.deltas.len().saturating_mul(ACCUMULATOR_DELTA_COST))
        .saturating_add(
            acc.open_exposures
                .iter()
                .map(|(k, _)| accumulator_exposure_cost(k.len()))
                .sum::<usize>(),
        )
}

pub(crate) fn namespace_mem_cost(ns: &Namespace) -> usize {
    ns.tables
        .values()
        .map(table_data_mem_cost)
        .sum::<usize>()
        .saturating_add(kv_data_mem_cost(&ns.kv))
        .saturating_add(
            ns.accumulators
                .values()
                .map(accumulator_data_mem_cost)
                .sum(),
        )
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
    use super::{
        Keyspace, KvData, KvEntry, NamespaceId, OpenExposureRecord, SmallKvEntry,
        collect_hot_kv_segment_entries, compact_kv_key, first_segment_position_for_start,
        get_sorted_segment_for_key, hot_kv_resident_cost, kv_entry_cost,
        persistent_value_ref_resident_cost, scan_kv_entries, segment_starts_after_end,
        segments_are_sorted_non_overlapping, small_kv_entry_cost,
    };
    use crate::catalog::types::{Row, Value};
    use crate::config::PrimaryIndexBackend;
    use crate::error::AedbError;
    use crate::storage::encoded_key::EncodedKey;
    use crate::storage::kv_segment::KvSegmentStore;
    use crate::storage::value_store::PersistentValueStore;
    use std::ops::Bound;
    use std::sync::Arc;
    use tempfile::tempdir;

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
        ks.kv_set("p", "app", b"k".to_vec(), b"v".to_vec(), 2)
            .expect("set kv");
        assert!(ks.estimate_memory_bytes() > 0);
    }

    #[test]
    fn small_kv_values_are_compacted_but_materialize_for_reads() {
        let mut ks = Keyspace::default();
        let value = [7u8; 32].to_vec();

        ks.kv_set("p", "app", b"balance".to_vec(), value.clone(), 1)
            .expect("set compact value");

        let stored = ks
            .namespaces
            .get(&NamespaceId::project_scope("p", "app"))
            .expect("namespace")
            .kv
            .small_entries
            .get(&compact_kv_key(b"balance"))
            .expect("stored entry");
        assert_eq!(stored.value.as_slice(), value.as_slice());
        assert_eq!(stored.resident_value_len(), value.len());

        let read = ks.kv_get("p", "app", b"balance").expect("read value");
        assert_eq!(read.value, value);
        assert!(read.value_ref.is_none());
        assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());
    }

    #[test]
    fn compact_kv_overwrite_and_delete_keep_memory_counter_exact() {
        let mut ks = Keyspace::default();
        ks.kv_set("p", "app", b"k".to_vec(), [1u8; 32].to_vec(), 1)
            .expect("set compact");
        let after_insert = ks.mem_bytes;
        assert_eq!(after_insert, ks.recompute_memory_bytes_full());

        ks.kv_set("p", "app", b"k".to_vec(), vec![2u8; 48], 2)
            .expect("overwrite with vec-backed inline value");
        assert!(ks.mem_bytes > after_insert);
        assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());

        assert!(ks.kv_del("p", "app", b"k", 3));
        assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());
        assert!(ks.kv_get("p", "app", b"k").is_none());
    }

    #[test]
    fn disk_kv_segments_keep_cold_keys_off_heap_and_visible() {
        let dir = tempdir().expect("temp dir");
        let segment_store = Arc::new(KvSegmentStore::open(dir.path()).expect("open segment store"));
        let mut ks = Keyspace::default();
        ks.attach_kv_segment_store(Arc::clone(&segment_store));

        for entry_number in 0..128u64 {
            ks.kv_set(
                "p",
                "app",
                format!("k{entry_number:03}").into_bytes(),
                [entry_number as u8; 32].to_vec(),
                entry_number + 1,
            )
            .expect("set kv");
        }
        let before_flush = ks.estimate_memory_bytes();
        let after_flush = ks
            .flush_kv_to_segments_to_memory_target(0)
            .expect("flush to segment");
        assert!(after_flush < before_flush);
        ks.compact_kv_segments().expect("compact generations");
        ks.compact_kv_segments().expect("compact generations");
        let namespace = ks
            .namespace(&NamespaceId::project_scope("p", "app"))
            .expect("namespace");
        assert!(namespace.kv.entries.is_empty());
        assert!(namespace.kv.small_entries.is_empty());
        assert_eq!(namespace.kv.segments.len(), 1);

        assert_eq!(
            ks.kv_get("p", "app", b"k001").expect("k001").value,
            [1u8; 32].to_vec()
        );
        assert_eq!(ks.kv_scan_prefix("p", "app", b"k", 200).len(), 128);
        assert_eq!(ks.kv_version("p", "app", b"k002"), 3);

        assert!(ks.kv_del("p", "app", b"k001", 200));
        assert!(ks.kv_get("p", "app", b"k001").is_none());
        assert_eq!(ks.kv_scan_prefix("p", "app", b"k", 200).len(), 127);

        ks.kv_set("p", "app", b"k001".to_vec(), [3u8; 32].to_vec(), 201)
            .expect("rewrite k001");
        assert_eq!(
            ks.kv_get("p", "app", b"k001")
                .expect("k001 rewritten")
                .value,
            [3u8; 32].to_vec()
        );

        let checkpoint = ks
            .snapshot()
            .materialized_for_checkpoint()
            .expect("materialized checkpoint");
        let checkpoint_namespace = checkpoint
            .namespaces
            .get(&NamespaceId::project_scope("p", "app"))
            .expect("checkpoint namespace");
        assert!(checkpoint_namespace.kv.segments.is_empty());
        assert!(checkpoint_namespace.kv.segment_tombstones.is_empty());
        assert_eq!(checkpoint_namespace.kv.entries.len(), 128);
    }

    #[test]
    fn memory_pressure_flushes_largest_hot_namespace_first() {
        let dir = tempdir().expect("temp dir");
        let segment_store = Arc::new(KvSegmentStore::open(dir.path()).expect("open segment store"));
        let mut ks = Keyspace::default();
        ks.attach_kv_segment_store(Arc::clone(&segment_store));

        for entry_number in 0..128u64 {
            ks.kv_set(
                "p",
                "hot",
                format!("hot:{entry_number:03}").into_bytes(),
                [entry_number as u8; 32].to_vec(),
                entry_number + 1,
            )
            .expect("set hot kv");
        }
        ks.kv_set("p", "cold", b"cold:001".to_vec(), [7u8; 32].to_vec(), 200)
            .expect("set cold kv");
        let hot_cost = hot_kv_resident_cost(
            &ks.namespace(&NamespaceId::project_scope("p", "hot"))
                .expect("hot namespace")
                .kv,
        );
        let target = ks.estimate_memory_bytes().saturating_sub(hot_cost / 2);

        let after_flush = ks
            .flush_kv_to_segments_to_memory_target(target)
            .expect("flush under pressure");

        assert!(after_flush <= target);
        let hot_namespace = ks
            .namespace(&NamespaceId::project_scope("p", "hot"))
            .expect("hot namespace");
        assert!(hot_namespace.kv.entries.is_empty());
        assert!(hot_namespace.kv.small_entries.is_empty());
        assert_eq!(hot_namespace.kv.segments.len(), 1);

        let cold_namespace = ks
            .namespace(&NamespaceId::project_scope("p", "cold"))
            .expect("cold namespace");
        assert!(cold_namespace.kv.segments.is_empty());
        assert!(
            cold_namespace
                .kv
                .small_entries
                .contains_key(&compact_kv_key(b"cold:001"))
        );
        assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());
    }

    #[test]
    fn inline_rewrite_after_segment_delete_survives_refreeze() {
        let dir = tempdir().expect("temp dir");
        let segment_store = Arc::new(KvSegmentStore::open(dir.path()).expect("open segment store"));
        let mut ks = Keyspace::default();
        ks.attach_kv_segment_store(Arc::clone(&segment_store));

        ks.kv_set_inline("p", "app", b"k".to_vec(), b"old".to_vec(), 1);
        ks.flush_kv_to_segments_to_memory_target(0)
            .expect("flush old value");
        assert_eq!(
            ks.kv_get("p", "app", b"k").map(|entry| entry.value),
            Some(b"old".to_vec())
        );

        assert!(ks.kv_del("p", "app", b"k", 2));
        assert!(ks.kv_get("p", "app", b"k").is_none());

        ks.kv_set_inline("p", "app", b"k".to_vec(), b"new".to_vec(), 3);
        assert_eq!(
            ks.kv_get("p", "app", b"k").map(|entry| entry.value),
            Some(b"new".to_vec())
        );
        ks.flush_kv_to_segments_to_memory_target(0)
            .expect("flush rewritten value");

        assert_eq!(
            ks.kv_get("p", "app", b"k").map(|entry| entry.value),
            Some(b"new".to_vec())
        );
        assert_eq!(
            ks.kv_scan_prefix("p", "app", b"k", 10)
                .into_iter()
                .map(|(_, entry)| entry.value)
                .collect::<Vec<_>>(),
            vec![b"new".to_vec()]
        );
        assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());
    }

    #[test]
    fn compacted_segment_prefix_scan_respects_limit_without_full_warmup() {
        let dir = tempdir().expect("temp dir");
        let segment_store = Arc::new(
            KvSegmentStore::open_with_block_cache_bytes(dir.path(), 256 * 1024)
                .expect("open segment store"),
        );
        let mut ks = Keyspace::default();
        ks.attach_kv_segment_store(Arc::clone(&segment_store));

        for number in 0..160u16 {
            ks.kv_set_inline(
                "p",
                "app",
                format!("k{number:03}").into_bytes(),
                vec![number as u8; 16],
                number as u64 + 1,
            );
        }
        ks.flush_kv_to_segments_to_memory_target(0)
            .expect("flush to segment");

        let rows = ks.kv_scan_prefix("p", "app", b"k", 10);

        assert_eq!(rows.len(), 10);
        assert_eq!(rows[0].0, b"k000".to_vec());
        assert_eq!(rows[9].0, b"k009".to_vec());
        let limited_resident_bytes = segment_store.block_cache_resident_bytes();
        assert!(limited_resident_bytes > 0);

        let all_rows = ks.kv_scan_prefix("p", "app", b"k", usize::MAX);
        assert_eq!(all_rows.len(), 160);
        assert!(
            segment_store.block_cache_resident_bytes() > limited_resident_bytes,
            "unlimited scan should warm additional segment blocks"
        );
    }

    #[test]
    fn materializing_single_segment_checkpoint_does_not_warm_block_cache() {
        let dir = tempdir().expect("temp dir");
        let segment_store = Arc::new(
            KvSegmentStore::open_with_block_cache_bytes(dir.path(), 256 * 1024)
                .expect("open segment store"),
        );
        let mut ks = Keyspace::default();
        ks.attach_kv_segment_store(Arc::clone(&segment_store));

        for number in 0..160u16 {
            ks.kv_set_inline(
                "p",
                "app",
                format!("k{number:03}").into_bytes(),
                vec![number as u8; 16],
                number as u64 + 1,
            );
        }
        ks.flush_kv_to_segments_to_memory_target(0)
            .expect("flush to segment");
        assert_eq!(segment_store.block_cache_resident_bytes(), 0);

        let materialized = ks
            .snapshot()
            .materialized_for_checkpoint()
            .expect("materialize checkpoint");

        assert_eq!(segment_store.block_cache_resident_bytes(), 0);
        let namespace = materialized
            .namespaces
            .get(&NamespaceId::project_scope("p", "app"))
            .expect("namespace");
        assert!(namespace.kv.segments.is_empty());
        assert_eq!(
            namespace.kv.entries.len() + namespace.kv.small_entries.len(),
            160
        );
    }

    #[test]
    fn disjoint_segment_prefix_scan_respects_limit_without_full_merge() {
        let dir = tempdir().expect("temp dir");
        let segment_store = Arc::new(
            KvSegmentStore::open_with_block_cache_bytes(dir.path(), 256 * 1024)
                .expect("open segment store"),
        );
        let mut ks = Keyspace::default();
        ks.attach_kv_segment_store(Arc::clone(&segment_store));

        for segment_number in 0..3u16 {
            for entry_number in 0..70u16 {
                let number = segment_number * 100 + entry_number;
                ks.kv_set_inline(
                    "p",
                    "app",
                    format!("k{number:03}").into_bytes(),
                    vec![number as u8; 16],
                    number as u64 + 1,
                );
            }
            ks.flush_kv_to_segments_to_memory_target(0)
                .expect("flush to segment");
        }
        let namespace = ks
            .namespace(&NamespaceId::project_scope("p", "app"))
            .expect("namespace");
        assert_eq!(namespace.kv.segments.len(), 3);

        let cold_rows = scan_kv_entries(
            &namespace.kv,
            Bound::Included(b"k100".to_vec()),
            Bound::Excluded(b"k200".to_vec()),
            10,
            None,
            Some(segment_store.as_ref()),
            false,
        )
        .expect("cold scan");
        assert_eq!(cold_rows.len(), 10);
        assert_eq!(cold_rows[0].0, b"k100".to_vec());
        assert_eq!(cold_rows[9].0, b"k109".to_vec());
        assert_eq!(segment_store.block_cache_resident_bytes(), 0);

        let rows = ks.kv_scan_prefix("p", "app", b"k", 10);

        assert_eq!(rows.len(), 10);
        assert_eq!(rows[0].0, b"k000".to_vec());
        assert_eq!(rows[9].0, b"k009".to_vec());
        let limited_resident_bytes = segment_store.block_cache_resident_bytes();
        assert!(limited_resident_bytes > 0);

        let all_rows = ks.kv_scan_prefix("p", "app", b"k", usize::MAX);
        assert_eq!(all_rows.len(), 210);
        assert_eq!(all_rows[70].0, b"k100".to_vec());
        assert_eq!(all_rows[140].0, b"k200".to_vec());
        assert!(
            segment_store.block_cache_resident_bytes() > limited_resident_bytes,
            "full scan should warm blocks from later disjoint segments"
        );
    }

    #[test]
    fn disjoint_segment_point_get_finds_only_possible_range() {
        let dir = tempdir().expect("temp dir");
        let segment_store = Arc::new(KvSegmentStore::open(dir.path()).expect("open segment store"));
        let mut ks = Keyspace::default();
        ks.attach_kv_segment_store(Arc::clone(&segment_store));

        for segment_number in 0..3u16 {
            for entry_number in 0..4u16 {
                let number = segment_number * 100 + entry_number;
                ks.kv_set_inline(
                    "p",
                    "app",
                    format!("k{number:03}").into_bytes(),
                    vec![number as u8; 4],
                    number as u64 + 1,
                );
            }
            ks.flush_kv_to_segments_to_memory_target(0)
                .expect("flush to segment");
        }

        assert_eq!(
            ks.kv_get("p", "app", b"k000").map(|entry| entry.value),
            Some(vec![0; 4])
        );
        assert_eq!(
            ks.kv_get("p", "app", b"k101").map(|entry| entry.value),
            Some(vec![101; 4])
        );
        assert_eq!(
            ks.kv_get("p", "app", b"k203").map(|entry| entry.value),
            Some(vec![203; 4])
        );
        assert!(ks.kv_get("p", "app", b"k050").is_none());
    }

    #[test]
    fn sorted_segment_range_helpers_detect_common_append_layout() {
        let dir = tempdir().expect("temp dir");
        let segment_store = Arc::new(KvSegmentStore::open(dir.path()).expect("open segment store"));
        let mut ks = Keyspace::default();
        ks.attach_kv_segment_store(Arc::clone(&segment_store));

        for segment_number in 0..3u16 {
            for entry_number in 0..4u16 {
                let number = segment_number * 100 + entry_number;
                ks.kv_set_inline(
                    "p",
                    "app",
                    format!("k{number:03}").into_bytes(),
                    vec![number as u8; 4],
                    number as u64 + 1,
                );
            }
            ks.flush_kv_to_segments_to_memory_target(0)
                .expect("flush to segment");
        }

        let namespace = ks
            .namespace(&NamespaceId::project_scope("p", "app"))
            .expect("namespace");
        assert!(segments_are_sorted_non_overlapping(&namespace.kv.segments));
        assert_eq!(
            get_sorted_segment_for_key(&namespace.kv.segments, b"k101")
                .map(|segment| segment.min_key.clone()),
            Some(b"k100".to_vec())
        );
        assert!(get_sorted_segment_for_key(&namespace.kv.segments, b"k050").is_none());
        assert_eq!(
            first_segment_position_for_start(
                &namespace.kv.segments,
                &Bound::Included(b"k101".to_vec())
            ),
            1
        );
        assert_eq!(
            first_segment_position_for_start(
                &namespace.kv.segments,
                &Bound::Excluded(b"k103".to_vec())
            ),
            2
        );
        assert!(segment_starts_after_end(
            &namespace.kv.segments[2],
            &Bound::Excluded(b"k200".to_vec())
        ));
    }

    #[test]
    fn hot_kv_segment_collection_merges_sorted_maps_without_resort() {
        let mut kv = KvData::default();
        kv.entries.insert(
            b"k001".to_vec(),
            KvEntry::inline(b"normal-1".to_vec(), 1, 1),
        );
        kv.entries.insert(
            b"k003".to_vec(),
            KvEntry::inline(b"normal-3".to_vec(), 3, 3),
        );
        kv.small_entries.insert(
            compact_kv_key(b"k000"),
            SmallKvEntry::new(b"small-0", 10, 10).expect("small entry"),
        );
        kv.small_entries.insert(
            compact_kv_key(b"k002"),
            SmallKvEntry::new(b"small-2", 12, 12).expect("small entry"),
        );
        kv.small_entries.insert(
            compact_kv_key(b"k003"),
            SmallKvEntry::new(b"small-3", 13, 13).expect("small entry"),
        );

        let (entries, resident_cost) = collect_hot_kv_segment_entries(&kv);

        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.key.clone())
                .collect::<Vec<_>>(),
            vec![
                b"k000".to_vec(),
                b"k001".to_vec(),
                b"k002".to_vec(),
                b"k003".to_vec()
            ]
        );
        assert_eq!(entries[3].entry.value, b"small-3".to_vec());
        assert_eq!(
            resident_cost,
            kv.entries
                .iter()
                .map(|(key, entry)| kv_entry_cost(key.len(), entry.resident_memory_value_len()))
                .chain(kv.small_entries.iter().map(|(key, entry)| {
                    small_kv_entry_cost(key.len(), entry.resident_value_len())
                }))
                .sum::<usize>()
        );
    }

    #[test]
    fn overlapping_segment_point_get_preserves_newest_value() {
        let dir = tempdir().expect("temp dir");
        let segment_store = Arc::new(KvSegmentStore::open(dir.path()).expect("open segment store"));
        let mut ks = Keyspace::default();
        ks.attach_kv_segment_store(Arc::clone(&segment_store));

        ks.kv_set_inline("p", "app", b"k".to_vec(), b"old".to_vec(), 1);
        ks.flush_kv_to_segments_to_memory_target(0)
            .expect("flush old value");
        ks.kv_set_inline("p", "app", b"k".to_vec(), b"new".to_vec(), 2);
        ks.flush_kv_to_segments_to_memory_target(0)
            .expect("flush new value");

        let namespace = ks
            .namespace(&NamespaceId::project_scope("p", "app"))
            .expect("namespace");
        assert_eq!(namespace.kv.segments.len(), 2);
        assert_eq!(
            ks.kv_get("p", "app", b"k").map(|entry| entry.value),
            Some(b"new".to_vec())
        );
        assert_eq!(ks.kv_version("p", "app", b"k"), 2);
    }

    #[test]
    fn kv_segment_compaction_preserves_latest_values_and_tombstones() {
        let dir = tempdir().expect("temp dir");
        let segment_store = Arc::new(KvSegmentStore::open(dir.path()).expect("open segment store"));
        let mut ks = Keyspace::default();
        ks.attach_kv_segment_store(Arc::clone(&segment_store));

        for generation_number in 0..6u64 {
            ks.kv_set(
                "p",
                "app",
                b"shared".to_vec(),
                [generation_number as u8; 32].to_vec(),
                generation_number + 1,
            )
            .expect("set shared");
            ks.kv_set(
                "p",
                "app",
                format!("unique-{generation_number:02}").into_bytes(),
                [generation_number as u8; 32].to_vec(),
                generation_number + 10,
            )
            .expect("set unique");
            ks.flush_kv_to_segments_to_memory_target(0)
                .expect("flush generation");
        }

        ks.compact_kv_segments().expect("compact generations");
        let namespace = ks
            .namespace(&NamespaceId::project_scope("p", "app"))
            .expect("namespace");
        assert_eq!(
            namespace.kv.segments.len(),
            1,
            "flush should compact segment fanout"
        );
        assert_eq!(
            ks.kv_get("p", "app", b"shared").expect("shared").value,
            [5u8; 32].to_vec()
        );

        assert!(ks.kv_del("p", "app", b"unique-01", 100));
        ks.compact_kv_segments().expect("compact tombstone");
        assert!(ks.kv_get("p", "app", b"unique-01").is_none());
        assert_eq!(ks.kv_scan_prefix("p", "app", b"unique-", 10).len(), 5);

        let namespace = ks
            .namespace(&NamespaceId::project_scope("p", "app"))
            .expect("namespace");
        assert!(namespace.kv.segment_tombstones.is_empty());
        assert_eq!(namespace.kv.segments.len(), 1);

        let referenced = ks.kv_segment_filenames();
        let before_reclaim = segment_store
            .list_segment_filenames()
            .expect("list segments before reclaim");
        assert!(
            before_reclaim.len() > referenced.len(),
            "compaction should leave old segment files until explicit GC"
        );
        let reclaimed = segment_store
            .reclaim_unreferenced_segments(&referenced)
            .expect("reclaim unreferenced segments");
        assert!(reclaimed > 0);
        let after_reclaim = segment_store
            .list_segment_filenames()
            .expect("list segments after reclaim");
        assert_eq!(after_reclaim.len(), referenced.len());
        for filename in after_reclaim {
            assert!(referenced.contains(&filename));
        }
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
        ks.kv_set("p", "app", b"ob:a:1".to_vec(), b"v1".to_vec(), 1)
            .expect("set a1");
        ks.kv_set("p", "app", b"ob:a:2".to_vec(), b"v2".to_vec(), 2)
            .expect("set a2");
        ks.kv_set("p", "app", b"ob:b:1".to_vec(), b"v3".to_vec(), 3)
            .expect("set b1");
        ks.kv_set("p", "app", b"zz".to_vec(), b"v4".to_vec(), 4)
            .expect("set zz");

        let rows = ks.kv_scan_prefix("p", "app", b"ob:a:", 10);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, b"ob:a:1".to_vec());
        assert_eq!(rows[1].0, b"ob:a:2".to_vec());

        let refs = ks.kv_scan_prefix_ref("p", "app", b"ob:", 10);
        assert_eq!(refs.len(), 3);
        assert!(refs.iter().all(|(k, _)| k.starts_with(b"ob:")));
    }

    #[test]
    fn materialized_checkpoint_hydrates_spilled_kv_without_mutating_source() {
        let dir = tempdir().expect("temp");
        let store = Arc::new(
            PersistentValueStore::open_with_hot_cache_bytes(dir.path(), 0).expect("open store"),
        );
        let mut ks = Keyspace::default();
        ks.attach_persistent_value_store(store, 4)
            .expect("attach value store");
        ks.kv_set("p", "app", b"big".to_vec(), b"large-value".to_vec(), 1)
            .expect("set spilled value");
        ks.kv_set("p", "app", b"tiny".to_vec(), b"tiny".to_vec(), 2)
            .expect("set inline value");

        let snapshot = ks.snapshot();
        let source_big = snapshot
            .namespaces
            .get(&NamespaceId::project_scope("p", "app"))
            .expect("namespace")
            .kv
            .entries
            .get(&b"big".to_vec())
            .expect("source big entry");
        assert!(source_big.value_ref.is_some());
        assert!(source_big.value.is_empty());

        let materialized = snapshot
            .materialized_for_checkpoint()
            .expect("materialize checkpoint");
        let materialized_namespace = materialized
            .namespaces
            .get(&NamespaceId::project_scope("p", "app"))
            .expect("materialized namespace");
        let materialized_big = materialized_namespace
            .kv
            .entries
            .get(&b"big".to_vec())
            .expect("materialized big entry");
        assert_eq!(materialized_big.value, b"large-value");
        assert!(materialized_big.value_ref.is_none());
        assert!(materialized.value_store.is_none());
        assert_eq!(
            materialized.mem_bytes,
            materialized.recompute_memory_bytes_full()
        );

        let source_big_after = snapshot
            .namespaces
            .get(&NamespaceId::project_scope("p", "app"))
            .expect("source namespace")
            .kv
            .entries
            .get(&b"big".to_vec())
            .expect("source big entry after materialize");
        assert!(source_big_after.value_ref.is_some());
        assert!(source_big_after.value.is_empty());
    }

    #[test]
    fn target_spill_counts_persistent_value_refs_and_skips_tiny_values() {
        let dir = tempdir().expect("temp");
        let store = Arc::new(
            PersistentValueStore::open_with_hot_cache_bytes(dir.path(), 0).expect("open store"),
        );
        let mut ks = Keyspace::default();
        ks.attach_persistent_value_store(store, usize::MAX)
            .expect("attach value store");
        let large_value = vec![0xAB; 128];
        let tiny_value = [0xCD; 32].to_vec();

        ks.kv_set("p", "app", b"large".to_vec(), large_value.clone(), 1)
            .expect("set large");
        ks.kv_set("p", "app", b"tiny".to_vec(), tiny_value.clone(), 2)
            .expect("set tiny");
        let before_spill = ks.mem_bytes;

        let after_spill = ks.spill_kv_values_to_memory_target(0).expect("spill");

        assert_eq!(after_spill, ks.mem_bytes);
        assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());
        assert!(ks.mem_bytes < before_spill);

        let namespace = ks
            .namespaces
            .get(&NamespaceId::project_scope("p", "app"))
            .expect("namespace");
        let large_entry = namespace
            .kv
            .entries
            .get(&b"large".to_vec())
            .expect("large entry");
        assert!(large_entry.value.is_empty());
        assert!(large_entry.value_ref.is_some());
        assert_eq!(
            large_entry.resident_memory_value_len(),
            persistent_value_ref_resident_cost()
        );
        assert!(
            namespace
                .kv
                .small_entries
                .contains_key(&compact_kv_key(b"tiny")),
            "spilling a tiny compact value would increase resident memory"
        );

        assert_eq!(
            ks.try_kv_get("p", "app", b"large")
                .expect("read large")
                .expect("large value")
                .value,
            large_value
        );
        assert_eq!(
            ks.try_kv_get("p", "app", b"tiny")
                .expect("read tiny")
                .expect("tiny value")
                .value,
            tiny_value
        );
    }

    #[test]
    fn accumulator_release_underflow_does_not_drop_open_exposure() {
        let mut ks = Keyspace::default();
        ks.create_accumulator("p", "app", "house", 1_000);
        let acc = ks.accumulator_mut("p", "app", "house");
        acc.total_exposure = 5;
        acc.open_exposures.insert(
            "hand-1".into(),
            OpenExposureRecord {
                amount: 10,
                opened_at_seq: 1,
            },
        );

        let err = ks
            .release_accumulator_exposure("p", "app", "house", "hand-1")
            .expect_err("corrupt exposure totals must fail");
        assert!(matches!(err, AedbError::Underflow));

        let acc = ks.accumulator("p", "app", "house").expect("accumulator");
        assert_eq!(acc.total_exposure, 5);
        assert!(acc.open_exposures.contains_key("hand-1"));
    }

    #[test]
    fn accumulator_append_rejects_pending_sum_overflow() {
        let mut ks = Keyspace::default();
        ks.create_accumulator("p", "app", "house", 1_000);
        let acc = ks.accumulator_mut("p", "app", "house");
        acc.latest_order_key = 7;
        acc.latest_seq = 9;
        acc.pending_delta_sum = i128::MAX;
        acc.pending_delta_sum_cache_valid = true;

        let err = ks
            .append_accumulator_delta("p", "app", "house", 1, "tx-1", 8, 1)
            .expect_err("pending sum overflow must not saturate");
        assert!(matches!(err, AedbError::Overflow));

        let acc = ks.accumulator("p", "app", "house").expect("accumulator");
        assert_eq!(acc.latest_order_key, 7);
        assert_eq!(acc.latest_seq, 9);
        assert_eq!(acc.pending_delta_sum, i128::MAX);
        assert!(acc.deltas.is_empty());
        assert!(acc.dedupe.is_empty());
    }

    #[test]
    fn mem_bytes_running_counter_matches_full_walk() {
        // Exercise every tracked mutation site and assert the running counter
        // stays in sync with a fresh O(N) recompute.
        let mut ks = Keyspace::default();
        assert_eq!(ks.mem_bytes, 0);
        assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());

        // Table inserts.
        for i in 0..50_i64 {
            ks.upsert_row(
                "p",
                "app",
                "users",
                vec![Value::Integer(i)],
                Row {
                    values: vec![Value::Integer(i), Value::Text(format!("name_{i}").into())],
                },
                i as u64 + 1,
            );
        }
        assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());

        // Update existing row (replaces, delta = new - old).
        ks.upsert_row(
            "p",
            "app",
            "users",
            vec![Value::Integer(0)],
            Row {
                values: vec![
                    Value::Integer(0),
                    Value::Text("a-much-longer-name-than-original".into()),
                ],
            },
            100,
        );
        assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());

        // Delete some rows.
        for i in 0..10_i64 {
            ks.delete_row("p", "app", "users", &[Value::Integer(i)], 200);
        }
        assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());

        // KV mutations.
        for i in 0..30_u64 {
            ks.kv_set(
                "p",
                "app",
                format!("key:{i}").into_bytes(),
                vec![0xAB; 64],
                i + 1,
            )
            .expect("set kv");
        }
        assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());

        // KV update (replaces).
        ks.kv_set("p", "app", b"key:0".to_vec(), vec![0xCD; 256], 50)
            .expect("update kv");
        assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());

        // KV delete.
        for i in 0..5_u64 {
            ks.kv_del("p", "app", format!("key:{i}").as_bytes(), 60);
        }
        assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());

        // Accumulator: deltas + dedupe.
        ks.create_accumulator("p", "app", "house", 1_000);
        for i in 0..20_u64 {
            ks.append_accumulator_delta(
                "p",
                "app",
                "house",
                10,
                &format!("dedupe-{i}"),
                i + 1,
                i + 1,
            )
            .expect("append");
        }
        assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());

        // Exposures.
        for i in 0..5_u64 {
            ks.expose_accumulator("p", "app", "house", 5, &format!("hand-{i}"), 100 + i)
                .expect("expose");
        }
        assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());

        // Release some.
        for i in 0..3_u64 {
            ks.release_accumulator_exposure("p", "app", "house", &format!("hand-{i}"))
                .expect("release");
        }
        assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());

        // Drops.
        ks.drop_accumulator("p", "app", "house");
        assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());

        ks.drop_table("p", "app", "users");
        assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());

        ks.drop_scope("p", "app");
        assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());

        ks.drop_project("p");
        assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());
        assert_eq!(ks.mem_bytes, 0);
    }

    #[test]
    fn kv_set_many_same_namespace_updates_entries_and_memory() {
        let mut ks = Keyspace::default();
        let entries = [
            (b"a".to_vec(), b"one".to_vec()),
            (b"b".to_vec(), b"two".to_vec()),
        ];
        ks.kv_set_many_same_namespace("p", "app", entries.iter().map(|(k, v)| (k, v)), 7)
            .expect("batch set");
        assert_eq!(
            ks.kv_get("p", "app", b"a").map(|entry| entry.value),
            Some(b"one".to_vec())
        );
        assert_eq!(
            ks.kv_get("p", "app", b"b").map(|entry| entry.value),
            Some(b"two".to_vec())
        );
        assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());

        let overwrite = [(b"a".to_vec(), b"three".to_vec())];
        ks.kv_set_many_same_namespace("p", "app", overwrite.iter().map(|(k, v)| (k, v)), 8)
            .expect("batch overwrite");
        let overwritten = ks.kv_get("p", "app", b"a").expect("overwritten");
        assert_eq!(overwritten.value, b"three".to_vec());
        assert_eq!(overwritten.created_at, 7);
        assert_eq!(overwritten.version, 8);
        assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());
    }

    #[test]
    fn kv_set_many_same_namespace_spills_large_values_and_keeps_them_hot() {
        let dir = tempdir().expect("temp");
        let store = Arc::new(
            PersistentValueStore::open_with_hot_cache_bytes(dir.path(), 128).expect("open store"),
        );
        let mut ks = Keyspace::default();
        ks.attach_persistent_value_store(Arc::clone(&store), 4)
            .expect("attach");
        let entries = [
            (b"big-a".to_vec(), b"large-value-a".to_vec()),
            (b"big-b".to_vec(), b"large-value-b".to_vec()),
        ];

        ks.kv_set_many_same_namespace("p", "app", entries.iter().map(|(k, v)| (k, v)), 11)
            .expect("batch set");

        let namespace = ks
            .namespaces
            .get(&NamespaceId::project_scope("p", "app"))
            .expect("namespace");
        for (key, value) in &entries {
            let entry = namespace.kv.entries.get(key).expect("entry");
            entry.value_ref.as_ref().expect("spilled value ref");
            assert!(entry.value.is_empty());
            assert_eq!(
                ks.try_kv_get("p", "app", key)
                    .expect("read")
                    .expect("value")
                    .value,
                *value
            );
        }
        assert!(
            store.hot_cache_resident_bytes() > 0,
            "spilled batch values should be hot immediately after write"
        );
        assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());
    }

    #[test]
    fn refresh_mem_bytes_recovers_from_external_construction() {
        // Simulates the checkpoint-load path: a Keyspace built from external
        // data with `mem_bytes` defaulting to 0 must be reseedable.
        let mut ks = Keyspace::default();
        for i in 0..10_i64 {
            ks.upsert_row(
                "p",
                "app",
                "t",
                vec![Value::Integer(i)],
                Row {
                    values: vec![Value::Integer(i)],
                },
                i as u64 + 1,
            );
        }
        let expected = ks.mem_bytes;
        ks.mem_bytes = 0;
        ks.refresh_mem_bytes();
        assert_eq!(ks.mem_bytes, expected);
    }
}
