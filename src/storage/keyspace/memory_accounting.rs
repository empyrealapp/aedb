use super::{
    AsyncProjectionData, INLINE_KV_VALUE_MAX_BYTES, KvData, Namespace, SecondaryIndex,
    SecondaryIndexStore, StoredRow, TableData,
};
use crate::catalog::types::{Row, Value};
use crate::storage::encoded_key::EncodedKey;
use crate::storage::kv_segment::KvSegmentMeta;
use crate::storage::value_store::PersistentValueRef;

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
///
/// Tables keep persistent map entries for rows and per-row versions, with
/// structural overhead that is not represented by row payload bytes alone.
/// The previous payload-only estimate was fast after the running-counter work,
/// but it undercounted resident memory for table-heavy workloads.
pub(crate) fn row_mem_cost(row: &Row) -> usize {
    const TABLE_ROW_RESIDENT_FACTOR: usize = 2;

    estimate_row_bytes(row)
        .saturating_add(32)
        .saturating_mul(TABLE_ROW_RESIDENT_FACTOR)
}

/// Resident cost of a stored row. A spilled row keeps only a value-ref stub in
/// memory (plus its key entry overhead), so it is accounted like a spilled KV
/// value rather than its full payload.
pub(crate) fn stored_row_mem_cost(stored: &StoredRow) -> usize {
    if let Some(row) = stored.resident() {
        row_mem_cost(row)
    } else if let Some(value_ref) = stored.value_ref() {
        persistent_value_ref_cost(value_ref)
    } else {
        0
    }
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

/// Resident cost of one posting in a secondary index: the primary key bytes a
/// `BTree`/`Hash`/`UniqueHash` store holds for an index entry, plus structural
/// overhead for the persistent-map/set node.
///
/// `pub(crate)` so the incremental delta math in [`crate::storage::index`] can
/// stay in lockstep with the full recompute here; the two must always agree to
/// preserve the `mem_bytes == recompute_memory_bytes_full()` invariant.
pub(crate) const INDEX_POSTING_OVERHEAD_BYTES: usize = 32;

/// Resident cost of one distinct indexed value in a secondary index: the index
/// key bytes plus the per-value map node and (for multi-valued stores) the
/// `OrdSet` header.
pub(crate) const INDEX_VALUE_OVERHEAD_BYTES: usize = 32;

fn index_postings_set_cost(values: &im::OrdSet<EncodedKey>) -> usize {
    values
        .iter()
        .map(|pk| {
            pk.as_slice()
                .len()
                .saturating_add(INDEX_POSTING_OVERHEAD_BYTES)
        })
        .sum()
}

/// Resident memory held by a single secondary index. Index postings grow 1:1
/// with indexed rows, so they must be counted against the keyspace budget
/// alongside the rows themselves; otherwise an index-heavy table can grow the
/// resident footprint without the spill cascade ever observing the pressure.
/// Resident cost of just the in-memory posting store (excluding cold-tier
/// segment metadata and tombstones). This is the amount freed when the resident
/// postings are evicted to a segment.
pub(crate) fn secondary_index_store_cost(index: &SecondaryIndex) -> usize {
    match &index.store {
        SecondaryIndexStore::BTree(map) => map
            .iter()
            .map(|(value, pks)| {
                value
                    .as_slice()
                    .len()
                    .saturating_add(INDEX_VALUE_OVERHEAD_BYTES)
                    .saturating_add(index_postings_set_cost(pks))
            })
            .sum(),
        SecondaryIndexStore::Hash(map) => map
            .iter()
            .map(|(value, pks)| {
                value
                    .as_slice()
                    .len()
                    .saturating_add(INDEX_VALUE_OVERHEAD_BYTES)
                    .saturating_add(index_postings_set_cost(pks))
            })
            .sum(),
        SecondaryIndexStore::UniqueHash(map) => map
            .iter()
            .map(|(value, pk)| {
                value
                    .as_slice()
                    .len()
                    .saturating_add(pk.as_slice().len())
                    .saturating_add(INDEX_VALUE_OVERHEAD_BYTES)
            })
            .sum(),
    }
}

/// Total resident memory held by a single secondary index: the in-memory
/// posting store plus runtime-only cold-tier bookkeeping (segment metadata that
/// stays resident even when the postings are evicted, and tombstones for
/// postings that live only in segments).
pub(crate) fn secondary_index_mem_cost(index: &SecondaryIndex) -> usize {
    let segment_cost: usize = index.segments.iter().map(kv_segment_meta_cost).sum();
    let tombstone_cost: usize = index
        .segment_tombstones
        .keys()
        .map(|key| key.as_slice().len().saturating_add(16))
        .sum();
    secondary_index_store_cost(index)
        .saturating_add(segment_cost)
        .saturating_add(tombstone_cost)
}

pub(crate) fn table_data_mem_cost(t: &TableData) -> usize {
    let rows = t.rows.values().map(stored_row_mem_cost).sum::<usize>();
    // Secondary index postings grow 1:1 with indexed rows and are fully
    // resident, so they must be counted against the keyspace budget alongside
    // the rows. `row_versions`/`row_tombstones`/`row_segments` are deliberately
    // excluded here: versions are inline on new rows, and the cold-tier
    // bookkeeping is key-only and tracked separately by the eviction paths.
    let indexes = t
        .indexes
        .values()
        .map(secondary_index_mem_cost)
        .sum::<usize>();
    rows.saturating_add(indexes)
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

pub(crate) fn namespace_mem_cost(ns: &Namespace) -> usize {
    ns.tables
        .values()
        .map(table_data_mem_cost)
        .sum::<usize>()
        .saturating_add(kv_data_mem_cost(&ns.kv))
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
