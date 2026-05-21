use super::*;

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
