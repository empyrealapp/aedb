use crate::catalog::schema::TableSchema;
use crate::catalog::types::{Row, Value};
use crate::error::AedbError;
use crate::query::operators::{compile_expr, eval_compiled_expr_public};
use crate::storage::encoded_key::{EncodedKey, prefix_successor};
use crate::storage::keyspace::memory_accounting::{
    INDEX_POSTING_OVERHEAD_BYTES, INDEX_VALUE_OVERHEAD_BYTES,
};
use crate::storage::keyspace::{SecondaryIndex, SecondaryIndexStore, decode_index_segment_entry};
use crate::storage::kv_segment::KvSegmentStore;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

/// Resident byte cost contributed by one distinct indexed value (the index key
/// plus per-value structural overhead).
fn index_value_cost(key: &EncodedKey) -> i64 {
    (key.as_slice().len() + INDEX_VALUE_OVERHEAD_BYTES) as i64
}

/// Resident byte cost contributed by one posting (a primary key plus per-posting
/// structural overhead) in a multi-valued index store.
fn index_posting_cost(pk: &EncodedKey) -> i64 {
    (pk.as_slice().len() + INDEX_POSTING_OVERHEAD_BYTES) as i64
}

impl SecondaryIndex {
    /// Inserts a posting, returning the signed change in this index's resident
    /// memory cost so callers can keep the keyspace `mem_bytes` running counter
    /// in lockstep with [`secondary_index_mem_cost`]. The math here must mirror
    /// that function exactly.
    ///
    /// [`secondary_index_mem_cost`]: crate::storage::keyspace::memory_accounting::secondary_index_mem_cost
    pub fn insert(&mut self, key: EncodedKey, pk: EncodedKey) -> i64 {
        match &mut self.store {
            SecondaryIndexStore::BTree(entries) => {
                let mut delta = 0i64;
                let mut pks = entries.get(&key).cloned().unwrap_or_else(|| {
                    delta += index_value_cost(&key);
                    Default::default()
                });
                if !pks.contains(&pk) {
                    delta += index_posting_cost(&pk);
                }
                pks.insert(pk);
                entries.insert(key, pks);
                delta
            }
            SecondaryIndexStore::Hash(entries) => {
                let mut delta = 0i64;
                let mut pks = entries.get(&key).cloned().unwrap_or_else(|| {
                    delta += index_value_cost(&key);
                    Default::default()
                });
                if !pks.contains(&pk) {
                    delta += index_posting_cost(&pk);
                }
                pks.insert(pk);
                entries.insert(key, pks);
                delta
            }
            SecondaryIndexStore::UniqueHash(entries) => {
                let new_pk_len = pk.as_slice().len() as i64;
                let key_value_cost = index_value_cost(&key);
                match entries.insert(key, pk) {
                    None => key_value_cost + new_pk_len,
                    Some(old_pk) => new_pk_len - old_pk.as_slice().len() as i64,
                }
            }
        }
    }

    /// Removes a posting, returning the signed change in this index's resident
    /// memory cost (always `<= 0`). Mirrors [`insert`] / [`secondary_index_mem_cost`].
    ///
    /// [`insert`]: SecondaryIndex::insert
    /// [`secondary_index_mem_cost`]: crate::storage::keyspace::memory_accounting::secondary_index_mem_cost
    pub fn remove(&mut self, key: &EncodedKey, pk: &EncodedKey) -> i64 {
        match &mut self.store {
            SecondaryIndexStore::BTree(entries) => {
                let Some(mut pks) = entries.get(key).cloned() else {
                    return 0;
                };
                if !pks.contains(pk) {
                    return 0;
                }
                let mut delta = -index_posting_cost(pk);
                pks.remove(pk);
                if pks.is_empty() {
                    entries.remove(key);
                    delta -= index_value_cost(key);
                } else {
                    entries.insert(key.clone(), pks);
                }
                delta
            }
            SecondaryIndexStore::Hash(entries) => {
                let Some(mut pks) = entries.get(key).cloned() else {
                    return 0;
                };
                if !pks.contains(pk) {
                    return 0;
                }
                let mut delta = -index_posting_cost(pk);
                pks.remove(pk);
                if pks.is_empty() {
                    entries.remove(key);
                    delta -= index_value_cost(key);
                } else {
                    entries.insert(key.clone(), pks);
                }
                delta
            }
            SecondaryIndexStore::UniqueHash(entries) => match entries.remove(key) {
                Some(old_pk) => -(index_value_cost(key) + old_pk.as_slice().len() as i64),
                None => 0,
            },
        }
    }

    /// Enumerates every resident `(index_value, primary_key)` posting in this
    /// index's in-memory store, in `(value, pk)` order for the ordered stores.
    /// Used by cold-tier eviction to serialize the hot postings to a segment.
    pub fn resident_postings(&self) -> Vec<(EncodedKey, EncodedKey)> {
        match &self.store {
            SecondaryIndexStore::BTree(entries) => entries
                .iter()
                .flat_map(|(value, pks)| pks.iter().map(move |pk| (value.clone(), pk.clone())))
                .collect(),
            SecondaryIndexStore::Hash(entries) => entries
                .iter()
                .flat_map(|(value, pks)| pks.iter().map(move |pk| (value.clone(), pk.clone())))
                .collect(),
            SecondaryIndexStore::UniqueHash(entries) => entries
                .iter()
                .map(|(value, pk)| (value.clone(), pk.clone()))
                .collect(),
        }
    }

    /// Whether the resident store currently holds any postings.
    pub fn resident_store_is_empty(&self) -> bool {
        match &self.store {
            SecondaryIndexStore::BTree(entries) => entries.is_empty(),
            SecondaryIndexStore::Hash(entries) => entries.is_empty(),
            SecondaryIndexStore::UniqueHash(entries) => entries.is_empty(),
        }
    }

    /// Empties the resident store, preserving its kind (BTree/Hash/UniqueHash).
    /// Segments and tombstones are left untouched.
    pub fn clear_resident_store(&mut self) {
        match &mut self.store {
            SecondaryIndexStore::BTree(entries) => *entries = im::OrdMap::new(),
            SecondaryIndexStore::Hash(entries) => *entries = im::HashMap::new(),
            SecondaryIndexStore::UniqueHash(entries) => *entries = im::HashMap::new(),
        }
    }

    /// Whether this index has any cold-tier state (evicted segments).
    pub fn has_cold_segments(&self) -> bool {
        !self.segments.is_empty()
    }

    pub fn scan_eq(&self, key: &EncodedKey) -> Vec<EncodedKey> {
        self.scan_eq_limit(key, usize::MAX)
    }

    pub fn scan_eq_limit(&self, key: &EncodedKey, limit: usize) -> Vec<EncodedKey> {
        if limit == 0 {
            return Vec::new();
        }
        match &self.store {
            SecondaryIndexStore::BTree(entries) => entries
                .get(key)
                .map(|pks| pks.iter().take(limit).cloned().collect())
                .unwrap_or_default(),
            SecondaryIndexStore::Hash(entries) => entries
                .get(key)
                .map(|pks| pks.iter().take(limit).cloned().collect())
                .unwrap_or_default(),
            SecondaryIndexStore::UniqueHash(entries) => entries
                .get(key)
                .map(|pk| vec![pk.clone()])
                .unwrap_or_default(),
        }
    }

    pub fn scan_range(&self, start: Bound<EncodedKey>, end: Bound<EncodedKey>) -> Vec<EncodedKey> {
        self.scan_range_limit(start, end, usize::MAX)
    }

    pub fn scan_range_limit(
        &self,
        start: Bound<EncodedKey>,
        end: Bound<EncodedKey>,
        limit: usize,
    ) -> Vec<EncodedKey> {
        self.scan_range_window_ordered(start, end, 0, limit, false)
    }

    pub fn scan_range_window_ordered(
        &self,
        start: Bound<EncodedKey>,
        end: Bound<EncodedKey>,
        offset: usize,
        limit: usize,
        reverse: bool,
    ) -> Vec<EncodedKey> {
        if limit == 0 {
            return Vec::new();
        }
        match &self.store {
            SecondaryIndexStore::BTree(entries) => {
                let mut out = Vec::with_capacity(index_scan_capacity_hint(limit));
                let mut skipped = 0usize;
                append_window_entries(
                    entries.range((start, end)),
                    offset,
                    limit,
                    reverse,
                    &mut skipped,
                    &mut out,
                );
                out
            }
            SecondaryIndexStore::Hash(_) | SecondaryIndexStore::UniqueHash(_) => Vec::new(),
        }
    }

    pub fn scan_prefix(&self, prefix: &EncodedKey) -> Vec<EncodedKey> {
        if !matches!(self.store, SecondaryIndexStore::BTree(_)) {
            return Vec::new();
        }
        let Some(end) = prefix_successor(prefix) else {
            return self.scan_range(Bound::Included(prefix.clone()), Bound::Unbounded);
        };
        self.scan_range(Bound::Included(prefix.clone()), Bound::Excluded(end))
    }

    pub fn scan_prefix_window(
        &self,
        prefix: Option<&EncodedKey>,
        offset: usize,
        limit: usize,
    ) -> Vec<EncodedKey> {
        self.scan_prefix_window_ordered(prefix, offset, limit, false)
    }

    pub fn scan_prefix_window_ordered(
        &self,
        prefix: Option<&EncodedKey>,
        offset: usize,
        limit: usize,
        reverse: bool,
    ) -> Vec<EncodedKey> {
        if limit == 0 {
            return Vec::new();
        }
        let SecondaryIndexStore::BTree(entries) = &self.store else {
            return Vec::new();
        };

        let mut out = Vec::with_capacity(index_scan_capacity_hint(limit));
        let mut skipped = 0usize;

        match prefix {
            None => append_window_entries(
                entries.iter(),
                offset,
                limit,
                reverse,
                &mut skipped,
                &mut out,
            ),
            Some(prefix_key) => {
                let range_end = prefix_successor(prefix_key);
                if let Some(end) = range_end {
                    append_window_entries(
                        entries.range((Bound::Included(prefix_key.clone()), Bound::Excluded(end))),
                        offset,
                        limit,
                        reverse,
                        &mut skipped,
                        &mut out,
                    );
                } else {
                    append_window_entries(
                        entries.range((Bound::Included(prefix_key.clone()), Bound::Unbounded)),
                        offset,
                        limit,
                        reverse,
                        &mut skipped,
                        &mut out,
                    );
                }
            }
        }

        out
    }

    pub fn rank_of_pk(&self, target_pk: &EncodedKey) -> Option<usize> {
        let SecondaryIndexStore::BTree(entries) = &self.store else {
            return None;
        };
        let mut rank = 0usize;
        for (_, pks) in entries {
            for pk in pks {
                if pk == target_pk {
                    return Some(rank);
                }
                rank += 1;
            }
        }
        None
    }

    pub fn unique_existing(&self, key: &EncodedKey) -> Option<EncodedKey> {
        match &self.store {
            SecondaryIndexStore::UniqueHash(entries) => entries.get(key).cloned(),
            _ => None,
        }
    }

    pub fn should_include_row(
        &self,
        row: &Row,
        schema: &TableSchema,
        table_name: &str,
    ) -> Result<bool, AedbError> {
        let Some(expr) = &self.partial_filter else {
            return Ok(true);
        };
        let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
        let compiled = compile_expr(expr, &columns, table_name)
            .map_err(|e| AedbError::Validation(format!("{e:?}")))?;
        Ok(eval_compiled_expr_public(&compiled, row))
    }

    // ---- Tier-aware reads -------------------------------------------------
    //
    // These mirror the resident `scan_*`/`unique_existing`/`rank_of_pk` methods
    // but additionally page in cold postings from `segments` (minus
    // `segment_tombstones`). When the index has no cold tier they delegate to the
    // resident fast path, so they are zero-overhead until eviction has run. A
    // missing `store` while cold segments exist is a configuration error.

    fn require_store<'a>(
        &self,
        store: Option<&'a KvSegmentStore>,
    ) -> Result<&'a KvSegmentStore, AedbError> {
        store.ok_or_else(|| AedbError::Unavailable {
            message: "index segment store is not attached".into(),
        })
    }

    /// Live (non-tombstoned) primary keys for `value` that live in the cold
    /// segment tier. Highest-versioned segment copy wins; a tombstone with
    /// `seq >= version` suppresses the posting.
    fn segment_pks_for_value(
        &self,
        value: &EncodedKey,
        store: Option<&KvSegmentStore>,
    ) -> Result<Vec<EncodedKey>, AedbError> {
        if self.segments.is_empty() {
            return Ok(Vec::new());
        }
        let store = self.require_store(store)?;
        let start = Bound::Included(value.as_slice().to_vec());
        let end = match prefix_successor(value) {
            Some(succ) => Bound::Excluded(succ.as_slice().to_vec()),
            None => Bound::Unbounded,
        };
        let mut best: BTreeMap<Vec<u8>, (u64, EncodedKey)> = BTreeMap::new();
        for segment in &self.segments {
            for item in store.scan_range(segment, &start, &end)? {
                let version = item.entry.version;
                if best
                    .get(&item.key)
                    .map(|(v, _)| version > *v)
                    .unwrap_or(true)
                {
                    let (_value, pk) = decode_index_segment_entry(&item.key, &item.entry.value);
                    best.insert(item.key.clone(), (version, pk));
                }
            }
        }
        let mut out = Vec::with_capacity(best.len());
        for (composite, (version, pk)) in best {
            if self.posting_is_tombstoned(&composite, version) {
                continue;
            }
            out.push(pk);
        }
        Ok(out)
    }

    fn posting_is_tombstoned(&self, composite: &[u8], version: u64) -> bool {
        self.segment_tombstones
            .get(&EncodedKey::from_bytes(composite.to_vec()))
            .map(|tomb| *tomb >= version)
            .unwrap_or(false)
    }

    /// Tier-aware equality lookup: resident postings plus live cold postings.
    pub fn tier_scan_eq_limit(
        &self,
        key: &EncodedKey,
        limit: usize,
        store: Option<&KvSegmentStore>,
    ) -> Result<Vec<EncodedKey>, AedbError> {
        if self.segments.is_empty() {
            return Ok(self.scan_eq_limit(key, limit));
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        // Union resident + cold, deduplicated and ordered for determinism.
        let mut set: BTreeSet<EncodedKey> =
            self.scan_eq_limit(key, usize::MAX).into_iter().collect();
        for pk in self.segment_pks_for_value(key, store)? {
            set.insert(pk);
        }
        Ok(set.into_iter().take(limit).collect())
    }

    /// Tier-aware unique-constraint probe: the single live primary key indexed
    /// under `key`, or `None`. Resident postings win; otherwise the cold tier is
    /// consulted. Critical for enforcing uniqueness against evicted postings.
    pub fn tier_unique_existing(
        &self,
        key: &EncodedKey,
        store: Option<&KvSegmentStore>,
    ) -> Result<Option<EncodedKey>, AedbError> {
        if let Some(pk) = self.unique_existing(key) {
            return Ok(Some(pk));
        }
        if self.segments.is_empty() {
            return Ok(None);
        }
        Ok(self.segment_pks_for_value(key, store)?.into_iter().next())
    }

    /// Merge resident + cold BTree postings whose index value falls in
    /// `[value_start, value_end]` into an ordered `value -> {pk}` map. Only
    /// meaningful for ordered (BTree) indexes; others yield an empty map.
    fn tier_btree_merged_in_range(
        &self,
        value_start: Bound<&EncodedKey>,
        value_end: Bound<&EncodedKey>,
        store: Option<&KvSegmentStore>,
    ) -> Result<BTreeMap<EncodedKey, BTreeSet<EncodedKey>>, AedbError> {
        let SecondaryIndexStore::BTree(entries) = &self.store else {
            return Ok(BTreeMap::new());
        };
        let mut merged: BTreeMap<EncodedKey, BTreeSet<EncodedKey>> = BTreeMap::new();
        let resident_range = (value_start.cloned(), value_end.cloned());
        for (value, pks) in entries.range(resident_range) {
            let set = merged.entry(value.clone()).or_default();
            for pk in pks {
                set.insert(pk.clone());
            }
        }
        if !self.segments.is_empty()
            && let Some((byte_start, byte_end)) = composite_byte_bounds(value_start, value_end)
        {
            let store = self.require_store(store)?;
            let mut best: BTreeMap<Vec<u8>, (u64, EncodedKey, EncodedKey)> = BTreeMap::new();
            for segment in &self.segments {
                for item in store.scan_range(segment, &byte_start, &byte_end)? {
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
                if self.posting_is_tombstoned(&composite, version) {
                    continue;
                }
                merged.entry(value).or_default().insert(pk);
            }
        }
        Ok(merged)
    }

    /// Tier-aware ordered range scan (BTree only), matching the windowing
    /// semantics of [`scan_range_window_ordered`](Self::scan_range_window_ordered).
    pub fn tier_scan_range_window_ordered(
        &self,
        start: Bound<EncodedKey>,
        end: Bound<EncodedKey>,
        offset: usize,
        limit: usize,
        reverse: bool,
        store: Option<&KvSegmentStore>,
    ) -> Result<Vec<EncodedKey>, AedbError> {
        if self.segments.is_empty() {
            return Ok(self.scan_range_window_ordered(start, end, offset, limit, reverse));
        }
        if limit == 0 || !matches!(self.store, SecondaryIndexStore::BTree(_)) {
            return Ok(Vec::new());
        }
        let merged = self.tier_btree_merged_in_range(start.as_ref(), end.as_ref(), store)?;
        Ok(window_merged_postings(&merged, offset, limit, reverse))
    }

    /// Tier-aware bounded range scan.
    pub fn tier_scan_range_limit(
        &self,
        start: Bound<EncodedKey>,
        end: Bound<EncodedKey>,
        limit: usize,
        store: Option<&KvSegmentStore>,
    ) -> Result<Vec<EncodedKey>, AedbError> {
        self.tier_scan_range_window_ordered(start, end, 0, limit, false, store)
    }

    /// Tier-aware ordered prefix scan (BTree only), matching
    /// [`scan_prefix_window_ordered`](Self::scan_prefix_window_ordered).
    pub fn tier_scan_prefix_window_ordered(
        &self,
        prefix: Option<&EncodedKey>,
        offset: usize,
        limit: usize,
        reverse: bool,
        store: Option<&KvSegmentStore>,
    ) -> Result<Vec<EncodedKey>, AedbError> {
        if self.segments.is_empty() {
            return Ok(self.scan_prefix_window_ordered(prefix, offset, limit, reverse));
        }
        if limit == 0 || !matches!(self.store, SecondaryIndexStore::BTree(_)) {
            return Ok(Vec::new());
        }
        let (start, end) = match prefix {
            None => (Bound::Unbounded, Bound::Unbounded),
            Some(prefix_key) => match prefix_successor(prefix_key) {
                Some(succ) => (Bound::Included(prefix_key.clone()), Bound::Excluded(succ)),
                None => (Bound::Included(prefix_key.clone()), Bound::Unbounded),
            },
        };
        let merged = self.tier_btree_merged_in_range(start.as_ref(), end.as_ref(), store)?;
        Ok(window_merged_postings(&merged, offset, limit, reverse))
    }

    /// Tier-aware prefix-window with default ascending order.
    pub fn tier_scan_prefix_window(
        &self,
        prefix: Option<&EncodedKey>,
        offset: usize,
        limit: usize,
        store: Option<&KvSegmentStore>,
    ) -> Result<Vec<EncodedKey>, AedbError> {
        self.tier_scan_prefix_window_ordered(prefix, offset, limit, false, store)
    }

    /// Tier-aware ordinal rank of `target_pk` across the full ordered index
    /// (BTree only); `None` if absent or not an ordered index.
    pub fn tier_rank_of_pk(
        &self,
        target_pk: &EncodedKey,
        store: Option<&KvSegmentStore>,
    ) -> Result<Option<usize>, AedbError> {
        if self.segments.is_empty() {
            return Ok(self.rank_of_pk(target_pk));
        }
        if !matches!(self.store, SecondaryIndexStore::BTree(_)) {
            return Ok(None);
        }
        let merged = self.tier_btree_merged_in_range(Bound::Unbounded, Bound::Unbounded, store)?;
        let mut rank = 0usize;
        for pks in merged.values() {
            for pk in pks {
                if pk == target_pk {
                    return Ok(Some(rank));
                }
                rank += 1;
            }
        }
        Ok(None)
    }
}

/// A byte range over composite segment keys.
type CompositeByteBounds = (Bound<Vec<u8>>, Bound<Vec<u8>>);

/// Translate an index-value range into the byte range over composite
/// `index_value ‖ pk` segment keys that contains exactly those postings. Returns
/// `None` when the value range is provably empty. Relies on `EncodedKey` values
/// being self-delimiting and prefix-free (see `index_segment_composite_key`).
fn composite_byte_bounds(
    value_start: Bound<&EncodedKey>,
    value_end: Bound<&EncodedKey>,
) -> Option<CompositeByteBounds> {
    let lo = match value_start {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(v) => Bound::Included(v.as_slice().to_vec()),
        // value > v: skip the whole `[v, successor(v))` block of v's composites.
        Bound::Excluded(v) => match prefix_successor(v) {
            Some(succ) => Bound::Included(succ.as_slice().to_vec()),
            None => return None, // nothing sorts after v
        },
    };
    let hi = match value_end {
        Bound::Unbounded => Bound::Unbounded,
        // value <= v: include all of v's composites, stop before successor(v).
        Bound::Included(v) => match prefix_successor(v) {
            Some(succ) => Bound::Excluded(succ.as_slice().to_vec()),
            None => Bound::Unbounded,
        },
        // value < v: v's composites all sort >= v's bytes, so exclude them.
        Bound::Excluded(v) => Bound::Excluded(v.as_slice().to_vec()),
    };
    Some((lo, hi))
}

/// Apply offset/limit/reverse windowing over a merged `value -> {pk}` map,
/// mirroring `append_window_entries`: in reverse mode the values iterate in
/// descending order while primary keys within a value stay ascending.
fn window_merged_postings(
    merged: &BTreeMap<EncodedKey, BTreeSet<EncodedKey>>,
    offset: usize,
    limit: usize,
    reverse: bool,
) -> Vec<EncodedKey> {
    let mut out = Vec::with_capacity(index_scan_capacity_hint(limit));
    let mut skipped = 0usize;
    let mut push_pks = |pks: &BTreeSet<EncodedKey>| -> bool {
        for pk in pks {
            if skipped < offset {
                skipped += 1;
                continue;
            }
            out.push(pk.clone());
            if out.len() >= limit {
                return true;
            }
        }
        false
    };
    if reverse {
        for (_, pks) in merged.iter().rev() {
            if push_pks(pks) {
                break;
            }
        }
    } else {
        for (_, pks) in merged.iter() {
            if push_pks(pks) {
                break;
            }
        }
    }
    out
}

fn append_window_entries<'a, I>(
    entries: I,
    offset: usize,
    limit: usize,
    reverse: bool,
    skipped: &mut usize,
    out: &mut Vec<EncodedKey>,
) where
    I: DoubleEndedIterator<Item = (&'a EncodedKey, &'a im::OrdSet<EncodedKey>)>,
{
    if reverse {
        for (_, pks) in entries.rev() {
            if append_window_pks(pks, offset, limit, skipped, out) {
                return;
            }
        }
    } else {
        for (_, pks) in entries {
            if append_window_pks(pks, offset, limit, skipped, out) {
                return;
            }
        }
    }
}

fn index_scan_capacity_hint(limit: usize) -> usize {
    const MAX_INITIAL_INDEX_SCAN_CAPACITY: usize = 1024;
    limit.min(MAX_INITIAL_INDEX_SCAN_CAPACITY)
}

fn append_window_pks(
    pks: &im::OrdSet<EncodedKey>,
    offset: usize,
    limit: usize,
    skipped: &mut usize,
    out: &mut Vec<EncodedKey>,
) -> bool {
    for pk in pks {
        if *skipped < offset {
            *skipped += 1;
            continue;
        }
        out.push(pk.clone());
        if out.len() >= limit {
            return true;
        }
    }
    false
}

pub fn extract_index_key(
    row: &Row,
    schema: &TableSchema,
    indexed_columns: &[String],
) -> Result<Vec<Value>, AedbError> {
    let mut out = Vec::with_capacity(indexed_columns.len());
    for col in indexed_columns {
        let column_index = schema
            .columns
            .iter()
            .position(|c| c.name == *col)
            .ok_or_else(|| AedbError::Validation(format!("indexed column not found: {col}")))?;
        // Treat missing slots as Null so index migrations against rows written
        // under an older schema (shorter `row.values`) succeed instead of panicking.
        out.push(row.values.get(column_index).cloned().unwrap_or(Value::Null));
    }
    Ok(out)
}

pub fn extract_index_key_encoded(
    row: &Row,
    schema: &TableSchema,
    indexed_columns: &[String],
) -> Result<EncodedKey, AedbError> {
    let values = extract_index_key(row, schema, indexed_columns)?;
    Ok(EncodedKey::from_values(&values))
}

#[cfg(test)]
#[path = "index_tests.rs"]
mod tests;
