use crate::catalog::schema::TableSchema;
use crate::catalog::types::{Row, Value};
use crate::error::AedbError;
use crate::query::operators::{compile_expr, eval_compiled_expr_public};
use crate::storage::encoded_key::{EncodedKey, prefix_successor};
use crate::storage::keyspace::{SecondaryIndex, SecondaryIndexStore};
use std::ops::Bound;

impl SecondaryIndex {
    pub fn insert(&mut self, key: EncodedKey, pk: EncodedKey) {
        match &mut self.store {
            SecondaryIndexStore::BTree(entries) => {
                let mut pks = entries.get(&key).cloned().unwrap_or_default();
                pks.insert(pk);
                entries.insert(key, pks);
            }
            SecondaryIndexStore::Hash(entries) => {
                let mut pks = entries.get(&key).cloned().unwrap_or_default();
                pks.insert(pk);
                entries.insert(key, pks);
            }
            SecondaryIndexStore::UniqueHash(entries) => {
                entries.insert(key, pk);
            }
        }
    }

    pub fn remove(&mut self, key: &EncodedKey, pk: &EncodedKey) {
        match &mut self.store {
            SecondaryIndexStore::BTree(entries) => {
                let Some(mut pks) = entries.get(key).cloned() else {
                    return;
                };
                pks.remove(pk);
                if pks.is_empty() {
                    entries.remove(key);
                } else {
                    entries.insert(key.clone(), pks);
                }
            }
            SecondaryIndexStore::Hash(entries) => {
                let Some(mut pks) = entries.get(key).cloned() else {
                    return;
                };
                pks.remove(pk);
                if pks.is_empty() {
                    entries.remove(key);
                } else {
                    entries.insert(key.clone(), pks);
                }
            }
            SecondaryIndexStore::UniqueHash(entries) => {
                entries.remove(key);
            }
        }
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
