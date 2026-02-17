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
        match &self.store {
            SecondaryIndexStore::BTree(entries) => entries
                .get(key)
                .map(|pks| pks.iter().cloned().collect())
                .unwrap_or_default(),
            SecondaryIndexStore::Hash(entries) => entries
                .get(key)
                .map(|pks| pks.iter().cloned().collect())
                .unwrap_or_default(),
            SecondaryIndexStore::UniqueHash(entries) => entries
                .get(key)
                .map(|pk| vec![pk.clone()])
                .unwrap_or_default(),
        }
    }

    pub fn scan_range(&self, start: Bound<EncodedKey>, end: Bound<EncodedKey>) -> Vec<EncodedKey> {
        match &self.store {
            SecondaryIndexStore::BTree(entries) => entries
                .range((start, end))
                .flat_map(|(_, pks)| pks.iter().cloned())
                .collect(),
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

pub fn extract_index_key(
    row: &Row,
    schema: &TableSchema,
    indexed_columns: &[String],
) -> Result<Vec<Value>, AedbError> {
    let mut out = Vec::with_capacity(indexed_columns.len());
    for col in indexed_columns {
        let idx = schema
            .columns
            .iter()
            .position(|c| c.name == *col)
            .ok_or_else(|| AedbError::Validation(format!("indexed column not found: {col}")))?;
        out.push(row.values[idx].clone());
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
mod tests {
    use super::{extract_index_key, extract_index_key_encoded};
    use crate::catalog::schema::{ColumnDef, TableSchema};
    use crate::catalog::types::{ColumnType, Row, Value};
    use crate::storage::encoded_key::EncodedKey;
    use crate::storage::keyspace::SecondaryIndex;
    use std::ops::Bound;

    #[test]
    fn secondary_index_insert_remove_and_range() {
        let mut idx = SecondaryIndex::default();
        idx.insert(
            EncodedKey::from_values(&[Value::Integer(10)]),
            EncodedKey::from_values(&[Value::Integer(1)]),
        );
        idx.insert(
            EncodedKey::from_values(&[Value::Integer(20)]),
            EncodedKey::from_values(&[Value::Integer(2)]),
        );
        idx.insert(
            EncodedKey::from_values(&[Value::Integer(30)]),
            EncodedKey::from_values(&[Value::Integer(3)]),
        );

        let eq = idx.scan_eq(&EncodedKey::from_values(&[Value::Integer(20)]));
        assert_eq!(eq, vec![EncodedKey::from_values(&[Value::Integer(2)])]);

        let range = idx.scan_range(
            Bound::Included(EncodedKey::from_values(&[Value::Integer(15)])),
            Bound::Included(EncodedKey::from_values(&[Value::Integer(30)])),
        );
        assert_eq!(range.len(), 2);

        idx.remove(
            &EncodedKey::from_values(&[Value::Integer(20)]),
            &EncodedKey::from_values(&[Value::Integer(2)]),
        );
        assert!(
            idx.scan_eq(&EncodedKey::from_values(&[Value::Integer(20)]))
                .is_empty()
        );
    }

    #[test]
    fn extract_index_key_reads_schema_positions() {
        let schema = TableSchema {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "t".into(),
            owner_id: None,
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "age".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
            ],
            primary_key: vec!["id".into()],
            constraints: Vec::new(),
            foreign_keys: Vec::new(),
        };
        let row = Row::from_values(vec![Value::Integer(1), Value::Integer(42)]);
        let key = extract_index_key(&row, &schema, &["age".into()]).expect("extract");
        assert_eq!(key, vec![Value::Integer(42)]);
        let encoded = extract_index_key_encoded(&row, &schema, &["age".into()]).expect("encoded");
        assert_eq!(encoded, EncodedKey::from_values(&[Value::Integer(42)]));
    }
}
