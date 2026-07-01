//! Read-set capture for reactive subscriptions.
//!
//! A `ReadSetCollector` is threaded through the query executor as an optional
//! sink. When present, it records the point keys (PK row lookups) and key
//! ranges (index scans, table scans) that the query touched, along with the
//! row versions observed at read time. The resulting `ReadSet` is shaped
//! identically to the read sets produced by `preflight`, so it can drive the
//! same invalidation paths.

use crate::catalog::schema::TableSchema;
use crate::catalog::types::Value;
use crate::commit::tx::{ReadBound, ReadKey, ReadRange, ReadRangeEntry, ReadSet, ReadSetEntry};
use crate::storage::encoded_key::EncodedKey;
use crate::storage::keyspace::{KeyspaceSnapshot, TableData};

/// Sink for read-set entries observed during query execution.
#[derive(Debug, Default)]
pub struct ReadSetCollector {
    set: ReadSet,
}

impl ReadSetCollector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn into_inner(self) -> ReadSet {
        self.set
    }

    /// Record a primary-key point read at the snapshot's current row version.
    pub fn record_point(
        &mut self,
        snapshot: &KeyspaceSnapshot,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        primary_key: Vec<Value>,
    ) {
        let encoded = EncodedKey::from_values(&primary_key);
        let version_at_read = snapshot
            .table(project_id, scope_id, table_name)
            .and_then(|t| t.version_of(&encoded))
            .unwrap_or(0);
        self.set.points.push(ReadSetEntry {
            key: ReadKey::TableRow {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: table_name.to_string(),
                primary_key,
            },
            version_at_read,
        });
    }

    /// Record a list of touched primary-key rows produced by an index scan.
    /// Each touched pk is recorded as a point entry using its logical PK
    /// values (decoded from the row data, matching the shape emitted by
    /// preflight and by mutation read-sets).
    pub fn record_touched_pks(
        &mut self,
        snapshot: &KeyspaceSnapshot,
        schema: &TableSchema,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        pks: &[EncodedKey],
    ) {
        let table = snapshot.table(project_id, scope_id, table_name);
        let pk_column_indices = pk_column_indices_in_schema(schema);
        for pk in pks {
            let version_at_read = table.and_then(|t| t.version_of(pk)).unwrap_or(0);
            let primary_key = extract_pk_values(snapshot, table, pk, &pk_column_indices);
            self.set.points.push(ReadSetEntry {
                key: ReadKey::TableRow {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    table_name: table_name.to_string(),
                    primary_key,
                },
                version_at_read,
            });
        }
    }

    /// Record a full or coarse table range with structural-version bounds.
    /// Used when the executor cannot enumerate exact touched pks (full scan,
    /// residual filter on full scan, join scan).
    pub fn record_full_table_scan(
        &mut self,
        snapshot: &KeyspaceSnapshot,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
    ) {
        let table = snapshot.table(project_id, scope_id, table_name);
        let (max_version, structural_version) = match table {
            Some(t) => (t.max_version(), t.structural_version),
            None => (0, 0),
        };
        self.set.ranges.push(ReadRangeEntry {
            range: ReadRange::TableRange {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: table_name.to_string(),
                start: ReadBound::Unbounded,
                end: ReadBound::Unbounded,
            },
            max_version_at_read: max_version,
            structural_version_at_read: structural_version,
        });
    }

    /// Record a read scoped to every row whose primary key begins with `prefix`
    /// (a leading primary-key-prefix scan). Unlike `record_full_table_scan`, a write
    /// only invalidates this read if its PK falls within the prefix band — so a
    /// reactive query over one key band (e.g. one instance's rows in a shared table)
    /// is not woken by writes to unrelated bands. Used when the executor bounded the
    /// scan to a PK prefix but could not enumerate exact touched pks (residual filter
    /// within the band).
    pub fn record_table_prefix(
        &mut self,
        snapshot: &KeyspaceSnapshot,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        prefix: Vec<Value>,
    ) {
        if prefix.is_empty() {
            // No prefix pinned → this is a whole-table dependency.
            self.record_full_table_scan(snapshot, project_id, scope_id, table_name);
            return;
        }
        let table = snapshot.table(project_id, scope_id, table_name);
        let (max_version, structural_version) = match table {
            Some(t) => (t.max_version(), t.structural_version),
            None => (0, 0),
        };
        self.set.ranges.push(ReadRangeEntry {
            range: ReadRange::TablePrefix {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: table_name.to_string(),
                prefix,
            },
            max_version_at_read: max_version,
            structural_version_at_read: structural_version,
        });
    }
}

fn pk_column_indices_in_schema(schema: &TableSchema) -> Vec<usize> {
    schema
        .primary_key
        .iter()
        .filter_map(|pk| schema.columns.iter().position(|c| c.name == *pk))
        .collect()
}

fn extract_pk_values(
    snapshot: &KeyspaceSnapshot,
    table: Option<&TableData>,
    pk_encoded: &EncodedKey,
    pk_column_indices: &[usize],
) -> Vec<Value> {
    if let Some(t) = table
        && let Some(stored) = t.rows.get(pk_encoded)
        && let Ok(row) = snapshot.materialize_row(stored)
    {
        return pk_column_indices
            .iter()
            .filter_map(|i| row.values.get(*i).cloned())
            .collect();
    }
    // Fallback: row was deleted between read and collector, degrade to an
    // opaque blob entry. The reactive layer can still invalidate via the
    // range entries recorded by the executor in that case.
    vec![Value::Blob(pk_encoded.as_slice().to_vec())]
}
