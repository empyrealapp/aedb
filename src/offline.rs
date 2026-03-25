use crate::checkpoint::writer::write_checkpoint_with_key;
use crate::commit::tx::{IdempotencyKey, IdempotencyRecord};
use crate::config::AedbConfig;
use crate::error::AedbError;
use crate::manifest::atomic::write_manifest_atomic_signed;
use crate::manifest::schema::Manifest;
use crate::recovery::{RecoveredState, recover_with_config};
use crate::storage::index::extract_index_key_encoded;
use crate::storage::keyspace::{NamespaceId, SecondaryIndexStore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

const DUMP_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotDumpEnvelope {
    pub version: u32,
    pub exported_at_micros: u64,
    pub state: SnapshotDumpState,
    pub parity_checksum_hex: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotDumpState {
    pub current_seq: u64,
    pub keyspace: crate::storage::keyspace::Keyspace,
    pub catalog: crate::catalog::Catalog,
    pub idempotency: HashMap<IdempotencyKey, IdempotencyRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotDumpReport {
    pub current_seq: u64,
    pub parity_checksum_hex: String,
    pub table_rows: u64,
    pub kv_entries: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotParityReport {
    pub expected_checksum_hex: String,
    pub actual_checksum_hex: String,
    pub matches: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InvariantReport {
    pub ok: bool,
    pub table_count: u64,
    pub table_rows: u64,
    pub kv_entries: u64,
    pub violations: Vec<String>,
}

pub fn export_snapshot_dump(
    data_dir: &Path,
    config: &AedbConfig,
    out_path: &Path,
) -> Result<SnapshotDumpReport, AedbError> {
    let recovered = recover_with_config(data_dir, config)?;
    let state = SnapshotDumpState {
        current_seq: recovered.current_seq,
        keyspace: recovered.keyspace,
        catalog: recovered.catalog,
        idempotency: recovered.idempotency,
    };
    let parity_checksum_hex = checksum_state(&state)?;
    let report = summarize_state(&state, parity_checksum_hex.clone());
    let envelope = SnapshotDumpEnvelope {
        version: DUMP_FORMAT_VERSION,
        exported_at_micros: now_micros(),
        state,
        parity_checksum_hex,
    };
    let bytes = rmp_serde::to_vec(&envelope).map_err(|e| AedbError::Encode(e.to_string()))?;
    fs::write(out_path, bytes)?;
    Ok(report)
}

pub fn restore_snapshot_dump(
    dump_path: &Path,
    data_dir: &Path,
    config: &AedbConfig,
) -> Result<SnapshotDumpReport, AedbError> {
    let envelope = load_dump(dump_path)?;
    if envelope.version != DUMP_FORMAT_VERSION {
        return Err(AedbError::Validation(format!(
            "unsupported dump format version: {}",
            envelope.version
        )));
    }
    let expected = checksum_state(&envelope.state)?;
    if expected != envelope.parity_checksum_hex {
        return Err(AedbError::Validation(
            "dump parity checksum mismatch".into(),
        ));
    }
    if data_dir.exists() && fs::read_dir(data_dir)?.next().is_some() {
        return Err(AedbError::Validation(
            "restore target directory must be empty".into(),
        ));
    }
    fs::create_dir_all(data_dir)?;
    let checkpoint = write_checkpoint_with_key(
        &envelope.state.keyspace.snapshot(),
        &envelope.state.catalog,
        envelope.state.current_seq,
        data_dir,
        config.checkpoint_key(),
        config.checkpoint_key_id.clone(),
        envelope.state.idempotency.clone(),
        config.checkpoint_compression_level,
    )?;
    let manifest = Manifest {
        durable_seq: envelope.state.current_seq,
        visible_seq: envelope.state.current_seq,
        active_segment_seq: envelope.state.current_seq.saturating_add(1),
        checkpoints: vec![checkpoint],
        segments: vec![],
    };
    write_manifest_atomic_signed(&manifest, data_dir, config.hmac_key())?;

    let recovered = recover_with_config(data_dir, config)?;
    let restored_state = SnapshotDumpState {
        current_seq: recovered.current_seq,
        keyspace: recovered.keyspace,
        catalog: recovered.catalog,
        idempotency: recovered.idempotency,
    };
    let actual = checksum_state(&restored_state)?;
    if actual != envelope.parity_checksum_hex {
        return Err(AedbError::Validation(
            "restored state parity mismatch".into(),
        ));
    }
    Ok(summarize_state(&restored_state, actual))
}

pub fn parity_report_against_data_dir(
    dump_path: &Path,
    data_dir: &Path,
    config: &AedbConfig,
) -> Result<SnapshotParityReport, AedbError> {
    let envelope = load_dump(dump_path)?;
    let recovered = recover_with_config(data_dir, config)?;
    let actual_state = SnapshotDumpState {
        current_seq: recovered.current_seq,
        keyspace: recovered.keyspace,
        catalog: recovered.catalog,
        idempotency: recovered.idempotency,
    };
    let actual_checksum_hex = checksum_state(&actual_state)?;
    Ok(SnapshotParityReport {
        expected_checksum_hex: envelope.parity_checksum_hex.clone(),
        actual_checksum_hex: actual_checksum_hex.clone(),
        matches: envelope.parity_checksum_hex == actual_checksum_hex,
    })
}

pub fn invariant_report(
    data_dir: &Path,
    config: &AedbConfig,
) -> Result<InvariantReport, AedbError> {
    let recovered = recover_with_config(data_dir, config)?;
    Ok(check_invariants(&recovered))
}

fn summarize_state(state: &SnapshotDumpState, parity_checksum_hex: String) -> SnapshotDumpReport {
    let (table_rows, kv_entries) = state_counts(state);
    SnapshotDumpReport {
        current_seq: state.current_seq,
        parity_checksum_hex,
        table_rows,
        kv_entries,
    }
}

fn load_dump(path: &Path) -> Result<SnapshotDumpEnvelope, AedbError> {
    let bytes = fs::read(path)?;
    rmp_serde::from_slice(&bytes).map_err(|e| AedbError::Decode(e.to_string()))
}

fn checksum_state(state: &SnapshotDumpState) -> Result<String, AedbError> {
    let mut h = Sha256::new();
    hash_label(&mut h, "current_seq");
    hash_encoded(&mut h, &state.current_seq)?;

    hash_label(&mut h, "primary_index_backend");
    hash_encoded(&mut h, &state.keyspace.primary_index_backend)?;

    hash_label(&mut h, "namespaces");
    let mut namespaces = state
        .keyspace
        .namespaces
        .iter()
        .map(|(ns_id, namespace)| -> Result<_, AedbError> {
            Ok((encode(ns_id)?, ns_id, namespace))
        })
        .collect::<Result<Vec<_>, _>>()?;
    namespaces.sort_by(|a, b| a.0.cmp(&b.0));
    for (ns_key_bytes, _, namespace) in namespaces {
        hash_bytes(&mut h, &ns_key_bytes);

        hash_label(&mut h, "kv_entries");
        for (key, entry) in namespace.kv.entries.iter() {
            hash_bytes(&mut h, key);
            hash_encoded(&mut h, entry)?;
        }

        hash_label(&mut h, "tables");
        let mut tables = namespace.tables.iter().collect::<Vec<_>>();
        tables.sort_by(|a, b| a.0.cmp(b.0));
        for (table_name, table) in tables {
            hash_bytes(&mut h, table_name.as_bytes());
            hash_encoded(&mut h, &table.structural_version)?;

            hash_label(&mut h, "rows");
            for (pk, row) in table.rows.iter() {
                hash_bytes(&mut h, pk.as_slice());
                hash_encoded(&mut h, row)?;
            }

            hash_label(&mut h, "row_versions");
            for (pk, version) in table.row_versions.iter() {
                hash_bytes(&mut h, pk.as_slice());
                hash_encoded(&mut h, version)?;
            }

            hash_label(&mut h, "indexes");
            let mut indexes = table.indexes.iter().collect::<Vec<_>>();
            indexes.sort_by(|a, b| a.0.cmp(b.0));
            for (index_name, index) in indexes {
                hash_bytes(&mut h, index_name.as_bytes());
                hash_encoded(&mut h, &index.columns_bitmask)?;
                hash_encoded(&mut h, &index.partial_filter)?;
                hash_secondary_index_store(&mut h, &index.store)?;
            }
        }
    }

    hash_label(&mut h, "async_indexes");
    let mut async_indexes = state
        .keyspace
        .async_indexes
        .iter()
        .map(|(key, value)| -> Result<_, AedbError> { Ok((encode(key)?, key, value)) })
        .collect::<Result<Vec<_>, _>>()?;
    async_indexes.sort_by(|a, b| a.0.cmp(&b.0));
    for (key_bytes, _, value) in async_indexes {
        hash_bytes(&mut h, &key_bytes);
        hash_encoded(&mut h, &value.materialized_seq)?;
        for (pk, row) in value.rows.iter() {
            hash_bytes(&mut h, pk.as_slice());
            hash_encoded(&mut h, row)?;
        }
    }

    hash_label(&mut h, "catalog");
    hash_sorted_entries(&mut h, state.catalog.projects.iter())?;
    hash_sorted_entries(&mut h, state.catalog.scopes.iter())?;
    hash_sorted_entries(&mut h, state.catalog.tables.iter())?;
    hash_sorted_entries(&mut h, state.catalog.indexes.iter())?;
    hash_sorted_entries(&mut h, state.catalog.async_indexes.iter())?;
    hash_sorted_entries(&mut h, state.catalog.kv_projections.iter())?;
    hash_sorted_entries(&mut h, state.catalog.permissions.iter())?;
    hash_sorted_entries(&mut h, state.catalog.permission_grants.iter())?;
    hash_sorted_entries(&mut h, state.catalog.read_policies.iter())?;

    hash_label(&mut h, "idempotency");
    hash_sorted_entries(&mut h, state.idempotency.iter())?;

    Ok(hex::encode(h.finalize()))
}

fn hash_secondary_index_store(
    hasher: &mut Sha256,
    store: &SecondaryIndexStore,
) -> Result<(), AedbError> {
    match store {
        SecondaryIndexStore::BTree(entries) => {
            hash_label(hasher, "btree");
            for (index_key, encoded_pks) in entries.iter() {
                hash_bytes(hasher, index_key.as_slice());
                for pk in encoded_pks.iter() {
                    hash_bytes(hasher, pk.as_slice());
                }
            }
        }
        SecondaryIndexStore::Hash(entries) => {
            hash_label(hasher, "hash");
            let mut ordered = entries
                .iter()
                .map(|(index_key, encoded_pks)| (index_key.as_slice().to_vec(), encoded_pks))
                .collect::<Vec<_>>();
            ordered.sort_by(|a, b| a.0.cmp(&b.0));
            for (index_key, encoded_pks) in ordered {
                hash_bytes(hasher, &index_key);
                let mut pks = encoded_pks
                    .iter()
                    .map(|pk| pk.as_slice().to_vec())
                    .collect::<Vec<_>>();
                pks.sort();
                for pk in pks {
                    hash_bytes(hasher, &pk);
                }
            }
        }
        SecondaryIndexStore::UniqueHash(entries) => {
            hash_label(hasher, "unique_hash");
            let mut ordered = entries
                .iter()
                .map(|(index_key, encoded_pk)| {
                    (
                        index_key.as_slice().to_vec(),
                        encoded_pk.as_slice().to_vec(),
                    )
                })
                .collect::<Vec<_>>();
            ordered.sort_by(|a, b| a.0.cmp(&b.0));
            for (index_key, encoded_pk) in ordered {
                hash_bytes(hasher, &index_key);
                hash_bytes(hasher, &encoded_pk);
            }
        }
    }
    Ok(())
}

fn hash_sorted_entries<'a, K, V, I>(hasher: &mut Sha256, entries: I) -> Result<(), AedbError>
where
    K: Serialize + 'a,
    V: Serialize + 'a,
    I: IntoIterator<Item = (&'a K, &'a V)>,
{
    let mut encoded = entries
        .into_iter()
        .map(|(key, value)| -> Result<_, AedbError> { Ok((encode(key)?, value)) })
        .collect::<Result<Vec<_>, _>>()?;
    encoded.sort_by(|a, b| a.0.cmp(&b.0));
    for (key_bytes, value) in encoded {
        hash_bytes(hasher, &key_bytes);
        hash_encoded(hasher, value)?;
    }
    Ok(())
}

fn hash_label(hasher: &mut Sha256, label: &str) {
    hash_bytes(hasher, label.as_bytes());
}

fn hash_encoded<T: Serialize>(hasher: &mut Sha256, value: &T) -> Result<(), AedbError> {
    let bytes = encode(value)?;
    hash_bytes(hasher, &bytes);
    Ok(())
}

fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, AedbError> {
    rmp_serde::to_vec(value).map_err(|e| AedbError::Encode(e.to_string()))
}

fn state_counts(state: &SnapshotDumpState) -> (u64, u64) {
    let mut table_rows = 0u64;
    let mut kv_entries = 0u64;
    for namespace in state.keyspace.namespaces.values() {
        kv_entries = kv_entries.saturating_add(namespace.kv.entries.len() as u64);
        for table in namespace.tables.values() {
            table_rows = table_rows.saturating_add(table.rows.len() as u64);
        }
    }
    (table_rows, kv_entries)
}

fn check_invariants(recovered: &RecoveredState) -> InvariantReport {
    let mut violations = Vec::new();
    let mut table_count = 0u64;
    let mut table_rows = 0u64;
    let mut kv_entries = 0u64;

    for (ns_id, ns) in recovered.keyspace.namespaces.iter() {
        kv_entries = kv_entries.saturating_add(ns.kv.entries.len() as u64);
        for (table_name, table_data) in &ns.tables {
            table_count = table_count.saturating_add(1);
            table_rows = table_rows.saturating_add(table_data.rows.len() as u64);
            if table_data.rows.len() != table_data.row_versions.len() {
                violations.push(format!(
                    "row_versions cardinality mismatch in namespace={:?} table={table_name}",
                    ns_id
                ));
            }
            if table_data.rows.len() != table_data.pk_hash.len() {
                violations.push(format!(
                    "pk_hash cardinality mismatch in namespace={:?} table={table_name}",
                    ns_id
                ));
            }
            let schema_key = match ns_id {
                NamespaceId::Project(namespace) => Some((namespace.clone(), table_name.clone())),
                NamespaceId::System | NamespaceId::Global => None,
            };
            if let Some(schema_key) = schema_key
                && let Some(schema) = recovered.catalog.tables.get(&schema_key)
            {
                for (index_name, index) in &table_data.indexes {
                    let mut expected = index.store.clone();
                    match &mut expected {
                        SecondaryIndexStore::BTree(entries) => entries.clear(),
                        SecondaryIndexStore::Hash(entries) => entries.clear(),
                        SecondaryIndexStore::UniqueHash(entries) => entries.clear(),
                    }
                    for (encoded_pk, row) in &table_data.rows {
                        match index.should_include_row(row, schema, table_name) {
                            Ok(true) => {}
                            Ok(false) => continue,
                            Err(err) => {
                                violations.push(format!(
                                    "index {index_name} predicate evaluation failed in namespace={:?} table={table_name}: {err}",
                                    ns_id
                                ));
                                continue;
                            }
                        }
                        let index_def = match recovered.catalog.indexes.get(&(
                            schema_key.0.clone(),
                            table_name.clone(),
                            index_name.clone(),
                        )) {
                            Some(index_def) => index_def,
                            None => {
                                violations.push(format!(
                                    "index definition missing for namespace={:?} table={table_name} index={index_name}",
                                    ns_id
                                ));
                                continue;
                            }
                        };
                        let index_key =
                            match extract_index_key_encoded(row, schema, &index_def.columns) {
                                Ok(key) => key,
                                Err(err) => {
                                    violations.push(format!(
                                        "index key extraction failed for namespace={:?} table={table_name} index={index_name}: {err}",
                                        ns_id
                                    ));
                                    continue;
                                }
                            };
                        match &mut expected {
                            SecondaryIndexStore::BTree(entries) => {
                                let mut pks = entries.get(&index_key).cloned().unwrap_or_default();
                                pks.insert(encoded_pk.clone());
                                entries.insert(index_key, pks);
                            }
                            SecondaryIndexStore::Hash(entries) => {
                                let mut pks = entries.get(&index_key).cloned().unwrap_or_default();
                                pks.insert(encoded_pk.clone());
                                entries.insert(index_key, pks);
                            }
                            SecondaryIndexStore::UniqueHash(entries) => {
                                entries.insert(index_key, encoded_pk.clone());
                            }
                        }
                    }
                    if expected != index.store {
                        violations.push(format!(
                            "secondary index mismatch in namespace={:?} table={table_name} index={index_name}",
                            ns_id
                        ));
                    }
                }
            }
        }
    }

    for ((namespace, table_name), schema) in &recovered.catalog.tables {
        let ns_id = NamespaceId::Project(namespace.clone());
        let Some(ns) = recovered.keyspace.namespaces.get(&ns_id) else {
            violations.push(format!(
                "catalog table missing namespace: {namespace}.{table_name}"
            ));
            continue;
        };
        if !ns.tables.contains_key(table_name) {
            violations.push(format!(
                "catalog table missing keyspace rows: {namespace}.{table_name}"
            ));
        }
        let expected_ns = crate::catalog::namespace_key(&schema.project_id, &schema.scope_id);
        if expected_ns != *namespace {
            violations.push(format!(
                "namespace mismatch for table {table_name}: catalog={expected_ns} key={namespace}"
            ));
        }
    }

    for ((namespace, table_name), schema) in &recovered.catalog.tables {
        for fk in &schema.foreign_keys {
            let child_ns_id = NamespaceId::Project(namespace.clone());
            let child_table = recovered
                .keyspace
                .namespaces
                .get(&child_ns_id)
                .and_then(|ns| ns.tables.get(table_name));
            let Some(child_table) = child_table else {
                continue;
            };
            let parent_ns =
                crate::catalog::namespace_key(&fk.references_project_id, &fk.references_scope_id);
            let parent_key = (parent_ns.clone(), fk.references_table.clone());
            let Some(parent_schema) = recovered.catalog.tables.get(&parent_key) else {
                violations.push(format!(
                    "fk {} references missing schema {}",
                    fk.name, fk.references_table
                ));
                continue;
            };
            let parent_table = recovered
                .keyspace
                .namespaces
                .get(&NamespaceId::Project(parent_ns))
                .and_then(|ns| ns.tables.get(&fk.references_table));
            let Some(parent_table) = parent_table else {
                violations.push(format!(
                    "fk {} references missing keyspace table {}",
                    fk.name, fk.references_table
                ));
                continue;
            };
            let child_positions: Option<Vec<usize>> = fk
                .columns
                .iter()
                .map(|c| schema.columns.iter().position(|col| col.name == *c))
                .collect();
            let parent_positions: Option<Vec<usize>> = fk
                .references_columns
                .iter()
                .map(|c| parent_schema.columns.iter().position(|col| col.name == *c))
                .collect();
            let (Some(child_positions), Some(parent_positions)) =
                (child_positions, parent_positions)
            else {
                violations.push(format!("fk {} has unresolved columns", fk.name));
                continue;
            };

            for row in child_table.rows.values() {
                let child_values: Vec<crate::catalog::types::Value> = child_positions
                    .iter()
                    .filter_map(|idx| row.values.get(*idx).cloned())
                    .collect();
                if child_values.len() != child_positions.len() {
                    violations.push(format!("fk {} child row missing values", fk.name));
                    continue;
                }
                if child_values
                    .iter()
                    .any(|v| matches!(v, crate::catalog::types::Value::Null))
                {
                    continue;
                }
                let mut found = false;
                for parent_row in parent_table.rows.values() {
                    let parent_values: Vec<crate::catalog::types::Value> = parent_positions
                        .iter()
                        .filter_map(|idx| parent_row.values.get(*idx).cloned())
                        .collect();
                    if parent_values == child_values {
                        found = true;
                        break;
                    }
                }
                if !found {
                    violations.push(format!("fk {} referential integrity violation", fk.name));
                }
            }
        }
    }

    InvariantReport {
        ok: violations.is_empty(),
        table_count,
        table_rows,
        kv_entries,
        violations,
    }
}

pub fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

#[cfg(test)]
mod tests {
    use super::check_invariants;
    use crate::catalog::schema::{ColumnDef, IndexDef, IndexType, TableSchema};
    use crate::catalog::types::{ColumnType, Row, Value};
    use crate::recovery::RecoveredState;
    use crate::storage::encoded_key::EncodedKey;
    use crate::storage::keyspace::{Keyspace, Namespace, NamespaceId, SecondaryIndex, SecondaryIndexStore, TableData};
    use im::{HashMap as ImHashMap, OrdMap};
    use std::collections::HashMap;

    #[test]
    fn check_invariants_detects_secondary_index_mismatch() {
        let namespace = "p::app".to_string();
        let pk = EncodedKey::from_values(&[Value::Integer(1)]);
        let row = Row::from_values(vec![Value::Integer(1), Value::Text("alice".into())]);

        let mut table = TableData {
            rows: OrdMap::new(),
            row_versions: OrdMap::new(),
            structural_version: 0,
            pk_hash: ImHashMap::new(),
            row_cache: ImHashMap::new(),
            row_versions_cache: ImHashMap::new(),
            indexes: ImHashMap::new(),
        };
        table.rows.insert(pk.clone(), row.clone());
        table.row_versions.insert(pk.clone(), 1);
        table.pk_hash.insert(pk.clone(), ());
        table.row_cache.insert(pk.clone(), row);
        table.row_versions_cache.insert(pk.clone(), 1);
        table.indexes.insert(
            "by_owner".into(),
            SecondaryIndex {
                store: SecondaryIndexStore::BTree(OrdMap::new()),
                columns_bitmask: 0,
                partial_filter: None,
            },
        );

        let mut ns = Namespace {
            id: NamespaceId::Project(namespace.clone()),
            ..Namespace::default()
        };
        ns.tables.insert("users".into(), table);

        let mut keyspace = Keyspace::default();
        keyspace.insert_namespace(NamespaceId::Project(namespace.clone()), ns);

        let mut catalog = crate::catalog::Catalog::default();
        catalog.tables.insert(
            (namespace.clone(), "users".into()),
            TableSchema {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                owner_id: None,
                columns: vec![
                    ColumnDef {
                        name: "id".into(),
                        col_type: ColumnType::Integer,
                        nullable: false,
                    },
                    ColumnDef {
                        name: "owner".into(),
                        col_type: ColumnType::Text,
                        nullable: false,
                    },
                ],
                primary_key: vec!["id".into()],
                constraints: Vec::new(),
                foreign_keys: Vec::new(),
            },
        );
        catalog.indexes.insert(
            (namespace, "users".into(), "by_owner".into()),
            IndexDef {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                index_name: "by_owner".into(),
                columns: vec!["owner".into()],
                index_type: IndexType::BTree,
                columns_bitmask: 0,
                partial_filter: None,
            },
        );

        let report = check_invariants(&RecoveredState {
            keyspace,
            catalog,
            current_seq: 1,
            idempotency: HashMap::new(),
        });

        assert!(!report.ok);
        assert!(report
            .violations
            .iter()
            .any(|violation| violation.contains("secondary index mismatch")));
    }
}
