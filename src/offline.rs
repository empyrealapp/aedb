use crate::checkpoint::writer::write_checkpoint_with_key;
use crate::commit::tx::{IdempotencyKey, IdempotencyRecord};
use crate::config::AedbConfig;
use crate::error::AedbError;
use crate::manifest::atomic::write_manifest_atomic_signed;
use crate::manifest::schema::Manifest;
use crate::recovery::{RecoveredState, recover_with_config};
use crate::storage::keyspace::NamespaceId;
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
    let bytes = rmp_serde::to_vec(state).map_err(|e| AedbError::Encode(e.to_string()))?;
    let mut h = Sha256::new();
    h.update(&bytes);
    Ok(hex::encode(h.finalize()))
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
