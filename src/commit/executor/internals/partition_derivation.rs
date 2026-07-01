use super::GLOBAL_PARTITION_TOKEN;
use crate::catalog::schema::TableSchema;
use crate::catalog::types::{Row, Value};
use crate::catalog::{Catalog, namespace_key};
use crate::commit::tx::{ReadAssertion, ReadBound, ReadKey, ReadRange, TransactionEnvelope};
use crate::commit::validation::Mutation;
use crate::error::AedbError;
use crate::storage::encoded_key::EncodedKey;
use crate::storage::keyspace::Keyspace;
use std::collections::HashSet;
use std::ops::Bound;

pub(in crate::commit::executor) fn derive_write_partitions_with_fk_expansion(
    catalog: &Catalog,
    mutations: &[Mutation],
) -> HashSet<String> {
    let mut out = HashSet::new();
    let mut touched_tables = HashSet::new();
    for mutation in mutations {
        match mutation {
            Mutation::Insert {
                project_id,
                scope_id,
                table_name,
                primary_key,
                ..
            }
            | Mutation::Upsert {
                project_id,
                scope_id,
                table_name,
                primary_key,
                ..
            }
            | Mutation::Delete {
                project_id,
                scope_id,
                table_name,
                primary_key,
            }
            | Mutation::TableIncU256 {
                project_id,
                scope_id,
                table_name,
                primary_key,
                ..
            }
            | Mutation::TableDecU256 {
                project_id,
                scope_id,
                table_name,
                primary_key,
                ..
            }
            | Mutation::UpdateFields {
                project_id,
                scope_id,
                table_name,
                primary_key,
                ..
            } => {
                let ns = namespace_key(project_id, scope_id);
                if table_supports_row_parallelism(catalog, &ns, table_name) {
                    out.insert(table_row_partition_token(&ns, table_name, primary_key));
                } else {
                    out.insert(table_partition_token(&ns, table_name));
                }
                touched_tables.insert((ns, table_name.clone()));
            }
            Mutation::InsertBatch {
                project_id,
                scope_id,
                table_name,
                rows,
            }
            | Mutation::UpsertBatch {
                project_id,
                scope_id,
                table_name,
                rows,
            } => {
                let ns = namespace_key(project_id, scope_id);
                if table_supports_row_parallelism(catalog, &ns, table_name) {
                    if let Some(primary_keys) =
                        extract_row_primary_keys_for_partitions(catalog, &ns, table_name, rows)
                    {
                        for primary_key in primary_keys {
                            out.insert(table_row_partition_token(&ns, table_name, &primary_key));
                        }
                    } else {
                        out.insert(table_partition_token(&ns, table_name));
                    }
                } else {
                    out.insert(table_partition_token(&ns, table_name));
                }
                touched_tables.insert((ns, table_name.clone()));
            }
            Mutation::UpsertOnConflict {
                project_id,
                scope_id,
                table_name,
                ..
            }
            | Mutation::UpsertBatchOnConflict {
                project_id,
                scope_id,
                table_name,
                ..
            }
            | Mutation::DeleteWhere {
                project_id,
                scope_id,
                table_name,
                ..
            }
            | Mutation::UpdateWhere {
                project_id,
                scope_id,
                table_name,
                ..
            }
            | Mutation::UpdateWhereExpr {
                project_id,
                scope_id,
                table_name,
                ..
            } => {
                let ns = namespace_key(project_id, scope_id);
                // Predicate-based writes do not expose touched primary keys at scheduling time,
                // so they conservatively serialize through the table partition.
                out.insert(table_partition_token(&ns, table_name));
                touched_tables.insert((ns, table_name.clone()));
            }
            Mutation::KvSet {
                project_id,
                scope_id,
                key,
                ..
            }
            | Mutation::KvDel {
                project_id,
                scope_id,
                key,
                ..
            }
            | Mutation::KvIncU256 {
                project_id,
                scope_id,
                key,
                ..
            }
            | Mutation::KvDecU256 {
                project_id,
                scope_id,
                key,
                ..
            }
            | Mutation::KvAddU256Ex {
                project_id,
                scope_id,
                key,
                ..
            }
            | Mutation::KvSubU256Ex {
                project_id,
                scope_id,
                key,
                ..
            }
            | Mutation::KvMaxU256 {
                project_id,
                scope_id,
                key,
                ..
            }
            | Mutation::KvMinU256 {
                project_id,
                scope_id,
                key,
                ..
            }
            | Mutation::KvMutateU256 {
                project_id,
                scope_id,
                key,
                ..
            }
            | Mutation::KvAddU64Ex {
                project_id,
                scope_id,
                key,
                ..
            }
            | Mutation::KvSubU64Ex {
                project_id,
                scope_id,
                key,
                ..
            }
            | Mutation::KvSubIntEx {
                project_id,
                scope_id,
                key,
                ..
            }
            | Mutation::KvAddI64Bounded {
                project_id,
                scope_id,
                key,
                ..
            }
            | Mutation::KvMaxU64 {
                project_id,
                scope_id,
                key,
                ..
            }
            | Mutation::KvMinU64 {
                project_id,
                scope_id,
                key,
                ..
            }
            | Mutation::KvMutateU64 {
                project_id,
                scope_id,
                key,
                ..
            } => {
                let ns = namespace_key(project_id, scope_id);
                out.insert(kv_key_partition_token(&ns, key));
            }
            Mutation::CounterAdd {
                project_id,
                scope_id,
                key,
                shard_count,
                shard_hint,
                ..
            } => {
                let ns = namespace_key(project_id, scope_id);
                let shard =
                    crate::commit::validation::counter_shard_index(*shard_hint, *shard_count);
                let shard_key = crate::commit::validation::counter_shard_storage_key(key, shard);
                out.insert(kv_key_partition_token(&ns, &shard_key));
            }
            Mutation::EmitEvent {
                project_id,
                scope_id,
                topic,
                ..
            } => {
                let ns = namespace_key(project_id, scope_id);
                out.insert(format!("evt:{ns}:{topic}"));
            }
            Mutation::OrderBookNew {
                project_id,
                scope_id,
                request,
            } => {
                let ns = namespace_key(project_id, scope_id);
                out.insert(order_book_partition_token(&ns, &request.instrument));
            }
            Mutation::OrderBookCancel {
                project_id,
                scope_id,
                instrument,
                ..
            }
            | Mutation::OrderBookCancelReplace {
                project_id,
                scope_id,
                instrument,
                ..
            }
            | Mutation::OrderBookMassCancel {
                project_id,
                scope_id,
                instrument,
                ..
            }
            | Mutation::OrderBookReduce {
                project_id,
                scope_id,
                instrument,
                ..
            }
            | Mutation::OrderBookMatch {
                project_id,
                scope_id,
                instrument,
                ..
            } => {
                let ns = namespace_key(project_id, scope_id);
                out.insert(order_book_partition_token(&ns, instrument));
            }
            Mutation::OrderBookDefineTable {
                project_id,
                scope_id,
                table_id,
                ..
            }
            | Mutation::OrderBookDropTable {
                project_id,
                scope_id,
                table_id,
            } => {
                let ns = namespace_key(project_id, scope_id);
                out.insert(order_book_meta_partition_token(&ns, "table", table_id));
            }
            Mutation::OrderBookSetInstrumentConfig {
                project_id,
                scope_id,
                instrument,
                ..
            }
            | Mutation::OrderBookSetInstrumentHalted {
                project_id,
                scope_id,
                instrument,
                ..
            } => {
                let ns = namespace_key(project_id, scope_id);
                out.insert(order_book_meta_partition_token(&ns, "cfg", instrument));
            }
            Mutation::Ddl(_) => {
                out.insert(GLOBAL_PARTITION_TOKEN.to_string());
            }
            Mutation::PostflightCheck { .. } => {}
        }
    }

    for (target_ns, target_table) in touched_tables {
        for ((dep_ns, dep_table), dep_schema) in &catalog.tables {
            for fk in &dep_schema.foreign_keys {
                if namespace_key(&fk.references_project_id, &fk.references_scope_id) == target_ns
                    && fk.references_table == target_table
                {
                    out.insert(table_partition_token(dep_ns, dep_table));
                    break;
                }
            }
        }
    }

    out
}

pub(in crate::commit::executor) fn derive_write_partitions(
    mutations: &[Mutation],
) -> HashSet<String> {
    derive_write_partitions_with_fk_expansion(&Catalog::default(), mutations)
}

pub(in crate::commit::executor) fn mutation_requires_fk_expansion(mutation: &Mutation) -> bool {
    matches!(
        mutation,
        Mutation::Insert { .. }
            | Mutation::InsertBatch { .. }
            | Mutation::Upsert { .. }
            | Mutation::UpsertBatch { .. }
            | Mutation::UpsertOnConflict { .. }
            | Mutation::UpsertBatchOnConflict { .. }
            | Mutation::Delete { .. }
            | Mutation::DeleteWhere { .. }
            | Mutation::UpdateWhere { .. }
            | Mutation::UpdateWhereExpr { .. }
            | Mutation::TableIncU256 { .. }
            | Mutation::TableDecU256 { .. }
    )
}

pub(super) fn table_supports_row_parallelism(
    catalog: &Catalog,
    namespace: &str,
    table_name: &str,
) -> bool {
    if !is_parallel_table_safe_by_namespace(catalog, namespace, table_name) {
        return false;
    }
    !catalog
        .indexes
        .keys()
        .any(|(idx_ns, idx_table, _)| idx_ns == namespace && idx_table == table_name)
}

pub(super) fn is_parallel_table_safe_by_namespace(
    catalog: &Catalog,
    namespace: &str,
    table_name: &str,
) -> bool {
    let Some(schema) = catalog
        .tables
        .get(&(namespace.to_string(), table_name.to_string()))
    else {
        return false;
    };
    if !schema.foreign_keys.is_empty() {
        return false;
    }
    for ((_dep_ns, _dep_table), dep_schema) in &catalog.tables {
        for fk in &dep_schema.foreign_keys {
            if namespace_key(&fk.references_project_id, &fk.references_scope_id) == namespace
                && fk.references_table == table_name
            {
                return false;
            }
        }
    }
    true
}

pub(super) fn extract_row_primary_keys_for_partitions(
    catalog: &Catalog,
    namespace: &str,
    table_name: &str,
    rows: &[Row],
) -> Option<Vec<Vec<Value>>> {
    let schema = catalog
        .tables
        .get(&(namespace.to_string(), table_name.to_string()))?;
    let mut primary_keys = Vec::with_capacity(rows.len());
    for row in rows {
        primary_keys.push(extract_primary_key_for_partition(schema, row)?);
    }
    Some(primary_keys)
}

pub(super) fn extract_primary_key_for_partition(
    schema: &TableSchema,
    row: &Row,
) -> Option<Vec<Value>> {
    let mut primary_key = Vec::with_capacity(schema.primary_key.len());
    for pk_name in &schema.primary_key {
        let column_index = schema.columns.iter().position(|c| c.name == *pk_name)?;
        primary_key.push(row.values.get(column_index)?.clone());
    }
    Some(primary_key)
}

pub(in crate::commit::executor) fn derive_read_partitions(
    envelope: &TransactionEnvelope,
) -> HashSet<String> {
    let mut out = HashSet::new();
    for entry in &envelope.read_set.points {
        match &entry.key {
            ReadKey::TableRow {
                project_id,
                scope_id,
                table_name,
                primary_key,
            } => {
                let ns = namespace_key(project_id, scope_id);
                out.insert(table_row_partition_token(&ns, table_name, primary_key));
            }
            ReadKey::KvKey {
                project_id,
                scope_id,
                key,
                ..
            } => {
                let ns = namespace_key(project_id, scope_id);
                out.insert(kv_key_partition_token(&ns, key));
            }
        }
    }
    for range in &envelope.read_set.ranges {
        match &range.range {
            ReadRange::TableRange {
                project_id,
                scope_id,
                table_name,
                ..
            }
            | ReadRange::TablePrefix {
                project_id,
                scope_id,
                table_name,
                ..
            } => {
                let ns = namespace_key(project_id, scope_id);
                out.insert(table_partition_token(&ns, table_name));
            }
            ReadRange::KvRange {
                project_id,
                scope_id,
                ..
            } => {
                let ns = namespace_key(project_id, scope_id);
                out.insert(kv_namespace_partition_token(&ns));
            }
        }
    }
    derive_tokens_for_assertions(&mut out, &envelope.assertions);
    out
}

pub(super) fn derive_tokens_for_assertions(
    out: &mut HashSet<String>,
    assertions: &[ReadAssertion],
) {
    for assertion in assertions {
        derive_tokens_for_assertion(out, assertion);
    }
}

pub(super) fn derive_tokens_for_assertion(out: &mut HashSet<String>, assertion: &ReadAssertion) {
    match assertion {
        ReadAssertion::KeyEquals {
            project_id,
            scope_id,
            key,
            ..
        }
        | ReadAssertion::KeyCompare {
            project_id,
            scope_id,
            key,
            ..
        }
        | ReadAssertion::KeyExists {
            project_id,
            scope_id,
            key,
            ..
        }
        | ReadAssertion::KeyVersion {
            project_id,
            scope_id,
            key,
            ..
        } => {
            let ns = namespace_key(project_id, scope_id);
            out.insert(kv_key_partition_token(&ns, key));
        }
        ReadAssertion::RowVersion {
            project_id,
            scope_id,
            table_name,
            primary_key,
            ..
        }
        | ReadAssertion::RowExists {
            project_id,
            scope_id,
            table_name,
            primary_key,
            ..
        } => {
            let ns = namespace_key(project_id, scope_id);
            out.insert(table_row_partition_token(&ns, table_name, primary_key));
        }
        ReadAssertion::RowColumnCompare {
            project_id,
            scope_id,
            table_name,
            primary_key,
            ..
        } => {
            let ns = namespace_key(project_id, scope_id);
            out.insert(table_row_partition_token(&ns, table_name, primary_key));
        }
        ReadAssertion::CountCompare {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | ReadAssertion::SumCompare {
            project_id,
            scope_id,
            table_name,
            ..
        } => {
            let ns = namespace_key(project_id, scope_id);
            out.insert(table_partition_token(&ns, table_name));
        }
        ReadAssertion::All(inner) | ReadAssertion::Any(inner) => {
            derive_tokens_for_assertions(out, inner);
        }
        ReadAssertion::Not(inner) => derive_tokens_for_assertion(out, inner),
    }
}

pub(super) fn table_partition_token(namespace: &str, table_name: &str) -> String {
    format!("t:{namespace}:{table_name}")
}

pub(super) fn table_row_partition_token(
    namespace: &str,
    table_name: &str,
    primary_key: &[Value],
) -> String {
    let encoded = EncodedKey::from_values(primary_key);
    let mut out = String::with_capacity(
        namespace.len() + table_name.len() + encoded.as_slice().len() * 2 + 6,
    );
    out.push_str("tr:");
    out.push_str(namespace);
    out.push(':');
    out.push_str(table_name);
    out.push(':');
    append_hex_lower(&mut out, encoded.as_slice());
    out
}

pub(super) fn kv_key_partition_token(namespace: &str, key: &[u8]) -> String {
    let mut out = String::with_capacity(key.len().saturating_mul(2) + namespace.len() + 3);
    out.push_str("k:");
    out.push_str(namespace);
    out.push(':');
    append_hex_lower(&mut out, key);
    out
}

pub(super) fn append_hex_lower(out: &mut String, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.reserve(bytes.len().saturating_mul(2));
    // The appended bytes are always ASCII hex digits, so the String remains valid UTF-8.
    let out_bytes = unsafe { out.as_mut_vec() };
    for b in bytes {
        out_bytes.push(HEX[(b >> 4) as usize]);
        out_bytes.push(HEX[(b & 0x0f) as usize]);
    }
}

pub(super) fn kv_namespace_partition_token(namespace: &str) -> String {
    format!("kns:{namespace}")
}

pub(super) fn order_book_partition_token(namespace: &str, instrument: &str) -> String {
    format!("ob:{namespace}:{instrument}")
}

pub(super) fn order_book_meta_partition_token(namespace: &str, kind: &str, id: &str) -> String {
    format!("obm:{namespace}:{kind}:{id}")
}

pub(super) fn is_cross_partition_write_set(write_set: &HashSet<String>) -> bool {
    if write_set.len() <= 1 {
        return false;
    }
    if write_set.contains(GLOBAL_PARTITION_TOKEN) {
        return true;
    }
    let mut first_ns: Option<&str> = None;
    for token in write_set {
        let Some(ns) = token_namespace(token) else {
            continue;
        };
        if let Some(existing) = first_ns {
            if existing != ns {
                return true;
            }
        } else {
            first_ns = Some(ns);
        }
    }
    false
}

pub(in crate::commit::executor) fn keyspace_mutations_cross_partition(
    mutations: &[Mutation],
) -> bool {
    if mutations.len() <= 1 {
        return false;
    }
    let mut first: Option<(&str, &str)> = None;
    for mutation in mutations {
        let Some((project_id, scope_id)) = keyspace_mutation_project_scope(mutation) else {
            return true;
        };
        if let Some((first_project, first_scope)) = first {
            if first_project != project_id || first_scope != scope_id {
                return true;
            }
        } else {
            first = Some((project_id, scope_id));
        }
    }
    false
}

pub(super) fn keyspace_mutation_project_scope(mutation: &Mutation) -> Option<(&str, &str)> {
    match mutation {
        Mutation::KvSet {
            project_id,
            scope_id,
            ..
        }
        | Mutation::KvDel {
            project_id,
            scope_id,
            ..
        }
        | Mutation::KvIncU256 {
            project_id,
            scope_id,
            ..
        }
        | Mutation::KvDecU256 {
            project_id,
            scope_id,
            ..
        }
        | Mutation::KvAddU256Ex {
            project_id,
            scope_id,
            ..
        }
        | Mutation::KvSubU256Ex {
            project_id,
            scope_id,
            ..
        }
        | Mutation::KvMaxU256 {
            project_id,
            scope_id,
            ..
        }
        | Mutation::KvMinU256 {
            project_id,
            scope_id,
            ..
        }
        | Mutation::KvMutateU256 {
            project_id,
            scope_id,
            ..
        }
        | Mutation::KvAddU64Ex {
            project_id,
            scope_id,
            ..
        }
        | Mutation::KvSubU64Ex {
            project_id,
            scope_id,
            ..
        }
        | Mutation::KvSubIntEx {
            project_id,
            scope_id,
            ..
        }
        | Mutation::KvAddI64Bounded {
            project_id,
            scope_id,
            ..
        }
        | Mutation::CounterAdd {
            project_id,
            scope_id,
            ..
        }
        | Mutation::KvMaxU64 {
            project_id,
            scope_id,
            ..
        }
        | Mutation::KvMinU64 {
            project_id,
            scope_id,
            ..
        }
        | Mutation::KvMutateU64 {
            project_id,
            scope_id,
            ..
        } => Some((project_id, scope_id)),
        _ => None,
    }
}

pub(super) fn token_namespace(token: &str) -> Option<&str> {
    if let Some(rest) = token.strip_prefix("kns:") {
        return Some(rest);
    }
    if let Some(rest) = token.strip_prefix("tr:")
        && let Some((prefix, _pk)) = rest.rsplit_once(':')
        && let Some((ns, _table)) = prefix.rsplit_once(':')
    {
        return Some(ns);
    }
    if let Some(rest) = token.strip_prefix("t:")
        && let Some((ns, _table)) = rest.rsplit_once(':')
    {
        return Some(ns);
    }
    if let Some(rest) = token.strip_prefix("k:")
        && let Some((ns, _key)) = rest.rsplit_once(':')
    {
        return Some(ns);
    }
    if let Some(rest) = token.strip_prefix("ob:")
        && let Some((ns, _instrument)) = rest.rsplit_once(':')
    {
        return Some(ns);
    }
    if let Some(rest) = token.strip_prefix("obm:")
        && let Some((ns, _tail)) = rest.split_once(':')
    {
        return Some(ns);
    }
    None
}

pub(in crate::commit::executor) fn revalidate_read_set_for_keyspace(
    keyspace: &Keyspace,
    envelope: &TransactionEnvelope,
) -> Result<(), AedbError> {
    for entry in &envelope.read_set.points {
        let current_version = match &entry.key {
            ReadKey::TableRow {
                project_id,
                scope_id,
                table_name,
                primary_key,
            } => keyspace.get_row_version(project_id, scope_id, table_name, primary_key),
            ReadKey::KvKey {
                project_id,
                scope_id,
                key,
            } => keyspace.try_kv_version(project_id, scope_id, key)?,
        };
        if current_version > envelope.base_seq || current_version > entry.version_at_read {
            return Err(AedbError::Conflict(format!(
                "read set conflict at seq {current_version}"
            )));
        }
    }
    for range in &envelope.read_set.ranges {
        let (current_max_version, current_structural_version) = match &range.range {
            ReadRange::TableRange {
                project_id,
                scope_id,
                table_name,
                start,
                end,
            } => (
                keyspace.max_row_version_in_encoded_range(
                    project_id,
                    scope_id,
                    table_name,
                    value_bound_to_encoded(start),
                    value_bound_to_encoded(end),
                ),
                keyspace.table_structural_version(project_id, scope_id, table_name),
            ),
            ReadRange::TablePrefix {
                project_id,
                scope_id,
                table_name,
                prefix,
            } => {
                // The prefix band's encoded key bounds — same bounds the scan used.
                let (start, end) = crate::query::executor::pk_prefix_scan_bounds(prefix);
                (
                    keyspace.max_row_version_in_encoded_range(
                        project_id,
                        scope_id,
                        table_name,
                        start,
                        end,
                    ),
                    keyspace.table_structural_version(project_id, scope_id, table_name),
                )
            }
            ReadRange::KvRange {
                project_id,
                scope_id,
                start,
                end,
            } => (
                keyspace.try_max_kv_version_in_range(
                    project_id,
                    scope_id,
                    bytes_bound_to_vec(start),
                    bytes_bound_to_vec(end),
                )?,
                keyspace.kv_structural_version(project_id, scope_id),
            ),
        };
        if current_max_version > envelope.base_seq
            || current_max_version > range.max_version_at_read
        {
            return Err(AedbError::Conflict(format!(
                "range read set conflict at seq {current_max_version}"
            )));
        }
        if current_structural_version > envelope.base_seq
            || current_structural_version > range.structural_version_at_read
        {
            return Err(AedbError::Conflict(format!(
                "range structural conflict at seq {current_structural_version}"
            )));
        }
    }
    Ok(())
}

pub(in crate::commit::executor) fn value_bound_to_encoded(
    bound: &ReadBound<Vec<crate::catalog::types::Value>>,
) -> Bound<EncodedKey> {
    match bound {
        ReadBound::Unbounded => Bound::Unbounded,
        ReadBound::Included(values) => Bound::Included(EncodedKey::from_values(values)),
        ReadBound::Excluded(values) => Bound::Excluded(EncodedKey::from_values(values)),
    }
}

pub(in crate::commit::executor) fn bytes_bound_to_vec(
    bound: &ReadBound<Vec<u8>>,
) -> Bound<Vec<u8>> {
    match bound {
        ReadBound::Unbounded => Bound::Unbounded,
        ReadBound::Included(value) => Bound::Included(value.clone()),
        ReadBound::Excluded(value) => Bound::Excluded(value.clone()),
    }
}
