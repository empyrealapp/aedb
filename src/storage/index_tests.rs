use super::{extract_index_key, extract_index_key_encoded};
use crate::catalog::schema::{ColumnDef, TableSchema};
use crate::catalog::types::{ColumnType, Row, Value};
use crate::storage::encoded_key::EncodedKey;
use crate::storage::keyspace::memory_accounting::secondary_index_mem_cost;
use crate::storage::keyspace::{SecondaryIndex, SecondaryIndexStore};
use std::ops::Bound;

fn encoded(values: &[Value]) -> EncodedKey {
    EncodedKey::from_values(values)
}

/// The signed deltas returned by `insert`/`remove` must always sum to exactly
/// what `secondary_index_mem_cost` reports, or the keyspace `mem_bytes` running
/// counter drifts from `recompute_memory_bytes_full()` and the checkpoint
/// loader's consistency assert fails. Exercise all three store kinds with
/// inserts, duplicate inserts, partial and full removals, and no-op removals.
fn assert_delta_tracks_cost(mut index: SecondaryIndex) {
    let key = |n: i64| EncodedKey::from_values(&[Value::Integer(n)]);
    let pk = |n: i64| EncodedKey::from_values(&[Value::Integer(1000 + n)]);

    let mut running: i64 = 0;
    let mut step = |index: &mut SecondaryIndex, delta: i64| {
        running += delta;
        assert_eq!(
            running,
            secondary_index_mem_cost(index) as i64,
            "running delta diverged from recomputed cost"
        );
    };

    // Distinct values, each with one posting.
    for n in 0..8 {
        let d = index.insert(key(n), pk(n));
        step(&mut index, d);
    }
    // Second posting under an existing value (multi-valued stores only grow;
    // UniqueHash overwrites).
    for n in 0..8 {
        let d = index.insert(key(n), pk(n + 100));
        step(&mut index, d);
    }
    // Duplicate insert of an existing (value, pk) must be a no-op (delta 0).
    let d = index.insert(key(0), pk(0));
    step(&mut index, d);
    // Remove a posting that may or may not exist.
    for n in 0..8 {
        let d = index.remove(&key(n), &pk(n));
        step(&mut index, d);
    }
    // No-op remove of an already-gone posting.
    let d = index.remove(&key(0), &pk(0));
    step(&mut index, d);
    // Remove the remaining postings, draining the index back toward empty.
    for n in 0..8 {
        let d = index.remove(&key(n), &pk(n + 100));
        step(&mut index, d);
    }
}

#[test]
fn index_mem_delta_tracks_recomputed_cost_btree() {
    assert_delta_tracks_cost(SecondaryIndex {
        store: SecondaryIndexStore::BTree(im::OrdMap::new()),
        columns_bitmask: 0,
        partial_filter: None,
        ..Default::default()
    });
}

#[test]
fn index_mem_delta_tracks_recomputed_cost_hash() {
    assert_delta_tracks_cost(SecondaryIndex {
        store: SecondaryIndexStore::Hash(im::HashMap::new()),
        columns_bitmask: 0,
        partial_filter: None,
        ..Default::default()
    });
}

#[test]
fn index_mem_delta_tracks_recomputed_cost_unique_hash() {
    assert_delta_tracks_cost(SecondaryIndex {
        store: SecondaryIndexStore::UniqueHash(im::HashMap::new()),
        columns_bitmask: 0,
        partial_filter: None,
        ..Default::default()
    });
}

#[test]
fn secondary_index_insert_remove_and_range() {
    let mut secondary_index = SecondaryIndex::default();
    secondary_index.insert(
        EncodedKey::from_values(&[Value::Integer(10)]),
        EncodedKey::from_values(&[Value::Integer(1)]),
    );
    secondary_index.insert(
        EncodedKey::from_values(&[Value::Integer(20)]),
        EncodedKey::from_values(&[Value::Integer(2)]),
    );
    secondary_index.insert(
        EncodedKey::from_values(&[Value::Integer(30)]),
        EncodedKey::from_values(&[Value::Integer(3)]),
    );

    let eq = secondary_index.scan_eq(&EncodedKey::from_values(&[Value::Integer(20)]));
    assert_eq!(eq, vec![EncodedKey::from_values(&[Value::Integer(2)])]);

    let range = secondary_index.scan_range(
        Bound::Included(EncodedKey::from_values(&[Value::Integer(15)])),
        Bound::Included(EncodedKey::from_values(&[Value::Integer(30)])),
    );
    assert_eq!(range.len(), 2);

    secondary_index.remove(
        &EncodedKey::from_values(&[Value::Integer(20)]),
        &EncodedKey::from_values(&[Value::Integer(2)]),
    );
    assert!(
        secondary_index
            .scan_eq(&EncodedKey::from_values(&[Value::Integer(20)]))
            .is_empty()
    );
}

#[test]
fn secondary_index_ordered_window_applies_offset_and_reverse_order() {
    let mut secondary_index = SecondaryIndex::default();
    for (key, pk) in [(10, 1), (10, 2), (20, 3), (30, 4), (30, 5), (40, 6)] {
        secondary_index.insert(
            encoded(&[Value::Integer(key)]),
            encoded(&[Value::Integer(pk)]),
        );
    }

    let forward = secondary_index.scan_prefix_window_ordered(None, 2, 3, false);
    assert_eq!(
        forward,
        vec![
            encoded(&[Value::Integer(3)]),
            encoded(&[Value::Integer(4)]),
            encoded(&[Value::Integer(5)])
        ]
    );

    let reverse = secondary_index.scan_prefix_window_ordered(None, 1, 4, true);
    assert_eq!(
        reverse,
        vec![
            encoded(&[Value::Integer(4)]),
            encoded(&[Value::Integer(5)]),
            encoded(&[Value::Integer(3)]),
            encoded(&[Value::Integer(1)])
        ]
    );
}

#[test]
fn secondary_index_ordered_prefix_window_respects_prefix_bounds() {
    let mut secondary_index = SecondaryIndex::default();
    for (account, ts, pk) in [
        (1, 10, 101),
        (1, 20, 102),
        (1, 30, 103),
        (2, 10, 201),
        (2, 20, 202),
    ] {
        secondary_index.insert(
            encoded(&[Value::Integer(account), Value::Integer(ts)]),
            encoded(&[Value::Integer(pk)]),
        );
    }

    let prefix = encoded(&[Value::Integer(1)]);
    let forward = secondary_index.scan_prefix_window_ordered(Some(&prefix), 1, 8, false);
    assert_eq!(
        forward,
        vec![
            encoded(&[Value::Integer(102)]),
            encoded(&[Value::Integer(103)])
        ]
    );

    let reverse = secondary_index.scan_prefix_window_ordered(Some(&prefix), 0, 2, true);
    assert_eq!(
        reverse,
        vec![
            encoded(&[Value::Integer(103)]),
            encoded(&[Value::Integer(102)])
        ]
    );
}

#[test]
fn secondary_index_window_does_not_preallocate_unbounded_limit() {
    let mut secondary_index = SecondaryIndex::default();
    for (key, pk) in [(10, 1), (20, 2), (30, 3)] {
        secondary_index.insert(
            encoded(&[Value::Integer(key)]),
            encoded(&[Value::Integer(pk)]),
        );
    }

    let rows = secondary_index.scan_prefix_window_ordered(None, 0, usize::MAX, false);

    assert_eq!(
        rows,
        vec![
            encoded(&[Value::Integer(1)]),
            encoded(&[Value::Integer(2)]),
            encoded(&[Value::Integer(3)])
        ]
    );
}

#[test]
fn secondary_index_range_does_not_preallocate_unbounded_limit() {
    let mut secondary_index = SecondaryIndex::default();
    for (key, pk) in [(10, 1), (20, 2), (30, 3)] {
        secondary_index.insert(
            encoded(&[Value::Integer(key)]),
            encoded(&[Value::Integer(pk)]),
        );
    }

    let rows = secondary_index.scan_range_limit(
        Bound::Included(encoded(&[Value::Integer(10)])),
        Bound::Included(encoded(&[Value::Integer(30)])),
        usize::MAX,
    );

    assert_eq!(
        rows,
        vec![
            encoded(&[Value::Integer(1)]),
            encoded(&[Value::Integer(2)]),
            encoded(&[Value::Integer(3)])
        ]
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

#[test]
fn extract_index_key_pads_short_rows_with_null() {
    // Schema declares three columns but the row was written under an older
    // schema with only the first value. Index extraction must not panic on
    // the missing slot; it should substitute Null so migrations against
    // legacy rows can complete.
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
                nullable: true,
            },
            ColumnDef {
                name: "tag".into(),
                col_type: ColumnType::Text,
                nullable: true,
            },
        ],
        primary_key: vec!["id".into()],
        constraints: Vec::new(),
        foreign_keys: Vec::new(),
    };
    let short_row = Row::from_values(vec![Value::Integer(1)]);
    let key =
        extract_index_key(&short_row, &schema, &["age".into(), "tag".into()]).expect("extract");
    assert_eq!(key, vec![Value::Null, Value::Null]);
    let encoded = extract_index_key_encoded(&short_row, &schema, &["age".into(), "tag".into()])
        .expect("encoded");
    assert_eq!(
        encoded,
        EncodedKey::from_values(&[Value::Null, Value::Null])
    );
}
