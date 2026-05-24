use super::{
    Keyspace, KvData, KvEntry, NamespaceId, SmallKvEntry, collect_hot_kv_segment_entries,
    compact_kv_key, first_segment_position_for_start, get_sorted_segment_for_key,
    hot_kv_resident_cost, kv_entry_cost, persistent_value_ref_resident_cost, scan_kv_entries,
    segment_starts_after_end, segments_are_sorted_non_overlapping, small_kv_entry_cost,
};
use crate::catalog::types::{Row, Value};
use crate::commit::validation::KvIntegerMissingPolicy;
use crate::config::PrimaryIndexBackend;
use crate::error::AedbError;
use crate::storage::encoded_key::EncodedKey;
use crate::storage::kv_segment::KvSegmentStore;
use crate::storage::value_store::PersistentValueStore;
use std::ops::Bound;
use std::sync::Arc;
use tempfile::tempdir;

fn row(values: Vec<Value>) -> Row {
    Row::from_values(values)
}

#[test]
fn snapshot_isolation_works() {
    let project = "p";
    let scope = "app";
    let table = "t";

    let mut ks = Keyspace::default();
    ks.upsert_row(
        project,
        scope,
        table,
        vec![Value::Integer(1)],
        row(vec![Value::Text("A".into())]),
        1,
    );
    ks.upsert_row(
        project,
        scope,
        table,
        vec![Value::Integer(2)],
        row(vec![Value::Text("B".into())]),
        2,
    );
    ks.upsert_row(
        project,
        scope,
        table,
        vec![Value::Integer(3)],
        row(vec![Value::Text("C".into())]),
        3,
    );

    let s1 = ks.snapshot();

    ks.delete_row(project, scope, table, &[Value::Integer(2)], 4);
    ks.upsert_row(
        project,
        scope,
        table,
        vec![Value::Integer(4)],
        row(vec![Value::Text("D".into())]),
        4,
    );

    let s2 = ks.snapshot();

    let s1_rows = &s1.table(project, scope, table).expect("table").rows;
    assert!(
        s1_rows.contains_key(&EncodedKey::from_values(&[Value::Integer(1)])),
        "s1 should contain row 1"
    );
    assert!(
        s1_rows.contains_key(&EncodedKey::from_values(&[Value::Integer(2)])),
        "s1 should contain row 2"
    );
    assert!(
        s1_rows.contains_key(&EncodedKey::from_values(&[Value::Integer(3)])),
        "s1 should contain row 3"
    );
    assert!(
        !s1_rows.contains_key(&EncodedKey::from_values(&[Value::Integer(4)])),
        "s1 should not contain row 4"
    );

    let s2_rows = &s2.table(project, scope, table).expect("table").rows;
    assert!(s2_rows.contains_key(&EncodedKey::from_values(&[Value::Integer(1)])));
    assert!(!s2_rows.contains_key(&EncodedKey::from_values(&[Value::Integer(2)])));
    assert!(s2_rows.contains_key(&EncodedKey::from_values(&[Value::Integer(3)])));
    assert!(s2_rows.contains_key(&EncodedKey::from_values(&[Value::Integer(4)])));
}

#[test]
fn memory_estimate_is_non_zero_for_populated_state() {
    let mut ks = Keyspace::default();
    ks.upsert_row(
        "p",
        "app",
        "t",
        vec![Value::Integer(1)],
        row(vec![Value::Text("abc".into()), Value::U256([1u8; 32])]),
        1,
    );
    ks.kv_set("p", "app", b"k".to_vec(), b"v".to_vec(), 2)
        .expect("set kv");
    assert!(ks.estimate_memory_bytes() > 0);
}

#[test]
fn small_kv_values_are_compacted_but_materialize_for_reads() {
    let mut ks = Keyspace::default();
    let value = [7u8; 32].to_vec();

    ks.kv_set("p", "app", b"balance".to_vec(), value.clone(), 1)
        .expect("set compact value");

    let stored = ks
        .namespaces
        .get(&NamespaceId::project_scope("p", "app"))
        .expect("namespace")
        .kv
        .small_entries
        .get(&compact_kv_key(b"balance"))
        .expect("stored entry");
    assert_eq!(stored.value.as_slice(), value.as_slice());
    assert_eq!(stored.resident_value_len(), value.len());

    let read = ks.kv_get("p", "app", b"balance").expect("read value");
    assert_eq!(read.value, value);
    assert!(read.value_ref.is_none());
    assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());
}

#[test]
fn compact_kv_overwrite_and_delete_keep_memory_counter_exact() {
    let mut ks = Keyspace::default();
    ks.kv_set("p", "app", b"k".to_vec(), [1u8; 32].to_vec(), 1)
        .expect("set compact");
    let after_insert = ks.mem_bytes;
    assert_eq!(after_insert, ks.recompute_memory_bytes_full());

    ks.kv_set("p", "app", b"k".to_vec(), vec![2u8; 48], 2)
        .expect("overwrite with vec-backed inline value");
    assert!(ks.mem_bytes > after_insert);
    assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());

    assert!(ks.kv_del("p", "app", b"k", 3));
    assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());
    assert!(ks.kv_get("p", "app", b"k").is_none());
}

#[test]
fn disk_kv_segments_keep_cold_keys_off_heap_and_visible() {
    let dir = tempdir().expect("temp dir");
    let segment_store = Arc::new(KvSegmentStore::open(dir.path()).expect("open segment store"));
    let mut ks = Keyspace::default();
    ks.attach_kv_segment_store(Arc::clone(&segment_store));

    for entry_number in 0..128u64 {
        ks.kv_set(
            "p",
            "app",
            format!("k{entry_number:03}").into_bytes(),
            [entry_number as u8; 32].to_vec(),
            entry_number + 1,
        )
        .expect("set kv");
    }
    let before_flush = ks.estimate_memory_bytes();
    let after_flush = ks
        .flush_kv_to_segments_to_memory_target(0)
        .expect("flush to segment");
    assert!(after_flush < before_flush);
    ks.compact_kv_segments().expect("compact generations");
    ks.compact_kv_segments().expect("compact generations");
    let namespace = ks
        .namespace(&NamespaceId::project_scope("p", "app"))
        .expect("namespace");
    assert!(namespace.kv.entries.is_empty());
    assert!(namespace.kv.small_entries.is_empty());
    assert_eq!(namespace.kv.segments.len(), 1);

    assert_eq!(
        ks.kv_get("p", "app", b"k001").expect("k001").value,
        [1u8; 32].to_vec()
    );
    assert_eq!(ks.kv_scan_prefix("p", "app", b"k", 200).len(), 128);
    assert_eq!(ks.kv_version("p", "app", b"k002"), 3);

    assert!(ks.kv_del("p", "app", b"k001", 200));
    assert!(ks.kv_get("p", "app", b"k001").is_none());
    assert_eq!(ks.kv_scan_prefix("p", "app", b"k", 200).len(), 127);

    ks.kv_set("p", "app", b"k001".to_vec(), [3u8; 32].to_vec(), 201)
        .expect("rewrite k001");
    assert_eq!(
        ks.kv_get("p", "app", b"k001")
            .expect("k001 rewritten")
            .value,
        [3u8; 32].to_vec()
    );

    let checkpoint = ks
        .snapshot()
        .materialized_for_checkpoint()
        .expect("materialized checkpoint");
    let checkpoint_namespace = checkpoint
        .namespaces
        .get(&NamespaceId::project_scope("p", "app"))
        .expect("checkpoint namespace");
    assert!(checkpoint_namespace.kv.segments.is_empty());
    assert!(checkpoint_namespace.kv.segment_tombstones.is_empty());
    assert_eq!(checkpoint_namespace.kv.entries.len(), 128);
}

#[test]
fn memory_pressure_flushes_largest_hot_namespace_first() {
    let dir = tempdir().expect("temp dir");
    let segment_store = Arc::new(KvSegmentStore::open(dir.path()).expect("open segment store"));
    let mut ks = Keyspace::default();
    ks.attach_kv_segment_store(Arc::clone(&segment_store));

    for entry_number in 0..128u64 {
        ks.kv_set(
            "p",
            "hot",
            format!("hot:{entry_number:03}").into_bytes(),
            [entry_number as u8; 32].to_vec(),
            entry_number + 1,
        )
        .expect("set hot kv");
    }
    ks.kv_set("p", "cold", b"cold:001".to_vec(), [7u8; 32].to_vec(), 200)
        .expect("set cold kv");
    let hot_cost = hot_kv_resident_cost(
        &ks.namespace(&NamespaceId::project_scope("p", "hot"))
            .expect("hot namespace")
            .kv,
    );
    let target = ks.estimate_memory_bytes().saturating_sub(hot_cost / 2);

    let after_flush = ks
        .flush_kv_to_segments_to_memory_target(target)
        .expect("flush under pressure");

    assert!(after_flush <= target);
    let hot_namespace = ks
        .namespace(&NamespaceId::project_scope("p", "hot"))
        .expect("hot namespace");
    assert!(hot_namespace.kv.entries.is_empty());
    assert!(hot_namespace.kv.small_entries.is_empty());
    assert_eq!(hot_namespace.kv.segments.len(), 1);

    let cold_namespace = ks
        .namespace(&NamespaceId::project_scope("p", "cold"))
        .expect("cold namespace");
    assert!(cold_namespace.kv.segments.is_empty());
    assert!(
        cold_namespace
            .kv
            .small_entries
            .contains_key(&compact_kv_key(b"cold:001"))
    );
    assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());
}

#[test]
fn inline_rewrite_after_segment_delete_survives_refreeze() {
    let dir = tempdir().expect("temp dir");
    let segment_store = Arc::new(KvSegmentStore::open(dir.path()).expect("open segment store"));
    let mut ks = Keyspace::default();
    ks.attach_kv_segment_store(Arc::clone(&segment_store));

    ks.kv_set_inline("p", "app", b"k".to_vec(), b"old".to_vec(), 1);
    ks.flush_kv_to_segments_to_memory_target(0)
        .expect("flush old value");
    assert_eq!(
        ks.kv_get("p", "app", b"k").map(|entry| entry.value),
        Some(b"old".to_vec())
    );

    assert!(ks.kv_del("p", "app", b"k", 2));
    assert!(ks.kv_get("p", "app", b"k").is_none());

    ks.kv_set_inline("p", "app", b"k".to_vec(), b"new".to_vec(), 3);
    assert_eq!(
        ks.kv_get("p", "app", b"k").map(|entry| entry.value),
        Some(b"new".to_vec())
    );
    ks.flush_kv_to_segments_to_memory_target(0)
        .expect("flush rewritten value");

    assert_eq!(
        ks.kv_get("p", "app", b"k").map(|entry| entry.value),
        Some(b"new".to_vec())
    );
    assert_eq!(
        ks.kv_scan_prefix("p", "app", b"k", 10)
            .into_iter()
            .map(|(_, entry)| entry.value)
            .collect::<Vec<_>>(),
        vec![b"new".to_vec()]
    );
    assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());
}

#[test]
fn compacted_segment_prefix_scan_respects_limit_without_full_warmup() {
    let dir = tempdir().expect("temp dir");
    let segment_store = Arc::new(
        KvSegmentStore::open_with_block_cache_bytes(dir.path(), 256 * 1024)
            .expect("open segment store"),
    );
    let mut ks = Keyspace::default();
    ks.attach_kv_segment_store(Arc::clone(&segment_store));

    for number in 0..160u16 {
        ks.kv_set_inline(
            "p",
            "app",
            format!("k{number:03}").into_bytes(),
            vec![number as u8; 16],
            number as u64 + 1,
        );
    }
    ks.flush_kv_to_segments_to_memory_target(0)
        .expect("flush to segment");

    let rows = ks.kv_scan_prefix("p", "app", b"k", 10);

    assert_eq!(rows.len(), 10);
    assert_eq!(rows[0].0, b"k000".to_vec());
    assert_eq!(rows[9].0, b"k009".to_vec());
    let limited_resident_bytes = segment_store.block_cache_resident_bytes();
    assert!(limited_resident_bytes > 0);

    let all_rows = ks.kv_scan_prefix("p", "app", b"k", usize::MAX);
    assert_eq!(all_rows.len(), 160);
    assert!(
        segment_store.block_cache_resident_bytes() > limited_resident_bytes,
        "unlimited scan should warm additional segment blocks"
    );
}

#[test]
fn materializing_single_segment_checkpoint_does_not_warm_block_cache() {
    let dir = tempdir().expect("temp dir");
    let segment_store = Arc::new(
        KvSegmentStore::open_with_block_cache_bytes(dir.path(), 256 * 1024)
            .expect("open segment store"),
    );
    let mut ks = Keyspace::default();
    ks.attach_kv_segment_store(Arc::clone(&segment_store));

    for number in 0..160u16 {
        ks.kv_set_inline(
            "p",
            "app",
            format!("k{number:03}").into_bytes(),
            vec![number as u8; 16],
            number as u64 + 1,
        );
    }
    ks.flush_kv_to_segments_to_memory_target(0)
        .expect("flush to segment");
    assert_eq!(segment_store.block_cache_resident_bytes(), 0);

    let materialized = ks
        .snapshot()
        .materialized_for_checkpoint()
        .expect("materialize checkpoint");

    assert_eq!(segment_store.block_cache_resident_bytes(), 0);
    let namespace = materialized
        .namespaces
        .get(&NamespaceId::project_scope("p", "app"))
        .expect("namespace");
    assert!(namespace.kv.segments.is_empty());
    assert_eq!(
        namespace.kv.entries.len() + namespace.kv.small_entries.len(),
        160
    );
}

#[test]
fn disjoint_segment_prefix_scan_respects_limit_without_full_merge() {
    let dir = tempdir().expect("temp dir");
    let segment_store = Arc::new(
        KvSegmentStore::open_with_block_cache_bytes(dir.path(), 256 * 1024)
            .expect("open segment store"),
    );
    let mut ks = Keyspace::default();
    ks.attach_kv_segment_store(Arc::clone(&segment_store));

    for segment_number in 0..3u16 {
        for entry_number in 0..70u16 {
            let number = segment_number * 100 + entry_number;
            ks.kv_set_inline(
                "p",
                "app",
                format!("k{number:03}").into_bytes(),
                vec![number as u8; 16],
                number as u64 + 1,
            );
        }
        ks.flush_kv_to_segments_to_memory_target(0)
            .expect("flush to segment");
    }
    let namespace = ks
        .namespace(&NamespaceId::project_scope("p", "app"))
        .expect("namespace");
    assert_eq!(namespace.kv.segments.len(), 3);

    let cold_rows = scan_kv_entries(
        &namespace.kv,
        Bound::Included(b"k100".to_vec()),
        Bound::Excluded(b"k200".to_vec()),
        10,
        None,
        Some(segment_store.as_ref()),
        false,
    )
    .expect("cold scan");
    assert_eq!(cold_rows.len(), 10);
    assert_eq!(cold_rows[0].0, b"k100".to_vec());
    assert_eq!(cold_rows[9].0, b"k109".to_vec());
    assert_eq!(segment_store.block_cache_resident_bytes(), 0);

    let rows = ks.kv_scan_prefix("p", "app", b"k", 10);

    assert_eq!(rows.len(), 10);
    assert_eq!(rows[0].0, b"k000".to_vec());
    assert_eq!(rows[9].0, b"k009".to_vec());
    let limited_resident_bytes = segment_store.block_cache_resident_bytes();
    assert!(limited_resident_bytes > 0);

    let all_rows = ks.kv_scan_prefix("p", "app", b"k", usize::MAX);
    assert_eq!(all_rows.len(), 210);
    assert_eq!(all_rows[70].0, b"k100".to_vec());
    assert_eq!(all_rows[140].0, b"k200".to_vec());
    assert!(
        segment_store.block_cache_resident_bytes() > limited_resident_bytes,
        "full scan should warm blocks from later disjoint segments"
    );
}

#[test]
fn disjoint_segment_point_get_finds_only_possible_range() {
    let dir = tempdir().expect("temp dir");
    let segment_store = Arc::new(KvSegmentStore::open(dir.path()).expect("open segment store"));
    let mut ks = Keyspace::default();
    ks.attach_kv_segment_store(Arc::clone(&segment_store));

    for segment_number in 0..3u16 {
        for entry_number in 0..4u16 {
            let number = segment_number * 100 + entry_number;
            ks.kv_set_inline(
                "p",
                "app",
                format!("k{number:03}").into_bytes(),
                vec![number as u8; 4],
                number as u64 + 1,
            );
        }
        ks.flush_kv_to_segments_to_memory_target(0)
            .expect("flush to segment");
    }

    assert_eq!(
        ks.kv_get("p", "app", b"k000").map(|entry| entry.value),
        Some(vec![0; 4])
    );
    assert_eq!(
        ks.kv_get("p", "app", b"k101").map(|entry| entry.value),
        Some(vec![101; 4])
    );
    assert_eq!(
        ks.kv_get("p", "app", b"k203").map(|entry| entry.value),
        Some(vec![203; 4])
    );
    assert!(ks.kv_get("p", "app", b"k050").is_none());
}

#[test]
fn sorted_segment_range_helpers_detect_common_append_layout() {
    let dir = tempdir().expect("temp dir");
    let segment_store = Arc::new(KvSegmentStore::open(dir.path()).expect("open segment store"));
    let mut ks = Keyspace::default();
    ks.attach_kv_segment_store(Arc::clone(&segment_store));

    for segment_number in 0..3u16 {
        for entry_number in 0..4u16 {
            let number = segment_number * 100 + entry_number;
            ks.kv_set_inline(
                "p",
                "app",
                format!("k{number:03}").into_bytes(),
                vec![number as u8; 4],
                number as u64 + 1,
            );
        }
        ks.flush_kv_to_segments_to_memory_target(0)
            .expect("flush to segment");
    }

    let namespace = ks
        .namespace(&NamespaceId::project_scope("p", "app"))
        .expect("namespace");
    assert!(segments_are_sorted_non_overlapping(&namespace.kv.segments));
    assert_eq!(
        get_sorted_segment_for_key(&namespace.kv.segments, b"k101")
            .map(|segment| segment.min_key.clone()),
        Some(b"k100".to_vec())
    );
    assert!(get_sorted_segment_for_key(&namespace.kv.segments, b"k050").is_none());
    assert_eq!(
        first_segment_position_for_start(
            &namespace.kv.segments,
            &Bound::Included(b"k101".to_vec())
        ),
        1
    );
    assert_eq!(
        first_segment_position_for_start(
            &namespace.kv.segments,
            &Bound::Excluded(b"k103".to_vec())
        ),
        2
    );
    assert!(segment_starts_after_end(
        &namespace.kv.segments[2],
        &Bound::Excluded(b"k200".to_vec())
    ));
}

#[test]
fn hot_kv_segment_collection_merges_sorted_maps_without_resort() {
    let mut kv = KvData::default();
    kv.entries.insert(
        b"k001".to_vec(),
        KvEntry::inline(b"normal-1".to_vec(), 1, 1),
    );
    kv.entries.insert(
        b"k003".to_vec(),
        KvEntry::inline(b"normal-3".to_vec(), 3, 3),
    );
    kv.small_entries.insert(
        compact_kv_key(b"k000"),
        SmallKvEntry::new(b"small-0", 10, 10).expect("small entry"),
    );
    kv.small_entries.insert(
        compact_kv_key(b"k002"),
        SmallKvEntry::new(b"small-2", 12, 12).expect("small entry"),
    );
    kv.small_entries.insert(
        compact_kv_key(b"k003"),
        SmallKvEntry::new(b"small-3", 13, 13).expect("small entry"),
    );

    let (entries, resident_cost) = collect_hot_kv_segment_entries(&kv);

    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.key.clone())
            .collect::<Vec<_>>(),
        vec![
            b"k000".to_vec(),
            b"k001".to_vec(),
            b"k002".to_vec(),
            b"k003".to_vec()
        ]
    );
    assert_eq!(entries[3].entry.value, b"small-3".to_vec());
    assert_eq!(
        resident_cost,
        kv.entries
            .iter()
            .map(|(key, entry)| kv_entry_cost(key.len(), entry.resident_memory_value_len()))
            .chain(
                kv.small_entries.iter().map(|(key, entry)| {
                    small_kv_entry_cost(key.len(), entry.resident_value_len())
                })
            )
            .sum::<usize>()
    );
}

#[test]
fn overlapping_segment_point_get_preserves_newest_value() {
    let dir = tempdir().expect("temp dir");
    let segment_store = Arc::new(KvSegmentStore::open(dir.path()).expect("open segment store"));
    let mut ks = Keyspace::default();
    ks.attach_kv_segment_store(Arc::clone(&segment_store));

    ks.kv_set_inline("p", "app", b"k".to_vec(), b"old".to_vec(), 1);
    ks.flush_kv_to_segments_to_memory_target(0)
        .expect("flush old value");
    ks.kv_set_inline("p", "app", b"k".to_vec(), b"new".to_vec(), 2);
    ks.flush_kv_to_segments_to_memory_target(0)
        .expect("flush new value");

    let namespace = ks
        .namespace(&NamespaceId::project_scope("p", "app"))
        .expect("namespace");
    assert_eq!(namespace.kv.segments.len(), 2);
    assert_eq!(
        ks.kv_get("p", "app", b"k").map(|entry| entry.value),
        Some(b"new".to_vec())
    );
    assert_eq!(ks.kv_version("p", "app", b"k"), 2);
}

#[test]
fn kv_segment_compaction_preserves_latest_values_and_tombstones() {
    let dir = tempdir().expect("temp dir");
    let segment_store = Arc::new(KvSegmentStore::open(dir.path()).expect("open segment store"));
    let mut ks = Keyspace::default();
    ks.attach_kv_segment_store(Arc::clone(&segment_store));

    for generation_number in 0..6u64 {
        ks.kv_set(
            "p",
            "app",
            b"shared".to_vec(),
            [generation_number as u8; 32].to_vec(),
            generation_number + 1,
        )
        .expect("set shared");
        ks.kv_set(
            "p",
            "app",
            format!("unique-{generation_number:02}").into_bytes(),
            [generation_number as u8; 32].to_vec(),
            generation_number + 10,
        )
        .expect("set unique");
        ks.flush_kv_to_segments_to_memory_target(0)
            .expect("flush generation");
    }

    ks.compact_kv_segments().expect("compact generations");
    let namespace = ks
        .namespace(&NamespaceId::project_scope("p", "app"))
        .expect("namespace");
    assert_eq!(
        namespace.kv.segments.len(),
        1,
        "flush should compact segment fanout"
    );
    assert_eq!(
        ks.kv_get("p", "app", b"shared").expect("shared").value,
        [5u8; 32].to_vec()
    );

    assert!(ks.kv_del("p", "app", b"unique-01", 100));
    ks.compact_kv_segments().expect("compact tombstone");
    assert!(ks.kv_get("p", "app", b"unique-01").is_none());
    assert_eq!(ks.kv_scan_prefix("p", "app", b"unique-", 10).len(), 5);

    let namespace = ks
        .namespace(&NamespaceId::project_scope("p", "app"))
        .expect("namespace");
    assert!(namespace.kv.segment_tombstones.is_empty());
    assert_eq!(namespace.kv.segments.len(), 1);

    let referenced = ks.kv_segment_filenames();
    let before_reclaim = segment_store
        .list_segment_filenames()
        .expect("list segments before reclaim");
    assert!(
        before_reclaim.len() > referenced.len(),
        "compaction should leave old segment files until explicit GC"
    );
    let reclaimed = segment_store
        .reclaim_unreferenced_segments(&referenced)
        .expect("reclaim unreferenced segments");
    assert!(reclaimed > 0);
    let after_reclaim = segment_store
        .list_segment_filenames()
        .expect("list segments after reclaim");
    assert_eq!(after_reclaim.len(), referenced.len());
    for filename in after_reclaim {
        assert!(referenced.contains(&filename));
    }
}

#[test]
fn drop_project_preserves_global_and_system_namespaces() {
    let mut ks = Keyspace::default();
    ks.namespace_mut(NamespaceId::System)
        .tables
        .entry("permissions".into())
        .or_default();
    ks.namespace_mut(NamespaceId::Global)
        .tables
        .entry("users".into())
        .or_default();
    ks.upsert_row(
        "tenant",
        "app",
        "users",
        vec![Value::Integer(1)],
        row(vec![Value::Text("alice".into())]),
        1,
    );

    ks.drop_project("tenant");

    assert!(ks.namespace(&NamespaceId::System).is_some());
    assert!(ks.namespace(&NamespaceId::Global).is_some());
    assert!(
        ks.namespace(&NamespaceId::Project("tenant::app".into()))
            .is_none()
    );
}

#[test]
fn art_experimental_backend_uses_primary_ordmap_storage() {
    let mut ks = Keyspace::with_backend(PrimaryIndexBackend::OrdMap);
    ks.upsert_row(
        "p",
        "app",
        "users",
        vec![Value::Integer(1)],
        row(vec![Value::Text("alice".into())]),
        11,
    );
    ks.set_backend(PrimaryIndexBackend::ArtExperimental);
    assert_eq!(
        ks.primary_index_backend,
        PrimaryIndexBackend::ArtExperimental
    );
    let row = ks
        .get_row_by_encoded(
            "p",
            "app",
            "users",
            &EncodedKey::from_values(&[Value::Integer(1)]),
        )
        .expect("row");
    assert_eq!(row.values[0], Value::Text("alice".into()));
    assert_eq!(
        ks.get_row_version("p", "app", "users", &[Value::Integer(1)]),
        11
    );
}

#[test]
fn kv_prefix_scans_are_lexicographically_bounded() {
    let mut ks = Keyspace::default();
    ks.kv_set("p", "app", b"ob:a:1".to_vec(), b"v1".to_vec(), 1)
        .expect("set a1");
    ks.kv_set("p", "app", b"ob:a:2".to_vec(), b"v2".to_vec(), 2)
        .expect("set a2");
    ks.kv_set("p", "app", b"ob:b:1".to_vec(), b"v3".to_vec(), 3)
        .expect("set b1");
    ks.kv_set("p", "app", b"zz".to_vec(), b"v4".to_vec(), 4)
        .expect("set zz");

    let rows = ks.kv_scan_prefix("p", "app", b"ob:a:", 10);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, b"ob:a:1".to_vec());
    assert_eq!(rows[1].0, b"ob:a:2".to_vec());

    let refs = ks.kv_scan_prefix_ref("p", "app", b"ob:", 10);
    assert_eq!(refs.len(), 3);
    assert!(refs.iter().all(|(k, _)| k.starts_with(b"ob:")));
}

#[test]
fn materialized_checkpoint_hydrates_spilled_kv_without_mutating_source() {
    let dir = tempdir().expect("temp");
    let store = Arc::new(
        PersistentValueStore::open_with_hot_cache_bytes(dir.path(), 0).expect("open store"),
    );
    let mut ks = Keyspace::default();
    ks.attach_persistent_value_store(store, 4)
        .expect("attach value store");
    ks.kv_set("p", "app", b"big".to_vec(), b"large-value".to_vec(), 1)
        .expect("set spilled value");
    ks.kv_set("p", "app", b"tiny".to_vec(), b"tiny".to_vec(), 2)
        .expect("set inline value");

    let snapshot = ks.snapshot();
    let source_big = snapshot
        .namespaces
        .get(&NamespaceId::project_scope("p", "app"))
        .expect("namespace")
        .kv
        .entries
        .get(&b"big".to_vec())
        .expect("source big entry");
    assert!(source_big.value_ref.is_some());
    assert!(source_big.value.is_empty());

    let materialized = snapshot
        .materialized_for_checkpoint()
        .expect("materialize checkpoint");
    let materialized_namespace = materialized
        .namespaces
        .get(&NamespaceId::project_scope("p", "app"))
        .expect("materialized namespace");
    let materialized_big = materialized_namespace
        .kv
        .entries
        .get(&b"big".to_vec())
        .expect("materialized big entry");
    assert_eq!(materialized_big.value, b"large-value");
    assert!(materialized_big.value_ref.is_none());
    assert!(materialized.value_store.is_none());
    assert_eq!(
        materialized.mem_bytes,
        materialized.recompute_memory_bytes_full()
    );

    let source_big_after = snapshot
        .namespaces
        .get(&NamespaceId::project_scope("p", "app"))
        .expect("source namespace")
        .kv
        .entries
        .get(&b"big".to_vec())
        .expect("source big entry after materialize");
    assert!(source_big_after.value_ref.is_some());
    assert!(source_big_after.value.is_empty());
}

#[test]
fn target_spill_counts_persistent_value_refs_and_skips_tiny_values() {
    let dir = tempdir().expect("temp");
    let store = Arc::new(
        PersistentValueStore::open_with_hot_cache_bytes(dir.path(), 0).expect("open store"),
    );
    let mut ks = Keyspace::default();
    ks.attach_persistent_value_store(store, usize::MAX)
        .expect("attach value store");
    let large_value = vec![0xAB; 128];
    let tiny_value = [0xCD; 32].to_vec();

    ks.kv_set("p", "app", b"large".to_vec(), large_value.clone(), 1)
        .expect("set large");
    ks.kv_set("p", "app", b"tiny".to_vec(), tiny_value.clone(), 2)
        .expect("set tiny");
    let before_spill = ks.mem_bytes;

    let after_spill = ks.spill_kv_values_to_memory_target(0).expect("spill");

    assert_eq!(after_spill, ks.mem_bytes);
    assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());
    assert!(ks.mem_bytes < before_spill);

    let namespace = ks
        .namespaces
        .get(&NamespaceId::project_scope("p", "app"))
        .expect("namespace");
    let large_entry = namespace
        .kv
        .entries
        .get(&b"large".to_vec())
        .expect("large entry");
    assert!(large_entry.value.is_empty());
    assert!(large_entry.value_ref.is_some());
    assert_eq!(
        large_entry.resident_memory_value_len(),
        persistent_value_ref_resident_cost()
    );
    assert!(
        namespace
            .kv
            .small_entries
            .contains_key(&compact_kv_key(b"tiny")),
        "spilling a tiny compact value would increase resident memory"
    );

    assert_eq!(
        ks.try_kv_get("p", "app", b"large")
            .expect("read large")
            .expect("large value")
            .value,
        large_value
    );
    assert_eq!(
        ks.try_kv_get("p", "app", b"tiny")
            .expect("read tiny")
            .expect("tiny value")
            .value,
        tiny_value
    );
}

#[test]
fn kv_add_i64_bounded_enforces_min_value() {
    let mut ks = Keyspace::default();
    ks.kv_add_i64_bounded(
        "p",
        "app",
        b"house/balance".to_vec(),
        100,
        &KvIntegerMissingPolicy::TreatAsZero,
        Some(-3_000),
        None,
        1,
    )
    .expect("seed signed kv value");
    ks.kv_add_i64_bounded(
        "p",
        "app",
        b"house/balance".to_vec(),
        -3_000,
        &KvIntegerMissingPolicy::TreatAsZero,
        Some(-3_000),
        None,
        2,
    )
    .expect("drawdown above floor should apply");
    let err = ks
        .kv_add_i64_bounded(
            "p",
            "app",
            b"house/balance".to_vec(),
            -101,
            &KvIntegerMissingPolicy::TreatAsZero,
            Some(-3_000),
            None,
            3,
        )
        .expect_err("drawdown below floor must fail");
    assert!(matches!(err, AedbError::Validation(_)));
    let value = ks
        .try_kv_get("p", "app", b"house/balance")
        .expect("read key")
        .expect("value exists")
        .value;
    assert_eq!(super::decode_i64(&value).expect("decode i64"), -2_900);
}

#[test]
fn mem_bytes_running_counter_matches_full_walk() {
    // Exercise every tracked mutation site and assert the running counter
    // stays in sync with a fresh O(N) recompute.
    let mut ks = Keyspace::default();
    assert_eq!(ks.mem_bytes, 0);
    assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());

    // Table inserts.
    for i in 0..50_i64 {
        ks.upsert_row(
            "p",
            "app",
            "users",
            vec![Value::Integer(i)],
            Row {
                values: vec![Value::Integer(i), Value::Text(format!("name_{i}").into())],
            },
            i as u64 + 1,
        );
    }
    assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());

    // Update existing row (replaces, delta = new - old).
    ks.upsert_row(
        "p",
        "app",
        "users",
        vec![Value::Integer(0)],
        Row {
            values: vec![
                Value::Integer(0),
                Value::Text("a-much-longer-name-than-original".into()),
            ],
        },
        100,
    );
    assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());

    // Delete some rows.
    for i in 0..10_i64 {
        ks.delete_row("p", "app", "users", &[Value::Integer(i)], 200);
    }
    assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());

    // KV mutations.
    for i in 0..30_u64 {
        ks.kv_set(
            "p",
            "app",
            format!("key:{i}").into_bytes(),
            vec![0xAB; 64],
            i + 1,
        )
        .expect("set kv");
    }
    assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());

    // KV update (replaces).
    ks.kv_set("p", "app", b"key:0".to_vec(), vec![0xCD; 256], 50)
        .expect("update kv");
    assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());

    // KV delete.
    for i in 0..5_u64 {
        ks.kv_del("p", "app", format!("key:{i}").as_bytes(), 60);
    }
    assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());

    ks.drop_table("p", "app", "users");
    assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());

    ks.drop_scope("p", "app");
    assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());

    ks.drop_project("p");
    assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());
    assert_eq!(ks.mem_bytes, 0);
}

#[test]
fn kv_set_many_same_namespace_updates_entries_and_memory() {
    let mut ks = Keyspace::default();
    let entries = [
        (b"a".to_vec(), b"one".to_vec()),
        (b"b".to_vec(), b"two".to_vec()),
    ];
    ks.kv_set_many_same_namespace("p", "app", entries.iter().map(|(k, v)| (k, v)), 7)
        .expect("batch set");
    assert_eq!(
        ks.kv_get("p", "app", b"a").map(|entry| entry.value),
        Some(b"one".to_vec())
    );
    assert_eq!(
        ks.kv_get("p", "app", b"b").map(|entry| entry.value),
        Some(b"two".to_vec())
    );
    assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());

    let overwrite = [(b"a".to_vec(), b"three".to_vec())];
    ks.kv_set_many_same_namespace("p", "app", overwrite.iter().map(|(k, v)| (k, v)), 8)
        .expect("batch overwrite");
    let overwritten = ks.kv_get("p", "app", b"a").expect("overwritten");
    assert_eq!(overwritten.value, b"three".to_vec());
    assert_eq!(overwritten.created_at, 7);
    assert_eq!(overwritten.version, 8);
    assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());
}

#[test]
fn kv_set_many_same_namespace_spills_large_values_and_keeps_them_hot() {
    let dir = tempdir().expect("temp");
    let store = Arc::new(
        PersistentValueStore::open_with_hot_cache_bytes(dir.path(), 128).expect("open store"),
    );
    let mut ks = Keyspace::default();
    ks.attach_persistent_value_store(Arc::clone(&store), 4)
        .expect("attach");
    let entries = [
        (b"big-a".to_vec(), b"large-value-a".to_vec()),
        (b"big-b".to_vec(), b"large-value-b".to_vec()),
    ];

    ks.kv_set_many_same_namespace("p", "app", entries.iter().map(|(k, v)| (k, v)), 11)
        .expect("batch set");

    let namespace = ks
        .namespaces
        .get(&NamespaceId::project_scope("p", "app"))
        .expect("namespace");
    for (key, value) in &entries {
        let entry = namespace.kv.entries.get(key).expect("entry");
        entry.value_ref.as_ref().expect("spilled value ref");
        assert!(entry.value.is_empty());
        assert_eq!(
            ks.try_kv_get("p", "app", key)
                .expect("read")
                .expect("value")
                .value,
            *value
        );
    }
    assert!(
        store.hot_cache_resident_bytes() > 0,
        "spilled batch values should be hot immediately after write"
    );
    assert_eq!(ks.mem_bytes, ks.recompute_memory_bytes_full());
}

#[test]
fn refresh_mem_bytes_recovers_from_external_construction() {
    // Simulates the checkpoint-load path: a Keyspace built from external
    // data with `mem_bytes` defaulting to 0 must be reseedable.
    let mut ks = Keyspace::default();
    for i in 0..10_i64 {
        ks.upsert_row(
            "p",
            "app",
            "t",
            vec![Value::Integer(i)],
            Row {
                values: vec![Value::Integer(i)],
            },
            i as u64 + 1,
        );
    }
    let expected = ks.mem_bytes;
    ks.mem_bytes = 0;
    ks.refresh_mem_bytes();
    assert_eq!(ks.mem_bytes, expected);
}
