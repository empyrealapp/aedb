use super::{
    KvSegmentEntry, KvSegmentStore, block_starts_after_end, first_block_position_for_start,
};
use crate::error::AedbError;
use crate::storage::keyspace::KvEntry;
use std::collections::HashSet;
use std::io::{Seek, SeekFrom, Write};
use std::ops::Bound;

fn entry(value: u8, version: u64) -> KvEntry {
    KvEntry {
        value: vec![value; 32],
        version,
        created_at: version,
        value_ref: None,
    }
}

#[test]
fn block_cache_hit_and_miss_counters_track_segment_reads() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = KvSegmentStore::open(dir.path()).expect("open store");
    let meta = store
        .write_segment(
            "test",
            vec![KvSegmentEntry {
                key: b"key".to_vec(),
                entry: entry(1, 1),
            }],
        )
        .expect("write segment");

    assert_eq!(store.block_cache_hits(), 0);
    assert_eq!(store.block_cache_misses(), 0);

    // First read populates the cache (miss).
    store.read_segment(&meta).expect("first read");
    assert_eq!(store.block_cache_misses(), 1);
    assert_eq!(store.block_cache_hits(), 0);

    // Second read is served from the block cache (hit).
    store.read_segment(&meta).expect("second read");
    assert_eq!(store.block_cache_hits(), 1);
    assert_eq!(store.block_cache_misses(), 1);
}

#[test]
fn reclaim_skips_pending_publish_segment_until_published() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = KvSegmentStore::open(dir.path()).expect("open store");
    let meta = store
        .write_segment(
            "test",
            vec![KvSegmentEntry {
                key: b"key".to_vec(),
                entry: entry(1, 1),
            }],
        )
        .expect("write segment");
    let referenced = HashSet::new();

    let reclaimed = store
        .reclaim_unreferenced_segments(&referenced)
        .expect("reclaim pending");
    assert_eq!(reclaimed, 0);
    assert_eq!(
        store.list_segment_filenames().expect("list pending"),
        vec![meta.filename.clone()]
    );

    store.mark_segment_published(&meta.filename);
    let reclaimed = store
        .reclaim_unreferenced_segments(&referenced)
        .expect("reclaim published");
    assert_eq!(reclaimed, 1);
    assert!(
        store
            .list_segment_filenames()
            .expect("list after reclaim")
            .is_empty()
    );
}

#[test]
fn segment_point_get_reads_indexed_block() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = KvSegmentStore::open(dir.path()).expect("open store");
    let entries = (0u8..160)
        .map(|number| KvSegmentEntry {
            key: format!("key-{number:03}").into_bytes(),
            entry: entry(number, number as u64),
        })
        .collect::<Vec<_>>();
    let meta = store.write_segment("test", entries).expect("write segment");

    assert_eq!(meta.blocks.len(), 3);
    let found = store
        .get(&meta, b"key-127")
        .expect("point get")
        .expect("entry");
    assert_eq!(found.version, 127);
    assert!(store.get(&meta, b"missing").expect("missing").is_none());
    assert_eq!(store.read_segment(&meta).expect("read all").len(), 160);
    let scanned = store
        .scan_range(
            &meta,
            &Bound::Included(b"key-070".to_vec()),
            &Bound::Excluded(b"key-075".to_vec()),
        )
        .expect("range scan");
    assert_eq!(
        scanned
            .iter()
            .map(|entry| entry.key.clone())
            .collect::<Vec<_>>(),
        vec![
            b"key-070".to_vec(),
            b"key-071".to_vec(),
            b"key-072".to_vec(),
            b"key-073".to_vec(),
            b"key-074".to_vec()
        ]
    );
}

#[test]
fn segment_block_cache_is_bounded_and_can_be_disabled() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store =
        KvSegmentStore::open_with_block_cache_bytes(dir.path(), 32 * 1024).expect("open store");
    let entries = (0u8..160)
        .map(|number| KvSegmentEntry {
            key: format!("key-{number:03}").into_bytes(),
            entry: entry(number, number as u64),
        })
        .collect::<Vec<_>>();
    let meta = store.write_segment("test", entries).expect("write segment");

    assert_eq!(store.block_cache_resident_bytes(), 0);
    store.get(&meta, b"key-000").expect("first get");
    assert!(store.block_cache_resident_bytes() > 0);
    for number in [70u8, 127, 150] {
        store
            .get(&meta, format!("key-{number:03}").as_bytes())
            .expect("cacheable get");
        assert!(store.block_cache_resident_bytes() <= store.block_cache_capacity_bytes());
    }

    let disabled_dir = tempfile::tempdir().expect("disabled tempdir");
    let disabled_store =
        KvSegmentStore::open_with_block_cache_bytes(disabled_dir.path(), 0).expect("open");
    let disabled_entries = (0u8..80)
        .map(|number| KvSegmentEntry {
            key: format!("key-{number:03}").into_bytes(),
            entry: entry(number, number as u64),
        })
        .collect::<Vec<_>>();
    let disabled_meta = disabled_store
        .write_segment("test", disabled_entries)
        .expect("write segment");
    disabled_store
        .get(&disabled_meta, b"key-070")
        .expect("disabled cache get");
    assert_eq!(disabled_store.block_cache_resident_bytes(), 0);
}

#[test]
fn cold_segment_scans_do_not_populate_block_cache() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store =
        KvSegmentStore::open_with_block_cache_bytes(dir.path(), 32 * 1024).expect("open store");
    let entries = (0u8..160)
        .map(|number| KvSegmentEntry {
            key: format!("key-{number:03}").into_bytes(),
            entry: entry(number, number as u64),
        })
        .collect::<Vec<_>>();
    let meta = store.write_segment("test", entries).expect("write segment");

    assert_eq!(store.block_cache_resident_bytes(), 0);
    assert_eq!(
        store
            .read_segment_cold(&meta)
            .expect("cold segment read")
            .len(),
        160
    );
    assert_eq!(store.block_cache_resident_bytes(), 0);
    assert_eq!(
        store
            .scan_range_cold(
                &meta,
                &Bound::Included(b"key-070".to_vec()),
                &Bound::Excluded(b"key-075".to_vec()),
            )
            .expect("cold range scan")
            .len(),
        5
    );
    assert_eq!(store.block_cache_resident_bytes(), 0);
    store
        .scan_range(
            &meta,
            &Bound::Included(b"key-070".to_vec()),
            &Bound::Excluded(b"key-075".to_vec()),
        )
        .expect("foreground range scan");
    assert!(store.block_cache_resident_bytes() > 0);
}

#[test]
fn limited_segment_scan_stops_after_first_matching_block() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store =
        KvSegmentStore::open_with_block_cache_bytes(dir.path(), 64 * 1024).expect("open store");
    let entries = (0u8..160)
        .map(|number| KvSegmentEntry {
            key: format!("key-{number:03}").into_bytes(),
            entry: entry(number, number as u64),
        })
        .collect::<Vec<_>>();
    let meta = store.write_segment("test", entries).expect("write segment");

    let rows = store
        .scan_range_limited(&meta, &Bound::Unbounded, &Bound::Unbounded, 10)
        .expect("limited scan");

    assert_eq!(rows.len(), 10);
    assert_eq!(rows[0].key, b"key-000".to_vec());
    assert_eq!(rows[9].key, b"key-009".to_vec());
    let first_block_resident_bytes = store.block_cache_resident_bytes();
    assert!(first_block_resident_bytes > 0);

    store
        .scan_range_limited(&meta, &Bound::Unbounded, &Bound::Unbounded, usize::MAX)
        .expect("full scan");
    assert!(
        store.block_cache_resident_bytes() > first_block_resident_bytes,
        "full scan should populate additional blocks"
    );
}

#[test]
fn range_scan_block_bounds_jump_to_first_possible_block() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store =
        KvSegmentStore::open_with_block_cache_bytes(dir.path(), 64 * 1024).expect("open store");
    let entries = (0u16..256)
        .map(|number| KvSegmentEntry {
            key: format!("key-{number:03}").into_bytes(),
            entry: entry(number as u8, number as u64),
        })
        .collect::<Vec<_>>();
    let meta = store.write_segment("test", entries).expect("write segment");

    assert_eq!(
        first_block_position_for_start(&meta.blocks, &Bound::Included(b"key-128".to_vec())),
        2
    );
    assert_eq!(
        first_block_position_for_start(&meta.blocks, &Bound::Excluded(b"key-127".to_vec())),
        2
    );
    assert!(block_starts_after_end(
        &meta.blocks[3],
        &Bound::Excluded(b"key-192".to_vec())
    ));

    let rows = store
        .scan_range_limited(
            &meta,
            &Bound::Included(b"key-128".to_vec()),
            &Bound::Excluded(b"key-132".to_vec()),
            10,
        )
        .expect("tail range");
    assert_eq!(
        rows.iter()
            .map(|entry| entry.key.clone())
            .collect::<Vec<_>>(),
        vec![
            b"key-128".to_vec(),
            b"key-129".to_vec(),
            b"key-130".to_vec(),
            b"key-131".to_vec()
        ]
    );
}

#[test]
fn segment_point_get_detects_block_corruption() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = KvSegmentStore::open(dir.path()).expect("open store");
    let entries = (0u8..80)
        .map(|number| KvSegmentEntry {
            key: format!("key-{number:03}").into_bytes(),
            entry: entry(number, number as u64),
        })
        .collect::<Vec<_>>();
    let meta = store.write_segment("test", entries).expect("write segment");
    let block = meta.blocks.last().expect("last block");
    let path = store.dir.join(&meta.filename);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open segment");
    file.seek(SeekFrom::Start(block.byte_start + 8))
        .expect("seek into block");
    file.write_all(&[0xff]).expect("corrupt block");
    file.sync_all().expect("sync corruption");

    let err = store
        .get(&meta, b"key-070")
        .expect_err("corruption detected");
    assert!(matches!(err, AedbError::IntegrityError { .. }));
}
