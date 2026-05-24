use super::{PAGE_FRAME_HEADER_BYTES, PAGE_STORE_MAGIC, PagedStore};
use crate::error::AedbError;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn append_read_and_reopen_pages() {
    let dir = tempdir().expect("temp");
    let store = PagedStore::open(dir.path(), "rows.aedbpg", 512, 2).expect("open");
    let first = store.append_page(b"alpha").expect("append first");
    let second = store.append_page(b"beta").expect("append second");
    assert_eq!(store.page_count(), 2);
    assert_eq!(store.read_page(&first).expect("read first"), b"alpha");
    assert_eq!(store.read_page(&second).expect("read second"), b"beta");
    store.sync_all().expect("sync");
    drop(store);

    let reopened = PagedStore::open(dir.path(), "rows.aedbpg", 512, 1).expect("reopen");
    assert_eq!(reopened.page_count(), 2);
    assert_eq!(reopened.read_page(&first).expect("read first"), b"alpha");
    assert_eq!(reopened.read_page(&second).expect("read second"), b"beta");
}

#[test]
fn cache_is_bounded_by_page_count() {
    let dir = tempdir().expect("temp");
    let store = PagedStore::open(dir.path(), "rows.aedbpg", 512, 2).expect("open");
    let refs = (0..4u8)
        .map(|i| store.append_page(&[i]).expect("append"))
        .collect::<Vec<_>>();
    assert_eq!(store.cache_resident_pages(), 2);
    for page_ref in &refs {
        store.read_page(page_ref).expect("read");
        assert!(store.cache_resident_pages() <= 2);
    }
}

#[test]
fn append_pages_batches_contiguous_page_ids() {
    let dir = tempdir().expect("temp");
    let store = PagedStore::open(dir.path(), "rows.aedbpg", 512, 4).expect("open");
    let payloads = [
        b"batch-alpha".as_slice(),
        b"batch-beta".as_slice(),
        b"batch-gamma".as_slice(),
    ];
    let refs = store.append_pages(&payloads).expect("append batch");
    assert_eq!(refs.len(), payloads.len());
    for (expected_page_id, page_ref) in refs.iter().enumerate() {
        assert_eq!(page_ref.page_id.0, expected_page_id as u64);
        assert_eq!(
            store.read_page(page_ref).expect("read batch page"),
            payloads[expected_page_id]
        );
    }
    assert_eq!(store.page_count(), payloads.len() as u64);
}

#[test]
fn append_pages_populates_only_cache_resident_tail() {
    let dir = tempdir().expect("temp");
    let store = PagedStore::open(dir.path(), "rows.aedbpg", 512, 2).expect("open");
    let payloads = [
        b"cache-alpha".as_slice(),
        b"cache-beta".as_slice(),
        b"cache-gamma".as_slice(),
        b"cache-delta".as_slice(),
    ];
    let refs = store.append_pages(&payloads).expect("append batch");
    assert_eq!(store.cache_resident_pages(), 2);
    assert_eq!(
        store.read_page(&refs[2]).expect("read cached tail"),
        payloads[2]
    );
    assert_eq!(
        store.read_page(&refs[3]).expect("read cached tail"),
        payloads[3]
    );
    assert!(store.cache_resident_pages() <= 2);
}

#[test]
fn concurrent_appends_assign_unique_page_ids() {
    let dir = tempdir().expect("temp");
    let store = Arc::new(PagedStore::open(dir.path(), "rows.aedbpg", 512, 8).expect("open"));
    let mut handles = Vec::new();
    for i in 0..32u8 {
        let store = Arc::clone(&store);
        handles.push(std::thread::spawn(move || {
            store.append_page(&[i]).expect("append")
        }));
    }
    let mut refs = handles
        .into_iter()
        .map(|handle| handle.join().expect("join"))
        .collect::<Vec<_>>();
    refs.sort_by_key(|page_ref| page_ref.page_id.0);
    assert_eq!(refs.len(), 32);
    for (expected, page_ref) in refs.iter().enumerate() {
        assert_eq!(page_ref.page_id.0, expected as u64);
        let payload = store.read_page(page_ref).expect("read");
        assert_eq!(payload.len(), 1);
    }
    assert_eq!(store.page_count(), 32);
}

#[test]
fn concurrent_cold_reads_do_not_share_seek_cursor() {
    let dir = tempdir().expect("temp");
    let store = Arc::new(PagedStore::open(dir.path(), "rows.aedbpg", 1024, 0).expect("open"));
    let payloads = (0..64u8)
        .map(|i| vec![i; 257 + usize::from(i % 17)])
        .collect::<Vec<_>>();
    let slices = payloads.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let refs = store.append_pages(&slices).expect("append batch");
    store.sync_all().expect("sync");

    let mut handles = Vec::new();
    for _ in 0..8 {
        let store = Arc::clone(&store);
        let refs = refs.clone();
        let payloads = payloads.clone();
        handles.push(std::thread::spawn(move || {
            for _ in 0..16 {
                for (page_ref, expected) in refs.iter().zip(payloads.iter()) {
                    assert_eq!(store.read_page(page_ref).expect("read page"), *expected);
                }
            }
        }));
    }
    for handle in handles {
        handle.join().expect("join");
    }
}

#[test]
fn read_detects_corrupted_page_payload() {
    let dir = tempdir().expect("temp");
    let store = PagedStore::open(dir.path(), "rows.aedbpg", 512, 0).expect("open");
    let page_ref = store.append_page(b"alpha").expect("append");
    store.sync_all().expect("sync");
    let path = store.path().to_path_buf();
    drop(store);

    let mut file = OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open page file");
    file.seek(SeekFrom::Start(
        PAGE_STORE_MAGIC.len() as u64 + PAGE_FRAME_HEADER_BYTES as u64,
    ))
    .expect("seek payload");
    file.write_all(b"z").expect("corrupt");
    file.flush().expect("flush");
    drop(file);

    let reopened = PagedStore::open(dir.path(), "rows.aedbpg", 512, 0).expect("reopen");
    let err = reopened
        .read_page(&page_ref)
        .expect_err("corruption should be detected");
    assert!(matches!(err, AedbError::IntegrityError { .. }));
}

#[test]
fn rejects_oversized_pages_and_partial_frames() {
    let dir = tempdir().expect("temp");
    let store = PagedStore::open(dir.path(), "rows.aedbpg", 512, 1).expect("open");
    let err = store
        .append_page(&vec![0u8; 513])
        .expect_err("oversized page");
    assert!(matches!(err, AedbError::Validation(_)));
    drop(store);

    let path = dir.path().join("rows.aedbpg");
    let mut file = OpenOptions::new().append(true).open(path).expect("open");
    file.write_all(b"partial").expect("partial frame");
    file.flush().expect("flush partial");
    drop(file);

    let err = PagedStore::open(dir.path(), "rows.aedbpg", 512, 1)
        .expect_err("partial frame must be rejected");
    assert!(matches!(err, AedbError::IntegrityError { .. }));
}
