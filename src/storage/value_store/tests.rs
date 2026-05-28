use super::PersistentValueStore;
use tempfile::tempdir;

#[test]
fn hot_cache_keeps_recent_values_and_evicts_cold_values() {
    let dir = tempdir().expect("temp");
    let store =
        PersistentValueStore::open_with_hot_cache_bytes(dir.path(), 20).expect("open store");

    let first = store.append(b"first-01").expect("append first");
    let second = store.append(b"second02").expect("append second");
    store.read(&first).expect("touch first");
    let third = store.append(b"third-03").expect("append third");

    let cache = store.hot_cache.lock();
    assert!(cache.values.contains_key(&first), "first should stay hot");
    assert!(cache.values.contains_key(&third), "third should be cached");
    assert!(
        !cache.values.contains_key(&second),
        "least recently used value should be cold"
    );
    assert!(cache.resident_bytes() <= cache.capacity_bytes);
}

#[test]
fn hot_cache_can_be_disabled() {
    let dir = tempdir().expect("temp");
    let store = PersistentValueStore::open_with_hot_cache_bytes(dir.path(), 0).expect("open store");
    let value_ref = store.append(b"cold-only").expect("append");

    assert_eq!(store.hot_cache_resident_bytes(), 0);
    assert_eq!(store.read(&value_ref).expect("read"), b"cold-only");
    assert_eq!(store.hot_cache_resident_bytes(), 0);
}

#[test]
fn append_many_cold_writes_contiguous_values_without_hot_cache_population() {
    let dir = tempdir().expect("temp");
    let store =
        PersistentValueStore::open_with_hot_cache_bytes(dir.path(), 128).expect("open store");
    let values = vec![
        b"batch-one".to_vec(),
        b"batch-two-longer".to_vec(),
        b"batch-three".to_vec(),
    ];

    let refs = store.append_many_cold(&values).expect("append batch");

    assert_eq!(refs.len(), values.len());
    assert_eq!(refs[0].offset, super::VALUE_STORE_MAGIC.len() as u64);
    for pair in refs.windows(2) {
        assert_eq!(pair[0].offset + pair[0].len, pair[1].offset);
    }
    assert_eq!(store.hot_cache_resident_bytes(), 0);
    for (value_ref, expected) in refs.iter().zip(values.iter()) {
        assert_eq!(store.read(value_ref).expect("read batch value"), *expected);
    }
}

#[test]
fn append_many_hot_writes_contiguous_values_and_populates_hot_cache() {
    let dir = tempdir().expect("temp");
    let store =
        PersistentValueStore::open_with_hot_cache_bytes(dir.path(), 128).expect("open store");
    let values = [
        b"batch-one".as_slice(),
        b"batch-two-longer".as_slice(),
        b"batch-three".as_slice(),
    ];

    let refs = store.append_many_hot_slices(&values).expect("append batch");

    assert_eq!(refs.len(), values.len());
    assert_eq!(refs[0].offset, super::VALUE_STORE_MAGIC.len() as u64);
    for pair in refs.windows(2) {
        assert_eq!(pair[0].offset + pair[0].len, pair[1].offset);
    }
    assert!(store.hot_cache_resident_bytes() > 0);
    for value_ref in &refs {
        assert!(
            store.hot_cache.lock().values.contains_key(value_ref),
            "hot batch append should cache each written value"
        );
    }
    for (value_ref, expected) in refs.iter().zip(values.iter()) {
        assert_eq!(store.read(value_ref).expect("read batch value"), *expected);
    }
}

#[test]
fn concurrent_cold_reads_share_mapped_file_without_hot_cache() {
    let dir = tempdir().expect("temp");
    let store = std::sync::Arc::new(
        PersistentValueStore::open_with_hot_cache_bytes(dir.path(), 0).expect("open store"),
    );
    let values = (0..64u8)
        .map(|i| vec![i; 513 + usize::from(i % 23)])
        .collect::<Vec<_>>();
    let refs = store.append_many_cold(&values).expect("append batch");

    let mut handles = Vec::new();
    for _ in 0..8 {
        let store = std::sync::Arc::clone(&store);
        let refs = refs.clone();
        let values = values.clone();
        handles.push(std::thread::spawn(move || {
            for _ in 0..16 {
                for (value_ref, expected) in refs.iter().zip(values.iter()) {
                    assert_eq!(store.read(value_ref).expect("read value"), *expected);
                }
            }
        }));
    }
    for handle in handles {
        handle.join().expect("join");
    }
    assert_eq!(store.hot_cache_resident_bytes(), 0);
}

#[test]
fn read_rejects_corrupted_payload_hash() {
    let dir = tempdir().expect("temp");
    let store = PersistentValueStore::open_with_hot_cache_bytes(dir.path(), 0).expect("open store");
    let value_ref = store.append(b"hash-checked-payload").expect("append");
    drop(store);

    let path = dir.path().join(super::VALUE_STORE_FILE);
    let mut bytes = std::fs::read(&path).expect("read value store");
    let corrupt_at = usize::try_from(value_ref.offset).expect("offset fits usize");
    bytes[corrupt_at] ^= 0xFF;
    std::fs::write(&path, bytes).expect("corrupt value store");

    let reopened =
        PersistentValueStore::open_with_hot_cache_bytes(dir.path(), 0).expect("reopen store");
    let err = reopened
        .read(&value_ref)
        .expect_err("hash mismatch must reject corrupted payload");
    assert!(
        matches!(err, crate::error::AedbError::IntegrityError { message } if message.contains("hash mismatch"))
    );
}
