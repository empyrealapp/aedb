use crate::error::AedbError;
use memmap2::{Mmap, MmapOptions};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

const VALUE_STORE_FILE: &str = "values.aedbdat";
const VALUE_STORE_MAGIC: &[u8; 8] = b"AEDBVAL1";

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PersistentValueRef {
    pub offset: u64,
    pub len: u64,
    pub blake3_hash: [u8; 32],
}

#[derive(Debug)]
pub struct PersistentValueStore {
    path: PathBuf,
    file: Mutex<File>,
    mmap: Mutex<Mmap>,
    hot_cache: Mutex<HotValueCache>,
    len: AtomicU64,
    dirty: AtomicBool,
}

impl PartialEq for PersistentValueStore {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
    }
}

impl Eq for PersistentValueStore {}

impl PersistentValueStore {
    pub fn open(data_dir: &Path) -> Result<Self, AedbError> {
        Self::open_with_hot_cache_bytes(data_dir, 0)
    }

    pub fn open_with_hot_cache_bytes(
        data_dir: &Path,
        hot_cache_capacity_bytes: usize,
    ) -> Result<Self, AedbError> {
        std::fs::create_dir_all(data_dir)?;
        let path = data_dir.join(VALUE_STORE_FILE);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        if file.metadata()?.len() == 0 {
            file.write_all(VALUE_STORE_MAGIC)?;
            file.flush()?;
            file.sync_all()?;
        } else {
            let mut magic = [0u8; VALUE_STORE_MAGIC.len()];
            file.seek(SeekFrom::Start(0))?;
            file.read_exact(&mut magic)?;
            if &magic != VALUE_STORE_MAGIC {
                return Err(AedbError::IntegrityError {
                    message: "persistent value store magic mismatch".into(),
                });
            }
        }

        let file_len = file.metadata()?.len();
        let mmap = map_file(&file)?;
        Ok(Self {
            path,
            file: Mutex::new(file),
            mmap: Mutex::new(mmap),
            hot_cache: Mutex::new(HotValueCache::new(hot_cache_capacity_bytes)),
            len: AtomicU64::new(file_len),
            dirty: AtomicBool::new(false),
        })
    }

    pub fn append(&self, value: &[u8]) -> Result<PersistentValueRef, AedbError> {
        self.append_with_cache_policy(value, true)
    }

    pub fn append_cold(&self, value: &[u8]) -> Result<PersistentValueRef, AedbError> {
        self.append_with_cache_policy(value, false)
    }

    pub fn append_many_cold(
        &self,
        values: &[Vec<u8>],
    ) -> Result<Vec<PersistentValueRef>, AedbError> {
        let value_slices: Vec<&[u8]> = values.iter().map(Vec::as_slice).collect();
        self.append_many_cold_slices(&value_slices)
    }

    pub fn append_many_cold_slices(
        &self,
        values: &[&[u8]],
    ) -> Result<Vec<PersistentValueRef>, AedbError> {
        if values.is_empty() {
            return Ok(Vec::new());
        }

        let value_metadata: Vec<([u8; 32], u64)> = values
            .iter()
            .map(|value| (*blake3::hash(value).as_bytes(), value.len() as u64))
            .collect();
        let append_start_offset = {
            let mut file = self.file.lock();
            let append_start_offset = file.seek(SeekFrom::End(0))?;
            for value in values {
                file.write_all(value)?;
            }
            file.flush()?;
            append_start_offset
        };

        let mut value_offset = append_start_offset;
        let mut refs = Vec::with_capacity(values.len());
        for (blake3_hash, len) in value_metadata {
            refs.push(PersistentValueRef {
                offset: value_offset,
                len,
                blake3_hash,
            });
            value_offset =
                value_offset
                    .checked_add(len)
                    .ok_or_else(|| AedbError::IntegrityError {
                        message: "persistent value store append offset overflows".into(),
                    })?;
        }
        self.len.store(value_offset, Ordering::Release);
        self.dirty.store(true, Ordering::Release);
        Ok(refs)
    }

    fn append_with_cache_policy(
        &self,
        value: &[u8],
        populate_hot_cache: bool,
    ) -> Result<PersistentValueRef, AedbError> {
        let blake3_hash = *blake3::hash(value).as_bytes();
        let (append_start_offset, next_file_len) = {
            let mut file = self.file.lock();
            let append_start_offset = file.seek(SeekFrom::End(0))?;
            file.write_all(value)?;
            file.flush()?;
            (
                append_start_offset,
                append_start_offset.saturating_add(value.len() as u64),
            )
        };
        self.len.store(next_file_len, Ordering::Release);
        let value_ref = PersistentValueRef {
            offset: append_start_offset,
            len: value.len() as u64,
            blake3_hash,
        };
        if populate_hot_cache {
            self.hot_cache.lock().insert(value_ref.clone(), value);
        }
        self.dirty.store(true, Ordering::Release);
        Ok(value_ref)
    }

    pub fn read(&self, value_ref: &PersistentValueRef) -> Result<Vec<u8>, AedbError> {
        if let Some(value) = { self.hot_cache.lock().get(value_ref) } {
            return Ok(value.as_ref().to_vec());
        }

        let end = value_ref.offset.checked_add(value_ref.len).ok_or_else(|| {
            AedbError::IntegrityError {
                message: "persistent value reference overflows".into(),
            }
        })?;
        if value_ref.offset < VALUE_STORE_MAGIC.len() as u64
            || end > self.len.load(Ordering::Acquire)
        {
            return Err(AedbError::IntegrityError {
                message: "persistent value reference outside value store".into(),
            });
        }

        let value = {
            let mut mmap = self.mmap.lock();
            let start =
                usize::try_from(value_ref.offset).map_err(|_| AedbError::IntegrityError {
                    message: "persistent value offset does not fit usize".into(),
                })?;
            let end = usize::try_from(end).map_err(|_| AedbError::IntegrityError {
                message: "persistent value end offset does not fit usize".into(),
            })?;
            if end > mmap.len() {
                let file = self.file.lock();
                *mmap = map_file(&file)?;
            }
            let Some(bytes) = mmap.get(start..end) else {
                return Err(AedbError::IntegrityError {
                    message: "persistent value reference outside mapped value store".into(),
                });
            };
            bytes.to_vec()
        };
        let actual = blake3::hash(&value);
        if actual.as_bytes() != &value_ref.blake3_hash {
            return Err(AedbError::IntegrityError {
                message: "persistent value hash mismatch".into(),
            });
        }
        self.hot_cache.lock().insert(value_ref.clone(), &value);
        Ok(value)
    }

    pub fn sync_all(&self) -> Result<(), AedbError> {
        if !self.dirty.load(Ordering::Acquire) {
            return Ok(());
        }
        let file = self.file.lock();
        if !self.dirty.load(Ordering::Acquire) {
            return Ok(());
        }
        file.sync_all()?;
        self.dirty.store(false, Ordering::Release);
        Ok(())
    }

    pub fn len_bytes(&self) -> u64 {
        self.len.load(Ordering::Acquire)
    }

    pub fn hot_cache_resident_bytes(&self) -> usize {
        self.hot_cache.lock().resident_bytes()
    }

    pub fn hot_cache_capacity_bytes(&self) -> usize {
        self.hot_cache.lock().capacity_bytes()
    }
}

fn map_file(file: &File) -> Result<Mmap, AedbError> {
    // SAFETY: the file handle lives inside PersistentValueStore and the mapping
    // is replaced only after append writes have completed.
    unsafe { MmapOptions::new().map(file).map_err(AedbError::Io) }
}

#[derive(Debug)]
struct HotValueCache {
    capacity_bytes: usize,
    resident_bytes: usize,
    next_access_tick: u64,
    values: HashMap<PersistentValueRef, CachedHotValue>,
    access_order: BTreeMap<u64, PersistentValueRef>,
}

#[derive(Debug)]
struct CachedHotValue {
    value: Arc<[u8]>,
    access_tick: u64,
}

impl HotValueCache {
    fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes,
            resident_bytes: 0,
            next_access_tick: 1,
            values: HashMap::new(),
            access_order: BTreeMap::new(),
        }
    }

    fn capacity_bytes(&self) -> usize {
        self.capacity_bytes
    }

    fn resident_bytes(&self) -> usize {
        self.resident_bytes
    }

    fn get(&mut self, value_ref: &PersistentValueRef) -> Option<Arc<[u8]>> {
        let access_tick = self.allocate_access_tick();
        let cached = self.values.get_mut(value_ref)?;
        self.access_order.remove(&cached.access_tick);
        let value = Arc::clone(&cached.value);
        cached.access_tick = access_tick;
        self.access_order.insert(access_tick, value_ref.clone());
        Some(value)
    }

    fn insert(&mut self, value_ref: PersistentValueRef, value: &[u8]) {
        let value_byte_count = value.len();
        self.remove(&value_ref);
        if self.capacity_bytes == 0 || value_byte_count > self.capacity_bytes {
            return;
        }

        let access_tick = self.allocate_access_tick();
        self.resident_bytes = self.resident_bytes.saturating_add(value_byte_count);
        self.access_order.insert(access_tick, value_ref.clone());
        self.values.insert(
            value_ref,
            CachedHotValue {
                value: Arc::from(value),
                access_tick,
            },
        );
        self.evict_cold_values();
    }

    fn remove(&mut self, value_ref: &PersistentValueRef) {
        if let Some(cached) = self.values.remove(value_ref) {
            self.access_order.remove(&cached.access_tick);
            self.resident_bytes = self.resident_bytes.saturating_sub(cached.value.len());
        }
    }

    fn evict_cold_values(&mut self) {
        while self.resident_bytes > self.capacity_bytes {
            let Some((_access_tick, value_ref)) = self.access_order.pop_first() else {
                self.resident_bytes = 0;
                return;
            };
            if let Some(cached) = self.values.remove(&value_ref) {
                self.resident_bytes = self.resident_bytes.saturating_sub(cached.value.len());
            }
        }
    }

    fn allocate_access_tick(&mut self) -> u64 {
        let access_tick = self.next_access_tick;
        self.next_access_tick = self.next_access_tick.saturating_add(1);
        access_tick
    }
}

#[cfg(test)]
mod tests {
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
        assert!(cache.resident_bytes() <= cache.capacity_bytes());
    }

    #[test]
    fn hot_cache_can_be_disabled() {
        let dir = tempdir().expect("temp");
        let store =
            PersistentValueStore::open_with_hot_cache_bytes(dir.path(), 0).expect("open store");
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
    fn read_rejects_corrupted_payload_hash() {
        let dir = tempdir().expect("temp");
        let store =
            PersistentValueStore::open_with_hot_cache_bytes(dir.path(), 0).expect("open store");
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
}
