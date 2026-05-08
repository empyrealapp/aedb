use crate::error::AedbError;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

const PAGE_STORE_MAGIC: &[u8; 8] = b"AEDBPG1\0";
const PAGE_FRAME_HEADER_BYTES: usize = 48;
const MIN_PAGE_BYTES: usize = 256;
const MAX_PAGE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PageId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PageRef {
    pub page_id: PageId,
    pub len: u32,
    pub blake3_hash: [u8; 32],
}

#[derive(Debug)]
pub struct PagedStore {
    path: PathBuf,
    file: Mutex<File>,
    page_size: usize,
    page_stride: u64,
    page_count: AtomicU64,
    dirty: AtomicBool,
    cache: Mutex<PageCache>,
}

impl PagedStore {
    pub fn open(
        data_dir: &Path,
        file_name: &str,
        page_size: usize,
        cache_capacity_pages: usize,
    ) -> Result<Self, AedbError> {
        if !(MIN_PAGE_BYTES..=MAX_PAGE_BYTES).contains(&page_size) {
            return Err(AedbError::InvalidConfig {
                message: format!(
                    "page_size must be between {MIN_PAGE_BYTES} and {MAX_PAGE_BYTES} bytes"
                ),
            });
        }
        if file_name.contains('/') || file_name.contains('\\') || file_name.is_empty() {
            return Err(AedbError::InvalidConfig {
                message: "page store file_name must be a single path segment".into(),
            });
        }

        std::fs::create_dir_all(data_dir)?;
        let path = data_dir.join(file_name);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        if file.metadata()?.len() == 0 {
            file.write_all(PAGE_STORE_MAGIC)?;
            file.flush()?;
            file.sync_all()?;
        } else {
            let mut magic = [0u8; PAGE_STORE_MAGIC.len()];
            file.seek(SeekFrom::Start(0))?;
            file.read_exact(&mut magic)?;
            if &magic != PAGE_STORE_MAGIC {
                return Err(AedbError::IntegrityError {
                    message: "page store magic mismatch".into(),
                });
            }
        }

        let page_stride = u64::try_from(PAGE_FRAME_HEADER_BYTES.saturating_add(page_size))
            .map_err(|_| AedbError::InvalidConfig {
                message: "page store stride does not fit u64".into(),
            })?;
        let file_len = file.metadata()?.len();
        let payload_len = file_len
            .checked_sub(PAGE_STORE_MAGIC.len() as u64)
            .ok_or_else(|| AedbError::IntegrityError {
                message: "page store file shorter than magic header".into(),
            })?;
        if payload_len % page_stride != 0 {
            return Err(AedbError::IntegrityError {
                message: "page store contains a partial page frame".into(),
            });
        }
        let page_count = payload_len / page_stride;
        Ok(Self {
            path,
            file: Mutex::new(file),
            page_size,
            page_stride,
            page_count: AtomicU64::new(page_count),
            dirty: AtomicBool::new(false),
            cache: Mutex::new(PageCache::new(cache_capacity_pages)),
        })
    }

    pub fn append_page(&self, payload: &[u8]) -> Result<PageRef, AedbError> {
        self.append_pages(&[payload]).map(|mut refs| refs.remove(0))
    }

    pub fn append_pages(&self, payloads: &[&[u8]]) -> Result<Vec<PageRef>, AedbError> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        for payload in payloads {
            self.validate_payload(payload)?;
        }

        let mut refs = Vec::with_capacity(payloads.len());
        {
            let mut file = self.file.lock();
            let mut next_page_id = self.page_count.load(Ordering::Acquire);
            let mut frame = vec![0u8; PAGE_FRAME_HEADER_BYTES + self.page_size];
            file.seek(SeekFrom::End(0))?;
            for payload in payloads {
                frame.fill(0);
                let page_id = PageId(next_page_id);
                let page_len = page_len_u32(payload)?;
                let blake3_hash = page_hash(page_id, payload);
                frame[0..8].copy_from_slice(&page_id.0.to_le_bytes());
                frame[8..12].copy_from_slice(&page_len.to_le_bytes());
                frame[16..48].copy_from_slice(&blake3_hash);
                frame[PAGE_FRAME_HEADER_BYTES..PAGE_FRAME_HEADER_BYTES + payload.len()]
                    .copy_from_slice(payload);
                file.write_all(&frame)?;
                refs.push(PageRef {
                    page_id,
                    len: page_len,
                    blake3_hash,
                });
                next_page_id =
                    next_page_id
                        .checked_add(1)
                        .ok_or_else(|| AedbError::IntegrityError {
                            message: "page store page id overflow".into(),
                        })?;
            }
            file.flush()?;
            self.page_count.store(next_page_id, Ordering::Release);
        }
        self.dirty.store(true, Ordering::Release);
        let mut cache = self.cache.lock();
        for (page_ref, payload) in refs.iter().zip(payloads.iter()) {
            cache.insert(page_ref.page_id, *payload);
        }
        Ok(refs)
    }

    fn validate_payload(&self, payload: &[u8]) -> Result<(), AedbError> {
        if payload.len() > self.page_size {
            return Err(AedbError::Validation(format!(
                "page payload exceeds page_size: payload_bytes={}, page_size={}",
                payload.len(),
                self.page_size
            )));
        }
        let _ = page_len_u32(payload)?;
        Ok(())
    }

    pub fn read_page(&self, page_ref: &PageRef) -> Result<Vec<u8>, AedbError> {
        if let Some(page) = self.cache.lock().get(&page_ref.page_id) {
            if page_hash(page_ref.page_id, &page) != page_ref.blake3_hash {
                return Err(AedbError::IntegrityError {
                    message: "cached page hash mismatch".into(),
                });
            }
            return Ok(page.as_ref().to_vec());
        }

        let current_count = self.page_count.load(Ordering::Acquire);
        if page_ref.page_id.0 >= current_count {
            return Err(AedbError::IntegrityError {
                message: "page reference outside page store".into(),
            });
        }
        let frame_offset = frame_offset(page_ref.page_id, self.page_stride)?;
        let mut header = [0u8; PAGE_FRAME_HEADER_BYTES];
        let mut payload = {
            let mut file = self.file.lock();
            file.seek(SeekFrom::Start(frame_offset))?;
            file.read_exact(&mut header)?;
            let stored_page_id = u64::from_le_bytes(header[0..8].try_into().map_err(|_| {
                AedbError::IntegrityError {
                    message: "invalid page id header".into(),
                }
            })?);
            if stored_page_id != page_ref.page_id.0 {
                return Err(AedbError::IntegrityError {
                    message: "page id header mismatch".into(),
                });
            }
            let stored_len = u32::from_le_bytes(header[8..12].try_into().map_err(|_| {
                AedbError::IntegrityError {
                    message: "invalid page length header".into(),
                }
            })?);
            if stored_len != page_ref.len || stored_len as usize > self.page_size {
                return Err(AedbError::IntegrityError {
                    message: "page length header mismatch".into(),
                });
            }
            let mut stored_hash = [0u8; 32];
            stored_hash.copy_from_slice(&header[16..48]);
            if stored_hash != page_ref.blake3_hash {
                return Err(AedbError::IntegrityError {
                    message: "page hash header mismatch".into(),
                });
            }
            let mut payload = vec![0u8; stored_len as usize];
            file.read_exact(&mut payload)?;
            payload
        };
        payload.shrink_to_fit();
        if page_hash(page_ref.page_id, &payload) != page_ref.blake3_hash {
            return Err(AedbError::IntegrityError {
                message: "page payload hash mismatch".into(),
            });
        }
        self.cache.lock().insert(page_ref.page_id, &payload);
        Ok(payload)
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

    pub fn page_size(&self) -> usize {
        self.page_size
    }

    pub fn page_count(&self) -> u64 {
        self.page_count.load(Ordering::Acquire)
    }

    pub fn len_bytes(&self) -> u64 {
        PAGE_STORE_MAGIC.len() as u64 + self.page_count() * self.page_stride
    }

    pub fn cache_resident_pages(&self) -> usize {
        self.cache.lock().resident_pages()
    }

    pub fn cache_capacity_pages(&self) -> usize {
        self.cache.lock().capacity_pages()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn frame_offset(page_id: PageId, page_stride: u64) -> Result<u64, AedbError> {
    page_id
        .0
        .checked_mul(page_stride)
        .and_then(|offset| offset.checked_add(PAGE_STORE_MAGIC.len() as u64))
        .ok_or_else(|| AedbError::IntegrityError {
            message: "page frame offset overflow".into(),
        })
}

fn page_len_u32(payload: &[u8]) -> Result<u32, AedbError> {
    u32::try_from(payload.len())
        .map_err(|_| AedbError::Validation("page payload length does not fit u32".into()))
}

fn page_hash(page_id: PageId, payload: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&page_id.0.to_le_bytes());
    hasher.update(&(payload.len() as u64).to_le_bytes());
    hasher.update(payload);
    *hasher.finalize().as_bytes()
}

#[derive(Debug)]
struct PageCache {
    capacity_pages: usize,
    next_access_tick: u64,
    pages: HashMap<PageId, CachedPage>,
    access_order: BTreeMap<u64, PageId>,
}

#[derive(Debug)]
struct CachedPage {
    payload: Arc<[u8]>,
    access_tick: u64,
}

impl PageCache {
    fn new(capacity_pages: usize) -> Self {
        Self {
            capacity_pages,
            next_access_tick: 0,
            pages: HashMap::new(),
            access_order: BTreeMap::new(),
        }
    }

    fn capacity_pages(&self) -> usize {
        self.capacity_pages
    }

    fn resident_pages(&self) -> usize {
        self.pages.len()
    }

    fn get(&mut self, page_id: &PageId) -> Option<Arc<[u8]>> {
        let access_tick = self.allocate_access_tick();
        let cached = self.pages.get_mut(page_id)?;
        self.access_order.remove(&cached.access_tick);
        cached.access_tick = access_tick;
        self.access_order.insert(access_tick, *page_id);
        Some(Arc::clone(&cached.payload))
    }

    fn insert(&mut self, page_id: PageId, payload: &[u8]) {
        if self.capacity_pages == 0 {
            return;
        }
        if let Some(existing) = self.pages.remove(&page_id) {
            self.access_order.remove(&existing.access_tick);
        }
        let access_tick = self.allocate_access_tick();
        self.pages.insert(
            page_id,
            CachedPage {
                payload: Arc::from(payload),
                access_tick,
            },
        );
        self.access_order.insert(access_tick, page_id);
        self.evict_to_capacity();
    }

    fn allocate_access_tick(&mut self) -> u64 {
        let tick = self.next_access_tick;
        self.next_access_tick = self.next_access_tick.wrapping_add(1);
        tick
    }

    fn evict_to_capacity(&mut self) {
        while self.pages.len() > self.capacity_pages {
            let Some((tick, page_id)) = self.access_order.pop_first() else {
                break;
            };
            if let Some(cached) = self.pages.get(&page_id)
                && cached.access_tick == tick
            {
                self.pages.remove(&page_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom, Write};
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
}
