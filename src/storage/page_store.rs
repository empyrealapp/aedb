use crate::error::AedbError;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

const PAGE_STORE_MAGIC: &[u8; 8] = b"AEDBPG1\0";
const PAGE_FRAME_HEADER_BYTES: usize = 48;
const MIN_PAGE_BYTES: usize = 256;
const MAX_PAGE_BYTES: usize = 16 * 1024 * 1024;
const PAGE_STORE_WRITE_BUFFER_BYTES: usize = 4 * 1024 * 1024;
const PAGE_STORE_ZERO_BUFFER_BYTES: usize = 16 * 1024;

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
    read_file: File,
    page_size: usize,
    page_stride: u64,
    cache_capacity_pages: usize,
    page_count: AtomicU64,
    dirty: AtomicBool,
    cache: RwLock<PageCache>,
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
        let read_file = file.try_clone()?;
        Ok(Self {
            path,
            file: Mutex::new(file),
            read_file,
            page_size,
            page_stride,
            cache_capacity_pages,
            page_count: AtomicU64::new(page_count),
            dirty: AtomicBool::new(false),
            cache: RwLock::new(PageCache::new(cache_capacity_pages)),
        })
    }

    pub fn append_page(&self, payload: &[u8]) -> Result<PageRef, AedbError> {
        let page_len = self.validate_payload(payload)?;
        let page_ref = {
            let mut file = self.file.lock();
            let page_id = PageId(self.page_count.load(Ordering::Acquire));
            let blake3_hash = page_hash(page_id, payload);

            let mut header = [0u8; PAGE_FRAME_HEADER_BYTES];
            header[0..8].copy_from_slice(&page_id.0.to_le_bytes());
            header[8..12].copy_from_slice(&page_len.to_le_bytes());
            header[16..48].copy_from_slice(&blake3_hash);

            file.seek(SeekFrom::End(0))?;
            file.write_all(&header)?;
            file.write_all(payload)?;
            write_zero_padding(&mut *file, self.page_size.saturating_sub(payload.len()))?;
            file.flush()?;
            let next_page_count =
                page_id
                    .0
                    .checked_add(1)
                    .ok_or_else(|| AedbError::IntegrityError {
                        message: "page store page id overflow".into(),
                    })?;
            self.page_count.store(next_page_count, Ordering::Release);
            PageRef {
                page_id,
                len: page_len,
                blake3_hash,
            }
        };
        self.dirty.store(true, Ordering::Release);
        if self.cache_capacity_pages > 0 {
            self.cache
                .write()
                .insert(page_ref.page_id, page_ref.blake3_hash, payload);
        }
        Ok(page_ref)
    }

    pub fn append_pages(&self, payloads: &[&[u8]]) -> Result<Vec<PageRef>, AedbError> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        if let [payload] = payloads {
            return self.append_page(payload).map(|page_ref| vec![page_ref]);
        }
        let mut page_lens = Vec::with_capacity(payloads.len());
        for payload in payloads {
            page_lens.push(self.validate_payload(payload)?);
        }

        let mut refs = Vec::with_capacity(payloads.len());
        {
            let mut file = self.file.lock();
            let mut next_page_id = self.page_count.load(Ordering::Acquire);
            let write_buf_capacity = PAGE_STORE_WRITE_BUFFER_BYTES
                .min(
                    PAGE_FRAME_HEADER_BYTES
                        .saturating_add(self.page_size)
                        .saturating_mul(payloads.len()),
                )
                .max(PAGE_FRAME_HEADER_BYTES);
            let mut write_buf = Vec::with_capacity(write_buf_capacity);
            file.seek(SeekFrom::End(0))?;
            for (payload, page_len) in payloads.iter().zip(page_lens.into_iter()) {
                let page_id = PageId(next_page_id);
                let blake3_hash = page_hash(page_id, payload);
                let mut header = [0u8; PAGE_FRAME_HEADER_BYTES];
                header[0..8].copy_from_slice(&page_id.0.to_le_bytes());
                header[8..12].copy_from_slice(&page_len.to_le_bytes());
                header[16..48].copy_from_slice(&blake3_hash);
                append_bytes_to_writer(&mut *file, &mut write_buf, &header)?;
                append_bytes_to_writer(&mut *file, &mut write_buf, payload)?;
                append_zero_padding_to_writer(
                    &mut *file,
                    &mut write_buf,
                    self.page_size.saturating_sub(payload.len()),
                )?;
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
            flush_write_buffer(&mut *file, &mut write_buf)?;
            file.flush()?;
            self.page_count.store(next_page_id, Ordering::Release);
        }
        self.dirty.store(true, Ordering::Release);
        if self.cache_capacity_pages > 0 {
            let cache_start = refs.len().saturating_sub(self.cache_capacity_pages);
            let mut cache = self.cache.write();
            for (page_ref, payload) in refs[cache_start..]
                .iter()
                .zip(payloads[cache_start..].iter())
            {
                cache.insert(page_ref.page_id, page_ref.blake3_hash, payload);
            }
        }
        Ok(refs)
    }

    fn validate_payload(&self, payload: &[u8]) -> Result<u32, AedbError> {
        if payload.len() > self.page_size {
            return Err(AedbError::Validation(format!(
                "page payload exceeds page_size: payload_bytes={}, page_size={}",
                payload.len(),
                self.page_size
            )));
        }
        page_len_u32(payload)
    }

    pub fn read_page(&self, page_ref: &PageRef) -> Result<Vec<u8>, AedbError> {
        if self.cache_capacity_pages > 0
            && let Some((page, cached_hash)) = self.cache.read().get(&page_ref.page_id)
        {
            if cached_hash != page_ref.blake3_hash {
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
        let expected_len =
            usize::try_from(page_ref.len).map_err(|_| AedbError::IntegrityError {
                message: "page reference length does not fit usize".into(),
            })?;
        if expected_len > self.page_size {
            return Err(AedbError::IntegrityError {
                message: "page reference length exceeds page size".into(),
            });
        }
        let frame_offset = frame_offset(page_ref.page_id, self.page_stride)?;
        let mut header = [0u8; PAGE_FRAME_HEADER_BYTES];
        read_exact_at(&self.read_file, &mut header, frame_offset)?;
        let stored_page_id =
            u64::from_le_bytes(
                header[0..8]
                    .try_into()
                    .map_err(|_| AedbError::IntegrityError {
                        message: "invalid page id header".into(),
                    })?,
            );
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
        let mut payload = vec![0u8; expected_len];
        read_exact_at(
            &self.read_file,
            &mut payload,
            frame_offset + PAGE_FRAME_HEADER_BYTES as u64,
        )?;
        if page_hash(page_ref.page_id, &payload) != page_ref.blake3_hash {
            return Err(AedbError::IntegrityError {
                message: "page payload hash mismatch".into(),
            });
        }
        if self.cache_capacity_pages > 0 {
            self.cache
                .write()
                .insert(page_ref.page_id, page_ref.blake3_hash, &payload);
        }
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
        self.cache.read().resident_pages()
    }

    pub fn cache_capacity_pages(&self) -> usize {
        self.cache_capacity_pages
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(unix)]
fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> Result<(), AedbError> {
    while !buf.is_empty() {
        match file.read_at(buf, offset) {
            Ok(0) => {
                return Err(AedbError::Io(std::io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "failed to fill whole page buffer",
                )));
            }
            Ok(bytes_read) => {
                offset = offset.checked_add(bytes_read as u64).ok_or_else(|| {
                    AedbError::IntegrityError {
                        message: "page read offset overflow".into(),
                    }
                })?;
                let remaining = buf;
                buf = &mut remaining[bytes_read..];
            }
            Err(err) if err.kind() == ErrorKind::Interrupted => {}
            Err(err) => return Err(AedbError::Io(err)),
        }
    }
    Ok(())
}

#[cfg(windows)]
fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> Result<(), AedbError> {
    while !buf.is_empty() {
        match file.seek_read(buf, offset) {
            Ok(0) => {
                return Err(AedbError::Io(std::io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "failed to fill whole page buffer",
                )));
            }
            Ok(bytes_read) => {
                offset = offset.checked_add(bytes_read as u64).ok_or_else(|| {
                    AedbError::IntegrityError {
                        message: "page read offset overflow".into(),
                    }
                })?;
                let remaining = buf;
                buf = &mut remaining[bytes_read..];
            }
            Err(err) if err.kind() == ErrorKind::Interrupted => {}
            Err(err) => return Err(AedbError::Io(err)),
        }
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
compile_error!("PagedStore requires positional file reads for concurrent cold read safety");

fn append_bytes_to_writer<W: Write>(
    writer: &mut W,
    write_buf: &mut Vec<u8>,
    bytes: &[u8],
) -> Result<(), AedbError> {
    if bytes.len() > PAGE_STORE_WRITE_BUFFER_BYTES {
        flush_write_buffer(writer, write_buf)?;
        writer.write_all(bytes)?;
        return Ok(());
    }
    if write_buf.len() + bytes.len() > PAGE_STORE_WRITE_BUFFER_BYTES {
        flush_write_buffer(writer, write_buf)?;
    }
    write_buf.extend_from_slice(bytes);
    Ok(())
}

fn append_zero_padding_to_writer<W: Write>(
    writer: &mut W,
    write_buf: &mut Vec<u8>,
    mut byte_count: usize,
) -> Result<(), AedbError> {
    let zeros = [0u8; PAGE_STORE_ZERO_BUFFER_BYTES];
    while byte_count > 0 {
        let write_count = byte_count.min(zeros.len());
        append_bytes_to_writer(writer, write_buf, &zeros[..write_count])?;
        byte_count -= write_count;
    }
    Ok(())
}

fn flush_write_buffer<W: Write>(writer: &mut W, write_buf: &mut Vec<u8>) -> Result<(), AedbError> {
    if !write_buf.is_empty() {
        writer.write_all(write_buf)?;
        write_buf.clear();
    }
    Ok(())
}

fn write_zero_padding<W: Write>(writer: &mut W, mut byte_count: usize) -> Result<(), AedbError> {
    let zeros = [0u8; PAGE_STORE_ZERO_BUFFER_BYTES];
    while byte_count > 0 {
        let write_count = byte_count.min(zeros.len());
        writer.write_all(&zeros[..write_count])?;
        byte_count -= write_count;
    }
    Ok(())
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
    pages: HashMap<PageId, CachedPage>,
    insertion_order: VecDeque<PageId>,
}

#[derive(Debug)]
struct CachedPage {
    payload: Arc<[u8]>,
    blake3_hash: [u8; 32],
}

impl PageCache {
    fn new(capacity_pages: usize) -> Self {
        Self {
            capacity_pages,
            pages: HashMap::with_capacity(capacity_pages),
            insertion_order: VecDeque::with_capacity(capacity_pages),
        }
    }

    fn resident_pages(&self) -> usize {
        self.pages.len()
    }

    fn get(&self, page_id: &PageId) -> Option<(Arc<[u8]>, [u8; 32])> {
        let cached = self.pages.get(page_id)?;
        Some((Arc::clone(&cached.payload), cached.blake3_hash))
    }

    fn insert(&mut self, page_id: PageId, blake3_hash: [u8; 32], payload: &[u8]) {
        if self.capacity_pages == 0 {
            return;
        }
        if let std::collections::hash_map::Entry::Occupied(mut cached) = self.pages.entry(page_id) {
            cached.insert(CachedPage {
                payload: Arc::from(payload),
                blake3_hash,
            });
            return;
        }
        self.insertion_order.push_back(page_id);
        self.pages.insert(
            page_id,
            CachedPage {
                payload: Arc::from(payload),
                blake3_hash,
            },
        );
        self.evict_to_capacity();
    }

    fn evict_to_capacity(&mut self) {
        while self.pages.len() > self.capacity_pages {
            let Some(page_id) = self.insertion_order.pop_front() else {
                break;
            };
            self.pages.remove(&page_id);
        }
    }
}

#[cfg(test)]
mod tests;
