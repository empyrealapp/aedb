use crate::error::AedbError;
use crate::storage::keyspace::KvEntry;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::NamedTempFile;

const KV_SEGMENT_DIR: &str = "kv_segments";
const KV_SEGMENT_MAGIC: &[u8; 8] = b"AEDBKV1\0";
const KV_SEGMENT_BLOCK_ENTRIES: usize = 64;
const DEFAULT_KV_SEGMENT_BLOCK_CACHE_BYTES: usize = 16 * 1024 * 1024;
const KV_SEGMENT_BLOOM_WORDS: usize = 32;
const KV_SEGMENT_BLOOM_BITS: u64 = (KV_SEGMENT_BLOOM_WORDS as u64) * 64;
const KV_SEGMENT_BLOOM_HASHES: u64 = 4;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvSegmentMeta {
    pub filename: String,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub entry_count: u64,
    pub sha256_hex: String,
    pub created_at_micros: u64,
    #[serde(default)]
    pub bloom_bits: Vec<u64>,
    #[serde(default)]
    pub blocks: Vec<KvSegmentBlockMeta>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvSegmentBlockMeta {
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
    pub byte_start: u64,
    pub byte_count: u64,
    pub entry_count: u32,
    pub sha256_hex: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvSegmentEntry {
    pub key: Vec<u8>,
    pub entry: KvEntry,
}

#[derive(Debug)]
pub struct KvSegmentStore {
    dir: PathBuf,
    block_cache_capacity_bytes: usize,
    block_cache: Mutex<KvSegmentBlockCache>,
}

impl PartialEq for KvSegmentStore {
    fn eq(&self, other: &Self) -> bool {
        self.dir == other.dir
    }
}

impl Eq for KvSegmentStore {}

impl KvSegmentStore {
    pub fn open(data_dir: &Path) -> Result<Self, AedbError> {
        Self::open_with_block_cache_bytes(data_dir, DEFAULT_KV_SEGMENT_BLOCK_CACHE_BYTES)
    }

    pub fn open_with_block_cache_bytes(
        data_dir: &Path,
        block_cache_capacity_bytes: usize,
    ) -> Result<Self, AedbError> {
        let dir = data_dir.join(KV_SEGMENT_DIR);
        fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            block_cache_capacity_bytes,
            block_cache: Mutex::new(KvSegmentBlockCache::new(block_cache_capacity_bytes)),
        })
    }

    pub fn block_cache_resident_bytes(&self) -> usize {
        self.block_cache.lock().resident_bytes()
    }

    pub fn block_cache_capacity_bytes(&self) -> usize {
        self.block_cache_capacity_bytes
    }

    pub fn list_segment_filenames(&self) -> Result<Vec<String>, AedbError> {
        let mut filenames = Vec::new();
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let filename = entry.file_name().to_string_lossy().to_string();
            if !is_kv_segment_filename(&filename) {
                continue;
            }
            filenames.push(filename);
        }
        filenames.sort();
        Ok(filenames)
    }

    pub fn reclaim_unreferenced_segments(
        &self,
        referenced_filenames: &HashSet<String>,
    ) -> Result<usize, AedbError> {
        let mut reclaimed_count = 0usize;
        for filename in self.list_segment_filenames()? {
            if referenced_filenames.contains(&filename) {
                continue;
            }
            validate_segment_filename(&filename)?;
            self.block_cache.lock().remove_filename(&filename);
            fs::remove_file(self.dir.join(&filename))?;
            reclaimed_count = reclaimed_count.saturating_add(1);
        }
        Ok(reclaimed_count)
    }

    pub fn write_segment(
        &self,
        namespace_hint: &str,
        entries: Vec<KvSegmentEntry>,
    ) -> Result<KvSegmentMeta, AedbError> {
        if entries.is_empty() {
            return Err(AedbError::Validation(
                "cannot write empty KV segment".into(),
            ));
        }
        for pair in entries.windows(2) {
            if pair[0].key >= pair[1].key {
                return Err(AedbError::Validation(
                    "KV segment entries must be strictly sorted".into(),
                ));
            }
        }

        let min_key = entries[0].key.clone();
        let max_key = entries[entries.len() - 1].key.clone();
        let bloom_bits = build_bloom(&entries);
        let mut payload = Vec::with_capacity(
            KV_SEGMENT_MAGIC.len()
                + entries
                    .iter()
                    .map(|entry| entry.key.len().saturating_add(entry.entry.value.len() + 64))
                    .sum::<usize>(),
        );
        payload.extend_from_slice(KV_SEGMENT_MAGIC);
        let mut blocks = Vec::with_capacity(entries.len().div_ceil(KV_SEGMENT_BLOCK_ENTRIES));
        for block in entries.chunks(KV_SEGMENT_BLOCK_ENTRIES) {
            let byte_start = payload.len() as u64;
            for item in block {
                let encoded_entry =
                    rmp_serde::to_vec(&item.entry).map_err(|e| AedbError::Encode(e.to_string()))?;
                write_u32(&mut payload, item.key.len(), "KV segment key")?;
                write_u32(&mut payload, encoded_entry.len(), "KV segment entry")?;
                payload.extend_from_slice(&item.key);
                payload.extend_from_slice(&encoded_entry);
            }
            let byte_count = (payload.len() as u64).saturating_sub(byte_start);
            let block_start = byte_start as usize;
            let block_end = payload.len();
            blocks.push(KvSegmentBlockMeta {
                first_key: block[0].key.clone(),
                last_key: block[block.len() - 1].key.clone(),
                byte_start,
                byte_count,
                entry_count: block.len() as u32,
                sha256_hex: hex_string(Sha256::digest(&payload[block_start..block_end]).as_slice()),
            });
        }
        let sha256_hex = hex_string(Sha256::digest(&payload).as_slice());
        let created_at_micros = now_micros();
        let filename = format!(
            "kvseg_{}_{}_{}.aedbkv",
            sanitize_hint(namespace_hint),
            created_at_micros,
            uuid::Uuid::new_v4().simple()
        );
        let final_path = self.dir.join(&filename);
        let mut tmp = NamedTempFile::new_in(&self.dir)?;
        tmp.write_all(&payload)?;
        tmp.flush()?;
        tmp.as_file().sync_all()?;
        tmp.persist(&final_path)
            .map_err(|e| AedbError::Io(e.error))?;

        Ok(KvSegmentMeta {
            filename,
            min_key,
            max_key,
            entry_count: entries.len() as u64,
            sha256_hex,
            created_at_micros,
            bloom_bits,
            blocks,
        })
    }

    pub fn read_segment(&self, meta: &KvSegmentMeta) -> Result<Vec<KvSegmentEntry>, AedbError> {
        self.read_segment_with_cache_policy(meta, true)
    }

    pub fn read_segment_cold(
        &self,
        meta: &KvSegmentMeta,
    ) -> Result<Vec<KvSegmentEntry>, AedbError> {
        self.read_segment_with_cache_policy(meta, false)
    }

    fn read_segment_with_cache_policy(
        &self,
        meta: &KvSegmentMeta,
        populate_block_cache: bool,
    ) -> Result<Vec<KvSegmentEntry>, AedbError> {
        validate_segment_filename(&meta.filename)?;
        if meta.blocks.is_empty() {
            return self.read_legacy_segment(meta);
        }
        let mut file = self.open_validated_segment_file(&meta.filename)?;
        let mut entries = Vec::with_capacity(meta.entry_count as usize);
        for block in &meta.blocks {
            let cache_key = block_cache_key(meta, block);
            let block_entries = if populate_block_cache
                && let Some(cached) = self.block_cache.lock().get(&cache_key)
            {
                cached
            } else {
                let decoded = read_block_entries_from_open_file(block, &mut file)?;
                if populate_block_cache {
                    self.block_cache
                        .lock()
                        .insert(cache_key, Arc::clone(&decoded));
                }
                decoded
            };
            entries.extend(block_entries.iter().cloned());
        }
        validate_segment_entries(meta, &entries)?;
        Ok(entries)
    }

    pub fn scan_range(
        &self,
        meta: &KvSegmentMeta,
        start: &Bound<Vec<u8>>,
        end: &Bound<Vec<u8>>,
    ) -> Result<Vec<KvSegmentEntry>, AedbError> {
        self.scan_range_with_cache_policy(meta, start, end, usize::MAX, true)
    }

    pub(crate) fn scan_range_limited(
        &self,
        meta: &KvSegmentMeta,
        start: &Bound<Vec<u8>>,
        end: &Bound<Vec<u8>>,
        limit: usize,
    ) -> Result<Vec<KvSegmentEntry>, AedbError> {
        self.scan_range_with_cache_policy(meta, start, end, limit, true)
    }

    pub(crate) fn scan_range_limited_cold(
        &self,
        meta: &KvSegmentMeta,
        start: &Bound<Vec<u8>>,
        end: &Bound<Vec<u8>>,
        limit: usize,
    ) -> Result<Vec<KvSegmentEntry>, AedbError> {
        self.scan_range_with_cache_policy(meta, start, end, limit, false)
    }

    pub fn scan_range_cold(
        &self,
        meta: &KvSegmentMeta,
        start: &Bound<Vec<u8>>,
        end: &Bound<Vec<u8>>,
    ) -> Result<Vec<KvSegmentEntry>, AedbError> {
        self.scan_range_with_cache_policy(meta, start, end, usize::MAX, false)
    }

    fn scan_range_with_cache_policy(
        &self,
        meta: &KvSegmentMeta,
        start: &Bound<Vec<u8>>,
        end: &Bound<Vec<u8>>,
        limit: usize,
        populate_block_cache: bool,
    ) -> Result<Vec<KvSegmentEntry>, AedbError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        if !segment_overlaps_bounds(meta, start, end) {
            return Ok(Vec::new());
        }
        if meta.blocks.is_empty() {
            return self
                .read_legacy_segment(meta)
                .map(|entries| filter_entries_by_bounds(entries, start, end, limit));
        }
        let mut entries = Vec::with_capacity(limit.min(KV_SEGMENT_BLOCK_ENTRIES));
        let first_block_position = first_block_position_for_start(&meta.blocks, start);
        let mut file = None;
        for block in &meta.blocks[first_block_position..] {
            if block_starts_after_end(block, end) {
                break;
            }
            let block_entries = self.read_block_entries_with_shared_file(
                meta,
                block,
                populate_block_cache,
                &mut file,
            )?;
            for entry in block_entries
                .iter()
                .filter(|entry| key_within_bounds(&entry.key, start, end))
            {
                entries.push(entry.clone());
                if entries.len() >= limit {
                    return Ok(entries);
                }
            }
        }
        Ok(entries)
    }

    fn read_block_entries_with_shared_file(
        &self,
        meta: &KvSegmentMeta,
        block: &KvSegmentBlockMeta,
        populate_block_cache: bool,
        file: &mut Option<File>,
    ) -> Result<Arc<[KvSegmentEntry]>, AedbError> {
        let cache_key = block_cache_key(meta, block);
        if populate_block_cache && let Some(entries) = self.block_cache.lock().get(&cache_key) {
            return Ok(entries);
        }
        if file.is_none() {
            *file = Some(self.open_validated_segment_file(&meta.filename)?);
        }
        let decoded =
            read_block_entries_from_open_file(block, file.as_mut().expect("segment file opened"))?;
        if populate_block_cache {
            self.block_cache
                .lock()
                .insert(cache_key, Arc::clone(&decoded));
        }
        Ok(decoded)
    }

    fn read_legacy_segment(&self, meta: &KvSegmentMeta) -> Result<Vec<KvSegmentEntry>, AedbError> {
        let payload = fs::read(self.dir.join(&meta.filename))?;
        let hash = hex_string(Sha256::digest(&payload).as_slice());
        if hash != meta.sha256_hex {
            return Err(AedbError::IntegrityError {
                message: "KV segment checksum mismatch".into(),
            });
        }
        if payload.len() < KV_SEGMENT_MAGIC.len()
            || &payload[..KV_SEGMENT_MAGIC.len()] != KV_SEGMENT_MAGIC
        {
            return Err(AedbError::IntegrityError {
                message: "KV segment magic mismatch".into(),
            });
        }
        let entries: Vec<KvSegmentEntry> =
            rmp_serde::from_slice(&payload[KV_SEGMENT_MAGIC.len()..])
                .map_err(|e| AedbError::Decode(e.to_string()))?;
        validate_segment_entries(meta, &entries)?;
        Ok(entries)
    }

    fn read_block_entries(
        &self,
        meta: &KvSegmentMeta,
        block: &KvSegmentBlockMeta,
    ) -> Result<Arc<[KvSegmentEntry]>, AedbError> {
        self.read_block_entries_with_cache_policy(meta, block, true)
    }

    fn read_block_entries_with_cache_policy(
        &self,
        meta: &KvSegmentMeta,
        block: &KvSegmentBlockMeta,
        populate_block_cache: bool,
    ) -> Result<Arc<[KvSegmentEntry]>, AedbError> {
        validate_segment_filename(&meta.filename)?;
        let cache_key = block_cache_key(meta, block);
        if populate_block_cache && let Some(entries) = self.block_cache.lock().get(&cache_key) {
            return Ok(entries);
        }
        let mut file = self.open_validated_segment_file(&meta.filename)?;
        let entries = read_block_entries_from_open_file(block, &mut file)?;
        if populate_block_cache {
            self.block_cache
                .lock()
                .insert(cache_key, Arc::clone(&entries));
        }
        Ok(entries)
    }

    fn open_validated_segment_file(&self, filename: &str) -> Result<File, AedbError> {
        let mut file = File::open(self.dir.join(filename))?;
        let mut magic = [0u8; KV_SEGMENT_MAGIC.len()];
        file.read_exact(&mut magic)?;
        if &magic != KV_SEGMENT_MAGIC {
            return Err(AedbError::IntegrityError {
                message: "KV segment magic mismatch".into(),
            });
        }
        Ok(file)
    }

    pub fn get(&self, meta: &KvSegmentMeta, key: &[u8]) -> Result<Option<KvEntry>, AedbError> {
        if key < meta.min_key.as_slice() || key > meta.max_key.as_slice() {
            return Ok(None);
        }
        if !bloom_may_contain(meta, key) {
            return Ok(None);
        }
        if meta.blocks.is_empty() {
            let entries = self.read_legacy_segment(meta)?;
            return Ok(entries
                .binary_search_by(|entry| entry.key.as_slice().cmp(key))
                .ok()
                .map(|entry_position| entries[entry_position].entry.clone()));
        }
        let block_position = match meta
            .blocks
            .binary_search_by(|block| block.last_key.as_slice().cmp(key))
        {
            Ok(found_position) => found_position,
            Err(candidate_position) => candidate_position,
        };
        let Some(block) = meta.blocks.get(block_position) else {
            return Ok(None);
        };
        if key < block.first_key.as_slice() || key > block.last_key.as_slice() {
            return Ok(None);
        }
        let entries = self.read_block_entries(meta, block)?;
        Ok(entries
            .as_ref()
            .binary_search_by(|entry| entry.key.as_slice().cmp(key))
            .ok()
            .map(|entry_position| entries[entry_position].entry.clone()))
    }
}

fn block_cache_key(meta: &KvSegmentMeta, block: &KvSegmentBlockMeta) -> KvSegmentBlockCacheKey {
    KvSegmentBlockCacheKey {
        filename: meta.filename.clone(),
        byte_start: block.byte_start,
        byte_count: block.byte_count,
        sha256_hex: block.sha256_hex.clone(),
    }
}

fn read_block_entries_from_open_file(
    block: &KvSegmentBlockMeta,
    file: &mut File,
) -> Result<Arc<[KvSegmentEntry]>, AedbError> {
    file.seek(SeekFrom::Start(block.byte_start))?;
    let block_bytes = usize::try_from(block.byte_count).map_err(|_| {
        AedbError::Validation("KV segment block is too large to read on this platform".into())
    })?;
    let mut payload = vec![0u8; block_bytes];
    file.read_exact(&mut payload)?;
    let hash = hex_string(Sha256::digest(&payload).as_slice());
    if hash != block.sha256_hex {
        return Err(AedbError::IntegrityError {
            message: "KV segment block checksum mismatch".into(),
        });
    }
    let mut cursor = std::io::Cursor::new(payload);
    let mut entries = Vec::with_capacity(block.entry_count as usize);
    while cursor.position() < block.byte_count {
        let key_bytes = read_u32(&mut cursor)? as usize;
        let entry_bytes = read_u32(&mut cursor)? as usize;
        let mut key = vec![0u8; key_bytes];
        cursor.read_exact(&mut key)?;
        let mut encoded_entry = vec![0u8; entry_bytes];
        cursor.read_exact(&mut encoded_entry)?;
        let entry: KvEntry =
            rmp_serde::from_slice(&encoded_entry).map_err(|e| AedbError::Decode(e.to_string()))?;
        entries.push(KvSegmentEntry { key, entry });
    }
    if entries.len() as u32 != block.entry_count {
        return Err(AedbError::IntegrityError {
            message: "KV segment block entry count mismatch".into(),
        });
    }
    if entries.first().map(|entry| &entry.key) != Some(&block.first_key)
        || entries.last().map(|entry| &entry.key) != Some(&block.last_key)
    {
        return Err(AedbError::IntegrityError {
            message: "KV segment block key bounds mismatch".into(),
        });
    }
    for pair in entries.windows(2) {
        if pair[0].key >= pair[1].key {
            return Err(AedbError::IntegrityError {
                message: "KV segment block keys are not strictly sorted".into(),
            });
        }
    }
    let entries: Arc<[KvSegmentEntry]> = Arc::from(entries);
    Ok(entries)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct KvSegmentBlockCacheKey {
    filename: String,
    byte_start: u64,
    byte_count: u64,
    sha256_hex: String,
}

#[derive(Debug)]
struct CachedKvSegmentBlock {
    entries: Arc<[KvSegmentEntry]>,
    resident_bytes: usize,
    access_tick: u64,
}

#[derive(Debug)]
struct KvSegmentBlockCache {
    capacity_bytes: usize,
    resident_bytes: usize,
    access_tick: u64,
    blocks: HashMap<KvSegmentBlockCacheKey, CachedKvSegmentBlock>,
    access_order: BTreeMap<u64, KvSegmentBlockCacheKey>,
}

impl KvSegmentBlockCache {
    fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes,
            resident_bytes: 0,
            access_tick: 0,
            blocks: HashMap::new(),
            access_order: BTreeMap::new(),
        }
    }

    fn resident_bytes(&self) -> usize {
        self.resident_bytes
    }

    fn next_access_tick(&mut self) -> u64 {
        self.access_tick = self.access_tick.saturating_add(1);
        self.access_tick
    }

    fn get(&mut self, cache_key: &KvSegmentBlockCacheKey) -> Option<Arc<[KvSegmentEntry]>> {
        let access_tick = self.next_access_tick();
        let cached = self.blocks.get_mut(cache_key)?;
        self.access_order.remove(&cached.access_tick);
        cached.access_tick = access_tick;
        self.access_order.insert(access_tick, cache_key.clone());
        Some(Arc::clone(&cached.entries))
    }

    fn insert(&mut self, cache_key: KvSegmentBlockCacheKey, entries: Arc<[KvSegmentEntry]>) {
        let resident_bytes = kv_segment_block_entries_cost(&entries);
        if self.capacity_bytes == 0 || resident_bytes > self.capacity_bytes {
            return;
        }
        if let Some(cached) = self.blocks.remove(&cache_key) {
            self.access_order.remove(&cached.access_tick);
            self.resident_bytes = self.resident_bytes.saturating_sub(cached.resident_bytes);
        }
        let access_tick = self.next_access_tick();
        self.resident_bytes = self.resident_bytes.saturating_add(resident_bytes);
        self.access_order.insert(access_tick, cache_key.clone());
        self.blocks.insert(
            cache_key,
            CachedKvSegmentBlock {
                entries,
                resident_bytes,
                access_tick,
            },
        );
        self.evict_to_capacity();
    }

    fn remove_filename(&mut self, filename: &str) {
        let keys = self
            .blocks
            .keys()
            .filter(|cache_key| cache_key.filename == filename)
            .cloned()
            .collect::<Vec<_>>();
        for cache_key in keys {
            if let Some(cached) = self.blocks.remove(&cache_key) {
                self.access_order.remove(&cached.access_tick);
                self.resident_bytes = self.resident_bytes.saturating_sub(cached.resident_bytes);
            }
        }
    }

    fn evict_to_capacity(&mut self) {
        while self.resident_bytes > self.capacity_bytes {
            let Some((access_tick, cache_key)) = self
                .access_order
                .iter()
                .next()
                .map(|(access_tick, cache_key)| (*access_tick, cache_key.clone()))
            else {
                break;
            };
            self.access_order.remove(&access_tick);
            if let Some(cached) = self.blocks.remove(&cache_key) {
                self.resident_bytes = self.resident_bytes.saturating_sub(cached.resident_bytes);
            }
        }
    }
}

fn kv_segment_block_entries_cost(entries: &[KvSegmentEntry]) -> usize {
    entries
        .iter()
        .map(|entry| {
            entry
                .key
                .len()
                .saturating_add(entry.entry.value.len())
                .saturating_add(96)
        })
        .sum()
}

fn validate_segment_entries(
    meta: &KvSegmentMeta,
    entries: &[KvSegmentEntry],
) -> Result<(), AedbError> {
    if entries.len() as u64 != meta.entry_count {
        return Err(AedbError::IntegrityError {
            message: "KV segment entry count mismatch".into(),
        });
    }
    if entries.first().map(|entry| &entry.key) != Some(&meta.min_key)
        || entries.last().map(|entry| &entry.key) != Some(&meta.max_key)
    {
        return Err(AedbError::IntegrityError {
            message: "KV segment key bounds mismatch".into(),
        });
    }
    for pair in entries.windows(2) {
        if pair[0].key >= pair[1].key {
            return Err(AedbError::IntegrityError {
                message: "KV segment entries are not strictly sorted".into(),
            });
        }
    }
    Ok(())
}

fn filter_entries_by_bounds(
    entries: Vec<KvSegmentEntry>,
    start: &Bound<Vec<u8>>,
    end: &Bound<Vec<u8>>,
    limit: usize,
) -> Vec<KvSegmentEntry> {
    entries
        .into_iter()
        .filter(|entry| key_within_bounds(&entry.key, start, end))
        .take(limit)
        .collect()
}

fn key_within_bounds(key: &[u8], start: &Bound<Vec<u8>>, end: &Bound<Vec<u8>>) -> bool {
    let after_start = match start {
        Bound::Included(start) => key >= start.as_slice(),
        Bound::Excluded(start) => key > start.as_slice(),
        Bound::Unbounded => true,
    };
    let before_end = match end {
        Bound::Included(end) => key <= end.as_slice(),
        Bound::Excluded(end) => key < end.as_slice(),
        Bound::Unbounded => true,
    };
    after_start && before_end
}

fn segment_overlaps_bounds(
    meta: &KvSegmentMeta,
    start: &Bound<Vec<u8>>,
    end: &Bound<Vec<u8>>,
) -> bool {
    let after_start = match start {
        Bound::Included(start) => meta.max_key.as_slice() >= start.as_slice(),
        Bound::Excluded(start) => meta.max_key.as_slice() > start.as_slice(),
        Bound::Unbounded => true,
    };
    let before_end = match end {
        Bound::Included(end) => meta.min_key.as_slice() <= end.as_slice(),
        Bound::Excluded(end) => meta.min_key.as_slice() < end.as_slice(),
        Bound::Unbounded => true,
    };
    after_start && before_end
}

fn first_block_position_for_start(blocks: &[KvSegmentBlockMeta], start: &Bound<Vec<u8>>) -> usize {
    match start {
        Bound::Unbounded => 0,
        Bound::Included(start) => {
            match blocks.binary_search_by(|block| block.last_key.as_slice().cmp(start.as_slice())) {
                Ok(position) | Err(position) => position,
            }
        }
        Bound::Excluded(start) => {
            match blocks.binary_search_by(|block| block.last_key.as_slice().cmp(start.as_slice())) {
                Ok(position) => position.saturating_add(1),
                Err(position) => position,
            }
        }
    }
}

fn block_starts_after_end(block: &KvSegmentBlockMeta, end: &Bound<Vec<u8>>) -> bool {
    match end {
        Bound::Unbounded => false,
        Bound::Included(end) => block.first_key.as_slice() > end.as_slice(),
        Bound::Excluded(end) => block.first_key.as_slice() >= end.as_slice(),
    }
}

fn write_u32(out: &mut Vec<u8>, value: usize, label: &str) -> Result<(), AedbError> {
    let value = u32::try_from(value)
        .map_err(|_| AedbError::Validation(format!("{label} exceeds u32 frame bound")))?;
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn read_u32(cursor: &mut std::io::Cursor<Vec<u8>>) -> Result<u32, AedbError> {
    let mut bytes = [0u8; 4];
    cursor.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn build_bloom(entries: &[KvSegmentEntry]) -> Vec<u64> {
    let mut bits = vec![0u64; KV_SEGMENT_BLOOM_WORDS];
    for entry in entries {
        for bit in bloom_bits_for_key(&entry.key) {
            let word = (bit / 64) as usize;
            let bit_shift = bit % 64;
            bits[word] |= 1u64 << bit_shift;
        }
    }
    bits
}

fn bloom_may_contain(meta: &KvSegmentMeta, key: &[u8]) -> bool {
    if meta.bloom_bits.len() != KV_SEGMENT_BLOOM_WORDS {
        return true;
    }
    bloom_bits_for_key(key).into_iter().all(|bit| {
        let word = (bit / 64) as usize;
        let bit_shift = bit % 64;
        meta.bloom_bits[word] & (1u64 << bit_shift) != 0
    })
}

fn bloom_bits_for_key(key: &[u8]) -> [u64; KV_SEGMENT_BLOOM_HASHES as usize] {
    let digest = blake3::hash(key);
    let bytes = digest.as_bytes();
    let first = u64::from_le_bytes(bytes[0..8].try_into().expect("hash prefix"));
    let second = u64::from_le_bytes(bytes[8..16].try_into().expect("hash suffix")) | 1;
    let mut out = [0u64; KV_SEGMENT_BLOOM_HASHES as usize];
    for probe_number in 0..KV_SEGMENT_BLOOM_HASHES {
        out[probe_number as usize] =
            first.wrapping_add(probe_number.wrapping_mul(second)) % KV_SEGMENT_BLOOM_BITS;
    }
    out
}

fn validate_segment_filename(filename: &str) -> Result<(), AedbError> {
    if filename.is_empty() || filename.contains('/') || filename.contains('\\') {
        return Err(AedbError::Validation("invalid KV segment filename".into()));
    }
    Ok(())
}

fn is_kv_segment_filename(filename: &str) -> bool {
    filename.starts_with("kvseg_")
        && filename.ends_with(".aedbkv")
        && validate_segment_filename(filename).is_ok()
}

fn sanitize_hint(input: &str) -> String {
    let mut out = String::with_capacity(input.len().min(48));
    for b in input.bytes().take(48) {
        if b.is_ascii_alphanumeric() {
            out.push(b as char);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() { "ns".into() } else { out }
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

fn hex_string(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(value: u8, version: u64) -> KvEntry {
        KvEntry {
            value: vec![value; 32],
            version,
            created_at: version,
            value_ref: None,
        }
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
}
