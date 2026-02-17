use crate::wal::frame::FrameWriter;
use crate::wal::rotation::RotationReason;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;

pub const SEGMENT_MAGIC: u32 = 0x4145_4442;
pub const SEGMENT_HEADER_SIZE: usize = 64;

#[derive(Debug, Error)]
pub enum SegmentError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid segment magic")]
    InvalidMagic,
    #[error("segment is not open")]
    NotOpen,
}

#[derive(Debug, Clone)]
pub struct SegmentConfig {
    pub max_segment_bytes: u64,
    pub max_segment_age: Duration,
}

impl Default for SegmentConfig {
    fn default() -> Self {
        Self {
            max_segment_bytes: 64 * 1024 * 1024,
            max_segment_age: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentHeader {
    pub magic: u32,
    pub format_version: u16,
    pub instance_id: u64,
    pub segment_seq: u64,
    pub created_at_micros: u64,
    pub prev_segment_hash: [u8; 32],
}

impl SegmentHeader {
    pub fn new(instance_id: u64, segment_seq: u64, prev_segment_hash: [u8; 32]) -> Self {
        let created_at_micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        Self {
            magic: SEGMENT_MAGIC,
            format_version: 1,
            instance_id,
            segment_seq,
            created_at_micros,
            prev_segment_hash,
        }
    }

    pub fn to_bytes(&self) -> [u8; SEGMENT_HEADER_SIZE] {
        let mut out = [0u8; SEGMENT_HEADER_SIZE];
        out[0..4].copy_from_slice(&self.magic.to_be_bytes());
        out[4..6].copy_from_slice(&self.format_version.to_be_bytes());
        out[6..8].copy_from_slice(&0u16.to_be_bytes());
        out[8..16].copy_from_slice(&self.instance_id.to_be_bytes());
        out[16..24].copy_from_slice(&self.segment_seq.to_be_bytes());
        out[24..32].copy_from_slice(&self.created_at_micros.to_be_bytes());
        out[32..64].copy_from_slice(&self.prev_segment_hash);
        out
    }

    pub fn from_bytes(bytes: &[u8; SEGMENT_HEADER_SIZE]) -> Result<Self, SegmentError> {
        let magic = u32::from_be_bytes(bytes[0..4].try_into().expect("slice len"));
        if magic != SEGMENT_MAGIC {
            return Err(SegmentError::InvalidMagic);
        }
        Ok(Self {
            magic,
            format_version: u16::from_be_bytes(bytes[4..6].try_into().expect("slice len")),
            instance_id: u64::from_be_bytes(bytes[8..16].try_into().expect("slice len")),
            segment_seq: u64::from_be_bytes(bytes[16..24].try_into().expect("slice len")),
            created_at_micros: u64::from_be_bytes(bytes[24..32].try_into().expect("slice len")),
            prev_segment_hash: bytes[32..64].try_into().expect("slice len"),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ClosedSegment {
    pub path: PathBuf,
    pub segment_seq: u64,
    pub hash: [u8; 32],
    pub size_bytes: u64,
}

struct ActiveSegment {
    file: File,
    path: PathBuf,
    segment_seq: u64,
    opened_at: Instant,
    size_bytes: u64,
    commit_count: u64,
    hasher: blake3::Hasher,
}

pub struct SegmentManager {
    dir: PathBuf,
    config: SegmentConfig,
    instance_id: u64,
    prev_hash: [u8; 32],
    active: Option<ActiveSegment>,
    force_rotate: bool,
}

impl SegmentManager {
    pub fn new(dir: impl Into<PathBuf>, config: SegmentConfig, instance_id: u64) -> Self {
        Self {
            dir: dir.into(),
            config,
            instance_id,
            prev_hash: [0u8; 32],
            active: None,
            force_rotate: false,
        }
    }

    pub fn open_active(&mut self, segment_seq: u64) -> Result<(), SegmentError> {
        fs::create_dir_all(&self.dir)?;
        let path = self.dir.join(format!("segment_{segment_seq:016}.aedbwal"));
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        let mut hasher = blake3::Hasher::new();
        if file.metadata()?.len() == 0 {
            let header = SegmentHeader::new(self.instance_id, segment_seq, self.prev_hash);
            let header_bytes = header.to_bytes();
            file.write_all(&header_bytes)?;
            hasher.update(&header_bytes);
            file.flush()?;
        } else {
            let mut reader = File::open(&path)?;
            let mut buf = [0u8; 64 * 1024];
            loop {
                let n = reader.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
        }

        let size_bytes = file.metadata()?.len();
        self.active = Some(ActiveSegment {
            file,
            path,
            segment_seq,
            opened_at: Instant::now(),
            size_bytes,
            commit_count: 0,
            hasher,
        });
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn append_frame(
        &mut self,
        seq: u64,
        timestamp_micros: u64,
        payload_type: u8,
        payload: &[u8],
    ) -> Result<(), SegmentError> {
        self.append_frame_with_sync(seq, timestamp_micros, payload_type, payload, true)
    }

    pub(crate) fn append_frame_with_sync(
        &mut self,
        seq: u64,
        timestamp_micros: u64,
        payload_type: u8,
        payload: &[u8],
        sync: bool,
    ) -> Result<(), SegmentError> {
        let active = self.active.as_mut().ok_or(SegmentError::NotOpen)?;
        {
            let mut writer = FrameWriter::new(Vec::<u8>::new());
            writer
                .append(seq, timestamp_micros, payload_type, payload)
                .map_err(|e| SegmentError::Io(std::io::Error::other(e.to_string())))?;
            let frame = writer.into_inner();
            active.file.write_all(&frame)?;
            active.hasher.update(&frame);
            active.size_bytes = active.size_bytes.saturating_add(frame.len() as u64);
        }
        if sync {
            active.file.flush()?;
            active.file.sync_data()?;
        }
        active.commit_count += 1;
        Ok(())
    }

    pub fn sync_active(&mut self) -> Result<(), SegmentError> {
        let active = self.active.as_mut().ok_or(SegmentError::NotOpen)?;
        active.file.sync_data()?;
        Ok(())
    }

    pub fn should_rotate(&self) -> Option<RotationReason> {
        self.should_rotate_at(Instant::now())
    }

    pub fn should_rotate_at(&self, now: Instant) -> Option<RotationReason> {
        let active = self.active.as_ref()?;
        if self.force_rotate {
            return Some(RotationReason::Forced);
        }
        if active.size_bytes >= self.config.max_segment_bytes {
            return Some(RotationReason::Size);
        }
        if now.duration_since(active.opened_at) >= self.config.max_segment_age {
            return Some(RotationReason::Time);
        }
        None
    }

    pub fn force_rotate(&mut self) {
        self.force_rotate = true;
    }

    pub fn rotate(&mut self) -> Result<ClosedSegment, SegmentError> {
        let closed = self.close_active()?;
        self.prev_hash = closed.hash;
        self.force_rotate = false;
        self.open_active(closed.segment_seq + 1)?;
        Ok(closed)
    }

    pub fn close_active(&mut self) -> Result<ClosedSegment, SegmentError> {
        let mut active = self.active.take().ok_or(SegmentError::NotOpen)?;
        active.file.flush()?;
        let hash = *active.hasher.finalize().as_bytes();
        let size_bytes = active.file.metadata()?.len();

        Ok(ClosedSegment {
            path: active.path,
            segment_seq: active.segment_seq,
            hash,
            size_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        SEGMENT_HEADER_SIZE, SEGMENT_MAGIC, SegmentConfig, SegmentError, SegmentHeader,
        SegmentManager,
    };
    use crate::wal::rotation::RotationReason;
    use std::fs;
    use std::time::{Duration, Instant};
    use tempfile::tempdir;

    #[test]
    fn segment_header_roundtrip_and_magic_validation() {
        let header = SegmentHeader::new(42, 7, [9u8; 32]);
        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), SEGMENT_HEADER_SIZE);
        let decoded = SegmentHeader::from_bytes(&bytes).expect("decode");
        assert_eq!(decoded.magic, SEGMENT_MAGIC);
        assert_eq!(decoded.instance_id, 42);
        assert_eq!(decoded.segment_seq, 7);
        assert_eq!(decoded.prev_segment_hash, [9u8; 32]);

        let mut bad = bytes;
        bad[0] = 0;
        assert!(matches!(
            SegmentHeader::from_bytes(&bad),
            Err(SegmentError::InvalidMagic)
        ));
    }

    #[test]
    fn size_rotation_trigger_and_no_split_invariant() {
        let dir = tempdir().expect("temp dir");
        let mut manager = SegmentManager::new(
            dir.path(),
            SegmentConfig {
                max_segment_bytes: 160,
                max_segment_age: Duration::from_secs(3600),
            },
            1,
        );
        manager.open_active(1).expect("open");

        manager
            .append_frame(1, 1, 0x01, &[0u8; 128])
            .expect("append");

        assert_eq!(manager.should_rotate(), Some(RotationReason::Size));
        let closed = manager.rotate().expect("rotate");
        assert_eq!(closed.segment_seq, 1);
        assert!(closed.size_bytes > 160);
    }

    #[test]
    fn time_rotation_trigger() {
        let dir = tempdir().expect("temp dir");
        let mut manager = SegmentManager::new(
            dir.path(),
            SegmentConfig {
                max_segment_bytes: 10_000,
                max_segment_age: Duration::from_millis(10),
            },
            2,
        );
        manager.open_active(1).expect("open");
        manager.append_frame(1, 1, 0x01, &[1]).expect("append");

        let now = Instant::now() + Duration::from_millis(20);
        assert_eq!(manager.should_rotate_at(now), Some(RotationReason::Time));
    }

    #[test]
    fn hash_chain_propagates_across_rotations() {
        let dir = tempdir().expect("temp dir");
        let mut manager = SegmentManager::new(
            dir.path(),
            SegmentConfig {
                max_segment_bytes: 200,
                max_segment_age: Duration::from_secs(3600),
            },
            3,
        );
        manager.open_active(1).expect("open");

        let mut closed = Vec::new();
        for i in 1..=5 {
            manager
                .append_frame(i, i, 0x01, &[i as u8; 128])
                .expect("append");
            if manager.should_rotate().is_some() {
                closed.push(manager.rotate().expect("rotate"));
            }
        }
        closed.push(manager.close_active().expect("close"));
        assert!(closed.len() >= 2);

        for s in &closed {
            let data = fs::read(&s.path).expect("read segment");
            let header: [u8; SEGMENT_HEADER_SIZE] =
                data[..SEGMENT_HEADER_SIZE].try_into().expect("header");
            let parsed = SegmentHeader::from_bytes(&header).expect("header parse");
            if parsed.segment_seq == 1 {
                assert_eq!(parsed.prev_segment_hash, [0u8; 32]);
            } else {
                let prev = closed
                    .iter()
                    .find(|x| x.segment_seq + 1 == parsed.segment_seq)
                    .expect("previous segment");
                assert_eq!(parsed.prev_segment_hash, prev.hash);
            }
        }
    }
}
