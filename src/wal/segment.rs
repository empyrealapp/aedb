use crate::wal::frame::append_frame_bytes;
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

pub(crate) struct PendingFrame<'a> {
    pub seq: u64,
    pub timestamp_micros: u64,
    pub payload_type: u8,
    pub payload: &'a [u8],
}

pub struct SegmentManager {
    dir: PathBuf,
    config: SegmentConfig,
    instance_id: u64,
    prev_hash: [u8; 32],
    active: Option<ActiveSegment>,
    force_rotate: bool,
    encode_scratch: Vec<u8>,
}

impl SegmentManager {
    const FRAME_FIXED_BYTES: usize = 4 + 8 + 8 + 1 + 4;

    pub fn new(dir: impl Into<PathBuf>, config: SegmentConfig, instance_id: u64) -> Self {
        Self {
            dir: dir.into(),
            config,
            instance_id,
            prev_hash: [0u8; 32],
            active: None,
            force_rotate: false,
            encode_scratch: Vec::new(),
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
        let single = [PendingFrame {
            seq,
            timestamp_micros,
            payload_type,
            payload,
        }];
        self.append_frames_with_sync(&single, true)
    }

    pub(crate) fn append_frames_with_sync(
        &mut self,
        frames: &[PendingFrame<'_>],
        sync: bool,
    ) -> Result<(), SegmentError> {
        if frames.is_empty() {
            return Ok(());
        }
        let active = self.active.as_mut().ok_or(SegmentError::NotOpen)?;

        let estimated_encoded_bytes = frames.iter().fold(0usize, |acc, frame| {
            acc.saturating_add(Self::FRAME_FIXED_BYTES.saturating_add(frame.payload.len()))
        });
        let mut encoded_frames = std::mem::take(&mut self.encode_scratch);
        encoded_frames.clear();
        if encoded_frames.capacity() < estimated_encoded_bytes {
            encoded_frames.reserve(estimated_encoded_bytes - encoded_frames.capacity());
        }
        for frame in frames {
            append_frame_bytes(
                &mut encoded_frames,
                frame.seq,
                frame.timestamp_micros,
                frame.payload_type,
                frame.payload,
            )
            .map_err(|e| SegmentError::Io(std::io::Error::other(e.to_string())))?;
        }
        let encoded_size_bytes = encoded_frames.len() as u64;

        active.file.write_all(&encoded_frames)?;
        active.hasher.update(&encoded_frames);
        active.size_bytes = active.size_bytes.saturating_add(encoded_size_bytes);

        if sync {
            active.file.flush()?;
            active.file.sync_data()?;
        }
        active.commit_count = active.commit_count.saturating_add(frames.len() as u64);
        encoded_frames.clear();
        self.encode_scratch = encoded_frames;
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
        let hash = *blake3::Hasher::finalize(&active.hasher).as_bytes();
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
    use crate::wal::frame::FrameReader;
    use crate::wal::rotation::RotationReason;
    use std::fs;
    use std::io::Cursor;
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

    #[test]
    fn close_active_hash_matches_segment_bytes() {
        let dir = tempdir().expect("temp dir");
        let mut manager = SegmentManager::new(
            dir.path(),
            SegmentConfig {
                max_segment_bytes: 10_000,
                max_segment_age: Duration::from_secs(3600),
            },
            11,
        );
        manager.open_active(1).expect("open");
        manager
            .append_frame(1, 1, 0x01, b"segment-payload")
            .expect("append");

        let closed = manager.close_active().expect("close");
        let bytes = fs::read(&closed.path).expect("read segment");

        assert_eq!(closed.hash, *blake3::hash(&bytes).as_bytes());
    }

    #[test]
    fn append_frames_batch_writes_multiple_frames_in_order() {
        let dir = tempdir().expect("temp dir");
        let mut manager = SegmentManager::new(
            dir.path(),
            SegmentConfig {
                max_segment_bytes: 10_000,
                max_segment_age: Duration::from_secs(3600),
            },
            9,
        );
        manager.open_active(1).expect("open");

        let batch = [
            super::PendingFrame {
                seq: 11,
                timestamp_micros: 111,
                payload_type: 0x01,
                payload: b"first",
            },
            super::PendingFrame {
                seq: 12,
                timestamp_micros: 222,
                payload_type: 0x02,
                payload: b"second",
            },
        ];
        manager
            .append_frames_with_sync(&batch, false)
            .expect("append batch");
        let closed = manager.close_active().expect("close");

        let data = fs::read(&closed.path).expect("read segment");
        let mut reader = FrameReader::new(Cursor::new(&data[SEGMENT_HEADER_SIZE..]));
        let first = reader
            .next_frame()
            .expect("first frame parse")
            .expect("first frame");
        assert_eq!(first.commit_seq, 11);
        assert_eq!(first.timestamp_micros, 111);
        assert_eq!(first.payload_type, 0x01);
        assert_eq!(first.payload, b"first");

        let second = reader
            .next_frame()
            .expect("second frame parse")
            .expect("second frame");
        assert_eq!(second.commit_seq, 12);
        assert_eq!(second.timestamp_micros, 222);
        assert_eq!(second.payload_type, 0x02);
        assert_eq!(second.payload, b"second");
        assert!(reader.next_frame().expect("end parse").is_none());
    }
}
