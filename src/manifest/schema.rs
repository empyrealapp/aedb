use crate::checkpoint::writer::CheckpointMeta;
use serde::{Deserialize, Serialize};

/// Identifies an AEDB manifest file. Equal to the WAL segment magic (`"AEDB"`).
pub const MANIFEST_MAGIC: u32 = 0x4145_4442;

/// Current manifest on-disk format version. Bump when the manifest layout
/// changes in a way older readers cannot tolerate; gate behaviour on
/// [`Manifest::feature_flags`] for additive, backward-compatible changes.
pub const MANIFEST_FORMAT_VERSION: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentMeta {
    pub filename: String,
    pub segment_seq: u64,
    #[serde(default)]
    pub sha256_hex: String,
    #[serde(default)]
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct Manifest {
    /// Format identifier. `0` denotes a legacy manifest written before headers
    /// existed (accepted for backward compatibility); current writers stamp
    /// [`MANIFEST_MAGIC`].
    #[serde(default)]
    pub magic: u32,
    /// On-disk format version. `0` is legacy/pre-versioning. See
    /// [`MANIFEST_FORMAT_VERSION`].
    #[serde(default)]
    pub format_version: u16,
    /// Bitset of optional features the writer relied on. Reserved for forward
    /// compatibility; currently always `0`.
    #[serde(default)]
    pub feature_flags: u64,
    pub durable_seq: u64,
    pub visible_seq: u64,
    pub active_segment_seq: u64,
    pub checkpoints: Vec<CheckpointMeta>,
    pub segments: Vec<SegmentMeta>,
}

impl Manifest {
    /// Stamp the current format header onto this manifest. Called on the write
    /// path so every freshly persisted manifest is self-describing.
    pub fn stamp_current_header(&mut self) {
        self.magic = MANIFEST_MAGIC;
        self.format_version = MANIFEST_FORMAT_VERSION;
    }

    /// Validate the format header after load. Accepts legacy manifests
    /// (`magic == 0`), rejects a wrong magic or a future format version this
    /// build cannot understand.
    pub fn validate_header(&self) -> Result<(), crate::error::AedbError> {
        if self.magic == 0 {
            // Legacy manifest written before headers existed.
            return Ok(());
        }
        if self.magic != MANIFEST_MAGIC {
            return Err(crate::error::AedbError::IntegrityError {
                message: format!(
                    "manifest magic mismatch: expected {MANIFEST_MAGIC:#010x}, found {:#010x}",
                    self.magic
                ),
            });
        }
        if self.format_version > MANIFEST_FORMAT_VERSION {
            return Err(crate::error::AedbError::Unavailable {
                message: format!(
                    "manifest format version {} is newer than supported {}",
                    self.format_version, MANIFEST_FORMAT_VERSION
                ),
            });
        }
        Ok(())
    }
}
