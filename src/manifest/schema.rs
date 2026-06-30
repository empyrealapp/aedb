use crate::checkpoint::writer::CheckpointMeta;
use serde::{Deserialize, Serialize};

/// Identifies an AEDB manifest file. Equal to the WAL segment magic (`"AEDB"`).
pub const MANIFEST_MAGIC: u32 = 0x4145_4442;

/// Current manifest on-disk format version. Bump when the on-disk format
/// changes in a way older readers cannot tolerate; gate behaviour on
/// [`Manifest::feature_flags`] for additive, backward-compatible changes.
///
/// History:
/// - `1`: first versioned header.
/// - `2`: `EncodedKey` sign-flips I256 so signed 256-bit keys sort correctly.
///   Keys persisted by older builds use the previous (raw two's-complement)
///   ordering, so pre-`2` databases are refused rather than read with a mix of
///   orderings. (Early-beta clean break; no in-place migration.)
pub const MANIFEST_FORMAT_VERSION: u16 = 2;

/// Oldest on-disk format version this build can read. Databases older than this
/// (including legacy pre-header manifests) are refused on load.
pub const MIN_SUPPORTED_FORMAT_VERSION: u16 = 2;

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

    /// Validate the format header after load. Rejects a wrong magic, a format
    /// version newer than this build understands, and any version older than
    /// [`MIN_SUPPORTED_FORMAT_VERSION`] (including legacy pre-header manifests,
    /// `magic == 0`), whose on-disk key ordering this build cannot interpret.
    pub fn validate_header(&self) -> Result<(), crate::error::AedbError> {
        if self.magic == 0 {
            // Legacy manifest written before headers existed; its keys predate
            // the signed-I256 encoding and cannot be read by this build.
            return Err(crate::error::AedbError::Unavailable {
                message: format!(
                    "legacy database predates on-disk format version {MIN_SUPPORTED_FORMAT_VERSION} \
                     and must be recreated"
                ),
            });
        }
        if self.magic != MANIFEST_MAGIC {
            return Err(crate::error::AedbError::IntegrityError {
                message: format!(
                    "manifest magic mismatch: expected {MANIFEST_MAGIC:#010x}, found {:#010x}",
                    self.magic
                ),
            });
        }
        if self.format_version < MIN_SUPPORTED_FORMAT_VERSION {
            return Err(crate::error::AedbError::Unavailable {
                message: format!(
                    "database on-disk format version {} is older than the minimum supported {} \
                     and must be recreated",
                    self.format_version, MIN_SUPPORTED_FORMAT_VERSION
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
