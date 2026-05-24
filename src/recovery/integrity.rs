use crate::error::AedbError;
use std::fs::File;
use std::io::Read;
use std::path::Path;

pub(super) fn is_valid_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.as_bytes().iter().all(|b| b.is_ascii_hexdigit())
}

pub(super) fn sha256_prefix_hex(path: &Path, bytes_to_hash: u64) -> Result<String, AedbError> {
    use sha2::{Digest, Sha256};

    let mut file = File::open(path)?;
    let mut reader = std::io::BufReader::with_capacity(1024 * 1024, &mut file);
    let mut hasher = Sha256::new();
    let mut remaining_size_bytes = bytes_to_hash;
    let mut buffer = vec![0u8; 1024 * 1024];
    while remaining_size_bytes > 0 {
        let read_size_bytes =
            usize::try_from(remaining_size_bytes.min(buffer.len() as u64)).unwrap_or(buffer.len());
        debug_assert!(read_size_bytes <= buffer.len());
        let read_count = reader.read(&mut buffer[..read_size_bytes])?;
        if read_count == 0 {
            break;
        }
        hasher.update(&buffer[..read_count]);
        remaining_size_bytes = remaining_size_bytes.saturating_sub(read_count as u64);
    }
    if remaining_size_bytes > 0 {
        return Err(AedbError::Validation(
            "segment shorter than expected".into(),
        ));
    }
    Ok(hex_string(hasher.finalize().as_slice()))
}

fn hex_string(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}
