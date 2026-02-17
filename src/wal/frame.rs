use crc32c::crc32c;
use std::io::{self, Read, Write};
use thiserror::Error;

pub const MAX_FRAME_BODY_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub frame_length: u32,
    pub commit_seq: u64,
    pub timestamp_micros: u64,
    pub payload_type: u8,
    pub payload: Vec<u8>,
    pub crc32c: u32,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum FrameError {
    #[error("truncated frame")]
    Truncation,
    #[error("corrupt frame")]
    Corruption,
    #[error("io error: {0}")]
    Io(String),
}

impl From<io::Error> for FrameError {
    fn from(value: io::Error) -> Self {
        Self::Io(value.to_string())
    }
}

pub struct FrameWriter<W: Write> {
    inner: W,
}

impl<W: Write> FrameWriter<W> {
    pub fn new(inner: W) -> Self {
        Self { inner }
    }

    pub fn append(
        &mut self,
        commit_seq: u64,
        timestamp_micros: u64,
        payload_type: u8,
        payload: &[u8],
    ) -> Result<(), FrameError> {
        let body_len = 8 + 8 + 1 + payload.len() + 4;
        let frame_length = u32::try_from(body_len).map_err(|_| FrameError::Corruption)?;
        let len_bytes = frame_length.to_be_bytes();
        let seq_bytes = commit_seq.to_be_bytes();
        let ts_bytes = timestamp_micros.to_be_bytes();
        let type_bytes = [payload_type];

        let mut crc_input = Vec::with_capacity(4 + body_len - 4);
        crc_input.extend_from_slice(&len_bytes);
        crc_input.extend_from_slice(&seq_bytes);
        crc_input.extend_from_slice(&ts_bytes);
        crc_input.extend_from_slice(&type_bytes);
        crc_input.extend_from_slice(payload);
        let crc = crc32c(&crc_input).to_be_bytes();

        self.inner.write_all(&len_bytes)?;
        self.inner.write_all(&seq_bytes)?;
        self.inner.write_all(&ts_bytes)?;
        self.inner.write_all(&type_bytes)?;
        self.inner.write_all(payload)?;
        self.inner.write_all(&crc)?;
        Ok(())
    }

    pub fn into_inner(self) -> W {
        self.inner
    }
}

pub struct FrameReader<R: Read> {
    inner: R,
}

impl<R: Read> FrameReader<R> {
    pub fn new(inner: R) -> Self {
        Self { inner }
    }

    pub fn next_frame(&mut self) -> Result<Option<Frame>, FrameError> {
        let mut len_buf = [0u8; 4];
        let first = self.inner.read(&mut len_buf[0..1])?;
        if first == 0 {
            return Ok(None);
        }
        match self.inner.read_exact(&mut len_buf[1..4]) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(FrameError::Truncation);
            }
            Err(e) => return Err(FrameError::Io(e.to_string())),
        }
        let frame_length = u32::from_be_bytes(len_buf);
        let body_len = frame_length as usize;
        if body_len < 8 + 8 + 1 + 4 {
            return Err(FrameError::Corruption);
        }
        if body_len > MAX_FRAME_BODY_BYTES {
            return Err(FrameError::Corruption);
        }

        let mut body = vec![0u8; body_len];
        match self.inner.read_exact(&mut body) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(FrameError::Truncation);
            }
            Err(e) => return Err(FrameError::Io(e.to_string())),
        }

        let crc_offset = body_len - 4;
        let stored_crc = u32::from_be_bytes(
            body[crc_offset..]
                .try_into()
                .map_err(|_| FrameError::Corruption)?,
        );
        let mut crc_input = Vec::with_capacity(4 + crc_offset);
        crc_input.extend_from_slice(&len_buf);
        crc_input.extend_from_slice(&body[..crc_offset]);
        let computed_crc = crc32c(&crc_input);
        if stored_crc != computed_crc {
            return Err(FrameError::Corruption);
        }

        let commit_seq =
            u64::from_be_bytes(body[0..8].try_into().map_err(|_| FrameError::Corruption)?);
        let timestamp_micros =
            u64::from_be_bytes(body[8..16].try_into().map_err(|_| FrameError::Corruption)?);
        let payload_type = body[16];
        let payload = body[17..crc_offset].to_vec();

        Ok(Some(Frame {
            frame_length,
            commit_seq,
            timestamp_micros,
            payload_type,
            payload,
            crc32c: stored_crc,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{FrameError, FrameReader, FrameWriter};
    use std::io::Cursor;

    #[test]
    fn frame_happy_path_reads_what_was_written() {
        let mut writer = FrameWriter::new(Vec::<u8>::new());
        for i in 1..=1000 {
            writer
                .append(i, 1000 + i, 0x01, format!("payload-{i}").as_bytes())
                .expect("append");
        }
        let bytes = writer.into_inner();

        let mut reader = FrameReader::new(Cursor::new(bytes));
        for i in 1..=1000 {
            let frame = reader.next_frame().expect("next").expect("frame");
            assert_eq!(frame.commit_seq, i);
            assert_eq!(frame.timestamp_micros, 1000 + i);
            assert_eq!(frame.payload_type, 0x01);
            assert_eq!(frame.payload, format!("payload-{i}").as_bytes());
        }
        assert!(reader.next_frame().expect("final next").is_none());
    }

    #[test]
    fn frame_corruption_detected() {
        let mut writer = FrameWriter::new(Vec::<u8>::new());
        for i in 1..=10 {
            writer
                .append(i, i, 0x01, format!("payload-{i}").as_bytes())
                .expect("append");
        }
        let mut bytes = writer.into_inner();
        let mut offset = 0usize;
        for frame_idx in 1..=10 {
            let len =
                u32::from_be_bytes(bytes[offset..offset + 4].try_into().expect("len")) as usize;
            if frame_idx == 5 {
                let payload_start = offset + 4 + 8 + 8 + 1;
                bytes[payload_start] ^= 0xFF;
                break;
            }
            offset += 4 + len;
        }

        let mut reader = FrameReader::new(Cursor::new(bytes));
        for _ in 0..4 {
            let _ = reader
                .next_frame()
                .expect("valid frame")
                .expect("some frame");
        }
        assert_eq!(
            reader.next_frame().expect_err("must be corruption"),
            FrameError::Corruption
        );
    }

    #[test]
    fn frame_truncation_detected() {
        let mut writer = FrameWriter::new(Vec::<u8>::new());
        for i in 1..=10 {
            writer.append(i, i, 0x01, &[1, 2, 3, 4, 5]).expect("append");
        }
        let bytes = writer.into_inner();

        for cut in 1..20 {
            let truncated = &bytes[..bytes.len() - cut];
            let mut reader = FrameReader::new(Cursor::new(truncated));
            loop {
                match reader.next_frame() {
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(FrameError::Truncation) => break,
                    Err(e) => panic!("unexpected error: {e:?}"),
                }
            }
        }
    }

    #[test]
    fn empty_file_returns_none() {
        let mut reader = FrameReader::new(Cursor::new(Vec::<u8>::new()));
        assert!(reader.next_frame().expect("next").is_none());
    }

    #[test]
    fn partial_length_returns_truncation() {
        let mut reader = FrameReader::new(Cursor::new(vec![0x00, 0x00]));
        assert_eq!(
            reader.next_frame().expect_err("truncation"),
            FrameError::Truncation
        );
    }

    #[test]
    fn oversized_frame_length_is_rejected_without_allocation() {
        let mut bytes = Vec::new();
        let oversized = (super::MAX_FRAME_BODY_BYTES as u32).saturating_add(1);
        bytes.extend_from_slice(&oversized.to_be_bytes());
        let mut reader = FrameReader::new(Cursor::new(bytes));
        assert_eq!(
            reader.next_frame().expect_err("oversized frame"),
            FrameError::Corruption
        );
    }
}
