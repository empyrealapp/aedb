use aedb::wal::frame::{FrameError, FrameReader};
use proptest::prelude::*;
use std::io::Cursor;

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64,
        .. ProptestConfig::default()
    })]

    #[test]
    fn prop_random_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..4096)) {
        // Exercise prefix truncations to emulate torn writes and random tails.
        let mut lengths = vec![bytes.len()];
        if !bytes.is_empty() {
            lengths.push(bytes.len() / 2);
            lengths.push(bytes.len().saturating_sub(1));
        }
        lengths.sort_unstable();
        lengths.dedup();

        for len in lengths {
            let mut candidate = bytes.clone();
            candidate.truncate(len);
            let mut reader = FrameReader::new(Cursor::new(candidate));
            loop {
                match reader.next_frame() {
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(FrameError::Truncation) | Err(FrameError::Corruption) | Err(FrameError::Io(_)) => break,
                }
            }
        }
    }
}
