use super::cursor::{CursorToken, decode_cursor, encode_cursor};
use super::pagination::{compute_remaining_limit_after_page, compute_split_recommended};
use crate::catalog::types::Value;

#[test]
fn split_recommended_threshold_at_75_percent() {
    // Helper: edges around the 75% boundary for an effective budget of 100.
    assert!(!compute_split_recommended(74, 100));
    assert!(compute_split_recommended(75, 100));
    assert!(compute_split_recommended(76, 100));
    assert!(compute_split_recommended(100, 100));

    // Budget of 4: exactly 3 rows examined is the smallest count that
    // satisfies the >= 75% threshold (3/4 = 75%).
    assert!(!compute_split_recommended(2, 4));
    assert!(compute_split_recommended(3, 4));

    // Zero-budget guard: never trigger split.
    assert!(!compute_split_recommended(100, 0));
}

#[test]
fn cursor_remaining_limit_is_threaded_across_pages() {
    let token = CursorToken {
        snapshot_seq: 1,
        last_sort_key: vec![],
        last_pk: vec![Value::Integer(5)],
        page_size: 3,
        remaining_limit: Some(10),
    };
    let encoded = encode_cursor(&token, None).expect("encode");
    let decoded = decode_cursor(&encoded, None).expect("decode");
    assert_eq!(decoded.remaining_limit, Some(10));

    assert_eq!(
        compute_remaining_limit_after_page(Some(10), None, 3),
        Some(7)
    );
    assert_eq!(
        compute_remaining_limit_after_page(Some(10), Some(7), 3),
        Some(4)
    );
    assert_eq!(compute_remaining_limit_after_page(None, None, 5), None);
    assert_eq!(
        compute_remaining_limit_after_page(Some(2), None, 5),
        Some(0)
    );
}
