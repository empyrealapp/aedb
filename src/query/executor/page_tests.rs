use super::execute_query_with_options;
use crate::query::error::QueryError;
use crate::query::plan::{Aggregate, Expr, Order, Query, QueryOptions, col, lit};
use crate::{Row, Value};

#[test]
fn page_window_rejects_offset_overflow() {
    let (keyspace, catalog) = super::tests::setup();
    let snapshot = keyspace.snapshot();
    let err = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"])
            .from("users")
            .order_by("id", Order::Asc)
            .limit(1)
            .offset(usize::MAX),
        &QueryOptions::default(),
        9,
        10,
        None,
    )
    .expect_err("offset overflow should be rejected");

    assert!(matches!(
        err,
        QueryError::ScanBoundExceeded {
            estimated_rows: u64::MAX,
            max_scan_rows: 10
        }
    ));
}

#[test]
fn cursor_pagination_returns_stable_pages() {
    let (keyspace, catalog) = super::tests::setup();
    let snapshot = keyspace.snapshot();
    let mut options = QueryOptions::default();
    let mut all = Vec::new();
    loop {
        let page = execute_query_with_options(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["*"])
                .from("users")
                .order_by("id", Order::Asc)
                .limit(10),
            &options,
            42,
            10_000,
            None,
        )
        .expect("page");
        all.extend(page.rows.clone());
        if let Some(cursor) = page.cursor {
            options.cursor = Some(cursor);
        } else {
            break;
        }
    }
    assert_eq!(all.len(), 100);
    for (i, row) in all.iter().enumerate().take(100) {
        assert_eq!(row.values[0], Value::Integer(i as i64));
    }
}

#[test]
fn invalid_cursor_is_rejected() {
    let (keyspace, catalog) = super::tests::setup();
    let snapshot = keyspace.snapshot();
    let options = QueryOptions {
        cursor: Some("xyz".into()),
        ..QueryOptions::default()
    };
    let err = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"])
            .from("users")
            .order_by("id", Order::Asc),
        &options,
        42,
        10_000,
        None,
    )
    .expect_err("invalid cursor should fail");
    assert!(matches!(err, QueryError::InvalidQuery { .. }));
}

#[test]
fn uppercase_hex_cursor_is_accepted() {
    let (keyspace, catalog) = super::tests::setup();
    let snapshot = keyspace.snapshot();
    let first = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"])
            .from("users")
            .order_by("id", Order::Asc)
            .limit(10),
        &QueryOptions::default(),
        42,
        10_000,
        None,
    )
    .expect("first page");
    let cursor = first
        .cursor
        .expect("first page should include cursor")
        .to_ascii_uppercase();
    let options = QueryOptions {
        cursor: Some(cursor),
        ..QueryOptions::default()
    };
    let second = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"])
            .from("users")
            .order_by("id", Order::Asc)
            .limit(10),
        &options,
        42,
        10_000,
        None,
    )
    .expect("uppercase cursor should decode");
    assert_eq!(second.rows.len(), 10);
}

#[test]
fn limit_offset_scan_stops_after_requested_window() {
    let (keyspace, catalog) = super::tests::setup();
    let snapshot = keyspace.snapshot();
    let result = super::tests::execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["id"]).from("users").limit(5).offset(10),
    )
    .expect("offset page");

    let ids = result
        .rows
        .iter()
        .map(|row| row.values[0].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        ids,
        vec![
            Value::Integer(10),
            Value::Integer(11),
            Value::Integer(12),
            Value::Integer(13),
            Value::Integer(14),
        ]
    );
    assert!(result.truncated);
    assert!(result.cursor.is_none());
    assert_eq!(result.rows_examined, 16);
}

#[test]
fn limit_offset_uses_index_candidate_window() {
    let (keyspace, catalog) = super::tests::setup();
    let snapshot = keyspace.snapshot();
    let result = super::tests::execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["id"])
            .from("users")
            .where_(col("age").eq(lit(20)))
            .limit(1)
            .offset(1),
    )
    .expect("indexed offset page");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], Value::Integer(52));
    // The exact-index OFFSET fast path drops the skipped primary key before
    // materializing, so only the single returned row is paged in (not offset+1).
    assert_eq!(result.rows_examined, 1);
}

#[test]
fn deep_offset_on_exact_index_does_not_materialize_skipped_rows() {
    // `age >= 18` matches all 100 rows via the ordered `by_age` index and is an
    // exact predicate with no ORDER BY, so the deep-OFFSET fast path engages:
    // a large offset must not materialize the skipped rows. `rows_examined`
    // should reflect only the returned page (plus the lookahead row), not the
    // offset.
    let (keyspace, catalog) = super::tests::setup();
    let snapshot = keyspace.snapshot();
    let result = super::tests::execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["id"])
            .from("users")
            .where_(col("age").gte(lit(18)))
            .limit(5)
            .offset(90),
    )
    .expect("deep offset page");

    assert_eq!(result.rows.len(), 5);
    // 5 returned + 1 lookahead row; crucially far below the 95 a scan-and-skip
    // implementation would have paged in.
    assert!(
        result.rows_examined <= 6,
        "expected deep offset to skip materialization, examined {}",
        result.rows_examined
    );
}

#[test]
fn offset_requires_limit_and_cannot_mix_with_cursor() {
    let (keyspace, catalog) = super::tests::setup();
    let snapshot = keyspace.snapshot();
    let err = super::tests::execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["id"]).from("users").offset(1),
    )
    .expect_err("offset without limit");
    assert!(matches!(err, QueryError::InvalidQuery { .. }));

    let first = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["id"])
            .from("users")
            .order_by("id", Order::Asc)
            .limit(5),
        &QueryOptions::default(),
        42,
        10_000,
        None,
    )
    .expect("first page");
    let options = QueryOptions {
        cursor: first.cursor,
        ..QueryOptions::default()
    };
    let err = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["id"])
            .from("users")
            .order_by("id", Order::Asc)
            .limit(5)
            .offset(5),
        &options,
        42,
        10_000,
        None,
    )
    .expect_err("cursor with offset");
    assert!(matches!(err, QueryError::InvalidQuery { .. }));
}

#[test]
fn cursor_snapshot_mismatch_is_rejected() {
    let (keyspace, catalog) = super::tests::setup();
    let snapshot = keyspace.snapshot();
    let first = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"])
            .from("users")
            .order_by("id", Order::Asc)
            .limit(10),
        &QueryOptions::default(),
        42,
        10_000,
        None,
    )
    .expect("first page");
    let options = QueryOptions {
        cursor: first.cursor,
        ..QueryOptions::default()
    };
    let err = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"])
            .from("users")
            .order_by("id", Order::Asc)
            .limit(10),
        &options,
        43,
        10_000,
        None,
    )
    .expect_err("snapshot mismatch");
    assert!(matches!(err, QueryError::InvalidQuery { .. }));
}

#[test]
fn descending_cursor_pagination_is_stable() {
    let (keyspace, catalog) = super::tests::setup();
    let snapshot = keyspace.snapshot();
    let mut options = QueryOptions::default();
    let mut all = Vec::new();
    loop {
        let page = execute_query_with_options(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["*"])
                .from("users")
                .order_by("id", Order::Desc)
                .limit(11),
            &options,
            55,
            10_000,
            None,
        )
        .expect("page");
        all.extend(page.rows.clone());
        if let Some(cursor) = page.cursor {
            options.cursor = Some(cursor);
        } else {
            break;
        }
    }
    assert_eq!(all.len(), 100);
    for window in all.windows(2) {
        assert!(window[0].values[0] > window[1].values[0]);
    }
}

#[test]
fn top_k_sort_matches_full_multi_column_ordering() {
    let (keyspace, catalog) = super::tests::setup();
    let snapshot = keyspace.snapshot();

    let limited = super::tests::execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["id", "age"])
            .from("users")
            .order_by("age", Order::Desc)
            .order_by("id", Order::Asc)
            .limit(7),
    )
    .expect("limited ordered query");
    let full = super::tests::execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["id", "age"])
            .from("users")
            .order_by("age", Order::Desc)
            .order_by("id", Order::Asc),
    )
    .expect("full ordered query");

    let expected: Vec<Row> = full.rows.into_iter().take(7).collect();
    assert_eq!(limited.rows, expected);
}

#[test]
fn split_recommended_set_on_full_consumption() {
    let (keyspace, catalog) = super::tests::setup();
    let snapshot = keyspace.snapshot();
    let result = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"])
            .from("users")
            .aggregate(Aggregate::Count),
        &QueryOptions {
            allow_full_scan: true,
            ..QueryOptions::default()
        },
        0,
        100,
        None,
    )
    .expect("aggregate count");
    assert_eq!(result.rows_examined, 100);
    assert!(result.split_recommended);
}

#[test]
fn split_not_recommended_when_consumption_below_threshold() {
    let (keyspace, catalog) = super::tests::setup();
    let snapshot = keyspace.snapshot();
    let result = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"])
            .from("users")
            .where_(Expr::Eq("name".into(), Value::Text("u7".into())))
            .limit(50),
        &QueryOptions {
            allow_full_scan: false,
            ..QueryOptions::default()
        },
        0,
        10_000,
        None,
    )
    .expect("indexed lookup");
    assert_eq!(result.rows.len(), 1);
    assert!(!result.split_recommended);
}
