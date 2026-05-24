use super::tests::setup;
use super::{
    CapturingQueryExecutionRequest, ReadSetCollector, execute_query_with_options_capturing,
};
use crate::catalog::types::Value;
use crate::commit::tx::{ReadKey, ReadRange};
use crate::query::plan::{Expr, Query, QueryOptions};

#[test]
fn read_set_captures_primary_key_point_lookup() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();

    let mut collector = ReadSetCollector::new();
    let result = execute_query_with_options_capturing(CapturingQueryExecutionRequest {
        snapshot: &snapshot,
        catalog: &catalog,
        project_id: "A",
        scope_id: "app",
        query: Query::select(&["id", "name"])
            .from("users")
            .where_(Expr::Eq("id".into(), Value::Integer(7))),
        options: &QueryOptions {
            allow_full_scan: true,
            ..QueryOptions::default()
        },
        snapshot_seq: 0,
        max_scan_rows: usize::MAX,
        read_set: Some(&mut collector),
    })
    .expect("query");
    assert_eq!(result.rows.len(), 1);

    let read_set = collector.into_inner();
    assert_eq!(read_set.points.len(), 1, "expected one point entry");
    assert!(read_set.ranges.is_empty(), "no ranges for PK lookup");
    let entry = &read_set.points[0];
    let ReadKey::TableRow {
        project_id,
        scope_id,
        table_name,
        primary_key,
    } = &entry.key
    else {
        panic!("expected TableRow read key");
    };
    assert_eq!(project_id, "A");
    assert_eq!(scope_id, "app");
    assert_eq!(table_name, "users");
    assert_eq!(primary_key, &vec![Value::Integer(7)]);
}

#[test]
fn read_set_captures_indexed_range_as_touched_pks() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();

    let mut collector = ReadSetCollector::new();
    let result = execute_query_with_options_capturing(CapturingQueryExecutionRequest {
        snapshot: &snapshot,
        catalog: &catalog,
        project_id: "A",
        scope_id: "app",
        query: Query::select(&["id", "age"])
            .from("users")
            .where_(Expr::Gte("age".into(), Value::Integer(60)))
            .limit(10),
        options: &QueryOptions {
            allow_full_scan: true,
            ..QueryOptions::default()
        },
        snapshot_seq: 0,
        max_scan_rows: usize::MAX,
        read_set: Some(&mut collector),
    })
    .expect("query");
    assert!(!result.rows.is_empty());

    let read_set = collector.into_inner();
    assert!(
        !read_set.points.is_empty(),
        "indexed range scan should capture touched pks as points"
    );
    assert!(read_set.ranges.is_empty(), "no coarse range expected");
}

#[test]
fn read_set_captures_full_table_scan_as_range() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();

    let mut collector = ReadSetCollector::new();
    let _ = execute_query_with_options_capturing(CapturingQueryExecutionRequest {
        snapshot: &snapshot,
        catalog: &catalog,
        project_id: "A",
        scope_id: "app",
        query: Query::select(&["*"]).from("users"),
        options: &QueryOptions {
            allow_full_scan: true,
            ..QueryOptions::default()
        },
        snapshot_seq: 0,
        max_scan_rows: usize::MAX,
        read_set: Some(&mut collector),
    })
    .expect("query");

    let read_set = collector.into_inner();
    assert_eq!(
        read_set.ranges.len(),
        1,
        "full scan should record one coarse range"
    );
    let ReadRange::TableRange { table_name, .. } = &read_set.ranges[0].range else {
        panic!("expected TableRange");
    };
    assert_eq!(table_name, "users");
}
