use super::execute_query_with_options;
use super::join::enforce_materialization_budget;
use super::tests::{execute_query, setup};
use crate::catalog::schema::ColumnDef;
use crate::catalog::types::{ColumnType, Row, Value};
use crate::query::error::QueryError;
use crate::query::plan::{
    Aggregate, CompareOp, Expr, JoinCond, Order, Query, QueryOptions, ScalarExpr,
};

#[test]
fn materialization_budget_rejects_oversized_row_set() {
    let rows: Vec<Row> = (0..1000)
        .map(|i| Row {
            values: vec![Value::Integer(i), Value::Text("x".repeat(256).into())],
        })
        .collect();

    // A generous budget admits the set.
    assert!(enforce_materialization_budget(&rows, u64::MAX).is_ok());

    // A tiny budget rejects it with a structured, machine-readable error.
    let err = enforce_materialization_budget(&rows, 64).unwrap_err();
    match err {
        QueryError::MaterializationBudgetExceeded {
            estimated_bytes,
            max_bytes,
        } => {
            assert_eq!(max_bytes, 64);
            assert!(estimated_bytes > 64);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn materialization_budget_admits_empty_and_small_sets() {
    assert!(enforce_materialization_budget(&[], 0).is_ok());
    let small = vec![Row {
        values: vec![Value::Integer(1)],
    }];
    assert!(enforce_materialization_budget(&small, 1024).is_ok());
}

fn add_profiles_table(catalog: &mut crate::catalog::Catalog, primary_key: Vec<String>) {
    catalog
        .create_table(
            "A",
            "app",
            "profiles",
            vec![
                ColumnDef {
                    name: "user_id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "country".into(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
            ],
            primary_key,
        )
        .expect("profiles table");
}

fn seed_profiles(
    keyspace: &mut crate::storage::keyspace::Keyspace,
    ids: impl Iterator<Item = i64>,
    country_for_id: impl Fn(i64) -> &'static str,
) {
    for id in ids {
        keyspace.upsert_row(
            "A",
            "app",
            "profiles",
            vec![Value::Integer(id)],
            Row::from_values(vec![
                Value::Integer(id),
                Value::Text(country_for_id(id).into()),
            ]),
            1,
        );
    }
}

#[test]
fn inner_join_returns_matching_rows() {
    let (mut keyspace, mut catalog) = setup();
    add_profiles_table(&mut catalog, vec!["user_id".into()]);
    seed_profiles(&mut keyspace, 0..50, |_| "US");

    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id", "p.country"])
            .from("users")
            .alias("u")
            .inner_join("profiles", "u.id", "user_id")
            .with_last_join_alias("p")
            .limit(100),
    )
    .expect("join query");
    assert_eq!(result.rows.len(), 50);
}

#[test]
fn join_on_right_primary_key_uses_bounded_probe_path() {
    let (mut keyspace, mut catalog) = setup();
    add_profiles_table(&mut catalog, vec!["user_id".into()]);
    seed_profiles(&mut keyspace, 0..50, |_| "US");

    let snapshot = keyspace.snapshot();
    let result = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id", "p.country"])
            .from("users")
            .alias("u")
            .inner_join("profiles", "u.id", "user_id")
            .with_last_join_alias("p")
            .limit(100),
        &QueryOptions::default(),
        1,
        100,
        None,
    )
    .expect("pk join should respect bounded probe path");
    assert_eq!(result.rows.len(), 50);
    assert!(result.rows_examined <= 50);
}

#[test]
fn join_aggregate_count_and_having_are_applied() {
    let (mut keyspace, mut catalog) = setup();
    add_profiles_table(&mut catalog, vec!["user_id".into()]);
    seed_profiles(
        &mut keyspace,
        0..50,
        |id| {
            if id % 2 == 0 { "US" } else { "CA" }
        },
    );

    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["p.country", "count_star"])
            .from("users")
            .alias("u")
            .inner_join("profiles", "u.id", "user_id")
            .with_last_join_alias("p")
            .group_by(&["p.country"])
            .aggregate(Aggregate::Count)
            .having(Expr::Gt("count_star".into(), Value::Integer(20)))
            .order_by("count_star", Order::Desc)
            .limit(10),
    )
    .expect("join aggregate query");

    assert_eq!(result.rows.len(), 2);
    for row in result.rows {
        assert!(matches!(row.values[1], Value::Integer(25)));
    }
}

#[test]
fn left_join_supports_global_table_reference() {
    let (mut keyspace, mut catalog) = setup();
    catalog.create_project("_global").expect("global project");
    catalog
        .create_table(
            "_global",
            "app",
            "users",
            vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "name".into(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
            ],
            vec!["id".into()],
        )
        .expect("global users");
    for i in 0..20 {
        keyspace.upsert_row(
            "_global",
            "app",
            "users",
            vec![Value::Integer(i)],
            Row::from_values(vec![Value::Integer(i), Value::Text(format!("g{i}").into())]),
            1,
        );
    }
    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id", "g.name"])
            .from("users")
            .alias("u")
            .left_join("_global.users", "u.id", "id")
            .with_last_join_alias("g")
            .limit(5),
    )
    .expect("left join");
    assert_eq!(result.rows.len(), 5);
}

#[test]
fn join_query_supports_cursor_pagination() {
    let (mut keyspace, mut catalog) = setup();
    add_profiles_table(&mut catalog, vec!["user_id".into()]);
    seed_profiles(&mut keyspace, 0..50, |_| "US");
    let snapshot = keyspace.snapshot();

    let first = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"])
            .from("users")
            .alias("u")
            .inner_join("profiles", "u.id", "user_id")
            .with_last_join_alias("p")
            .order_by("u.id", Order::Asc)
            .limit(5),
        &QueryOptions::default(),
        7,
        10_000,
        None,
    )
    .expect("first page");
    assert_eq!(first.rows.len(), 5);
    assert!(first.cursor.is_some());

    let options = QueryOptions {
        cursor: first.cursor,
        ..QueryOptions::default()
    };
    let second = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id", "p.country"])
            .from("users")
            .alias("u")
            .inner_join("profiles", "u.id", "user_id")
            .with_last_join_alias("p")
            .limit(5),
        &options,
        7,
        10_000,
        None,
    )
    .expect("join cursor page");
    assert_eq!(second.rows.len(), 5);
    assert!(second.cursor.is_some());

    let first_ids: Vec<Value> = first.rows.iter().map(|r| r.values[0].clone()).collect();
    let second_ids: Vec<Value> = second.rows.iter().map(|r| r.values[0].clone()).collect();
    assert!(
        first_ids
            .iter()
            .all(|id| !second_ids.iter().any(|other| other == id))
    );
}

#[test]
fn right_join_includes_unmatched_right_rows_with_nulls() {
    let (mut keyspace, mut catalog) = setup();
    add_profiles_table(&mut catalog, vec!["user_id".into()]);
    seed_profiles(&mut keyspace, 90..110, |_| "US");

    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id", "p.user_id"])
            .from("users")
            .alias("u")
            .right_join("profiles", "u.id", "user_id")
            .with_last_join_alias("p")
            .order_by("p.user_id", Order::Asc)
            .limit(200),
    )
    .expect("right join");

    assert_eq!(result.rows.len(), 20);
    let unmatched = result
        .rows
        .iter()
        .filter(|r| matches!(r.values[0], Value::Null))
        .count();
    assert_eq!(unmatched, 10);
}

#[test]
fn pushdown_filters_base_and_right_on_pk_probe_path() {
    // profiles PK is user_id, so the inner join takes the bounded PK-probe path.
    // `u.age >= 60` pushes into the base scan; `p.country = 'US'` pushes into the
    // probe. Result must match a plain post-join filter.
    let (mut keyspace, mut catalog) = setup();
    add_profiles_table(&mut catalog, vec!["user_id".into()]);
    seed_profiles(
        &mut keyspace,
        0..100,
        |id| if id < 50 { "US" } else { "CA" },
    );

    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id", "p.country"])
            .from("users")
            .alias("u")
            .inner_join("profiles", "u.id", "user_id")
            .with_last_join_alias("p")
            .where_(
                Expr::Gte("u.age".into(), Value::Integer(60))
                    .and(Expr::Eq("p.country".into(), Value::Text("US".into()))),
            )
            .limit(200),
    )
    .expect("inner join with pushdown");

    // age = 18 + (id % 50) >= 60 => id % 50 in 42..=49; AND country US => id < 50.
    // => ids 42..=49 => 8 rows.
    assert_eq!(result.rows.len(), 8);
    for row in &result.rows {
        assert!(matches!(&row.values[1], Value::Text(c) if c.as_str() == "US"));
    }
}

#[test]
fn indexed_base_lookup_avoids_full_base_scan() {
    // The base `users` table has a `by_age` index and 100 rows. With
    // `max_scan_rows = 10` and full scans disallowed, a join that full-scanned
    // the base would be rejected with ScanBoundExceeded. The indexed base lookup
    // over `u.age = 20` pages in only the 2 matching rows, so the join succeeds.
    let (mut keyspace, mut catalog) = setup();
    add_profiles_table(&mut catalog, vec!["user_id".into()]);
    // age = 18 + (id % 50) == 20 => ids 2 and 52.
    seed_profiles(&mut keyspace, vec![2i64, 52].into_iter(), |_| "US");

    let snapshot = keyspace.snapshot();
    let result = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id", "p.country"])
            .from("users")
            .alias("u")
            .inner_join("profiles", "u.id", "user_id")
            .with_last_join_alias("p")
            .where_(Expr::Eq("u.age".into(), Value::Integer(20)))
            .limit(100),
        &QueryOptions {
            allow_full_scan: false,
            ..QueryOptions::default()
        },
        1,
        10,
        None,
    )
    .expect("indexed base lookup should avoid the full base scan guard");

    assert_eq!(result.rows.len(), 2);
}

#[test]
fn indexed_base_lookup_applies_residual_filter() {
    // `u.age = 18 AND u.email IS NOT NULL`: age is indexed (candidates ids 0, 50)
    // but the IS NOT NULL conjunct is residual. id 0 has a NULL email and must be
    // filtered out of the base, leaving only id 50 to join.
    let (mut keyspace, mut catalog) = setup();
    add_profiles_table(&mut catalog, vec!["user_id".into()]);
    seed_profiles(&mut keyspace, vec![0i64, 50].into_iter(), |_| "US");

    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id", "p.country"])
            .from("users")
            .alias("u")
            .inner_join("profiles", "u.id", "user_id")
            .with_last_join_alias("p")
            .where_(
                Expr::Eq("u.age".into(), Value::Integer(18)).and(Expr::IsNotNull("u.email".into())),
            )
            .limit(100),
    )
    .expect("indexed base lookup with residual filter");

    assert_eq!(result.rows.len(), 1);
    assert!(matches!(result.rows[0].values[0], Value::Integer(50)));
}

#[test]
fn pushdown_filters_right_on_hash_join_path() {
    // Composite PK forces the non-PK hash-join path; `p.country = 'US'` pushes
    // into the materialized right side, `u.id < 10` into the base scan.
    let (mut keyspace, mut catalog) = setup();
    add_profiles_table(&mut catalog, vec!["user_id".into(), "country".into()]);
    for id in 0..100i64 {
        let country = if id % 2 == 0 { "US" } else { "CA" };
        keyspace.upsert_row(
            "A",
            "app",
            "profiles",
            vec![Value::Integer(id), Value::Text(country.into())],
            Row::from_values(vec![Value::Integer(id), Value::Text(country.into())]),
            1,
        );
    }

    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id", "p.country"])
            .from("users")
            .alias("u")
            .inner_join("profiles", "u.id", "user_id")
            .with_last_join_alias("p")
            .where_(
                Expr::Lt("u.id".into(), Value::Integer(10))
                    .and(Expr::Eq("p.country".into(), Value::Text("US".into()))),
            )
            .limit(200),
    )
    .expect("hash join with pushdown");

    // id < 10 AND country US (even ids) => {0,2,4,6,8} => 5 rows.
    assert_eq!(result.rows.len(), 5);
}

#[test]
fn left_join_null_rejecting_right_filter_preserves_semantics() {
    // A null-rejecting predicate on the right table of a LEFT JOIN behaves like
    // an inner filter: only matched rows survive, even though pushdown drops the
    // right rows early. profiles cover ids 0..50; all are US.
    let (mut keyspace, mut catalog) = setup();
    add_profiles_table(&mut catalog, vec!["user_id".into()]);
    seed_profiles(&mut keyspace, 0..50, |_| "US");

    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id", "p.country"])
            .from("users")
            .alias("u")
            .left_join("profiles", "u.id", "user_id")
            .with_last_join_alias("p")
            .where_(Expr::Eq("p.country".into(), Value::Text("US".into())))
            .limit(200),
    )
    .expect("left join with null-rejecting right filter");

    assert_eq!(result.rows.len(), 50);
}

#[test]
fn left_join_is_null_right_filter_is_not_pushed_down() {
    // `p.user_id IS NULL` is NOT null-rejecting and must not be pushed into the
    // probe — otherwise every left row would be null-extended and wrongly kept.
    // Correct semantics: surface the 50 unmatched left rows (ids 50..100).
    let (mut keyspace, mut catalog) = setup();
    add_profiles_table(&mut catalog, vec!["user_id".into()]);
    seed_profiles(&mut keyspace, 0..50, |_| "US");

    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id", "p.user_id"])
            .from("users")
            .alias("u")
            .left_join("profiles", "u.id", "user_id")
            .with_last_join_alias("p")
            .where_(Expr::IsNull("p.user_id".into()))
            .limit(200),
    )
    .expect("left join anti-join");

    assert_eq!(result.rows.len(), 50);
    for row in &result.rows {
        assert!(matches!(row.values[0], Value::Integer(id) if id >= 50));
        assert!(matches!(row.values[1], Value::Null));
    }
}

#[test]
fn cross_join_applies_pushdown_to_both_sides() {
    let (mut keyspace, mut catalog) = setup();
    add_profiles_table(&mut catalog, vec!["user_id".into()]);
    seed_profiles(
        &mut keyspace,
        0..3,
        |id| if id % 2 == 0 { "US" } else { "CA" },
    );

    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id", "p.user_id"])
            .from("users")
            .alias("u")
            .cross_join("profiles")
            .with_last_join_alias("p")
            .where_(
                Expr::Eq("u.id".into(), Value::Integer(1))
                    .and(Expr::Eq("p.country".into(), Value::Text("US".into()))),
            )
            .limit(200),
    )
    .expect("cross join with pushdown");

    // 1 base row (u.id = 1) x 2 US profiles (ids 0, 2) = 2 rows.
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn cross_join_cardinality_and_limit_are_correct() {
    let (mut keyspace, mut catalog) = setup();
    add_profiles_table(&mut catalog, vec!["user_id".into()]);
    seed_profiles(&mut keyspace, 0..5, |_| "US");

    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id", "p.user_id"])
            .from("users")
            .alias("u")
            .cross_join("profiles")
            .with_last_join_alias("p")
            .limit(123),
    )
    .expect("cross join");
    assert_eq!(result.rows.len(), 123);
}

#[test]
fn computed_projection_over_join_appends_column() {
    let (mut keyspace, mut catalog) = setup();
    add_profiles_table(&mut catalog, vec!["user_id".into()]);
    seed_profiles(&mut keyspace, 0..50, |_| "US");

    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id"])
            .from("users")
            .alias("u")
            .inner_join("profiles", "u.id", "user_id")
            .with_last_join_alias("p")
            .where_(Expr::Eq("u.id".into(), Value::Integer(4)))
            .compute(
                "double_age",
                ScalarExpr::col("u.age").mul(ScalarExpr::lit(2i64)),
            ),
    )
    .expect("computed over join");

    assert_eq!(result.rows.len(), 1);
    // user 4: age = 18 + 4 = 22, doubled = 44. Output [u.id, double_age].
    assert_eq!(result.rows[0].values.len(), 2);
    assert!(matches!(result.rows[0].values[1], Value::Integer(44)));
}

#[test]
fn distinct_dedupes_join_output() {
    // 50 users join 50 US profiles; SELECT DISTINCT p.country yields one row.
    let (mut keyspace, mut catalog) = setup();
    add_profiles_table(&mut catalog, vec!["user_id".into()]);
    seed_profiles(
        &mut keyspace,
        0..50,
        |id| if id % 2 == 0 { "US" } else { "CA" },
    );

    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["p.country"])
            .from("users")
            .alias("u")
            .inner_join("profiles", "u.id", "user_id")
            .with_last_join_alias("p")
            .order_by("p.country", Order::Asc)
            .distinct()
            .limit(100),
    )
    .expect("distinct join");

    // Only "CA" and "US" remain after dedup.
    assert_eq!(result.rows.len(), 2);
    assert!(matches!(&result.rows[0].values[0], Value::Text(c) if c.as_str() == "CA"));
    assert!(matches!(&result.rows[1].values[0], Value::Text(c) if c.as_str() == "US"));
}

/// A `memberships` table keyed by `user_id` with an `age` column, used to
/// exercise composite and non-equi join conditions against `users`.
fn add_memberships_table(catalog: &mut crate::catalog::Catalog) {
    catalog
        .create_table(
            "A",
            "app",
            "memberships",
            vec![
                ColumnDef {
                    name: "user_id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "age".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "tier".into(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
            ],
            vec!["user_id".into()],
        )
        .expect("memberships table");
}

fn seed_membership(
    keyspace: &mut crate::storage::keyspace::Keyspace,
    user_id: i64,
    age: i64,
    tier: &str,
) {
    keyspace.upsert_row(
        "A",
        "app",
        "memberships",
        vec![Value::Integer(user_id)],
        Row::from_values(vec![
            Value::Integer(user_id),
            Value::Integer(age),
            Value::Text(tier.into()),
        ]),
        1,
    );
}

#[test]
fn cross_type_integer_join_matches_via_coercion() {
    // `users.id` is Integer; `events.user_ref` is U64. The hash join (user_ref is
    // not the events PK) must match across the integer types, matching how WHERE
    // filters coerce U8/U64/Integer.
    let (mut keyspace, mut catalog) = setup();
    catalog
        .create_table(
            "A",
            "app",
            "events",
            vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "user_ref".into(),
                    col_type: ColumnType::U64,
                    nullable: false,
                },
            ],
            vec!["id".into()],
        )
        .expect("events table");
    for (id, user_ref) in [(1000i64, 1u64), (1001, 2), (1002, 3)] {
        keyspace.upsert_row(
            "A",
            "app",
            "events",
            vec![Value::Integer(id)],
            Row::from_values(vec![Value::Integer(id), Value::U64(user_ref)]),
            1,
        );
    }

    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id", "e.id"])
            .from("users")
            .alias("u")
            .inner_join("events", "u.id", "e.user_ref")
            .with_last_join_alias("e")
            .limit(100),
    )
    .expect("cross-type join");

    // users 1, 2, 3 each match one event despite Integer vs U64 key types.
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn transitivity_propagates_constant_to_base_index_lookup() {
    // WHERE is on the joined table (p.user_id = 5), but the join key u.id =
    // p.user_id lets transitivity derive u.id = 5 and drive a base PK lookup.
    // With max_scan_rows = 10, a full base scan (100 users) would be rejected, so
    // a successful run proves the constant propagated to the base index.
    let (mut keyspace, mut catalog) = setup();
    add_profiles_table(&mut catalog, vec!["user_id".into()]);
    seed_profiles(&mut keyspace, 0..50, |_| "US");

    let snapshot = keyspace.snapshot();
    let result = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id", "p.country"])
            .from("users")
            .alias("u")
            .inner_join("profiles", "u.id", "user_id")
            .with_last_join_alias("p")
            .where_(Expr::Eq("p.user_id".into(), Value::Integer(5)))
            .limit(100),
        &QueryOptions {
            allow_full_scan: false,
            ..QueryOptions::default()
        },
        1,
        10,
        None,
    )
    .expect("transitivity should drive a base PK lookup");

    assert_eq!(result.rows.len(), 1);
    assert!(matches!(result.rows[0].values[0], Value::Integer(5)));
}

#[test]
fn single_column_on_equi_uses_pk_probe_fast_path() {
    // `inner_join_on` with a single equality whose right side is the profiles PK
    // must route to the bounded PK-probe path. With max_scan_rows = 100, the
    // probe estimate (base rows = 100) passes, whereas the general hash path's
    // estimate (100 base x 50 right = 5000) would exceed it — so a successful
    // run proves the fast path engaged.
    let (mut keyspace, mut catalog) = setup();
    add_profiles_table(&mut catalog, vec!["user_id".into()]);
    seed_profiles(&mut keyspace, 0..50, |_| "US");

    let snapshot = keyspace.snapshot();
    let result = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id", "p.country"])
            .from("users")
            .alias("u")
            .inner_join_on("profiles", JoinCond::on("u.id", "p.user_id"))
            .with_last_join_alias("p")
            .limit(200),
        &QueryOptions {
            allow_full_scan: false,
            ..QueryOptions::default()
        },
        1,
        100,
        None,
    )
    .expect("single-column ON should use the bounded probe path");

    assert_eq!(result.rows.len(), 50);
}

#[test]
fn composite_key_inner_join_matches_on_both_columns() {
    // users.age = 18 + (id % 50). The composite join requires BOTH id and age to
    // match, so a membership row with a mismatched age does not join.
    let (mut keyspace, mut catalog) = setup();
    add_memberships_table(&mut catalog);
    seed_membership(&mut keyspace, 1, 19, "gold"); // age matches user 1 -> joins
    seed_membership(&mut keyspace, 2, 99, "silver"); // age != user 2 (20) -> drops
    seed_membership(&mut keyspace, 3, 21, "bronze"); // age matches user 3 -> joins

    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id", "m.tier"])
            .from("users")
            .alias("u")
            .inner_join_on(
                "memberships",
                JoinCond::on("u.id", "m.user_id").and(JoinCond::on("u.age", "m.age")),
            )
            .with_last_join_alias("m")
            .order_by("u.id", Order::Asc)
            .limit(100),
    )
    .expect("composite key join");

    assert_eq!(result.rows.len(), 2);
    assert!(matches!(result.rows[0].values[0], Value::Integer(1)));
    assert!(matches!(result.rows[1].values[0], Value::Integer(3)));
}

#[test]
fn non_equi_join_uses_nested_loop() {
    // A single membership row with age = 20; join keeps users whose age > 20.
    // age = 18 + (id % 50) > 20 => id % 50 in 3..=49 (47 values) x2 over 0..100.
    let (mut keyspace, mut catalog) = setup();
    add_memberships_table(&mut catalog);
    seed_membership(&mut keyspace, 0, 20, "threshold");

    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id"])
            .from("users")
            .alias("u")
            .inner_join_on(
                "memberships",
                JoinCond::compare("u.age", CompareOp::Gt, "m.age"),
            )
            .with_last_join_alias("m")
            .limit(1000),
    )
    .expect("non-equi join");

    assert_eq!(result.rows.len(), 94);
}

#[test]
fn left_join_on_null_extends_unmatched_left_rows() {
    let (mut keyspace, mut catalog) = setup();
    add_memberships_table(&mut catalog);
    seed_membership(&mut keyspace, 1, 19, "gold");
    seed_membership(&mut keyspace, 3, 21, "bronze");

    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id", "m.tier"])
            .from("users")
            .alias("u")
            .left_join_on(
                "memberships",
                JoinCond::on("u.id", "m.user_id").and(JoinCond::on("u.age", "m.age")),
            )
            .with_last_join_alias("m")
            .limit(1000),
    )
    .expect("left join on");

    // All 100 left rows preserved; only 2 carry a non-null tier.
    assert_eq!(result.rows.len(), 100);
    let matched = result
        .rows
        .iter()
        .filter(|r| !matches!(r.values[1], Value::Null))
        .count();
    assert_eq!(matched, 2);
}

#[test]
fn right_join_on_null_extends_unmatched_right_rows() {
    let (mut keyspace, mut catalog) = setup();
    add_memberships_table(&mut catalog);
    seed_membership(&mut keyspace, 1, 19, "gold"); // matches user 1
    seed_membership(&mut keyspace, 2, 99, "silver"); // age mismatch -> no user
    seed_membership(&mut keyspace, 3, 21, "bronze"); // matches user 3

    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id", "m.tier"])
            .from("users")
            .alias("u")
            .right_join_on(
                "memberships",
                JoinCond::on("u.id", "m.user_id").and(JoinCond::on("u.age", "m.age")),
            )
            .with_last_join_alias("m")
            .limit(1000),
    )
    .expect("right join on");

    // All 3 membership rows preserved; the mismatched one has a NULL left side.
    assert_eq!(result.rows.len(), 3);
    let null_left = result
        .rows
        .iter()
        .filter(|r| matches!(r.values[0], Value::Null))
        .count();
    assert_eq!(null_left, 1);
}

#[test]
fn transitivity_conflicting_constants_yields_empty_result() {
    // WHERE u.id = 5 AND p.user_id = 6 with join u.id = p.user_id is
    // unsatisfiable. Transitivity must not mis-derive; the original conjuncts
    // (base id=5, profiles user_id=6) still force an empty result.
    let (mut keyspace, mut catalog) = setup();
    add_profiles_table(&mut catalog, vec!["user_id".into()]);
    seed_profiles(&mut keyspace, 0..50, |_| "US");

    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id", "p.user_id"])
            .from("users")
            .alias("u")
            .inner_join("profiles", "u.id", "user_id")
            .with_last_join_alias("p")
            .where_(
                Expr::Eq("u.id".into(), Value::Integer(5))
                    .and(Expr::Eq("p.user_id".into(), Value::Integer(6))),
            )
            .limit(100),
    )
    .expect("conflicting transitive constants");
    assert_eq!(result.rows.len(), 0);
}

#[test]
fn cross_type_non_equi_join_uses_coerced_comparison() {
    // u.age (Integer) > e.threshold (U64): the nested-loop non-equi join must
    // compare across integer types like WHERE filters do.
    let (mut keyspace, mut catalog) = setup();
    catalog
        .create_table(
            "A",
            "app",
            "thresholds",
            vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "threshold".into(),
                    col_type: ColumnType::U64,
                    nullable: false,
                },
            ],
            vec!["id".into()],
        )
        .expect("thresholds table");
    keyspace.upsert_row(
        "A",
        "app",
        "thresholds",
        vec![Value::Integer(0)],
        Row::from_values(vec![Value::Integer(0), Value::U64(20)]),
        1,
    );

    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id"])
            .from("users")
            .alias("u")
            .inner_join_on(
                "thresholds",
                JoinCond::compare("u.age", CompareOp::Gt, "t.threshold"),
            )
            .with_last_join_alias("t")
            .limit(1000),
    )
    .expect("cross-type non-equi join");
    // age = 18 + (id % 50) > 20 => id % 50 in 3..=49 (47 values) x2 = 94.
    assert_eq!(result.rows.len(), 94);
}

#[test]
fn three_table_join_chain_is_correct() {
    let (mut keyspace, mut catalog) = setup();
    add_profiles_table(&mut catalog, vec!["user_id".into()]);
    seed_profiles(&mut keyspace, 0..30, |_| "US");
    add_memberships_table(&mut catalog);
    for id in 0..20 {
        seed_membership(&mut keyspace, id, 18 + (id % 50), "gold");
    }

    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id", "p.country", "m.tier"])
            .from("users")
            .alias("u")
            .inner_join("profiles", "u.id", "user_id")
            .with_last_join_alias("p")
            .inner_join("memberships", "u.id", "user_id")
            .with_last_join_alias("m")
            .order_by("u.id", Order::Asc)
            .limit(100),
    )
    .expect("three-table join");
    // profiles cover 0..30, memberships cover 0..20 => intersection 0..20.
    assert_eq!(result.rows.len(), 20);
    assert!(matches!(result.rows[0].values[0], Value::Integer(0)));
    assert!(matches!(result.rows[19].values[0], Value::Integer(19)));
}

#[test]
fn distinct_with_computed_projection_over_join() {
    let (mut keyspace, mut catalog) = setup();
    add_profiles_table(&mut catalog, vec!["user_id".into()]);
    seed_profiles(&mut keyspace, 0..50, |_| "US");

    let snapshot = keyspace.snapshot();
    // age = 18 + (id % 50); over ids 0..50 that is 18..=67, all distinct, so the
    // computed (age * 0 = 0) collapses every row to a single distinct value.
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["p.country"])
            .from("users")
            .alias("u")
            .inner_join("profiles", "u.id", "user_id")
            .with_last_join_alias("p")
            .compute("zero", ScalarExpr::col("u.age").mul(ScalarExpr::lit(0i64)))
            .distinct()
            .limit(100),
    )
    .expect("distinct + computed over join");
    // All rows project to ("US", 0) => one distinct row.
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(result.rows[0].values[1], Value::Integer(0)));
}
