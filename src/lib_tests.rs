use super::{
    AedbInstance, CommitFinality, CommitTelemetryEvent, LifecycleEvent, LifecycleHook, QueryBatchItem,
    QueryCommitTelemetryHook, QueryTelemetryEvent, ReadOnlySqlAdapter, RecoveryCache,
    RemoteBackupAdapter, SYSTEM_CALLER_ID,
};
use crate::catalog::schema::ColumnDef;
use crate::catalog::types::{ColumnType, Row, Value};
use crate::catalog::{DdlOperation, ResourceType};
use crate::commit::validation::Mutation;
use crate::commit::tx::{TransactionEnvelope, WriteClass, WriteIntent};
use crate::config::{AedbConfig, DurabilityMode, RecoveryMode};
use crate::error::{AedbError, AedbErrorCode, ResourceType as ErrorResourceType};
use crate::permission::{CallerContext, Permission};
use crate::query::error::QueryError;
use crate::query::plan::{ConsistencyMode, Expr, Query, QueryOptions};
use crate::query::planner::ExecutionStage;
use std::fs;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tempfile::tempdir;

fn mk_recovery_view(seq: u64) -> crate::snapshot::reader::SnapshotReadView {
    crate::snapshot::reader::SnapshotReadView {
        keyspace: Arc::new(crate::storage::keyspace::Keyspace::default().snapshot()),
        catalog: Arc::new(crate::catalog::Catalog::default()),
        seq,
    }
}

struct RecordingLifecycleHook {
    events: Arc<std::sync::Mutex<Vec<LifecycleEvent>>>,
}

impl LifecycleHook for RecordingLifecycleHook {
    fn on_event(&self, event: &LifecycleEvent) {
        self.events
            .lock()
            .expect("recording hook mutex poisoned")
            .push(event.clone());
    }
}

#[test]
fn recovery_cache_refresh_keeps_recent_entry() {
    let mut cache = RecoveryCache::default();
    for seq in 1..=16 {
        cache.put(seq, mk_recovery_view(seq));
    }
    assert!(
        cache.get(1).is_some(),
        "first entry should exist before refresh"
    );

    cache.put(17, mk_recovery_view(17));

    assert!(
        cache.get(1).is_some(),
        "refreshing an entry must protect it from immediate eviction"
    );
    assert!(
        cache.get(2).is_none(),
        "oldest non-refreshed entry should be evicted first"
    );
}

#[test]
fn recovery_cache_prunes_expired_entries() {
    let mut cache = RecoveryCache::default();
    cache.put(7, mk_recovery_view(7));
    {
        let entry = cache.entries.get_mut(&7).expect("entry");
        entry.created = Instant::now() - super::RECOVERY_CACHE_TTL - Duration::from_secs(1);
    }
    cache.prune_expired();
    assert!(cache.get(7).is_none(), "expired entry must be removed");
}

#[tokio::test]
async fn api_open_commit_query_shutdown() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        columns: vec![
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
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Text("reader".into())],
        },
    })
    .await
    .expect("insert");

    let result = db
        .query(
            "p",
            "app",
            Query::select(&["*"])
                .from("users")
                .where_(Expr::Eq("id".into(), Value::Integer(1))),
        )
        .await
        .expect("query");
    assert_eq!(result.rows.len(), 1);

    db.create_scope("p", "private").await.expect("create scope");
    let scopes = db.list_scopes("p").await.expect("list scopes");
    assert!(scopes.contains(&"app".to_string()));
    assert!(scopes.contains(&"private".to_string()));
    db.drop_scope("p", "private").await.expect("drop scope");
    db.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn permissions_enforced_at_preflight_commit_and_query() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        columns: vec![
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
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "alice".into(),
        permission: Permission::TableWrite {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
        },
    }))
    .await
    .expect("grant write");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "alice".into(),
        permission: Permission::TableRead {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
        },
    }))
    .await
    .expect("grant read");

    let caller = CallerContext::new("alice");
    let insert = Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Text("alice".into())],
        },
    };
    db.preflight_as(&caller, insert.clone())
        .await
        .expect("preflight allowed");
    db.commit_as(caller.clone(), insert)
        .await
        .expect("commit allowed");

    db.commit(Mutation::Ddl(DdlOperation::RevokePermission {
        actor_id: None,
        caller_id: "alice".into(),
        permission: Permission::TableWrite {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
        },
    }))
    .await
    .expect("revoke");

    let denied = db
        .preflight_as(
            &caller,
            Mutation::Upsert {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                primary_key: vec![Value::Integer(2)],
                row: Row {
                    values: vec![Value::Integer(2), Value::Text("x".into())],
                },
            },
        )
        .await
        .expect_err("preflight denied");
    assert!(matches!(
        denied,
        crate::error::AedbError::PermissionDenied(_)
    ));

    let q = db
        .query_with_options_as(
            Some(&caller),
            "p",
            "app",
            Query::select(&["*"]).from("users").limit(10),
            crate::query::plan::QueryOptions::default(),
        )
        .await
        .expect("query with read perm");
    assert_eq!(q.rows.len(), 1);
}

#[tokio::test]
async fn insert_rejects_duplicate_primary_key() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        columns: vec![
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
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");

    db.insert(
        "p",
        "app",
        "users",
        vec![Value::Integer(1)],
        Row {
            values: vec![Value::Integer(1), Value::Text("alice".into())],
        },
    )
    .await
    .expect("first insert");

    let duplicate = db
        .insert(
            "p",
            "app",
            "users",
            vec![Value::Integer(1)],
            Row {
                values: vec![Value::Integer(1), Value::Text("alice-2".into())],
            },
        )
        .await
        .expect_err("duplicate insert must fail");
    let duplicate_text = duplicate.to_string();
    assert!(
        duplicate_text.contains("duplicate primary key"),
        "unexpected duplicate insert error: {duplicate:?}"
    );
}

#[tokio::test]
async fn join_queries_require_permissions_for_all_referenced_tables() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project p");
    db.create_project("_global").await.expect("project global");

    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        columns: vec![
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
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("users table");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "_global".into(),
        scope_id: "app".into(),
        table_name: "profiles".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "email".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("profiles table");

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "alice".into(),
        permission: Permission::TableRead {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
        },
    }))
    .await
    .expect("grant base read");
    let caller = CallerContext::new("alice");
    let query = Query::select(&["u.id"])
        .from("users")
        .alias("u")
        .left_join("_global.profiles", "u.id", "id")
        .with_last_join_alias("g")
        .limit(10);
    let denied = db
        .query_with_options_as(
            Some(&caller),
            "p",
            "app",
            query.clone(),
            crate::query::plan::QueryOptions::default(),
        )
        .await
        .expect_err("join should require global table read");
    assert!(matches!(
        denied,
        crate::query::error::QueryError::PermissionDenied { .. }
    ));

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "alice".into(),
        permission: Permission::TableRead {
            project_id: "_global".into(),
            scope_id: "app".into(),
            table_name: "profiles".into(),
        },
    }))
    .await
    .expect("grant global read");
    let _ = db
        .query_with_options_as(
            Some(&caller),
            "p",
            "app",
            query,
            crate::query::plan::QueryOptions::default(),
        )
        .await
        .expect("join allowed");
}

#[tokio::test]
async fn permission_revocation_between_preflight_and_commit_is_caught() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        columns: vec![
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
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "alice".into(),
        permission: Permission::TableWrite {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
        },
    }))
    .await
    .expect("grant");
    let caller = CallerContext::new("alice");
    let mutation = Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Text("alice".into())],
        },
    };
    db.preflight_as(&caller, mutation.clone())
        .await
        .expect("preflight ok");
    db.commit(Mutation::Ddl(DdlOperation::RevokePermission {
        actor_id: None,
        caller_id: "alice".into(),
        permission: Permission::TableWrite {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
        },
    }))
    .await
    .expect("revoke");
    let err = db
        .commit_as(caller, mutation)
        .await
        .expect_err("denied at commit");
    assert!(matches!(err, crate::error::AedbError::PermissionDenied(_)));
}

#[tokio::test]
async fn async_projection_index_reports_materialized_seq() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        columns: vec![
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
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");

    let r = db
        .commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(1)],
            row: Row {
                values: vec![Value::Integer(1), Value::Text("alice".into())],
            },
        })
        .await
        .expect("insert");
    db.commit(Mutation::Ddl(DdlOperation::CreateAsyncIndex {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        index_name: "users_view".into(),
        if_not_exists: false,
        projected_columns: vec!["id".into(), "name".into()],
    }))
    .await
    .expect("create async index");

    let opts = QueryOptions {
        async_index: Some("users_view".into()),
        allow_full_scan: true,
        ..QueryOptions::default()
    };

    let mut observed = None;
    for _ in 0..20 {
        let q = db
            .query_with_options(
                "p",
                "app",
                Query::select(&["*"]).from("users"),
                opts.clone(),
            )
            .await
            .expect("query async");
        observed = q.materialized_seq;
        if let Some(m) = q.materialized_seq
            && m >= r.commit_seq
        {
            assert_eq!(q.rows.len(), 1);
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("async index did not catch up, last materialized_seq={observed:?}");
}

#[tokio::test]
async fn kv_query_apis_enforce_permissions_and_paginate() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let caller = CallerContext::new("alice");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "alice".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: None,
        },
    }))
    .await
    .expect("grant kv read");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "alice".into(),
        permission: Permission::KvWrite {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: None,
        },
    }))
    .await
    .expect("grant kv write");

    for (k, v) in [("user:1", "alice"), ("user:2", "bob"), ("user:3", "carol")] {
        db.commit_as(
            caller.clone(),
            Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: k.as_bytes().to_vec(),
                value: v.as_bytes().to_vec(),
            },
        )
        .await
        .expect("kv set");
    }

    let one = db
        .kv_get("p", "app", b"user:1", ConsistencyMode::AtLatest, &caller)
        .await
        .expect("kv_get")
        .expect("exists");
    assert_eq!(one.value, b"alice".to_vec());

    let first = db
        .kv_scan_prefix(
            "p",
            "app",
            b"user:",
            2,
            None,
            ConsistencyMode::AtLatest,
            &caller,
        )
        .await
        .expect("scan 1");
    assert_eq!(first.entries.len(), 2);
    assert!(first.truncated);

    let second = db
        .kv_scan_prefix(
            "p",
            "app",
            b"user:",
            2,
            first.cursor.clone(),
            ConsistencyMode::AtLatest,
            &caller,
        )
        .await
        .expect("scan 2");
    assert_eq!(second.entries.len(), 1);
    assert!(!second.truncated);

    let ranged = db
        .kv_scan_range(
            "p",
            "app",
            Bound::Included(b"user:1".to_vec()),
            Bound::Included(b"user:2".to_vec()),
            10,
            None,
            ConsistencyMode::AtLatest,
            &caller,
        )
        .await
        .expect("scan range");
    assert_eq!(ranged.entries.len(), 2);
}

#[tokio::test]
async fn kv_permissions_scope_reads_and_writes_with_prefix_filters() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    let caller = CallerContext::new("alice");

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "alice".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: Some(b"bank:user:42:".to_vec()),
        },
    }))
    .await
    .expect("grant kv prefix read");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "alice".into(),
        permission: Permission::KvWrite {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: Some(b"bank:user:42:".to_vec()),
        },
    }))
    .await
    .expect("grant kv prefix write");

    db.commit_as(
        caller.clone(),
        Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"bank:user:42:balance".to_vec(),
            value: b"100".to_vec(),
        },
    )
    .await
    .expect("allowed prefix write");

    let err = db
        .commit_as(
            caller.clone(),
            Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"bank:user:99:balance".to_vec(),
                value: b"100".to_vec(),
            },
        )
        .await
        .expect_err("write outside prefix must fail");
    assert!(matches!(err, AedbError::PermissionDenied(_)));

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"bank:user:99:balance".to_vec(),
        value: b"77".to_vec(),
    })
    .await
    .expect("seed other key");

    let scan = db
        .kv_scan_prefix(
            "p",
            "app",
            b"bank:user:",
            10,
            None,
            ConsistencyMode::AtLatest,
            &caller,
        )
        .await
        .expect("scan with broad prefix");
    assert_eq!(scan.entries.len(), 1);
    assert_eq!(scan.entries[0].0, b"bank:user:42:balance".to_vec());
}

#[tokio::test]
async fn kv_write_helpers_respect_scope_boundaries() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "private").await.expect("scope");

    db.kv_set("p", "app", b"counter".to_vec(), b"100".to_vec())
        .await
        .expect("write default scope");
    db.kv_set("p", "private", b"counter".to_vec(), b"7".to_vec())
        .await
        .expect("write private scope");

    let app_reader = CallerContext::new("app_reader");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: app_reader.caller_id.clone(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: None,
        },
    }))
    .await
    .expect("grant app read");

    let private_reader = CallerContext::new("private_reader");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: private_reader.caller_id.clone(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("private".into()),
            prefix: None,
        },
    }))
    .await
    .expect("grant private read");

    let app_value = db
        .kv_get(
            "p",
            "app",
            b"counter",
            ConsistencyMode::AtLatest,
            &app_reader,
        )
        .await
        .expect("app read")
        .expect("app key");
    assert_eq!(app_value.value, b"100".to_vec());

    let private_denied = db
        .kv_get(
            "p",
            "private",
            b"counter",
            ConsistencyMode::AtLatest,
            &app_reader,
        )
        .await
        .expect_err("app reader cannot read private scope");
    assert!(matches!(
        private_denied,
        crate::query::error::QueryError::PermissionDenied { .. }
    ));

    let private_value = db
        .kv_get(
            "p",
            "private",
            b"counter",
            ConsistencyMode::AtLatest,
            &private_reader,
        )
        .await
        .expect("private read")
        .expect("private key");
    assert_eq!(private_value.value, b"7".to_vec());
}

#[tokio::test]
async fn kv_projection_table_materializes_kv_state() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.enable_kv_projection("p", "app")
        .await
        .expect("enable kv projection");

    db.kv_set("p", "app", b"user:1".to_vec(), b"alice".to_vec())
        .await
        .expect("kv set 1");
    db.kv_set("p", "app", b"user:2".to_vec(), b"bob".to_vec())
        .await
        .expect("kv set 2");

    let mut projected = None;
    for _ in 0..25 {
        let result = db
            .query(
                "p",
                "app",
                Query::select(&["*"])
                    .from(crate::catalog::KV_INDEX_TABLE)
                    .limit(10),
            )
            .await
            .expect("query projection table");
        if result.rows.len() == 2 {
            projected = Some(result.rows);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    let rows = projected.expect("projection rows");
    let mut saw_user1 = false;
    let mut saw_user2 = false;
    for row in rows {
        let project = row.values.first().expect("project_id");
        let scope = row.values.get(1).expect("scope_id");
        let key = row.values.get(2).expect("key");
        let value = row.values.get(3).expect("value");
        let commit_seq = row.values.get(4).expect("commit_seq");
        assert_eq!(project, &Value::Text("p".into()));
        assert_eq!(scope, &Value::Text("app".into()));
        assert!(matches!(commit_seq, Value::Integer(v) if *v > 0));
        match (key, value) {
            (Value::Blob(k), Value::Blob(v)) if k == b"user:1" && v == b"alice" => {
                saw_user1 = true;
            }
            (Value::Blob(k), Value::Blob(v)) if k == b"user:2" && v == b"bob" => {
                saw_user2 = true;
            }
            other => panic!("unexpected kv projection row: {other:?}"),
        }
    }
    assert!(saw_user1, "missing user:1 projection row");
    assert!(saw_user2, "missing user:2 projection row");
}

#[tokio::test]
async fn kv_projection_table_is_managed_and_read_only() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.enable_kv_projection("p", "app")
        .await
        .expect("enable projection");

    let err = db
        .commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: crate::catalog::KV_INDEX_TABLE.into(),
            primary_key: vec![Value::Blob(b"k".to_vec())],
            row: Row {
                values: vec![
                    Value::Text("p".into()),
                    Value::Text("app".into()),
                    Value::Blob(b"k".to_vec()),
                    Value::Blob(b"v".to_vec()),
                    Value::Integer(1),
                    Value::Timestamp(1),
                ],
            },
        })
        .await
        .expect_err("managed table writes must fail");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[tokio::test]
async fn kv_query_apis_are_scope_aware() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "tenant1").await.expect("scope");

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "tenant1".into(),
        key: b"user:1".to_vec(),
        value: b"alice".to_vec(),
    })
    .await
    .expect("seed kv");

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "alice".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("tenant1".into()),
            prefix: None,
        },
    }))
    .await
    .expect("grant kv read");

    let caller = CallerContext::new("alice");
    let hit = db
        .kv_get(
            "p",
            "tenant1",
            b"user:1",
            ConsistencyMode::AtLatest,
            &caller,
        )
        .await
        .expect("read in granted scope")
        .expect("value exists");
    assert_eq!(hit.value, b"alice".to_vec());

    let denied = db
        .kv_get("p", "app", b"user:1", ConsistencyMode::AtLatest, &caller)
        .await
        .expect_err("read should be denied in ungranted scope");
    assert!(matches!(
        denied,
        crate::query::error::QueryError::PermissionDenied { .. }
    ));
}

#[tokio::test]
async fn scoped_admin_permissions_bound_grant_revoke() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "s1").await.expect("scope s1");
    db.create_scope("p", "s2").await.expect("scope s2");

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "owner".into(),
        permission: Permission::ScopeAdmin {
            project_id: "p".into(),
            scope_id: "s1".into(),
        },
    }))
    .await
    .expect("grant scope admin");

    let owner = CallerContext::new("owner");
    db.commit_as(
        owner.clone(),
        Mutation::Ddl(DdlOperation::GrantPermission {
            actor_id: None,
            delegable: false,
            caller_id: "alice".into(),
            permission: Permission::KvRead {
                project_id: "p".into(),
                scope_id: Some("s1".into()),
                prefix: None,
            },
        }),
    )
    .await
    .expect("scope admin can grant in own scope");

    let denied = db
        .commit_as(
            owner.clone(),
            Mutation::Ddl(DdlOperation::GrantPermission {
                actor_id: None,
                delegable: false,
                caller_id: "alice".into(),
                permission: Permission::KvRead {
                    project_id: "p".into(),
                    scope_id: Some("s2".into()),
                    prefix: None,
                },
            }),
        )
        .await
        .expect_err("scope admin cannot grant in other scope");
    assert!(matches!(denied, AedbError::PermissionDenied(_)));

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "proj".into(),
        permission: Permission::ProjectAdmin {
            project_id: "p".into(),
        },
    }))
    .await
    .expect("grant project admin");
    let proj = CallerContext::new("proj");
    db.commit_as(
        proj,
        Mutation::Ddl(DdlOperation::GrantPermission {
            actor_id: None,
            delegable: false,
            caller_id: "alice".into(),
            permission: Permission::KvRead {
                project_id: "p".into(),
                scope_id: Some("s2".into()),
                prefix: None,
            },
        }),
    )
    .await
    .expect("project admin can grant across project scopes");
}

#[tokio::test]
async fn project_owner_and_delegable_grants_control_authz_delegation() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    db.commit(Mutation::Ddl(DdlOperation::CreateProject {
        project_id: "p".into(),
        owner_id: Some("owner".into()),
        if_not_exists: true,
    }))
    .await
    .expect("owner creates project");
    let owner = CallerContext::new("owner");

    let attacker = CallerContext::new("mallory");
    let denied = db
        .commit_as(
            attacker,
            Mutation::Ddl(DdlOperation::GrantPermission {
                caller_id: "alice".into(),
                permission: Permission::KvRead {
                    project_id: "p".into(),
                    scope_id: Some("app".into()),
                    prefix: None,
                },
                actor_id: None,
                delegable: false,
            }),
        )
        .await
        .expect_err("non-owner cannot grant");
    assert!(matches!(denied, AedbError::PermissionDenied(_)));

    db.commit_as(
        owner,
        Mutation::Ddl(DdlOperation::GrantPermission {
            caller_id: "alice".into(),
            permission: Permission::KvRead {
                project_id: "p".into(),
                scope_id: Some("app".into()),
                prefix: None,
            },
            actor_id: None,
            delegable: true,
        }),
    )
    .await
    .expect("owner grants delegable read");
    let (_, catalog, _) = db.executor.snapshot_state().await;
    assert!(catalog.has_delegable_grant(
        "alice",
        &Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: None,
        }
    ));

    let alice = CallerContext::new("alice");
    db.commit_as(
        alice.clone(),
        Mutation::Ddl(DdlOperation::GrantPermission {
            caller_id: "bob".into(),
            permission: Permission::KvRead {
                project_id: "p".into(),
                scope_id: Some("app".into()),
                prefix: None,
            },
            actor_id: None,
            delegable: false,
        }),
    )
    .await
    .expect("delegable grant allows onward delegation");

    let denied = db
        .commit_as(
            alice,
            Mutation::Ddl(DdlOperation::GrantPermission {
                caller_id: "bob".into(),
                permission: Permission::KvWrite {
                    project_id: "p".into(),
                    scope_id: Some("app".into()),
                    prefix: None,
                },
                actor_id: None,
                delegable: false,
            }),
        )
        .await
        .expect_err("delegable grant is permission-specific");
    assert!(matches!(denied, AedbError::PermissionDenied(_)));
}

#[tokio::test]
async fn kv_permission_overlap_revoke_precedence_is_explicit() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "private")
        .await
        .expect("private scope");

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"user:1".to_vec(),
        value: b"alice".to_vec(),
    })
    .await
    .expect("seed app user");
    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"order:1".to_vec(),
        value: b"100".to_vec(),
    })
    .await
    .expect("seed app order");
    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "private".into(),
        key: b"user:1".to_vec(),
        value: b"secret".to_vec(),
    })
    .await
    .expect("seed private user");

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "alice".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: None,
            prefix: None,
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant project wide read");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "alice".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: Some(b"user:".to_vec()),
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant narrow read");
    db.commit(Mutation::Ddl(DdlOperation::RevokePermission {
        caller_id: "alice".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: None,
            prefix: None,
        },
        actor_id: None,
    }))
    .await
    .expect("revoke broad read");

    let alice = CallerContext::new("alice");
    let allowed = db
        .kv_get("p", "app", b"user:1", ConsistencyMode::AtLatest, &alice)
        .await
        .expect("narrow grant still applies");
    assert!(allowed.is_some());

    let denied_key = db
        .kv_get("p", "app", b"order:1", ConsistencyMode::AtLatest, &alice)
        .await
        .expect_err("revoke broad grant should deny non-matching key");
    assert!(matches!(
        denied_key,
        crate::query::error::QueryError::PermissionDenied { .. }
    ));

    let denied_scope = db
        .kv_get("p", "private", b"user:1", ConsistencyMode::AtLatest, &alice)
        .await
        .expect_err("narrow app grant should not spill into private scope");
    assert!(matches!(
        denied_scope,
        crate::query::error::QueryError::PermissionDenied { .. }
    ));
}

#[tokio::test]
async fn grant_metadata_tracks_actor_and_system_source() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    db.commit(Mutation::Ddl(DdlOperation::CreateProject {
        project_id: "p".into(),
        owner_id: Some("owner".into()),
        if_not_exists: true,
    }))
    .await
    .expect("project");

    let owner = CallerContext::new("owner");
    db.commit_as(
        owner.clone(),
        Mutation::Ddl(DdlOperation::GrantPermission {
            caller_id: "bob".into(),
            permission: Permission::KvRead {
                project_id: "p".into(),
                scope_id: Some("app".into()),
                prefix: None,
            },
            actor_id: Some("mallory".into()),
            delegable: true,
        }),
    )
    .await
    .expect("owner grant with explicit actor");

    db.commit_as(
        owner,
        Mutation::Ddl(DdlOperation::GrantPermission {
            caller_id: "carol".into(),
            permission: Permission::KvRead {
                project_id: "p".into(),
                scope_id: Some("app".into()),
                prefix: None,
            },
            actor_id: None,
            delegable: false,
        }),
    )
    .await
    .expect("owner grant with implicit actor");

    let (_, catalog, _) = db.executor.snapshot_state().await;
    let bob_meta = catalog
        .permission_grants
        .get(&(
            "bob".to_string(),
            Permission::KvRead {
                project_id: "p".into(),
                scope_id: Some("app".into()),
                prefix: None,
            },
        ))
        .expect("bob metadata");
    assert_eq!(bob_meta.granted_by, "owner");
    assert!(bob_meta.delegable);

    let carol_meta = catalog
        .permission_grants
        .get(&(
            "carol".to_string(),
            Permission::KvRead {
                project_id: "p".into(),
                scope_id: Some("app".into()),
                prefix: None,
            },
        ))
        .expect("carol metadata");
    assert_eq!(carol_meta.granted_by, "owner");
    assert!(!carol_meta.delegable);
}

#[tokio::test]
async fn grant_and_revoke_authorization_matrix_is_enforced() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    db.commit(Mutation::Ddl(DdlOperation::CreateProject {
        project_id: "p".into(),
        owner_id: Some("owner".into()),
        if_not_exists: true,
    }))
    .await
    .expect("project");
    db.create_scope("p", "s1").await.expect("scope");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "p".into(),
        scope_id: "s1".into(),
        table_name: "events".into(),
        owner_id: Some("table_owner".into()),
        if_not_exists: false,
        columns: vec![ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Integer,
            nullable: false,
        }],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "s1".into(),
        table_name: "events".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1)],
        },
    })
    .await
    .expect("seed row");

    let denied = db
        .commit_as(
            CallerContext::new("mallory"),
            Mutation::Ddl(DdlOperation::GrantPermission {
                caller_id: "pa".into(),
                permission: Permission::ProjectAdmin {
                    project_id: "p".into(),
                },
                actor_id: None,
                delegable: false,
            }),
        )
        .await
        .expect_err("non-owner cannot grant project admin");
    assert!(matches!(denied, AedbError::PermissionDenied(_)));

    db.commit_as(
        CallerContext::new("owner"),
        Mutation::Ddl(DdlOperation::GrantPermission {
            caller_id: "pa".into(),
            permission: Permission::ProjectAdmin {
                project_id: "p".into(),
            },
            actor_id: None,
            delegable: false,
        }),
    )
    .await
    .expect("owner grants project admin");

    db.commit_as(
        CallerContext::new("pa"),
        Mutation::Ddl(DdlOperation::GrantPermission {
            caller_id: "sa".into(),
            permission: Permission::ScopeAdmin {
                project_id: "p".into(),
                scope_id: "s1".into(),
            },
            actor_id: None,
            delegable: false,
        }),
    )
    .await
    .expect("project admin grants scope admin");

    let denied = db
        .commit_as(
            CallerContext::new("sa"),
            Mutation::Ddl(DdlOperation::GrantPermission {
                caller_id: "other".into(),
                permission: Permission::ProjectAdmin {
                    project_id: "p".into(),
                },
                actor_id: None,
                delegable: false,
            }),
        )
        .await
        .expect_err("scope admin cannot grant project admin");
    assert!(matches!(denied, AedbError::PermissionDenied(_)));

    db.commit_as(
        CallerContext::new("table_owner"),
        Mutation::Ddl(DdlOperation::GrantPermission {
            caller_id: "reader".into(),
            permission: Permission::TableRead {
                project_id: "p".into(),
                scope_id: "s1".into(),
                table_name: "events".into(),
            },
            actor_id: None,
            delegable: false,
        }),
    )
    .await
    .expect("table owner grants table read");

    let reader = CallerContext::new("reader");
    let query = db
        .query_with_options_as(
            Some(&reader),
            "p",
            "s1",
            Query::select(&["id"]).from("events").limit(10),
            QueryOptions::default(),
        )
        .await
        .expect("reader query");
    assert_eq!(query.rows.len(), 1);

    let denied = db
        .commit_as(
            CallerContext::new("mallory"),
            Mutation::Ddl(DdlOperation::RevokePermission {
                caller_id: "reader".into(),
                permission: Permission::TableRead {
                    project_id: "p".into(),
                    scope_id: "s1".into(),
                    table_name: "events".into(),
                },
                actor_id: None,
            }),
        )
        .await
        .expect_err("non-admin/non-owner cannot revoke");
    assert!(matches!(denied, AedbError::PermissionDenied(_)));

    db.commit_as(
        CallerContext::new("pa"),
        Mutation::Ddl(DdlOperation::RevokePermission {
            caller_id: "reader".into(),
            permission: Permission::TableRead {
                project_id: "p".into(),
                scope_id: "s1".into(),
                table_name: "events".into(),
            },
            actor_id: None,
        }),
    )
    .await
    .expect("project admin revokes table read");

    let denied_query = db
        .query_with_options_as(
            Some(&reader),
            "p",
            "s1",
            Query::select(&["id"]).from("events").limit(10),
            QueryOptions::default(),
        )
        .await
        .expect_err("reader should lose access after revoke");
    assert!(matches!(
        denied_query,
        crate::query::error::QueryError::PermissionDenied { .. }
    ));
}

#[tokio::test]
async fn kv_scan_all_scopes_requires_project_wide_read() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "s1").await.expect("scope s1");
    db.create_scope("p", "s2").await.expect("scope s2");

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "s1".into(),
        key: b"user:1".to_vec(),
        value: b"alice".to_vec(),
    })
    .await
    .expect("seed s1");
    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "s2".into(),
        key: b"user:2".to_vec(),
        value: b"bob".to_vec(),
    })
    .await
    .expect("seed s2");

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "alice".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: None,
            prefix: None,
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant project-wide read");

    let alice = CallerContext::new("alice");
    let entries = db
        .kv_scan_all_scopes("p", b"user:", 10, ConsistencyMode::AtLatest, &alice)
        .await
        .expect("scan all scopes");
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().any(|e| e.scope_id == "s1"));
    assert!(entries.iter().any(|e| e.scope_id == "s2"));

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "bob".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("s1".into()),
            prefix: None,
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant scope-only read");
    let bob = CallerContext::new("bob");
    let denied = db
        .kv_scan_all_scopes("p", b"user:", 10, ConsistencyMode::AtLatest, &bob)
        .await
        .expect_err("scope-only grant cannot scan all scopes");
    assert!(matches!(
        denied,
        crate::query::error::QueryError::PermissionDenied { .. }
    ));
}

#[tokio::test]
async fn ownership_transfer_requires_owner_or_global_admin() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.commit(Mutation::Ddl(DdlOperation::CreateProject {
        project_id: "p".into(),
        owner_id: Some("owner".into()),
        if_not_exists: true,
    }))
    .await
    .expect("create project with owner");

    let mallory = CallerContext::new("mallory");
    let denied = db
        .commit_as(
            mallory,
            Mutation::Ddl(DdlOperation::TransferOwnership {
                resource_type: ResourceType::Project,
                project_id: "p".into(),
                scope_id: None,
                table_name: None,
                new_owner_id: "next".into(),
                actor_id: None,
            }),
        )
        .await
        .expect_err("non-owner transfer must fail");
    assert!(matches!(denied, AedbError::PermissionDenied(_)));

    let owner = CallerContext::new("owner");
    db.commit_as(
        owner,
        Mutation::Ddl(DdlOperation::TransferOwnership {
            resource_type: ResourceType::Project,
            project_id: "p".into(),
            scope_id: None,
            table_name: None,
            new_owner_id: "next".into(),
            actor_id: None,
        }),
    )
    .await
    .expect("owner transfer");

    let (_, catalog, _) = db.executor.snapshot_state().await;
    assert_eq!(
        catalog
            .projects
            .get("p")
            .and_then(|p| p.owner_id.as_deref()),
        Some("next")
    );
}

#[tokio::test]
async fn authz_grant_revoke_events_are_persisted_in_system_audit_table() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.commit(Mutation::Ddl(DdlOperation::CreateProject {
        project_id: "p".into(),
        owner_id: Some("owner".into()),
        if_not_exists: true,
    }))
    .await
    .expect("project");
    let owner = CallerContext::new("owner");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        owner_id: None,
        if_not_exists: false,
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "owner".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("users table");

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "alice".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: None,
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant");
    db.commit(Mutation::Ddl(DdlOperation::RevokePermission {
        caller_id: "alice".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: None,
        },
        actor_id: None,
    }))
    .await
    .expect("revoke");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "owner".into(),
        permission: Permission::TableDdl {
            project_id: "p".into(),
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant owner ddl");
    db.commit_as(
        owner.clone(),
        Mutation::Ddl(DdlOperation::SetReadPolicy {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            predicate: Expr::Eq("owner".into(), Value::Text("$caller_id".into())),
            actor_id: None,
        }),
    )
    .await
    .expect("set policy");
    db.commit_as(
        owner.clone(),
        Mutation::Ddl(DdlOperation::ClearReadPolicy {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            actor_id: None,
        }),
    )
    .await
    .expect("clear policy");
    db.commit_as(
        owner,
        Mutation::Ddl(DdlOperation::TransferOwnership {
            resource_type: ResourceType::Project,
            project_id: "p".into(),
            scope_id: None,
            table_name: None,
            new_owner_id: "next".into(),
            actor_id: None,
        }),
    )
    .await
    .expect("transfer policy");

    let audit = db
        .query(
            "_system",
            "app",
            Query::select(&["*"]).from("authz_audit").limit(20),
        )
        .await
        .expect("audit query");
    assert!(audit.rows.len() >= 2, "expected at least grant+revoke rows");
    let actions: Vec<String> = audit
        .rows
        .iter()
        .filter_map(|r| match r.values.get(2) {
            Some(Value::Text(v)) => Some(v.to_string()),
            _ => None,
        })
        .collect();
    assert!(actions.iter().any(|a| a == "grant"));
    assert!(actions.iter().any(|a| a == "revoke"));
    assert!(actions.iter().any(|a| a == "set_read_policy"));
    assert!(actions.iter().any(|a| a == "clear_read_policy"));
    assert!(actions.iter().any(|a| a == "transfer_ownership"));

    let owner_actor_actions: Vec<String> = audit
        .rows
        .iter()
        .filter_map(|r| match (r.values.get(2), r.values.get(3)) {
            (Some(Value::Text(action)), Some(Value::Text(actor))) if actor.as_str() == "owner" => {
                Some(action.to_string())
            }
            _ => None,
        })
        .collect();
    assert!(owner_actor_actions.iter().any(|a| a == "set_read_policy"));
    assert!(owner_actor_actions.iter().any(|a| a == "clear_read_policy"));
    assert!(
        owner_actor_actions
            .iter()
            .any(|a| a == "transfer_ownership")
    );
}

#[tokio::test]
async fn read_policies_filter_rows_for_caller_and_can_be_cleared() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        owner_id: None,
        if_not_exists: false,
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "owner".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Text("reader".into())],
        },
    })
    .await
    .expect("row 1");
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(2)],
        row: Row {
            values: vec![Value::Integer(2), Value::Text("bob".into())],
        },
    })
    .await
    .expect("row 2");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "reader".into(),
        permission: Permission::TableRead {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant read");
    db.set_read_policy(
        "p",
        "app",
        "users",
        Expr::Eq("owner".into(), Value::Text("$caller_id".into())),
    )
    .await
    .expect("set read policy");

    let reader = CallerContext::new("reader");
    let filtered = db
        .query_with_options_as(
            Some(&reader),
            "p",
            "app",
            Query::select(&["*"]).from("users").limit(10),
            QueryOptions::default(),
        )
        .await
        .expect("filtered query");
    assert_eq!(filtered.rows.len(), 1);

    db.clear_read_policy("p", "app", "users")
        .await
        .expect("clear read policy");
    let unfiltered = db
        .query_with_options_as(
            Some(&reader),
            "p",
            "app",
            Query::select(&["*"]).from("users").limit(10),
            QueryOptions::default(),
        )
        .await
        .expect("unfiltered query");
    assert_eq!(unfiltered.rows.len(), 2);
}

#[tokio::test]
async fn read_policy_applies_to_joined_tables() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        owner_id: None,
        if_not_exists: false,
        columns: vec![ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Integer,
            nullable: false,
        }],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("users");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "profiles".into(),
        owner_id: None,
        if_not_exists: false,
        columns: vec![
            ColumnDef {
                name: "user_id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "owner".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["user_id".into()],
    }))
    .await
    .expect("profiles");

    for id in [1, 2] {
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row {
                values: vec![Value::Integer(id)],
            },
        })
        .await
        .expect("seed user");
    }
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "profiles".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Text("reader".into())],
        },
    })
    .await
    .expect("profile 1");
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "profiles".into(),
        primary_key: vec![Value::Integer(2)],
        row: Row {
            values: vec![Value::Integer(2), Value::Text("bob".into())],
        },
    })
    .await
    .expect("profile 2");

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "reader".into(),
        permission: Permission::TableRead {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant users read");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "reader".into(),
        permission: Permission::TableRead {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "profiles".into(),
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant profiles read");
    db.set_read_policy(
        "p",
        "app",
        "profiles",
        Expr::Eq("owner".into(), Value::Text("$caller_id".into())),
    )
    .await
    .expect("set join policy");

    let reader = CallerContext::new("reader");
    let result = db
        .query_with_options_as(
            Some(&reader),
            "p",
            "app",
            Query::select(&["*"])
                .from("users")
                .alias("u")
                .inner_join("profiles", "u.id", "user_id")
                .with_last_join_alias("p"),
            QueryOptions::default(),
        )
        .await
        .expect("join query");
    assert_eq!(result.rows.len(), 1);
}

#[tokio::test]
async fn production_profile_requires_hmac() {
    let dir = tempdir().expect("temp");
    let invalid = AedbConfig {
        recovery_mode: RecoveryMode::Strict,
        durability_mode: DurabilityMode::Full,
        hash_chain_required: true,
        manifest_hmac_key: None,
        ..AedbConfig::default()
    };
    let err = AedbInstance::open_production(invalid, dir.path())
        .err()
        .expect("must reject missing hmac");
    assert!(matches!(err, AedbError::InvalidConfig { .. }));

    let valid = AedbConfig::production([7u8; 32]);
    AedbInstance::open_production(valid, dir.path()).expect("open production");
}

#[tokio::test]
async fn secure_profile_requires_hardened_storage_settings() {
    let dir = tempdir().expect("temp");
    let weak = AedbConfig::default();
    let err = AedbInstance::open_secure(weak, dir.path())
        .err()
        .expect("must reject weak secure profile");
    assert!(matches!(err, AedbError::InvalidConfig { .. }));

    let hardened = AedbConfig::production([9u8; 32]);
    AedbInstance::open_secure(hardened, dir.path()).expect("open secure");
}

#[test]
fn low_latency_profile_uses_batch_durability_with_strict_recovery() {
    let cfg = AedbConfig::low_latency([5u8; 32]);
    assert_eq!(cfg.durability_mode, DurabilityMode::Batch);
    assert_eq!(cfg.recovery_mode, RecoveryMode::Strict);
    assert!(cfg.hash_chain_required);
    assert!(cfg.batch_interval_ms > 0);
    assert!(cfg.batch_max_bytes > 0);
    assert!(cfg.manifest_hmac_key.is_some());
}

#[tokio::test]
async fn secure_mode_requires_authenticated_apis() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_production(AedbConfig::production([7u8; 32]), dir.path())
        .expect("open secure");

    let err = db
        .commit(Mutation::Ddl(DdlOperation::CreateProject {
            owner_id: None,
            if_not_exists: true,
            project_id: "p".into(),
        }))
        .await
        .expect_err("anonymous commit must fail");
    assert!(matches!(err, AedbError::PermissionDenied(_)));

    db.commit_as(
        CallerContext::system_internal(),
        Mutation::Ddl(DdlOperation::CreateProject {
            owner_id: None,
            if_not_exists: true,
            project_id: "p".into(),
        }),
    )
    .await
    .expect("system commit");

    let spoof_err = db
        .commit_as(
            CallerContext::new(SYSTEM_CALLER_ID),
            Mutation::Ddl(DdlOperation::CreateProject {
                owner_id: None,
                if_not_exists: true,
                project_id: "q".into(),
            }),
        )
        .await
        .expect_err("spoofed system caller must fail");
    assert!(matches!(spoof_err, AedbError::PermissionDenied(_)));

    let qerr = db
        .query("p", "app", Query::select(&["*"]).from("users").limit(1))
        .await
        .expect_err("anonymous query must fail");
    assert!(matches!(qerr, QueryError::PermissionDenied { .. }));

    let qerr_direct = db
        .query_with_options_as(
            None,
            "p",
            "app",
            Query::select(&["*"]).from("users").limit(1),
            QueryOptions::default(),
        )
        .await
        .expect_err("anonymous query_with_options_as must fail");
    assert!(matches!(qerr_direct, QueryError::PermissionDenied { .. }));

    let explain_err = db
        .explain_query_as(
            None,
            "p",
            "app",
            Query::select(&["*"]).from("users").limit(1),
            QueryOptions::default(),
        )
        .await
        .expect_err("anonymous explain_query_as must fail");
    assert!(matches!(explain_err, QueryError::PermissionDenied { .. }));
}

#[tokio::test]
async fn existence_and_introspection_apis_report_catalog_state() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    db.create_project("p").await.expect("project");
    db.create_scope("p", "s1").await.expect("scope");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "p".into(),
        scope_id: "s1".into(),
        table_name: "users".into(),
        columns: vec![
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
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");
    db.commit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: "p".into(),
        scope_id: "s1".into(),
        table_name: "users".into(),
        index_name: "idx_users_name".into(),
        if_not_exists: false,
        columns: vec!["name".into()],
        index_type: crate::catalog::schema::IndexType::BTree,
        partial_filter: None,
    }))
    .await
    .expect("index");

    assert!(db.project_exists("p").await.expect("project_exists"));
    assert!(db.scope_exists("p", "s1").await.expect("scope_exists"));
    assert!(
        db.table_exists("p", "s1", "users")
            .await
            .expect("table_exists")
    );
    assert!(
        db.index_exists("p", "s1", "users", "idx_users_name")
            .await
            .expect("index_exists")
    );

    let projects = db.list_projects().await.expect("list projects");
    assert!(projects.iter().any(|p| p.project_id == "p"));

    let scopes = db.list_scopes_info("p").await.expect("list scopes");
    let scope = scopes
        .iter()
        .find(|s| s.scope_id == "s1")
        .expect("scope info");
    assert_eq!(scope.table_count, 1);

    let tables = db.list_tables_info("p", "s1").await.expect("list tables");
    let table = tables
        .iter()
        .find(|t| t.table_name == "users")
        .expect("table info");
    assert_eq!(table.column_count, 2);
    assert_eq!(table.index_count, 1);
}

#[tokio::test]
async fn no_auth_read_apis_work_in_non_secure_mode() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "cfg".into(),
        columns: vec![
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
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("create cfg table");
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "cfg".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Text("bootstrap".into())],
        },
    })
    .await
    .expect("upsert cfg row");
    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"config:bootstrap".to_vec(),
        value: b"v1".to_vec(),
    })
    .await
    .expect("set kv");

    let got = db
        .kv_get_no_auth("p", "app", b"config:bootstrap", ConsistencyMode::AtLatest)
        .await
        .expect("kv_get_no_auth")
        .expect("entry");
    assert_eq!(got.value, b"v1".to_vec());

    let scanned = db
        .kv_scan_prefix_no_auth("p", "app", b"config:", 10, ConsistencyMode::AtLatest)
        .await
        .expect("scan no auth");
    assert_eq!(scanned.len(), 1);

    let query_res = db
        .query_no_auth(
            "p",
            "app",
            Query::select(&["id", "name"]).from("cfg").limit(10),
            QueryOptions::default(),
        )
        .await
        .expect("query_no_auth");
    assert_eq!(query_res.rows.len(), 1);
}

#[tokio::test]
async fn ddl_if_not_exists_is_idempotent_and_reports_applied() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    let first = db
        .commit_ddl(DdlOperation::CreateProject {
            owner_id: None,
            project_id: "arcana".into(),
            if_not_exists: true,
        })
        .await
        .expect("create project first");
    assert!(first.applied);

    let second = db
        .commit_ddl(DdlOperation::CreateProject {
            owner_id: None,
            project_id: "arcana".into(),
            if_not_exists: true,
        })
        .await
        .expect("create project second");
    assert!(!second.applied);

    let scope_first = db
        .commit_ddl(DdlOperation::CreateScope {
            owner_id: None,
            project_id: "arcana".into(),
            scope_id: "app".into(),
            if_not_exists: true,
        })
        .await
        .expect("create scope first");
    assert!(!scope_first.applied, "default app scope already exists");

    let table_first = db
        .commit_ddl(DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: true,
            project_id: "arcana".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            columns: vec![
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
            primary_key: vec!["id".into()],
        })
        .await
        .expect("create table first");
    assert!(table_first.applied);

    let table_second = db
        .commit_ddl(DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: true,
            project_id: "arcana".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            columns: vec![
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
            primary_key: vec!["id".into()],
        })
        .await
        .expect("create table second");
    assert!(!table_second.applied);

    let err = db
        .commit(Mutation::Ddl(DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: false,
            project_id: "arcana".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            columns: vec![
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
            primary_key: vec!["id".into()],
        }))
        .await
        .expect_err("duplicate create should error without if_not_exists");
    assert!(matches!(
        err,
        AedbError::AlreadyExists {
            resource_type: ErrorResourceType::Table,
            ..
        }
    ));

    let drop_noop = db
        .commit_ddl(DdlOperation::DropTable {
            project_id: "arcana".into(),
            scope_id: "app".into(),
            table_name: "does_not_exist".into(),
            if_exists: true,
        })
        .await
        .expect("drop noop with if_exists");
    assert!(!drop_noop.applied);

    let drop_err = db
        .commit(Mutation::Ddl(DdlOperation::DropTable {
            project_id: "arcana".into(),
            scope_id: "app".into(),
            table_name: "does_not_exist".into(),
            if_exists: false,
        }))
        .await
        .expect_err("drop missing should error when if_exists=false");
    assert!(matches!(
        drop_err,
        AedbError::NotFound {
            resource_type: ErrorResourceType::Table,
            ..
        }
    ));
}

#[tokio::test]
async fn commit_ddl_batch_is_atomic_and_reports_per_op_results() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("arcana").await.expect("project");

    let failed = db
        .commit_ddl_batch(vec![
            DdlOperation::CreateTable {
                owner_id: None,
                if_not_exists: false,
                project_id: "arcana".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                columns: vec![
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
                primary_key: vec!["id".into()],
            },
            DdlOperation::CreateTable {
                owner_id: None,
                if_not_exists: false,
                project_id: "arcana".into(),
                scope_id: "missing".into(),
                table_name: "invalid".into(),
                columns: vec![ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                }],
                primary_key: vec!["id".into()],
            },
        ])
        .await
        .expect_err("invalid second ddl should fail entire batch");
    assert!(matches!(
        failed,
        AedbError::NotFound {
            resource_type: ErrorResourceType::Scope,
            ..
        }
    ));
    assert!(
        !db.table_exists("arcana", "app", "users")
            .await
            .expect("users table exists check")
    );

    let first = db
        .commit_ddl_batch(vec![DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: true,
            project_id: "arcana".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            columns: vec![
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
            primary_key: vec!["id".into()],
        }])
        .await
        .expect("first batch");
    assert_eq!(first.results.len(), 1);
    assert!(first.results[0].applied);
    assert!(
        db.table_exists("arcana", "app", "users")
            .await
            .expect("table exists")
    );

    let second = db
        .commit_ddl_batch(vec![DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: true,
            project_id: "arcana".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            columns: vec![
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
            primary_key: vec!["id".into()],
        }])
        .await
        .expect("second batch");
    assert_eq!(second.results.len(), 1);
    assert!(!second.results[0].applied);
}

#[tokio::test]
async fn commit_ddl_batch_supports_dependent_ddl_ordering() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    let batch = db
        .commit_ddl_batch(vec![
            DdlOperation::CreateProject {
                owner_id: None,
                project_id: "arcana".into(),
                if_not_exists: true,
            },
            DdlOperation::CreateScope {
                owner_id: None,
                project_id: "arcana".into(),
                scope_id: "ops".into(),
                if_not_exists: true,
            },
            DdlOperation::CreateTable {
                owner_id: None,
                if_not_exists: true,
                project_id: "arcana".into(),
                scope_id: "ops".into(),
                table_name: "users".into(),
                columns: vec![
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
                primary_key: vec!["id".into()],
            },
        ])
        .await
        .expect("dependent batch");

    assert_eq!(batch.results.len(), 3);
    assert!(batch.results.iter().all(|r| r.applied));
    assert!(
        db.project_exists("arcana")
            .await
            .expect("project exists after batch")
    );
    assert!(
        db.scope_exists("arcana", "ops")
            .await
            .expect("scope exists after batch")
    );
    assert!(
        db.table_exists("arcana", "ops", "users")
            .await
            .expect("table exists after batch")
    );
}

#[tokio::test]
async fn dependency_aware_ddl_batch_reorders_create_dependencies() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    let batch = db
        .commit_ddl_batch_dependency_aware(vec![
            DdlOperation::CreateIndex {
                project_id: "arcana".into(),
                scope_id: "ops".into(),
                table_name: "users".into(),
                index_name: "idx_users_name".into(),
                if_not_exists: true,
                columns: vec!["name".into()],
                index_type: crate::catalog::schema::IndexType::BTree,
                partial_filter: None,
            },
            DdlOperation::CreateTable {
                owner_id: None,
                if_not_exists: true,
                project_id: "arcana".into(),
                scope_id: "ops".into(),
                table_name: "users".into(),
                columns: vec![
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
                primary_key: vec!["id".into()],
            },
            DdlOperation::CreateScope {
                owner_id: None,
                project_id: "arcana".into(),
                scope_id: "ops".into(),
                if_not_exists: true,
            },
            DdlOperation::CreateProject {
                owner_id: None,
                project_id: "arcana".into(),
                if_not_exists: true,
            },
        ])
        .await
        .expect("dependency-aware batch");

    assert_eq!(batch.results.len(), 4);
    assert!(
        db.index_exists("arcana", "ops", "users", "idx_users_name")
            .await
            .expect("index exists")
    );
}

#[tokio::test]
async fn ddl_errors_expose_stable_error_codes() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("arcana").await.expect("project");

    let err = db
        .commit(Mutation::Ddl(DdlOperation::CreateIndex {
            project_id: "arcana".into(),
            scope_id: "app".into(),
            table_name: "missing_users".into(),
            index_name: "idx_missing".into(),
            if_not_exists: false,
            columns: vec!["name".into()],
            index_type: crate::catalog::schema::IndexType::BTree,
            partial_filter: None,
        }))
        .await
        .expect_err("create index on missing table should fail");

    assert_eq!(err.code(), AedbErrorCode::TableNotFound);
}

#[tokio::test]
async fn index_introspection_apis_return_index_definitions() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("arcana").await.expect("project");
    db.commit_ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: true,
        project_id: "arcana".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        columns: vec![
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
        primary_key: vec!["id".into()],
    })
    .await
    .expect("create table");
    db.commit_ddl(DdlOperation::CreateIndex {
        project_id: "arcana".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        index_name: "idx_users_name".into(),
        if_not_exists: false,
        columns: vec!["name".into()],
        index_type: crate::catalog::schema::IndexType::BTree,
        partial_filter: None,
    })
    .await
    .expect("create index");

    let listed = db
        .list_indexes("arcana", "app", "users")
        .await
        .expect("list indexes");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].index_name, "idx_users_name");

    let described = db
        .describe_index("arcana", "app", "users", "idx_users_name")
        .await
        .expect("describe index");
    assert_eq!(described.columns, vec!["name".to_string()]);
}

#[tokio::test]
async fn lifecycle_hooks_receive_post_commit_events_for_applied_ddl() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let hook: Arc<dyn LifecycleHook> = Arc::new(RecordingLifecycleHook {
        events: Arc::clone(&events),
    });
    db.add_lifecycle_hook(Arc::clone(&hook));

    db.commit_ddl(DdlOperation::CreateProject {
        owner_id: None,
        project_id: "arcana".into(),
        if_not_exists: true,
    })
    .await
    .expect("create project");
    db.commit_ddl(DdlOperation::CreateProject {
        owner_id: None,
        project_id: "arcana".into(),
        if_not_exists: true,
    })
    .await
    .expect("idempotent create");

    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if !events.lock().expect("events lock").is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let seen = events.lock().expect("events lock");
    assert_eq!(seen.len(), 1);
    assert!(matches!(
        &seen[0],
        LifecycleEvent::ProjectCreated { project_id, .. } if project_id == "arcana"
    ));
}

#[test]
fn open_rejects_invalid_config() {
    let dir = tempdir().expect("temp");
    let bad = AedbConfig {
        durability_mode: DurabilityMode::Batch,
        batch_interval_ms: 0,
        ..AedbConfig::default()
    };
    let err = AedbInstance::open(bad, dir.path())
        .err()
        .expect("invalid config");
    assert!(matches!(err, crate::error::AedbError::InvalidConfig { .. }));

    let too_large_txn = AedbConfig {
        max_transaction_bytes: crate::wal::frame::MAX_FRAME_BODY_BYTES + 1,
        ..AedbConfig::default()
    };
    let err = AedbInstance::open(too_large_txn, dir.path())
        .err()
        .expect("oversized transaction bound");
    assert!(matches!(err, crate::error::AedbError::InvalidConfig { .. }));

    let deadlock_unsafe = AedbConfig {
        recovery_mode: RecoveryMode::Strict,
        coordinator_locking_enabled: false,
        ..AedbConfig::default()
    };
    let err = AedbInstance::open(deadlock_unsafe, dir.path())
        .err()
        .expect("strict mode must require coordinator locking");
    assert!(matches!(err, crate::error::AedbError::InvalidConfig { .. }));
}

#[tokio::test]
async fn snapshot_limit_enforced_on_read_path() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(
        AedbConfig {
            max_concurrent_snapshots: 1,
            ..AedbConfig::default()
        },
        dir.path(),
    )
    .expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "alice".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: None,
        },
    }))
    .await
    .expect("grant");
    let caller = CallerContext::new("alice");

    let handle = {
        let mut mgr = db.snapshot_manager.lock();
        mgr.acquire_bounded(
            crate::snapshot::reader::SnapshotReadView {
                keyspace: Arc::new(crate::storage::keyspace::Keyspace::default().snapshot()),
                catalog: Arc::new(crate::catalog::Catalog::default()),
                seq: 0,
            },
            1,
        )
        .expect("occupy")
    };

    let err = db
        .kv_get("p", "app", b"k", ConsistencyMode::AtLatest, &caller)
        .await
        .expect_err("snapshot cap");
    assert!(matches!(
        err,
        crate::query::error::QueryError::SnapshotLimitReached
    ));

    let mut mgr = db.snapshot_manager.lock();
    mgr.release(handle);
    let _ = mgr.gc();
}

#[tokio::test]
async fn metrics_surface_reflects_commits() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    let m = db.metrics();
    assert!(m.commits_total >= 1);
    let op = db.operational_metrics().await;
    assert!(op.commits_total >= 1);
    assert!(op.read_set_conflicts <= op.conflict_rejections);
    assert!(op.snapshot_age_micros <= u64::MAX / 2);
}

#[tokio::test]
async fn checkpoint_now_enables_clean_restart() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        columns: vec![
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
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(5)],
        row: Row {
            values: vec![Value::Integer(5), Value::Text("bob".into())],
        },
    })
    .await
    .expect("insert");

    let seq = db.checkpoint_now().await.expect("checkpoint");
    assert!(seq >= 3);

    let reopened = AedbInstance::open(AedbConfig::default(), dir.path()).expect("reopen");
    let rows = reopened
        .query(
            "p",
            "app",
            Query::select(&["*"])
                .from("users")
                .where_(Expr::Eq("id".into(), Value::Integer(5))),
        )
        .await
        .expect("query");
    assert_eq!(rows.rows.len(), 1);
}

#[tokio::test]
async fn checkpoint_now_in_batch_mode_flushes_wal_and_recovers_tail() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        durability_mode: DurabilityMode::Batch,
        batch_interval_ms: 60_000,
        batch_max_bytes: usize::MAX,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"tail".to_vec(),
        value: b"v1".to_vec(),
    })
    .await
    .expect("tail write");

    let before = db.head_state().await;
    assert!(before.visible_head_seq > before.durable_head_seq);

    let cp_seq = db.checkpoint_now().await.expect("checkpoint");
    let after = db.head_state().await;
    assert_eq!(after.visible_head_seq, after.durable_head_seq);
    assert!(after.durable_head_seq >= cp_seq);
    drop(db);

    let recovered = crate::recovery::recover_with_config(dir.path(), &config).expect("recover");
    let tail = recovered
        .keyspace
        .kv_get("p", "app", b"tail")
        .expect("tail recovered");
    assert_eq!(tail.value, b"v1".to_vec());
}

#[tokio::test]
async fn commit_with_visible_finality_can_return_before_durable_head_in_batch_mode() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        durability_mode: DurabilityMode::Batch,
        batch_interval_ms: 60_000,
        batch_max_bytes: usize::MAX,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open(config, dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let result = db
        .commit_with_finality(
            Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"fast-visible".to_vec(),
                value: b"v".to_vec(),
            },
            CommitFinality::Visible,
        )
        .await
        .expect("commit");

    assert!(
        result.durable_head_seq < result.commit_seq,
        "visible finality should not require durable head in batch mode"
    );
}

#[tokio::test]
async fn commit_with_durable_finality_waits_until_durable_head_catches_up() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        durability_mode: DurabilityMode::Batch,
        batch_interval_ms: 60_000,
        batch_max_bytes: usize::MAX,
        ..AedbConfig::default()
    };
    let db = Arc::new(AedbInstance::open(config, dir.path()).expect("open"));
    db.create_project("p").await.expect("project");

    let fsync_db = Arc::clone(&db);
    let fsync_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        fsync_db.force_fsync().await.expect("force fsync");
    });

    let started = Instant::now();
    let result = db
        .commit_with_finality(
            Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"fast-durable".to_vec(),
                value: b"v".to_vec(),
            },
            CommitFinality::Durable,
        )
        .await
        .expect("commit");
    fsync_task.await.expect("join fsync");

    assert!(
        started.elapsed() >= Duration::from_millis(15),
        "durable finality should wait for WAL durability in batch mode"
    );
    assert!(
        result.durable_head_seq >= result.commit_seq,
        "durable finality must report durable head at or beyond commit sequence"
    );
}

#[tokio::test]
async fn multi_update_transaction_envelope_updates_table_and_kv() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        durability_mode: DurabilityMode::Batch,
        batch_interval_ms: 60_000,
        batch_max_bytes: usize::MAX,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open(config, dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "balance".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("accounts table");

    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Integer(100)],
        },
    })
    .await
    .expect("seed account 1");
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        primary_key: vec![Value::Integer(2)],
        row: Row {
            values: vec![Value::Integer(2), Value::Integer(80)],
        },
    })
    .await
    .expect("seed account 2");

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "reader".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: None,
        },
    }))
    .await
    .expect("grant kv read");
    let caller = CallerContext::new("reader");

    let result = db
        .commit_envelope_with_finality(
            TransactionEnvelope {
                caller: None,
                idempotency_key: None,
                write_class: WriteClass::Standard,
                assertions: Vec::new(),
                read_set: Default::default(),
                write_intent: WriteIntent {
                    mutations: vec![
                        Mutation::Upsert {
                            project_id: "p".into(),
                            scope_id: "app".into(),
                            table_name: "accounts".into(),
                            primary_key: vec![Value::Integer(1)],
                            row: Row {
                                values: vec![Value::Integer(1), Value::Integer(90)],
                            },
                        },
                        Mutation::Upsert {
                            project_id: "p".into(),
                            scope_id: "app".into(),
                            table_name: "accounts".into(),
                            primary_key: vec![Value::Integer(2)],
                            row: Row {
                                values: vec![Value::Integer(2), Value::Integer(90)],
                            },
                        },
                        Mutation::KvSet {
                            project_id: "p".into(),
                            scope_id: "app".into(),
                            key: b"tx:last".to_vec(),
                            value: b"t1".to_vec(),
                        },
                        Mutation::KvSet {
                            project_id: "p".into(),
                            scope_id: "app".into(),
                            key: b"ledger:1->2".to_vec(),
                            value: b"10".to_vec(),
                        },
                    ],
                },
                base_seq: db.head_state().await.visible_head_seq,
            },
            CommitFinality::Visible,
        )
        .await
        .expect("multi-update tx");

    assert!(
        result.durable_head_seq < result.commit_seq,
        "visible finality should return before durable head in batch mode"
    );

    let acct1 = db
        .query(
            "p",
            "app",
            Query::select(&["balance"])
                .from("accounts")
                .where_(Expr::Eq("id".into(), Value::Integer(1)))
                .limit(1),
        )
        .await
        .expect("query account1");
    let acct2 = db
        .query(
            "p",
            "app",
            Query::select(&["balance"])
                .from("accounts")
                .where_(Expr::Eq("id".into(), Value::Integer(2)))
                .limit(1),
        )
        .await
        .expect("query account2");
    assert_eq!(acct1.rows.len(), 1);
    assert_eq!(acct2.rows.len(), 1);
    assert_eq!(acct1.rows[0].values[0], Value::Integer(90));
    assert_eq!(acct2.rows[0].values[0], Value::Integer(90));

    let tx_last = db
        .kv_get("p", "app", b"tx:last", ConsistencyMode::AtLatest, &caller)
        .await
        .expect("tx:last");
    let ledger = db
        .kv_get("p", "app", b"ledger:1->2", ConsistencyMode::AtLatest, &caller)
        .await
        .expect("ledger entry");
    assert_eq!(tx_last.as_ref().map(|v| v.value.clone()), Some(b"t1".to_vec()));
    assert_eq!(ledger.as_ref().map(|v| v.value.clone()), Some(b"10".to_vec()));
}

#[tokio::test]
async fn checkpoint_now_waits_for_active_writes() {
    let dir = tempdir().expect("temp");
    let db = Arc::new(AedbInstance::open(AedbConfig::default(), dir.path()).expect("open"));
    db.create_project("p").await.expect("project");

    // Simulate an active operation by holding a semaphore permit
    let _permit = db.checkpoint_gate.acquire().await.unwrap();
    let checkpoint_db = Arc::clone(&db);
    let checkpoint_task = tokio::spawn(async move { checkpoint_db.checkpoint_now().await });

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(
        !checkpoint_task.is_finished(),
        "checkpoint should wait while operations are active"
    );

    drop(_permit);
    checkpoint_task
        .await
        .expect("checkpoint join")
        .expect("checkpoint");
}

#[tokio::test]
async fn backup_lock_blocks_new_write_submissions() {
    let dir = tempdir().expect("temp");
    let db = Arc::new(AedbInstance::open(AedbConfig::default(), dir.path()).expect("open"));
    db.create_project("p").await.expect("project");

    // Simulate a checkpoint/backup in progress
    db.checkpoint_in_progress.store(true, Ordering::Release);
    let _all_permits = db.checkpoint_gate.acquire_many(1000).await.unwrap();

    // Commit should fail fast with checkpoint error (better than blocking)
    let result = db
        .commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"blocked".to_vec(),
            value: b"1".to_vec(),
        })
        .await;

    assert!(
        result.is_err(),
        "write should be rejected while checkpoint is in progress"
    );
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("checkpoint in progress"),
        "error should indicate checkpoint"
    );

    // Release checkpoint lock and verify commits now work
    db.checkpoint_in_progress.store(false, Ordering::Release);
    drop(_all_permits);

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"success".to_vec(),
        value: b"1".to_vec(),
    })
    .await
    .expect("commit after checkpoint should succeed");
}

#[tokio::test]
async fn snapshot_probe_returns_snapshot_seq() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    let seq = db
        .snapshot_probe(ConsistencyMode::AtLatest)
        .await
        .expect("probe");
    assert!(seq >= 1);
}

#[tokio::test]
async fn at_checkpoint_falls_back_when_version_evicted() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        max_versions: 2,
        min_version_age_ms: 0,
        version_gc_interval_ms: 1,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open(config, dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"checkpoint:key".to_vec(),
        value: b"v1".to_vec(),
    })
    .await
    .expect("seed checkpoint value");
    let checkpoint_seq = db.checkpoint_now().await.expect("checkpoint");
    for i in 0..8 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("tail:{i}").into_bytes(),
            value: vec![i as u8],
        })
        .await
        .expect("tail write");
    }
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let cp_view_seq = db
        .snapshot_probe(ConsistencyMode::AtCheckpoint)
        .await
        .expect("checkpoint snapshot");
    assert_eq!(cp_view_seq, checkpoint_seq);
}

#[tokio::test]
async fn migrations_are_idempotent_and_checksum_guarded() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    let migration = crate::migration::Migration {
        version: 1,
        name: "create-users".into(),
        project_id: "p".into(),
        scope_id: "app".into(),
        mutations: vec![Mutation::Ddl(DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: false,
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            columns: vec![
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
            primary_key: vec!["id".into()],
        })],
        down_mutations: Some(vec![Mutation::Ddl(DdlOperation::DropTable {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            if_exists: true,
        })]),
    };
    db.apply_migration(migration.clone())
        .await
        .expect("apply 1");
    db.apply_migration(migration).await.expect("apply 2");

    let changed = crate::migration::Migration {
        version: 1,
        name: "create-users-v2".into(),
        project_id: "p".into(),
        scope_id: "app".into(),
        mutations: vec![],
        down_mutations: None,
    };
    let err = db
        .apply_migration(changed)
        .await
        .expect_err("checksum guard");
    assert!(matches!(err, AedbError::IntegrityError { .. }));
}

#[tokio::test]
async fn run_migrations_reports_applied_and_skipped_versions() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let migration = crate::migration::Migration {
        version: 1,
        name: "create-users".into(),
        project_id: "p".into(),
        scope_id: "app".into(),
        mutations: vec![Mutation::Ddl(DdlOperation::CreateTable {
            owner_id: None,
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            if_not_exists: false,
            columns: vec![
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
            primary_key: vec!["id".into()],
        })],
        down_mutations: None,
    };

    let first = db
        .run_migrations(vec![migration.clone()])
        .await
        .expect("run first");
    assert_eq!(first.applied.len(), 1);
    assert!(first.skipped.is_empty());
    assert_eq!(first.current_version, 1);

    let second = db
        .run_migrations(vec![migration])
        .await
        .expect("run second");
    assert!(second.applied.is_empty());
    assert_eq!(second.skipped, vec![1]);
    assert_eq!(second.current_version, 1);
    assert_eq!(db.current_version("p", "app").await.expect("current"), 1);
}

#[tokio::test]
async fn concurrent_apply_migration_converges_idempotently() {
    let dir = tempdir().expect("temp");
    let db = Arc::new(AedbInstance::open(AedbConfig::default(), dir.path()).expect("open"));
    db.create_project("p").await.expect("project");

    let migration = crate::migration::Migration {
        version: 1,
        name: "create-users".into(),
        project_id: "p".into(),
        scope_id: "app".into(),
        mutations: vec![Mutation::Ddl(DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: false,
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            columns: vec![
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
            primary_key: vec!["id".into()],
        })],
        down_mutations: None,
    };

    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let db_a = Arc::clone(&db);
    let db_b = Arc::clone(&db);
    let migration_a = migration.clone();
    let migration_b = migration.clone();
    let barrier_a = Arc::clone(&barrier);
    let barrier_b = Arc::clone(&barrier);

    let task_a = tokio::spawn(async move {
        barrier_a.wait().await;
        db_a.apply_migration(migration_a).await
    });
    let task_b = tokio::spawn(async move {
        barrier_b.wait().await;
        db_b.apply_migration(migration_b).await
    });

    task_a
        .await
        .expect("task a join")
        .expect("task a apply migration");
    task_b
        .await
        .expect("task b join")
        .expect("task b apply migration");

    let applied = db
        .list_applied_migrations("p", "app")
        .await
        .expect("list applied");
    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].version, 1);
    assert!(
        db.table_exists("p", "app", "users")
            .await
            .expect("users table exists")
    );
}

struct RecordingTelemetryHook {
    queries: Arc<std::sync::Mutex<Vec<QueryTelemetryEvent>>>,
    commits: Arc<std::sync::Mutex<Vec<CommitTelemetryEvent>>>,
}

impl QueryCommitTelemetryHook for RecordingTelemetryHook {
    fn on_query(&self, event: &QueryTelemetryEvent) {
        self.queries
            .lock()
            .expect("query telemetry mutex poisoned")
            .push(event.clone());
    }

    fn on_commit(&self, event: &CommitTelemetryEvent) {
        self.commits
            .lock()
            .expect("commit telemetry mutex poisoned")
            .push(event.clone());
    }
}

struct EchoSqlAdapter;

impl ReadOnlySqlAdapter for EchoSqlAdapter {
    fn execute_read_only(
        &self,
        _project_id: &str,
        _scope_id: &str,
        sql: &str,
    ) -> Result<(Query, QueryOptions), QueryError> {
        if sql.trim() == "select id from items" {
            Ok((
                Query::select(&["id"]).from("items"),
                QueryOptions::default(),
            ))
        } else {
            Err(QueryError::InvalidQuery {
                reason: "unsupported sql".into(),
            })
        }
    }
}

struct LocalRemoteAdapter;

impl RemoteBackupAdapter for LocalRemoteAdapter {
    fn store_backup_dir(&self, uri: &str, backup_dir: &Path) -> Result<(), AedbError> {
        let target = PathBuf::from(uri);
        if target.exists() {
            fs::remove_dir_all(&target)?;
        }
        copy_dir_recursive(backup_dir, &target)
    }

    fn materialize_backup_chain(
        &self,
        uri: &str,
        scratch_dir: &Path,
    ) -> Result<Vec<PathBuf>, AedbError> {
        let source = PathBuf::from(uri);
        let target = scratch_dir.join("remote_chain_0");
        copy_dir_recursive(&source, &target)?;
        Ok(vec![target])
    }
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<(), AedbError> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let entry_path = entry.path();
        let out_path = dst.join(entry.file_name());
        let ty = entry.file_type()?;
        if ty.is_dir() {
            copy_dir_recursive(&entry_path, &out_path)?;
        } else if ty.is_file() {
            fs::copy(&entry_path, &out_path)?;
        }
    }
    Ok(())
}

async fn create_table(
    db: &AedbInstance,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    columns: Vec<ColumnDef>,
    primary_key: Vec<&str>,
) {
    db.commit_ddl(DdlOperation::CreateTable {
        owner_id: None,
        project_id: project_id.to_string(),
        scope_id: scope_id.to_string(),
        table_name: table_name.to_string(),
        if_not_exists: false,
        columns,
        primary_key: primary_key.into_iter().map(|v| v.to_string()).collect(),
    })
    .await
    .expect("create table");
}

#[tokio::test]
async fn read_tx_keeps_snapshot_consistency_across_queries() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");
    create_table(
        &db,
        "p",
        "app",
        "accounts",
        vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "balance".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        vec!["id"],
    )
    .await;
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1), Value::Integer(10)]),
    })
    .await
    .expect("seed");

    let tx = db
        .begin_read_tx(ConsistencyMode::AtLatest)
        .await
        .expect("read tx");
    let before = tx
        .query(
            "p",
            "app",
            Query::select(&["balance"])
                .from("accounts")
                .where_(Expr::Eq("id".into(), Value::Integer(1))),
        )
        .await
        .expect("before");
    assert_eq!(before.rows[0].values[0], Value::Integer(10));

    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1), Value::Integer(99)]),
    })
    .await
    .expect("update");

    let still_before = tx
        .query(
            "p",
            "app",
            Query::select(&["balance"])
                .from("accounts")
                .where_(Expr::Eq("id".into(), Value::Integer(1))),
        )
        .await
        .expect("stable read");
    assert_eq!(still_before.rows[0].values[0], Value::Integer(10));
}

#[tokio::test]
async fn list_batch_and_lookup_helpers_work() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");
    create_table(
        &db,
        "p",
        "app",
        "items",
        vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "user_id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        vec!["id"],
    )
    .await;
    create_table(
        &db,
        "p",
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
        vec!["id"],
    )
    .await;
    for i in 1..=5 {
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "items".into(),
            primary_key: vec![Value::Integer(i)],
            row: Row::from_values(vec![Value::Integer(i), Value::Integer(i)]),
        })
        .await
        .expect("seed item");
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(i)],
            row: Row::from_values(vec![Value::Integer(i), Value::Text(format!("u{i}").into())]),
        })
        .await
        .expect("seed user");
    }

    let batched = db
        .query_batch(
            "p",
            "app",
            vec![
                QueryBatchItem {
                    query: Query::select(&["id"]).from("items").limit(2),
                    options: QueryOptions::default(),
                },
                QueryBatchItem {
                    query: Query::select(&["id"]).from("items").limit(3),
                    options: QueryOptions::default(),
                },
            ],
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("query batch");
    assert_eq!(batched.len(), 2);
    assert_eq!(batched[0].snapshot_seq, batched[1].snapshot_seq);

    let first = db
        .list_with_total(
            "p",
            "app",
            Query::select(&["id"]).from("items"),
            None,
            None,
            2,
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("list");
    assert_eq!(first.total_count, 5);
    assert_eq!(first.rows.len(), 2);
    assert!(first.next_cursor.is_some());

    let via_offset = db
        .list_with_total(
            "p",
            "app",
            Query::select(&["id"]).from("items"),
            None,
            Some(3),
            2,
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("offset");
    assert_eq!(via_offset.total_count, 5);
    assert_eq!(via_offset.rows.len(), 2);

    let (source, hydrated) = db
        .lookup_then_hydrate(
            "p",
            "app",
            Query::select(&["user_id"]).from("items").limit(3),
            0,
            Query::select(&["id", "name"]).from("users"),
            "id",
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("lookup/hydrate");
    assert_eq!(source.rows.len(), 3);
    assert_eq!(hydrated.rows.len(), 3);
}

#[tokio::test]
async fn telemetry_sql_and_remote_adapter_paths_work() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");
    create_table(
        &db,
        "p",
        "app",
        "items",
        vec![ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Integer,
            nullable: false,
        }],
        vec!["id"],
    )
    .await;
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "items".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1)]),
    })
    .await
    .expect("seed");

    let hook = Arc::new(RecordingTelemetryHook {
        queries: Arc::new(std::sync::Mutex::new(Vec::new())),
        commits: Arc::new(std::sync::Mutex::new(Vec::new())),
    });
    db.add_telemetry_hook(hook.clone());

    let sql_result = db
        .query_sql_read_only(
            &EchoSqlAdapter,
            "p",
            "app",
            "select id from items",
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("sql adapter");
    assert_eq!(sql_result.rows.len(), 1);

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"remote:test".to_vec(),
        value: b"ok".to_vec(),
    })
    .await
    .expect("kv set");

    let remote_dir = tempdir().expect("remote");
    let uri = remote_dir.path().join("snapshot");
    let adapter = LocalRemoteAdapter;
    let uri_str = uri.to_string_lossy().to_string();
    db.backup_full_to_remote(&adapter, &uri_str)
        .await
        .expect("backup remote");

    let restored_dir = tempdir().expect("restored");
    AedbInstance::restore_from_remote(
        &adapter,
        &uri_str,
        restored_dir.path(),
        &AedbConfig::default(),
    )
    .expect("restore remote");
    let restored =
        AedbInstance::open(AedbConfig::default(), restored_dir.path()).expect("open restored");
    let restored_val = restored
        .kv_get_no_auth("p", "app", b"remote:test", ConsistencyMode::AtLatest)
        .await
        .expect("kv get");
    assert_eq!(restored_val.expect("entry").value, b"ok".to_vec());

    let query_count = hook.queries.lock().expect("query telemetry").len();
    let commit_count = hook.commits.lock().expect("commit telemetry").len();
    assert!(query_count >= 1);
    assert!(commit_count >= 1);
}

#[tokio::test]
async fn exists_and_explain_diagnostics_work() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");
    create_table(
        &db,
        "p",
        "app",
        "items",
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
        vec!["id"],
    )
    .await;
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "items".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1), Value::Text("a".into())]),
    })
    .await
    .expect("seed");

    let tx = db
        .begin_read_tx(ConsistencyMode::AtLatest)
        .await
        .expect("read tx");
    let exists = tx
        .exists(
            "p",
            "app",
            Query::select(&["id"])
                .from("items")
                .where_(Expr::Eq("id".into(), Value::Integer(1))),
        )
        .await
        .expect("exists");
    assert!(exists);

    let explain = tx
        .explain(
            "p",
            "app",
            Query::select(&["id"]).from("items"),
            QueryOptions::default(),
        )
        .expect("explain");
    assert!(explain.estimated_scan_rows >= 1);
    assert!(explain.stages.contains(&ExecutionStage::Scan));

    let with_diag = tx
        .query_with_diagnostics(
            "p",
            "app",
            Query::select(&["id"]).from("items"),
            QueryOptions {
                allow_full_scan: true,
                ..QueryOptions::default()
            },
        )
        .await
        .expect("diag query");
    assert_eq!(with_diag.result.rows.len(), 1);
    assert_eq!(with_diag.diagnostics.snapshot_seq, tx.snapshot_seq());
}

#[tokio::test]
async fn sql_transaction_plan_helpers_commit() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");
    create_table(
        &db,
        "p",
        "app",
        "items",
        vec![ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Integer,
            nullable: false,
        }],
        vec!["id"],
    )
    .await;

    let plan = db
        .plan_sql_transaction(
            ConsistencyMode::AtLatest,
            vec![Mutation::Upsert {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "items".into(),
                primary_key: vec![Value::Integer(10)],
                row: Row::from_values(vec![Value::Integer(10)]),
            }],
        )
        .await
        .expect("plan");
    db.commit_sql_transaction_plan(plan)
        .await
        .expect("commit plan");

    let exists = db
        .exists(
            "p",
            "app",
            Query::select(&["id"])
                .from("items")
                .where_(Expr::Eq("id".into(), Value::Integer(10))),
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("exists");
    assert!(exists);
}
