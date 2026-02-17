use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::validation::Mutation;
use aedb::config::AedbConfig;
use aedb::permission::{CallerContext, Permission};
use aedb::query::error::QueryError;
use aedb::query::plan::{Order, Query, QueryOptions};
use aedb::repository::{
    PageRequest, RepositoryContext, RepositoryError, RowDecodeError, TryFromRow, bool_at, i64_at,
    text_at,
};
use tempfile::tempdir;

#[derive(Debug, Clone, PartialEq, Eq)]
struct UserRecord {
    id: String,
    name: String,
    age: i64,
}

impl TryFromRow for UserRecord {
    fn try_from_row(row: Row) -> Result<Self, RowDecodeError> {
        Ok(Self {
            id: text_at(&row, 0, "id")?.to_string(),
            name: text_at(&row, 1, "name")?.to_string(),
            age: i64_at(&row, 2, "age")?,
        })
    }
}

#[derive(Debug)]
struct BrokenRecord;

impl TryFromRow for BrokenRecord {
    fn try_from_row(row: Row) -> Result<Self, RowDecodeError> {
        let _ = bool_at(&row, 2, "age")?;
        Ok(Self)
    }
}

#[tokio::test]
async fn repository_context_commit_checked_and_page_query_work() {
    let dir = tempdir().expect("tempdir");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    db.create_project("p").await.expect("create project");
    db.create_scope("p", "app").await.expect("create scope");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        if_not_exists: false,
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "age".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("create table");

    let repo = RepositoryContext::new(&db, "p", "app");
    for (id, name, age) in [
        ("u1", "alice", 10_i64),
        ("u2", "bob", 20),
        ("u3", "carl", 30),
    ] {
        repo.commit_checked(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Text(id.into())],
            row: Row::from_values(vec![
                Value::Text(id.into()),
                Value::Text(name.into()),
                Value::Integer(age),
            ]),
        })
        .await
        .expect("upsert");
    }

    let query = Query::select(&["id", "name", "age"])
        .from("users")
        .order_by("id", Order::Asc);

    let page1 = repo
        .query_page_rows(query.clone(), PageRequest::new(2))
        .await
        .expect("page 1");
    assert_eq!(page1.items.len(), 2);
    assert!(page1.next_cursor.is_some());

    let page2 = repo
        .query_page_rows(
            query,
            PageRequest::new(2).with_cursor(page1.next_cursor.clone().expect("cursor present")),
        )
        .await
        .expect("page 2");
    assert_eq!(page2.items.len(), 1);
    assert!(page2.next_cursor.is_none());
}

#[tokio::test]
async fn repository_context_decoded_pages_surface_decode_errors() {
    let dir = tempdir().expect("tempdir");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    db.create_project("p").await.expect("create project");
    db.create_scope("p", "app").await.expect("create scope");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        if_not_exists: false,
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "age".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("create table");

    let repo = RepositoryContext::new(&db, "p", "app");
    repo.commit_checked(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Text("u1".into())],
        row: Row::from_values(vec![
            Value::Text("u1".into()),
            Value::Text("alice".into()),
            Value::Integer(42),
        ]),
    })
    .await
    .expect("upsert");

    let query = Query::select(&["id", "name", "age"])
        .from("users")
        .order_by("id", Order::Asc);

    let decoded = repo
        .query_page::<UserRecord>(query.clone(), PageRequest::new(10))
        .await
        .expect("decoded page");
    assert_eq!(
        decoded.items,
        vec![UserRecord {
            id: "u1".into(),
            name: "alice".into(),
            age: 42,
        }]
    );

    let err = repo
        .query_page::<BrokenRecord>(query, PageRequest::new(10))
        .await
        .expect_err("decode should fail");
    assert!(matches!(
        err,
        RepositoryError::Decode(RowDecodeError::TypeMismatch { .. })
    ));
}

#[tokio::test]
async fn repository_context_explain_respects_caller_permissions() {
    let dir = tempdir().expect("tempdir");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    db.create_project("p").await.expect("create project");
    db.create_scope("p", "app").await.expect("create scope");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        if_not_exists: false,
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Text,
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
    .expect("create table");

    let repo = RepositoryContext::new(&db, "p", "app").with_caller(CallerContext::new("reader"));
    let query = Query::select(&["id", "name"]).from("users");
    let denied = repo
        .explain(query.clone(), QueryOptions::default())
        .await
        .expect_err("explain should require table read permission");
    assert!(matches!(denied, QueryError::PermissionDenied { .. }));

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "reader".into(),
        permission: Permission::TableRead {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
        },
    }))
    .await
    .expect("grant read");

    repo.explain(query, QueryOptions::default())
        .await
        .expect("explain should pass once read permission is granted");
}
