use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::executor::CommitResult;
use aedb::commit::validation::Mutation;
use aedb::config::AedbConfig;
use aedb::query::plan::{Expr, Order, Query};
use aedb::repository::{
    Page, PageRequest, RepositoryContext, RepositoryError, RowDecodeError, TryFromRow, bool_at,
    text_at, timestamp_at,
};
use tempfile::tempdir;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Session {
    id: String,
    user_id: String,
    expires_at: i64,
    revoked: bool,
}

impl TryFromRow for Session {
    fn try_from_row(row: Row) -> Result<Self, RowDecodeError> {
        Ok(Self {
            id: text_at(&row, 0, "id")?.to_string(),
            user_id: text_at(&row, 1, "user_id")?.to_string(),
            expires_at: timestamp_at(&row, 2, "expires_at")?,
            revoked: bool_at(&row, 3, "revoked")?,
        })
    }
}

struct SessionRepo<'a> {
    ctx: RepositoryContext<'a>,
}

impl<'a> SessionRepo<'a> {
    fn new(ctx: RepositoryContext<'a>) -> Self {
        Self { ctx }
    }

    async fn upsert_session(
        &self,
        session: &Session,
    ) -> Result<CommitResult, aedb::error::AedbError> {
        self.ctx
            .commit_checked(Mutation::Upsert {
                project_id: self.ctx.project_id().to_string(),
                scope_id: self.ctx.scope_id().to_string(),
                table_name: "sessions".to_string(),
                primary_key: vec![Value::Text(session.id.clone().into())],
                row: Row::from_values(vec![
                    Value::Text(session.id.clone().into()),
                    Value::Text(session.user_id.clone().into()),
                    Value::Timestamp(session.expires_at),
                    Value::Boolean(session.revoked),
                ]),
            })
            .await
    }

    async fn list_active_for_user(
        &self,
        user_id: &str,
        now_ts: i64,
        page: PageRequest,
    ) -> Result<Page<Session>, RepositoryError> {
        let query = Query::select(&["id", "user_id", "expires_at", "revoked"])
            .from("sessions")
            .where_(Expr::And(
                Box::new(Expr::Eq(
                    "user_id".into(),
                    Value::Text(user_id.to_string().into()),
                )),
                Box::new(Expr::And(
                    Box::new(Expr::Eq("revoked".into(), Value::Boolean(false))),
                    Box::new(Expr::Gt("expires_at".into(), Value::Timestamp(now_ts))),
                )),
            ))
            .order_by("expires_at", Order::Asc)
            .order_by("id", Order::Asc);

        self.ctx.query_page(query, page).await
    }
}

#[tokio::test]
async fn example_session_repo_usage() {
    let dir = tempdir().expect("tempdir");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    db.create_project("p").await.expect("create project");
    db.create_scope("p", "auth").await.expect("create scope");

    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        project_id: "p".into(),
        scope_id: "auth".into(),
        table_name: "sessions".into(),
        if_not_exists: false,
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "user_id".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "expires_at".into(),
                col_type: ColumnType::Timestamp,
                nullable: false,
            },
            ColumnDef {
                name: "revoked".into(),
                col_type: ColumnType::Boolean,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("create table");

    let repo = SessionRepo::new(RepositoryContext::new(&db, "p", "auth"));
    repo.upsert_session(&Session {
        id: "s1".into(),
        user_id: "u1".into(),
        expires_at: 200,
        revoked: false,
    })
    .await
    .expect("insert s1");
    repo.upsert_session(&Session {
        id: "s2".into(),
        user_id: "u1".into(),
        expires_at: 300,
        revoked: false,
    })
    .await
    .expect("insert s2");
    repo.upsert_session(&Session {
        id: "s3".into(),
        user_id: "u1".into(),
        expires_at: 150,
        revoked: true,
    })
    .await
    .expect("insert revoked");

    let first = repo
        .list_active_for_user("u1", 100, PageRequest::new(1))
        .await
        .expect("first page");
    assert_eq!(first.items.len(), 1);
    assert_eq!(first.items[0].id, "s1");
    assert!(first.next_cursor.is_some());

    let second = repo
        .list_active_for_user(
            "u1",
            100,
            PageRequest::new(1).with_cursor(first.next_cursor.expect("cursor")),
        )
        .await
        .expect("second page");
    assert_eq!(second.items.len(), 1);
    assert_eq!(second.items[0].id, "s2");
    assert!(second.next_cursor.is_none());
}
