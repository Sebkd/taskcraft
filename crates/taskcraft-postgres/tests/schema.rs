//! Schema versions and migrations (change store-schema-and-indexes,
//! criteria 1–3). Each test works in a PostgreSQL schema of its own, dropped
//! at the end. Set `TASKCRAFT_POSTGRES_URL`, or the tests are skipped.

// Test helpers may unwrap and panic (invariant 1.3.19 covers library code).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use sqlx::postgres::PgPoolOptions;
use sqlx::{AssertSqlSafe, PgPool};
use taskcraft_postgres::{PgStore, PgStoreError};

fn url() -> Option<String> {
    let url = std::env::var("TASKCRAFT_POSTGRES_URL").ok();
    if url.is_none() {
        assert!(
            std::env::var_os("TASKCRAFT_REQUIRE_SERVICES").is_none(),
            "TASKCRAFT_POSTGRES_URL is required"
        );
        eprintln!("TASKCRAFT_POSTGRES_URL is not set: skipped");
    }
    url
}

/// A schema of its own: a pool whose connections see only it.
struct Isolated {
    admin: PgPool,
    schema: String,
    pool: PgPool,
}

impl Isolated {
    async fn new(url: &str) -> Self {
        let schema = format!("taskcraft_test_{}", uuid::Uuid::new_v4().simple());
        let admin = PgPool::connect(url).await.unwrap();
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .unwrap();
        let path = schema.clone();
        let pool = PgPoolOptions::new()
            .after_connect(move |conn, _| {
                let path = path.clone();
                Box::pin(async move {
                    sqlx::query(AssertSqlSafe(format!("SET search_path TO {path}")))
                        .execute(conn)
                        .await?;
                    Ok(())
                })
            })
            .connect(url)
            .await
            .unwrap();
        Self {
            admin,
            schema,
            pool,
        }
    }

    async fn drop(self) {
        self.pool.close().await;
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA {} CASCADE",
            self.schema
        )))
        .execute(&self.admin)
        .await
        .unwrap();
    }
}

/// The schema of taskcraft-postgres 0.1 and 0.2, as they created it.
const SCHEMA_0_1: &str = "
CREATE TABLE taskcraft_tasks (
    queue            text        NOT NULL,
    id               text        NOT NULL,
    task             jsonb       NOT NULL,
    state            text        NOT NULL,
    attempt          integer     NOT NULL DEFAULT 0,
    retries          integer     NOT NULL DEFAULT 0,
    cancel_requested boolean     NOT NULL DEFAULT false,
    next_delivery    timestamptz,
    owner            text,
    lease_until      timestamptz,
    reason           jsonb,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (queue, id)
);
CREATE INDEX taskcraft_tasks_ready ON taskcraft_tasks (queue, state, next_delivery, created_at);
CREATE INDEX taskcraft_tasks_owner ON taskcraft_tasks (owner) WHERE owner IS NOT NULL;
CREATE TABLE taskcraft_processes (id text PRIMARY KEY, seen_at timestamptz NOT NULL);
INSERT INTO taskcraft_tasks (queue, id, task, state, attempt)
VALUES ('invoices', 'waiting', '{\"id\":\"waiting\",\"args\":1,\"metadata\":{}}', 'queued', 0),
       ('invoices', 'done', '{\"id\":\"done\",\"args\":2,\"metadata\":{}}', 'succeeded', 1),
       ('invoices', 'broken', '{\"id\":\"broken\",\"args\":3,\"metadata\":{}}', 'failed', 3);
";

/// Criteria 1 and 2: two processes of the new version start together on a
/// base of 0.1 — the migrations run once, tasks and history stay.
#[tokio::test(flavor = "multi_thread")]
async fn old_base_is_migrated_once_by_two_processes() {
    let Some(url) = url() else { return };
    let base = Isolated::new(&url).await;
    sqlx::raw_sql(SCHEMA_0_1).execute(&base.pool).await.unwrap();

    let (a, b) = tokio::join!(
        PgStore::builder("a").with_pool(base.pool.clone()),
        PgStore::builder("b").with_pool(base.pool.clone()),
    );
    let (a, b) = (a.unwrap(), b.unwrap());

    let version: i32 = sqlx::query_scalar("SELECT version FROM taskcraft_schema")
        .fetch_one(&base.pool)
        .await
        .unwrap();
    assert_eq!(version, 2);
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM taskcraft_schema")
        .fetch_one(&base.pool)
        .await
        .unwrap();
    assert_eq!(rows, 1);
    let indexes: Vec<String> = sqlx::query_scalar(
        "SELECT indexname::text FROM pg_indexes
          WHERE schemaname = current_schema() AND tablename = 'taskcraft_tasks'
          ORDER BY 1",
    )
    .fetch_all(&base.pool)
    .await
    .unwrap();
    assert_eq!(
        indexes,
        [
            "taskcraft_tasks_due",
            "taskcraft_tasks_finished",
            "taskcraft_tasks_lease",
            "taskcraft_tasks_owner",
            "taskcraft_tasks_pkey"
        ]
    );
    let kept: Vec<(String, String, i32)> =
        sqlx::query_as("SELECT id, state, attempt FROM taskcraft_tasks ORDER BY id")
            .fetch_all(&base.pool)
            .await
            .unwrap();
    assert_eq!(
        kept,
        [
            ("broken".to_owned(), "failed".to_owned(), 3),
            ("done".to_owned(), "succeeded".to_owned(), 1),
            ("waiting".to_owned(), "queued".to_owned(), 0),
        ]
    );
    drop((a, b));
    base.drop().await;
}

/// Criterion 3: a base migrated by a newer version refuses this one.
#[tokio::test(flavor = "multi_thread")]
async fn newer_schema_is_refused() {
    let Some(url) = url() else { return };
    let base = Isolated::new(&url).await;
    sqlx::raw_sql("CREATE TABLE taskcraft_schema (version integer NOT NULL); INSERT INTO taskcraft_schema VALUES (99);")
        .execute(&base.pool)
        .await
        .unwrap();
    let refused = PgStore::builder("p").with_pool(base.pool.clone()).await;
    let Err(error) = refused else {
        panic!("a newer schema was accepted");
    };
    assert!(
        matches!(
            error,
            PgStoreError::SchemaTooNew {
                found: 99,
                supported: 2
            }
        ),
        "{error:?}"
    );
    assert_eq!(
        error.to_string(),
        "task store schema is newer than this library: found=99, supported=2"
    );
    base.drop().await;
}
