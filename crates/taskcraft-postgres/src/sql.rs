//! The schema, its migrations, and the statements whose plans matter: the
//! claim on poll and the cleanup (change store-schema-and-indexes). The
//! `EXPLAIN` test checks these very texts.

// A process holds tasks in 'accepted', 'running' and 'retry_waiting' until it
// finishes them; 'succeeded', 'failed', 'panicked' and 'cancelled' are final
// and kept for the retention period.

/// Migration `i` brings the schema to version `i + 1`; the supported version
/// is their count. Applied in order, once, under [`SCHEMA_LOCK`].
pub(crate) const MIGRATIONS: &[&str] = &[
    // 1: the schema of 0.1 and 0.2, created without a version. On such a
    // base every statement is a no-op.
    "
CREATE TABLE IF NOT EXISTS taskcraft_tasks (
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
CREATE INDEX IF NOT EXISTS taskcraft_tasks_ready
    ON taskcraft_tasks (queue, state, next_delivery, created_at);
CREATE INDEX IF NOT EXISTS taskcraft_tasks_owner
    ON taskcraft_tasks (owner) WHERE owner IS NOT NULL;
CREATE TABLE IF NOT EXISTS taskcraft_processes (
    id      text        PRIMARY KEY,
    seen_at timestamptz NOT NULL
);
",
    // 2: indexes for the claim and the cleanup. No `IF NOT EXISTS`: applying
    // it twice fails.
    "
DROP INDEX IF EXISTS taskcraft_tasks_ready;
CREATE INDEX taskcraft_tasks_due
    ON taskcraft_tasks (queue, (COALESCE(next_delivery, created_at)), created_at)
    WHERE state IN ('queued', 'deferred');
CREATE INDEX taskcraft_tasks_lease
    ON taskcraft_tasks (queue, lease_until)
    WHERE state IN ('accepted', 'running', 'retry_waiting');
CREATE INDEX taskcraft_tasks_finished
    ON taskcraft_tasks (updated_at)
    WHERE state IN ('succeeded', 'failed', 'panicked', 'cancelled');
",
];

/// Serialises schema creation and migrations between processes starting
/// together.
pub(crate) const SCHEMA_LOCK: i64 = 0x7461_736b_6372_6166;

/// Takes the earliest due task of queue `$1` for owner `$2`; `$3` — leases
/// are on, `$4` — their duration in seconds. Uses `taskcraft_tasks_due`.
pub(crate) const CLAIM_DUE: &str = "
WITH next AS (
     SELECT queue, id, owner AS previous
       FROM taskcraft_tasks
      WHERE queue = $1
        AND state IN ('queued', 'deferred')
        AND COALESCE(next_delivery, created_at) <= now()
      ORDER BY COALESCE(next_delivery, created_at), created_at
      LIMIT 1
        FOR UPDATE SKIP LOCKED)
UPDATE taskcraft_tasks t
   SET state = 'accepted', owner = $2, next_delivery = NULL, updated_at = now(),
       lease_until = CASE WHEN $3 THEN now() + make_interval(secs => $4) END
  FROM next
 WHERE t.queue = next.queue AND t.id = next.id
RETURNING t.id,
          (t.task || jsonb_build_object('attempt', t.attempt, 'retries', t.retries))::text,
          next.previous";

/// Takes over the task of queue `$1` whose lease ran out first, for owner
/// `$2`; same parameters as [`CLAIM_DUE`]. Uses `taskcraft_tasks_lease`.
pub(crate) const CLAIM_EXPIRED: &str = "
WITH next AS (
     SELECT queue, id, owner AS previous
       FROM taskcraft_tasks
      WHERE queue = $1
        AND state IN ('accepted', 'running', 'retry_waiting')
        AND lease_until < now()
      ORDER BY lease_until
      LIMIT 1
        FOR UPDATE SKIP LOCKED)
UPDATE taskcraft_tasks t
   SET state = 'accepted', owner = $2, next_delivery = NULL, updated_at = now(),
       lease_until = CASE WHEN $3 THEN now() + make_interval(secs => $4) END
  FROM next
 WHERE t.queue = next.queue AND t.id = next.id
RETURNING t.id,
          (t.task || jsonb_build_object('attempt', t.attempt, 'retries', t.retries))::text,
          next.previous";

/// Deletes at most `$2` finished tasks older than `$1` seconds, oldest
/// first. Uses `taskcraft_tasks_finished`.
pub(crate) const CLEANUP_BATCH: &str = "
DELETE FROM taskcraft_tasks
 WHERE (queue, id) IN (
       SELECT queue, id
         FROM taskcraft_tasks
        WHERE state IN ('succeeded', 'failed', 'panicked', 'cancelled')
          AND updated_at < now() - make_interval(secs => $1)
        ORDER BY updated_at
        LIMIT $2)";

/// Criterion 4: on a million tasks the claim and the cleanup go by their
/// indexes, never through the whole table. Needs `TASKCRAFT_POSTGRES_URL`;
/// works in a schema of its own, dropped at the end.
#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{AssertSqlSafe, PgPool};

    use super::*;

    const MILLION: &str = "
INSERT INTO taskcraft_tasks
       (queue, id, task, state, next_delivery, owner, lease_until, created_at, updated_at)
SELECT 'q' || (n % 10), 't' || n, '{}'::jsonb,
       CASE WHEN n % 100 < 90 THEN 'succeeded'
            WHEN n % 100 < 95 THEN 'queued'
            WHEN n % 100 < 98 THEN 'deferred'
            ELSE 'running' END,
       CASE WHEN n % 100 BETWEEN 95 AND 97 THEN now() + interval '1 hour' END,
       CASE WHEN n % 100 >= 98 THEN 'p' END,
       CASE WHEN n % 100 >= 98 THEN now() + interval '1 minute' END,
       now() - (n % 1000) * interval '1 minute',
       now() - (n % 1000) * interval '1 hour'
  FROM generate_series(1, 1000000) AS n";

    async fn plan(pool: &PgPool, statement: &'static str, binds: Binds) -> String {
        let explain = AssertSqlSafe(format!("EXPLAIN {statement}"));
        let lines: Vec<String> = match binds {
            Binds::Claim => sqlx::query_scalar(explain)
                .bind("q3")
                .bind("p")
                .bind(true)
                .bind(60.0_f64)
                .fetch_all(pool)
                .await
                .unwrap(),
            Binds::Cleanup => sqlx::query_scalar(explain)
                .bind(3600.0_f64)
                .bind(1000_i64)
                .fetch_all(pool)
                .await
                .unwrap(),
        };
        lines.join("\n")
    }

    enum Binds {
        Claim,
        Cleanup,
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn claim_and_cleanup_use_their_indexes() {
        let Ok(url) = std::env::var("TASKCRAFT_POSTGRES_URL") else {
            assert!(
                std::env::var_os("TASKCRAFT_REQUIRE_SERVICES").is_none(),
                "TASKCRAFT_POSTGRES_URL is required"
            );
            return;
        };
        let schema = format!("taskcraft_explain_{}", uuid::Uuid::new_v4().simple());
        let admin = PgPool::connect(&url).await.unwrap();
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .unwrap();
        let path = schema.clone();
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .after_connect(move |conn, _| {
                let path = path.clone();
                Box::pin(async move {
                    sqlx::query(AssertSqlSafe(format!("SET search_path TO {path}")))
                        .execute(conn)
                        .await?;
                    Ok(())
                })
            })
            .connect(&url)
            .await
            .unwrap();
        let store = crate::PgStore::builder("p")
            .with_pool(pool.clone())
            .await
            .unwrap();
        sqlx::raw_sql(MILLION).execute(&pool).await.unwrap();
        sqlx::raw_sql("ANALYZE taskcraft_tasks")
            .execute(&pool)
            .await
            .unwrap();

        let checks = [
            (CLAIM_DUE, Binds::Claim, "taskcraft_tasks_due"),
            (CLAIM_EXPIRED, Binds::Claim, "taskcraft_tasks_lease"),
            (CLEANUP_BATCH, Binds::Cleanup, "taskcraft_tasks_finished"),
        ];
        for (statement, binds, index) in checks {
            let plan = plan(&pool, statement, binds).await;
            assert!(plan.contains(index), "{index} not used:\n{plan}");
            assert!(!plan.contains("Seq Scan"), "a full scan:\n{plan}");
        }

        drop(store);
        pool.close().await;
        sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
            .execute(&admin)
            .await
            .unwrap();
    }
}
