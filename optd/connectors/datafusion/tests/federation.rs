//! End-to-end federation tests: optd places remote regions, lowers them, and remote sources
//! execute the generated SQL.
//!
//! Each remote source is a separate DataFusion session behind a recording SQL executor. Tests
//! check results against plain DataFusion and check which tables each source's SQL scanned: a
//! remote scan that optd failed to push would be read through its local fallback copy and be
//! missing from the recorded SQL, and table `d` has no fallback at all.

use std::sync::{Arc, Mutex};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::common::TableReference;
use datafusion::error::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::unparser::dialect::{DefaultDialect, Dialect};
use datafusion_federation::sql::{SQLExecutor, SQLFederationProvider, SQLTableSource};
use datafusion_federation::{FederatedQueryPlanner, FederatedTableProviderAdaptor};
use futures::{StreamExt, TryStreamExt};
use optd_datafusion::config::OptdExtensionConfig;
use optd_datafusion::runner::{OptdRunner, RunnerOutput};

/// A remote source: a DataFusion session that records every SQL query it executes.
struct RecordingExecutor {
    context: &'static str,
    remote: SessionContext,
    queries: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl SQLExecutor for RecordingExecutor {
    fn name(&self) -> &str {
        "test"
    }

    fn compute_context(&self) -> Option<String> {
        Some(self.context.to_string())
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(DefaultDialect {})
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
        _filters: &[Arc<dyn PhysicalExpr>],
    ) -> Result<SendableRecordBatchStream> {
        self.queries.lock().unwrap().push(query.to_string());
        let remote = self.remote.clone();
        let query = query.to_string();
        let batches =
            futures::stream::once(async move { remote.sql(&query).await?.execute_stream().await })
                .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            batches.boxed(),
        )))
    }

    async fn table_names(&self) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    async fn get_table_schema(&self, table_name: &str) -> Result<SchemaRef> {
        Ok(self
            .remote
            .table(table_name)
            .await?
            .schema()
            .inner()
            .clone())
    }
}

/// Tables `a` and `c` live at source `db1`, `d` at `db2`, and `b` is local.
struct Federation {
    optd: OptdRunner,
    baseline: SessionContext,
    db1: Arc<RecordingExecutor>,
    db2: Arc<RecordingExecutor>,
}

/// Creates `name(id, value_column)` with `rows` in `session`.
async fn create_table(session: &SessionContext, name: &str, value_column: &str, rows: &str) {
    session
        .sql(&format!(
            "CREATE TABLE {name} AS SELECT * FROM (VALUES {rows}) AS t(id, {value_column})"
        ))
        .await
        .unwrap();
}

/// Builds the federation; `physical_planning` selects optd's execution path.
async fn federation(physical_planning: bool) -> Federation {
    let db1 = SessionContext::new();
    create_table(&db1, "a", "x", "(1, 10), (2, 20), (3, 30)").await;
    create_table(&db1, "c", "y", "(1, 100), (3, 300), (4, 400)").await;
    let db2 = SessionContext::new();
    create_table(&db2, "d", "w", "(1, 7), (3, 9)").await;
    let baseline = SessionContext::new();
    create_table(&baseline, "b", "z", "(1, 5), (2, 6), (3, 8)").await;

    let mut optd_config = OptdExtensionConfig::default();
    optd_config.physical_planning = physical_planning;
    // Only the logical path plans `FederatedPlanNode`s through the query planner, so the physical
    // path runs without it and would fail if it fell back to the logical path.
    let mut state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_option_extension(optd_config))
        .with_default_features();
    if !physical_planning {
        state = state.with_query_planner(Arc::new(FederatedQueryPlanner::new()));
    }
    let optd = SessionContext::new_with_state(state.build());
    optd.register_table("b", baseline.table_provider("b").await.unwrap())
        .unwrap();

    let executor = |context, remote| {
        Arc::new(RecordingExecutor {
            context,
            remote,
            queries: Mutex::new(Vec::new()),
        })
    };
    let federation = Federation {
        optd: OptdRunner::new(optd.clone()),
        baseline,
        db1: executor("db1", db1),
        db2: executor("db2", db2),
    };
    for (name, executor, fallback) in [
        ("a", &federation.db1, true),
        ("c", &federation.db1, true),
        ("d", &federation.db2, false),
    ] {
        let rows = executor.remote.table_provider(name).await.unwrap();
        federation
            .baseline
            .register_table(name, rows.clone())
            .unwrap();
        let source = Arc::new(SQLTableSource::new_with_schema(
            Arc::new(SQLFederationProvider::new(executor.clone())),
            TableReference::bare(name).into(),
            rows.schema(),
        ));
        // A fallback copy gives optd real statistics; without one the table gets defaults.
        let adaptor = if fallback {
            FederatedTableProviderAdaptor::new_with_provider(source, rows)
        } else {
            FederatedTableProviderAdaptor::new(source)
        };
        optd.register_table(name, Arc::new(adaptor)).unwrap();
    }
    federation
}

/// Formats rows as `v1|v2|...`, sorted, so results compare regardless of order and column names.
fn rows(batches: &[RecordBatch]) -> Vec<String> {
    let mut rows = Vec::new();
    for batch in batches {
        let formatters = batch
            .columns()
            .iter()
            .map(|column| ArrayFormatter::try_new(column.as_ref(), &FormatOptions::default()))
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let values = formatters
                .iter()
                .map(|formatter| formatter.value(row).to_string())
                .collect::<Vec<_>>();
            rows.push(values.join("|"));
        }
    }
    rows.sort();
    rows
}

impl Federation {
    /// Runs `sql` through optd and asserts it returns the same rows as plain DataFusion.
    async fn assert_matches_baseline(&self, sql: &str) {
        let RunnerOutput::Rows { batches, .. } = self.optd.execute_sql(sql).await.unwrap() else {
            panic!("query should return rows: {sql}");
        };
        let expected = self
            .baseline
            .sql(sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(rows(&batches), rows(&expected), "{sql}");
    }

    /// Asserts which tables each source's recorded SQL scanned.
    fn assert_scanned(&self, db1: &[&str], db2: &[&str]) {
        for (executor, expected) in [(&self.db1, db1), (&self.db2, db2)] {
            let queries = executor.queries.lock().unwrap();
            let scanned = ["a", "b", "c", "d"]
                .into_iter()
                .filter(|table| {
                    queries.iter().any(|sql| {
                        sql.contains(&format!("FROM {table}"))
                            || sql.contains(&format!("JOIN {table}"))
                    })
                })
                .collect::<Vec<_>>();
            assert_eq!(scanned, expected, "{}: {queries:?}", executor.context);
        }
    }
}

#[tokio::test]
async fn single_remote_scan_with_filter_runs_remotely() {
    let federation = federation(true).await;

    federation
        .assert_matches_baseline("SELECT id, x FROM a WHERE x > 15")
        .await;

    federation.assert_scanned(&["a"], &[]);
    assert!(federation.db1.queries.lock().unwrap()[0].contains("> 15"));
}

#[tokio::test]
async fn same_source_join_runs_remotely() {
    let federation = federation(true).await;

    federation
        .assert_matches_baseline("SELECT a.id, a.x, c.y FROM a JOIN c ON a.id = c.id")
        .await;

    federation.assert_scanned(&["a", "c"], &[]);
    assert_eq!(federation.db1.queries.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn mixed_source_join_keeps_each_table_at_its_source() {
    let federation = federation(true).await;

    federation
        .assert_matches_baseline(
            "SELECT a.id, b.z, c.y, d.w FROM a \
             JOIN c ON a.id = c.id \
             JOIN b ON b.id = a.id \
             JOIN d ON d.id = b.id",
        )
        .await;

    federation.assert_scanned(&["a", "c"], &["d"]);
}

#[tokio::test]
async fn logical_path_runs_regions_remotely() {
    let federation = federation(false).await;

    federation
        .assert_matches_baseline("SELECT a.id, a.x, c.y FROM a JOIN c ON a.id = c.id")
        .await;

    federation.assert_scanned(&["a", "c"], &[]);
}
