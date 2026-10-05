//! End-to-end federation tests: optd places remote regions, lowers them, and remote sources
//! execute the generated SQL.
//!
//! Each remote source is a separate DataFusion session behind a recording SQL executor. Tests
//! check results against plain DataFusion and check which tables each source's SQL scanned: a
//! remote scan that optd failed to push would be read through its local fallback copy and be
//! missing from the recorded SQL, and table `d` has no fallback at all.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_federation::FederatedQueryPlanner;
use optd_datafusion::config::OptdExtensionConfig;
use optd_datafusion::federation::SimulatedSource;
use optd_datafusion::runner::{OptdRunner, RunnerOutput};

/// Tables `a` and `c` live at source `db1`, `d` at `db2`, and `b` is local.
struct Federation {
    optd: OptdRunner,
    baseline: SessionContext,
    db1: Arc<SimulatedSource>,
    db2: Arc<SimulatedSource>,
}

/// Small tables: every placement is visible in a few rows.
const SMALL: [(&str, &str); 4] = [
    (
        "a",
        "SELECT * FROM (VALUES (1, 10), (2, 20), (3, 30)) AS t(id, x)",
    ),
    (
        "c",
        "SELECT * FROM (VALUES (1, 100), (3, 300), (4, 400)) AS t(id, y)",
    ),
    ("d", "SELECT * FROM (VALUES (1, 7), (3, 9)) AS t(id, w)"),
    (
        "b",
        "SELECT * FROM (VALUES (1, 5), (2, 6), (3, 8)) AS t(id, z)",
    ),
];

/// Skewed sizes, as in the core federation cost model tests: `a` has 10,000 rows and `c` 1,000,
/// both at `db1`, and local `b` has 10. Joining `b` first is the locally cheapest order, while
/// joining `a` and `c` at `db1` transfers fewer rows.
const SKEWED: [(&str, &str); 4] = [
    ("a", "SELECT value AS id, value AS x FROM range(1, 10001)"),
    ("c", "SELECT value AS id, value AS y FROM range(1, 1001)"),
    ("d", "SELECT * FROM (VALUES (1, 7)) AS t(id, w)"),
    ("b", "SELECT value AS id, value AS z FROM range(1, 11)"),
];

/// Builds the federation over the small tables; `physical_planning` selects optd's execution path.
async fn federation(physical_planning: bool) -> Federation {
    let mut config = OptdExtensionConfig::default();
    config.physical_planning = physical_planning;
    federation_with(config, &SMALL).await
}

/// Builds the federation over `tables`, given as `(name, query)`, with optd settings `config`.
async fn federation_with(config: OptdExtensionConfig, tables: &[(&str, &str)]) -> Federation {
    // Only the logical path plans `FederatedPlanNode`s through the query planner, so the physical
    // path runs without it and would fail if it fell back to the logical path.
    let physical_planning = config.physical_planning;
    let mut state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_option_extension(config))
        .with_default_features();
    if !physical_planning {
        state = state.with_query_planner(Arc::new(FederatedQueryPlanner::new()));
    }
    let optd = SessionContext::new_with_state(state.build());
    let baseline = SessionContext::new();
    for (name, query) in tables {
        optd.sql(&format!("CREATE TABLE {name} AS {query}"))
            .await
            .unwrap();
        baseline
            .register_table(*name, optd.table_provider(*name).await.unwrap())
            .unwrap();
    }

    let db1 = SimulatedSource::new("db1", false);
    let db2 = SimulatedSource::new("db2", false);
    db1.adopt(&optd, "a", true).await.unwrap();
    db1.adopt(&optd, "c", true).await.unwrap();
    // No fallback copy: `d` gets default statistics and cannot be read locally.
    db2.adopt(&optd, "d", false).await.unwrap();

    Federation {
        optd: OptdRunner::new(optd),
        baseline,
        db1,
        db2,
    }
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
        for (source, expected) in [(&self.db1, db1), (&self.db2, db2)] {
            let queries = source.queries();
            let scanned = ["a", "b", "c", "d"]
                .into_iter()
                .filter(|table| {
                    queries.iter().any(|sql| {
                        sql.contains(&format!("FROM {table}"))
                            || sql.contains(&format!("JOIN {table}"))
                    })
                })
                .collect::<Vec<_>>();
            assert_eq!(scanned, expected, "{queries:?}");
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
    assert!(federation.db1.queries()[0].contains("> 15"));
}

#[tokio::test]
async fn same_source_join_runs_remotely() {
    let federation = federation(true).await;

    federation
        .assert_matches_baseline("SELECT a.id, a.x, c.y FROM a JOIN c ON a.id = c.id")
        .await;

    federation.assert_scanned(&["a", "c"], &[]);
    assert_eq!(federation.db1.queries().len(), 1);
}

#[tokio::test]
async fn same_source_aggregation_runs_remotely() {
    let federation = federation(true).await;

    federation
        .assert_matches_baseline(
            "SELECT a.x, count(*) AS matches FROM a JOIN c ON a.id = c.id \
             GROUP BY a.x ORDER BY a.x",
        )
        .await;

    federation.assert_scanned(&["a", "c"], &[]);
    assert_eq!(federation.db1.queries().len(), 1);
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

/// Runs `(a ⋈ b) ⋈ c` over the skewed tables with the given transfer cost and returns `db1`'s SQL.
async fn skewed_join_at_transfer_cost(transfer_cost_per_row: f64) -> Vec<String> {
    let mut config = OptdExtensionConfig::default();
    config.transfer_cost_per_row = transfer_cost_per_row;
    let federation = federation_with(config, &SKEWED).await;

    federation
        .assert_matches_baseline(
            "SELECT a.id, a.x, b.z, c.y FROM a JOIN b ON a.id = b.id JOIN c ON a.id = c.id",
        )
        .await;

    federation.assert_scanned(&["a", "c"], &[]);
    federation.db1.queries()
}

#[tokio::test]
async fn expensive_transfer_joins_same_source_tables_at_the_source() {
    let queries = skewed_join_at_transfer_cost(100.0).await;

    assert_eq!(queries.len(), 1, "{queries:?}");
    assert!(queries[0].contains("JOIN"), "{queries:?}");
}

#[tokio::test]
async fn cheap_transfer_fetches_same_source_tables_separately() {
    let queries = skewed_join_at_transfer_cost(0.001).await;

    assert_eq!(queries.len(), 2, "{queries:?}");
    assert!(
        queries.iter().all(|sql| !sql.contains("JOIN")),
        "{queries:?}"
    );
}

/// Records a limitation: the hypergraph has edges only for written predicates, not for ones implied
/// by equivalence classes. Written as `c.id = b.id`, the query is equivalent to the one in
/// `expensive_transfer_joins_same_source_tables_at_the_source`, but DPhyp never considers `a ⋈ c`,
/// so `db1` receives two scans instead of one join.
#[tokio::test]
async fn transitively_joined_same_source_tables_are_fetched_separately() {
    let mut config = OptdExtensionConfig::default();
    config.transfer_cost_per_row = 100.0;
    let federation = federation_with(config, &SKEWED).await;

    federation
        .assert_matches_baseline(
            "SELECT a.id, a.x, b.z, c.y FROM a JOIN b ON a.id = b.id JOIN c ON c.id = b.id",
        )
        .await;

    federation.assert_scanned(&["a", "c"], &[]);
    let queries = federation.db1.queries();
    assert_eq!(queries.len(), 2, "{queries:?}");
}
