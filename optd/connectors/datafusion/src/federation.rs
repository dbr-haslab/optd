//! Connector-side federation support built on `datafusion-federation`.

use std::sync::{Arc, Mutex};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{TableReference, plan_err};
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, SessionState};
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, Projection};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion::prelude::SessionContext;
use datafusion::sql::unparser::dialect::{DefaultDialect, Dialect};
use datafusion_federation::sql::{
    SQLExecutor, SQLFederationPlanner, SQLFederationProvider, SQLTableSource,
};
use datafusion_federation::{
    FederatedPlanNode, FederatedTableProviderAdaptor, FederationPlanner, get_table_source,
};
use futures::{StreamExt, TryStreamExt};
use optd_core::SourceId;

/// Returns the remote source holding a table, or `None` for a local table.
///
/// A table is remote when its provider is a `FederatedTableProviderAdaptor` over a SQL source
/// whose executor reports a compute context. The source is `"{executor name}/{compute context}"`,
/// the identity `datafusion-federation` uses to group tables.
pub fn table_location(provider: &dyn TableProvider) -> Option<SourceId> {
    let adaptor = provider
        .as_any()
        .downcast_ref::<FederatedTableProviderAdaptor>()?;
    let source = adaptor.source.as_any().downcast_ref::<SQLTableSource>()?;
    executor_source(source.provider.executor.as_ref())
}

fn executor_source(executor: &dyn SQLExecutor) -> Option<SourceId> {
    let context = executor.compute_context()?;
    Some(SourceId(format!("{}/{context}", executor.name()).into()))
}

/// Wraps `region` in a `FederatedPlanNode` that runs it at `source` through its SQL executor.
///
/// Every table scanned in `region` must be a federated SQL table at `source`.
pub fn federate(region: LogicalPlan, source: &SourceId) -> Result<LogicalPlan> {
    let planner = Arc::new(SQLFederationPlanner::new(region_executor(&region, source)?));
    let region = project_aggregates(region)?;
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(FederatedPlanNode::new(region, planner)),
    }))
}

/// Plans `region` as an execution node that runs it at `source`, like [`federate`] followed by
/// `datafusion-federation`'s physical planning.
pub async fn federated_exec(
    region: LogicalPlan,
    source: &SourceId,
    state: &SessionState,
) -> Result<Arc<dyn ExecutionPlan>> {
    let planner = Arc::new(SQLFederationPlanner::new(region_executor(&region, source)?));
    let node = FederatedPlanNode::new(project_aggregates(region)?, planner.clone());
    planner.plan_federation(&node, state).await
}

/// Puts a projection directly above every aggregate that lacks one.
///
/// DataFusion's SQL planner always produces that shape, and its SQL unparser relies on it to
/// rebuild `SELECT ... GROUP BY`: without it, the unparser emits columns out of schema order or
/// nests the aggregation. optd's converted plans may omit the projection.
fn project_aggregates(region: LogicalPlan) -> Result<LogicalPlan> {
    let project = |plan: LogicalPlan| -> Result<LogicalPlan> {
        if !matches!(plan, LogicalPlan::Aggregate(_)) {
            return Ok(plan);
        }
        let columns = plan
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect();
        Ok(LogicalPlan::Projection(Projection::try_new(
            columns,
            Arc::new(plan),
        )?))
    };
    let region = region
        .transform_up(|node| {
            if matches!(node, LogicalPlan::Projection(_)) {
                return Ok(Transformed::no(node));
            }
            node.map_children(|child| project(child).map(Transformed::yes))
        })?
        .data;
    project(region)
}

/// Returns the executor for `source`, checking that every table `region` scans is at `source`.
fn region_executor(region: &LogicalPlan, source: &SourceId) -> Result<Arc<dyn SQLExecutor>> {
    let mut executor = None;
    region.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            let scan_executor = get_table_source(&scan.source)?.and_then(|table| {
                let sql = table.as_any().downcast_ref::<SQLTableSource>()?;
                Some(sql.provider.executor.clone())
            });
            match scan_executor {
                Some(scan_executor)
                    if executor_source(scan_executor.as_ref()).as_ref() == Some(source) =>
                {
                    executor = Some(scan_executor);
                }
                _ => {
                    return plan_err!(
                        "table {} is not at remote source {}",
                        scan.table_name,
                        source.0
                    );
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    let Some(executor) = executor else {
        return plan_err!("remote region at {} scans no tables", source.0);
    };
    Ok(executor)
}

/// A simulated remote database: a separate DataFusion session that executes the SQL optd sends.
///
/// Useful for experiments and tests without an external database. Its source is
/// `simulated/{name}`. Every query it receives is recorded and, with `echo`, printed to stderr.
pub struct SimulatedSource {
    name: String,
    session: SessionContext,
    echo: bool,
    queries: Mutex<Vec<String>>,
}

impl SimulatedSource {
    pub fn new(name: impl Into<String>, echo: bool) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            session: SessionContext::new(),
            echo,
            queries: Mutex::new(Vec::new()),
        })
    }

    /// Returns the SQL queries this source has executed, in order.
    pub fn queries(&self) -> Vec<String> {
        self.queries.lock().unwrap().clone()
    }

    /// Moves `table` from `local` into this source and re-registers it in `local` as remote.
    ///
    /// With `fallback`, `local` can still scan the original table, so optd collects statistics
    /// for it; without one, it gets the estimator's fallback defaults.
    pub async fn adopt(
        self: &Arc<Self>,
        local: &SessionContext,
        table: &str,
        fallback: bool,
    ) -> Result<()> {
        let rows = local.table_provider(table).await?;
        self.session.register_table(table, rows.clone())?;
        let source = Arc::new(SQLTableSource::new_with_schema(
            Arc::new(SQLFederationProvider::new(self.clone())),
            TableReference::bare(table).into(),
            rows.schema(),
        ));
        let remote = if fallback {
            FederatedTableProviderAdaptor::new_with_provider(source, rows)
        } else {
            FederatedTableProviderAdaptor::new(source)
        };
        local.deregister_table(table)?;
        local.register_table(table, Arc::new(remote))?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl SQLExecutor for SimulatedSource {
    fn name(&self) -> &str {
        "simulated"
    }

    fn compute_context(&self) -> Option<String> {
        Some(self.name.clone())
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
        if self.echo {
            eprintln!("-- simulated/{}: {query}", self.name);
        }
        self.queries.lock().unwrap().push(query.to_string());
        let session = self.session.clone();
        let query = query.to_string();
        let batches =
            futures::stream::once(async move { session.sql(&query).await?.execute_stream().await })
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
            .session
            .table(table_name)
            .await?
            .schema()
            .inner()
            .clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn rejects_region_with_table_at_another_source() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let session = SessionContext::new();
        let local = MemTable::try_new(schema, vec![vec![]]).unwrap();
        session.register_table("t", Arc::new(local)).unwrap();
        let region = session.table("t").await.unwrap().into_unoptimized_plan();

        let error = federate(region, &SourceId("stub/db1".into())).unwrap_err();

        assert!(
            error.to_string().contains("not at remote source"),
            "{error}"
        );
    }
}
