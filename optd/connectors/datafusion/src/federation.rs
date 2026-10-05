//! Connector-side federation support built on `datafusion-federation`.

use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::common::plan_err;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::Result;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_federation::sql::{SQLExecutor, SQLFederationPlanner, SQLTableSource};
use datafusion_federation::{
    FederatedPlanNode, FederatedTableProviderAdaptor, FederationPlanner, get_table_source,
};
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
    let node = FederatedPlanNode::new(region, planner.clone());
    planner.plan_federation(&node, state).await
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
