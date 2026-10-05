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
pub(crate) mod test_support {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion::catalog::TableProvider;
    use datafusion::common::{TableReference, not_impl_err};
    use datafusion::error::Result;
    use datafusion::execution::SendableRecordBatchStream;
    use datafusion::physical_plan::PhysicalExpr;
    use datafusion::sql::unparser::dialect::{DefaultDialect, Dialect};
    use datafusion_federation::FederatedTableProviderAdaptor;
    use datafusion_federation::sql::{SQLExecutor, SQLFederationProvider, SQLTableSource};

    /// A SQL executor at source `stub/db1` that never executes anything.
    pub(crate) struct StubExecutor;

    #[async_trait::async_trait]
    impl SQLExecutor for StubExecutor {
        fn name(&self) -> &str {
            "stub"
        }

        fn compute_context(&self) -> Option<String> {
            Some("db1".to_string())
        }

        fn dialect(&self) -> Arc<dyn Dialect> {
            Arc::new(DefaultDialect {})
        }

        fn execute(
            &self,
            _query: &str,
            _schema: SchemaRef,
            _filters: &[Arc<dyn PhysicalExpr>],
        ) -> Result<SendableRecordBatchStream> {
            not_impl_err!("stub executor")
        }

        async fn table_names(&self) -> Result<Vec<String>> {
            Ok(Vec::new())
        }

        async fn get_table_schema(&self, _table_name: &str) -> Result<SchemaRef> {
            not_impl_err!("stub executor")
        }
    }

    /// Returns a table at source `stub/db1`, optionally scannable through a local `fallback`.
    pub(crate) fn remote_table(
        name: &str,
        schema: SchemaRef,
        fallback: Option<Arc<dyn TableProvider>>,
    ) -> Arc<FederatedTableProviderAdaptor> {
        let provider = Arc::new(SQLFederationProvider::new(Arc::new(StubExecutor)));
        let source = Arc::new(SQLTableSource::new_with_schema(
            provider,
            TableReference::bare(name).into(),
            schema,
        ));
        Arc::new(match fallback {
            Some(fallback) => FederatedTableProviderAdaptor::new_with_provider(source, fallback),
            None => FederatedTableProviderAdaptor::new(source),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::remote_table;
    use super::*;
    use crate::to_df_logical::to_logical_plan;
    use crate::to_df_physical::to_physical_plan;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::SessionContext;
    use optd_core::{
        Catalog, ColumnData, ExprData, Join, JoinType, MemoryCatalog, OperatorData, Output,
        PlannedQuery, QueryContext, Remote, ScalarValue, TableRef,
    };

    /// Names of tables scanned by `plan`, not looking inside federated regions.
    fn scanned_tables(plan: &LogicalPlan) -> Vec<String> {
        let mut tables = Vec::new();
        plan.apply(|node| {
            if let LogicalPlan::TableScan(scan) = node {
                tables.push(scan.table_name.to_string());
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        tables.sort();
        tables
    }

    fn federated_regions(plan: &LogicalPlan) -> Vec<LogicalPlan> {
        let mut regions = Vec::new();
        plan.apply(|node| {
            if let LogicalPlan::Extension(extension) = node
                && let Some(federated) = extension.node.as_any().downcast_ref::<FederatedPlanNode>()
            {
                regions.push(federated.plan().clone());
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        regions
    }

    /// Returns `Output(Join(Remote[stub/db1](Join(a, c)), b))`, where `a` and `c` are remote
    /// tables registered in `session` and `b` is local.
    fn query_with_remote_region(session: &SessionContext) -> PlannedQuery {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let catalog = Arc::new(MemoryCatalog::new("datafusion", "public"));
        for name in ["a", "b", "c"] {
            let provider: Arc<dyn TableProvider> = if name == "b" {
                Arc::new(MemTable::try_new(schema.clone(), vec![vec![]]).unwrap())
            } else {
                remote_table(name, schema.clone(), None)
            };
            session.register_table(name, provider).unwrap();
            catalog
                .create_table(TableRef::bare(name), schema.clone(), None)
                .unwrap();
        }

        let mut query = QueryContext::new();
        // Qualified like the columns `from_df_logical` imports, so equal names stay distinct.
        let mut scan = |table: &str| {
            let id = ColumnData::with_qualifier("id", DataType::Int64, table).add(&mut query);
            query.add_scan(TableRef::bare(table), vec![id])
        };
        let (a, b, c) = (scan("a"), scan("b"), scan("c"));
        let on = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut query);
        let join = |outer, inner, query: &mut QueryContext| {
            OperatorData::Join(Join {
                join_type: JoinType::Inner,
                on,
                outer,
                inner,
            })
            .add(query)
        };
        let ac = join(a, c, &mut query);
        let remote = OperatorData::Remote(Remote {
            source: SourceId("stub/db1".into()),
            input: ac,
        })
        .add(&mut query);
        let root = join(remote, b, &mut query);
        let output = OperatorData::Output(Output { input: root }).add(&mut query);
        query.set_root(output);

        PlannedQuery::new(query, catalog)
    }

    #[tokio::test]
    async fn lowers_remote_marker_to_federated_node() {
        let session = SessionContext::new();
        let planned = query_with_remote_region(&session);

        let plan = to_logical_plan(&planned, &session).await.unwrap();

        assert_eq!(scanned_tables(&plan), vec!["b"]);
        let regions = federated_regions(&plan);
        assert_eq!(regions.len(), 1);
        assert_eq!(scanned_tables(&regions[0]), vec!["a", "c"]);
    }

    #[tokio::test]
    async fn lowers_remote_marker_to_remote_execution_node() {
        let session = SessionContext::new();
        let planned = query_with_remote_region(&session);

        let exec = to_physical_plan(&planned, &session).await.unwrap();

        let plan = displayable(exec.as_ref()).indent(true).to_string();
        assert_eq!(plan.matches("VirtualExecutionPlan").count(), 1, "{plan}");
        assert!(
            plan.contains(
                "VirtualExecutionPlan name=stub compute_context=db1 \
                 base_sql=SELECT a.id, c.id FROM a INNER JOIN c ON true"
            ),
            "{plan}"
        );
    }

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
