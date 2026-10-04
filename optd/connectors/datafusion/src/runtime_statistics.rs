//! Query-local runtime statistics for optd planning.
//!
//! This module collects table and column statistics from DataFusion and stores
//! them in an optd catalog. It does not collect selectivities directly:
//! cardinality analysis derives filter and join selectivities later from row
//! count, non-null frequency, NDV, min, and max.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::common::TableReference;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use optd_core::{Catalog, MemoryCatalog, TableRef, TableStatistics};

use crate::federation::table_location;
use crate::statistics::collect_table_statistics_for_table_ref;

type RuntimeStatisticsResult<T> = Result<T, String>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct StatsCacheKey {
    table: String,
    columns: Vec<String>,
}

#[derive(Debug, Clone)]
struct ReferencedScan {
    table: TableReference,
    columns: BTreeSet<String>,
}

/// Builds an optd catalog with runtime-collected stats for one DataFusion plan.
pub(crate) struct RuntimeStatisticsCatalogBuilder {
    session: SessionContext,
    cache: tokio::sync::Mutex<HashMap<StatsCacheKey, TableStatistics>>,
}

impl RuntimeStatisticsCatalogBuilder {
    pub(crate) fn new(session: SessionContext) -> Self {
        Self {
            session,
            cache: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    pub(crate) async fn build_for_plan(
        &self,
        plan: &LogicalPlan,
    ) -> RuntimeStatisticsResult<Arc<dyn Catalog>> {
        let scans = referenced_scan_columns(plan);
        let catalog = Arc::new(MemoryCatalog::new("datafusion", "public"));
        for scan in scans {
            let provider = self
                .session
                .table_provider(scan.table.clone())
                .await
                .map_err(|e| e.to_string())?;
            let table_ref = table_ref_from_df(&scan.table);
            catalog
                .create_table(table_ref.clone(), provider.schema(), None)
                .map_err(|e| e.to_string())?;
            let location = table_location(provider.as_ref());
            if let Some(location) = location.clone() {
                catalog
                    .set_table_location(table_ref.clone(), location)
                    .map_err(|e| e.to_string())?;
            }
            let statistics = match self
                .cached_table_statistics(&scan.table, provider.as_ref(), &scan.columns)
                .await
            {
                Ok(statistics) => statistics,
                // ponytail: remote statistics are stubbed. A remote table without a local fallback
                // provider cannot be scanned here, so it keeps the estimator's fallback defaults.
                Err(_) if location.is_some() => continue,
                Err(error) => return Err(error),
            };
            catalog
                .set_table_statistics(table_ref, statistics)
                .map_err(|e| e.to_string())?;
        }
        Ok(catalog)
    }

    #[cfg(test)]
    pub(crate) async fn cache_len(&self) -> usize {
        self.cache.lock().await.len()
    }

    async fn cached_table_statistics(
        &self,
        table: &TableReference,
        provider: &dyn TableProvider,
        columns: &BTreeSet<String>,
    ) -> RuntimeStatisticsResult<TableStatistics> {
        let columns = columns.iter().cloned().collect::<Vec<_>>();
        let key = StatsCacheKey {
            table: table.to_string(),
            columns: columns.clone(),
        };
        if let Some(statistics) = self.cache.lock().await.get(&key).cloned() {
            return Ok(statistics);
        }

        let schema = provider.schema();
        let available_columns = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<BTreeSet<_>>();
        let columns = columns
            .iter()
            .filter(|column| available_columns.contains(column.as_str()))
            .map(String::as_str)
            .collect::<Vec<_>>();
        let statistics = collect_table_statistics_for_table_ref(&self.session, table, &columns)
            .await
            .map_err(|e| e.to_string())?;
        self.cache.lock().await.insert(key, statistics.clone());
        Ok(statistics)
    }
}

fn referenced_scan_columns(plan: &LogicalPlan) -> Vec<ReferencedScan> {
    let mut scans = BTreeMap::<String, ReferencedScan>::new();
    plan.apply_with_subqueries(|plan| {
        collect_referenced_scan(plan, &mut scans);
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("collecting table scans cannot fail");
    scans.into_values().collect()
}

fn collect_referenced_scan(plan: &LogicalPlan, out: &mut BTreeMap<String, ReferencedScan>) {
    if let LogicalPlan::TableScan(scan) = plan {
        let key = scan.table_name.to_string();
        let entry = out.entry(key).or_insert_with(|| ReferencedScan {
            table: scan.table_name.clone(),
            columns: BTreeSet::new(),
        });
        for field in scan.projected_schema.fields() {
            entry.columns.insert(field.name().clone());
        }
    }
}

fn table_ref_from_df(table: &TableReference) -> TableRef {
    match (table.catalog(), table.schema()) {
        (Some(catalog), Some(schema)) => TableRef::full(catalog, schema, table.table()),
        (None, Some(schema)) => TableRef::partial(schema, table.table()),
        _ => TableRef::bare(table.table()),
    }
}

#[cfg(test)]
mod tests {
    use super::RuntimeStatisticsCatalogBuilder;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use optd_core::{ScalarValue, SourceId, TableRef};
    use std::sync::Arc;

    #[tokio::test]
    async fn runtime_statistics_catalog_populates_and_caches_scan_stats() {
        let session = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 2, 4])),
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    Some("b"),
                    None,
                ])),
            ],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        session.register_table("t", Arc::new(table)).unwrap();
        let builder = RuntimeStatisticsCatalogBuilder::new(session.clone());
        let plan = session
            .state()
            .create_logical_plan("SELECT id FROM t WHERE id >= 2")
            .await
            .unwrap();

        let catalog = builder.build_for_plan(&plan).await.unwrap();
        let stats = catalog
            .table_by_ref(&TableRef::bare("t"))
            .unwrap()
            .statistics
            .unwrap();

        assert_eq!(stats.row_count, Some(4));
        assert_eq!(stats.column_statistics["id"].distinct, Some(3));
        assert_eq!(
            stats.column_statistics["id"].lower_bound,
            Some(ScalarValue::Int64(1))
        );
        assert_eq!(builder.cache_len().await, 1);

        let _ = builder.build_for_plan(&plan).await.unwrap();
        assert_eq!(builder.cache_len().await, 1);
    }

    #[tokio::test]
    async fn runtime_statistics_catalog_uses_qualified_table_reference() {
        use datafusion::common::TableReference;

        let session = SessionContext::new();
        session
            .sql("CREATE SCHEMA s")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 20, 20]))],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        session
            .register_table(TableReference::partial("s", "t"), Arc::new(table))
            .unwrap();
        let builder = RuntimeStatisticsCatalogBuilder::new(session.clone());
        let plan = session
            .state()
            .create_logical_plan("SELECT id FROM s.t")
            .await
            .unwrap();

        let catalog = builder.build_for_plan(&plan).await.unwrap();
        let stats = catalog
            .table_by_ref(&TableRef::partial("s", "t"))
            .unwrap()
            .statistics
            .unwrap();

        assert_eq!(stats.row_count, Some(3));
        assert_eq!(stats.column_statistics["id"].distinct, Some(2));
    }

    #[tokio::test]
    async fn runtime_statistics_catalog_includes_scans_inside_subqueries() {
        let session = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let table = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
        for name in ["outer_t", "middle_t", "inner_t"] {
            session.register_table(name, table.clone()).unwrap();
        }
        let plan = session
            .state()
            .create_logical_plan(
                "SELECT id FROM outer_t WHERE id IN (\
                 SELECT id FROM middle_t WHERE id > (SELECT max(id) FROM inner_t)\
                 )",
            )
            .await
            .unwrap();
        let builder = RuntimeStatisticsCatalogBuilder::new(session);

        let catalog = builder.build_for_plan(&plan).await.unwrap();

        for name in ["outer_t", "middle_t", "inner_t"] {
            assert!(catalog.table_by_ref(&TableRef::bare(name)).is_ok());
        }
        assert_eq!(builder.cache_len().await, 3);
    }

    /// A SQL executor that reports a source identity but never executes anything.
    struct StubExecutor;

    #[async_trait::async_trait]
    impl datafusion_federation::sql::SQLExecutor for StubExecutor {
        fn name(&self) -> &str {
            "stub"
        }

        fn compute_context(&self) -> Option<String> {
            Some("db1".to_string())
        }

        fn dialect(&self) -> Arc<dyn datafusion::sql::unparser::dialect::Dialect> {
            Arc::new(datafusion::sql::unparser::dialect::DefaultDialect {})
        }

        fn execute(
            &self,
            _query: &str,
            _schema: datafusion::arrow::datatypes::SchemaRef,
            _filters: &[Arc<dyn datafusion::physical_plan::PhysicalExpr>],
        ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
            datafusion::common::not_impl_err!("stub executor")
        }

        async fn table_names(&self) -> datafusion::error::Result<Vec<String>> {
            Ok(Vec::new())
        }

        async fn get_table_schema(
            &self,
            _table_name: &str,
        ) -> datafusion::error::Result<datafusion::arrow::datatypes::SchemaRef> {
            datafusion::common::not_impl_err!("stub executor")
        }
    }

    #[tokio::test]
    async fn remote_tables_get_location_and_fallback_statistics() {
        use datafusion_federation::FederatedTableProviderAdaptor;
        use datafusion_federation::sql::{SQLFederationProvider, SQLTableSource};

        let session = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let rows =
            || Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch.clone()]]).unwrap());
        let provider = Arc::new(SQLFederationProvider::new(Arc::new(StubExecutor)));
        let remote_source = |table: &str| {
            Arc::new(SQLTableSource::new_with_schema(
                provider.clone(),
                datafusion::common::TableReference::bare(table).into(),
                schema.clone(),
            ))
        };
        session.register_table("local_t", rows()).unwrap();
        session
            .register_table(
                "remote_t",
                Arc::new(FederatedTableProviderAdaptor::new(remote_source(
                    "remote_t",
                ))),
            )
            .unwrap();
        session
            .register_table(
                "mirrored_t",
                Arc::new(FederatedTableProviderAdaptor::new_with_provider(
                    remote_source("mirrored_t"),
                    rows(),
                )),
            )
            .unwrap();
        let plan = session
            .state()
            .create_logical_plan("SELECT * FROM local_t, remote_t, mirrored_t")
            .await
            .unwrap();

        let catalog = RuntimeStatisticsCatalogBuilder::new(session.clone())
            .build_for_plan(&plan)
            .await
            .unwrap();
        let table = |name: &str| catalog.table_by_ref(&TableRef::bare(name)).unwrap();
        let db1 = Some(SourceId("stub/db1".into()));

        assert_eq!(table("local_t").location, None);
        assert_eq!(table("local_t").statistics.unwrap().row_count, Some(3));
        assert_eq!(table("remote_t").location, db1);
        assert!(table("remote_t").statistics.is_none());
        assert_eq!(table("mirrored_t").location, db1);
        assert_eq!(table("mirrored_t").statistics.unwrap().row_count, Some(3));
    }
}
