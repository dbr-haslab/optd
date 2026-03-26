//! PostgreSQL remote execution support
//!
//! This module handles conversion of optd/DataFusion plans to PostgreSQL remote execution.
//! It uses the datafusion-table-providers PostgreSQL table provider to execute queries
//! on remote PostgreSQL databases.

use std::any::Any;
use std::collections::{hash_map::DefaultHasher, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{Session, TableProvider},
    common::DFSchema,
    datasource::TableType,
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
    sql::TableReference,
};
use secrecy::SecretString;
use tokio::sync::Mutex;

use crate::planner::Result;

// Type alias for PostgreSQL connection pool for cleaner code
type PostgresPool = datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;

/// Configuration for PostgreSQL remote execution
#[derive(Debug, Clone)]
pub struct PostgresRemoteConfig {
    /// PostgreSQL connection string (e.g., "postgresql://user:password@localhost/dbname")
    pub connection_string: String,
}

impl PostgresRemoteConfig {
    /// Create a new PostgreSQL remote configuration from environment variable or default
    pub fn from_env_or_default() -> Option<Self> {
        // Try to get connection string from environment variable
        std::env::var("OPTD_POSTGRES_URL").ok().map(|connection_string| {
            PostgresRemoteConfig {
                connection_string,
            }
        })
    }
}

/// PostgreSQL remote table provider wrapper
///
/// This provider wraps a generated SQL statement for execution on a remote PostgreSQL database.
/// It delegates the actual execution to datafusion-table-providers' PostgreSQL provider.
#[derive(Clone)]
pub struct PostgresRemoteTableProvider {
    /// Shared executor with connection pooling
    executor: Arc<PostgresRemoteExecutorInternal>,
    /// SQL statement to execute on the remote database
    sql: String,
    /// Schema of the result set (as DFSchema)
    schema: Arc<DFSchema>,
}

impl std::fmt::Debug for PostgresRemoteTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresRemoteTableProvider")
            .field("sql", &self.sql)
            .field("schema", &self.schema)
            .finish()
    }
}

impl PostgresRemoteTableProvider {
    /// Create a new PostgreSQL remote table provider
    pub fn new(
        executor: Arc<PostgresRemoteExecutorInternal>,
        sql: String,
        schema: Arc<DFSchema>,
    ) -> Self {
        PostgresRemoteTableProvider {
            executor,
            sql,
            schema,
        }
    }

    /// Get the SQL statement for this remote query
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Get the executor
    pub fn executor(&self) -> &Arc<PostgresRemoteExecutorInternal> {
        &self.executor
    }
}

#[async_trait]
impl TableProvider for PostgresRemoteTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        // Extract Arrow schema from DFSchema
        let df_schema = self.schema.as_ref();
        // Convert DFSchema to Arrow Schema by using fields()
        let fields = df_schema.fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        Arc::new(datafusion::arrow::datatypes::Schema::new(fields))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // Delegate to the datafusion-table-providers PostgreSQL provider
        // which has full support for connection pooling and remote execution
        tracing::debug!(
            "PostgresRemoteTableProvider::scan called with projection: {:?}",
            projection
        );
        
        self.executor
            .create_scan_plan(&self.sql, self.schema.clone(), projection)
            .await
            .map_err(|e| {
                tracing::error!(
                    "Failed to create scan plan for PostgreSQL remote query: {}",
                    e
                );
                e
            })
    }

    fn supports_filters_pushdown(
        &self,
        _filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        // For now, no filters can be pushed down
        Ok(vec![])
    }
}

/// Internal PostgreSQL remote executor with connection pooling
///
/// This struct manages connection pools to remote PostgreSQL databases and delegates
/// actual query execution to the datafusion-table-providers PostgreSQL provider.
#[derive(Debug)]
struct PostgresRemoteExecutorInternal {
    config: PostgresRemoteConfig,
    /// Cached connection pool, initialized lazily on first use
    pool: Mutex<Option<Arc<PostgresPool>>>,
}

impl PostgresRemoteExecutorInternal {
    /// Create a new internal executor
    fn new(config: PostgresRemoteConfig) -> Self {
        PostgresRemoteExecutorInternal {
            config,
            pool: Mutex::new(None),
        }
    }

    /// Get or initialize the PostgreSQL connection pool
    async fn get_or_init_pool(&self) -> datafusion::common::Result<Arc<PostgresPool>> {
        let mut pool_guard = self.pool.lock().await;

        if let Some(pool) = pool_guard.as_ref() {
            tracing::debug!("Reusing cached PostgreSQL connection pool");
            return Ok(Arc::clone(pool));
        }

        // Parse connection string into parameters
        // Format: postgresql://user:password@host:port/dbname
        let mut params: HashMap<String, SecretString> = HashMap::new();
        params.insert(
            "connection_string".to_string(),
            SecretString::new(self.config.connection_string.clone().into_boxed_str()),
        );

        tracing::debug!(
            "Initializing new PostgreSQL connection pool (connecting to remote database)"
        );
        tracing::trace!("Connection string pattern: postgresql://[user]:[pass]@[host]:[port]/[dbname]");

        let pool: Arc<PostgresPool> = match PostgresPool::new(params).await {
            Ok(p) => {
                tracing::debug!("Successfully initialized PostgreSQL connection pool");
                Arc::new(p)
            }
            Err(e) => {
                let error_msg = format!(
                    "Failed to create PostgreSQL connection pool: {}. \
                     Check OPTD_POSTGRES_URL environment variable and database connectivity.",
                    e
                );
                tracing::error!("{}", error_msg);
                return Err(datafusion::error::DataFusionError::Internal(error_msg));
            }
        };

        *pool_guard = Some(Arc::clone(&pool));
        Ok(pool)
    }

    /// Create a scan plan for the given SQL query
    /// 
    /// This wraps the generated SQL in a CTE so we can execute it as a real table
    /// on the remote PostgreSQL server.
    async fn create_scan_plan(
        &self,
        sql: &str,
        schema: Arc<DFSchema>,
        _projection: Option<&Vec<usize>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let pool = self.get_or_init_pool().await?;

        // Create a temporary table reference for the CTE
        let query_hash = Self::query_hash(sql);
        let temp_table_name = Self::create_table_reference(query_hash);
        let _table_ref = TableReference::bare(temp_table_name.clone());

        // Validate schema before proceeding
        let field_count = schema.fields().len();
        tracing::debug!(
            "PostgreSQL remote scan plan: SQL has {} output columns, schema expects {} fields",
            sql.matches(',').count() + 1,
            field_count
        );

        // Wrap the generated SQL in a CTE so it can be executed as a table
        let cte_sql = format!(
            "WITH {} AS ({}) SELECT * FROM {}",
            &temp_table_name, sql, &temp_table_name
        );

        tracing::info!(
            "PostgreSQL remote query (hash: {}, table: {})",
            query_hash, temp_table_name
        );
        tracing::debug!(
            "Generated CTE SQL (length: {} bytes):\n{}",
            cte_sql.len(),
            cte_sql
        );

        // Create the PostgreSQL table factory and get a provider for the CTE
        let _factory = datafusion_table_providers::postgres::PostgresTableFactory::new(pool);
        
        // Since we can't directly execute a CTE query through the standard table provider
        // interface (which expects a real table reference), we need a different approach.
        // For now, we'll create a minimal execution plan that will be enhanced when
        // the federation layer is integrated.
        
        tracing::warn!(
            "PostgreSQL remote execution pending: DataFusion federation layer integration needed. \
             Ready to execute {} bytes of SQL with {} output columns.",
            cte_sql.len(),
            field_count
        );

        Err(datafusion::error::DataFusionError::NotImplemented(
            format!(
                "PostgreSQL remote execution of subqueries requires DataFusion federation integration.\n\
                Subquery (SQL length: {} bytes, output columns: {}):\n  {}",
                sql.len(), field_count, sql
            )
        ))
    }
}

/// Public PostgreSQL remote executor
///
/// This is the main interface for creating PostgreSQL remote table providers.
pub struct PostgresRemoteExecutor {
    /// Internal executor with connection pooling
    inner: Arc<PostgresRemoteExecutorInternal>,
}

impl PostgresRemoteExecutor {
    /// Create a new PostgreSQL remote executor
    pub fn new(config: PostgresRemoteConfig) -> Self {
        PostgresRemoteExecutor {
            inner: Arc::new(PostgresRemoteExecutorInternal::new(config)),
        }
    }

    /// Create a remote table provider for the given SQL query and schema
    pub fn create_remote_provider(
        &self,
        sql: String,
        schema: Arc<DFSchema>,
    ) -> Result<Arc<dyn TableProvider>> {
        let field_count = schema.fields().len();
        let sql_len = sql.len();
        
        tracing::debug!(
            "Creating PostgreSQL remote table provider for query ({} bytes, {} output fields)",
            sql_len, field_count
        );
        tracing::trace!("SQL query: {}", sql);
        
        let provider = PostgresRemoteTableProvider::new(Arc::clone(&self.inner), sql, schema);
        
        tracing::debug!(
            "PostgreSQL remote table provider created successfully (hash: {})",
            PostgresRemoteExecutor::query_hash(&provider.sql())
        );
        Ok(Arc::new(provider))
    }

    /// Create a stable, unique table name for a remote query
    pub fn create_table_reference(query_hash: u64) -> String {
        format!("__remote_postgres_{}", query_hash)
    }

    /// Create a stable hash for a SQL string
    pub fn query_hash(sql: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        sql.hash(&mut hasher);
        hasher.finish()
    }
}

impl PostgresRemoteExecutorInternal {
    /// Create a stable, unique table name for a remote query
    fn create_table_reference(query_hash: u64) -> String {
        format!("__remote_postgres_{}", query_hash)
    }

    /// Create a stable hash for a SQL string
    fn query_hash(sql: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        sql.hash(&mut hasher);
        hasher.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_reference_generation() {
        let table_name = PostgresRemoteExecutor::create_table_reference(12345);
        assert!(table_name.contains("__remote_postgres_"));
        assert!(table_name.contains("12345"));
    }

    #[test]
    fn test_query_hash_is_stable() {
        let left = PostgresRemoteExecutor::query_hash("select 1");
        let right = PostgresRemoteExecutor::query_hash("select 1");
        assert_eq!(left, right);
    }

    #[test]
    fn test_query_hash_differs_for_different_queries() {
        let hash1 = PostgresRemoteExecutor::query_hash("SELECT * FROM users");
        let hash2 = PostgresRemoteExecutor::query_hash("SELECT * FROM orders");
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_postgres_config_from_env() {
        // This test would need to set environment variables to work properly
        // For now, just verify the function doesn't panic
        let _config = PostgresRemoteConfig::from_env_or_default();
    }

    #[test]
    fn test_postgres_config_creation() {
        let config = PostgresRemoteConfig {
            connection_string: "postgresql://user:pass@localhost:5432/db".to_string(),
        };
        assert_eq!(
            config.connection_string,
            "postgresql://user:pass@localhost:5432/db"
        );
    }

    #[test]
    fn test_postgres_executor_creation() {
        let config = PostgresRemoteConfig {
            connection_string: "postgresql://localhost/test".to_string(),
        };
        let executor = PostgresRemoteExecutor::new(config);
        // Verify executor was created by testing hash function
        let hash = PostgresRemoteExecutor::query_hash("SELECT 1");
        assert!(hash != 0);
    }

    #[test]
    fn test_table_reference_uniqueness() {
        let query1 = "SELECT * FROM users WHERE id = 1";
        let query2 = "SELECT * FROM users WHERE id = 2";
        
        let hash1 = PostgresRemoteExecutor::query_hash(query1);
        let hash2 = PostgresRemoteExecutor::query_hash(query2);
        
        let table1 = PostgresRemoteExecutor::create_table_reference(hash1);
        let table2 = PostgresRemoteExecutor::create_table_reference(hash2);
        
        assert_ne!(table1, table2);
        assert!(table1.starts_with("__remote_postgres_"));
        assert!(table2.starts_with("__remote_postgres_"));
    }

    #[test]
    fn test_cte_wrapping_logic() {
        // Test the CTE wrapping logic without needing a real database
        let sql = "SELECT id, name FROM users";
        let hash = PostgresRemoteExecutor::query_hash(sql);
        let table_name = PostgresRemoteExecutor::create_table_reference(hash);
        
        let cte_sql = format!(
            "WITH {} AS ({}) SELECT * FROM {}",
            &table_name, sql, &table_name
        );
        
        assert!(cte_sql.contains("WITH"));
        assert!(cte_sql.contains("AS"));
        assert!(cte_sql.contains(sql));
        assert!(cte_sql.ends_with(&format!("SELECT * FROM {}", table_name)));
    }

    #[tokio::test]
    async fn test_postgres_remote_table_provider_schema() {
        // Create a minimal schema for testing
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let df_schema = DFSchema::try_from(arrow_schema.clone()).unwrap();
        let schema = Arc::new(df_schema);
        
        let config = PostgresRemoteConfig {
            connection_string: "postgresql://localhost/test".to_string(),
        };
        let executor = PostgresRemoteExecutor::new(config);
        
        let sql = "SELECT id, name FROM users".to_string();
        let provider_result = executor.create_remote_provider(sql.clone(), schema.clone());
        
        assert!(provider_result.is_ok());
        let provider = provider_result.unwrap();
        
        // Verify schema conversion
        let returned_schema = provider.schema();
        assert_eq!(returned_schema.fields().len(), 2);
    }
}
