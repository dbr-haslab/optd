//! Connector-side federation support built on `datafusion-federation`.

use datafusion::catalog::TableProvider;
use datafusion_federation::FederatedTableProviderAdaptor;
use datafusion_federation::sql::SQLTableSource;
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
    let executor = &source.provider.executor;
    let context = executor.compute_context()?;
    Some(SourceId(format!("{}/{context}", executor.name()).into()))
}
