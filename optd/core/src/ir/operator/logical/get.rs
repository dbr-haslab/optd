//! The get operator is a scan on some data source. Its metadata tracks whether
//! the operator is still logical or has been lowered to a physical scan.

use crate::ir::{
    IRCommon,
    catalog::DataSourceId,
    explain::Explain,
    macros::{define_node, impl_operator_conversion},
    properties::{OperatorProperties, ExecutionLocation},
};
use crate::error::Result as CoreResult;
use pretty_xmlish::Pretty;
use std::sync::Arc;

define_node!(
    /// Metadata:
    /// - source: The data source to scan.
    /// - first_column: The columns of the data source have monotonic indices
    ///                 starting from this column.
    /// - projections: The list of column indices to project from this table.
    /// Scalars: (none)
    Get, GetBorrowed {
        properties: OperatorProperties,
        metadata: GetMetadata {
            data_source_id: DataSourceId,
            table_index: i64,
            projections: Arc<[usize]>,
            implementation: Option<GetImplementation>,
        },
        inputs: {
            operators: [],
            scalars: [],
        }
    }
);
impl_operator_conversion!(Get, GetBorrowed);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum GetImplementation {
    #[default]
    TableScan,
}

impl Get {
    pub fn new(
        data_source_id: DataSourceId,
        table_index: i64,
        projections: Arc<[usize]>,
        implementation: Option<GetImplementation>,
    ) -> Self {
        Self {
            meta: GetMetadata {
                data_source_id,
                table_index,
                projections,
                implementation,
            },
            common: IRCommon::empty(),
        }
    }

    pub fn logical(
        data_source_id: DataSourceId,
        table_index: i64,
        projections: Arc<[usize]>,
    ) -> Self {
        Self::new(data_source_id, table_index, projections, None)
    }

    pub fn table_scan(
        data_source_id: DataSourceId,
        table_index: i64,
        projections: Arc<[usize]>,
    ) -> Self {
        Self::new(
            data_source_id,
            table_index,
            projections,
            Some(GetImplementation::TableScan),
        )
    }
}

impl GetBorrowed<'_> {
    /// Retrieves the execution location for this scan from the catalog.
    /// Returns the location stored in the table metadata, or ExecutionLocation::any() as default.
    pub fn get_location(&self, ctx: &crate::ir::IRContext) -> CoreResult<ExecutionLocation> {
        match ctx.catalog.table(self.data_source_id().clone()) {
            Ok(table_metadata) => Ok(ExecutionLocation(table_metadata.location.clone())),
            Err(_) => {
                // If table not found in catalog, default to Any
                Ok(ExecutionLocation::any())
            }
        }
    }
}

impl Explain for GetBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let mut fields = Vec::with_capacity(6);
        fields.push((".data_source_id", Pretty::display(&self.data_source_id().0)));
        fields.push((".table_index", Pretty::display(&self.table_index())));
        fields.push((".implementation", Pretty::debug(self.implementation())));
        fields.push((".location", Pretty::debug(&self.get_location(ctx).unwrap().location())));
        fields.extend(self.common.explain_operator_properties(ctx, option));
        Pretty::childless_record("Get", fields)
    }
}
