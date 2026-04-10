//! The location enforcer operator enforces execution at a specific location
//! and can be interpreted by the physical execution layer as a remoting boundary.

use crate::ir::{
    IRCommon, Operator,
    explain::Explain,
    macros::{define_node, impl_operator_conversion},
    properties::{OperatorProperties, ExecutionLocation},
};
use pretty_xmlish::Pretty;
use std::sync::Arc;

define_node!(
    /// Metadata:
    /// - location: The execution location that this enforcer enforces.
    /// Scalars: (none)
    EnforcerLocation, EnforcerLocationBorrowed {
        properties: OperatorProperties,
        metadata: EnforcerLocationMetadata {
            output_location: ExecutionLocation,
            input_location: ExecutionLocation,
        },
        inputs: {
            operators: [input],
            scalars: [],
        }
    }
);
impl_operator_conversion!(EnforcerLocation, EnforcerLocationBorrowed);

impl EnforcerLocation {
    pub fn new(output_location: ExecutionLocation, input_location: ExecutionLocation, input: Arc<Operator>) -> Self {
        Self {
            meta: EnforcerLocationMetadata { output_location, input_location },
            common: IRCommon::with_input_operators_only(Arc::new([input])),
        }
    }
}

impl Explain for EnforcerLocationBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> Pretty<'a> {
        let mut fields = Vec::with_capacity(2);
        fields.push((".output_location", Pretty::display(self.output_location())));
        fields.push((".input_location", Pretty::display(self.input_location())));

        let metadata = self.common.explain_operator_properties(ctx, option);
        fields.extend(metadata);
        let children = self.common.explain_input_operators(ctx, option);

        Pretty::simple_record("EnforcerLocation", fields, children)
    }
}
