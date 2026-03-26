//! This module defines the `Required` property for operators in the IR,
//! which specifies the required tuple ordering for the output of an operator.

use crate::ir::{
    Operator,
    context::IRContext,
    properties::{PropertyMarker, TupleOrdering, ExecutionLocation},
};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct Required {
    pub tuple_ordering: TupleOrdering,
    pub execution_location: ExecutionLocation,
}

impl std::fmt::Display for Required {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entry(&"tuple_ordering", &self.tuple_ordering)
            .entry(&"execution_location", &self.execution_location)
            .finish()
    }
}

impl PropertyMarker for Arc<Required> {
    type Output = Self;
}

impl crate::ir::properties::TrySatisfy<Arc<Required>> for Operator {
    fn try_satisfy(
        &self,
        property: &Arc<Required>,
        ctx: &IRContext,
    ) -> crate::error::Result<Option<std::sync::Arc<[Arc<Required>]>>> {
        // First check if we can satisfy tuple ordering
        let tuple_ordering_inputs = self.try_satisfy(&property.tuple_ordering, ctx)?;
        
        // Then check if we can satisfy execution location
        let location_inputs = self.try_satisfy(&property.execution_location, ctx)?;
        
        // Both must be satisfied
        match (tuple_ordering_inputs, location_inputs) {
            (Some(tuple_ordering_inputs), Some(location_inputs)) => {
                // Both properties can be satisfied
                let result = tuple_ordering_inputs
                    .iter()
                    .zip(location_inputs.iter())
                    .map(|(tuple_ord, location)| {
                        Arc::new(Required {
                            tuple_ordering: tuple_ord.clone(),
                            execution_location: location.clone(),
                        })
                    })
                    .collect();
                Ok(Some(result))
            }
            _ => Ok(None),
        }
    }
}
