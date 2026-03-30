use std::sync::Arc;

use crate::ir::{
    convert::IntoOperator,
    operator::{EnforcerSort, EnforcerLocation},
    properties::{TupleOrdering, ExecutionLocation},
    rule::{OperatorPattern, Rule},
};

pub struct EnforceTupleOrderingRule {
    ordering: TupleOrdering,
    pattern: OperatorPattern,
}

impl Rule for EnforceTupleOrderingRule {
    fn name(&self) -> &'static str {
        "enforce_tuple_ordering"
    }

    fn pattern(&self) -> &crate::ir::rule::OperatorPattern {
        &self.pattern
    }

    fn transform(
        &self,
        operator: &crate::ir::Operator,
        _ctx: &crate::ir::IRContext,
    ) -> crate::error::Result<Vec<Arc<crate::ir::Operator>>> {
        if self.ordering.is_empty() {
            return Ok(vec![]);
        }
        Ok(vec![
            EnforcerSort::new(self.ordering.clone(), Arc::new(operator.clone())).into_operator(),
        ])
    }
}

impl EnforceTupleOrderingRule {
    pub fn new(ordering: TupleOrdering) -> Self {
        let pattern = OperatorPattern::with_top_matches(|_| true);
        Self { ordering, pattern }
    }
}

pub struct EnforceExecutionLocationRule {
    location: ExecutionLocation,
    pattern: OperatorPattern,
}

impl Rule for EnforceExecutionLocationRule {
    fn name(&self) -> &'static str {
        "enforce_execution_location"
    }

    fn pattern(&self) -> &crate::ir::rule::OperatorPattern {
        &self.pattern
    }

    fn transform(
        &self,
        operator: &crate::ir::Operator,
        ctx: &crate::ir::IRContext,
    ) -> crate::error::Result<Vec<Arc<crate::ir::Operator>>> {
        if self.location.0 != crate::ir::properties::Location::Local {
            return Ok(vec![]);
        }
        Ok(vec![
            EnforcerLocation::new(self.location.clone(), Arc::new(operator.clone())).into_operator(),
        ])
    }
}

impl EnforceExecutionLocationRule {
    pub fn new(location: ExecutionLocation) -> Self {
        let pattern = OperatorPattern::with_top_matches(|_| true);
        Self { location, pattern }
    }
}
