use std::sync::Arc;

use crate::{
    error::CatalogSnafu,
    ir::{
        convert::IntoOperator,
        operator::{EnforcerSort, EnforcerLocation},
        properties::{TupleOrdering, ExecutionLocation},
        rule::{OperatorPattern, Rule, RuleSet},
    },
};
use snafu::ResultExt;

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
        // Enforcers are only generated when the required location is Local.
        // We produce one Remote(cs) enforcer per known remote engine – child must run on
        //     that engine and results are materialised locally.
        // Cascades will pick the cheapest alternative.
        if self.location.0 != crate::ir::properties::Location::Local {
            return Ok(vec![]);
        }

        let remote_locations = ctx.catalog.list_remote_locations().context(CatalogSnafu)?;
        let enforcers = remote_locations.iter().map(|cs| {
            let loc = ExecutionLocation::new(crate::ir::properties::Location::Remote(cs.clone()));
            EnforcerLocation::new(self.location.clone(), loc, Arc::new(operator.clone())).into_operator()
        }).collect();

        Ok(enforcers)
    }
}

impl EnforceExecutionLocationRule {
    pub fn new(location: ExecutionLocation) -> Self {
        let pattern = OperatorPattern::with_top_matches(|_| true);
        Self { location, pattern }
    }
}
