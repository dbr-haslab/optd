//! This module defines the `ExecutionLocation` property for operators,
//! specifying which execution engine (DataFusion, PostgreSQL, etc.) should
//! execute the operator.

use std::sync::Arc;

use crate::error::Result;
use crate::ir::operator::*;
use crate::ir::{Operator, OperatorCategory, OperatorKind};

/// Specifies where an operator should be executed.
///
/// `Remote(cs)` carries a *connection-string* that uniquely identifies the remote
/// engine (e.g. the SQLite file path).  Two tables are on the same engine only
/// when their connection strings compare equal.  This allows the optimizer to
/// group same-engine tables into a single federated SQL statement while keeping
/// cross-engine operations local.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub enum Location {
    /// Default/unspecified location (usually means any location)
    Any,
    /// Local execution engine
    #[default]
    Local,
    /// Remote SQL execution engine identified by `cs` (connection string).
    Remote(Arc<str>),
}

impl std::fmt::Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Location::Any => write!(f, "Any"),
            Location::Local => write!(f, "Local"),
            Location::Remote(cs) => write!(f, "Remote({cs})"),
        }
    }
}

/// The execution location property for an operator.
#[derive(Clone, PartialEq, Eq, Hash, Default, Debug)]
pub struct ExecutionLocation(pub Location);

impl std::fmt::Display for ExecutionLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ExecutionLocation {
    pub fn new(location: Location) -> Self {
        Self(location)
    }

    pub fn any() -> Self {
        Self(Location::Any)
    }

    pub fn matches(&self, other: &ExecutionLocation) -> bool {
        self == other || self.0 == Location::Any || other.0 == Location::Any
    }

    /// Returns a reference to the underlying [`Location`].
    pub fn location(&self) -> &Location {
        &self.0
    }
}

impl crate::ir::properties::PropertyMarker for ExecutionLocation {
    type Output = Self;
}

/// Derives the execution location for different operator types.
/// Most operators inherit the location from their input.
/// Table scans (Get) have explicit locations based on catalog metadata.
fn satisfy_passthrough_location(
    _input: &Operator,
    location: &ExecutionLocation,
    _ctx: &crate::ir::context::IRContext,
) -> Result<Option<Arc<[ExecutionLocation]>>> {
    // Pass-through operators require their input to execute at the same location
    Ok(Some(Arc::from(vec![location.clone()])))
}

fn satisfy_join_location(
    _outer: &Operator,
    _inner: &Operator,
    location: &ExecutionLocation,
    _ctx: &crate::ir::context::IRContext,
) -> Result<Option<Arc<[ExecutionLocation]>>> {
    // For joins, both inputs can be at any location; the join itself executes
    // at the specified location. If locations differ, a remoting enforcer will be added.
    // For now, we say both inputs must be at the same location as the join.
    Ok(Some(Arc::from(vec![location.clone(), location.clone()])))
}

fn satisfy_scan_location(
    get: &GetBorrowed<'_>,
    location: &ExecutionLocation,
    ctx: &crate::ir::context::IRContext,
) -> Result<Option<Arc<[ExecutionLocation]>>> {
    let table_location = get.get_location(ctx);
    if (table_location?).matches(location) {
        Ok(Some(Arc::from(vec![])))
    } else {
        // If the scan's location doesn't match the required location, it cannot satisfy the property
        Ok(None)
    }
}

impl crate::ir::properties::TrySatisfy<ExecutionLocation> for Operator {
    fn try_satisfy(
        &self,
        location: &ExecutionLocation,
        ctx: &crate::ir::context::IRContext,
    ) -> Result<Option<Arc<[ExecutionLocation]>>> {
        let satisfied = match &self.kind {
            OperatorKind::Group(_) => None,
            OperatorKind::Get(meta) => {
                let get = Get::borrow_raw_parts(meta, &self.common);
                satisfy_scan_location(&get, location, ctx)?
            },
            OperatorKind::Join(meta) => {
                let join = Join::borrow_raw_parts(meta, &self.common);
                satisfy_join_location(join.outer(), join.inner(), location, ctx)?
            }
            OperatorKind::DependentJoin(meta) => {
                let join = DependentJoin::borrow_raw_parts(meta, &self.common);
                satisfy_join_location(join.outer(), join.inner(), location, ctx)?
            }
            OperatorKind::Project(meta) => {
                let project = Project::borrow_raw_parts(meta, &self.common);
                satisfy_passthrough_location(project.input(), location, ctx)?
            }
            OperatorKind::Select(meta) => {
                let select = Select::borrow_raw_parts(meta, &self.common);
                satisfy_passthrough_location(select.input(), location, ctx)?
            }
            OperatorKind::Limit(meta) => {
                let limit = Limit::borrow_raw_parts(meta, &self.common);
                satisfy_passthrough_location(limit.input(), location, ctx)?
                //Some(Arc::from(vec![ExecutionLocation(Location::Local)]))
            }
            OperatorKind::OrderBy(meta) => {
                let order_by = OrderBy::borrow_raw_parts(meta, &self.common);
                satisfy_passthrough_location(order_by.input(), location, ctx)?
            }
            OperatorKind::Aggregate(meta) => {
                let agg = Aggregate::borrow_raw_parts(meta, &self.common);
                satisfy_passthrough_location(agg.input(), location, ctx)?
                //Some(Arc::from(vec![ExecutionLocation(Location::Local)]))
            }
            OperatorKind::Remap(meta) => {
                let remap = Remap::borrow_raw_parts(meta, &self.common);
                satisfy_passthrough_location(remap.input(), location, ctx)?
            }
            OperatorKind::Subquery(_) => {
                assert_eq!(self.kind.category(), OperatorCategory::Logical);
                todo!("try_satisfy ExecutionLocation for LogicalSubquery")
            }
            OperatorKind::EnforcerSort(_meta) => {
                // EnforcerSort executes at the specified location
                Some(Arc::from(vec![location.clone()]))
            }
            OperatorKind::EnforcerLocation(meta) => {
                // EnforcerLocation enforces the location; only satisfies if it matches
                // demands the correct input
                (&meta.output_location == location)
                    .then(|| Arc::from(vec![meta.input_location.clone()]))
            }
        };
        Ok(satisfied)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{
        convert::IntoOperator, operator::join::JoinType, properties::TrySatisfy, scalar::*,
        table_ref::TableRef, test_utils::test_ctx_with_tables, ScalarValue,
    };

    fn remote(cs: &str) -> ExecutionLocation {
        ExecutionLocation::new(Location::Remote(Arc::from(cs)))
    }

    #[test]
    fn location_matches() {
        assert!(ExecutionLocation::any().matches(&ExecutionLocation::new(Location::Local)));
        assert!(ExecutionLocation::new(Location::Local)
            .matches(&ExecutionLocation::new(Location::Local)));
        assert!(!ExecutionLocation::new(Location::Local).matches(&remote("db")));
        assert!(!remote("db1").matches(&remote("db2")));
        assert!(remote("db").matches(&remote("db")));
    }

    #[test]
    fn scan_try_satisfy_location() -> crate::error::Result<()> {
        let ctx = test_ctx_with_tables(&[("t1", 2)])?;
        let t1 = ctx.logical_get(TableRef::bare("t1"), None)?.build();

        let location = ExecutionLocation::new(Location::Local);
        let res = t1.try_satisfy(&location, &ctx)?.unwrap();
        assert_eq!(res.len(), 0);

        let any_location = ExecutionLocation::any();
        let res = t1.try_satisfy(&any_location, &ctx)?.unwrap();
        assert_eq!(res.len(), 0);

        Ok(())
    }

    #[test]
    fn enforcer_location_try_satisfy() -> crate::error::Result<()> {
        let ctx = test_ctx_with_tables(&[("t1", 2)])?;
        let t1 = ctx.logical_get(TableRef::bare("t1"), None)?.build();

        // --- Remote(cs) enforcer ---
        let remote_enforcer =
            EnforcerLocation::new(remote("db1"), t1.clone()).into_operator();

        // Satisfies exact Remote(db1)
        let res = remote_enforcer.try_satisfy(&remote("db1"), &ctx)?.unwrap();
        assert_eq!(res.as_ref()[0], remote("db1"));

        // Satisfies Local (materialises data locally)
        let res = remote_enforcer
            .try_satisfy(&ExecutionLocation::new(Location::Local), &ctx)?
            .unwrap();
        assert_eq!(res.as_ref()[0], remote("db1"));

        // Satisfies Any
        let res = remote_enforcer
            .try_satisfy(&ExecutionLocation::any(), &ctx)?
            .unwrap();
        assert_eq!(res.as_ref()[0], remote("db1"));

        // Does not satisfy a different Remote engine
        assert_eq!(None, remote_enforcer.try_satisfy(&remote("db2"), &ctx)?);

        // --- Local (pass-through) enforcer ---
        let local_enforcer =
            EnforcerLocation::new(ExecutionLocation::new(Location::Local), t1.clone())
                .into_operator();

        // Satisfies Local
        let res = local_enforcer
            .try_satisfy(&ExecutionLocation::new(Location::Local), &ctx)?
            .unwrap();
        assert_eq!(res.as_ref()[0], ExecutionLocation::any());

        // Does not satisfy Remote
        assert_eq!(None, local_enforcer.try_satisfy(&remote("db1"), &ctx)?);

        Ok(())
    }
}
