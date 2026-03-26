use crate::ir::{
    cost::{Cost, CostModel},
    operator::*,
    properties::Location,
    scalar::List,
};
use snafu::whatever;

pub struct MagicCostModel;

impl MagicCostModel {
    const MAGIC_COMPUTATION_FACTOR: f64 = 5f64;
    // Location remoting costs
    const REMOTING_CONSTANT_PENALTY: f64 = 10000.0;
    const REMOTING_FACTOR: f64 = 0.5;
    const FORBIDDEN_REMOTING_COST: f64 = 1e10; // Very large cost for forbidden combinations

    fn sort_cost(input_card: crate::ir::properties::Cardinality) -> Cost {
        Cost::UNIT * input_card.as_f64() * input_card.as_f64().ln_1p().max(1.0)
    }

    fn scan_cost(card: crate::ir::properties::Cardinality) -> Cost {
        Cost::UNIT * card * 2f64
    }

    fn nested_loop_cost(
        outer_card: crate::ir::properties::Cardinality,
        inner_card: crate::ir::properties::Cardinality,
    ) -> Cost {
        outer_card.as_f64()
            * inner_card.as_f64()
            * MagicCostModel::MAGIC_COMPUTATION_FACTOR
            * Cost::UNIT
            + outer_card * Cost::UNIT
    }

    fn filter_cost(input_card: crate::ir::properties::Cardinality) -> Cost {
        input_card.as_f64() * Self::MAGIC_COMPUTATION_FACTOR * Cost::UNIT
    }

    fn project_cost(input_card: crate::ir::properties::Cardinality) -> Cost {
        input_card.as_f64() * Self::MAGIC_COMPUTATION_FACTOR * Cost::UNIT
    }

    fn hash_aggregate_cost(
        input_card: crate::ir::properties::Cardinality,
        num_exprs: usize,
    ) -> Cost {
        input_card.as_f64() * num_exprs as f64 * Self::MAGIC_COMPUTATION_FACTOR * Cost::UNIT
    }

    /// Calculates the cost of remoting data from source location to target location.
    /// Returns a finite cost for PostgreSQL/DuckDB -> DataFusion transfers,
    /// and infinite cost for all other combinations.
    fn location_remoting_cost(
        source_location: Location,
        target_location: Location,
        input_card: crate::ir::properties::Cardinality,
    ) -> Cost {
        use Location::*;
        match (source_location, target_location) {
            // Allow PostgreSQL -> DataFusion with cardinality-proportional cost
            (PostgreSQL, DataFusion) => {
                Cost::UNIT * (Self::REMOTING_CONSTANT_PENALTY
                    + input_card.as_f64() * Self::REMOTING_FACTOR)
            }
            // Allow DuckDB -> DataFusion with cardinality-proportional cost
            (DuckDB, DataFusion) => {
                Cost::UNIT * (Self::REMOTING_CONSTANT_PENALTY
                    + input_card.as_f64() * Self::REMOTING_FACTOR)
            }
            // All other combinations are forbidden
            _ => Cost::new(Self::FORBIDDEN_REMOTING_COST),
        }
    }

    /// Extracts the location of an input operator.
    /// For Get operators, queries the catalog for table location.
    /// For other operators, recursively finds the underlying Get operator.
    fn extract_input_location(
        input: &crate::ir::Operator,
        ctx: &crate::ir::IRContext,
    ) -> Location {
        use crate::ir::OperatorKind;
        match &input.kind {
            OperatorKind::Get(meta) => {
                // Get operators have catalog-stored locations
                match ctx.catalog.table(meta.data_source_id) {
                    Ok(table_meta) => table_meta.location.location(),
                    Err(_) => Location::Any, // Default if table not found
                }
            }
            // Pass-through operators: recursively extract from input
            OperatorKind::Project(meta) => {
                let project = Project::borrow_raw_parts(meta, &input.common);
                Self::extract_input_location(project.input(), ctx)
            }
            OperatorKind::Select(meta) => {
                let select = Select::borrow_raw_parts(meta, &input.common);
                Self::extract_input_location(select.input(), ctx)
            }
            OperatorKind::Limit(meta) => {
                let limit = Limit::borrow_raw_parts(meta, &input.common);
                Self::extract_input_location(limit.input(), ctx)
            }
            OperatorKind::OrderBy(meta) => {
                let order_by = OrderBy::borrow_raw_parts(meta, &input.common);
                Self::extract_input_location(order_by.input(), ctx)
            }
            OperatorKind::EnforcerSort(meta) => {
                let enforcer = EnforcerSort::borrow_raw_parts(meta, &input.common);
                Self::extract_input_location(enforcer.input(), ctx)
            }
            OperatorKind::Remap(meta) => {
                let remap = Remap::borrow_raw_parts(meta, &input.common);
                Self::extract_input_location(remap.input(), ctx)
            }
            // For other operators, default to Any
            _ => Location::Any,
        }
    }
}

impl CostModel for MagicCostModel {
    fn compute_operator_cost(
        &self,
        op: &crate::ir::Operator,
        ctx: &crate::ir::IRContext,
    ) -> crate::error::Result<crate::ir::cost::Cost> {
        use crate::ir::OperatorKind;
        match &op.kind {
            OperatorKind::Group(_) => whatever!("cannot compute cost for Group operator"),
            OperatorKind::Get(_) => {
                let card = op.cardinality(ctx);
                Ok(Self::scan_cost(card))
            }
            OperatorKind::Join(meta) => {
                let join = Join::borrow_raw_parts(meta, &op.common);
                match join.implementation() {
                    Some(JoinImplementation::Hash { build_side, .. }) => {
                        let (build_side, probe_side) = match build_side {
                            JoinSide::Outer => (join.outer(), join.inner()),
                            JoinSide::Inner => (join.inner(), join.outer()),
                        };
                        let build_card = build_side.cardinality(ctx);
                        let probe_card = probe_side.cardinality(ctx);
                        let cost = (build_card.as_f64() * 2. + probe_card.as_f64()) * Cost::UNIT;
                        Ok(cost)
                    }
                    _ => {
                        let outer_card = join.outer().cardinality(ctx);
                        let inner_card = join.inner().cardinality(ctx);
                        Ok(Self::nested_loop_cost(outer_card, inner_card))
                    }
                }
            }
            OperatorKind::DependentJoin(meta) => {
                let join = DependentJoin::borrow_raw_parts(meta, &op.common);
                let outer_card = join.outer().cardinality(ctx);
                let inner_card = join.inner().cardinality(ctx);
                Ok(Self::nested_loop_cost(outer_card, inner_card))
            }
            OperatorKind::Select(meta) => {
                let filter = Select::borrow_raw_parts(meta, &op.common);
                let input_card = filter.input().cardinality(ctx);
                Ok(Self::filter_cost(input_card))
            }
            OperatorKind::Limit(_meta) => Ok(Cost::ZERO),
            OperatorKind::Project(meta) => {
                let project = Project::borrow_raw_parts(meta, &op.common);
                let input_card = project.input().cardinality(ctx);
                Ok(Self::project_cost(input_card))
            }
            OperatorKind::Aggregate(meta) => match meta.implementation {
                None | Some(AggregateImplementation::Hash) => {
                    let agg = Aggregate::borrow_raw_parts(meta, &op.common);
                    let num_exprs = agg.exprs().borrow::<List>().members().len();
                    let input_card = agg.input().cardinality(ctx);
                    Ok(Self::hash_aggregate_cost(input_card, num_exprs))
                }
            },
            OperatorKind::OrderBy(meta) => {
                let order_by = OrderBy::borrow_raw_parts(meta, &op.common);
                let input_card = order_by.input().cardinality(ctx);
                Ok(Self::sort_cost(input_card))
            }
            OperatorKind::Subquery(_) => Ok(Cost::ZERO),
            OperatorKind::Remap(_) => Ok(Cost::ZERO),
            OperatorKind::EnforcerSort(_) => {
                let input_card = op.input_operators()[0].cardinality(ctx);
                Ok(Self::sort_cost(input_card))
            }
            OperatorKind::EnforcerLocation(meta) => {
                let enforcer = EnforcerLocation::borrow_raw_parts(meta, &op.common);
                let input = enforcer.input();
                let input_card = input.cardinality(ctx);
                let target_location = enforcer.location().location();
                let source_location = Self::extract_input_location(input, ctx);
                Ok(Self::location_remoting_cost(
                    source_location,
                    target_location,
                    input_card,
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::ir::{
        builder::{boolean, column_ref, int64},
        convert::{IntoOperator, IntoScalar},
        cost::Cost,
        operator::{join::JoinType, *},
        properties::{TupleOrdering, TupleOrderingDirection},
        scalar::Literal,
        table_ref::TableRef,
        test_utils::{test_col, test_ctx_with_tables},
        *,
    };

    #[test]
    fn sort_at_lower_cardinality_is_cheaper() -> crate::error::Result<()> {
        let ctx = test_ctx_with_tables(&[("t1", 2), ("t2", 2)])?;
        let m1 = ctx.logical_get(TableRef::bare("t1"), None)?.build();
        let m2 = ctx.logical_get(TableRef::bare("t2"), None)?.build();
        let t1_c0 = test_col(&ctx, "t1", "c0")?;

        let join_cond = Literal::new(ScalarValue::Boolean(Some(true))).into_scalar();

        let tuple_ordering = TupleOrdering::from_iter([(t1_c0, TupleOrderingDirection::Asc)]);

        // plan with sort on top.
        let op1 = EnforcerSort::new(
            tuple_ordering.clone(),
            Join::new(
                JoinType::Inner,
                m1.clone(),
                m2.clone(),
                join_cond.clone(),
                Some(JoinImplementation::nested_loop()),
            )
            .into_operator(),
        )
        .into_operator();

        // plan with sort passed down.
        let op2 = Join::new(
            JoinType::Inner,
            EnforcerSort::new(tuple_ordering, m1).into_operator(),
            m2,
            join_cond.clone(),
            Some(JoinImplementation::nested_loop()),
        )
        .into_operator();

        let op1_cost = ctx.cm.compute_total_cost(&op1, &ctx).unwrap();
        let op2_cost = ctx.cm.compute_total_cost(&op2, &ctx).unwrap();
        println!("cost1: {op1_cost:?}, cost2: {op2_cost:?}");
        assert!(op1_cost > op2_cost);
        Ok(())
    }

    #[test]
    fn logical_cost_matches_physical_counterparts() -> crate::error::Result<()> {
        let ctx = test_ctx_with_tables(&[("t1", 2), ("t2", 2)])?;
        let logical_get = ctx.logical_get(TableRef::bare("t1"), None)?.build();
        let t1_c0 = test_col(&ctx, "t1", "c0")?;
        let t1_c1 = test_col(&ctx, "t1", "c1")?;
        let physical_scan = ctx.table_scan(TableRef::bare("t1"), None)?.build();
        assert_eq!(
            ctx.cm.compute_operator_cost(&logical_get, &ctx)?,
            ctx.cm.compute_operator_cost(&physical_scan, &ctx)?
        );

        let outer = ctx.logical_get(TableRef::bare("t1"), None)?.build();
        let inner = ctx.logical_get(TableRef::bare("t2"), None)?.build();
        let join_cond = Literal::new(ScalarValue::Boolean(Some(true))).into_scalar();
        let logical_join = outer
            .clone()
            .with_ctx(&ctx)
            .logical_join(inner.clone(), join_cond.clone(), JoinType::Inner)
            .build();
        let physical_join = outer
            .clone()
            .with_ctx(&ctx)
            .nested_loop(inner.clone(), join_cond.clone(), JoinType::Inner)
            .build();
        assert_eq!(
            ctx.cm.compute_operator_cost(&logical_join, &ctx)?,
            ctx.cm.compute_operator_cost(&physical_join, &ctx)?
        );

        let logical_dep_join = DependentJoin::new(
            JoinType::Inner,
            outer.clone(),
            inner.clone(),
            join_cond.clone(),
        )
        .into_operator();
        assert_eq!(
            ctx.cm.compute_operator_cost(&logical_dep_join, &ctx)?,
            ctx.cm.compute_operator_cost(&physical_join, &ctx)?
        );

        let logical_select = outer
            .clone()
            .with_ctx(&ctx)
            .select(join_cond.clone())
            .build();
        let physical_filter = outer
            .clone()
            .with_ctx(&ctx)
            .select(join_cond.clone())
            .build();
        assert_eq!(
            ctx.cm.compute_operator_cost(&logical_select, &ctx)?,
            ctx.cm.compute_operator_cost(&physical_filter, &ctx)?
        );

        let logical_project = ctx
            .project(outer.clone(), [column_ref(t1_c0), column_ref(t1_c1)])?
            .build();
        let physical_project = ctx
            .project(outer.clone(), [column_ref(t1_c0), column_ref(t1_c1)])?
            .build();
        assert_eq!(
            ctx.cm.compute_operator_cost(&logical_project, &ctx)?,
            ctx.cm.compute_operator_cost(&physical_project, &ctx)?
        );

        let logical_agg = outer
            .clone()
            .with_ctx(&ctx)
            .logical_aggregate([column_ref(t1_c1)], [column_ref(t1_c0)])?
            .build();
        let physical_agg = outer
            .clone()
            .with_ctx(&ctx)
            .hash_aggregate([column_ref(t1_c1)], [column_ref(t1_c0)])?
            .build();
        assert_eq!(
            ctx.cm.compute_operator_cost(&logical_agg, &ctx)?,
            ctx.cm.compute_operator_cost(&physical_agg, &ctx)?
        );

        let ordering = TupleOrdering::from_iter([(t1_c0, TupleOrderingDirection::Asc)]);
        let logical_order_by = OrderBy::new(
            outer.clone(),
            vec![(column_ref(t1_c0), TupleOrderingDirection::Asc)],
        )
        .into_operator();
        let enforcer_sort = EnforcerSort::new(ordering, outer).into_operator();
        assert_eq!(
            ctx.cm.compute_operator_cost(&logical_order_by, &ctx)?,
            ctx.cm.compute_operator_cost(&enforcer_sort, &ctx)?
        );
        Ok(())
    }

    #[test]
    fn remaining_logical_operators_have_costs() -> crate::error::Result<()> {
        let ctx = test_ctx_with_tables(&[("t1", 2), ("t2", 1)])?;
        let input = ctx.logical_get(TableRef::bare("t1"), None)?.build();
        let filter_input = ctx.logical_get(TableRef::bare("t2"), None)?.build();

        let limit = Limit::new(input.clone(), int64(10), int64(20)).into_operator();
        let subquery = Subquery::new(input.clone()).into_operator();
        let remap = ctx.remap(input, TableRef::bare("t1_remap"))?.build();

        assert_eq!(
            ctx.cm.compute_operator_cost(&limit, &ctx).unwrap(),
            Cost::ZERO
        );
        assert_eq!(
            ctx.cm.compute_operator_cost(&subquery, &ctx).unwrap(),
            Cost::ZERO
        );
        assert_eq!(
            ctx.cm.compute_operator_cost(&remap, &ctx).unwrap(),
            Cost::ZERO
        );
        assert!(
            ctx.cm
                .compute_operator_cost(
                    &Select::new(filter_input, boolean(true)).into_operator(),
                    &ctx
                )
                .is_ok()
        );
        Ok(())
    }
}
