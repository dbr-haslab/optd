//! Remote placement: wraps each maximal single-source subtree in a `Remote` marker.
//!
//! Runs last, after join ordering, so markers never split join groups. A subtree is pushable
//! when every table it scans is at the same remote source; see [`operator_source`]. Every
//! remote scan ends up under exactly one marker, and markers never nest.

use crate::{
    AnalysisContext, CardinalityEstimationV1, Catalog, CostModel, DefaultCostModel, Operator,
    OperatorData, OptimizerContext, QueryContext, Relation, Remote, SourceId,
    optimize::{OptimizeError, OptimizeResult, Pass, PassResult, QueryPass},
};

/// Returns the remote source that can execute `op` as a whole, given the sources of its inputs.
///
/// This is the single pushdown rule shared by placement and federated costing. A scan runs at
/// its table's catalog location. Any other operator runs at source `s` when it has inputs and all
/// of them run at `s`. `Output` and `Remote` always run locally, as do inputless operators other
/// than scans.
pub fn operator_source(
    op: &OperatorData,
    input_sources: &[Option<SourceId>],
    catalog: &dyn Catalog,
) -> Option<SourceId> {
    match op {
        OperatorData::Scan(scan) => catalog.table_by_ref(&scan.table).ok()?.location,
        OperatorData::Output(_) | OperatorData::Remote(_) => None,
        _ => {
            let (first, rest) = input_sources.split_first()?;
            let source = first.as_ref()?;
            rest.iter()
                .all(|input| input.as_ref() == Some(source))
                .then(|| source.clone())
        }
    }
}

/// Wraps each maximal single-source subtree in a `Remote` marker.
pub struct RemotePlacement;

impl Pass for RemotePlacement {
    fn name(&self) -> &'static str {
        "RemotePlacement"
    }
}

impl QueryPass for RemotePlacement {
    fn run(&mut self, ctx: &mut OptimizerContext) -> OptimizeResult<PassResult> {
        let Some(root) = ctx.query.root() else {
            return Ok(PassResult::Unchanged);
        };
        let catalog = ctx.analyses.catalog().clone();
        let (placed, source) = place(root, &mut ctx.query, catalog.as_ref());
        let placed = wrap(placed, source, &mut ctx.query);
        if placed == root {
            return Ok(PassResult::Unchanged);
        }
        ctx.rewrites.replace(root, placed);
        Ok(PassResult::Changed)
    }
}

/// Places markers below `op` and returns the rebuilt operator with the source it runs at.
///
/// ponytail: subqueries inside expressions are not visited; unnesting runs earlier and removes them.
fn place(
    op: Operator,
    query: &mut QueryContext,
    catalog: &dyn Catalog,
) -> (Operator, Option<SourceId>) {
    let data = query.operator(op).clone();
    if matches!(data, OperatorData::Remote(_)) {
        return (op, None);
    }

    let mut inputs = Vec::new();
    let data = data.map_inputs(|input| {
        let placed = place(input, query, catalog);
        inputs.push(placed.clone());
        placed.0
    });
    let input_sources: Vec<_> = inputs.iter().map(|(_, source)| source.clone()).collect();
    let source = operator_source(&data, &input_sources, catalog);

    // A remote input of a local operator is a maximal region.
    let data = if source.is_none() {
        let mut inputs = inputs.into_iter();
        data.map_inputs(|_| {
            let (input, source) = inputs.next().expect("one placed input per input");
            wrap(input, source, query)
        })
    } else {
        data
    };

    let placed = if data == *query.operator(op) {
        op
    } else {
        data.add(query)
    };
    (placed, source)
}

fn wrap(op: Operator, source: Option<SourceId>, query: &mut QueryContext) -> Operator {
    match source {
        Some(source) => OperatorData::Remote(Remote { source, input: op }).add(query),
        None => op,
    }
}

/// Cost of a subtree under federation.
#[derive(Debug, Clone, PartialEq)]
pub struct FederatedCost {
    /// The source and the cost accumulated there, when the whole subtree runs at one source.
    pub remote: Option<(SourceId, f64)>,
    /// Cost to have the subtree's rows available locally, including any transfer.
    pub delivered: f64,
}

/// Cost model that predicts remote placement and charges transfer where rows leave a source.
///
/// A local operator costs what [`DefaultCostModel`] says. An operator that [`operator_source`]
/// places at a source costs `remote_cost_factor` times that at the source, and bringing its rows
/// local costs `transfer_cost_per_row` each. Join ordering with this model keeps same-source
/// tables together when that transfers fewer rows. A `Remote` marker adds nothing: its input's
/// delivered cost already includes the transfer, so placement does not change a plan's cost.
#[derive(Debug, Clone, Copy)]
pub struct FederationCostModel {
    pub transfer_cost_per_row: f64,
    pub remote_cost_factor: f64,
}

impl Default for FederationCostModel {
    /// unit costs; calibrate against real sources when they are available.
    fn default() -> Self {
        Self {
            transfer_cost_per_row: 1.0,
            remote_cost_factor: 1.0,
        }
    }
}

impl CostModel for FederationCostModel {
    type Cost = FederatedCost;

    fn zero(&self) -> Self::Cost {
        FederatedCost {
            remote: None,
            delivered: 0.0,
        }
    }

    /// Adds delivered costs. The sum is not attributed to any source.
    fn add(&self, left: Self::Cost, right: Self::Cost) -> Self::Cost {
        FederatedCost {
            remote: None,
            delivered: left.delivered + right.delivered,
        }
    }

    fn is_better(&self, candidate: &Self::Cost, existing: &Self::Cost) -> bool {
        candidate.delivered.total_cmp(&existing.delivered).is_lt()
    }

    /// Returns the operator's local cost, ignoring placement.
    fn operator_cost(
        &self,
        op: Operator,
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
    ) -> OptimizeResult<Self::Cost> {
        Ok(FederatedCost {
            remote: None,
            delivered: DefaultCostModel.operator_cost(op, ctx, analyses)?,
        })
    }

    fn total_cost_from_children(
        &self,
        op: Operator,
        child_costs: &[Self::Cost],
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
    ) -> OptimizeResult<Self::Cost> {
        let data = ctx.operator(op);
        if data.inputs().len() != child_costs.len() {
            return Err(OptimizeError::PassError {
                pass: "FederationCostModel",
                message: format!(
                    "operator {op} has {} inputs but received {} child costs",
                    data.inputs().len(),
                    child_costs.len()
                ),
            });
        }
        let inputs_delivered: f64 = child_costs.iter().map(|cost| cost.delivered).sum();
        if matches!(data, OperatorData::Remote(_)) {
            return Ok(FederatedCost {
                remote: None,
                delivered: inputs_delivered,
            });
        }

        let local = DefaultCostModel.operator_cost(op, ctx, analyses)?;
        let input_sources: Vec<_> = child_costs
            .iter()
            .map(|cost| cost.remote.as_ref().map(|(source, _)| source.clone()))
            .collect();
        let catalog = analyses.catalog().clone();
        let Some(source) = operator_source(data, &input_sources, catalog.as_ref()) else {
            return Ok(FederatedCost {
                remote: None,
                // Same summation order as `DefaultCostModel`, so all-local plans cost the same
                // to the bit and keep their join order.
                delivered: child_costs
                    .iter()
                    .fold(local, |total, cost| total + cost.delivered),
            });
        };

        let inputs_remote: f64 = child_costs
            .iter()
            .filter_map(|cost| cost.remote.as_ref().map(|(_, remote)| remote))
            .sum();
        let remote = self.remote_cost_factor * local + inputs_remote;
        let rows = analyses
            .get::<CardinalityEstimationV1>(ctx, op)
            .map_err(|error| OptimizeError::PassError {
                pass: "FederationCostModel",
                message: error.to_string(),
            })?
            .rows
            .value;
        Ok(FederatedCost {
            remote: Some((source, remote)),
            delivered: remote + self.transfer_cost_per_row * rows,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BinaryOp, Column, ColumnStatistics, ExprData, Join, JoinOrdering, JoinType, MemoryCatalog,
        Output, PassManager, ScalarValue, TableRef, TableStatistics,
    };
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    /// Registers `tables` as `(name, location)` and returns a catalog and empty query.
    fn catalog_with(tables: &[(&str, Option<&str>)]) -> (Arc<MemoryCatalog>, QueryContext) {
        let catalog = Arc::new(MemoryCatalog::new("memory", "public"));
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        for (name, location) in tables {
            catalog
                .create_table(TableRef::bare(*name), schema.clone(), None)
                .unwrap();
            if let Some(location) = location {
                catalog
                    .set_table_location(TableRef::bare(*name), SourceId((*location).into()))
                    .unwrap();
            }
        }
        (catalog, QueryContext::new())
    }

    fn scan(query: &mut QueryContext, catalog: &MemoryCatalog, table: &str) -> Operator {
        query
            .add_scan_from_catalog(catalog, TableRef::bare(table))
            .unwrap()
    }

    fn join(query: &mut QueryContext, outer: Operator, inner: Operator) -> Operator {
        let on = ExprData::Literal(ScalarValue::Boolean(true)).add(query);
        OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on,
            outer,
            inner,
        })
        .add(query)
    }

    fn run(query: QueryContext, catalog: Arc<MemoryCatalog>) -> QueryContext {
        let mut opt = OptimizerContext::new(query, catalog);
        let mut pm = PassManager::with_max_iterations(2);
        pm.add_pass(RemotePlacement);
        pm.run(&mut opt).unwrap();
        opt.into_query()
    }

    /// Renders the plan as nested `Kind(children)` with remote sources and table names.
    fn shape(query: &QueryContext, op: Operator) -> String {
        let children = |inputs: Vec<Operator>| {
            inputs
                .into_iter()
                .map(|input| shape(query, input))
                .collect::<Vec<_>>()
                .join(", ")
        };
        match query.operator(op) {
            OperatorData::Scan(scan) => scan.table.table().to_string(),
            OperatorData::Remote(remote) => {
                format!(
                    "Remote[{}]({})",
                    remote.source.0,
                    shape(query, remote.input)
                )
            }
            data @ OperatorData::Join(_) => format!("Join({})", children(data.inputs())),
            data @ OperatorData::Output(_) => format!("Output({})", children(data.inputs())),
            other => panic!("unexpected operator in test plan: {other:?}"),
        }
    }

    #[test]
    fn wraps_maximal_single_source_regions() {
        let (catalog, mut query) = catalog_with(&[
            ("a", Some("pg/db1")),
            ("c", Some("pg/db1")),
            ("d", Some("pg/db2")),
            ("b", None),
        ]);
        let a = scan(&mut query, &catalog, "a");
        let c = scan(&mut query, &catalog, "c");
        let d = scan(&mut query, &catalog, "d");
        let b = scan(&mut query, &catalog, "b");
        let ac = join(&mut query, a, c);
        let acd = join(&mut query, ac, d);
        let acdb = join(&mut query, acd, b);
        let output = OperatorData::Output(Output { input: acdb }).add(&mut query);
        query.set_root(output);

        let placed = run(query, catalog);

        assert_eq!(
            shape(&placed, placed.root().unwrap()),
            "Output(Join(Join(Remote[pg/db1](Join(a, c)), Remote[pg/db2](d)), b))"
        );
    }

    #[test]
    fn single_source_query_is_wrapped_below_output() {
        let (catalog, mut query) = catalog_with(&[("a", Some("pg/db1")), ("c", Some("pg/db1"))]);
        let a = scan(&mut query, &catalog, "a");
        let c = scan(&mut query, &catalog, "c");
        let ac = join(&mut query, a, c);
        let output = OperatorData::Output(Output { input: ac }).add(&mut query);
        query.set_root(output);

        let placed = run(query, catalog);

        assert_eq!(
            shape(&placed, placed.root().unwrap()),
            "Output(Remote[pg/db1](Join(a, c)))"
        );
    }

    #[test]
    fn local_query_is_unchanged() {
        let (catalog, mut query) = catalog_with(&[("a", None), ("b", None)]);
        let a = scan(&mut query, &catalog, "a");
        let b = scan(&mut query, &catalog, "b");
        let ab = join(&mut query, a, b);
        let output = OperatorData::Output(Output { input: ab }).add(&mut query);
        query.set_root(output);

        let mut opt = OptimizerContext::new(query, catalog);
        assert_eq!(
            RemotePlacement.run(&mut opt).unwrap(),
            PassResult::Unchanged
        );
    }

    fn stats(rows: usize) -> TableStatistics {
        TableStatistics {
            row_count: Some(rows),
            size_bytes: None,
            column_statistics: [(
                "id".to_string(),
                ColumnStatistics {
                    lower_bound: None,
                    upper_bound: None,
                    frequency: Some(rows),
                    distinct: Some(rows),
                    distribution: None,
                },
            )]
            .into_iter()
            .collect(),
            constraints: Default::default(),
        }
    }

    fn id(query: &QueryContext, scan: Operator) -> Column {
        let OperatorData::Scan(scan) = query.operator(scan) else {
            panic!("expected a scan");
        };
        scan.columns[0]
    }

    fn eq_join(
        query: &mut QueryContext,
        (outer, left): (Operator, Column),
        (inner, right): (Operator, Column),
    ) -> Operator {
        let left = ExprData::ColumnRef(left).add(query);
        let right = ExprData::ColumnRef(right).add(query);
        let on = ExprData::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        }
        .add(query);
        OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on,
            outer,
            inner,
        })
        .add(query)
    }

    /// `(a ⋈ b) ⋈ c`, where `a` (10,000 rows) and `c` (1,000 rows) are at `pg/db1` and local
    /// `b` holds 10.
    ///
    /// Joining `b` first is the locally cheapest order, but it brings all 11,000 rows of `a` and
    /// `c` local; joining `a` and `c` at the source transfers an estimated 1,000.
    fn skewed_federated_query() -> OptimizerContext {
        let (catalog, mut query) =
            catalog_with(&[("a", Some("pg/db1")), ("b", None), ("c", Some("pg/db1"))]);
        for (name, rows) in [("a", 10_000), ("b", 10), ("c", 1_000)] {
            catalog
                .set_table_statistics(TableRef::bare(name), stats(rows))
                .unwrap();
        }
        let a = scan(&mut query, &catalog, "a");
        let b = scan(&mut query, &catalog, "b");
        let c = scan(&mut query, &catalog, "c");
        let (a_id, b_id, c_id) = (id(&query, a), id(&query, b), id(&query, c));
        let ab = eq_join(&mut query, (a, a_id), (b, b_id));
        let abc = eq_join(&mut query, (ab, a_id), (c, c_id));
        let output = OperatorData::Output(Output { input: abc }).add(&mut query);
        query.set_root(output);
        OptimizerContext::new(query, catalog)
    }

    /// Orders joins with `model`, places remote regions, and returns the plan shape.
    fn place_with(model: FederationCostModel) -> String {
        let mut opt = skewed_federated_query();
        let mut pm = PassManager::with_max_iterations(2);
        pm.add_pass(JoinOrdering::with_cost_model(model));
        pm.add_pass(RemotePlacement);
        pm.run(&mut opt).unwrap();
        shape(&opt.query, opt.query.root().unwrap())
    }

    #[test]
    fn expensive_transfer_joins_same_source_tables_at_the_source() {
        let shape = place_with(FederationCostModel {
            transfer_cost_per_row: 100.0,
            remote_cost_factor: 1.0,
        });

        assert!(
            shape.contains("Remote[pg/db1](Join(a, c))")
                || shape.contains("Remote[pg/db1](Join(c, a))"),
            "{shape}"
        );
    }

    #[test]
    fn cheap_transfer_keeps_the_locally_cheapest_order() {
        let shape = place_with(FederationCostModel {
            transfer_cost_per_row: 0.001,
            remote_cost_factor: 1.0,
        });

        assert!(shape.contains("Remote[pg/db1](a)"), "{shape}");
        assert!(shape.contains("Remote[pg/db1](c)"), "{shape}");
    }

    #[test]
    fn placement_does_not_change_federated_cost() {
        let model = FederationCostModel {
            transfer_cost_per_row: 100.0,
            remote_cost_factor: 1.0,
        };
        let mut opt = skewed_federated_query();
        let mut pm = PassManager::with_max_iterations(2);
        pm.add_pass(JoinOrdering::with_cost_model(model));
        pm.run(&mut opt).unwrap();
        let root = opt.query.root().unwrap();
        let ordered = model
            .total_cost(root, &opt.query, &mut opt.analyses)
            .unwrap();

        let mut pm = PassManager::with_max_iterations(2);
        pm.add_pass(RemotePlacement);
        pm.run(&mut opt).unwrap();
        let root = opt.query.root().unwrap();
        let placed = model
            .total_cost(root, &opt.query, &mut opt.analyses)
            .unwrap();

        assert!(shape(&opt.query, root).contains("Remote[pg/db1](Join("));
        assert_eq!(ordered, placed);
    }

    #[test]
    fn local_plans_cost_the_same_as_the_default_model() {
        let opt = skewed_federated_query();
        // The same tables and statistics, but local.
        let (catalog, _) = catalog_with(&[("a", None), ("b", None), ("c", None)]);
        for (name, rows) in [("a", 10_000), ("b", 10), ("c", 1_000)] {
            catalog
                .set_table_statistics(TableRef::bare(name), stats(rows))
                .unwrap();
        }
        let mut analyses = AnalysisContext::new(catalog);
        let root = opt.query.root().unwrap();

        let federated = FederationCostModel::default()
            .total_cost(root, &opt.query, &mut analyses)
            .unwrap();
        let default = DefaultCostModel
            .total_cost(root, &opt.query, &mut analyses)
            .unwrap();

        assert_eq!(federated.remote, None);
        assert_eq!(federated.delivered.to_bits(), default.to_bits());
    }
}
