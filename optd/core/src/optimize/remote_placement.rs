//! Remote placement: wraps each maximal single-source subtree in a `Remote` marker.
//!
//! Runs last, after join ordering, so markers never split join groups. A subtree is pushable
//! when every table it scans is at the same remote source; see [`operator_source`]. Every
//! remote scan ends up under exactly one marker, and markers never nest.

use crate::{
    Catalog, Operator, OperatorData, OptimizerContext, QueryContext, Remote, SourceId,
    optimize::{OptimizeResult, Pass, PassResult, QueryPass},
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ExprData, Join, JoinType, MemoryCatalog, Output, PassManager, Relation, ScalarValue,
        TableRef,
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
}
