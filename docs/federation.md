# Federation

Implemented parts of the federated join pushdown design. The full plan, including steps not yet
implemented, is in `docs/federation_plan.md`.

## Table Location

A remote source is identified by an opaque `SourceId`. Two tables are at the same source exactly
when their `SourceId`s are equal. Connectors choose the identity and map it back to the executor
for that source; `optd-core` never interprets it.

`TableMetadata::location` records where a table lives: `Some(source)` for a remote table and `None`
for a local one. A table is local until `Catalog::set_table_location` records a source for it.
Location is metadata, separate from statistics. A remote table's statistics use the same
`TableStatistics` contract as a local table's.

## `Remote` Marker

`OperatorData::Remote { source, input }` marks a subtree that runs entirely at `source`. Rows move
from the source to the local engine at this node. It is relationally transparent: analyses return
its input's values, and it creates no columns. The default cost model charges one unit per row as a
placeholder for transfer cost. Substrait export rejects it.

## Remote Placement

`RemotePlacement` runs last in the DataFusion pipeline, after join ordering, so markers never split
join groups. It wraps each maximal single-source subtree in a `Remote` marker. Every remote scan
ends up under exactly one marker, and markers never nest. A query without located tables is left
unchanged.

`operator_source` is the pushdown rule shared by placement and, later, federated costing. A scan
runs at its table's location. Any other operator runs at source `s` when it has inputs and all of
them run at `s`. `Output`, `Remote`, and inputless operators other than scans run locally. Every
operator is assumed pushable; per-dialect support checks are future work. Subqueries inside
expressions are not visited because unnesting removes them earlier.

## Federation Cost Model

`FederationCostModel` lets join ordering account for where tables live. Its cost,
`FederatedCost { remote, delivered }`, records the source and accumulated remote cost when a whole
subtree runs at one source, and the cost to have the subtree's rows available locally; plans compare
by `delivered`. It uses `operator_source`, the same rule as placement:

- A local operator costs what `DefaultCostModel` says, plus its inputs' delivered costs.
- An operator at source `s` costs `remote_cost_factor` times that at `s`, plus its inputs' remote
  costs, and its delivered cost adds `transfer_cost_per_row` for each output row.
- A `Remote` marker adds nothing, because its input's delivered cost already includes the transfer.
  Placement therefore does not change a plan's cost.

Because a single-source table set is pushable in every join order, DPhyp's one best plan per table
set stays correct with this cost. Whether pushdown wins depends on the parameters: the default model
charges hash joins `(left_rows * right_rows)^0.75`, wherever they run, so pushing a join pays off
when the rows it saves transferring outweigh its remote cost. Both parameters default to `1.0` and
are not calibrated; transfer is charged per row, not per byte.

The DataFusion pipeline always orders joins with this model, configured by the session settings
`optd.transfer_cost_per_row` and `optd.remote_cost_factor` (both `1.0` by default). With no remote
tables its costs equal `DefaultCostModel`'s to the bit, including summation order, so local queries
keep their plans.

The two settings describe the deployment, not optd's local operators, whose costs stay fixed in
`DefaultCostModel`. They are therefore relative to that model's units: a transfer cost of `100`
means moving a row costs as much as the default model charges for about 100 units of local work.
Until real sources are calibrated, treat them as an experiment dial rather than a measured value.

## DataFusion Connector

The connector depends on `datafusion-federation` 0.5.3, which requires DataFusion 53; upgrade both
together. `federation::table_location` treats a table as remote when its provider is a
`FederatedTableProviderAdaptor` over a SQL source whose executor reports a compute context. The
source is `"{executor name}/{compute context}"`, the identity `datafusion-federation` uses to group
tables.

When building the per-query catalog, the runtime statistics builder records each table's location.
Remote statistics are stubbed: if statistics collection fails for a remote table (an adaptor
without a local fallback provider cannot be scanned), the table is registered without statistics
and cardinality estimation uses its fallback defaults. An adaptor created with
`FederatedTableProviderAdaptor::new_with_provider` is scanned through its fallback provider, which
is how tests give remote tables real statistics.

## Lowering

`to_df_logical` converts a `Remote` marker's subtree as usual and wraps it with
`federation::federate` in a `datafusion-federation` `FederatedPlanNode`, planned by a
`SQLFederationPlanner` over the source's executor. The executor comes from the region's own table
scans, and every scan must be a federated SQL table at the marker's source; otherwise lowering
fails. Executing a logical plan with such a node requires a session whose query planner is
`FederatedQueryPlanner`. The region's SQL is generated only at execution, so unsupported shapes fail
then. `FederationOptimizerRule` is not used.

`to_df_physical`, optd's default execution path, lowers `Remote` the same way: it converts the
subtree with `to_df_logical`, projects it to the columns the parent requires (so only those are
transferred), and plans it with `federation::federated_exec`, which yields the
`datafusion-federation` remote execution node (`VirtualExecutionPlan`). Federated queries therefore
take the same direct physical path as local ones and need no `FederatedQueryPlanner`; that planner is
only needed for the logical path. The projection refers to columns by qualifier and name, like the
rest of `to_df_logical`.

Before SQL generation, both paths put a projection directly above every aggregate in the region
that lacks one. DataFusion's SQL planner always produces that shape and its SQL unparser relies on it
to rebuild `SELECT ... GROUP BY`; without it, the unparser nests the aggregation or emits columns out
of schema order. Plans converted from optd may omit the projection.

## End-to-End Tests

`optd/connectors/datafusion/tests/federation.rs` runs queries through `OptdRunner` against
`federation::SimulatedSource`s, the same simulated remote databases the CLI uses. Each test compares
results with plain DataFusion and checks exactly which tables each source's SQL scanned. A remote
scan that optd failed to push would be read through its local fallback copy and be missing from the
recorded SQL; one remote table has no fallback, so it cannot be read locally at all. The physical
path tests run without `FederatedQueryPlanner`, so falling back to the logical path would fail; one
test runs the logical path explicitly.

Two tests check join-order selection on skewed tables, with statistics collected from the loaded
data: the same query joins the two `db1` tables at the source when `optd.transfer_cost_per_row` is
high and fetches them separately when it is low.

## Simulated Sources

`federation::SimulatedSource` is a remote database simulated by a separate DataFusion session.
`SimulatedSource::adopt` moves a registered table into the source and re-registers it locally as a
remote table at `simulated/{name}`. With a fallback, the local registration can still scan the
original table, so optd collects statistics for it. The source executes the SQL optd sends, records
it, and optionally prints it to stderr.

## Command-Line Examples

The CLI's `--remote SOURCE=TABLE[,TABLE...]` option moves already registered tables to a simulated
source; pass it once per source. Each source prints the SQL it receives as
`-- simulated/{source}: ...`. The examples use TPC-H data, downloaded with
`./scripts/download_tpch_hf.sh`. The CLI logs optd's optimizer steps for each statement; a leading
`-c "SET optd.log_explain_steps = false"` turns that off. The `SET` statement itself prints
`DataFusion error: Error during planning: non-query plan` (step logging does not apply to it) and
`OK`; the outputs below omit those two lines.

Build the CLI once:

```sh
cargo build --release -p optd-datafusion --bin optd-cli
```

### A query whose tables share a source

All tables are at `db1`, so the whole query, including the aggregation and sort, runs there as one
SQL statement:

```sh
./target/release/optd-cli --tpch --remote db1=orders,customer \
  -c "SET optd.log_explain_steps = false" \
  -c "SELECT c_mktsegment, count(*) AS orders FROM orders JOIN customer ON o_custkey = c_custkey
      WHERE o_orderstatus = 'F' GROUP BY c_mktsegment ORDER BY c_mktsegment"
```

```text
-- simulated/db1: SELECT customer.c_mktsegment, count(1) AS "count(Int64(1))" FROM orders INNER JOIN customer ON ((orders.o_custkey = customer.c_custkey) AND (orders.o_orderstatus = CAST('F' AS VARCHAR))) GROUP BY customer.c_mktsegment ORDER BY customer.c_mktsegment ASC NULLS LAST
+--------------+-----------------+
| c_mktsegment | count(Int64(1)) |
+--------------+-----------------+
| AUTOMOBILE   | 14571           |
| BUILDING     | 15264           |
| FURNITURE    | 14083           |
| HOUSEHOLD    | 14457           |
| MACHINERY    | 14509           |
+--------------+-----------------+
```

### A query across sources

`orders` and `lineitem` are at `db1`, and `customer` is at `db2`. Each source receives only its own
tables and only the columns the query needs; the filter on `orders` runs at `db1`:

```sh
./target/release/optd-cli --tpch --remote db1=orders,lineitem --remote db2=customer \
  -c "SET optd.log_explain_steps = false" \
  -c "SELECT c_mktsegment, sum(l_extendedprice) AS revenue
      FROM customer JOIN orders ON c_custkey = o_custkey JOIN lineitem ON l_orderkey = o_orderkey
      WHERE o_orderdate < DATE '1995-03-15' GROUP BY c_mktsegment ORDER BY c_mktsegment"
```

```text
-- simulated/db1: SELECT lineitem.l_extendedprice, lineitem.l_orderkey FROM lineitem
-- simulated/db2: SELECT customer.c_mktsegment, customer.c_custkey FROM customer
-- simulated/db1: SELECT orders.o_orderkey, orders.o_custkey FROM orders WHERE (orders.o_orderdate < CAST('1995-03-15' AS DATE))
```

At the default transfer cost of `1.0` per row, `orders` and `lineitem` are fetched separately and
joined locally even though both are at `db1`: the default model's join costs dwarf the transfer this
join would save. Making transfer more expensive moves the join to `db1`, which then returns only the
two columns the rest of the query needs:

```sh
./target/release/optd-cli --tpch --remote db1=orders,lineitem --remote db2=customer \
  -c "SET optd.log_explain_steps = false" \
  -c "SET optd.transfer_cost_per_row = 100" \
  -c "SELECT c_mktsegment, sum(l_extendedprice) AS revenue
      FROM customer JOIN orders ON c_custkey = o_custkey JOIN lineitem ON l_orderkey = o_orderkey
      WHERE o_orderdate < DATE '1995-03-15' GROUP BY c_mktsegment ORDER BY c_mktsegment"
```

```text
-- simulated/db1: SELECT lineitem.l_extendedprice, orders.o_custkey FROM lineitem INNER JOIN orders ON ((lineitem.l_orderkey = orders.o_orderkey) AND (orders.o_orderdate < CAST('1995-03-15' AS DATE)))
-- simulated/db2: SELECT customer.c_mktsegment, customer.c_custkey FROM customer
```

### Inspecting the plan without executing

`--direct-physical-plan` prints the physical plan instead of running the query. Each remote region
appears as a `VirtualExecutionPlan` with the SQL it would send:

```sh
./target/release/optd-cli --tpch --remote db1=orders,customer --direct-physical-plan \
  -c "SELECT c_mktsegment, count(*) FROM orders JOIN customer ON o_custkey = c_custkey
      GROUP BY c_mktsegment"
```

```text
SchemaCastScanExec
  VirtualExecutionPlan name=simulated compute_context=db1 base_sql=SELECT customer.c_mktsegment, count(1) AS "count(Int64(1))" FROM orders INNER JOIN customer ON (orders.o_custkey = customer.c_custkey) GROUP BY customer.c_mktsegment
```

`--logical` runs the same queries on the logical conversion path; the CLI's session includes
`FederatedQueryPlanner` for it.
