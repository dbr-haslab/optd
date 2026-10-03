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
placeholder for transfer cost. Substrait export and both DataFusion converters reject it until
remote lowering is implemented.
