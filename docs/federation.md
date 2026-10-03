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
