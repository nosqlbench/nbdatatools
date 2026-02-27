# Predicate Store: Architecture Overview

## The Problem

Predicate test datasets need persistent storage for four related facets:
**predicates** (query filter trees), **result indices** (ground-truth matching ordinals),
**metadata layout** (schema), and **metadata content** (the actual rows). Two backends
are supported — slabtastic (`.slab`) and SQLite (`.db`) — selected automatically by
file extension in `dataset.yaml`.

## Layer Diagram

```
  dataset.yaml profile
        │
        ▼
┌─────────────────────────────────┐
│  FilesystemPredicateTestDataView│  (or VirtualPredicateTestDataView for remote)
│  ─ resolves file paths          │
│  ─ infers backend from extension│
│  ─ shares backends across facets│
└────────┬────────────────────────┘
         │ creates
         ▼
┌─────────────────────────────────┐
│   PredicateStoreBackend         │  interface: getPredicate(), getResultIndices(),
│                                 │  getMetadataLayout(), getMetadataContent()
├────────────────┬────────────────┤
│ Slabtastic     │ SQLite         │
│ (.slab)        │ (.db/.sqlite)  │
│ SlabReader     │ JDBC           │
│ 4 namespaces   │ 4 tables       │
└────────────────┴────────────────┘
         │ raw ByteBuffers
         ▼
┌─────────────────────────────────┐
│   DatasetView Adapters          │  decode bytes → typed objects
│   ─ PredicatesDatasetView       │  → PNode<?> via PNode.fromBuffer()
│   ─ ResultIndicesDatasetView    │  → int[] (length-prefixed)
│   ─ MetadataContentDatasetView  │  → Map<String,Object> via MetadataRecordCodec
└─────────────────────────────────┘
```

## Type System

`FieldType` enum defines wire tags for five types: `TEXT(0)`, `INT(1)`, `FLOAT(2)`, `BOOL(3)`, `ENUM(4)`.

`FieldDescriptor` bundles a field name + type + enum values, and self-serializes as
`[type:1][nameLen:2][nameBytes:N]` (plus enum entries for ENUM fields).

`MetadataLayoutImpl` holds an ordered list of `FieldDescriptor`s with name→index lookup.
Serialized as `[fieldCount:2][field0][field1]...`.

`MetadataRecordCodec` encodes/decodes a `Map<String,Object>` against a layout. Per field:
`[fieldIndex:2][fieldType:1][value...]` where value encoding varies by type (TEXT: len+utf8,
INT: 8-byte long, FLOAT: 8-byte double, BOOL: 1 byte, ENUM: 4-byte ordinal).

## Backend Selection

`SourceType.inferFromPath()` maps extensions:

| Extension        | SourceType | Backend                    |
|------------------|------------|----------------------------|
| `.slab`          | `SLAB`     | `SlabtasticPredicateBackend` |
| `.db`, `.sqlite` | `SQLITE`   | `SQLitePredicateBackend`     |

Both backends implement `PredicateStoreBackend` — same interface, same wire encoding for
record data, just different storage engines underneath.

## Slabtastic Backend

A single `.slab` file stores four namespaces: `predicates`, `result_indices`,
`metadata_layout`, `metadata_content`. The `SlabReader` provides O(log n) lookup by
ordinal within each namespace. The new `SlabReader(AsynchronousFileChannel, long)`
constructor accepts external channels (e.g. `MAFileChannel`) for remote access.

## SQLite Backend

Four tables with identical schema: `(ordinal INTEGER PRIMARY KEY, data BLOB)`.
The BLOB content uses the same encoding as slabtastic. For remote access,
`VirtualPredicateTestDataView` prebuffers the entire file via `MAFileChannel`, then
opens SQLite on the local cache path.

## Writers

`SlabtasticPredicateWriter` and `SQLitePredicateWriter` both accept a `MetadataLayoutImpl`
and provide `writePredicate()`, `writeResultIndices()`, `writeMetadataRecord()`. The slab
writer produces a multi-namespace `.slab` file; the SQLite writer produces a `.db` with
four tables.

## TestDataKind & dataset.yaml Integration

Three new kinds were added: `predicate_results`, `metadata_layout`, `metadata_content`
(with aliases like `meta_results`, `layout`, `content`). These join the existing
`metadata_predicates` to form the four predicate facets. In a profile, each facet names
a file whose extension selects the backend:

```yaml
profiles:
  default:
    metadata_predicates: predicates.slab
    predicate_results: results.slab
    metadata_layout: layout.slab
    metadata_content: content.slab
```

## File Index

| File | Role |
|------|------|
| `FieldType.java` | Wire-tagged enum for metadata field types |
| `FieldDescriptor.java` | Field name + type + enum values, self-serializing |
| `MetadataLayout.java` | Interface: schema of metadata records |
| `MetadataLayoutImpl.java` | Concrete layout with serialization |
| `MetadataContent.java` | Interface: `DatasetView<Map<String,Object>>` |
| `MetadataRecordCodec.java` | Static encode/decode for metadata records |
| `PredicateStoreBackend.java` | Backend-agnostic read interface |
| `slab/SlabtasticPredicateBackend.java` | SlabReader-backed implementation |
| `sqlite/SQLitePredicateBackend.java` | JDBC-backed implementation |
| `views/PredicatesDatasetView.java` | Adapter: backend → `Predicates<PNode<?>>` |
| `views/ResultIndicesDatasetView.java` | Adapter: backend → `ResultIndices` |
| `views/MetadataContentDatasetView.java` | Adapter: backend → `MetadataContent` |
| `FilesystemPredicateTestDataView.java` | Local filesystem orchestrator |
| `VirtualPredicateTestDataView.java` | Remote/MAFileChannel orchestrator |
| `slab/SlabtasticPredicateWriter.java` | Produces `.slab` files |
| `sqlite/SQLitePredicateWriter.java` | Produces `.db` files |
