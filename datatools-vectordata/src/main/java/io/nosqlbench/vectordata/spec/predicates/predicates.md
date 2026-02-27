# Predicates

This package defines a compact predicate tree model and byte-level encoding for query filters.
It is used to represent filter expressions as a typed AST and serialize/deserialize them through
`ByteBuffer`.

## Scope

This package provides:

- Predicate tree structure (`PNode`)
- Atomic predicates (`PredicateNode`)
- Boolean composition (`ConjugateNode` with `AND`/`OR`)
- Binary encoding/decoding (`BBWriter`, `PNode.fromBuffer`)
- Context-aware decoding with field mode selection (`PredicateContext`)

This package does not provide predicate evaluation against records. It defines representation and
serialization semantics.

## Representation Model

All nodes implement `PNode<?>`.

- `PredicateNode`
  - Represents one atomic predicate: `field op comparands`
  - Fields:
    - `field` (`int` API, encoded as one byte in indexed mode, or -1 in named mode)
    - `fieldName` (`String`, null in indexed mode, UTF-8 encoded in named mode)
    - `op` (`OpType`)
    - `v` (`long[]`) comparand values
- `ConjugateNode`
  - Represents n-ary boolean composition
  - `type` is `ConjugateType.AND` or `ConjugateType.OR`
  - `values` is an ordered array of child `PNode<?>` instances

## Field Modes

Predicates support two field identification modes:

### Indexed Mode (original)
Fields are identified by a 1-byte positional index. This couples predicates to a specific
`MetadataLayout` and is compact on the wire.

### Named Mode
Fields are identified by a UTF-8 name string. This makes predicates self-describing and
decoupled from any specific layout. The name is encoded as a 2-byte length prefix followed
by the UTF-8 bytes.

## PredicateContext

A `PredicateContext` determines the field mode once at construction time. Within that context,
all predicates use the same representation. Consumer code calls `ctx.fieldName(pred)` /
`ctx.fieldIndex(pred)` — no type checks, no conditionals. The mode decision is baked into the
context class at creation time.

### Factory Methods

- `PredicateContext.indexed(layout)` — indexed mode with a layout for name resolution
- `PredicateContext.named()` — named mode without a layout (index lookups return -1)
- `PredicateContext.named(layout)` — named mode with a layout for index resolution

### Usage

```java
PredicateContext ctx = PredicateContext.named(layout);
PNode<?> tree = ctx.decode(buf);

PredicateNode pred = ...;
String name = ctx.fieldName(pred);              // always works
int index = ctx.fieldIndex(pred);               // always works (if layout provided)
FieldDescriptor fd = ctx.fieldDescriptor(pred); // always works (if layout provided)
```

## Logical Semantics

- `PredicateNode` is a leaf expression over one field and zero or more `long` comparands.
- `ConjugateNode(AND, children...)` is true when all children are true.
- `ConjugateNode(OR, children...)` is true when any child is true.
- Nesting forms a full predicate tree.

Supported operators are `GT`, `LT`, `EQ`, `NE`, `GE`, `LE`, `IN`, and `MATCHES`.
Operator interpretation by comparand count is consumer-defined; this package stores structure and
operator identity only.

## Binary Encoding

Encoding is recursive, pre-order (parent first, then children). Decoding is the inverse.

### Node Dispatch

`PNode.fromBuffer(ByteBuffer)` peeks the next byte at the current position and interprets it as a
`ConjugateType` ordinal:

- `PRED` -> decode `PredicateNode`
- `AND`/`OR` -> decode `ConjugateNode`

For context-aware decoding that supports both indexed and named fields, use
`PredicateContext.decode(ByteBuffer)` instead.

### `ConjugateNode` Layout

`ConjugateNode.encode` writes:

1. `type` ordinal as 1 byte (`AND` or `OR`)
2. child count as 1 byte
3. each child node, encoded recursively

Encoded size:

- `1 + 1 + sum(encodedSize(child_i))`

### `PredicateNode` Layout — Indexed Mode

`PredicateNode.encode` writes (when `fieldName` is null):

1. `ConjugateType.PRED` ordinal as 1 byte
2. `field` as 1 byte
3. `op` ordinal as 1 byte
4. comparand length `v.length` as 2-byte signed short
5. each comparand as 8-byte signed long

Encoded size for `n` comparands:

- `1 + 1 + 1 + 2 + (8 * n)`

### `PredicateNode` Layout — Named Mode

`PredicateNode.encode` writes (when `fieldName` is non-null):

1. `ConjugateType.PRED` ordinal as 1 byte
2. name length as 2-byte unsigned short
3. name as UTF-8 bytes
4. `op` ordinal as 1 byte
5. comparand length `v.length` as 2-byte signed short
6. each comparand as 8-byte signed long

Encoded size for a name of `m` UTF-8 bytes and `n` comparands:

- `1 + 2 + m + 1 + 2 + (8 * n)`

### Important Compatibility Notes

- Enum ordinals are on-wire values (`ConjugateType`, `OpType`).
  Reordering enum constants is wire-incompatible.
- In indexed mode, `field` is narrowed from `int` to byte on encode.
- `ConjugateNode` child count is one byte.
- `PredicateNode` comparand count is a short.
- No explicit version tag is embedded in node bytes; compatibility relies on stable layout.
- A given buffer is always read with the same context mode that wrote it. The PRED tag byte
  does not distinguish modes; the context determines the format.

## Example Shape

Logical expression:

- `(field0 >= 10) AND ((field2 = 5) OR (field3 IN [7, 9]))`

Tree shape (indexed):

- `ConjugateNode(AND, ...)`
  - `PredicateNode(field=0, op=GE, v=[10])`
  - `ConjugateNode(OR, ...)`
    - `PredicateNode(field=2, op=EQ, v=[5])`
    - `PredicateNode(field=3, op=IN, v=[7, 9])`

Tree shape (named):

- `ConjugateNode(AND, ...)`
  - `PredicateNode(fieldName='age', op=GE, v=[10])`
  - `ConjugateNode(OR, ...)`
    - `PredicateNode(fieldName='score', op=EQ, v=[5])`
    - `PredicateNode(fieldName='category', op=IN, v=[7, 9])`

These structures encode deterministically using the layouts above and can be reconstructed with
`PNode.fromBuffer` (indexed) or `PredicateContext.decode` (either mode).
