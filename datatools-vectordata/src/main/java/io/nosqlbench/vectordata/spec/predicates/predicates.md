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

This package does not provide predicate evaluation against records. It defines representation and
serialization semantics.

## Representation Model

All nodes implement `PNode<?>`.

- `PredicateNode`
  - Represents one atomic predicate: `field op comparands`
  - Fields:
    - `field` (`int` API, encoded as one byte)
    - `op` (`OpType`)
    - `v` (`long[]`) comparand values
- `ConjugateNode`
  - Represents n-ary boolean composition
  - `type` is `ConjugateType.AND` or `ConjugateType.OR`
  - `values` is an ordered array of child `PNode<?>` instances

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

### `ConjugateNode` Layout

`ConjugateNode.encode` writes:

1. `type` ordinal as 1 byte (`AND` or `OR`)
2. child count as 1 byte
3. each child node, encoded recursively

Encoded size:

- `1 + 1 + sum(encodedSize(child_i))`

### `PredicateNode` Layout

`PredicateNode.encode` writes:

1. `ConjugateType.PRED` ordinal as 1 byte
2. `field` as 1 byte
3. `op` ordinal as 1 byte
4. comparand length `v.length` as 2-byte signed short
5. each comparand as 8-byte signed long

Encoded size for `n` comparands:

- `1 + 1 + 1 + 2 + (8 * n)`

### Important Compatibility Notes

- Enum ordinals are on-wire values (`ConjugateType`, `OpType`).
  Reordering enum constants is wire-incompatible.
- `field` is narrowed from `int` to byte on encode.
- `ConjugateNode` child count is one byte.
- `PredicateNode` comparand count is a short.
- No explicit version tag is embedded in node bytes; compatibility relies on stable layout.

## Example Shape

Logical expression:

- `(field0 >= 10) AND ((field2 = 5) OR (field3 IN [7, 9]))`

Tree shape:

- `ConjugateNode(AND, ...)`
  - `PredicateNode(field=0, op=GE, v=[10])`
  - `ConjugateNode(OR, ...)`
    - `PredicateNode(field=2, op=EQ, v=[5])`
    - `PredicateNode(field=3, op=IN, v=[7, 9])`

This structure encodes deterministically using the layouts above and can be reconstructed with
`PNode.fromBuffer`.
