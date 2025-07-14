# Predicate Format Specification v1

_A standard for representing predicate expressions in JSON format._
_version_: 1

## Overview

This specification defines a JSON format for representing predicate expressions that can be used for
filtering and querying. The format supports both simple predicates and nested logical conjunctions.

# Binary Format

TBD

# JSON Format

This format is what is used as the accepted format for writing hdf5 filter datasets.
It is the format that you should process your source data into for use with the buildhdf5 command.

## Node Types

There are two types of nodes in the predicate structure, which are inferred from the operator:

- Predicate nodes: Use comparison operators (`GT`, `LT`, etc.)
- Conjugate nodes: Use logical operators (`AND`, `OR`)

### Predicate Nodes

A predicate node represents a single comparison operation and has the following structure:

```json
{
  "field": <number>,
  "op": <string>,
  "values": [
    <number>,
    ...
  ]
}
```

Fields:

- `field`: A non-negative integer representing the field index
- `op`: One of the following comparison operators:
    - `"GT"` or `">"`: Greater than
    - `"LT"` or `"<"`: Less than
    - `"EQ"` or `"="`: Equals
    - `"NE"` or `"!="`: Not equals
    - `"GE"` or `">="`: Greater than or equal
    - `"LE"` or `"<="`: Less than or equal
    - `"IN"`: Value is in set
- `values`: Array of numeric values. For all operators except `IN`, this should contain exactly one
  value.

### Conjugate Nodes

A conjugate node represents a logical combination of other nodes and has the following structure:

```json
{
  "op": <string>,
  "nodes": [
    <node>,
    ...
  ]
}
```

Fields:

- `op`: One of the following logical operators:
    - `"AND"`: Logical AND of all child nodes
    - `"OR"`: Logical OR of all child nodes
- `nodes`: Array of child nodes, each being either a predicate or conjugate node

## Examples

### Simple Equality Predicate

```json
{
  "field": 0,
  "op": "EQ",
  "values": [
    123
  ]
}
```

Or using symbolic operator:

```json
{
  "field": 0,
  "op": "=",
  "values": [
    123
  ]
}
```

### IN Predicate

```json
{
  "field": 1,
  "op": "IN",
  "values": [
    3,
    4,
    5
  ]
}
```

### Compound AND Expression

```json
{
  "op": "AND",
  "nodes": [
    {
      "field": 0,
      "op": ">=",
      "values": [
        100
      ]
    },
    {
      "field": 0,
      "op": "<=",
      "values": [
        200
      ]
    }
  ]
}
```

## Validation Rules

1. All field indices must be non-negative integers
2. All operators must be one of the defined values (either name or symbol form)
3. All non-IN predicates must have exactly one value
4. Conjugate nodes must have at least one child node
5. Values must be numeric (integers in this version)

## Version History

- 1.0: Initial specification