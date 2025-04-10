# Attribute Syntax spec v1
_A standard for specifying HDF5 attribute names and values as parameters or configuration data._
_version_: 1

Test data tooling often needs to query or update metadata on testing assets. For the HDF5 data
format, metadata support is provided as part of the HDF5 attribute specification.

To make integration with test tooling systems more robust and streamlined, a syntax for specifying,
assigning, and accessing attribute values is provided here.

## Attribute Names

An attribute name can be any valid name starting with an alphabetic character and containing zero or
more following alphanumeric characters. When presented as only a name, the attribute is presumed to
be attached to the root HDF5 group.

Ann attribute path can be provided to attach an attribute to a different group or a dataset.
Attribute paths and names are separated from each other by either a dot `.` or a colon `:`.

The explicit name of the root group is `/`, but is optional in the case that it is the only part of
the path. Thus, all the following are equivalent:

```
attrname
:attrname
.attrname
/:attrname
/.attrname
```

Attribute groups and dataset names which are part of the attribute path are simply concatenated to
the root group with a forward slash `/`. No further restrictions are placed on the group and dataset
names except that they must be valid HDF5 identifiers.

### attribute name pattern

An attribute path, when provided, should match the following Java Pattern:

```
(?<path>/|(?:/[^:/.]+)+)?          # Optional HDF5 path (e.g., /, /group, /group1/group2)
[:.]?                              # Optional : or . separating path from attribute
(?<attr>[a-zA-Z_][a-zA-Z0-9_]*)    # Variable name (required, follows identifier rules)
```

### attribute name examples

```
attrname
:attrname
.attrname
/:attrname
/.attrname
/group1:attrname
/group1.attrname
/group1/group2:attrname
/group1/group2.attrname
```

## Attribute Value Literals

### attribute value type casting

Where attribute values are provided, they may be optionally prefixed with a type coercion hint. This
follows the typical casting operator form of C++ and Java. Attribute value literals should match the
following Java Pattern:

```
(?:\((?<typename>[a-zA-Z0-9_]+)\))? # Optional type hint (e.g., (String), (int))
(?<literal>.+)                      # Value as String literal
```

That is, they may have an optional type name, and then must have a literal value. The types allowed
include `byte|int|long|short|float|double|string` and are not case sensitive.

When a type is specified in this form, it takes precedence over implied types as explained below.
Thus, if a user specifies a type such as `(int)234L`, the value will be parsed as an integer and an
error should be thrown.

### attribute value type formats

Attribute values must be assignable to on of the raw types supported by the core HDF5 specification.
Each type should have an unambiguous literal form. When a value is given with an implied type, the
following rules should determine which type is selected and thus how the literal is parsed. These
rules are order-specific, so act as a sieve, preferring some types over others by precedence where
order might otherwise be ambiguous. The formats are qualified with Java Pattern syntax.

1. `[+-]?\d+[bB]` - an explicit byte literal with `b` or `B` suffix
2. `[+-]?\\d{10,}` - an implicit long, having 10 or more digits, as in 1000000000 or higher
3. `[+-]?\d+[lL]` - an explicit long with `l` or `L` suffix
4. `[+-]?\d+[sS]` - an explicit short, with `s` or `S` suffix
5. `[+-]?\d+[iI]?` - an implicit or explicit integer, with optional `i` or `I` suffix
6. `[+-]?\d+\.\d+` and `digits(literal)<=7` - an implicit float, when there are **seven or fewer
   digits**
7. `[+-]?\d+\.\d+` and `digits(literal)>7` - an implicit double, when there are **eight or more
   digits**
8. `[+-]?\d+(\.\d+)?[fF]` - an explicit float, with `f` or `F` suffix
9. `[+-]?\d+(\.\d+)?[dD]` - an explicit double, with `d` or `D` suffix
10. `.+` - anything else not matching a previous pattern is a String

### attribute value literal examples

```
(String)astring
(int)234
(float)234
(byte)234
12345678901234567890l
(long)12345678901234567890L
(String)12345678901234567890l
(int)12345678901234567890
(int)foobarbaz
```

## Attribute Value Assignments

An assignment of a value to an attribute is simply the joining of the attribute name to a value with
an equals `=` character between them.

### attribute value assignment examples

These examples were made simply by taking the examples from attribute names and attribute value
literals above and joining them with an equals sign.

```
attrname=(String)astring
:attrname=(int)234
.attrname=(float)234
/:attrname=(byte)234
/.attrname=12345678901234567890l
/group1:attrname=(long)12345678901234567890L
/group1.attrname=(String)12345678901234567890l
/group1/group2:attrname=(int)12345678901234567890
/group1/group2.attrname=(int)foobarbaz
```

