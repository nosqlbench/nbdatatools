# Ordinal Predicate Writers spec v1

_A standard for writing vector search predicates into HDF5 files._
_version:_ 1

This section specifies a way to read and write non-vector predicates in tandem
with vector-predicates and pre-computed KNN results.

In this section, **ordinal** emphasizes the simplification of using only numeric field types and 
values in non-vector predicates. There are several reasons for this:

1. To allow focus on the numerical relationships between vector spaces and related fields. 
   Ordinals provide the most direct _identity_ semantics for emulating operational behaviors 
   during hybrid query.
2. To keep the hdf5 encoding uniform in most cases. Array-based indexing makes for efficient IO 
   which is necessary to give test systems operational leverage.
3. To simplify integration across a number of client runtimes and tools. Generalized syntax 
   mapping between disparate systems is not the fundamental goal of this specification and would 
   hinder adoption.
4. The ordinal-based approach can serve as a theoretic and practical foundation upon which more 
   advanced testing regimes may be derived.

The result of a hybrid query is the logical conjunction of both the 
vector ANN predicate and the non-vector predicates. Thus, the augmentative data can be 
considered to be a parallel version of what we already provide for the KNN dataset.

## HDF5 addressing

The dataset containing filtering predicates will be named "/filters", and the association with query
vectors shall be made as with other named datasets. Filter predicate '0' is the first one to appear
in the dataset, and corresponds to ANN query vector '0', from the "/test" dataset.

# Encoding Schemes

## Structured Predicates

## Predicate Types

The types of predicates which can be represented are limited to ordinal comparators, and 64-bit
signed values (Java long) are used as a standard.

Basic conjugates like 'and' and 'or' are supported, as are all the basic operators `>`, `<`, `>=`,
`<=`, `=`, `!=`, and `in`. Although the
`in` case is a logical way of expressing something like `a=3 or a=4 or ...`, which could be provided
by the other operators, it is maintained as a separate predicate structure to ensure that testing
covers query processing and normative forms as users and applications would see them.

## Predicate Fields

The fields used with the predicates are indicated as offsets into some other view of the fields that
the host system should know. It is not important for the predicates to have specific field names, so
long as the calling system can map them to actual field names during op construction.

## Predicate Structure

A predicate may be encoded as a variable-length byte stream, or it may be fixed-length. When all
predicates in a dataset have the same structure, then they should be by-definition the same length.
The native `byte` type in Java is a signed two's compliment value. This is the form that is used 
for hdf5 datasets.

## Encoding

TBD Updates Needed


The byte stream which is the encoded filtering predicate consists of the following:

* a series of 1-byte node tokens, each representing a conjugate type or a predicate type.
    1. `0x00` - Predicate
    2. `0x01` - AND
    3. `0x02` - OR
* Each non-predicate node token is followed by the number of leaf nodes under it as `0x00 .. 
0x80`. This is a signed byte, limiting leaf nodes to between 0 and 127 inclusive.
* Predicate node tokens are followed by:
    1. a one-byte field offset as `0x00 .. 0x80` corresponding to a field name table owned by the
       host system.
    2. a one-byte operator code, enumerating `>`, `<`, `=`, `!=`, `>=`, `<=`, and `IN`, respectively
    3. a one-byte value cardinality in `0x00 .. 0x80`, indicating the number of values.

### Example

Taking a toy example `a=5 and b in(3,7)`, the encoding would be:

- `0x01` AND
- `0x02` 2 leafs
- `0x00` Predicate (first leaf)
- `0x00` field 0 (from field table)
- `0x02` EQ (`=`)
- `0x01` 1 value follows
- 5 encoded as 64-bit signed (2s compliment, little-endian)
- `0x00` Predicate (second leaf)
- `0x01` field 1 (from field table)
- `0x06` IN
- `0x02` 2 values follow
- 3 encoded as 64-bit signed (2s compliment, little-endian)
- 7 encoded as 64-bit signed (2s compliment, little-endian)

This yields a total of 34 bytes for a predicate containing 24 bytes of data and a hierchical
predicate structure.

# Metadata

## predicate_structure

Indicates whether the test harness can process the predicate structure ahead of time for efficiency,
as in prepared statements. This requires the predicate structure to be uniform, as in the only thing
that varies are the comparators.

* `uniform` | `mixed`


