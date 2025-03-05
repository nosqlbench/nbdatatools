# Vector Test Data Formats

The documents herein serve as a basic specification for storing KNN answer keys in HDF5 files. Each
of the sections listed below is canonically part of the specification.

## version_1

- [HDF5 Dataset Writers v1](dataset_writers_v1.md) - The HDF5 format elements for writers.
- [HDF5 Metadata Writers v1](metadata_writers_v1.md) - HDF5 supplemental and required attributes for
  writers.
- [HDF5 Dataset Readers v1](dataset_readers_v1.md) - The HDF5 format elements for readers.
- [HDF5 Metadata Readers v1](metadata_readers_v1.md) - HDF5 test data attributes for readers.
- [HDF5 Attribute Syntax spec v1](attribute_syntax_v1.md) - Canonical path specifier for managing
  attributes.
- [Optional Annex 1 - Ordinal Predicates Writers v1](ordinal_predicate_writers_v1.md) - Data format
  for encoding predicates for filtering vector subspaces.
- [Optional Annex 2 - Variable Length Predicate Writers v1](variable_length_predicates_v1.md) - Data
  format for encoding raw predicate structure

## Versioning

Each component of the specification will be explicitly versioned, and previous versions shall be
maintained in perpetuity as part of this repo. With respect to versions, compatibility of components
shall be tied to a specific overall version, such as with [version_1](#version_1) as shown above.

## Compatibility

The words `MUST` and `SHOULD` are used to indicate a level of compatibility with the format. When
referencing this standard, systems which implement all the requirements for one of the versioned
specifications above, including both `MUST` and `SHOULD` elements, may be described as
***COMPATIBLE with _________ v1***. Implementations which cover only the reader or writer portions
may still remain fully compatible as long as the distinction is made for users.

