# Metadata Reader spec v1

_A standard for reading vector testing metadata from HDF5 files._
_version_: 1

In this version of the reader spec

Readers SHOULD be able to write all attributes which writers SHOULD write, but they should be able
to function without error for elements which writers SHOULD write but do not.

Readers MUST be able to read all attributes that writers MUST write, and must throw errors if the
data format does not comply.

Where user-specified attribute paths, names or values are provided, readers should adhere to the
parsing rules described in (attribute_syntax_v1.md)[attribute_syntax_v1.md].