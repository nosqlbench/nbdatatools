# HDF5 Vector KNN Reader spec v1
_A standard for reading vector search answer keys from HDF5 data files._
_version_: 1

In this version of the reader spec

* Readers SHOULD be able to write all elements which writers SHOULD write, but they should be able
  to function without error for elements which writers SHOULD write but do not.
* Readers MUST be able to read all elements which writers MUST write, and must throw errors if the
  data format does not comply* * Where user-specified attribute paths, names or values are provided, readers should adhere to the
    parsing rules described in (attribute_syntax_v1.md)[attribute_syntax_v1.md].