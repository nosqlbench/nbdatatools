# Metadata Writers v1
_A standard for writing vector testing metadata to HDF5 files._
_version_: 1


For the sake of configuring tests and systems with the proper defaults, it is useful to have some
metadata available. Reasons for having this metadata include:

- managing and cataloging testing assets
- cross-checking internal consistency
- test system auto-configuration

When provided, these properties `MUST` be stored in hdf5 attributes on the root group by default
except where specific attributes pertain to specific datasets or groups.

## neighbors

The maximum number of neighbors provided for each test vector, AKA the first dimension of the
`/neighbors` dataset.

Writers MUST provide this attribute.

## dimensions

The number of dimensions in each vector, AKA the second dimension of the `/test` and `/train`
datasets.

Writers MUST provide this attribute.

## training_vectors

The number of vectors in the `/train` dataset, AKA the first dimension of the `/train` dataset.

Writers MUST provide this attribute.

## test_vectors

The number of vectors in the `/test` dataset, aka the first dimension of the `/test` dataset.

Writers MUST provide this attribute.

## model

The name of the model used to generate the data, if any. This should be a descriptive and canonical
name for the model. The name should be the same as what customers may use to select a specific
variant or version of a model in their systems.

Writers MUST provide this attribute.

## distance_function

The case-insensitive name of the distance function used to compute distance between vectors. The
format of this function should be the shortest unambiguous name for the distance function as
commonly used in the literature. If provided, any readers should validate that the distance function
is explicitly supported, and throw an error if it is not. If not provided, the default distance
function should be presumed as `cosine`.

Example values: `cosine` | `euclidean` | `manhattan` | `hamming` | `jaccard` (case-insensitive)

Fully-compatible writers MUST provide this attribute.

## component_encoding

This is an optional attribute of the root group or of a specific dataset. The value is taken from,
in order of precedence, 1) the attribute on the dataset, 2) the attribute of the root group, and 3)
the underlying datatype on the dataset.

This is a named encoding which encapsulates the low-level data type of each component as well as how
it should be encoded/decoded to the underlying hdf5 data type.

In most cases, this will be trivial. For example, `float` represents a 32-bit signed floating point
value in Java parlance unambiguously. When the underlying data encoding (as described by built-in
HDF5 metadata) maps exactly to the runtime vector component type, this is sufficient.

In other cases, a model provider may choose to encode component data differently from how it is
actually used at runtime. A good example of this is the voyage-3 model when used with binary
embeddings. A canonically correct HDF5 data type for this would be a bit field, which would
disambiguate it from the 8-bit integer embeddings from the same model. Without the benefit of the
HDF5 datatypes, however, the same 8-bit type is used for both integer and binary embeddings. Thus,
the underlying HDF5 data type may not be used to infer both the decoding and the correct distance
function implementation (type-specific).

Further, HDF5 does not actually support all the data types used with TPU/GPU libraries, such as
bfloat16. For these types, a cross-encoding scheme is required, such as packing four bfloat-16
values into a single 64-bit integer.

To support both the simple and direct case as well as special cases, the default value of
`component_encoding` is presumed to be the underlying hdf5 data type as documented by hdf5 type
information of the respective dataset. However, if this value is provided as an attribute, it will
take precedence. In this case, the value can be any other unique value which is not one of the
standard identifiers.

Here are the (case-insensitive) standard identifiers for the known simple cases:

__supported presently in nbvectors__

* `float32` | `float` - 32-bit signed floating-point values, stored as such. (H5T_IEEE_F32LE) (
  supported in nbvectors)

__future support available in nbvectors__

* `double64` | `double`- 64-bit signed floating-point values, stored as such. (H5T_IEEE_F64LE)
* `int32` | `int`- 32-bit signed integer values, stored as such. (H5T_STD_I32LE)
* `int16` | `short` - 16-bit signed integer values, stored as such. (H5T_STD_I16LE)

Other types which need to be converted from HDF5-native types to the runtime types include:

__future support available in nbvectors__

* `binary_int8` - single-bit component encoding, with values stored in chunks of 8 as signed 8-bit
  integers.
* `float16_int16` - 16-bit signed floating-point values, stored as int16. The bitwise representation
  of int16 is used to hold the IEEE 754 “half” image.
* `bfloat16_int16` - [brain float](https://en.wikipedia.org/wiki/Bfloat16_floating-point_format)
  details TBD

For most special cases, a suitable HDF5 integer type can provide the bit image needed to store other
types (signed 8,16,32,64). In these cases, an explicit `component_encoding` MUST be provided, with
the naming convention as shown above.

Data providers _SHOULD_ provide this value.
