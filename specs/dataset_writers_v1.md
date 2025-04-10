# HDF5 Vector KNN Writer spec v1

_A standard for writing vector search answer keys into HDF5 data files._
_version_: 1

This specification defines the format of the HDF5 data files used to store vector search answer
keys. The format is designed to be simple and easy to use, while still providing enough information
to perform accurate testing of vector search implementations. The HDF5 format solves a myriad of 
data sharing problems within a single file format, including random access, annotations and so on.

Both the datasets and their required attributes are indicated here.

## Endianness

All encodings in this standard `MUST` be little-endian. If big-endian systems need support for
native encodings, then the specification can be extended. Unless/Until this happens, endianness is
out of scope. Big-endian systems are expected to translate on their own behalf.

## Base Content

### /base_vectors (required)

This dataset contains the base vectors used to train the vector database implementation. These
vectors must be loaded into the database prior to testing performance and accuracy of vector
queries. The format of the data is a multidimensional array, with x number of y-dimensional vectors
where x is the number of individual vectors and y is the dimensionality of all vectors.

By default, the data is stored as a float array. However, other formats are supported. For
non-floating point representations, some systems have their own prescribed encoding and decoding of
vector components.

#### required attributes

- **dimensions** The number of dimensions in each vector, AKA the second dimension of the 
  `/base_vectors` dataset.
- **count** The number of vectors in the `/base_vectors` dataset, AKA the first dimension of the 
  `/base_vectors` dataset. 
- **model** The name of the model used to generate the data, if any. This should be a descriptive and
  canonical name for the model. The name should be the same as what customers may use to select a
  specific variant or version of a model in their systems.
- **distance_function** The case-insensitive name of the distance function used to compute distance
  between vectors. 

### /base_content (optional)

This is an optional dataset. The original content may be provided which corresponds to the 
base_vectors data. If provided, the major dimension of this dataset must match the major 
dimension of the base_vectors dataset, where the major coordinate corresponds pair-wise. In 
other words, base_vectors[i] would be the embeddings for base_content[i] and so on, irrespective 
of the other dimensions on either dataset.  

#### required attributes

- **media_type** the media type of the content, such as "text/plain" or "image/jpeg"
- **count** the number of content items, AKA the first dimension of the `/base_content` dataset.

## Queries

### /query_vectors (required)

This dataset contains the vectors used to test the performance and accuracy of the vector database
implementation. The format of the data is a multi-dimensional float array, with x number of
y-dimensional vectors where x is the number of individual vectors and y is the number of dimensions
in each vector.

#### required attributes

- **model** The name of the model used to generate the data, if any. This should be a descriptive and
  canonical name for the model. The name should be the same as what customers may use to select a
  specific variant or version of a model in their systems.
- **count** The number of vectors in the `/query_vectors` dataset, AKA the first dimension of the 
  `/query_vectors` dataset.
- **dimensions** The number of dimensions in each vector, AKA the second dimension of the 
  `/query_vectors` dataset.

### /query_terms (optional)

This is an optional dataset. It may be used for more advanced testing scenarios where the original
content may be useful. This includes all media forms. It is not limited to text. Both the type of
content and the HDF5 data format used are unspecified. The user may use whatever conventions are
needed. It is suggested that the content provided here is what users would be most familiar with, to
make troubleshooting and reproduction of issues easier. The primary dimension of this dataset must
correspond to that of the query dataset, with each major coordinate matching pair-wise for
associated query terms.

#### required attributes

(none)

### /query_filters (optional)

This is an optional dataset. It may be used to describe the query filters which accompany the
query_vectors. When provided, it is presumed that valid ground-truth results from the
_neighbors_ and _distances_ datasets were both constructed with correct filtering. This means that
either the ground truth was constructed with these filters or that the filters were captured from
another form which was also used to build the ground truth.

The format of the query_filters data is a one-dimensional table which includes a compact encoding of
predicates. These raw data are stored in Jave `byte` representation, which is simply twos-compliment
8 bit, or in HDF5 terms, `H5T_STD_I8LE`. These are called _predicate buffers_. The nbvectors module
contains SERDES support predicate buffers, and acts as the reference implementation.

The types of filters which may be represented include Java long values, basic conjunctions `AND`,
`OR`, equality and inequality operators `>`, `<`, `>=`, `<=`, `!=`, and set membership `IN(...)`.
The query structure is maintained where possible to match term-for-term with the native syntax of a
variety of systems. Long values are the only currently supported value, but this may change.
This supports the salient predicate testing questions where vector and non-vector
predicate terms are used together. Additional types will be supported as needed.

Writers of this table *SHOULD* use a constant width if possible, but it is not required. This may
avoid some overhead of indexing structure in the HDF5 format. The encoded format supports reading
only the valid portion of any buffer.

#### required attributes

(none)

## Results

### /neighbor_indices (required)

This dataset contains the ground truth nearest neighbors for each vector in the test dataset. The
format of the data is a multi-dimensional integer array, where x is the number of individual vectors
and y is the number of nearest neighbors to each vector.

Data in the neighbors dataset should be encoded as either a 32-bit signed integer, little-endian,
or a 64-bit signed integer. 

#### required attributes

- **count** - the total number of neighborhoods provided
- **k** - the number of neighbors provided per query vector

### /neighbor_distances (optional)

This dataset contains the ground truth distances for each vector in the test dataset. The format of
the data is a multi-dimensional float array, with x number of y-dimensional vectors where x=the
number of individual vectors and y=the number of nearest neighbors to each vector.

Although, these can be computed dynamically when needed for correctness testing, they may be provided
to avoid the need to compute them. This can be beneficial to make testing harnesses more 
efficient, so that performance and accuracy can be tested in a single pass.

#### required attributes

- **k** - the number of neighbors provided per query vector
- **count** - the total number of neighborhoods provided




