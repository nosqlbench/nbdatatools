# HDF5 Vector KNN Writer spec v1
_A standard for writing vector search answer keys into HDF5 data files._
_version_: 1

## Basics

The following four datasets are contained within the hdf5 file:

### /train

This dataset contains the vectors used to train the vector database implementation. These vectors
must be loaded into the database prior to testing performance and accuracy of vector queries. The
format of the data is a multidimensional array, with x number of y-dimensional vectors where x=the
number of individual vectors and y=the number of dimensions in each vector.

By default, the data is stored as a float array. However, other formats are supported. For
non-floating point representations, some systems have their own prescribed encoding and decoding of
vector components.

### /test

This dataset contains the vectors used to test the performance and accuracy of the vector database
implementation. The format of the data is a multi-dimensional float array, with x number of
y-dimensional vectors where x=the number of individual vectors and y=the number of dimensions in
each vector.

### /distances

This dataset contains the ground truth distances for each vector in the test dataset. The format of
the data is a multi-dimensional float array, with x number of y-dimensional vectors where x=the
number of individual vectors and y=the number of nearest neighbors to each vector.

### /neighbors

This dataset contains the ground truth nearest neighbors for each vector in the test dataset. The
format of the data is a multi-dimensional integer array, with x number of y-dimensional vectors
where x=the number of individual vectors and y=the number of nearest neighbors to each vector.

Additionally, metadata and mixed predicates will be added to this format when they have been proven
out.

## Endianness

All encodings in this standard `MUST` be little-endian. If big-endian systems need support for
native encodings, then the specification can be extended. Unless/Until this happens, endianness is
out of scope. Big-endian systems are expected to translate on their own behalf.



