# Dataset Format

## HDF5 KNN Answer Key Format

Each contains the following four datasets within the hdf5 file:

### /train

This dataset contains the vectors used to train the vector database implementation. These vectors
must be loaded into the database prior to testing performance and accuracy of vector queries. The
format of the data is a multi-dimensional float array, with x number of y-dimensional vectors where
x=the number of individual vectors and y=the number of dimensions in each vector.

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