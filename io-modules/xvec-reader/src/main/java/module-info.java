/// readers for ivec, fvec, ...
module xvec.reader {
  requires com.google.gson;
  requires testdata.apis;
  provides io.nosqlbench.nbvectors.api.fileio.VectorRandomAccessReader with io.nosqlbench.readers.UniformBvecReader;
}
