/**
VectorWriters module
 */
module xvec.writer {
  requires testdata.apis;
  provides io.nosqlbench.nbvectors.api.fileio.VectorWriter with io.nosqlbench.xvec.writers.IvecVectorWriter;
}
