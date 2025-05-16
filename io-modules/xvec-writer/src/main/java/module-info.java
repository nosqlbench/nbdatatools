import io.nosqlbench.nbvectors.api.fileio.VectorFileStore;

/**
VectorWriters module
 */
module xvec.writer {
  requires testdata.apis;
  provides VectorFileStore with
    io.nosqlbench.xvec.writers.IvecVectorWriter,
    io.nosqlbench.xvec.writers.DvecVectorWriter,
    io.nosqlbench.xvec.writers.FvecVectorWriter,
    io.nosqlbench.xvec.writers.HvecVectorWriter,
    io.nosqlbench.xvec.writers.SvecVectorWriter;
}
