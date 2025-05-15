import io.nosqlbench.nbvectors.api.fileio.VectorWriter;
import io.nosqlbench.xvec.writers.IvecVectorWriter;

/**
VectorWriters module
 */
module io.nosqlbench.xvec {
  requires testdata.apis;
  provides VectorWriter with IvecVectorWriter;
}
