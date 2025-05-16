import io.nosqlbench.nbvectors.api.fileio.BoundedVectorFileStream;
import io.nosqlbench.nbvectors.api.fileio.VectorFileStream;
import io.nosqlbench.nbvectors.api.noncore.VectorRandomAccessReader;

/// readers for ivec, fvec, ...
module xvec.reader {
  requires com.google.gson;
  requires testdata.apis;
  requires annotations;
  provides VectorRandomAccessReader with
    io.nosqlbench.readers.UniformIvecReader,
    io.nosqlbench.readers.UniformBvecReader,
    io.nosqlbench.readers.UniformDvecReader,
    io.nosqlbench.readers.UniformFvecReader;
  provides VectorFileStream with
    io.nosqlbench.readers.CsvJsonArrayStreamer;
  provides BoundedVectorFileStream with
    io.nosqlbench.readers.UniformBvecStreamer,
    io.nosqlbench.readers.UniformDvecStreamer,
    io.nosqlbench.readers.UniformFvecStreamer,
    io.nosqlbench.readers.UniformIvecStreamer;
}
