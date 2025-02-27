package io.nosqlbench.nbvectors.buildhdf5;

import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import io.nosqlbench.nbvectors.buildhdf5.predicates.types.PNode;
import io.nosqlbench.nbvectors.verifyknn.datatypes.LongIndexedFloatVector;
import io.nosqlbench.nbvectors.verifyknn.statusview.Glyphs;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class KnnDataWriter implements AutoCloseable {
  private final Path hdfOutPath;
  private final WritableHdfFile writable;

  public KnnDataWriter(Path hdfOutPath) {
    this.hdfOutPath = hdfOutPath;
    writable = HdfFile.write(hdfOutPath);
  }

  public void writeTrainingStream(Iterator<LongIndexedFloatVector> iterator) {
    List<LongIndexedFloatVector> vectors = new ArrayList<>();
    iterator.forEachRemaining(vectors::add);
    float[][] ary = new float[vectors.size()][vectors.getFirst().vector().length];
    for (LongIndexedFloatVector vector : vectors) {
      ary[(int) vector.index()] = vector.vector();
    }
    //    WritableDataset ds = new WritableDatasetImpl(ary,"/train",writable);
    this.writable.putDataset("train", ary);
  }

  public void writeTestStream(Iterator<LongIndexedFloatVector> iterator) {
    List<LongIndexedFloatVector> vectors = new ArrayList<>();
    iterator.forEachRemaining(vectors::add);
    float[][] ary = new float[vectors.size()][vectors.getFirst().vector().length];
    for (LongIndexedFloatVector vector : vectors) {
      ary[(int) vector.index()] = vector.vector();
    }
    //    WritableDataset ds = new WritableDatasetImpl(ary,"/train",writable);
    this.writable.putDataset("test", ary);
  }

  public void writeNeighborsStream(Iterator<long[]> iterator) {
    List<long[]> vectors = new ArrayList<>();
    iterator.forEachRemaining(vectors::add);
    long[][] ary = new long[vectors.size()][vectors.getFirst().length];
    for (int i = 0; i < ary.length; i++) {
      ary[i] = vectors.get(i);
    }
    this.writable.putDataset("neighbors", ary);
  }

  public void writeDistancesStream(Iterator<float[]> iterator) {
    List<float[]> distances = new ArrayList<>();
    iterator.forEachRemaining(distances::add);
    float[][] ary = new float[distances.size()][distances.getFirst().length];
    for (int i = 0; i < ary.length; i++) {
      ary[i] = distances.get(i);
    }
    this.writable.putDataset("distances", ary);

  }

  @Override
  public void close() throws Exception {
    this.writable.close();
  }

  public void writeFiltersStream(Iterator<PNode<?>> nodeIterator) {
    List<byte[]> predicateEncodings = new ArrayList<>();
    ByteBuffer workingBuffer = ByteBuffer.allocate(5_000_000);

    int maxlen = 0;
    int minlen = Integer.MAX_VALUE;
    while (nodeIterator.hasNext()) {
      PNode<?> node = nodeIterator.next();
      workingBuffer.clear();
      node.encode(workingBuffer);
      workingBuffer.flip();
      byte[] bytes = new byte[workingBuffer.remaining()];
      workingBuffer.get(bytes);
      predicateEncodings.add(bytes);
      maxlen = Math.max(maxlen,bytes.length);
      minlen = Math.min(minlen,bytes.length);
    }
    byte[][] encoded = new byte[predicateEncodings.size()][maxlen];
    for (int i = 0; i < encoded.length; i++) {
      encoded[i]=predicateEncodings.get(i);
    }
    this.writable.putDataset("filters", encoded);

  }
}
