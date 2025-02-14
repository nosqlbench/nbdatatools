package io.nosqlbench.nbvectors.buildhdf5;

import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import io.nosqlbench.nbvectors.verifyknn.datatypes.LongIndexedFloatVector;

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
//    WritableDataset ds = new WritableDatasetImpl(ary,"/training",writable);
    this.writable.putDataset("train",ary);
  }

  public void writeTestStream(Iterator<LongIndexedFloatVector> iterator) {
    List<LongIndexedFloatVector> vectors = new ArrayList<>();
    iterator.forEachRemaining(vectors::add);
    float[][] ary = new float[vectors.size()][vectors.getFirst().vector().length];
    for (LongIndexedFloatVector vector : vectors) {
      ary[(int) vector.index()] = vector.vector();
    }
    //    WritableDataset ds = new WritableDatasetImpl(ary,"/training",writable);
    this.writable.putDataset("test",ary);
  }

  public void writeNeighborsStream(Iterator<long[]> iterator) {
    List<long[]> vectors = new ArrayList<>();
    iterator.forEachRemaining(vectors::add);
    long[][] ary = new long[vectors.size()][vectors.getFirst().length];
    for (int i = 0; i < ary.length; i++) {
      ary[i]=vectors.get(i);
    }
    this.writable.putDataset("neighbors",ary);
  }

  public void writeDistancesStream(Iterator<float[]> iterator) {
    List<float[]> distances = new ArrayList<>();
    iterator.forEachRemaining(distances::add);
    float[][] ary = new float[distances.size()][distances.getFirst().length];
    for (int i = 0; i < ary.length; i++) {
      ary[i]=distances.get(i);
    }
    this.writable.putDataset("distances",ary);

  }

  @Override
  public void close() throws Exception {
    this.writable.close();
  }

}
