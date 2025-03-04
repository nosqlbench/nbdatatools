package io.nosqlbench.nbvectors.buildhdf5;

/*
 * Copyright (c) nosqlbench
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


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
    // Record number of records in train dataset as an attribute
    this.writable.putAttribute("train_vectors", ary.length);
    // Record vector dimensionality (this will be the same for both train and test) as an attribute
    this.writable.putAttribute("dimensions", ary[0].length);
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
    // Record number of records in test dataset as an attribute
    this.writable.putAttribute("test_vectors", ary.length);
  }

  public void writeNeighborsStream(Iterator<long[]> iterator) {
    List<long[]> vectors = new ArrayList<>();
    iterator.forEachRemaining(vectors::add);
    long[][] ary = new long[vectors.size()][vectors.getFirst().length];
    for (int i = 0; i < ary.length; i++) {
      ary[i] = vectors.get(i);
    }
    this.writable.putDataset("neighbors", ary);
    // Record many neighbors were computed for each vector as an attribute
    this.writable.putAttribute("neighbors", ary[0].length);
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

  public void writeMetadata(MapperConfig config) {
    // The name of the model used to generate the data, if any
    this.writable.putAttribute("model", config.getModel());
    // The name of the distance function used to compute distance between vectors
    this.writable.putAttribute("distance_function", config.getDistanceFunction());
  }
}
