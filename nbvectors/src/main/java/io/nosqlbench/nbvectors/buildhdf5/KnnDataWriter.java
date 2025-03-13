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
import io.jhdf.api.WritableDataset;
import io.nosqlbench.nbvectors.buildhdf5.predicates.types.PNode;
import io.nosqlbench.nbvectors.spec.attributes.*;
import io.nosqlbench.nbvectors.spec.SpecDataSource;
import io.nosqlbench.nbvectors.spec.SpecDatasets;
import io.nosqlbench.nbvectors.verifyknn.datatypes.LongIndexedFloatVector;

import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/// A writer for KNN data in the HDF5 format
public class KnnDataWriter implements AutoCloseable {
  private final WritableHdfFile writable;
  private final SpecDataSource loader;

  /// create a new KNN data writer
  /// @param hdfOutPath
  ///     the path to the file to write to
  /// @param loader
  ///     the loader for the data
  public KnnDataWriter(Path hdfOutPath, SpecDataSource loader) {
    writable = HdfFile.write(hdfOutPath);
    this.loader = loader;
  }

  /// write the training vector data to a dataset
  /// @param iterator
  ///     an iterator for the training vectors
  public void writeBaseVectors(Iterator<LongIndexedFloatVector> iterator) {
    List<LongIndexedFloatVector> vectors = new ArrayList<>();
    iterator.forEachRemaining(vectors::add);
    float[][] ary = new float[vectors.size()][vectors.getFirst().vector().length];
    for (LongIndexedFloatVector vector : vectors) {
      ary[(int) vector.index()] = vector.vector();
    }
    WritableDataset writableDataset =
        this.writable.putDataset(SpecDatasets.base_vectors.name(), ary);
    this.writeAttributes(
        writableDataset, SpecDatasets.base_vectors, new BaseVectorAttributes(
            ary[0].length,
            ary.length,
            this.loader.getMetadata().model(),
            this.loader.getMetadata().distance_function()
        )
    );
    //
    //    // Record number of records in train dataset as an attribute
    //    this.writable.putAttribute(BaseVectorAttributes.count.name(), ary.length);
    //    // Record vector dimensionality (this will be the same for both train and test) as an attribute
    //    this.writable.putAttribute("dimensions", ary[0].length);
  }

  private <T extends Record> void writeAttributes(WritableDataset wds, SpecDatasets dstype, T attrs)
  {
    if (!dstype.getAttributesType().isAssignableFrom(attrs.getClass())) {
      throw new RuntimeException(
          "unable to assign attributes from " + attrs.getClass().getCanonicalName()
          + " to dataset for " + dstype.name());
    }

    try {
      if (attrs instanceof Record record) {
        RecordComponent[] comps = record.getClass().getRecordComponents();
        for (RecordComponent comp : comps) {
          String fieldname = comp.getName();
          Method accessor = comp.getAccessor();
          Object value = accessor.invoke(record);
          if (value instanceof Enum<?> e) {
            value = e.name();
          }
          wds.putAttribute( fieldname, value);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /// write the test vector data to a dataset
  /// @param iterator
  ///     an iterator for the test vectors
  public void writeQueryVectors(Iterator<LongIndexedFloatVector> iterator) {
    List<LongIndexedFloatVector> vectors = new ArrayList<>();
    iterator.forEachRemaining(vectors::add);
    float[][] ary = new float[vectors.size()][vectors.getFirst().vector().length];
    for (LongIndexedFloatVector vector : vectors) {
      ary[(int) vector.index()] = vector.vector();
    }
    //    WritableDataset ds = new WritableDatasetImpl(ary,"/train",writable);
    WritableDataset wds = this.writable.putDataset(SpecDatasets.query_vectors.name(), ary);
    writeAttributes(
        wds,
        SpecDatasets.query_vectors,
        new QueryVectorsAttributes(loader.getMetadata().model(), ary.length, ary[0].length)
    );
  }

  /// write the neighbors data to a dataset
  /// @param iterator
  ///     an iterator for the neighbors
  public void writeNeighborsIntStream(Iterator<int[]> iterator) {
    List<int[]> vectors = new ArrayList<>();
    iterator.forEachRemaining(vectors::add);
    int[][] ary = new int[vectors.size()][vectors.getFirst().length];
    for (int i = 0; i < ary.length; i++) {
      ary[i] = vectors.get(i);
    }
    WritableDataset wds = this.writable.putDataset(SpecDatasets.neighbor_indices.name(), ary);
    writeAttributes(
        wds,
        SpecDatasets.neighbor_indices,
        new NeighborIndicesAttributes(ary[0].length, ary.length)
    );
  }

  /// write the distances data to a dataset
  /// @param iterator
  ///     an iterator for the distances
  public void writeDistancesStream(Iterator<float[]> iterator) {
    List<float[]> distances = new ArrayList<>();
    iterator.forEachRemaining(distances::add);
    float[][] ary = new float[distances.size()][distances.getFirst().length];
    for (int i = 0; i < ary.length; i++) {
      ary[i] = distances.get(i);
    }
    WritableDataset wds = this.writable.putDataset(SpecDatasets.neighbor_distances.name(), ary);
    writeAttributes(
        wds,
        SpecDatasets.neighbor_distances,
        new NeighborDistancesAttributes(ary[0].length, ary.length)
    );

  }

  @Override
  public void close() throws Exception {
    this.writable.close();
  }

  /// write the filters data to a dataset
  /// @param nodeIterator
  ///     an iterator for the filters
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
      maxlen = Math.max(maxlen, bytes.length);
      minlen = Math.min(minlen, bytes.length);
    }
    byte[][] encoded = new byte[predicateEncodings.size()][maxlen];
    for (int i = 0; i < encoded.length; i++) {
      encoded[i] = predicateEncodings.get(i);
    }
    this.writable.putDataset(SpecDatasets.query_filters.name(), encoded);
  }

  /// write the metadata to the file
  /// @param metadata
  ///     the metadata to write
  public void writeRootMetadata(SpecAttributes metadata) {
    this.writable.putAttribute("model", metadata.model());
    this.writable.putAttribute("distance_function", metadata.distance_function().name());
    this.writable.putAttribute("url", metadata.url());
    if (metadata.notes().isPresent()) {
      this.writable.putAttribute("notes", metadata.notes().get());
    }
  }

  /// write the data to the file
  public void writeHdf5() {

    System.err.println("writing metadata...");
    writeRootMetadata(loader.getMetadata());

    System.err.println("writing base vectors stream...");
    writeBaseVectors(loader.getBaseVectors());

    System.err.println("writing test stream...");
    writeQueryVectors(loader.getQueryVectors());

    if (loader.getQueryFilters().isPresent()) {
      System.err.println("writing filters stream...");
      writeFiltersStream(loader.getQueryFilters().orElseThrow());
    }

    System.err.println("writing neighbors...");
    writeNeighborsIntStream(loader.getNeighborIndices());

    System.err.println("writing distances stream...");
    writeDistancesStream(loader.getNeighborDistances());

  }
}
