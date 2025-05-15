package io.nosqlbench.nbvectors.commands.build_hdf5.writers;

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


import com.google.gson.Gson;
import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import io.jhdf.api.WritableDataset;
import io.jhdf.api.WritableNode;
import io.nosqlbench.nbvectors.api.fileio.Sized;
import io.nosqlbench.vectordata.discovery.TestDataGroup;
import io.nosqlbench.vectordata.spec.attributes.BaseVectorAttributes;
import io.nosqlbench.vectordata.spec.attributes.NeighborDistancesAttributes;
import io.nosqlbench.vectordata.spec.attributes.NeighborIndicesAttributes;
import io.nosqlbench.vectordata.spec.attributes.QueryVectorsAttributes;
import io.nosqlbench.vectordata.spec.attributes.RootGroupAttributes;
import io.nosqlbench.vectordata.spec.predicates.PNode;
import io.nosqlbench.nbvectors.commands.build_hdf5.datasource.ArrayChunkingIterable;
import io.nosqlbench.nbvectors.api.commands.jjq.bulkio.iteration.ConvertingIterable;
import io.nosqlbench.nbvectors.common.FilePaths;
import io.nosqlbench.nbvectors.common.jhdf.StreamableDataset;
import io.nosqlbench.nbvectors.common.jhdf.StreamableDatasetImpl;
import io.nosqlbench.vectordata.spec.tokens.SpecDataSource;
import io.nosqlbench.vectordata.spec.datasets.types.TestDataKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;

/// A writer for KNN data in the HDF5 format
public class KnnDataWriter {
  private final static Logger logger = LogManager.getLogger(KnnDataWriter.class);

  private final String outTemplate;
  private WritableHdfFile writable;
  private final SpecDataSource loader;
  private final Path tempFile;
  private QueryVectorsAttributes queryVectorsAttributes;
  private BaseVectorAttributes baseVectorAttributes;
  private NeighborDistancesAttributes neighborDistancesAttributes;
  private NeighborIndicesAttributes neighborIndicesAttributes;
  private RootGroupAttributes rootGroupAttributes;

  /// create a new KNN data writer
  /// @param outfileTemplate
  ///     the path to the file to write to
  /// @param loader
  ///     the loader for the data
  public KnnDataWriter(String outfileTemplate, SpecDataSource loader) {
    try {
      Path dir = Path.of(".").toAbsolutePath();
      this.tempFile = Files.createTempFile(
          dir,
          ".hdf5buffer",
          ".hdf5",
          PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-xr-x"))
      );
      this.outTemplate = outfileTemplate;
      this.writable = HdfFile.write(tempFile);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.loader = loader;
  }

  /// write the training vector data to a dataset
  /// @param iterable
  ///     an iterator for the training vectors
  public void writeBaseVectors(Iterable<float[]> iterable) {

    //    ConvertingIterable<LongIndexedFloatVector, float[]> remapper =
    //        new ConvertingIterable<LongIndexedFloatVector, float[]>(
    //            iterable,
    //            LongIndexedFloatVector::vector
    //        );

    Class<? extends float[]> fclass = new float[0].getClass();
    ArrayChunkingIterable aci = new ArrayChunkingIterable(fclass, iterable, 1024 * 1024 * 512);

    StreamableDataset streamer =
        new StreamableDatasetImpl(aci, TestDataKind.base_vectors.name(), this.writable);

    if (iterable instanceof Sized sized) {
      streamer.modifyDimensions(new int[]{sized.getSize()});
    } else {
      logger.warn("no dimensions are calculated for this dataset during writing");
      throw new RuntimeException("Dimensions are required to be known at time of writing. "
                                 + "Labeling and dataset inventory need this data.");
    }

    WritableDataset wds = this.writable.putDataset(TestDataKind.base_vectors.name(), streamer);

    // TODO: remove dimensions from attributes because they are shape data
    this.baseVectorAttributes = new BaseVectorAttributes(
        wds.getDimensions()[0],
        wds.getDimensions()[1],
        this.loader.getMetadata().model(),
        this.loader.getMetadata().distance_function()
    );

    this.writeAttributes(wds, TestDataKind.base_vectors, baseVectorAttributes);
  }

  /// write the test vector data to a dataset
  /// @param iterator
  ///     an iterator for the test vectors
  public void writeQueryVectors(Iterable<float[]> iterator) {

    Class<? extends float[]> fclass = new float[0].getClass();
    ArrayChunkingIterable aci = new ArrayChunkingIterable(fclass, iterator, 1024 * 1024 * 512);

    StreamableDatasetImpl streamer =
        new StreamableDatasetImpl(aci, TestDataKind.query_vectors.name(), this.writable);

    if (iterator instanceof Sized sized) {
      streamer.modifyDimensions(new int[]{sized.getSize()});
    }

    WritableDataset wds = this.writable.putDataset(TestDataKind.query_vectors.name(), streamer);

    this.queryVectorsAttributes = new QueryVectorsAttributes(
        loader.getMetadata().model(),
        wds.getDimensions()[0],
        wds.getDimensions()[1]
    );
    writeAttributes(wds, TestDataKind.query_vectors, queryVectorsAttributes);
  }

  /// write the neighbors data to a dataset, including all values available in the iterator
  /// @param iterable
  ///     an iterator for the neighbors
  public void writeNeighborsIntStream(Iterable<int[]> iterable) {
    this.writeNeighborsIntStream(iterable, "default", -1L, -1L);
  }


  /// write the neighbors data to a dataset, using the major coordinate intervals
  /// {@code [startInclusive, end)}
  /// @param iterable
  ///     an iterator for the neighbors
  /// @param configName
  ///     the name of the config to use for the dataset. This is analogous to the config concept
  ///         from huggingface datasets. If there is only one config, then this should be set to
  ///        "default"
  /// @param start
  ///     the startInclusive of the major coordinate intervals, inclusive
  /// @param end
  ///     the end of the major coordinate intervals, exclusive
  /// @see #writeNeighborsIntStream(Iterable)
  public void writeNeighborsIntStream(
      Iterable<int[]> iterable,
      String configName,
      long start,
      long end
  )
  {
    Class<? extends int[]> fclass = new int[0].getClass();
    ArrayChunkingIterable aci = new ArrayChunkingIterable(fclass, iterable, 1024 * 1024 * 512);
    StreamableDatasetImpl streamer =
        new StreamableDatasetImpl(aci, TestDataKind.neighbor_indices.name(), this.writable);

    if (iterable instanceof Sized sized) {
      streamer.modifyDimensions(new int[]{sized.getSize()});
    }

    WritableDataset wds = this.writable.putDataset(TestDataKind.neighbor_indices.name(), streamer);

    this.neighborIndicesAttributes =
        new NeighborIndicesAttributes(wds.getDimensions()[0], wds.getDimensions()[1]);
    writeAttributes(wds, TestDataKind.neighbor_indices, neighborIndicesAttributes);
  }

  /// write the distances data to a dataset
  /// @param iterator
  ///     an iterator for the distances
  public void writeDistancesStream(Iterable<float[]> iterator) {

    Class<? extends float[]> fclass = new float[0].getClass();
    ArrayChunkingIterable aci = new ArrayChunkingIterable(fclass, iterator, 1024 * 1024 * 512);
    StreamableDatasetImpl streamer =
        new StreamableDatasetImpl(aci, TestDataKind.neighbor_distances.name(), this.writable);

    if (iterator instanceof Sized sized) {
      streamer.modifyDimensions(new int[]{sized.getSize()});
    }

    WritableDataset wds =
        this.writable.putDataset(TestDataKind.neighbor_distances.name(), streamer);

    this.neighborDistancesAttributes =
        new NeighborDistancesAttributes(wds.getDimensions()[0], wds.getDimensions()[1]);
    writeAttributes(wds, TestDataKind.neighbor_distances, neighborDistancesAttributes);

  }

  /// write the filters data to a dataset
  /// @param iterable
  ///     an Iterable for the filters
  /// @see PNode
  public void writeFiltersStream(Iterable<PNode<?>> iterable) {

    ByteBuffer workingBuffer = ByteBuffer.allocate(5_000_000);
    ConvertingIterable<PNode<?>, byte[]> remapper = new ConvertingIterable<PNode<?>, byte[]>(
        iterable, node -> {
      workingBuffer.clear();
      node.encode(workingBuffer);
      workingBuffer.flip();
      byte[] bytes = new byte[workingBuffer.remaining()];
      workingBuffer.get(bytes);
      return bytes;
    }
    );

    Class<? extends byte[]> fclass = new byte[0].getClass();
    ArrayChunkingIterable aci = new ArrayChunkingIterable(fclass, remapper, 1024 * 1024 * 512);

    StreamableDatasetImpl streamer =
        new StreamableDatasetImpl(aci, TestDataKind.query_filters.name(), this.writable);

    if (iterable instanceof Sized sized) {
      streamer.modifyDimensions(new int[]{sized.getSize()});
    }

    WritableDataset wds = this.writable.putDataset(TestDataKind.query_filters.name(), streamer);
  }


  /// write the data to the file
  public void writeHdf5() {
    try {
      this.writable = HdfFile.write(tempFile);

      loader.getBaseVectors().ifPresent(baseVectors -> {
        System.err.println("writing base vectors...");
        writeBaseVectors(baseVectors);
      });

      loader.getQueryVectors().ifPresent(queryVectors -> {
        System.err.println("writing query vectors...");
        writeQueryVectors(queryVectors);
      });

      loader.getQueryFilters().ifPresent(queryFilters -> {
        System.err.println("writing query filters...");
        writeFiltersStream(queryFilters);
      });

      loader.getNeighborIndices().ifPresent(neighborIndices -> {
        System.err.println("writing neighbors indices...");
        writeNeighborsIntStream(neighborIndices);
      });

      loader.getNeighborDistances().ifPresent(neighborDistances -> {
        System.err.println("writing neighbor distances...");
        writeDistancesStream(neighborDistances);
      });

      writeRootGroupAttributes();
      this.writable.close();
      relinkFile();

    } catch (Exception e) {
      rmTempFile(tempFile);
      throw new RuntimeException(e);
    }
  }

  private void relinkFile() {
    TestDataGroup vectorData = new TestDataGroup(tempFile);
    Path filePath = vectorData.tokenize(this.outTemplate.toString()).map(Path::of)
        .orElseThrow(() -> new RuntimeException("error tokenizing file"));
    Path newPath = FilePaths.relinkPath(tempFile, filePath);
    logger.debug("moved {} to {}", tempFile, newPath);

  }

  private void rmTempFile(Path tempFile) {
    if (Files.exists(tempFile)) {
      try {
        Files.delete(tempFile);
      } catch (IOException ignored) {
      }
    }
  }

  private void writeRootGroupAttributes() {
    System.err.println("writing metadata...");
    this.rootGroupAttributes = new RootGroupAttributes(
        loader.getMetadata().model(),
        loader.getMetadata().url(),
        loader.getMetadata().distance_function(),
        loader.getMetadata().notes(),
        loader.getMetadata().license(),
        loader.getMetadata().vendor(),
        loader.getMetadata().tags()
    );
    this.writeAttributes(this.writable, null, rootGroupAttributes);

  }


  private <T extends Record> void writeAttributes(WritableNode wnode, TestDataKind dstype, T attrs)
  {
    if (dstype != null && !dstype.getAttributesType().isAssignableFrom(attrs.getClass())) {
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

          if (value instanceof Optional<?> o) {
            if (o.isPresent()) {
              value = o.get();
            } else {
              continue;
            }
          }

          if (value instanceof Enum<?> e) {
            value = e.name();
          }
          if (value instanceof Map || value instanceof List || value instanceof Set) {
            value = new Gson().toJson(value);
          }
          if (value == null) {
            throw new RuntimeException(
                "attribute value for requied attribute " + fieldname + " " + "was null");
          }
          wnode.putAttribute(fieldname, value);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private int sizeOf(Object row0) {
    if (row0 instanceof int[] ia) {
      return ia.length * Integer.BYTES;
    } else if (row0 instanceof byte[] ba) {
      return ba.length * Byte.BYTES;
    } else if (row0 instanceof short[] sa) {
      return sa.length * Short.BYTES;
    } else if (row0 instanceof long[] la) {
      return la.length * Long.BYTES;
    } else if (row0 instanceof float[] fa) {
      return fa.length * Float.BYTES;
    } else if (row0 instanceof double[] da) {
      return da.length * Double.BYTES;
    } else {
      throw new RuntimeException("Unknown type for sizing:" + row0.getClass());
    }
  }

}
