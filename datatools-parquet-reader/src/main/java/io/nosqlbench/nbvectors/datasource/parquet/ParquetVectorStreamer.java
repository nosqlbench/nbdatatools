package io.nosqlbench.nbvectors.datasource.parquet;

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


import io.nosqlbench.nbdatatools.api.fileio.BoundedVectorFileStream;
import io.nosqlbench.nbdatatools.api.iteration.ConvertingIterable;
import io.nosqlbench.nbdatatools.api.iteration.FlatteningIterable;
import io.nosqlbench.nbdatatools.api.services.DataType;
import io.nosqlbench.nbdatatools.api.services.Encoding;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbvectors.datasource.parquet.conversion.ConverterType;
import io.nosqlbench.nbvectors.datasource.parquet.traversal.ParquetGroupIterable;
import io.nosqlbench.nbvectors.datasource.parquet.traversal.RecordReaderIterable;
import io.nosqlbench.nbvectors.datasource.parquet.traversal.functional.BoundedRecordReader;
import org.apache.parquet.example.data.Group;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;


/// Read vectors from parquet files. This layer of reading/parsing is expected to be applied to a
///  set of Paths which are files only, and which are part of a logical group.
@Encoding(FileType.parquet)
@DataType(float[].class)
public class ParquetVectorStreamer implements BoundedVectorFileStream<float[]> {

  private Iterable<float[]> compositeIterable;
  private List<Path> paths;
  private long size;
  private String converterType = "EMBEDDINGS_LIST_FLOAT"; // Default converter type

  public ParquetVectorStreamer() {
  }

  /**
   * Constructor with converter type
   * @param converterType The type of converter to use (e.g., "hfembed")
   */
  public ParquetVectorStreamer(String converterType) {
    this.converterType = converterType;
  }

  /**
   * Get the converter function based on the converter type
   * @return The converter function
   * @throws RuntimeException if the converter type is not supported
   */
  private Function<Group, float[]> getConverter() {
    try {
      return ConverterType.createConverter(converterType);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  @Override
  public void open(Path path) {
    open(List.of(path));
  }

  public void open(Path path, String converterType) {
    this.converterType = converterType;
    open(List.of(path));
  }

  public void open(List<Path> paths) {
    this.paths = paths;

    for (Path file : paths) {
      if (!Files.isRegularFile(file) && !Files.isSymbolicLink(file)) {
        throw new RuntimeException("unhandled file type for '" + file + "', only regular files "
                               + "and symlinks are allowed here");
      }
    }

    Iterable<BoundedRecordReader<Group>> recordReaderIterable =
        new FlatteningIterable<Path, BoundedRecordReader<Group>>(paths, RecordReaderIterable::new);

    Iterable<Group> groupIterable = new FlatteningIterable<BoundedRecordReader<Group>, Group>(
        recordReaderIterable,
        ParquetGroupIterable::new
    );

    Function<Group, float[]> embeddingDecoder = getConverter();

    this.compositeIterable =
        new ConvertingIterable<Group, float[]>(groupIterable, embeddingDecoder);
  }

  public void open(List<Path> paths, String converterType) {
    this.converterType = converterType;
    open(paths);
  }

  /**
   * Create a ParquetVectorStreamer with the default converter
   * @param paths The paths to the parquet files
   * @return A new ParquetVectorStreamer instance
   */
  public static ParquetVectorStreamer of(List<Path> paths) {
    ParquetVectorStreamer reader = new ParquetVectorStreamer();
    reader.open(paths);
    return reader;
  }

  /**
   * Create a ParquetVectorStreamer with a specific converter
   * @param paths The paths to the parquet files
   * @param converterType The type of converter to use
   * @return A new ParquetVectorStreamer instance
   */
  public static ParquetVectorStreamer of(List<Path> paths, String converterType) {
    ParquetVectorStreamer reader = new ParquetVectorStreamer(converterType);
    reader.open(paths);
    return reader;
  }

  @Override
  public Iterator<float[]> iterator() {
    return compositeIterable.iterator();
  }

  @Override
  public int getSize() {
    // Return -1 to indicate that the size is unknown without prescanning
    // This avoids the expensive prescan phase
    return -1;
  }

  @Override
  public String getName() {
    return "parquet";
  }

  @Override
  public void close() {
    // Close any resources held by the iterables
    if (compositeIterable instanceof AutoCloseable) {
      try {
        ((AutoCloseable) compositeIterable).close();
      } catch (Exception e) {
        // Log but don't rethrow to ensure other resources are closed
        System.err.println("Error closing composite iterable: " + e.getMessage());
      }
    }
  }
}
