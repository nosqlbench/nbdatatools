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


import io.nosqlbench.nbvectors.api.commands.jjq.bulkio.iteration.ConvertingIterable;
import io.nosqlbench.nbvectors.api.commands.jjq.bulkio.iteration.FlatteningIterable;
import io.nosqlbench.nbvectors.api.fileio.BoundedVectorFileStream;
import io.nosqlbench.nbvectors.api.services.DataType;
import io.nosqlbench.nbvectors.api.services.Encoding;
import io.nosqlbench.nbvectors.api.services.FileType;
import io.nosqlbench.nbvectors.datasource.parquet.conversion.HFEmbedToFloatAry;
import io.nosqlbench.nbvectors.datasource.parquet.traversal.ParquetGroupIterable;
import io.nosqlbench.nbvectors.datasource.parquet.traversal.ParquetTabulator;
import io.nosqlbench.nbvectors.datasource.parquet.traversal.RecordReaderIterable;
import io.nosqlbench.nbvectors.datasource.parquet.traversal.functional.BoundedRecordReader;
import io.nosqlbench.nbvectors.datasource.parquet.traversal.functional.ParquetTraversal;
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
public class ParquetVectorsReader implements BoundedVectorFileStream<float[]> {

  private Iterable<float[]> compositeIterable;
  private List<Path> paths;
  private long size;

  public ParquetVectorsReader() {
  }

  @Override
  public void open(Path path) {
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

    Function<Group , float[]> embeddingDecoder = new HFEmbedToFloatAry();

    this.compositeIterable =
        new ConvertingIterable<Group, float[]>(groupIterable, embeddingDecoder);

  }

  public static ParquetVectorsReader of(List<Path> paths) {
    ParquetVectorsReader reader = new ParquetVectorsReader();
    reader.open(paths);
    return reader;
  }

  @Override
  public Iterator<float[]> iterator() {
    return compositeIterable.iterator();
  }

  @Override
  public int getSize() {
    if (size == 0) {
      ParquetTabulator tabulator = new ParquetTabulator();
      ParquetTraversal traversal = new ParquetTraversal(paths, 10);
      traversal.traverse(tabulator);
      this.size = tabulator.getRecordCount();
    }
    if (size > Integer.MAX_VALUE) {
      throw new RuntimeException("int overflow with long size:" + size);
    }
    return (int) size;
  }

  @Override
  public String getName() {
    return "parquet";
  }

}
