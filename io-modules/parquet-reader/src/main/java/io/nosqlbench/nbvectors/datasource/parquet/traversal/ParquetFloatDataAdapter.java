package io.nosqlbench.nbvectors.datasource.parquet.traversal;

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


import io.nosqlbench.nbvectors.api.commands.jjq.bulkio.iteration.FlatteningIterable;
import io.nosqlbench.nbvectors.datasource.parquet.ParquetVectorsReader;
import io.nosqlbench.nbvectors.datasource.parquet.layout.PathBinning;
import io.nosqlbench.nbvectors.datasource.parquet.layout.PathsSorter;
import io.nosqlbench.nbvectors.api.noncore.Sized;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/// Handle parquet data appropriately, depending on how it is specified
public class ParquetFloatDataAdapter implements Iterable<float[]>, Sized {

  private final List<Path> paths;
  private final Iterable<ParquetVectorsReader> parquetVectorsReaders;
  private final Iterable<float[]> iterable;

  /// create a new parquet data adapter
  /// @param paths the paths to the parquet data
  public ParquetFloatDataAdapter(List<Path> paths) {
    this.paths = paths;
    this.parquetVectorsReaders = composeAggregatorsIterable();
    iterable = new FlatteningIterable<ParquetVectorsReader, float[]>(parquetVectorsReaders, f -> f);
  }

  /// compose an iterable of parquet vectors readers
  /// @return an iterable of parquet vectors readers
  public Iterable<ParquetVectorsReader> composeAggregatorsIterable() {
    int dirs = 0;
    int files = 0;
    for (Path path : paths) {
      if (Files.isDirectory(path)) {
        dirs++;
      } else if (Files.isRegularFile(path) || Files.isSymbolicLink(path)) {
        files++;
      } else {
        throw new RuntimeException("unhandled file type for '" + path + "'");
      }
    }
    if (dirs != 0 && files != 0) {
      throw new RuntimeException("unable to handle mixed dirs (" + dirs + ") and files (" + files
                                 + "). Either pass dirs which represent groupings, or files which represent the contents of a single grouping.");
    }
    if (files > 0) {
      return List.of(new ParquetVectorsReader(paths));
      //      aggregators.add(new ParquetVectorsReader(paths));
    } else {
      PathBinning binning = new PathBinning(paths);
      PathBinning.BinningResult bins = binning.getBins();
      Map<Path, List<Path>> parentGroups = bins.toParentGroups();

      PathsSorter sorter = new PathsSorter(parentGroups.keySet(), true);
      PathsSorter.SortedResults sorted =
          sorter.sorted(PathsSorter.BY_REVERSE_TOTAL_SIZE, PathsSorter.BY_NAME);
      System.out.println("sorted:\n");
      sorted.forEach(s -> System.out.println(s.getRootPath()));

      return sorted.stream()
          .map(a -> new ParquetVectorsReader(a.getFileList().stream().sorted().toList())).toList();
    }
  }

  @Override
  public Iterator<float[]> iterator() {
    return iterable.iterator();
  }

  @Override
  public int getSize() {
    long total = 0;

    for (ParquetVectorsReader parquetVectorsReader : this.parquetVectorsReaders) {
      if (parquetVectorsReader instanceof Sized sized) {
        total += sized.getSize();
      }
    }
    if (total > Integer.MAX_VALUE) {
      throw new RuntimeException("int overflow on long size: " + total);
    }
    return (int) total;
  }
}
