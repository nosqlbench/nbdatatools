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


import io.nosqlbench.nbdatatools.api.fileio.BoundedVectorFileStream;
import io.nosqlbench.nbdatatools.api.iteration.FlatteningIterable;
import io.nosqlbench.nbdatatools.api.services.DataType;
import io.nosqlbench.nbdatatools.api.services.Encoding;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbvectors.datasource.parquet.ParquetVectorStreamer;
import io.nosqlbench.nbvectors.datasource.parquet.layout.PathBinning;
import io.nosqlbench.nbvectors.datasource.parquet.layout.PathsSorter;
import io.nosqlbench.nbvectors.datasource.parquet.traversal.functional.ParquetTraversal;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/// Handle parquet data appropriately, depending on how it is specified
@Encoding(FileType.parquet)
@DataType(float[].class)
public class ParquetFloatDataAdapter implements BoundedVectorFileStream<float[]> {

  private List<Path> paths;
  private Iterable<ParquetVectorStreamer> parquetVectorsReaders;
  private Iterable<float[]> iterable;
  private Path mainPath;
  private ParquetScanResult scanResult;

  public ParquetFloatDataAdapter() {
  }

  public void open(Path path) {
    this.mainPath = path;
    open(List.of(mainPath));
  }

  /// create a new parquet data adapter
  /// @param paths
  ///     the paths to the parquet data
  private void open(List<Path> paths) {
    this.paths = paths;

    // Skip pre-scanning phase to avoid expensive traversal
    this.scanResult = null;

    this.parquetVectorsReaders = composeAggregatorsIterable();
    iterable =
        new FlatteningIterable<ParquetVectorStreamer, float[]>(parquetVectorsReaders, f -> f);
  }

  /// Perform a pre-scanning phase to determine the total number of records in the file tree
  /// @param paths the paths to scan
  /// @return a ParquetScanResult containing the total record count
  private ParquetScanResult prescanParquetFiles(List<Path> paths) {
    ParquetTabulator tabulator = new ParquetTabulator();
    ParquetTraversal traversal = new ParquetTraversal(paths, 10);
    traversal.traverse(tabulator);
    long totalRecords = tabulator.getRecordCount();
    return new ParquetScanResult(totalRecords);
  }

  /// compose an iterable of parquet vectors readers
  /// @return an iterable of parquet vectors readers
  public Iterable<ParquetVectorStreamer> composeAggregatorsIterable() {
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
      return List.of(ParquetVectorStreamer.of(paths));
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
          .map(a -> ParquetVectorStreamer.of(a.getFileList().stream().sorted().collect(java.util.stream.Collectors.toList()))).collect(java.util.stream.Collectors.toList());
    }
  }

  @Override
  public Iterator<float[]> iterator() {
    return iterable.iterator();
  }

  @Override
  public int getSize() {
    if (scanResult == null) {
      // Return -1 to indicate that the size is unknown without prescanning
      return -1;
    }
    return scanResult.getTotalRecordsAsInt();
  }

  @Override
  public String getName() {
    return (mainPath!=null) ? mainPath.toString() : this.getClass().getCanonicalName()+":NOPATH";
  }
}
