package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal;

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


import org.apache.commons.io.file.AccumulatorPathVisitor;
import org.apache.commons.io.filefilter.WildcardFileFilter;

import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

/// parquet path traverser
public class PathAggregator {

  private final static WildcardFileFilter fileFilter =
      WildcardFileFilter.builder().setWildcards("*" + ".parquet").get();
  private final static WildcardFileFilter pathFilter =
      WildcardFileFilter.builder().setWildcards("*").get();
  private final Path root;
  private final boolean recurse;

  private AccumulatorPathVisitor pv =
      AccumulatorPathVisitor.withLongCounters(fileFilter, pathFilter);

  /// create a new path traverser
  /// @param root
  ///     the root path to traverse
  public PathAggregator(Path root, boolean recurse) {
    this.root = root;
    this.recurse = recurse;
    try {
      Set<FileVisitOption> options = Set.of();
      Files.walkFileTree(root, Set.of(FileVisitOption.FOLLOW_LINKS), recurse ? 100 : 1, pv);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /// get the size of the path and included matching files
  /// @return the size of the path in bytes
  public long getTotalSizeInBytes() {
    return pv.getPathCounters().getByteCounter().getLong();
  }

  /// get the root path
  /// @return the root path
  public Path getRootPath() {
    return root;
  }

  /// get the list of files in the path
  /// @return the list of files in the path
  public List<Path> getFileList() {
    return pv.getFileList();
  }

  /// get the list of directories in the path
  /// @return the list of directories in the path
  public List<Path> getDirList() {
    return pv.getDirList();
  }

}
