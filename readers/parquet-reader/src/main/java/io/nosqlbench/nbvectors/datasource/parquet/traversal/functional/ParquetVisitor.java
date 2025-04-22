package io.nosqlbench.nbvectors.datasource.parquet.traversal.functional;

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


import io.nosqlbench.nbvectors.datasource.parquet.layout.PathAggregator;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.InputFile;

/// implementors of this interface know how to visit all levels of parquet types
/// @see ParquetTraversal
public interface ParquetVisitor {

  /// The depth of traversal to perform
  public enum Depth {
    /// no traversal
    NONE,
    /// traverse only the book-ends of the traversal call
    CALL,
    /// traverse the root paths
    ROOTS,
    /// traverse the input files
    FILES,
    /// traverse the pages
    PAGES,
    /// traverse the groups
    GROUPS;

    /// determine if this depth is enabled for the given depth
    /// @param depth the depth to check
    /// @return true if this depth is enabled for the given depth
    public boolean isEnabledFor(Depth depth) {
      return this.ordinal() >= depth.ordinal();
    }
  }

  ;

  /// get the depth of traversal to perform
  /// @return the depth of traversal to perform
  default Depth getTraversalDepth() {
    return Depth.GROUPS;
  };

  /// visit the startInclusive of the traversal
  default void beforeAll() {};

  /// visit the startInclusive of a root path
  /// @param path the root path to visit
  default void beforeRoot(PathAggregator path) {};

  /// visit the startInclusive of an input file
  /// @param inputFile the input file to visit
  default void beforeInputFile(InputFile inputFile) {};

  /// visit the startInclusive of a page
  /// @param pageStore the page store to visit
  default void beforePage(io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal.functional.BoundedPageStore pageStore) {};

  /// visit the end of a group
  /// @param group the group to visit
  default void onGroup(Group group) {};

  /// visit the end of a page
  /// @param pageStore the page store to visit
  /// @see #beforePage
  default void afterPage(io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal.functional.BoundedPageStore pageStore) {};

  /// visit the end of an input file
  /// @param inputFile the input file to visit
  /// @see #beforeInputFile
  default void afterInputFile(InputFile inputFile) {};

  /// visit the end of a root path
  /// @param path the root path to visit
  /// @see #beforeRoot
  default void afterRoot(PathAggregator path) {};

  /// visit the end of the traversal
  /// @see #beforeAll
  default void afterAll() {};

}
