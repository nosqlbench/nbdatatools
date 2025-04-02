package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet;

import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal.functional.BoundedPageStore;
import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal.PathAggregator;
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

  /// visit the start of the traversal
  default void beforeAll() {};

  /// visit the start of a root path
  /// @param path the root path to visit
  default void beforeRoot(PathAggregator path) {};

  /// visit the start of an input file
  /// @param inputFile the input file to visit
  default void beforeInputFile(InputFile inputFile) {};

  /// visit the start of a page
  /// @param pageStore the page store to visit
  default void beforePage(BoundedPageStore pageStore) {};

  /// visit the end of a group
  /// @param group the group to visit
  default void onGroup(Group group) {};

  /// visit the end of a page
  /// @param pageStore the page store to visit
  /// @see #beforePage
  default void afterPage(BoundedPageStore pageStore) {};

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
