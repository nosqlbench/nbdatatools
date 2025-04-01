package io.nosqlbench.nbvectors.common.parquet;

import org.apache.commons.io.file.AccumulatorPathVisitor;
import org.apache.commons.io.filefilter.WildcardFileFilter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/// parquet path traverser
public class PathTraverser {

  private final static WildcardFileFilter fileFilter =
      WildcardFileFilter.builder().setWildcards("*" + ".parquet").get();
  private final static WildcardFileFilter pathFilter =
      org.apache.commons.io.filefilter.WildcardFileFilter.builder().setWildcards("*").get();
  private final Path root;

  private AccumulatorPathVisitor pv =
      AccumulatorPathVisitor.withLongCounters(fileFilter, pathFilter);

  /// create a new path traverser
  /// @param root the root path to traverse
  public PathTraverser(Path root) {
    this.root = root;
    try {
      Files.walkFileTree(root, pv);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  /// get the size of the path and included matching files
  /// @return the size of the path in bytes
  public long getSize() {
    return pv.getPathCounters().getByteCounter().getLong();
  }

  /// get the root path
  /// @return the root path
  public Path getRoot() {
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
