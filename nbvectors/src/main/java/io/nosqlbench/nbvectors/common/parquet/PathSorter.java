package io.nosqlbench.nbvectors.common.parquet;

import smile.io.Parquet;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/// Sorts base paths by reverse size, then by name
public class PathSorter implements Iterable<Path> {
  private final List<PathTraverser> entries = new ArrayList<>();
  private final Comparator<PathTraverser> comparator = new SizeReversedThenNameComparator();

  /// create a new path sorter
  /// @param root the root path to sort
  /// @param glob the glob to use for matching base entries
  /// @see PathTraverser
  public PathSorter(Path root, String glob) {
    try {
      DirectoryStream<Path> stream = Files.newDirectoryStream(root, glob);
      for (Path path : stream) {
        PathTraverser traverser = new PathTraverser(path);
        this.entries.add(traverser);
      }
      entries.sort(comparator);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  /// get the ordered paths
  /// @return the ordered paths
  public List<Path> getOrderedPaths() {
    return entries.stream().map(PathTraverser::getRoot).toList();
  }

  @Override
  public Iterator<Path> iterator() {
    List<Path> mapped = getOrderedPaths();
    return mapped.iterator();
  }

  /// A comparator which sorts by reverse size, then by name
  /// @see PathTraverser
  public final static class SizeReversedThenNameComparator implements Comparator<PathTraverser> {
    private static Comparator<PathTraverser> sizeComparator =
        Collections.reverseOrder(Comparator.comparingLong(PathTraverser::getSize));
    private static Comparator<PathTraverser> nameComparator =
        Comparator.comparing(pt -> pt.getRoot().toString());

    @Override
    public int compare(PathTraverser o1, PathTraverser o2) {
      int result = sizeComparator.compare(o1, o2);
      if (result != 0)
        return result;

      return nameComparator.compare(o1, o2);
    }
  }
}
