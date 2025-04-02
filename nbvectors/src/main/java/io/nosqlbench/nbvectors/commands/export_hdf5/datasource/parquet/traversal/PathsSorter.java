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


import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/// Sorts base paths by reverse size, then by name
public class PathsSorter implements Iterable<PathAggregator> {
  private final List<PathAggregator> entries = new ArrayList<>();

  /// create a new path sorter
  /// @param paths
  ///     the root path to sort
  /// @see PathAggregator
  public PathsSorter(List<Path> paths, boolean recurse) {
    // TODO resolve differences between directory stream and path traversal
    for (Path path : paths) {
      PathAggregator traverser = new PathAggregator(path, recurse);
      this.entries.add(traverser);
    }
    //    entries.sort(comparator);
  }
  public PathsSorter(Collection<? extends Path> paths) {
    this(paths, true);
  }
  public PathsSorter(Collection<? extends Path> paths, boolean recurse) {
    this(new ArrayList<>(paths),recurse);
  }

  public SortedResults sorted() {
    return sorted(PathsSorter.BY_REVERSE_TOTAL_SIZE, PathsSorter.BY_NAME);
  }
  public SortedResults sorted(Comparator<PathAggregator>... inOrder) {
    OrderedComparator oc = new OrderedComparator(inOrder);
    entries.sort(oc);
    return new SortedResults(entries);
  }

  public final static class SortedResults extends ArrayList<PathAggregator> {
    public SortedResults(List<? extends PathAggregator> c) {
      super(c);
    }
    public List<Path> toPaths() {
      return stream().map(PathAggregator::getRootPath).toList();
    };
  }

  @Override
  public Iterator<PathAggregator> iterator() {
    return entries.iterator();
  }

  public final static Comparator<PathAggregator> BY_REVERSE_TOTAL_SIZE =
      Collections.reverseOrder(Comparator.comparingLong(PathAggregator::getTotalSizeInBytes));
  public final static Comparator<PathAggregator> BY_TOTAL_SIZE =
      Comparator.comparingLong(PathAggregator::getTotalSizeInBytes);
  public final static Comparator<PathAggregator> BY_NAME =
      Comparator.comparing(pt -> pt.getRootPath().toString());

  public static class OrderedComparator implements Comparator<PathAggregator> {
    private final Comparator<PathAggregator>[] comparators;

    public OrderedComparator(Comparator<PathAggregator>... comparators) {
      this.comparators = comparators;
    }

    @Override
    public int compare(PathAggregator o1, PathAggregator o2) {
      for (Comparator<PathAggregator> comparator : comparators) {
        int result = comparator.compare(o1, o2);
        if (result != 0) {
          return result;
        }
      }
      return 0;
    }
  }
}
