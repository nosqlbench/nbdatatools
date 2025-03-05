package io.nosqlbench.nbvectors.jjq.bulkio;

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


import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/// a partitioner of lines from a file
public class LinePartitioner implements Iterable<LinePartitioner.Extent> {

  private final List<Extent> extents = new ArrayList<>();

  /// create a line partitioner
  /// @param path the path to the file to partition
  /// @param startIncl the starting offset, inclusive
  /// @param endExcl the ending offset, exclusive
  /// @param partitions the number of partitions to create
  public LinePartitioner(Path path, long startIncl, long endExcl, int partitions) {
    try {
      FileChannel channel = FileChannel.open(path);
      long startAt = startIncl;
      long endAt = Math.min(endExcl, channel.size());
      if (endExcl == 0) {
        endExcl = channel.size();
      }
      extents.addAll(new Extent(startAt, endAt).partition(channel,partitions));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /// get the extents
  @Override
  public Iterator<Extent> iterator() {
    return extents.iterator();
  }

  /// an extent of a file
  /// @param start the starting offset, inclusive
  /// @param end the ending offset, exclusive
  public record Extent(long start, long end) {
    /// partition the extent into a list of extents
    /// @param br the file channel to read from
    /// @param partitions the number of partitions to create
    /// @return a list of extents
    public List<Extent> partition(FileChannel br, int partitions) {
      return null;
    }
  }
}
