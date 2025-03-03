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

public class LinePartitioner implements Iterable<LinePartitioner.Extent> {

  private final List<Extent> extents = new ArrayList<>();

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

  @Override
  public Iterator<Extent> iterator() {
    return extents.iterator();
  }

  public static record Extent(long start, long end) {
    public List<Extent> partition(FileChannel br, int partitions) {
      return null;
    }
  }
}
