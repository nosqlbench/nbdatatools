package io.nosqlbench.nbvectors.jjq.bulkio;

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
