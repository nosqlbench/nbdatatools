package io.nosqlbench.nbvectors.jsonalyze;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

public class LinePartitioner implements Iterable<LinePartitioner.Extent> {
  private final FileChannel channel;
  public long lastOffset;
  private final int maxBufSize;

  public LinePartitioner(Path path, long startIncl, long endExcl, int partitions) {
    try {
      BufferedReader br = Files.newBufferedReader(path);
      Extent extent = new Extent(startIncl, endExcl);
      List<Extent> extents = extent.partition(br, partitions);
      long sumlen = 0;
      int sumcount = 0;
      String line;
      while ((line = br.readLine()) != null && sumcount < 32) {
        sumlen += line.length();
        sumcount++;
      }
      this.maxBufSize = ((int) (sumlen / sumcount)) * linesPerChunk;
      this.channel = FileChannel.open(path);
      channel.position(startIncl);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    lastOffset = startIncl;
  }

  @Override
  public Iterator<CharBuffer> iterator() {
    return new LineChunkIterator();
  }

  private class LineChunkIterator implements Iterator<CharBuffer> {
    @Override
    public boolean hasNext() {
      try {
        return (channel.position() < channel.size());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public CharBuffer next() {
      ByteBuffer buffer = ByteBuffer.allocate((int) maxBufSize);
      try {
        int readSize = channel.read(buffer);
        if (readSize == 0)
          return null;
        if (channel.position() != channel.size()) {
          for (int i = buffer.position() - 1; i > 0; i--) {
            byte c = buffer.get(i);
            if (c == '\n') {
              // remove newline
              int extra = buffer.position() - (i + 1);
              buffer.limit(i);
              channel.position(channel.position() - extra);
              break;
            }
          }
        }
        buffer.flip();
        return StandardCharsets.UTF_8.decode(buffer);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
  public static record Extent(long start, long end) {
    public List<Extent> partition(BufferedReader br, int partitions) {
      return null;
    }
  }
}
