package io.nosqlbench.nbvectors.jjq.bulkio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

public record FilePartition(Path path, long start, long end) {

  public static FilePartition of(String path) {
    return FilePartition.of(Path.of(path));
  }

  public static FilePartition of(Path path) {
    try {
      return new FilePartition(path, 0, Files.size(path));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /// Break a file up into partitions on newline boundaries
  /// while ensuring that each boundary is less than the size
  /// you can mmap with Java's 2
  public FilePartitions partition(int minPartitions) {
    long len = this.end - this.start;
    long minP = len / 2000000000;
    long partitions = Math.max(minPartitions, (Math.max(1, len / 2000000000)));
    long psize = len / partitions;

    FilePartitions extents = new FilePartitions();
    try {
      FileChannel channel = FileChannel.open(path);
      ByteBuffer buf = ByteBuffer.allocate(1024);
      long pstart = start;

      for (long part = 0; part < minPartitions; part++) {
        long pend = Math.min(pstart + psize, channel.size());
        channel.position(pend);
        if (channel.position() < channel.size()) {
          int bytes = channel.read(buf);
          if (bytes >= 0) {
            buf.flip();
            for (int i = 0; i < buf.limit(); i++) {
              if (buf.get(i) == '\n') {
                pend = channel.position() - buf.limit() + i;
                break;
              }
            }
          }
        }
        extents.add(new FilePartition(path, pstart, pend));
        pstart = pend;
      }
      return extents;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public ByteBuffer mapFile() {
    long size = this.end - this.start;
    if (size > Integer.MAX_VALUE) {
      throw new RuntimeException("File partition is too large to read into a ByteBuffer");
    }
    if (size == 0) {
      throw new RuntimeException("File partition is empty");
    }
    ByteBuffer.allocate((int) size);
    try {
      FileChannel channel = FileChannel.open(path);
      MappedByteBuffer buf = channel.map(FileChannel.MapMode.READ_ONLY, start, size);
      return buf;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
