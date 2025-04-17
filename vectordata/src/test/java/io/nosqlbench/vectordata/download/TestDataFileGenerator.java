package io.nosqlbench.vectordata.download;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;

public class TestDataFileGenerator {

  public static void genSequenceFile(Path path, long len) throws IOException {
    SeekableByteChannel channel = Files.newByteChannel(path);
    int chunksize=1000000;
    for (long offset = channel.position(); offset < len; offset+=Long.BYTES*chunksize) {
      long[] data = new long[chunksize];
      long windowStart=offset;
      ByteBuffer buffer = ByteBuffer.allocate(chunksize*Long.BYTES);
      for (long value = windowStart; value < windowStart+chunksize; value++) {
        buffer.putLong(value);
      }
      buffer.flip();
      channel.write(buffer);
    }
    for (long offset = channel.position(); offset < len; offset+=Long.BYTES) {
      channel.write(ByteBuffer.allocate(Long.BYTES).putLong(offset));
    }


  }
}
