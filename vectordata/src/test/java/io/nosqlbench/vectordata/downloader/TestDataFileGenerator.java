package io.nosqlbench.vectordata.downloader;

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
import java.nio.ByteBuffer;
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
