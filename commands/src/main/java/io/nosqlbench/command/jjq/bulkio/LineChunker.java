package io.nosqlbench.command.jjq.bulkio;

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


import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

/// chunk a file in to [CharBuffer] chunks
public class LineChunker implements Iterable<CharBuffer> {
  private final FileChannel channel;
  private final int maxBufSize;

  /// create a line chunker
  /// @param path the path to the file to chunk
  /// @param startAt the starting offset, inclusive
  /// @param linesPerChunk the number of lines per chunk
  public LineChunker(Path path, long startAt, int linesPerChunk) {
    try {
      BufferedReader br = Files.newBufferedReader(path);
      long sumlen = 0;
      int sumcount = 0;
      String line;
      while ((line = br.readLine()) != null && sumcount < 32) {
        sumlen += line.length();
        sumcount++;
      }
      this.maxBufSize = ((int) (sumlen / sumcount)) * linesPerChunk;
      this.channel = FileChannel.open(path);
      channel.position(startAt);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /// {@inheritDoc}
  @Override
  public Iterator<CharBuffer> iterator() {
    return new LineChunkIterator();
  }

  private class LineChunkIterator implements Iterator<CharBuffer> {

    /// {@inheritDoc}
    @Override
    public boolean hasNext() {
      try {
        return (channel.position() < channel.size());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    /// {@inheritDoc}
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
}
