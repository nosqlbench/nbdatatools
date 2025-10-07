package io.nosqlbench.command.json.subcommands.jjq.bulkio;

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


import io.nosqlbench.nbdatatools.api.iteration.DiagToString;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

/// Chunk byte buffers on newline boundaries
public class BytebufChunker implements Iterable<CharBuffer> {
  private final ByteBuffer buf;
  private final int chunkSize;
  private final String desc;

  /// Create a bytebuf chunker
  /// @param chunkSize minimum chunk size
  /// @param buf source byte buffer
  /// @param desc a description, for debugging
  public BytebufChunker(String desc, ByteBuffer buf, int chunkSize) {
    this.desc = desc;
    this.buf = buf;
    this.chunkSize = chunkSize;
  }

  /// Create a [CharBuffer] iterator from this chunker
  /// @return a [CharBuffer] iterator from this chunker
  @Override
  public Iterator<CharBuffer> iterator() {
    return new CBIterator(desc, buf, chunkSize);
  }


  /// Convert to a char buf iterator
  public static class CBIterator implements Iterator<CharBuffer>, DiagToString {
    private final ByteBuffer buf;
    private final int chunkSize;
    private final String desc;

    /// create a char buf iterator
    /// @param desc a description, for debugging
    /// @param buffer source byte buffer
    /// @param chunkSize minimum chunk size
    public CBIterator(String desc, ByteBuffer buffer, int chunkSize) {
      this.desc = desc;
      this.buf = buffer;
      this.chunkSize = chunkSize;
    }

    /// {@inheritDoc}
    @Override
    public synchronized boolean hasNext() {
      return (buf.remaining() > 0);
    }

    /// Take the next slice which starts at the current buffer position,
    /// with length at least chunkSize, but ending at the next newline or limit().
    /// This should leave the position on the next valid position to read the
    /// next byte which wasn't returned in the last slice.
    @Override
    public synchronized CharBuffer next() {
      int was = 0;
      int at = 0;
      try {
        at = Math.min(buf.position() + chunkSize, buf.limit());
        was = at;
        while (at < buf.limit() && buf.get(at) != '\n')
          at++;

        if (at < buf.limit()) {
          at++;
        }
        int len = at - buf.position();
        ByteBuffer slice = buf.duplicate();
        slice.position(buf.position());
        slice.limit(buf.position() + len);
        buf.position(at);
        return StandardCharsets.UTF_8.decode(slice);
      } catch (Exception e) {
        System.err.println(
            "chunksize:" + this.chunkSize + "; was:" + was + "; at:" + at + "; " + "buf=" + buf);
        throw new RuntimeException(e);
      }
    }

    /// {@inheritDoc}
    @Override
    public String toDiagString() {
      return "desc:" + desc + ", position:" + buf.position();
    }

  }
}
