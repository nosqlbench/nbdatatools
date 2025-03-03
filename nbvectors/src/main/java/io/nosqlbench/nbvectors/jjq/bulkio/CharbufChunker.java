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


import java.nio.CharBuffer;
import java.util.Iterator;

public class CharbufChunker implements Iterable<CharBuffer> {
  private final CharBuffer buf;
  private final int chunkSize;

  public CharbufChunker(CharBuffer buf, int chunkSize) {
    this.buf = buf;
    this.chunkSize = chunkSize;
  }

  @Override
  public Iterator<CharBuffer> iterator() {
    return new CBIterator(buf, chunkSize);
  }

  public static class CBIterator implements Iterator<CharBuffer> {
    private final CharBuffer buf;
    private final int chunkSize;

    public CBIterator(CharBuffer buffer, int chunkSize) {
      this.buf = buffer;
      this.chunkSize = chunkSize;
    }

    @Override
    public synchronized boolean hasNext() {
      return (buf.position() < buf.limit());
    }

    @Override
    public synchronized CharBuffer next() {
      int at = Math.min(chunkSize,buf.remaining()-1);
      char c = c = buf.charAt(at);
      while (c != '\n' && at < buf.remaining())
        c = buf.charAt(at++);
      CharBuffer slice = buf.slice(buf.position(), at);
      buf.position(buf.position() + at);
      return slice;
    }
  }
}
