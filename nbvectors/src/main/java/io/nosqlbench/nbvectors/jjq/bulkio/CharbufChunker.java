package io.nosqlbench.nbvectors.jjq.bulkio;

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
    public boolean hasNext() {
      return (buf.position() < buf.limit());
    }

    @Override
    public CharBuffer next() {
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
