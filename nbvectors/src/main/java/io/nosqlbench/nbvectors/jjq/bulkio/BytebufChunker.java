package io.nosqlbench.nbvectors.jjq.bulkio;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class BytebufChunker implements Iterable<CharBuffer> {
  private final ByteBuffer buf;
  private final int chunkSize;

  public BytebufChunker(ByteBuffer buf, int chunkSize) {
    this.buf = buf;
    this.chunkSize = chunkSize;
  }

  @Override
  public Iterator<CharBuffer> iterator() {
    return new CBIterator(buf, chunkSize);
  }

  public static class CBIterator implements Iterator<CharBuffer> {
    private final ByteBuffer buf;
    private final int chunkSize;

    public CBIterator(ByteBuffer buffer, int chunkSize) {
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
      /// TODO: ensure this is not an issue for character encoding with UTF-8
      char c = (char) buf.get(at);
      while (c != '\n' && at < buf.remaining())
        c = (char) buf.get(at++);
      ByteBuffer slice = buf.slice(buf.position(), at);
      buf.position(buf.position() + at);
      return StandardCharsets.UTF_8.decode(slice);
    }
  }
}
