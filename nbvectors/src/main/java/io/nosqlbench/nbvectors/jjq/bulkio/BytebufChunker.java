package io.nosqlbench.nbvectors.jjq.bulkio;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class BytebufChunker implements Iterable<CharBuffer> {
  private final ByteBuffer buf;
  private final int chunkSize;
  private final String desc;

  public BytebufChunker(String desc, ByteBuffer buf, int chunkSize) {
    this.desc = desc;
    this.buf = buf;
    this.chunkSize = chunkSize;
  }

  @Override
  public Iterator<CharBuffer> iterator() {
    return new CBIterator(desc, buf, chunkSize);
  }


  public static class CBIterator implements Iterator<CharBuffer>, DiagToString {
    private final ByteBuffer buf;
    private final int chunkSize;
    private final String desc;

    public CBIterator(String desc, ByteBuffer buffer, int chunkSize) {
      this.desc = desc;
      this.buf = buffer;
      this.chunkSize = chunkSize;
    }

    @Override
    public boolean hasNext() {
      return (buf.remaining()>0);
    }

    @Override
    public CharBuffer next() {
      int at = Math.min(buf.position() + chunkSize, buf.limit());
      if (at<buf.limit()) {
        for (; buf.get(at)!='\n' && at < buf.limit(); at++);
        at++; /// incl newline
      }
      ByteBuffer slice = buf.slice(buf.position(), at - buf.position());
      buf.position(at);
      return StandardCharsets.UTF_8.decode(slice);
    }

    @Override
    public String toDiagString() {
      return "desc:" + desc + ", position:" + buf.position();
    }

  }
}
