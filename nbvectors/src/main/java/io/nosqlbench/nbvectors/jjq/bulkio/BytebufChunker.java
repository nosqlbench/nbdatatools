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
    public synchronized boolean hasNext() {
      return (buf.remaining() > 0);
    }

    ///  Take the next slice which starts at the current buffer position,
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
        ByteBuffer slice = buf.slice(buf.position(), len);
        buf.position(at);
        return StandardCharsets.UTF_8.decode(slice);
      } catch (Exception e) {
        System.err.println(
            "chunksize:" + this.chunkSize + "; was:" + was + "; at:" + at + "; " + "buf=" + buf);
        throw new RuntimeException(e);
      }
    }

    @Override
    public String toDiagString() {
      return "desc:" + desc + ", position:" + buf.position();
    }

  }
}
