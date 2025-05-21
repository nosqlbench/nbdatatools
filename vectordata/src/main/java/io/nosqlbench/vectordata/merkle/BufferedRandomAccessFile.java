package io.nosqlbench.vectordata.merkle;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/// Capture the interface of a RandomAccessFile
/// Ideally, all the methods on RandomAccessFile would belong to some faceted interface for easy
/// extension, but instead we reify the gaps in its signature here.
public interface BufferedRandomAccessFile extends DataOutput, DataInput, Closeable {
  CompletableFuture<Void> prebuffer(long position, long length);
  void awaitPrebuffer(long minIncl, long maxExcl);
  long length();
  void seek(long i) throws IOException;
  int read(byte[] dimBytes) throws IOException;
}
