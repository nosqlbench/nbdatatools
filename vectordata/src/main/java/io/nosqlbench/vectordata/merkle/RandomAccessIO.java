package io.nosqlbench.vectordata.merkle;

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


import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/// Capture the interface of a RandomAccessFile
/// Ideally, all the methods on RandomAccessFile would belong to some faceted interface for easy
/// extension, but instead we reify the gaps in its signature here.
public interface RandomAccessIO extends DataOutput, DataInput, Closeable {

  /// Prebuffer a byte range in the file asynchronously.
  /// If you need to synchronously block until this is complete, then you can await on the returned
  /// future.
  /// @param position
  ///     The starting byte position to prebuffer
  /// @param length
  ///     The number of bytes to prebuffer
  /// @return a future indicating when the prebuffering is done.
  public CompletableFuture<Void> prebuffer(long position, long length);

  /// Get the length of the file, according to the known length of the source file
  /// @return The length of the file
  long length();

  /// Seek to a position in the file, and asynchronously prebuffering the block in anticipation
  /// that it will be accessed
  /// @param i
  ///     The position to seek to
  /// @throws IOException
  ///     if the seek operation fails
  void seek(long i) throws IOException;

  /// Read an array of bytes from the current position, but synchronously prebuffer the block if
  /// needed.
  /// @param dimBytes
  ///     The byte array (and size) of data to read
  /// @return The number of bytes read
  /// @throws IOException
  ///     if the read operation fails
  int read(byte[] dimBytes) throws IOException;
}
