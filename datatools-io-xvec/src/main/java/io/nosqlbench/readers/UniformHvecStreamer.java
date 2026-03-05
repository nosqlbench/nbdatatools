package io.nosqlbench.readers;

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


import io.nosqlbench.nbdatatools.api.fileio.BoundedVectorFileStream;
import io.nosqlbench.nbdatatools.api.services.DataType;
import io.nosqlbench.nbdatatools.api.services.Encoding;
import io.nosqlbench.nbdatatools.api.services.FileExtension;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbdatatools.api.types.Half;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/// A reader for uniform hvec files (half-precision float vectors).
///
/// The hvec format consists of:
/// - Per vector: 4-byte little-endian integer with the vector dimension
/// - Followed by: dimension * 2-byte IEEE 754 binary16 (half-precision) values
///
/// Each half-precision value is widened to {@code float} on read via {@link Half}.
///
/// ```
///         │
///         ▼
/// ┌─────────────────────┐
/// │ Calculate Offsets   │ Only when needed
/// │ Using Fixed Size    │ O(1) per vector
/// └─────────────────────┘
///```
@DataType(float[].class)
@Encoding(FileType.xvec)
@FileExtension({".hvec", ".hvecs"})
public class UniformHvecStreamer implements BoundedVectorFileStream<float[]> {
  private Path filePath;
  private int dimension;
  private int recordSize;
  private int size;
  private AsynchronousFileChannel fileChannel;

  /// Creates a new UniformHvecStreamer.
  public UniformHvecStreamer() {
  }

  /// Opens the hvec file at the given path for streaming.
  /// @param filePath
  ///     The path to the hvec file
  public void open(Path filePath) {
    this.filePath = Objects.requireNonNull(filePath, "filePath cannot be null");

    try {
      this.fileChannel = AsynchronousFileChannel.open(filePath, StandardOpenOption.READ);

      // Read the first 4 bytes to get the dimension
      ByteBuffer dimBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
      CompletableFuture<Integer> readFuture = new CompletableFuture<>();

      fileChannel.read(dimBuffer, 0, null, new CompletionHandler<Integer, Void>() {
        @Override
        public void completed(Integer result, Void attachment) {
          readFuture.complete(result);
        }

        @Override
        public void failed(Throwable exc, Void attachment) {
          readFuture.completeExceptionally(exc);
        }
      });

      try {
        int bytesRead = readFuture.get();
        if (bytesRead != 4) {
          throw new IOException("Failed to read dimension from file: " + filePath);
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new IOException("Failed to read dimension from file: " + filePath, e);
      }

      dimBuffer.flip();
      this.dimension = dimBuffer.getInt();

      if (this.dimension <= 0) {
        throw new IOException("Invalid dimension in file: " + this.dimension);
      }

      // 4 bytes for dimension + (dimension * 2 bytes for half-precision values)
      this.recordSize = 4 + (dimension * 2);

      try {
        long fileSize = fileChannel.size();
        this.size = ReaderUtils.computeVectorCount(this.filePath, fileSize, this.recordSize, 2);
      } catch (IOException e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /// Reads a vector at the specified index, converting half-precision values to float.
  /// @param index
  ///     The index of the vector to read
  /// @return The float vector at the specified index
  /// @throws IOException
  ///     If an I/O error occurs
  public float[] readVector(int index) throws IOException {
    if (index < 0 || index >= size) {
      throw new IndexOutOfBoundsException("Index " + index + " out of bounds for size " + size);
    }

    long offset = (long) index * recordSize;
    long vectorDataOffset = offset + 4; // Skip the dimension field
    ByteBuffer buffer = ByteBuffer.allocate(dimension * 2).order(ByteOrder.LITTLE_ENDIAN);

    CompletableFuture<Integer> readFuture = new CompletableFuture<>();

    fileChannel.read(buffer, vectorDataOffset, null, new CompletionHandler<Integer, Void>() {
        @Override
        public void completed(Integer bytesRead, Void attachment) {
            readFuture.complete(bytesRead);
        }

        @Override
        public void failed(Throwable exc, Void attachment) {
            readFuture.completeExceptionally(exc);
        }
    });

    int bytesRead;
    try {
        bytesRead = readFuture.get();
    } catch (InterruptedException | ExecutionException e) {
        throw new IOException("Failed to read vector data at index " + index, e);
    }

    if (bytesRead != dimension * 2) {
        throw new IOException("Failed to read vector data at index " + index +
                              ": expected " + (dimension * 2) + " bytes, got " + bytesRead);
    }

    buffer.flip();

    float[] vector = new float[dimension];
    for (int i = 0; i < dimension; i++) {
        vector[i] = Half.fromBits(buffer.getShort()).toFloat();
    }

    return vector;
  }

  @Override
  public void close() {
    if (fileChannel != null) {
      try {
        fileChannel.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public int getSize() {
    return size;
  }

  @Override
  public String getName() {
    return this.filePath.getFileName().toString();
  }

  @Override
  public Iterator<float[]> iterator() {
    return new Iterator<float[]>() {
      private int currentIndex = 0;

      @Override
      public boolean hasNext() {
        return currentIndex < size;
      }

      @Override
      public float[] next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        try {
          return readVector(currentIndex++);
        } catch (IOException e) {
          throw new RuntimeException("Error reading vector at index " + (currentIndex - 1), e);
        }
      }
    };
  }
}
