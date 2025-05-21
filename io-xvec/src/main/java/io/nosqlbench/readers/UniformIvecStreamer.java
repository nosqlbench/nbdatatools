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


import io.nosqlbench.nbvectors.api.fileio.BoundedVectorFileStream;
import io.nosqlbench.nbvectors.api.services.DataType;
import io.nosqlbench.nbvectors.api.services.Encoding;
import io.nosqlbench.nbvectors.api.services.FileType;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/// A reader for uniform ivec files (integer vectors).
///
/// The ivec format consists of:
/// - Per vector: 4-byte integer with the vector dimension
/// - Followed by: dimension * 4-byte integer values
///
/// ```
///         │
///         ▼
/// ┌─────────────────────┐
/// │ Calculate Offsets   │ Only when needed
/// │ Using Fixed Size    │ O(1) per vector
/// └─────────────────────┘
///```
@DataType(int[].class)
@Encoding(FileType.xvec)
public class UniformIvecStreamer implements BoundedVectorFileStream<int[]> {
  private Path filePath;
  private int dimension;
  private int recordSize;
  private int size;
  private RandomAccessFile randomAccessFile;

  /// Creates a new uniform ivec streamer.
  public UniformIvecStreamer() {
  }

  /// Creates a new UniformIvecReader for the given file path.
  /// @param filePath
  ///     The path to the ivec file
  public void open(Path filePath) {
    this.filePath = Objects.requireNonNull(filePath, "filePath cannot be null");

    // Open the file and prepare for reading
    try {
      this.randomAccessFile = new RandomAccessFile(filePath.toFile(), "r");

      // Read the first 4 bytes to get the dimension
      byte[] dimBytes = new byte[4];
      if (randomAccessFile.read(dimBytes) != 4) {
        throw new IOException("Failed to read dimension from file: " + filePath);
      }

      // Convert bytes to integer (little-endian)
      ByteBuffer dimBuffer = ByteBuffer.wrap(dimBytes).order(ByteOrder.LITTLE_ENDIAN);
      this.dimension = dimBuffer.getInt();

      if (this.dimension <= 0) {
        throw new IOException("Invalid dimension in file: " + this.dimension);
      }

      // Calculate record size: 4 bytes for dimension + (dimension * 4 bytes for integer values)
      this.recordSize = 4 + (dimension * 4);

      // Calculate the total number of vectors in the file
      long fileSize = randomAccessFile.length();
      if (fileSize % recordSize != 0) {
        throw new IOException("File size is not a multiple of record size. File may be corrupted.");
      }

      this.size = (int) (fileSize / recordSize);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  /// Reads a vector at the specified index.
  /// @param index
  ///     The index of the vector to read
  /// @return The integer vector at the specified index
  /// @throws IOException
  ///     If an I/O error occurs
  public int[] readVector(int index) throws IOException {
    if (index < 0 || index >= size) {
      throw new IndexOutOfBoundsException("Index " + index + " out of bounds for size " + size);
    }

    // Calculate offset for the specific vector
    long offset = (long) index * recordSize;
    randomAccessFile.seek(offset);

    // Skip the dimension field (we already know it)
    randomAccessFile.skipBytes(4);

    // Read the vector values
    int[] vector = new int[dimension];
    byte[] buffer = new byte[dimension * 4];
    int bytesRead = randomAccessFile.read(buffer);

    if (bytesRead != dimension * 4) {
      throw new IOException("Failed to read vector data at index " + index);
    }

    // Convert bytes to integer values
    ByteBuffer intBuffer = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < dimension; i++) {
      vector[i] = intBuffer.getInt(i * 4);
    }

    return vector;
  }

  /// Closes the file resources.
  /// Releases any system resources associated with this streamer.
  @Override
  public void close() {
    if (randomAccessFile != null) {
      try {
        randomAccessFile.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /// Returns the total number of vectors in the file.
  /// @return The number of vectors
  @Override
  public int getSize() {
    return size;
  }

  /// Returns the name of the file being read.
  /// @return The filename
  @Override
  public String getName() {
    return filePath.getFileName().toString();
  }

  /// Returns an iterator over the vectors in this file.
  /// The iterator reads vectors sequentially from the file.
  /// @return An iterator over the vectors
  @Override
  public Iterator<int[]> iterator() {
    return new Iterator<int[]>() {
      private int currentIndex = 0;

      @Override
      public boolean hasNext() {
        return currentIndex < size;
      }

      @Override
      public int[] next() {
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
