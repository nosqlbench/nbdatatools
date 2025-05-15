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


import io.nosqlbench.nbvectors.api.services.DataType;
import io.nosqlbench.nbvectors.api.services.Encoding;
import io.nosqlbench.nbvectors.api.fileio.SizedVectorStreamReader;
import io.nosqlbench.nbvectors.api.services.FileType;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/// A reader for uniform fvec files (float vectors).
///
/// The fvec format consists of:
/// - Per vector: 4-byte integer with the vector dimension
/// - Followed by: dimension * 4-byte float values
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
public class UniformFvecStreamer implements SizedVectorStreamReader<float[]>, AutoCloseable {
  private Path filePath;
  private int dimension;
  private int recordSize;
  private int size;
  private RandomAccessFile randomAccessFile;

  /// Creates a new UniformFvecReader for the given file path.
  /// @param filePath
  ///     The path to the fvec file
  public void open(Path filePath) {
    this.filePath = Objects.requireNonNull(filePath, "filePath cannot be null");

    // Open the file and prepare for reading
    try {
      this.randomAccessFile = new RandomAccessFile(filePath.toFile(), "r");
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }

    // Read the first 4 bytes to get the dimension
    byte[] dimBytes = new byte[4];
    try {
      if (randomAccessFile.read(dimBytes) != 4) {
        throw new RuntimeException("Failed to read dimension from file: " + filePath);
      }

      // Convert bytes to integer (little-endian)
      ByteBuffer dimBuffer = ByteBuffer.wrap(dimBytes).order(ByteOrder.LITTLE_ENDIAN);
      this.dimension = dimBuffer.getInt();

      if (this.dimension <= 0) {
        throw new RuntimeException("Invalid dimension in file: " + this.dimension);
      }

      // Calculate record size: 4 bytes for dimension + (dimension * 4 bytes for float values)
      this.recordSize = 4 + (dimension * 4);

      // Calculate the total number of vectors in the file
      long fileSize = randomAccessFile.length();
      if (fileSize % recordSize != 0) {
        throw new RuntimeException(
            "File size is not a multiple of record size. File may be " + "corrupted.");
      }

      this.size = (int) (fileSize / recordSize);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /// Reads a vector at the specified index.
  /// @param index
  ///     The index of the vector to read
  /// @return The float vector at the specified index
  /// @throws IOException
  ///     If an I/O error occurs
  public float[] readVector(int index) throws IOException {
    if (index < 0 || index >= size) {
      throw new IndexOutOfBoundsException("Index " + index + " out of bounds for size " + size);
    }

    // Calculate offset for the specific vector
    long offset = (long) index * recordSize;
    randomAccessFile.seek(offset);

    // Skip the dimension field (we already know it)
    randomAccessFile.skipBytes(4);

    // Read the vector values
    float[] vector = new float[dimension];
    byte[] buffer = new byte[dimension * 4];
    int bytesRead = randomAccessFile.read(buffer);

    if (bytesRead != dimension * 4) {
      throw new IOException("Failed to read vector data at index " + index);
    }

    // Convert bytes to float values
    ByteBuffer floatBuffer = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < dimension; i++) {
      vector[i] = floatBuffer.getFloat(i * 4);
    }

    return vector;
  }

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
