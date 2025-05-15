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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/// A reader for uniform bvec files (byte/binary vectors stored as integers).
///
/// The bvec format consists of:
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
/// ```
@DataType(int[].class)
@Encoding(Encoding.Type.xvec)
public class UniformBvecStreamer implements SizedVectorStreamReader<int[]>, AutoCloseable {
    private final Path filePath;
    private final String name;
    private final int dimension;
    private final int recordSize;
    private final int size;
    private final RandomAccessFile randomAccessFile;

    /// Creates a new UniformBvecReader for the given file path.
    ///
    /// @param filePath The path to the bvec file
    /// @throws IOException If the file cannot be opened or read
    public UniformBvecStreamer(Path filePath) throws IOException {
        this.filePath = Objects.requireNonNull(filePath, "filePath cannot be null");
        this.name = filePath.getFileName().toString();
        
        // Open the file and prepare for reading
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
    }

    /// Reads a vector at the specified index.
    ///
    /// @param index The index of the vector to read
    /// @return The integer vector at the specified index
    /// @throws IOException If an I/O error occurs
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
        return name;
    }

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
