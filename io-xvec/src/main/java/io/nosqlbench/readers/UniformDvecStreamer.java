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

/// A reader for uniform dvec files (double vectors).
///
/// The dvec format consists of:
/// - Per vector: 4-byte integer with the vector dimension
/// - Followed by: dimension * 8-byte double values
///
/// ```
///         │
///         ▼
/// ┌─────────────────────┐
/// │ Calculate Offsets   │ Only when needed
/// │ Using Fixed Size    │ O(1) per vector
/// └─────────────────────┘
/// ```
@DataType(double[].class)
@Encoding(FileType.xvec)
@FileExtension({".dvec",".dvecs"})
public class UniformDvecStreamer implements BoundedVectorFileStream<double[]> {
    private Path filePath;
    private int dimension;
    private int recordSize;
    private int size;
    private AsynchronousFileChannel fileChannel;

    /// Constructs a new UniformDvecStreamer.
    public UniformDvecStreamer() {
    }

    /// Creates a new UniformDvecStreamer for the given file path.
    ///
    /// @param filePath The path to the dvec file
    public void open(Path filePath) {
        try {
            this.filePath = Objects.requireNonNull(filePath, "filePath cannot be null");

            // Open the file and prepare for reading
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

            // Convert bytes to integer (little-endian)
            dimBuffer.flip();
            this.dimension = dimBuffer.getInt();

            if (this.dimension <= 0) {
                throw new IOException("Invalid dimension in file: " + this.dimension);
            }

            // Calculate record size: 4 bytes for dimension + (dimension * 8 bytes for double values)
            this.recordSize = 4 + (dimension * 8);

            // Calculate the total number of vectors in the file
            try {
                long fileSize = fileChannel.size();
                if (fileSize % recordSize != 0) {
                    throw new IOException("File size is not a multiple of record size. File may be corrupted.");
                }

                this.size = (int) (fileSize / recordSize);
            } catch (IOException e) {
                throw new RuntimeException("Failed to get file size: " + e.getMessage(), e);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /// Reads a vector at the specified index.
    ///
    /// @param index The index of the vector to read
    /// @return The double vector at the specified index
    /// @throws IOException If an I/O error occurs
    public double[] readVector(int index) throws IOException {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Index " + index + " out of bounds for size " + size);
        }

        // Calculate offset for the specific vector
        long offset = (long) index * recordSize;

        // Skip the dimension field (we already know it) and read the vector values
        long vectorDataOffset = offset + 4; // Skip the dimension field
        ByteBuffer buffer = ByteBuffer.allocate(dimension * 8).order(ByteOrder.LITTLE_ENDIAN);

        // Create a CompletableFuture to track the read operation
        CompletableFuture<Integer> readFuture = new CompletableFuture<>();

        // Start the asynchronous read operation
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

        // Wait for the read operation to complete
        int bytesRead;
        try {
            bytesRead = readFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException("Failed to read vector data at index " + index, e);
        }

        if (bytesRead != dimension * 8) {
            throw new IOException("Failed to read vector data at index " + index + 
                                  ": expected " + (dimension * 8) + " bytes, got " + bytesRead);
        }

        // Prepare the buffer for reading
        buffer.flip();

        // Convert bytes to double values
        double[] vector = new double[dimension];
        for (int i = 0; i < dimension; i++) {
            vector[i] = buffer.getDouble();
        }

        return vector;
    }

    /// Closes the file resources.
    /// Releases any system resources associated with this streamer.
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
        return this.filePath.getFileName().toString();
    }

    /// Returns an iterator over the vectors in this file.
    /// The iterator reads vectors sequentially from the file.
    /// @return An iterator over the vectors
    @Override
    public Iterator<double[]> iterator() {
        return new Iterator<double[]>() {
            private int currentIndex = 0;

            @Override
            public boolean hasNext() {
                return currentIndex < size;
            }

            @Override
            public double[] next() {
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
