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

import io.nosqlbench.nbdatatools.api.fileio.ImmutableSizedReader;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileArray;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/// A SizedReader implementation for uniform hvec files (half-precision float vectors).
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
/// ```
@DataType(float[].class)
@Encoding(FileType.xvec)
@FileExtension({".hvec", ".hvecs"})
public class UniformHvecReader extends ImmutableSizedReader<float[]> implements VectorFileArray<float[]> {
    private Path filePath;
    private int dimension;
    private int recordSize;
    private int size;
    private AsynchronousFileChannel fileChannel;

    /// Constructs a new UniformHvecReader.
    public UniformHvecReader() {
    }

    /// Opens the hvec file at the given path for reading.
    ///
    /// @param filePath The path to the hvec file
    public void open(Path filePath) {
        try {
            this.filePath = Objects.requireNonNull(filePath, "filePath cannot be null");

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

    /// Retrieves the vector at the specified index, converting half-precision values to float.
    ///
    /// @param index The index of the vector to read
    /// @return The float vector at the specified index
    /// @throws IndexOutOfBoundsException If the index is out of bounds
    @Override
    public float[] get(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Index " + index + " out of bounds for size " + size);
        }

        try {
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
        } catch (IOException e) {
            throw new RuntimeException("Error reading vector at index " + index, e);
        }
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
        return filePath.getFileName().toString();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean contains(Object o) {
        if (!(o instanceof float[])) {
            return false;
        }
        float[] array = (float[]) o;

        if (array.length != dimension) {
            return false;
        }

        for (int i = 0; i < size; i++) {
            float[] vector = get(i);
            if (Arrays.equals(vector, array)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public int indexOf(Object o) {
        if (!(o instanceof float[]) || ((float[]) o).length != dimension) {
            return -1;
        }
        float[] array = (float[]) o;

        for (int i = 0; i < size; i++) {
            float[] vector = get(i);
            if (Arrays.equals(vector, array)) {
                return i;
            }
        }

        return -1;
    }

    @Override
    public int lastIndexOf(Object o) {
        if (!(o instanceof float[]) || ((float[]) o).length != dimension) {
            return -1;
        }
        float[] array = (float[]) o;

        for (int i = size - 1; i >= 0; i--) {
            float[] vector = get(i);
            if (Arrays.equals(vector, array)) {
                return i;
            }
        }

        return -1;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object o : c) {
            if (!contains(o)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Object[] toArray() {
        return getRange(0, size);
    }

    /// Reads a range of vectors in bulk for optimal I/O performance.
    /// Uses microbatching to read multiple vectors per I/O operation.
    ///
    /// @param startIndex starting index (inclusive)
    /// @param endIndex ending index (exclusive)
    /// @return array of vectors in the range
    public float[][] getRange(int startIndex, int endIndex) {
        if (startIndex < 0 || endIndex > size || startIndex > endIndex) {
            throw new IndexOutOfBoundsException(
                "Range [" + startIndex + ", " + endIndex + ") out of bounds for size " + size);
        }

        int count = endIndex - startIndex;
        if (count == 0) {
            return new float[0][];
        }

        float[][] result = new float[count][];

        int batchSize = Math.max(64, Math.min(4096, 65536 / dimension));

        try {
            int processed = 0;
            while (processed < count) {
                int currentBatch = Math.min(batchSize, count - processed);
                int batchStart = startIndex + processed;

                readVectorBatch(batchStart, currentBatch, result, processed);

                processed += currentBatch;
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading vector range [" + startIndex + ", " + endIndex + ")", e);
        }

        return result;
    }

    /// Reads a batch of vectors with a single I/O operation.
    private void readVectorBatch(int startIndex, int count, float[][] dest, int destOffset) throws IOException {
        int bytesPerVector = dimension * 2;
        int totalBytes = count * recordSize;

        ByteBuffer buffer = ByteBuffer.allocate(totalBytes).order(ByteOrder.LITTLE_ENDIAN);

        long readPosition = (long) startIndex * recordSize;
        CompletableFuture<Integer> readFuture = new CompletableFuture<>();
        fileChannel.read(buffer, readPosition, null, new CompletionHandler<Integer, Void>() {
            @Override
            public void completed(Integer bytesRead, Void attachment) {
                readFuture.complete(bytesRead);
            }
            @Override
            public void failed(Throwable exc, Void attachment) {
                readFuture.completeExceptionally(exc);
            }
        });

        try {
            int bytesRead = readFuture.get();
            if (bytesRead != totalBytes) {
                throw new IOException("Failed to read batch: expected " + totalBytes + " bytes, got " + bytesRead);
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException("Failed to read vector batch", e);
        }

        buffer.flip();

        for (int i = 0; i < count; i++) {
            // Skip dimension header (4 bytes)
            buffer.getInt();

            // Read half-precision values and convert to float
            dest[destOffset + i] = new float[dimension];
            for (int j = 0; j < dimension; j++) {
                dest[destOffset + i][j] = Half.fromBits(buffer.getShort()).toFloat();
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E> E[] toArray(E[] a) {
        if (a.length < size) {
            return (E[]) Arrays.copyOf(toArray(), size, a.getClass());
        }

        System.arraycopy(toArray(), 0, a, 0, size);

        if (a.length > size) {
            a[size] = null;
        }

        return a;
    }

    @Override
    public List<float[]> subList(int fromIndex, int toIndex) {
        if (fromIndex < 0 || toIndex > size || fromIndex > toIndex) {
            throw new IndexOutOfBoundsException("fromIndex: " + fromIndex + ", toIndex: " + toIndex + ", size: " + size);
        }

        return new SublistHvecReader(this, fromIndex, toIndex);
    }

    /// Returns the dimension of vectors in this reader.
    ///
    /// @return The dimension of each vector
    public int getDimension() {
        return dimension;
    }

    /// A view of a sublist of an HvecReader.
    private static class SublistHvecReader extends ImmutableSizedReader<float[]> {
        private final UniformHvecReader parent;
        private final int offset;
        private final int size;

        public SublistHvecReader(UniformHvecReader parent, int fromIndex, int toIndex) {
            this.parent = parent;
            this.offset = fromIndex;
            this.size = toIndex - fromIndex;
        }

        @Override
        public float[] get(int index) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
            }
            return parent.get(offset + index);
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public int getSize() {
            return size;
        }

        @Override
        public String getName() {
            return parent.getName() + "[" + offset + ":" + (offset + size) + "]";
        }

        @Override
        public boolean isEmpty() {
            return size == 0;
        }

        @Override
        public boolean contains(Object o) {
            if (!(o instanceof float[])) {
                return false;
            }
            float[] array = (float[]) o;

            if (array.length != parent.getDimension()) {
                return false;
            }

            for (int i = 0; i < size; i++) {
                float[] vector = get(i);
                if (Arrays.equals(vector, array)) {
                    return true;
                }
            }

            return false;
        }

        @Override
        public int indexOf(Object o) {
            if (!(o instanceof float[]) || ((float[]) o).length != parent.getDimension()) {
                return -1;
            }
            float[] array = (float[]) o;

            for (int i = 0; i < size; i++) {
                float[] vector = get(i);
                if (Arrays.equals(vector, array)) {
                    return i;
                }
            }

            return -1;
        }

        @Override
        public int lastIndexOf(Object o) {
            if (!(o instanceof float[]) || ((float[]) o).length != parent.getDimension()) {
                return -1;
            }
            float[] array = (float[]) o;

            for (int i = size - 1; i >= 0; i--) {
                float[] vector = get(i);
                if (Arrays.equals(vector, array)) {
                    return i;
                }
            }

            return -1;
        }

        @Override
        public List<float[]> subList(int fromIndex, int toIndex) {
            if (fromIndex < 0 || toIndex > size || fromIndex > toIndex) {
                throw new IndexOutOfBoundsException("fromIndex: " + fromIndex + ", toIndex: " + toIndex + ", size: " + size);
            }

            return parent.subList(offset + fromIndex, offset + toIndex);
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            for (Object o : c) {
                if (!contains(o)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public Object[] toArray() {
            return parent.getRange(offset, offset + size);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <E> E[] toArray(E[] a) {
            if (a.length < size) {
                return (E[]) Arrays.copyOf(toArray(), size, a.getClass());
            }

            System.arraycopy(toArray(), 0, a, 0, size);

            if (a.length > size) {
                a[size] = null;
            }

            return a;
        }
    }
}
