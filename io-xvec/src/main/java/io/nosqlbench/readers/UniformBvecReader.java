/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.nosqlbench.readers;

import io.nosqlbench.nbdatatools.api.fileio.VectorFileArray;
import io.nosqlbench.nbdatatools.api.fileio.ImmutableSizedReader;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/// A SizedReader implementation for uniform bvec files (byte/binary vectors stored as integers).
///
/// The bvec format consists of:
/// - Per vector: 4-byte integer with the vector dimension
/// - Followed by: dimension * 4-byte integer values
///
/// This reader provides immutable List-like access to vectors in the file,
/// using random access to read vectors on demand.
///
/// ```
///         │
///         ▼
/// ┌─────────────────────┐
/// │ Calculate Offsets   │ Only when needed
/// │ Using Fixed Size    │ O(1) per vector
/// └─────────────────────┘
/// ```
//@DataType(int[].class) TODO: This needs something to distinguish it from int[] types, since the
// consumed type in memory is different, even though the storage format is the same.
@Encoding(FileType.xvec)
@DataType(int[].class)
@FileExtension({".bvec", ".bvecs"})
public class UniformBvecReader extends ImmutableSizedReader<int[]> implements VectorFileArray<int[]> {
    private Path filePath;
    private int dimension;
    private int recordSize;
    private int size;
    private AsynchronousFileChannel fileChannel;

    /// Constructs a new UniformBvecReader.
    public UniformBvecReader() {
    }

    /// Creates a new BvecReader for the given file path.
    ///
    /// @param filePath The path to the bvec file
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

            // Calculate record size: 4 bytes for dimension + (dimension * 4 bytes for integer values)
            this.recordSize = 4 + (dimension * 4);

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

    /// Retrieves the vector at the specified index.
    ///
    /// @param index The index of the vector to read
    /// @return The integer vector at the specified index
    /// @throws IndexOutOfBoundsException If the index is out of bounds
    @Override
    public int[] get(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Index " + index + " out of bounds for size " + size);
        }

        try {
            // Calculate offset for the specific vector
            long offset = (long) index * recordSize;

            // Skip the dimension field (we already know it) and read the vector values
            long vectorDataOffset = offset + 4; // Skip the dimension field
            ByteBuffer buffer = ByteBuffer.allocate(dimension * 4).order(ByteOrder.LITTLE_ENDIAN);

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

            if (bytesRead != dimension * 4) {
                throw new IOException("Failed to read vector data at index " + index + 
                                     ": expected " + (dimension * 4) + " bytes, got " + bytesRead);
            }

            // Prepare the buffer for reading
            buffer.flip();

            // Convert bytes to integer values
            int[] vector = new int[dimension];
            for (int i = 0; i < dimension; i++) {
                vector[i] = buffer.getInt();
            }

            return vector;
        } catch (IOException e) {
            throw new RuntimeException("Error reading vector at index " + index, e);
        }
    }

    /// Closes the file channel associated with this reader.
    /// This should be called when done reading vectors to ensure all resources are released.
    /// @throws RuntimeException if there is an error closing the file channel
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

    /// Returns the size of this vector file array.
    /// @return The number of vectors in the file
    @Override
    public int getSize() {
        return size;
    }

    /// Returns the name of this vector file array.
    /// @return The filename of the vector file
    @Override
    public String getName() {
        return filePath.getFileName().toString();
    }

    /// Returns the size of this vector file array.
    /// This is equivalent to getSize() but implements the List interface.
    /// @return The number of vectors in the file
    @Override
    public int size() {
        return size;
    }

    /// Checks if this vector file array is empty.
    /// @return true if there are no vectors in the file, false otherwise
    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    /// Checks if this vector file array contains the specified vector.
    /// This performs a linear search through all vectors, which can be slow for large files.
    /// @param o The object to check for containment
    /// @return true if the vector is found in the file, false otherwise
    @Override
    public boolean contains(Object o) {
        if (!(o instanceof int[])) {
            return false;
        }
        int[] array = (int[]) o;

        if (array.length != dimension) {
            return false;
        }

        // Linear search through all vectors
        for (int i = 0; i < size; i++) {
            int[] vector = get(i);
            if (Arrays.equals(vector, array)) {
                return true;
            }
        }

        return false;
    }

    /// Returns the index of the first occurrence of the specified vector in this array.
    /// This performs a linear search through all vectors, which can be slow for large files.
    /// @param o The vector to search for
    /// @return The index of the first occurrence of the vector, or -1 if not found
    @Override
    public int indexOf(Object o) {
        if (!(o instanceof int[]) || ((int[]) o).length != dimension) {
            return -1;
        }
        int[] array = (int[]) o;

        // Linear search through all vectors
        for (int i = 0; i < size; i++) {
            int[] vector = get(i);
            if (Arrays.equals(vector, array)) {
                return i;
            }
        }

        return -1;
    }

    /// Returns the index of the last occurrence of the specified vector in this array.
    /// This performs a linear search through all vectors in reverse order, which can be slow for large files.
    /// @param o The vector to search for
    /// @return The index of the last occurrence of the vector, or -1 if not found
    @Override
    public int lastIndexOf(Object o) {
        if (!(o instanceof int[]) || ((int[]) o).length != dimension) {
            return -1;
        }
        int[] array = (int[]) o;

        // Linear search through all vectors in reverse
        for (int i = size - 1; i >= 0; i--) {
            int[] vector = get(i);
            if (Arrays.equals(vector, array)) {
                return i;
            }
        }

        return -1;
    }

    /// Checks if this vector file array contains all the vectors in the specified collection.
    /// This performs a linear search for each vector in the collection, which can be slow for large files.
    /// @param c The collection of vectors to check for containment
    /// @return true if all vectors in the collection are found in the file, false otherwise
    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object o : c) {
            if (!contains(o)) {
                return false;
            }
        }
        return true;
    }

    /// Returns an array containing all the vectors in this vector file array.
    /// This creates a new array and loads all vectors into memory, which can be memory-intensive for large files.
    /// @return An array containing all the vectors in this vector file array
    @Override
    public Object[] toArray() {
        Object[] result = new Object[size];
        for (int i = 0; i < size; i++) {
            result[i] = get(i);
        }
        return result;
    }

    /// Returns an array containing all the vectors in this vector file array.
    /// The runtime type of the returned array is that of the specified array.
    /// This creates a new array and loads all vectors into memory, which can be memory-intensive for large files.
    /// @param a The array into which the vectors are to be stored, if it is big enough
    /// @return An array containing all the vectors in this vector file array
    @Override
    @SuppressWarnings("unchecked")
    public <E> E[] toArray(E[] a) {
        if (a.length < size) {
            // Make a new array of a's runtime type, but my contents:
            return (E[]) Arrays.copyOf(toArray(), size, a.getClass());
        }

        System.arraycopy(toArray(), 0, a, 0, size);

        if (a.length > size) {
            a[size] = null;
        }

        return a;
    }

    /// Returns a view of the portion of this vector file array between the specified fromIndex, inclusive, and toIndex, exclusive.
    /// This creates a view of the sublist without copying the data, which is memory-efficient.
    /// @param fromIndex The low endpoint (inclusive) of the subList
    /// @param toIndex The high endpoint (exclusive) of the subList
    /// @return A view of the specified range within this vector file array
    /// @throws IndexOutOfBoundsException If fromIndex or toIndex are out of range
    @Override
    public List<int[]> subList(int fromIndex, int toIndex) {
        if (fromIndex < 0 || toIndex > size || fromIndex > toIndex) {
            throw new IndexOutOfBoundsException("fromIndex: " + fromIndex + ", toIndex: " + toIndex + ", size: " + size);
        }

        // Create a view of a sublist - this doesn't copy the data
        return new SublistBvecReader(this, fromIndex, toIndex);
    }

    /// Returns the dimension of vectors in this reader.
    ///
    /// @return The dimension of each vector
    public int getDimension() {
        return dimension;
    }

    /// A view of a sublist of a BvecReader.
    /// This class provides a view of a portion of the parent BvecReader without copying the data.
    /// It implements the same interfaces as the parent class but only exposes a subset of the vectors.
    private static class SublistBvecReader extends ImmutableSizedReader<int[]> {
        private final UniformBvecReader parent;
        private final int offset;
        private final int size;

        /// Creates a new SublistBvecReader that provides a view of a portion of the parent BvecReader.
        /// @param parent The parent BvecReader
        /// @param fromIndex The starting index (inclusive) in the parent reader
        /// @param toIndex The ending index (exclusive) in the parent reader
        public SublistBvecReader(UniformBvecReader parent, int fromIndex, int toIndex) {
            this.parent = parent;
            this.offset = fromIndex;
            this.size = toIndex - fromIndex;
        }

        /// Retrieves the vector at the specified index in this sublist.
        /// @param index The index of the vector to retrieve within this sublist
        /// @return The vector at the specified index
        /// @throws IndexOutOfBoundsException If the index is out of bounds
        @Override
        public int[] get(int index) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
            }
            return parent.get(offset + index);
        }

        /// Returns the size of this sublist.
        /// @return The number of vectors in this sublist
        @Override
        public int size() {
            return size;
        }

        /// Returns the size of this sublist.
        /// This is equivalent to size() but implements the VectorFileArray interface.
        /// @return The number of vectors in this sublist
        @Override
        public int getSize() {
            return size;
        }

        /// Returns the name of this sublist, which includes the parent name and the range.
        /// @return The name of this sublist in the format "parentName[fromIndex:toIndex]"
        @Override
        public String getName() {
            return parent.getName() + "[" + offset + ":" + (offset + size) + "]";
        }

        /// Checks if this sublist is empty.
        /// @return true if there are no vectors in this sublist, false otherwise
        @Override
        public boolean isEmpty() {
            return size == 0;
        }

        /// Checks if this sublist contains the specified vector.
        /// This performs a linear search through all vectors in this sublist, which can be slow for large sublists.
        /// @param o The object to check for containment
        /// @return true if the vector is found in this sublist, false otherwise
        @Override
        public boolean contains(Object o) {
            if (!(o instanceof int[])) {
                return false;
            }
            int[] array = (int[]) o;

            if (array.length != parent.getDimension()) {
                return false;
            }

            // Linear search through this sublist's vectors
            for (int i = 0; i < size; i++) {
                int[] vector = get(i);
                if (Arrays.equals(vector, array)) {
                    return true;
                }
            }

            return false;
        }

        /// Returns the index of the first occurrence of the specified vector in this sublist.
        /// This performs a linear search through all vectors in this sublist, which can be slow for large sublists.
        /// @param o The vector to search for
        /// @return The index of the first occurrence of the vector within this sublist, or -1 if not found
        @Override
        public int indexOf(Object o) {
            if (!(o instanceof int[]) || ((int[]) o).length != parent.getDimension()) {
                return -1;
            }
            int[] array = (int[]) o;

            // Linear search through this sublist's vectors
            for (int i = 0; i < size; i++) {
                int[] vector = get(i);
                if (Arrays.equals(vector, array)) {
                    return i;
                }
            }

            return -1;
        }

        /// Returns the index of the last occurrence of the specified vector in this sublist.
        /// This performs a linear search through all vectors in this sublist in reverse order, which can be slow for large sublists.
        /// @param o The vector to search for
        /// @return The index of the last occurrence of the vector within this sublist, or -1 if not found
        @Override
        public int lastIndexOf(Object o) {
            if (!(o instanceof int[]) || ((int[]) o).length != parent.getDimension()) {
                return -1;
            }
            int[] array = (int[]) o;

            // Linear search through this sublist's vectors in reverse
            for (int i = size - 1; i >= 0; i--) {
                int[] vector = get(i);
                if (Arrays.equals(vector, array)) {
                    return i;
                }
            }

            return -1;
        }

        /// Returns a view of the portion of this sublist between the specified fromIndex, inclusive, and toIndex, exclusive.
        /// This creates a view of the sublist without copying the data, which is memory-efficient.
        /// @param fromIndex The low endpoint (inclusive) of the subList within this sublist
        /// @param toIndex The high endpoint (exclusive) of the subList within this sublist
        /// @return A view of the specified range within this sublist
        /// @throws IndexOutOfBoundsException If fromIndex or toIndex are out of range
        @Override
        public List<int[]> subList(int fromIndex, int toIndex) {
            if (fromIndex < 0 || toIndex > size || fromIndex > toIndex) {
                throw new IndexOutOfBoundsException("fromIndex: " + fromIndex + ", toIndex: " + toIndex + ", size: " + size);
            }

            return parent.subList(offset + fromIndex, offset + toIndex);
        }

        /// Checks if this sublist contains all the vectors in the specified collection.
        /// This performs a linear search for each vector in the collection, which can be slow for large sublists.
        /// @param c The collection of vectors to check for containment
        /// @return true if all vectors in the collection are found in this sublist, false otherwise
        @Override
        public boolean containsAll(Collection<?> c) {
            for (Object o : c) {
                if (!contains(o)) {
                    return false;
                }
            }
            return true;
        }

        /// Returns an array containing all the vectors in this sublist.
        /// This creates a new array and loads all vectors into memory, which can be memory-intensive for large sublists.
        /// @return An array containing all the vectors in this sublist
        @Override
        public Object[] toArray() {
            Object[] result = new Object[size];
            for (int i = 0; i < size; i++) {
                result[i] = get(i);
            }
            return result;
        }

        /// Returns an array containing all the vectors in this sublist.
        /// The runtime type of the returned array is that of the specified array.
        /// This creates a new array and loads all vectors into memory, which can be memory-intensive for large sublists.
        /// @param a The array into which the vectors are to be stored, if it is big enough
        /// @return An array containing all the vectors in this sublist
        @Override
        @SuppressWarnings("unchecked")
        public <E> E[] toArray(E[] a) {
            if (a.length < size) {
                // Make a new array of a's runtime type, but my contents:
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
