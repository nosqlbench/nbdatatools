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

import io.nosqlbench.nbvectors.api.fileio.ImmutableSizedReader;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/// A SizedReader implementation for uniform dvec files (double vectors).
///
/// The dvec format consists of:
/// - Per vector: 4-byte integer with the vector dimension
/// - Followed by: dimension * 8-byte double values
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
public class UniformDvecReader extends ImmutableSizedReader<double[]> implements AutoCloseable {
    private Path filePath;
    private int dimension;
    private int recordSize;
    private int size;
    private RandomAccessFile randomAccessFile;

    /// Creates a new DvecReader for the given file path.
    ///
    /// @param filePath The path to the dvec file
    /// @throws IOException If the file cannot be opened or read
    public void open(Path filePath) throws IOException {
        this.filePath = Objects.requireNonNull(filePath, "filePath cannot be null");
        
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
        
        // Calculate record size: 4 bytes for dimension + (dimension * 8 bytes for double values)
        this.recordSize = 4 + (dimension * 8);
        
        // Calculate the total number of vectors in the file
        long fileSize = randomAccessFile.length();
        if (fileSize % recordSize != 0) {
            throw new IOException("File size is not a multiple of record size. File may be corrupted.");
        }
        
        this.size = (int) (fileSize / recordSize);
    }

    /// Retrieves the vector at the specified index.
    ///
    /// @param index The index of the vector to read
    /// @return The double vector at the specified index
    /// @throws IndexOutOfBoundsException If the index is out of bounds
    @Override
    public double[] get(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Index " + index + " out of bounds for size " + size);
        }
        
        try {
            // Calculate offset for the specific vector
            long offset = (long) index * recordSize;
            randomAccessFile.seek(offset);
            
            // Skip the dimension field (we already know it)
            randomAccessFile.skipBytes(4);
            
            // Read the vector values
            double[] vector = new double[dimension];
            byte[] buffer = new byte[dimension * 8];
            int bytesRead = randomAccessFile.read(buffer);
            
            if (bytesRead != dimension * 8) {
                throw new IOException("Failed to read vector data at index " + index);
            }
            
            // Convert bytes to double values
            ByteBuffer doubleBuffer = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN);
            for (int i = 0; i < dimension; i++) {
                vector[i] = doubleBuffer.getDouble(i * 8);
            }
            
            return vector;
        } catch (IOException e) {
            throw new RuntimeException("Error reading vector at index " + index, e);
        }
    }

    @Override
    public void close() throws Exception {
        if (randomAccessFile != null) {
            randomAccessFile.close();
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
        if (!(o instanceof double[] array)) {
            return false;
        }
        
        if (array.length != dimension) {
            return false;
        }
        
        // Linear search through all vectors
        for (int i = 0; i < size; i++) {
            double[] vector = get(i);
            if (Arrays.equals(vector, array)) {
                return true;
            }
        }
        
        return false;
    }

    @Override
    public int indexOf(Object o) {
        if (!(o instanceof double[] array) || array.length != dimension) {
            return -1;
        }
        
        // Linear search through all vectors
        for (int i = 0; i < size; i++) {
            double[] vector = get(i);
            if (Arrays.equals(vector, array)) {
                return i;
            }
        }
        
        return -1;
    }

    @Override
    public int lastIndexOf(Object o) {
        if (!(o instanceof double[] array) || array.length != dimension) {
            return -1;
        }
        
        // Linear search through all vectors in reverse
        for (int i = size - 1; i >= 0; i--) {
            double[] vector = get(i);
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
        Object[] result = new Object[size];
        for (int i = 0; i < size; i++) {
            result[i] = get(i);
        }
        return result;
    }

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

    @Override
    public List<double[]> subList(int fromIndex, int toIndex) {
        if (fromIndex < 0 || toIndex > size || fromIndex > toIndex) {
            throw new IndexOutOfBoundsException("fromIndex: " + fromIndex + ", toIndex: " + toIndex + ", size: " + size);
        }
        
        // Create a view of a sublist - this doesn't copy the data
        return new SublistDvecReader(this, fromIndex, toIndex);
    }
    
    /// Returns the dimension of vectors in this reader.
    ///
    /// @return The dimension of each vector
    public int getDimension() {
        return dimension;
    }
    
    /// A view of a sublist of a DvecReader.
    private static class SublistDvecReader extends ImmutableSizedReader<double[]> {
        private final UniformDvecReader parent;
        private final int offset;
        private final int size;
        
        public SublistDvecReader(UniformDvecReader parent, int fromIndex, int toIndex) {
            this.parent = parent;
            this.offset = fromIndex;
            this.size = toIndex - fromIndex;
        }
        
        @Override
        public double[] get(int index) {
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
            if (!(o instanceof double[] array)) {
                return false;
            }
            
            if (array.length != parent.getDimension()) {
                return false;
            }
            
            // Linear search through this sublist's vectors
            for (int i = 0; i < size; i++) {
                double[] vector = get(i);
                if (Arrays.equals(vector, array)) {
                    return true;
                }
            }
            
            return false;
        }
        
        @Override
        public int indexOf(Object o) {
            if (!(o instanceof double[] array) || array.length != parent.getDimension()) {
                return -1;
            }
            
            // Linear search through this sublist's vectors
            for (int i = 0; i < size; i++) {
                double[] vector = get(i);
                if (Arrays.equals(vector, array)) {
                    return i;
                }
            }
            
            return -1;
        }
        
        @Override
        public int lastIndexOf(Object o) {
            if (!(o instanceof double[] array) || array.length != parent.getDimension()) {
                return -1;
            }
            
            // Linear search through this sublist's vectors in reverse
            for (int i = size - 1; i >= 0; i--) {
                double[] vector = get(i);
                if (Arrays.equals(vector, array)) {
                    return i;
                }
            }
            
            return -1;
        }
        
        @Override
        public List<double[]> subList(int fromIndex, int toIndex) {
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
            Object[] result = new Object[size];
            for (int i = 0; i < size; i++) {
                result[i] = get(i);
            }
            return result;
        }
        
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
