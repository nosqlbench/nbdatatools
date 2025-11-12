package io.nosqlbench.vectordata.spec.datasets.impl.xvec;

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


import io.nosqlbench.vectordata.layoutv2.DSInterval;
import io.nosqlbench.vectordata.merklev2.MAFileChannel;
import io.nosqlbench.vectordata.spec.datasets.types.Indexed;
import io.nosqlbench.vectordata.spec.datasets.types.DatasetView;
import io.nosqlbench.vectordata.layoutv2.DSWindow;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.AsynchronousFileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;

/// Core implementation of DatasetView for xvec file formats.
///
/// This class provides methods for accessing vector data stored in xvec files,
/// which are binary files containing vectors of various types (float, int, byte, etc.).
/// It supports different vector formats based on file extensions.
///
/// @param <T> The type of vector returned by this view (e.g., float[], int[], byte[])
public class CoreXVecDatasetViewMethods<T> implements DatasetView<T>, Prebufferable<T> {

  private final AsynchronousFileChannel channel;
  private final DSWindow window;
  private Class<?> type;
  private Class<?> aryType;
  private long sourceSize;
  private int dimensions;
  private int componentBytes;


  /// Creates a new CoreXVecDatasetViewMethods instance.
  ///
  /// @param channel The MAFileChannel to read from
  /// @param sourceSize The size of the source file in bytes
  /// @param window The window to use for accessing the data
  /// @param extension The file extension indicating the vector format
  public CoreXVecDatasetViewMethods(
      AsynchronousFileChannel channel,
      long sourceSize,
      DSWindow window,
      String extension
  )
  {
    this.channel = channel;
    this.sourceSize = sourceSize;
    this.type = deriveTypeFromExtension(extension);
    this.aryType = type.getComponentType();
    this.componentBytes = componentBytesFromType(this.aryType);
    this.dimensions = readDimensions();
    if (window == null || window.isEmpty()) {
      this.window = new DSWindow(List.of(new DSInterval(0, getCount()),
          new DSInterval(0, getVectorDimensions())));
    } else {
      this.window = window;
    }
  }

  private Class<?> deriveTypeFromExtension(String extension) {
    String lowerExt = extension.toLowerCase();
    switch (lowerExt) {
      case "ivecs":
      case "ivec":
        return int[].class;
      case "bvecs":
      case "bvec":
        return byte[].class;
      case "fvecs":
      case "fvec":
        return float[].class;
      default:
        throw new RuntimeException("Unsupported extension: " + extension);
    }
  }

  @Override
  public CompletableFuture<Void> prebuffer(long startIncl, long endExcl) {
    if (channel instanceof Prebufferable<?>) {
      return ((Prebufferable<?>)channel).prebuffer(startIncl, endExcl);
    } else {
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public CompletableFuture<Void> prebuffer() {
    try {
      long minStart = Long.MAX_VALUE;
      long maxEnd = Long.MIN_VALUE;

      // If window is empty, prebuffer entire file
      if (this.window.isEmpty()) {
        minStart=0;
        maxEnd=channel.size();
      } else {
        // Calculate the full range across all windows
        long recordSize = 4 + (dimensions * componentBytes);

        for (DSInterval interval : this.window) {
          long start = interval.getMinIncl() * recordSize;
          long end = interval.getMaxExcl() * recordSize;
          minStart = Math.min(minStart, start);
          maxEnd = Math.max(maxEnd, end);
        }
      }

      if (this.channel instanceof Prebufferable<?>) {
        return ((Prebufferable<?>)channel).prebuffer(minStart, maxEnd);
      } else {
        return CompletableFuture.completedFuture(null);
      }
    } catch (IOException e) {
      CompletableFuture<Void> failed = new CompletableFuture<>();
      failed.completeExceptionally(e);
      return failed;
    }
  }

  /// Returns the number of bytes per component based on the data type.
  ///
  /// @return The number of bytes per component
  public int componentBytes() {
    return componentBytes;
  }

  /// Determines the number of bytes per component based on the data type.
  ///
  /// @param componentType 
  ///     The component type class
  /// @return The number of bytes per component
  private int componentBytesFromType(Class<?> componentType) {
    if (componentType == int.class) {
      return Integer.BYTES;
    } else if (componentType == byte.class) {
      return Byte.BYTES;
    } else if (componentType == float.class) {
      return Float.BYTES;
    } else if (componentType == double.class) {
      return Double.BYTES;
    } else if (componentType == short.class) {
      return Short.BYTES;
    } else if (componentType == long.class) {
      return Long.BYTES;
    } else {
      throw new RuntimeException("Unsupported component type: " + componentType.getName());
    }
  }

  /// Reads the dimensions (number of components per vector) from the file.
  /// For xvec files, this is typically stored as the first 4 bytes of the file as a little-endian integer.
  /// This method uses absolute positioning to avoid race conditions in multi-threaded environments.
  ///
  /// @return The number of dimensions
  private int readDimensions() {
    try {
      if (channel.size() < 4) {
        throw new RuntimeException("File size is too small to contain dimension information");
      }

      // Read the first 4 bytes to get the dimensions using absolute positioning
      ByteBuffer dimBuffer = ByteBuffer.allocate(4);
      dimBuffer.order(ByteOrder.LITTLE_ENDIAN);
      
      // Use absolute positioning with MAFileChannel
      int bytesRead = channel.read(dimBuffer, 0).get();
      
      if (bytesRead != 4) {
        throw new RuntimeException("Failed to read dimension information from file");
      }

      // Convert to little-endian integer
      dimBuffer.flip();
      int dimensions = dimBuffer.getInt();
      
      if (dimensions <= 0) {
        throw new RuntimeException("Invalid dimensions read from file: " + dimensions);
      }
      
      return dimensions;
    } catch (IOException | InterruptedException | ExecutionException e) {
      throw new RuntimeException("Error reading dimensions from file", e);
    }
  }

  /// Returns the total number of vectors in this dataset.
  ///
  /// This method calculates the count based on the file size, dimensions, and component bytes.
  /// Each vector consists of a 4-byte dimension count followed by the actual vector data.
  ///
  /// @return The total number of vectors
  @Override
  public int getCount() {
      try {
          // If the file is empty or very small, return 0
          long fileSize = channel.size();

          if (fileSize < 4) {
              return 0;
          }

          // Calculate the count based on file size, dimensions, and component bytes
          // Each vector consists of:
          // - 4 bytes for the dimension count (int)
          // - dimensions * componentBytes for the actual data
          long recordSize = 4 + (dimensions * componentBytes);

          // Calculate how many complete records fit in the file
          long vectorCount = fileSize / recordSize;
          
          // Check for integer overflow before casting
          if (vectorCount > Integer.MAX_VALUE) {
              throw new RuntimeException("Vector count " + vectorCount + " exceeds maximum int value. File size: " + fileSize + ", record size: " + recordSize);
          }
          
          return (int) vectorCount;
      } catch (Exception e) {
          // If we can't calculate the count, return 0
          return 0;
      }
  }

  /// Returns the number of dimensions (components) in each vector.
  ///
  /// This value is determined when the file is first opened and is the same for all vectors in the dataset.
  ///
  /// @return The number of dimensions in each vector
  @Override
  public int getVectorDimensions() {
    return dimensions;
  }

  /// Returns the component type of the vectors in this dataset.
  ///
  /// This is determined based on the file extension when the dataset is opened.
  /// For example, .ivec files contain int[] vectors, .fvec files contain float[] vectors, etc.
  ///
  /// @return The component type class (int.class, float.class, etc.)
  @Override
  public Class<?> getDataType() {
    return aryType;
  }

  /// Returns the MAFileChannel used by this dataset view.
  ///
  /// @return The MAFileChannel instance
  public AsynchronousFileChannel getChannel() {
    return channel;
  }

  /// Retrieves a vector at the specified index.
  ///
  /// This method reads the vector data from the file, handling the xvec file format:
  /// ```
  /// +----------------+------------------+
  /// | dimension (4B) | vector data      |
  /// +----------------+------------------+
  /// ```
  ///
  /// If the index is invalid or there's an error reading the data, a default vector is returned.
  ///
  /// @param index 
  ///     The index of the vector to retrieve (0-based)
  /// @return The vector at the specified index, or a default vector if not available
  @Override
  public T get(long index) {
    try {
      // If the file is empty or very small, return a default value
      long fileSize = channel.size();
      if (fileSize < 4) {
        throw new RuntimeException("File size is too small to even contain a single vector");
      }

      // Apply window offset if needed
      // Note: DSWindow doesn't have a translate method, so we'll just use the index as is
      // If window functionality is needed in the future, it would need to be implemented

      // Calculate the record size and position directly
      long recordSize = 4 + (dimensions * componentBytes);
      long position = index * recordSize;

      // Check if the position is valid
      if (position >= fileSize) {
        throw new RuntimeException("position " + position + " is beyond the end of the file of "
                                   + "size " + fileSize + ", index " + index + " is out of bounds"
                                   + " for " + (fileSize/recordSize) + " vectors");
      }

      // Read the dimensions using absolute positioning
      ByteBuffer dimBuffer = ByteBuffer.allocate(4);
      dimBuffer.order(ByteOrder.LITTLE_ENDIAN);
      
      int bytesRead = channel.read(dimBuffer, position).get();
      if (bytesRead != 4) {
        throw new RuntimeException("Failed to read dimension from file, read only " + bytesRead + " bytes");
      }

      dimBuffer.flip();
      int vectorDim = dimBuffer.getInt();

      if (vectorDim <= 0 || vectorDim != dimensions) {
        throw new RuntimeException("Invalid dimension in file: " + vectorDim);
      }

      // Read the vector data at absolute position (after the 4-byte dimension)
      ByteBuffer vectorBuffer = ByteBuffer.allocate(vectorDim * componentBytes);
      vectorBuffer.order(ByteOrder.LITTLE_ENDIAN);
      
      bytesRead = channel.read(vectorBuffer, position + 4).get();
      if (bytesRead != vectorDim * componentBytes) {
        throw new RuntimeException("Failed to read vector data from file, read only " + bytesRead + " bytes for dim " + vectorDim);
      }
      
      vectorBuffer.flip();
      
      // Convert the bytes to the appropriate type
      return (T) convertBytesToVector(vectorBuffer.array(), vectorDim);
    } catch (IOException | InterruptedException | ExecutionException e) {
      throw new RuntimeException("Error reading vector at index " + index, e);
    }
  }

  /// Retrieves a vector at the specified index asynchronously.
  ///
  /// This method provides an asynchronous version of get(long index).
  /// Since the underlying synchronous get method is already implemented,
  /// this returns a completed future with the result.
  ///
  /// @param index 
  ///     The index of the vector to retrieve (0-based)
  /// @return A Future containing the vector at the specified index
  @Override
  public Future<T> getAsync(long index) {
    return CompletableFuture.completedFuture(get(index));
  }

  /// Converts raw bytes to the appropriate vector type.
  ///
  /// @param bytes 
  ///     The raw bytes to convert
  /// @param vectorDim 
  ///     The number of dimensions in this specific vector
  /// @return The vector as the appropriate type
  private Object convertBytesToVector(byte[] bytes, int vectorDim) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    if (aryType == int.class) {
      int[] result = new int[vectorDim];
      for (int i = 0; i < vectorDim; i++) {
        result[i] = buffer.getInt();
      }
      return result;
    } else if (aryType == byte.class) {
      byte[] result = new byte[vectorDim];
      buffer.get(result);
      return result;
    } else if (aryType == float.class) {
      float[] result = new float[vectorDim];
      for (int i = 0; i < vectorDim; i++) {
        result[i] = buffer.getFloat();
      }
      return result;
    } else if (aryType == double.class) {
      double[] result = new double[vectorDim];
      for (int i = 0; i < vectorDim; i++) {
        result[i] = buffer.getDouble();
      }
      return result;
    } else {
      throw new RuntimeException("Unsupported component type: " + aryType.getName());
    }
  }

  /// Retrieves a range of vectors from the dataset.
  ///
  /// This method returns an array of vectors from the specified range of indices.
  /// The range is inclusive of the start index and exclusive of the end index.
  /// Uses chunked bulk reads for efficiency when ranges are large.
  ///
  /// @param startInclusive
  ///     The starting index (inclusive)
  /// @param endExclusive
  ///     The ending index (exclusive)
  /// @return An array containing the vectors in the specified range
  @Override
  public T[] getRange(long startInclusive, long endExclusive) {
    try {
      int count = (int)(endExclusive - startInclusive);
      if (count <= 0) {
        @SuppressWarnings("unchecked")
        T[] empty = (T[]) java.lang.reflect.Array.newInstance(type, 0);
        return empty;
      }

      // Create result array
      @SuppressWarnings("unchecked")
      T[] array = (T[]) java.lang.reflect.Array.newInstance(type, count);

      long recordSize = 4 + (dimensions * componentBytes);

      // ByteBuffer has a 2GB limit (Integer.MAX_VALUE bytes)
      // Calculate max vectors per chunk to stay under limit
      long maxBytesPerChunk = Integer.MAX_VALUE - 1000000;  // Leave some headroom
      int maxVectorsPerChunk = (int)(maxBytesPerChunk / recordSize);

      // Read in chunks if needed
      int processed = 0;
      while (processed < count) {
        int chunkSize = Math.min(maxVectorsPerChunk, count - processed);
        long chunkStart = startInclusive + processed;
        long startPosition = chunkStart * recordSize;
        long chunkBytes = chunkSize * recordSize;

        // Read this chunk
        ByteBuffer buffer = ByteBuffer.allocate((int)chunkBytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        int bytesRead = channel.read(buffer, startPosition).get();
        if (bytesRead != chunkBytes) {
          throw new RuntimeException("Failed to read vector chunk: expected " + chunkBytes + " bytes, got " + bytesRead);
        }

        buffer.flip();

        // Parse vectors from this chunk
        for (int i = 0; i < chunkSize; i++) {
          // Read and validate dimension
          int vectorDim = buffer.getInt();
          if (vectorDim != dimensions) {
            throw new RuntimeException("Invalid dimension in vector " + (chunkStart + i) + ": " + vectorDim);
          }

          // Read vector data
          byte[] vectorBytes = new byte[dimensions * componentBytes];
          buffer.get(vectorBytes);

          // Convert to appropriate type
          array[processed + i] = (T) convertBytesToVector(vectorBytes, dimensions);
        }

        processed += chunkSize;
      }

      return array;
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Error reading vector range [" + startInclusive + ", " + endExclusive + ")", e);
    }
  }

  /// Retrieves a range of vectors from the dataset asynchronously.
  ///
  /// This method provides an asynchronous version of getRange(long, long).
  /// Since the underlying synchronous getRange method is already implemented,
  /// this returns a completed future with the result.
  ///
  /// @param startInclusive 
  ///     The starting index (inclusive)
  /// @param endExclusive 
  ///     The ending index (exclusive)
  /// @return A Future containing an array of vectors in the specified range
  @Override
  public Future<T[]> getRangeAsync(long startInclusive, long endExclusive) {
    return CompletableFuture.completedFuture(getRange(startInclusive, endExclusive));
  }

  /// Retrieves a vector at the specified index, wrapped in an Indexed container.
  ///
  /// This method is similar to get(long index) but returns the vector wrapped in an Indexed object
  /// that includes both the index and the vector value.
  ///
  /// @param index 
  ///     The index of the vector to retrieve
  /// @return An Indexed object containing the index and the vector
  @Override
  public Indexed<T> getIndexed(long index) {
    T value = get(index);
    return new Indexed<>(index, value);
  }

  /// Retrieves a vector at the specified index, wrapped in an Indexed container, asynchronously.
  ///
  /// This method provides an asynchronous version of getIndexed(long index).
  /// Since the underlying synchronous getIndexed method is already implemented,
  /// this returns a completed future with the result.
  ///
  /// @param index 
  ///     The index of the vector to retrieve
  /// @return A Future containing an Indexed object with the index and vector
  @Override
  public Future<Indexed<T>> getIndexedAsync(long index) {
    return CompletableFuture.completedFuture(getIndexed(index));
  }

  /// Retrieves a range of vectors, each wrapped in an Indexed container.
  ///
  /// This method returns an array of Indexed objects, each containing an index and its corresponding vector.
  /// The range is inclusive of the start index and exclusive of the end index.
  ///
  /// @param startInclusive 
  ///     The starting index (inclusive)
  /// @param endExclusive 
  ///     The ending index (exclusive)
  /// @return An array of Indexed objects for the specified range
  @Override
  public Indexed<T>[] getIndexedRange(long startInclusive, long endExclusive) {
    int count = (int)(endExclusive - startInclusive);
    @SuppressWarnings("unchecked")
    Indexed<T>[] result = new Indexed[count];

    for (int i = 0; i < count; i++) {
      result[i] = getIndexed(startInclusive + i);
    }

    return result;
  }

  /// Retrieves a range of vectors, each wrapped in an Indexed container, asynchronously.
  ///
  /// This method provides an asynchronous version of getIndexedRange(long, long).
  /// Since the underlying synchronous getIndexedRange method is already implemented,
  /// this returns a completed future with the result.
  ///
  /// @param startInclusive 
  ///     The starting index (inclusive)
  /// @param endExclusive 
  ///     The ending index (exclusive)
  /// @return A Future containing an array of Indexed objects for the specified range
  @Override
  public Future<Indexed<T>[]> getIndexedRangeAsync(long startInclusive, long endExclusive) {
    return CompletableFuture.completedFuture(getIndexedRange(startInclusive, endExclusive));
  }

  /// Converts the entire dataset to a List of vectors.
  ///
  /// This method reads all vectors from the dataset and returns them as a List.
  /// Note that this can be memory-intensive for large datasets.
  ///
  /// @return A List containing all vectors in the dataset
  @Override
  public List<T> toList() {
    int count = getCount();
    List<T> result = new ArrayList<>(count);

    for (int i = 0; i < count; i++) {
      result.add(get(i));
    }

    return result;
  }

  /// Converts the entire dataset to a List, applying a transformation function to each vector.
  ///
  /// This method reads all vectors from the dataset, applies the given transformation function to each,
  /// and returns the results as a List. This is useful for converting vectors to a different format
  /// or extracting specific information from each vector.
  ///
  /// @param f 
  ///     The transformation function to apply to each vector
  /// @param <U> 
  ///     The type of the transformed elements
  /// @return A List containing the transformed vectors
  @Override
  public <U> List<U> toList(Function<T, U> f) {
    int count = getCount();
    List<U> result = new ArrayList<>(count);

    for (int i = 0; i < count; i++) {
      result.add(f.apply(get(i)));
    }

    return result;
  }

  /// Returns an iterator over the vectors in this dataset.
  ///
  /// This method allows the dataset to be used in for-each loops and other constructs
  /// that work with Iterable objects. The iterator provides sequential access to all
  /// vectors in the dataset.
  ///
  /// @return An iterator over the vectors in this dataset
  @NotNull
  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      private int currentIndex = 0;
      private final int totalCount = getCount();

      @Override
      public boolean hasNext() {
        return currentIndex < totalCount;
      }

      @Override
      public T next() {
        return get(currentIndex++);
      }
    };
  }
}
