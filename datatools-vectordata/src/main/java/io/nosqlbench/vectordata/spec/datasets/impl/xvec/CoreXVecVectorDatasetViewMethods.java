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
import io.nosqlbench.vectordata.spec.datasets.types.VectorDatasetView;
import io.nosqlbench.vectordata.layoutv2.DSWindow;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
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
/// Each record in the file consists of a 4-byte little-endian dimension header
/// followed by `dimension * componentBytes` of vector data. The dimension is
/// uniform across all records and is validated once at construction time.
///
/// Two construction modes are supported:
/// - **Path-based** ({@link #CoreXVecVectorDatasetViewMethods(Path, DSWindow, String)}):
///   Memory-maps the file via {@link SegmentedMappedBuffer} for zero-copy reads.
///   This is the fastest read path.
/// - **Channel-based** ({@link #CoreXVecVectorDatasetViewMethods(AsynchronousFileChannel, long, DSWindow, String)}):
///   Reads through the channel (e.g. {@link MAFileChannel} for remote data).
///   View-level promotion from channel to Path-based mmap is handled by
///   {@link io.nosqlbench.vectordata.downloader.VirtualVectorTestDataView}.
///
/// ## Performance characteristics
///
/// - {@link #get(long)} performs a single channel read per vector and reuses a
///   thread-local buffer to avoid allocation pressure. With mmap, it does a
///   zero-copy slice instead.
/// - {@link #getRange(long, long)} performs one bulk channel read per ~2 GB chunk
///   and parses vectors directly from the bulk buffer without intermediate copies.
/// - {@link #iterator()} prefetches vectors in batches of 1024 via
///   {@link #getRange(long, long)} to amortize I/O overhead.
///
/// @param <T> The type of vector returned by this view (e.g., float[], int[], byte[])
public class CoreXVecVectorDatasetViewMethods<T> implements VectorDatasetView<T>, Prebufferable<T> {

  private final AsynchronousFileChannel channel;
  private final DSWindow window;
  private final Class<?> type;
  private final Class<?> aryType;
  private final int dimensions;
  private final int componentBytes;
  private final long recordSize;
  private final int cachedCount;

  /// Multi-segment memory-mapped view of the file, or {@code null} if the file
  /// is accessed through the {@link #channel} (remote/MAFileChannel case).
  /// When non-null, all read operations use zero-copy slicing from this buffer
  /// instead of kernel syscalls through the channel.
  private final SegmentedMappedBuffer mappedFile;

  /// Thread-local buffer for single-record reads in {@link #get(long)} when
  /// no memory-mapped buffer is available. Sized to hold one complete record
  /// (4-byte header + vector data) and reused across calls to avoid per-read
  /// allocation pressure.
  private final ThreadLocal<ByteBuffer> recordBuffer;

  /// Creates a new CoreXVecDatasetViewMethods instance backed by an
  /// {@link AsynchronousFileChannel}. Each read operation performs a kernel
  /// syscall through the channel. Use the
  /// {@linkplain #CoreXVecVectorDatasetViewMethods(Path, DSWindow, String) path-based constructor}
  /// for local files to enable memory-mapped zero-copy access.
  ///
  /// @param channel The AsynchronousFileChannel to read from
  /// @param sourceSize The size of the source file in bytes
  /// @param window The window to use for accessing the data
  /// @param extension The file extension indicating the vector format
  public CoreXVecVectorDatasetViewMethods(
      AsynchronousFileChannel channel,
      long sourceSize,
      DSWindow window,
      String extension
  )
  {
    this.channel = channel;
    this.mappedFile = null;
    this.type = deriveTypeFromExtension(extension);
    this.aryType = type.getComponentType();
    this.componentBytes = componentBytesFromType(this.aryType);
    this.dimensions = readDimensions();
    this.recordSize = 4 + ((long) dimensions * componentBytes);
    this.window = (window == null || window.isEmpty()) ? null : window;
    this.cachedCount = computeCount(sourceSize);
    this.recordBuffer = ThreadLocal.withInitial(
        () -> ByteBuffer.allocate((int) recordSize).order(ByteOrder.LITTLE_ENDIAN)
    );
  }

  /// Creates a new CoreXVecDatasetViewMethods instance backed by a
  /// {@link SegmentedMappedBuffer}. The file is mapped using multiple
  /// segments so that files of any size are supported. Subsequent reads
  /// are zero-copy within each segment — no kernel syscalls, no buffer
  /// allocation, no Future overhead.
  ///
  /// @param filePath The path to the xvec file
  /// @param window The window to use for accessing the data
  /// @param extension The file extension indicating the vector format
  public CoreXVecVectorDatasetViewMethods(
      Path filePath,
      DSWindow window,
      String extension
  )
  {
    this.type = deriveTypeFromExtension(extension);
    this.aryType = type.getComponentType();
    this.componentBytes = componentBytesFromType(this.aryType);

    try {
      long fileSize = java.nio.file.Files.size(filePath);

      // Memory-map the file using segmented buffers (supports >2 GB)
      this.mappedFile = new SegmentedMappedBuffer(filePath, fileSize);
      this.channel = null;
      this.dimensions = mappedFile.getInt(0);

      this.recordSize = 4 + ((long) dimensions * componentBytes);
      this.window = (window == null || window.isEmpty()) ? null : window;
      this.cachedCount = computeCount(fileSize);
      this.recordBuffer = null;
    } catch (IOException e) {
      throw new RuntimeException("Failed to open xvec file: " + filePath, e);
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
    if (mappedFile != null || startIncl >= endExcl) {
      return CompletableFuture.completedFuture(null);
    }
    if (channel instanceof MAFileChannel) {
      long length = endExcl - startIncl;
      return ((MAFileChannel) channel).prebuffer(startIncl, length);
    }
    if (channel instanceof Prebufferable<?>) {
      @SuppressWarnings("unchecked")
      Prebufferable<T> prebufferable = (Prebufferable<T>) channel;
      return prebufferable.prebuffer(startIncl, endExcl);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> prebuffer() {
    if (mappedFile != null) {
      // Memory-mapped files are managed by the OS page cache — nothing to prebuffer
      return CompletableFuture.completedFuture(null);
    }
    try {
      long minStart = Long.MAX_VALUE;
      long maxEnd = Long.MIN_VALUE;

      // If window is null or empty, prebuffer entire file
      if (this.window == null || this.window.isEmpty()) {
        minStart = 0;
        maxEnd = channel.size();
      } else {
        // Calculate the full range across all windows
        for (DSInterval interval : this.window) {
          long start = interval.getMinIncl() * recordSize;
          long end = interval.getMaxExcl() * recordSize;
          minStart = Math.min(minStart, start);
          maxEnd = Math.max(maxEnd, end);
        }
      }

      if (minStart >= maxEnd) {
        return CompletableFuture.completedFuture(null);
      }
      if (this.channel instanceof MAFileChannel) {
        long length = maxEnd - minStart;
        return ((MAFileChannel) this.channel).prebuffer(minStart, length);
      }
      if (this.channel instanceof Prebufferable<?>) {
        @SuppressWarnings("unchecked")
        Prebufferable<T> prebufferable = (Prebufferable<T>) this.channel;
        return prebufferable.prebuffer(minStart, maxEnd);
      }
      return CompletableFuture.completedFuture(null);
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

  /// Computes the total number of vectors in this dataset view.
  ///
  /// Called once during construction; the result is cached in {@link #cachedCount}.
  ///
  /// @param fileSize the source file size in bytes
  /// @return the total number of accessible vectors
  private int computeCount(long fileSize) {
    if (fileSize < 4) {
      return 0;
    }

    long totalVectorCount = fileSize / recordSize;

    if (window != null && !window.isEmpty()) {
      long windowedCount = 0;
      for (DSInterval interval : window) {
        long intervalSize = interval.getMaxExcl() - interval.getMinIncl();
        windowedCount += intervalSize;
      }
      return (int) Math.min(windowedCount, totalVectorCount);
    }

    if (totalVectorCount > Integer.MAX_VALUE) {
      throw new RuntimeException("Vector count " + totalVectorCount
          + " exceeds maximum int value. File size: " + fileSize
          + ", record size: " + recordSize);
    }

    return (int) totalVectorCount;
  }

  /// Returns the total number of vectors in this dataset view, respecting the configured window.
  ///
  /// @return The total number of vectors accessible through this view
  @Override
  public int getCount() {
    return cachedCount;
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

  /// Returns the AsynchronousFileChannel used by this dataset view.
  ///
  /// @return The AsynchronousFileChannel instance
  public AsynchronousFileChannel getChannel() {
    return channel;
  }

  /// Retrieves a vector at the specified index.
  ///
  /// When a memory-mapped buffer is available (Path constructor), creates a
  /// zero-copy slice at the record position and parses the vector directly —
  /// no syscalls, no allocation. Otherwise, reads the full record in a single
  /// channel read using a thread-local reusable buffer.
  ///
  /// @param index
  ///     The index of the vector to retrieve (0-based)
  /// @return The vector at the specified index
  /// @throws IndexOutOfBoundsException if the index is out of range
  @Override
  @SuppressWarnings("unchecked")
  public T get(long index) {
    if (index < 0 || index >= cachedCount) {
      throw new IndexOutOfBoundsException("Index " + index + " out of range [0, " + cachedCount + ")");
    }

    SegmentedMappedBuffer mmap = this.mappedFile;
    if (mmap != null) {
      long offset = index * recordSize + 4; // skip 4-byte dim header
      int dataBytes = dimensions * componentBytes;
      ByteBuffer slice = mmap.slice(offset, dataBytes);
      return (T) readVectorDirect(slice, dimensions);
    }

    try {
      long position = index * recordSize;
      int recordBytes = (int) recordSize;

      ByteBuffer buf = recordBuffer.get();
      buf.clear();

      int bytesRead = channel.read(buf, position).get();
      if (bytesRead != recordBytes) {
        throw new RuntimeException("Failed to read record at index " + index
            + ": expected " + recordBytes + " bytes, got " + bytesRead);
      }

      buf.flip();
      buf.position(4); // skip dimension header (uniform, validated at construction)

      return (T) readVectorDirect(buf, dimensions);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Error reading vector at index " + index, e);
    }
  }

  /// Reads a typed vector directly from a buffer at its current position.
  ///
  /// Uses bulk typed-buffer views ({@code asFloatBuffer()}, {@code asIntBuffer()},
  /// etc.) to copy vector components into the result array without intermediate
  /// byte array allocation. Advances the source buffer position past the
  /// consumed bytes.
  ///
  /// @param buffer the source buffer positioned at the start of vector data
  /// @param dim the number of components to read
  /// @return the vector as the appropriate array type
  private Object readVectorDirect(ByteBuffer buffer, int dim) {
    int dataBytes = dim * componentBytes;
    if (aryType == float.class) {
      float[] result = new float[dim];
      buffer.asFloatBuffer().get(result);
      buffer.position(buffer.position() + dataBytes);
      return result;
    } else if (aryType == int.class) {
      int[] result = new int[dim];
      buffer.asIntBuffer().get(result);
      buffer.position(buffer.position() + dataBytes);
      return result;
    } else if (aryType == byte.class) {
      byte[] result = new byte[dim];
      buffer.get(result); // get() already advances position
      return result;
    } else if (aryType == double.class) {
      double[] result = new double[dim];
      buffer.asDoubleBuffer().get(result);
      buffer.position(buffer.position() + dataBytes);
      return result;
    } else {
      throw new RuntimeException("Unsupported component type: " + aryType.getName());
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

  /// Retrieves a range of vectors from the dataset.
  ///
  /// When a memory-mapped buffer is available, creates a single zero-copy slice
  /// spanning the entire range and parses vectors directly from it — no syscalls,
  /// no bulk buffer allocation. Otherwise, performs one channel read per ~2 GB
  /// chunk and parses vectors from the read buffer.
  ///
  /// @param startInclusive
  ///     The starting index (inclusive)
  /// @param endExclusive
  ///     The ending index (exclusive)
  /// @return An array containing the vectors in the specified range
  @Override
  @SuppressWarnings("unchecked")
  public T[] getRange(long startInclusive, long endExclusive) {
    int count = (int)(endExclusive - startInclusive);
    if (count <= 0) {
      return (T[]) java.lang.reflect.Array.newInstance(type, 0);
    }

    T[] array = (T[]) java.lang.reflect.Array.newInstance(type, count);

    SegmentedMappedBuffer mmap = this.mappedFile;
    if (mmap != null) {
      for (int i = 0; i < count; i++) {
        long recordOffset = (startInclusive + i) * recordSize;
        ByteBuffer slice = mmap.slice(recordOffset + 4, dimensions * componentBytes);
        array[i] = (T) readVectorDirect(slice, dimensions);
      }
      return array;
    }

    try {
      // ByteBuffer has a 2GB limit (Integer.MAX_VALUE bytes)
      long maxBytesPerChunk = Integer.MAX_VALUE - 1_000_000L;
      int maxVectorsPerChunk = (int)(maxBytesPerChunk / recordSize);

      int processed = 0;
      while (processed < count) {
        int chunkSize = Math.min(maxVectorsPerChunk, count - processed);
        long chunkStart = startInclusive + processed;
        long startPosition = chunkStart * recordSize;
        int chunkBytes = (int)(chunkSize * recordSize);

        ByteBuffer buffer = ByteBuffer.allocate(chunkBytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        int bytesRead = channel.read(buffer, startPosition).get();
        if (bytesRead != chunkBytes) {
          throw new RuntimeException("Failed to read vector chunk: expected "
              + chunkBytes + " bytes, got " + bytesRead);
        }

        buffer.flip();

        for (int i = 0; i < chunkSize; i++) {
          int vectorDim = buffer.getInt();
          if (vectorDim != dimensions) {
            throw new RuntimeException("Invalid dimension in vector "
                + (chunkStart + i) + ": " + vectorDim);
          }
          array[processed + i] = (T) readVectorDirect(buffer, dimensions);
        }

        processed += chunkSize;
      }

      return array;
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Error reading vector range ["
          + startInclusive + ", " + endExclusive + ")", e);
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

    T[] vectors = getRange(startInclusive, endExclusive);
    for (int i = 0; i < count; i++) {
      result[i] = new Indexed<>(startInclusive + i, vectors[i]);
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

  /// Number of vectors to prefetch per batch in the iterator.
  private static final int ITERATOR_PREFETCH_SIZE = 1024;

  /// Returns an iterator over the vectors in this dataset.
  ///
  /// The iterator prefetches vectors in batches of {@value #ITERATOR_PREFETCH_SIZE}
  /// via {@link #getRange(long, long)} to amortize I/O overhead across many
  /// elements. This is substantially faster than per-element {@link #get(long)}
  /// calls, especially for sequential scans.
  ///
  /// @return An iterator over the vectors in this dataset
  @NotNull
  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      private int nextIndex = 0;
      private final int totalCount = cachedCount;
      private T[] batch;
      private int batchOffset = 0;
      private int batchLen = 0;

      @Override
      public boolean hasNext() {
        return nextIndex < totalCount;
      }

      @Override
      public T next() {
        if (batch == null || batchOffset >= batchLen) {
          int remaining = totalCount - nextIndex;
          int fetchSize = Math.min(ITERATOR_PREFETCH_SIZE, remaining);
          batch = getRange(nextIndex, nextIndex + fetchSize);
          batchLen = batch.length;
          batchOffset = 0;
        }
        nextIndex++;
        return batch[batchOffset++];
      }
    };
  }
}
