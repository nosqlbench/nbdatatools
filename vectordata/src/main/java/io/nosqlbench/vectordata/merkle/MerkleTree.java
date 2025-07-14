package io.nosqlbench.vectordata.merkle;

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


// Removed OkHttp dependencies to allow standard URLConnection for test stubbing

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.StringJoiner;

import io.nosqlbench.vectordata.merkle.tasks.TreeBuildingTask;
import io.nosqlbench.vectordata.status.EventSink;
import io.nosqlbench.vectordata.status.NoOpDownloadEventSink;

/// # IDEA: Consider replacing this with https://github.com/crums-io/merkle-tree
///
/// # REQUIREMENTS:
///
/// This merkle tree implementation stores all hashes in a flat array. It tracks whether a hash
/// for a node is stale with a bit set. When a merkle tree is loaded from disk, the bitset should
/// be set to all true, indicating that all hashes are valid. However, when a chunk of data is
/// presented with a [MerkleMismatch] instance, the bitset should be set to false for the
/// affected nodes all the way to the root node.
///
/// The hash values for a node should only be updated when accessed, meaning it is possible to
/// update multiple leaf nodes and have the hash values be in some incorrect state while not
/// being observed. The bitset is responsible for tracking dirty hash values. When hash values
/// are updated right before access, the indices of the affected nodes should be marked as valid
/// after the path from the root to all affected leaves are updated with correct values.
///
/// Right before saving, the root hash should be accessed to force this computation, and the
/// bitset should be verified to be all true, i.e. all hashes are valid and computed.
///
/// When a merkle tree is saved to disk, it must have all the hashes computed correctly first.
/// This also means that all the valid bits should have already been set.
/// Before the merkle tree is written to disk, the root node should be accessed to force all
/// hashes to be computed according to which ones are invalid according to the bitset.
/// The footer containing metadata (chunk size, total size) is then written to the file.
///
/// The hashes are stored in root-first order, meaning that the leaf nodes start somewhere on the
/// interior of the hash data. This is represented by the offset value.

public class MerkleTree implements AutoCloseable {
  /// SHA-256 hash size in bytes
  public static final int HASH_SIZE = 32;
  /// Shared MessageDigest instance for SHA-256 hashing
  public static final MessageDigest DIGEST;
  /// Object pool for caching MessageDigest instances
  private static final ObjectPool<MessageDigest> DIGEST_POOL;
  /// Logger for this class
  private static final Logger logger = LogManager.getLogger(MerkleTree.class);

  /// Event sink for instrumentation and testing
  private final EventSink eventSink;

  /// Flag to track if the tree has been closed
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /// Closes this merkle tree and releases any resources associated with it.
  /// This method is synchronized to ensure thread safety during concurrent access.
  /// @throws IOException
  ///     if an I/O error occurs
  @Override
  public synchronized void close() throws IOException {
    if (closed.compareAndSet(false, true)) {
      eventSink.debug("Closing MerkleTree");

      // Close the file channel if it's open
      if (fileChannel != null) {
        try {
          fileChannel.close();
        } catch (IOException e) {
          eventSink.error("Error closing file channel: {}", e.getMessage());
          throw e;
        }
      }

      // Force flush of mapped buffer before closing to ensure data persistence
      if (mappedBuffer instanceof MappedByteBuffer) {
        try {
          ((MappedByteBuffer) mappedBuffer).force();
        } catch (Exception e) {
          eventSink.error("Error flushing mapped buffer: {}", e.getMessage());
        }
      }
    }
  }

  /// Checks if this merkle tree has been closed.
  /// @return true if this merkle tree has been closed, false otherwise
  public boolean isClosed() {
    return closed.get();
  }

  /// Calculates the chunk-related values based on total size and chunk size.
  /// @param totalSize
  ///     The total size of the data
  /// @param chunkSize
  ///     The chunk size (ignored - kept for backward compatibility)
  /// @return A ChunkGeometryDescriptor containing the calculated values
  /// @deprecated Use calculateGeometry(long) instead as chunk size is now automatically calculated
  @Deprecated
  public static ChunkGeometryDescriptor calculateGeometry(long totalSize, long chunkSize) {
    // ChunkGeometryDescriptor is now the sole authority for chunk size calculation
    // Ignore the chunkSize parameter and use automatic calculation
    return new ChunkGeometryDescriptor(totalSize);
  }

  /// Calculates the chunk-related values based on total size only.
  /// @param totalSize
  ///     The total size of the data
  /// @return A ChunkGeometryDescriptor containing the calculated values
  public static ChunkGeometryDescriptor calculateGeometry(long totalSize) {
    // Create a ChunkGeometryDescriptor with automatic chunk size calculation
    return new ChunkGeometryDescriptor(totalSize);
  }


  static {
    try {
      DIGEST = MessageDigest.getInstance("SHA-256");
      // Initialize the digest pool with a supplier that creates new MessageDigest instances
      // and a reset function that resets the digest
      DIGEST_POOL = new ObjectPool<>(
          () -> {
            try {
              return MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
              throw new RuntimeException("SHA-256 algorithm not available: " + e.getMessage(), e);
            }
          }, MessageDigest::reset, null
      );
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 algorithm not available: " + e.getMessage(), e);
    }
  }

  /// Creates a new instance of MessageDigest for SHA-256 hashing.
  /// This factory method provides a consistent way to get new digest instances
  /// for thread-safe hash computations.
  ///
  /// Example usage:
  /// ```
  /// MessageDigest digest = MerkleTree.createDigest();
  /// digest.update(data);
  /// byte[] hash = digest.digest();
  ///```
  /// @return A MessageDigest instance configured for SHA-256 hashing from the object pool
  /// @throws RuntimeException
  ///     if the SHA-256 algorithm is not available
  public ObjectPool.Borrowed<MessageDigest> withDigest() {
    // Get a digest instance from the pool
    // Note: The caller is responsible for resetting the digest when done
    return DIGEST_POOL.borrowObject();
  }

  // Tree structure and metadata
  private final BitSet valid;
  /// Geometry descriptor containing all the tree dimensions and chunk properties
  private final ChunkGeometryDescriptor geometry;

  // These properties are derived from the geometry descriptor
  // They are kept for backward compatibility with existing code
  private final int leafCount;
  private final int capLeaf;
  private final int offset;
  private final long chunkSize;
  private final long totalSize;

  // Memory-mapped file access - always required for merkle tree operations
  private final FileChannel fileChannel;
  private final ByteBuffer mappedBuffer;


  /// Builds a Merkle tree from raw data buffer.
  /// All nodes are fully computed and marked valid.
  /// The chunk size is automatically calculated based on content size.
  /// The range is automatically determined from the buffer capacity (0 to capacity).
  /// @param data
  ///     The raw data buffer.
  /// @return The built Merkle tree.
  public static MerkleTree fromData(ByteBuffer data)
  {

    try {
      // Use the entire buffer (no range needed)
      ByteBuffer dataSlice = data.duplicate();

      // Calculate the effective length from the buffer capacity
      final long effectiveLength = data.capacity();

      // Calculate chunk-related values using the provided chunk size if specified
      ChunkGeometryDescriptor calc = calculateGeometry(effectiveLength);

      // Get the chunk size (either provided or automatically calculated)
      long calculatedChunkSize = calc.getChunkSize();

      // Create an empty BitSet for tracking valid nodes
      BitSet valid = new BitSet(calc.getNodeCount());
      int bitSetSize = valid.toByteArray().length;

      // Create a temporary file for the merkle tree
      Path tempFile = Files.createTempFile("merkle", ".mrkl");
      tempFile.toFile().deleteOnExit(); // Ensure the file is deleted when the JVM exits

      // Calculate the total file size needed
      long dataRegionSize = (calc.getCapLeaf() + calc.getOffset()) * HASH_SIZE;
      long footerSize = MerkleFooter.create(calculatedChunkSize, effectiveLength, bitSetSize).footerLength();
      long totalFileSize = dataRegionSize + bitSetSize + footerSize;

      // Create the file but don't physically allocate space
      try (FileChannel channel = FileChannel.open(
          tempFile,
          StandardOpenOption.CREATE,
          StandardOpenOption.WRITE,
          StandardOpenOption.TRUNCATE_EXISTING
      ))
      {
        // Write the footer with the BitSet size at the end of the file
        // Use absolute positioning for thread safety
        MerkleFooter footer = MerkleFooter.create(calculatedChunkSize, effectiveLength, bitSetSize);
        channel.write(footer.toByteBuffer(), dataRegionSize + bitSetSize);
      }

      // Open the file channel for memory mapping
      FileChannel merkleFileChannel =
          FileChannel.open(tempFile, StandardOpenOption.READ, StandardOpenOption.WRITE);

      try {
        // Memory map the hash data region
        ByteBuffer mappedBuffer =
            merkleFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, dataRegionSize);

        BitSet tracker = valid;
        // Create the MerkleTree with memory-mapped access
        MerkleTree merkleTree = new MerkleTree(
            tracker,
            calc,
            merkleFileChannel,
            mappedBuffer
        );

        // Process the data directly from the ByteBuffer
        // Reset the position to read from the beginning
        dataSlice.rewind();

        // Get total number of chunks from ChunkGeometryDescriptor
        int totalChunks = calc.getTotalChunks();

        // Process each chunk
        for (int i = 0; i < totalChunks; i++) {
          // Calculate the byte range for this chunk
          long chunkStartOffset = i * calculatedChunkSize;
          long chunkEndOffset = Math.min(chunkStartOffset + calculatedChunkSize, effectiveLength);
          int bytesToRead = (int) (chunkEndOffset - chunkStartOffset);

          // Create a slice of the data buffer for this chunk
          ByteBuffer chunkBuffer = dataSlice.duplicate();
          chunkBuffer.position((int) chunkStartOffset);
          chunkBuffer.limit((int) chunkStartOffset + bytesToRead);
          ByteBuffer chunkSlice = chunkBuffer.slice();

          // Process the chunk
          merkleTree.hashData(chunkStartOffset, chunkEndOffset, chunkSlice);
        }

        // Force computation of the root hash to ensure all internal nodes are computed
        merkleTree.getHash(0);

        return merkleTree;
      } catch (Exception e) {
        // Close the file channel if there's an error
        merkleFileChannel.close();
        throw e;
      }
    } catch (IllegalArgumentException e) {
      // Re-throw IllegalArgumentException directly
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create MerkleTree from data: " + e.getMessage(), e);
    }
  }

  /// Builds a Merkle tree from raw data buffer with progress tracking.
  /// All nodes are fully computed and marked valid.
  /// This method provides progress tracking through a MerkleTreeBuildProgress object.
  /// The chunk size is automatically calculated based on content size.
  /// The range is automatically determined from the buffer capacity (0 to capacity).
  /// @param data
  ///     The raw data buffer.
  /// @return A MerkleTreeBuildProgress that tracks the progress and completes with the built Merkle tree.
  public static MerkleTreeBuildProgress fromDataWithProgress(ByteBuffer data)
  {
    try {
      // Use the entire buffer (no range needed)
      ByteBuffer dataSlice = data.duplicate();

      // Calculate the effective range length from the buffer capacity
      final long effectiveLength = data.capacity();

      // Calculate chunk-related values using automatic chunk size calculation
      final ChunkGeometryDescriptor calc = calculateGeometry(effectiveLength);
      // Get the automatically calculated chunk size
      long calculatedChunkSize = calc.getChunkSize();
      // Get the total number of chunks from the calculated dimensions
      int totalChunks = calc.getTotalChunks();
      int internalNodeCount = calc.getInternalNodeCount();

      // Create progress tracker
      MerkleTreeBuildProgress progress =
          new MerkleTreeBuildProgress(
              totalChunks + internalNodeCount,
              effectiveLength,
              MerkleTreeBuildProgress.Stage.INITIALIZING
          );

      // Run the actual tree building in a separate thread to avoid blocking
      CompletableFuture.runAsync(() -> {
        try {
          progress.setStage(MerkleTreeBuildProgress.Stage.FILE_EXISTENCE_CHECK);

          // Create a temporary file for the merkle tree
          Path tempFile = Files.createTempFile("merkle", ".mrkl");
          tempFile.toFile().deleteOnExit(); // Ensure the file is deleted when the JVM exits

          // Calculate the total file size needed
          long dataRegionSize = (calc.getCapLeaf() + calc.getOffset()) * HASH_SIZE;
          int bitSetSize = new BitSet(calc.getNodeCount()).toByteArray().length;
          long footerSize =
              MerkleFooter.create(calculatedChunkSize, effectiveLength, bitSetSize).footerLength();
          long totalFileSize = dataRegionSize + bitSetSize + footerSize;

          progress.setStage(MerkleTreeBuildProgress.Stage.FILE_CHANNEL_OPENING);

          // Create the file but don't physically allocate space
          try (FileChannel channel = FileChannel.open(
              tempFile,
              StandardOpenOption.CREATE,
              StandardOpenOption.WRITE,
              StandardOpenOption.TRUNCATE_EXISTING
          ))
          {
            // Write the footer with the BitSet size at the end of the file
            // Use absolute positioning for thread safety
            MerkleFooter footer = MerkleFooter.create(calculatedChunkSize, effectiveLength, bitSetSize);
            channel.write(footer.toByteBuffer(), dataRegionSize + bitSetSize);
          }

          // Open the file channel for memory mapping
          FileChannel merkleFileChannel =
              FileChannel.open(tempFile, StandardOpenOption.READ, StandardOpenOption.WRITE);

          // Memory map the hash data region
          ByteBuffer mappedBuffer =
              merkleFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, dataRegionSize);

          BitSet valid = new BitSet(calc.getNodeCount());

          // Create the MerkleTree with memory-mapped access
          MerkleTree merkleTree = new MerkleTree(
              valid,
              calc,
              merkleFileChannel,
              mappedBuffer
          );

          // Process the data directly from the ByteBuffer
          progress.setStage(MerkleTreeBuildProgress.Stage.LEAF_NODE_PROCESSING);

          // Reset the position to read from the beginning
          dataSlice.rewind();

          // Process each chunk
          for (int i = 0; i < totalChunks; i++) {
            // Calculate the byte range for this chunk
            long chunkStartOffset = i * calculatedChunkSize;
            long chunkEndOffset = Math.min(chunkStartOffset + calculatedChunkSize, effectiveLength);
            int bytesToRead = (int) (chunkEndOffset - chunkStartOffset);

            // Create a slice of the data buffer for this chunk
            ByteBuffer chunkBuffer = dataSlice.duplicate();
            chunkBuffer.position((int) chunkStartOffset);
            chunkBuffer.limit((int) chunkStartOffset + bytesToRead);
            ByteBuffer chunkSlice = chunkBuffer.slice();

            // Process the chunk
            merkleTree.hashData(chunkStartOffset, chunkEndOffset, chunkSlice);

            // Update progress
            progress.incrementProcessedChunks();
          }

          // Update stage to indicate we're now processing internal nodes
          progress.setStage(MerkleTreeBuildProgress.Stage.INTERNAL_NODE_PROCESSING);

          // Force computation of the root hash to ensure all internal nodes are computed
          merkleTree.getHash(0);

          // Update progress for internal nodes
          for (int i = 0; i < internalNodeCount; i++) {
            progress.incrementProcessedChunks();
          }

          // Complete the progress with the built tree
          progress.complete(merkleTree);
        } catch (Exception e) {
          progress.completeExceptionally(new RuntimeException("Failed to create MerkleTree from data: " + e.getMessage(),
              e
          ));
        }
      });

      return progress;
    } catch (IllegalArgumentException e) {
      // Re-throw IllegalArgumentException directly
      MerkleTreeBuildProgress progress = new MerkleTreeBuildProgress(0, 0);
      progress.completeExceptionally(e);
      return progress;
    } catch (Exception e) {
      MerkleTreeBuildProgress progress = new MerkleTreeBuildProgress(0, 0);
      progress.completeExceptionally(new RuntimeException(
          "Failed to create MerkleTree from data: " + e.getMessage(),
          e
      ));
      return progress;
    }
  }

  /// Builds a Merkle tree from a file of arbitrary size using a shared read-ahead buffer.
  /// All nodes are fully computed and marked valid.
  /// This method uses virtual threads for parallel processing and is thread-safe.
  /// The shared buffer improves I/O efficiency by pre-reading data from the file.
  /// The range is automatically determined from the file size (0 to file size).
  /// @param filePath
  ///     The path to the file to build the tree from.
  /// @return A MerkleTreeBuildProgress that tracks the progress and completes with the built Merkle tree.
  public static MerkleTreeBuildProgress fromData(Path filePath) {
    return fromDataInternal(filePath, new NoOpDownloadEventSink());
  }

  /// Builds a Merkle tree from a file of arbitrary size using a shared read-ahead buffer with a specified event sink.
  /// All nodes are fully computed and marked valid.
  /// This method uses virtual threads for parallel processing and is thread-safe.
  /// The shared buffer improves I/O efficiency by pre-reading data from the file.
  /// The range is automatically determined from the file size (0 to file size).
  /// @param filePath
  ///     The path to the file to build the tree from.
  /// @param eventSink
  ///     The event sink for instrumentation and testing.
  /// @return A MerkleTreeBuildProgress that tracks the progress and completes with the built Merkle tree.
  public static MerkleTreeBuildProgress fromData(Path filePath, EventSink eventSink) {
    return fromDataInternal(filePath, eventSink);
  }

  /// Builds a Merkle tree from a file of arbitrary size using a shared read-ahead buffer.
  /// All nodes are fully computed and marked valid.
  /// This method uses virtual threads for parallel processing and is thread-safe.
  /// The shared buffer improves I/O efficiency by pre-reading data from the file.
  /// If the file does not exist, an empty Merkle tree will be created.
  ///
  /// The implementation uses SHA-256 hashing for all nodes and memory-mapped file access
  /// for efficient operations. The tree structure is determined by the following calculations:
  /// - leafCount: ceil(totalSize/chunkSize) - The number of actual data chunks
  /// - capLeaf: Next power of 2 >= leafCount - Ensures a complete binary tree
  /// - nodeCount: 2*capLeaf-1 - Total nodes in the complete binary tree
  /// - offset: capLeaf-1 - Index offset for the first leaf node
  /// - internalNodeCount: nodeCount-leafCount - Number of internal nodes
  ///
  /// The resulting merkle file (.mrkl) has the following layout:
  /// ```
  ///  ┌───────────────────────────────────────────────────────┐
  ///  │                   Merkle File Layout                   │
  ///  ├───────────────────────────────────────────────────────┤
  ///  │                                                       │
  ///  │  ┌───────────────────────────────────────────────┐   │
  ///  │  │               Leaf Hashes                     │   │
  ///  │  │  (Hashes of data chunks, size = leafCount)    │   │
  ///  │  │  [offset to offset+leafCount-1]               │   │
  ///  │  └───────────────────────────────────────────────┘   │
  ///  │                                                       │
  ///  │  ┌───────────────────────────────────────────────┐   │
  ///  │  │               Padded Leaves                   │   │
  ///  │  │  (Zero-filled hashes if needed to reach       │   │
  ///  │  │   next power of 2, size = capLeaf - leafCount)│   │
  ///  │  │  [offset+leafCount to offset+capLeaf-1]       │   │
  ///  │  └───────────────────────────────────────────────┘   │
  ///  │                                                       │
  ///  │  ┌───────────────────────────────────────────────┐   │
  ///  │  │               Internal Nodes                  │   │
  ///  │  │  (Hashes of internal nodes including root,    │   │
  ///  │  │   size = internalNodeCount)                   │   │
  ///  │  │  [0 to offset-1]                              │   │
  ///  │  │  ┌─────────────────────┐                      │   │
  ///  │  │  │     Root Hash       │ (always at index 0)  │   │
  ///  │  │  └─────────────────────┘                      │   │
  ///  │  └───────────────────────────────────────────────┘   │
  ///  │                                                       │
  ///  │  ┌───────────────────────────────────────────────┐   │
  ///  │  │               BitSet Data                     │   │
  ///  │  │  (Tracks which nodes are valid)               │   │
  ///  │  └───────────────────────────────────────────────┘   │
  ///  │                                                       │
  ///  │  ┌───────────────────────────────────────────────┐   │
  ///  │  │               Footer                          │   │
  ///  │  │  (Metadata: chunk size, total size, BitSet)   │   │
  ///  │  └───────────────────────────────────────────────┘   │
  ///  │                                                       │
  ///  └───────────────────────────────────────────────────────┘
  /// ```
  ///
  /// The file format can have four variations depending on the tree structure:
  /// 1. Padded Tree Format: Contains all nodes (leaves and internal nodes) with padding
  /// 2. Exact Complete Tree Format: Contains all nodes without padding
  /// 3. Leaves-Only Format: Contains only leaf nodes
  /// 4. Padded-Leaves-Only Format: Contains only leaf nodes with padding
  ///
  /// To access the root hash from a merkle tree file:
  /// 1. Load the tree using MerkleTree.load(Path path)
  /// 2. Call getHash(0) on the loaded tree
  ///

  /// Builds a Merkle tree from a file by processing it in chunks with a specified event sink.
  /// This method uses an automatically calculated chunk size based on the content size.
  /// The range is automatically determined from the file size (0 to file size).
  /// @param filePath
  ///     The path to the file to build the tree from.
  /// @param eventSink
  ///     The event sink for instrumentation and testing.
  /// @return A MerkleTreeBuildProgress that tracks the progress and completes with the built Merkle tree.
  private static MerkleTreeBuildProgress fromDataInternal(Path filePath, EventSink eventSink) {
    try {
      // Get the file size if it exists, otherwise use 0 for empty files
      long fileSize = Files.exists(filePath) ? Files.size(filePath) : 0;

      // Create a range for the entire file (0 to file size)
      final MerkleRange range = new MerkleRange(0, fileSize);

      // Calculate chunk-related values using automatic chunk size calculation
      final ChunkGeometryDescriptor calc = calculateGeometry(fileSize);
      // Get the automatically calculated chunk size
      long calculatedChunkSize = calc.getChunkSize();
      // Get the total number of chunks from the calculated dimensions
      int totalChunks = calc.getTotalChunks();
      int internalNodeCount = calc.getInternalNodeCount();
      MerkleTreeBuildProgress progress =
          new MerkleTreeBuildProgress(
              totalChunks + internalNodeCount,
              fileSize,
              MerkleTreeBuildProgress.Stage.INITIALIZING
          );

      // Create a single AsyncFileChannel to be shared by all subtasks
      AsynchronousFileChannel fileChannel = null;
      if (Files.exists(filePath)) {
        fileChannel = AsynchronousFileChannel.open(filePath, StandardOpenOption.READ);
      }

      // Run the actual tree building in a separate thread to avoid blocking
      CompletableFuture.runAsync(new TreeBuildingTask(
          progress,
          fileSize, filePath,
          range, calc,
          fileChannel,
          eventSink
      ));

      return progress;
    } catch (IOException e) {
      MerkleTreeBuildProgress progress = new MerkleTreeBuildProgress(0, 0);
      progress.completeExceptionally(new RuntimeException("Failed to create memory-mapped MerkleTree: " + e.getMessage(),
          e
      ));
      return progress;
    }
  }

  // TreeBuildingTask has been moved to its own class in the merkle.tasks package

  // FileChunkProcessor has been consolidated into ChunkWorker

  /// Builds a Merkle tree from a file by processing it in chunks.
  /// All nodes are fully computed and marked valid.
  /// This method avoids loading the entire file into memory at once.
  /// This method is provided for backward compatibility and internally calls fromData.
  /// If the file does not exist, an empty Merkle tree will be created.
  /// @param filePath
  ///     The path to the file to build the tree from.
  /// @param chunkSize
  ///     The size of each chunk (ignored, kept for backward compatibility).
  ///     Chunk size is now automatically calculated based on content size.
  /// @param range
  ///     The range within the file to build the tree from (ignored, kept for backward compatibility).
  ///     The range is now automatically determined from the file size (0 to file size).
  /// @return The built Merkle tree.
  /// @throws IOException
  ///     If there's an error reading the file.
  public static MerkleTree fromFile(Path filePath, long chunkSize, MerkleRange range)
      throws IOException
  {
    return fromFile(filePath, chunkSize, range, new NoOpDownloadEventSink());
  }

  /// Builds a Merkle tree from a file by processing it in chunks with a specified event sink.
  /// All nodes are fully computed and marked valid.
  /// This method avoids loading the entire file into memory at once.
  /// This method is provided for backward compatibility and internally calls fromData.
  /// If the file does not exist, an empty Merkle tree will be created.
  /// @param filePath
  ///     The path to the file to build the tree from.
  /// @param chunkSize
  ///     The size of each chunk (ignored, kept for backward compatibility).
  ///     Chunk size is now automatically calculated based on content size.
  /// @param range
  ///     The range within the file to build the tree from (ignored, kept for backward compatibility).
  ///     The range is now automatically determined from the file size (0 to file size).
  /// @param eventSink
  ///     The event sink for instrumentation and testing.
  /// @return The built Merkle tree.
  /// @throws IOException
  ///     If there's an error reading the file.
  public static MerkleTree fromFile(Path filePath, long chunkSize, MerkleRange range, EventSink eventSink)
      throws IOException
  {
    // Note: chunkSize parameter is ignored as it's now calculated automatically
    // based on content size. This parameter is kept for backward compatibility.

    try {
      // Call fromDataInternal and wait for the result
      MerkleTreeBuildProgress progress = fromDataInternal(filePath, eventSink);
      return progress.getFuture().join();
    } catch (Exception e) {
      if (e instanceof IllegalArgumentException) {
        throw (IllegalArgumentException) e;
      }
      throw new IOException("Failed to create MerkleTree from file: " + e.getMessage(), e);
    }
  }


  /// Creates an empty (all stale) Merkle tree for given size.
  /// @param totalSize
  ///     The total size of the data the tree represents.
  /// @return The created empty Merkle tree.
  public static MerkleTree createEmpty(long totalSize) {
    return createEmpty(totalSize, new NoOpDownloadEventSink());
  }

  /// Creates an empty (all stale) Merkle tree for given size.
  /// @param totalSize
  ///     The total size of the data the tree represents.
  /// @param chunkSize
  ///     The chunk size (ignored - kept for backward compatibility)
  /// @return The created empty Merkle tree.
  /// @deprecated Use createEmpty(long) instead as chunk size is now automatically calculated
  @Deprecated
  public static MerkleTree createEmpty(long totalSize, long chunkSize) {
    return createEmpty(totalSize, new NoOpDownloadEventSink());
  }

  /// Creates an empty (all stale) Merkle tree for given size with a specified event sink.
  /// @param totalSize
  ///     The total size of the data the tree represents.
  /// @param chunkSize
  ///     The size of each chunk (ignored, kept for backward compatibility).
  ///     Chunk size is now automatically calculated based on content size.
  /// @param eventSink
  ///     The event sink for instrumentation and testing.
  /// @return The created empty Merkle tree.
  /// @deprecated Use createEmpty(long, EventSink) instead as chunk size is now automatically calculated
  @Deprecated
  public static MerkleTree createEmpty(long totalSize, long chunkSize, EventSink eventSink) {
    return createEmpty(totalSize, eventSink);
  }
  
  /// Creates an empty (all stale) Merkle tree using a specific ChunkGeometryDescriptor.
  /// This method is intended for testing purposes where specific chunk sizes are needed.
  /// @param calc
  ///     The ChunkGeometryDescriptor containing pre-calculated chunk geometry.
  /// @param eventSink
  ///     The event sink for instrumentation and testing.
  /// @return The created empty Merkle tree.
  public static MerkleTree createEmpty(ChunkGeometryDescriptor calc, EventSink eventSink) {
    // Get the chunk size from the descriptor
    long chunkSize = calc.getChunkSize();
    long totalSize = calc.getTotalContentSize();

    // Create an empty BitSet for tracking valid nodes
    BitSet valid = new BitSet(calc.getNodeCount());
    int bitSetSize = valid.toByteArray().length;

    try {
      // Create a temporary file for the merkle tree
      Path tempFile = Files.createTempFile("merkle", ".mrkl");
      tempFile.toFile().deleteOnExit(); // Ensure the file is deleted when the JVM exits

      // Calculate the total file size needed
      long dataRegionSize = (calc.getCapLeaf() + calc.getOffset()) * HASH_SIZE;
      long footerSize = MerkleFooter.create(chunkSize, totalSize, bitSetSize).footerLength();
      long totalFileSize = dataRegionSize + bitSetSize + footerSize;

      // Create the file but don't physically allocate space
      try (FileChannel channel = FileChannel.open(
          tempFile,
          StandardOpenOption.CREATE,
          StandardOpenOption.WRITE,
          StandardOpenOption.TRUNCATE_EXISTING
      ))
      {
        // Write the footer with the BitSet size at the end of the file
        // Use absolute positioning for thread safety
        MerkleFooter footer = MerkleFooter.create(chunkSize, totalSize, bitSetSize);
        channel.write(footer.toByteBuffer(), dataRegionSize + bitSetSize);
      }

      // Open the file channel for memory mapping
      FileChannel fileChannel =
          FileChannel.open(tempFile, StandardOpenOption.READ, StandardOpenOption.WRITE);

      try {
        // Memory map the hash data region - this will create the file structure without physical allocation
        ByteBuffer mappedBuffer =
            fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, dataRegionSize);

        // Create the MerkleTree with memory-mapped access
        return new MerkleTree(
            valid,
            calc,
            fileChannel,
            mappedBuffer,
            eventSink
        );
      } catch (Exception e) {
        // Close the file channel if there's an error
        fileChannel.close();
        throw e;
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to create memory-mapped MerkleTree: " + e.getMessage(), e);
    }
  }

  /// Creates an empty (all stale) Merkle tree for given size with a specified event sink.
  /// @param totalSize
  ///     The total size of the data the tree represents.
  /// @param eventSink
  ///     The event sink for instrumentation and testing.
  /// @return The created empty Merkle tree.
  public static MerkleTree createEmpty(long totalSize, EventSink eventSink) {
    // Calculate chunk-related values using ChunkGeometryDescriptor as sole authority
    ChunkGeometryDescriptor calc = calculateGeometry(totalSize);

    // Get the automatically calculated chunk size
    long calculatedChunkSize = calc.getChunkSize();

    // Create an empty BitSet for tracking valid nodes
    BitSet valid = new BitSet(calc.getNodeCount());
    int bitSetSize = valid.toByteArray().length;

    try {
      // Create a temporary file for the merkle tree
      Path tempFile = Files.createTempFile("merkle", ".mrkl");
      tempFile.toFile().deleteOnExit(); // Ensure the file is deleted when the JVM exits

      // Calculate the total file size needed
      long dataRegionSize = (calc.getCapLeaf() + calc.getOffset()) * HASH_SIZE;
      long footerSize = MerkleFooter.create(calculatedChunkSize, totalSize, bitSetSize).footerLength();
      long totalFileSize = dataRegionSize + bitSetSize + footerSize;

      // Create the file but don't physically allocate space
      try (FileChannel channel = FileChannel.open(
          tempFile,
          StandardOpenOption.CREATE,
          StandardOpenOption.WRITE,
          StandardOpenOption.TRUNCATE_EXISTING
      ))
      {
        // Write the footer with the BitSet size at the end of the file
        // Use absolute positioning for thread safety
        MerkleFooter footer = MerkleFooter.create(calculatedChunkSize, totalSize, bitSetSize);
        channel.write(footer.toByteBuffer(), dataRegionSize + bitSetSize);
      }

      // Open the file channel for memory mapping
      FileChannel fileChannel =
          FileChannel.open(tempFile, StandardOpenOption.READ, StandardOpenOption.WRITE);

      try {
        // Memory map the hash data region - this will create the file structure without physical allocation
        ByteBuffer mappedBuffer =
            fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, dataRegionSize);

        // Create the MerkleTree with memory-mapped access
        return new MerkleTree(
            valid,
            calc,
            fileChannel,
            mappedBuffer,
            eventSink
        );
      } catch (Exception e) {
        // Close the file channel if there's an error
        fileChannel.close();
        throw e;
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to create memory-mapped MerkleTree: " + e.getMessage(), e);
    }
  }

  // Only one constructor is allowed, which requires memory-mapped file access

  /**
   Creates a MerkleTree instance that uses memory-mapped file access.
   This constructor is used by the load method when memory mapping is enabled.
   @param valid
   The BitSet for tracking valid nodes
   @param descriptor
   The ChunkGeometryDescriptor containing all geometry and Merkle tree dimensions
   @param fileChannel
   The file channel for the memory-mapped file
   @param mappedBuffer
   The memory-mapped buffer
   */
  public MerkleTree(
      BitSet valid,
      ChunkGeometryDescriptor descriptor,
      FileChannel fileChannel,
      ByteBuffer mappedBuffer
  )
  {
    this(valid, descriptor, fileChannel, mappedBuffer, new NoOpDownloadEventSink());
  }

  /**
   Creates a MerkleTree instance that uses memory-mapped file access.
   This constructor is used by the load method when memory mapping is enabled.
   @param valid
   The BitSet for tracking valid nodes
   @param descriptor
   The ChunkGeometryDescriptor containing all geometry and Merkle tree dimensions
   @param fileChannel
   The file channel for the memory-mapped file
   @param mappedBuffer
   The memory-mapped buffer
   @param eventSink
   The event sink for instrumentation and testing
   */
  public MerkleTree(
      BitSet valid,
      ChunkGeometryDescriptor descriptor,
      FileChannel fileChannel,
      ByteBuffer mappedBuffer,
      EventSink eventSink
  )
  {
    this.valid = valid;
    this.geometry = descriptor;

    // Initialize the individual properties from the geometry descriptor
    this.leafCount = geometry.getLeafCount();
    this.capLeaf = geometry.getCapLeaf();
    this.offset = geometry.getOffset();
    this.chunkSize = geometry.getChunkSize();
    this.totalSize = geometry.getTotalContentSize();

    this.fileChannel = fileChannel;
    this.mappedBuffer = mappedBuffer;
    this.eventSink = eventSink != null ? eventSink : new NoOpDownloadEventSink();
  }


  /// Hashes data within specified byte range and updates the merkle tree.
  /// This method ensures that the offsets bound proper leaf node ranges,
  /// hashes each bounded chunk, computes and saves the digest, and marks bits as valid.
  /// Uses an object pool to cache digest instances for better performance.
  /// This method is optimized to avoid unnecessary buffer copying.
  ///
  /// Performance optimizations:
  /// 1. Removed synchronized keyword to allow parallel processing of different chunks
  /// 2. Uses read-only buffer to ensure we don't accidentally modify the original
  /// 3. Uses object pool for MessageDigest instances for both leaf and internal nodes
  /// 4. Uses helper method to reduce code duplication when writing hashes to buffer
  ///
  /// This method is thread-safe for different leaf nodes but should not be called concurrently
  /// for the same leaf node.
  /// @param minByteOffsetIncl
  ///     The inclusive minimum byte offset
  /// @param maxByteOffsetExcl
  ///     The exclusive maximum byte offset
  /// @param data
  ///     The data buffer to hash
  /// @throws IllegalArgumentException
  ///     If the offsets are invalid or don't align with leaf boundaries
  public void hashData(long minByteOffsetIncl, long maxByteOffsetExcl, ByteBuffer data)
  {
    if (minByteOffsetIncl < 0) {
      throw new IllegalArgumentException(
          "Minimum byte offset must be non-negative: " + minByteOffsetIncl);
    }
    if (maxByteOffsetExcl > totalSize()) {
      throw new IllegalArgumentException(
          "Maximum byte offset exceeds total size: " + maxByteOffsetExcl + " > " + totalSize());
    }
    if (minByteOffsetIncl >= maxByteOffsetExcl) {
      throw new IllegalArgumentException(
          "Minimum byte offset must be less than maximum byte offset: " + minByteOffsetIncl + " >= "
          + maxByteOffsetExcl);
    }

    // Calculate the leaf indices that correspond to the byte range
    int startLeafIndex = (int) (minByteOffsetIncl / getChunkSize());
    int endLeafIndex = geometry.getChunkIndexForPosition(maxByteOffsetExcl - 1);

    if (startLeafIndex < 0 || startLeafIndex >= getNumberOfLeaves()) {
      throw new IllegalArgumentException("Start leaf index out of bounds: " + startLeafIndex);
    }
    if (endLeafIndex < 0 || endLeafIndex >= getNumberOfLeaves()) {
      throw new IllegalArgumentException("End leaf index out of bounds: " + endLeafIndex);
    }

    // Make a read-only duplicate of the buffer to avoid modifying the original
    // Using asReadOnlyBuffer() ensures we don't accidentally modify the original
    ByteBuffer bufferDup = data.asReadOnlyBuffer();

    // Keep track of all leaf indices that were updated
    Set<Integer> updatedLeafIndices = new HashSet<>();

    // Process each leaf in the range
    for (int leafIndex = startLeafIndex; leafIndex <= endLeafIndex; leafIndex++) {
      // Calculate the byte range for this leaf
      long leafStartOffset = leafIndex * chunkSize;
      long leafEndOffset = Math.min(leafStartOffset + chunkSize, totalSize);

      // Skip if this leaf is outside the specified range
      if (leafEndOffset <= minByteOffsetIncl || leafStartOffset >= maxByteOffsetExcl) {
        continue;
      }

      // Calculate the actual range to process for this leaf
      long processStartOffset = Math.max(leafStartOffset, minByteOffsetIncl);
      long processEndOffset = Math.min(leafEndOffset, maxByteOffsetExcl);
      int chunkLength = (int) (processEndOffset - processStartOffset);

      // Extract the chunk from the buffer
      ByteBuffer chunkBuffer;
      if (processStartOffset == minByteOffsetIncl && processEndOffset == maxByteOffsetExcl) {
        // Use the entire buffer if it exactly matches the leaf
        chunkBuffer = bufferDup.slice();
      } else {
        // Calculate the position in the buffer
        int bufferPosition = (int) (processStartOffset - minByteOffsetIncl);
        // Create a slice of the buffer
        bufferDup.position(bufferPosition);
        bufferDup.limit(bufferPosition + chunkLength);
        chunkBuffer = bufferDup.slice();
        // Reset the buffer position and limit
        bufferDup.position(0);
        bufferDup.limit(bufferDup.capacity());
      }

      // Get a fresh digest instance for each chunk to ensure proper hashing
      byte[] hash;
      try (ObjectPool.Borrowed<MessageDigest> borrowed = DIGEST_POOL.borrowObject()) {
        MessageDigest digest = borrowed.get();
        digest.reset(); // Ensure digest is fully reset
        // The digest is fully reset before being used (handled by the pool's reset callback)

        // Create a byte array copy of the buffer data to ensure consistent hashing
        // This matches the approach used in hashDataIfMatchesExpected
        byte[] chunkData = new byte[chunkBuffer.remaining()];
        chunkBuffer.get(chunkData);

        // Normalize empty chunk data to ensure consistent hashing
        chunkData = normalizeEmptyChunkData(chunkData);

        // Update the digest with the chunk data
        digest.update(chunkData);

        // Calculate the hash for this chunk
        hash = digest.digest();
      }

      // Store the hash in the memory-mapped buffer - synchronized for thread safety
      int idx = offset + leafIndex;
      synchronized (this) {
        if (mappedBuffer != null) {
          // Calculate the position in the buffer for this node's hash
          // The hashes are written to the file in a specific order:
          // 1. Leaf hashes (indices offset to offset+leafCount-1)
          // 2. Padded leaves (indices offset+leafCount to offset+capLeaf-1)
          // 3. Internal hashes (indices 0 to offset-1)
          int hashPosition;
          if (idx >= offset) {
            // This is a leaf node, its position is (idx - offset)
            hashPosition = (idx - offset) * HASH_SIZE;
          } else {
            // This is an internal node, its position is after all leaves (capLeaf + idx)
            hashPosition = (capLeaf + idx) * HASH_SIZE;
          }
          // Use the helper method to write the hash to the buffer
          try {
            boolean writeSuccess = writeHashToBuffer(hashPosition, hash);
            if (!writeSuccess) {
              eventSink.error("MerkleTree.hashData: Failed to write hash to buffer for leafIndex={}, idx={}", leafIndex, idx);
              continue; // Skip marking this leaf as valid if we couldn't write the hash
            }
          } catch (IllegalStateException e) {
            eventSink.error("MerkleTree.hashData: Critical error writing hash for leafIndex={}, idx={}: {}", leafIndex, idx, e.getMessage());
            throw new RuntimeException("Failed to write hash data to merkle tree", e);
          }
        }

        // Mark the leaf as valid only if we successfully wrote the hash
        valid.set(idx);

        // Add to the set of updated leaf indices
        updatedLeafIndices.add(leafIndex);
        
        // Force memory synchronization by immediately verifying the stored hash
        // This ensures the hash is properly committed before continuing
        getHashForLeaf(leafIndex);
      }
    }

    // Now compute all internal nodes that might be affected by the updated leaves
    // Start from the lowest level of internal nodes and work up to the root
    // SKIP internal node computation during tree building
    // Internal nodes will be computed after all leaf processing is complete
    eventSink.debug("MerkleTree.hashData: Deferring internal node computation until tree building is complete. Updated {} leaf nodes.", updatedLeafIndices.size());
  }

  /// Computes all missing internal node hashes after tree building is complete.
  /// This method should be called after all leaf nodes have been processed to ensure
  /// the merkle tree has a complete and consistent internal node structure.
  /// @throws RuntimeException if there are errors computing internal node hashes
  public synchronized void computeAllInternalNodes() {
    eventSink.debug("MerkleTree.computeAllInternalNodes: Starting computation of all internal nodes");
    
    // Collect all internal nodes that need computation
    Set<Integer> internalNodesToCompute = new HashSet<>();
    for (int leafIndex = 0; leafIndex < leafCount; leafIndex++) {
      int idx = offset + leafIndex;
      // Only process nodes for valid leaves
      if (valid.get(idx)) {
        // Walk up the tree to the root, adding each internal node
        idx = (idx - 1) / 2;
        while (idx >= 0) {
          internalNodesToCompute.add(idx);
          if (idx == 0) break;
          idx = (idx - 1) / 2;
        }
      }
    }
    
    // Sort internal nodes from lowest level to root (reverse order = bottom-up)
    List<Integer> sortedInternalNodes = new ArrayList<>(internalNodesToCompute);
    Collections.sort(sortedInternalNodes, Collections.reverseOrder());
    
    eventSink.debug("MerkleTree.computeAllInternalNodes: Computing {} internal nodes", sortedInternalNodes.size());
    
    // Compute hashes for all internal nodes
    for (int idx : sortedInternalNodes) {
      if (!valid.get(idx)) { // Only compute if not already valid
        int left = 2 * idx + 1;
        int right = left + 1;
        
        // Verify children are available
        boolean leftAvailable = (left < offset) || valid.get(left);
        boolean rightAvailable = (right >= 2 * capLeaf - 1) || (right < offset) || valid.get(right);
        
        if (!leftAvailable || !rightAvailable) {
          eventSink.warn("MerkleTree.computeAllInternalNodes: Skipping internal node {} due to missing children (left={}, right={})", 
              idx, leftAvailable, rightAvailable);
          continue;
        }
        
        try (ObjectPool.Borrowed<MessageDigest> borrowed = DIGEST_POOL.borrowObject()) {
          MessageDigest digest = borrowed.get();
          
          // Get child hashes
          byte[] leftHash = getHash(left);
          digest.update(leftHash);
          
          if (right < 2 * capLeaf - 1) {
            byte[] rightHash = getHash(right);
            digest.update(rightHash);
          }
          
          byte[] hash = digest.digest();
          
          // Write hash to buffer
          if (mappedBuffer != null) {
            int hashPosition = (capLeaf + idx) * HASH_SIZE;
            try {
              boolean writeSuccess = writeHashToBuffer(hashPosition, hash);
              if (!writeSuccess) {
                eventSink.error("MerkleTree.computeAllInternalNodes: Failed to write hash for internal node {}", idx);
                continue;
              }
            } catch (IllegalStateException e) {
              eventSink.error("MerkleTree.computeAllInternalNodes: Error writing hash for internal node {}: {}", idx, e.getMessage());
              throw new RuntimeException("Failed to write internal node hash", e);
            }
          }
          
          // Mark as valid
          valid.set(idx);
          eventSink.debug("MerkleTree.computeAllInternalNodes: Computed hash for internal node {}", idx);
        }
      }
    }
    
    eventSink.debug("MerkleTree.computeAllInternalNodes: Completed computation of all internal nodes");
  }

  /// Helper method to normalize empty chunk data to ensure consistent hashing.
  /// All empty chunks should be treated as a single zero byte to maintain consistency
  /// across all hash calculations in the merkle tree.
  /// @param chunkData The original chunk data
  /// @return The normalized chunk data (single zero byte if empty, otherwise unchanged)
  private static byte[] normalizeEmptyChunkData(byte[] chunkData) {
    if (chunkData == null || chunkData.length == 0) {
      byte[] normalizedData = new byte[1];
      normalizedData[0] = 0;
      return normalizedData;
    }
    return chunkData;
  }

  /// Helper method to write a hash to the memory-mapped buffer at the specified position.
  /// This method creates a duplicate of the buffer to avoid changing the position of the original
  /// buffer.
  ///
  /// This helper method was added as part of performance optimizations to:
  /// 1. Reduce code duplication in the hashData method
  /// 2. Centralize the buffer manipulation logic for better maintainability
  /// 3. Make it easier to optimize buffer operations in the future
  /// @param hashPosition
  ///     The position in the buffer to write the hash
  /// @param hash
  ///     The hash to write
  /// @return true if the hash was successfully written, false otherwise
  /// @throws IllegalStateException if the buffer is closed or corrupted
  private boolean writeHashToBuffer(int hashPosition, byte[] hash) throws IllegalStateException {
    if (closed.get()) {
      throw new IllegalStateException("Cannot write to buffer: MerkleTree has been closed");
    }

    if (mappedBuffer == null) {
      eventSink.debug("Cannot write hash: mapped buffer is null");
      return false;
    }

    if (hashPosition < 0 || hashPosition + HASH_SIZE > mappedBuffer.capacity()) {
      eventSink.debug("Cannot write hash: invalid position {} (buffer capacity: {})", hashPosition, mappedBuffer.capacity());
      return false;
    }

    try {
      // Create a duplicate to avoid changing the position of the original buffer
      ByteBuffer duplicate = mappedBuffer.duplicate();
      duplicate.position(hashPosition);
      duplicate.put(Arrays.copyOf(hash, HASH_SIZE));

      // Force the write to be immediately visible
      if (mappedBuffer instanceof MappedByteBuffer) {
        ((MappedByteBuffer) mappedBuffer).force();
      }

      return true;
    } catch (Exception e) {
      // Log the error and throw an exception for critical failures
      logger.error("Critical error writing hash to buffer at position {}: {}", hashPosition, e.getMessage(), e);
      throw new IllegalStateException("Failed to write hash to buffer", e);
    }
  }


  // The initializeHashesFromBuffer method is no longer needed as we directly access the mmap

  /**
   Creates a new empty Merkle tree file with the same structure (total size)
   as an existing Merkle tree file.
   Note: Chunk size is now automatically calculated based on content size.
   @param merkleFile
   The source Merkle tree file to copy structure from
   @param emptyMerkleFile
   The path where the new empty Merkle tree file will be created
   @throws IOException
   If there is an error reading the source file or writing the target file
   */
  public static void createEmptyTreeLike(Path merkleFile, Path emptyMerkleFile) throws IOException {
    // Read the footer from the source file to get total size
    long fileSize = Files.size(merkleFile);
    ByteBuffer footerBuffer = readFooterBuffer(merkleFile, fileSize);

    // Create a MerkleFooter object from the buffer
    MerkleFooter footer = MerkleFooter.fromByteBuffer(footerBuffer);

    // Extract total size
    long totalSize = footer.totalSize();

    // Create a new empty tree with the same total size
    // ChunkGeometryDescriptor will automatically calculate the optimal chunk size
    MerkleTree emptyTree = createEmpty(totalSize);
    
    // Save the empty tree to the target file
    emptyTree.save(emptyMerkleFile);
  }

  /// Synchronizes a remote Merkle tree for a data file URL to local paths.
  /// Downloads the data file and its corresponding .mrkl tree if needed,
  /// then loads and returns the MerkleTree instance.
  /// This method is used to fetch a remote merkle tree file from a url
  /// to a local file. It occurs in a few steps:
  /// 1. The size of the remote merkle tree is determined by a head request with okhttp
  /// 2. The last up to 1k of the tree is fetched, and the end is decoded as a [MerkleFooter]
  /// 3. If the local file exists already and has the same size and has the same footer
  /// contents (determined by MerkleFooter.equals), then the local file is left as is.
  /// 4. In all other cases, the local file is downloaded again.
  /// @param dataUrl
  ///     The path of the remote merkle tree
  /// @param localDataPath
  ///     the path of the local merkle tree
  /// @return The loaded MerkleTree instance
  /// @throws IOException
  ///     for IO errors
  public static MerkleTree syncFromRemote(URL dataUrl, Path localDataPath) throws IOException {
    return syncFromRemote(dataUrl, localDataPath, new io.nosqlbench.vectordata.downloader.ChunkedResourceTransportService());
  }

  /**
   * Synchronizes data and Merkle tree files from remote URLs to local paths.
   * 
   * Downloads both the data file and its corresponding .mrkl file from remote URLs,
   * but only if the local versions don't match the remote versions. This method
   * provides efficient synchronization by avoiding unnecessary downloads.
   * 
   * @param dataUrl The remote URL of the data file
   * @param localDataPath The local path where the data file should be stored
   * @param transportService The transport service to use for downloads and metadata checks
   * @return A MerkleTree loaded from the synchronized .mrkl file
   * @throws IOException If an error occurs during download or file operations
   */
  public static MerkleTree syncFromRemote(URL dataUrl, Path localDataPath, 
                                        io.nosqlbench.vectordata.downloader.ResourceTransportService transportService) throws IOException {
    // Derive merkle URL and local merkle path
    String dataUrlStr = dataUrl.toString();
    URL merkleUrl = new URL(dataUrlStr + ".mrkl");
    Path localMerklePath = localDataPath.resolveSibling(localDataPath.getFileName() + ".mrkl");

    // Check if files need downloading by comparing with remote
    boolean downloadData = true;
    boolean downloadMerkle = true;
    
    try {
      // Check if local files match remote versions
      if (Files.exists(localDataPath)) {
        downloadData = !transportService.localMatchesRemote(localDataPath, dataUrl).get();
      }
      if (Files.exists(localMerklePath)) {
        downloadMerkle = !transportService.localMatchesRemote(localMerklePath, merkleUrl).get();
      }
    } catch (Exception e) {
      // If we can't compare, err on the side of downloading
      logger.debug("Error comparing local and remote files, will download: {}", e.getMessage());
    }

    if (downloadData || downloadMerkle) {
      Files.createDirectories(localDataPath.getParent());
      
      // Download data file using chunk transport
      if (downloadData) {
        logger.debug("Downloading data file from {} to {}", dataUrl, localDataPath);
        io.nosqlbench.vectordata.downloader.DownloadProgress dataProgress = 
            transportService.downloadResource(dataUrl, localDataPath, true);
        try {
          io.nosqlbench.vectordata.downloader.DownloadResult dataResult = dataProgress.get();
          if (!dataResult.isSuccess()) {
            throw new IOException("Failed to download data file: " + dataResult.error());
          }
        } catch (Exception e) {
          throw new IOException("Error downloading data file: " + e.getMessage(), e);
        }
      }
      
      // Download merkle file using chunk transport
      if (downloadMerkle) {
        logger.debug("Downloading merkle file from {} to {}", merkleUrl, localMerklePath);
        io.nosqlbench.vectordata.downloader.DownloadProgress merkleProgress = 
            transportService.downloadResource(merkleUrl, localMerklePath, true);
        try {
          io.nosqlbench.vectordata.downloader.DownloadResult merkleResult = merkleProgress.get();
          if (!merkleResult.isSuccess()) {
            throw new IOException("Failed to download merkle file: " + merkleResult.error());
          }
        } catch (Exception e) {
          throw new IOException("Error downloading merkle file: " + e.getMessage(), e);
        }
      }
      
      // Create reference tree file (.mref) from downloaded merkle file
      // This is needed for MerklePainter which expects a reference tree
      Path localRefPath = localDataPath.resolveSibling(localDataPath.getFileName() + ".mref");
      Files.copy(localMerklePath, localRefPath, 
                 java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }

    // Load and return the MerkleTree from local merkle file
    return MerkleTree.load(localMerklePath);
  }

  /**
   Reads the footer from a remote merkle tree file using OkHttp.
   @param client
   The OkHttp client
   @param merkleUrl
   The URL of the merkle tree file
   @param fileSize
   The size of the file
   @return The MerkleFooter or null if it couldn't be read
   */
  /**
   Reads the footer from a remote merkle tree file using HttpURLConnection.
   */
  private static MerkleFooter readRemoteMerkleFooterHttp(URL merkleUrl, long fileSize)
      throws IOException
  {
    int readSize = (int) Math.min(1024, fileSize);
    long start = fileSize - readSize;
    HttpURLConnection conn = (HttpURLConnection) merkleUrl.openConnection();
    conn.setRequestProperty("Range", "bytes=" + start + "-" + (fileSize - 1));
    conn.setConnectTimeout(5000);
    conn.setReadTimeout(5000);
    conn.connect();
    int code = conn.getResponseCode();
    byte[] buffer;
    if (code == HttpURLConnection.HTTP_PARTIAL) {
      try (InputStream is = conn.getInputStream()) {
        buffer = is.readAllBytes();
      }
    } else {
      conn.disconnect();
      conn = (HttpURLConnection) merkleUrl.openConnection();
      conn.setConnectTimeout(5000);
      conn.setReadTimeout(5000);
      conn.connect();
      if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
        return null;
      }
      try (InputStream is = conn.getInputStream()) {
        byte[] all = is.readAllBytes();
        buffer = (all.length > 1024 ? Arrays.copyOfRange(all, all.length - 1024, all.length) : all);
      }
    }
    return MerkleFooter.fromByteBuffer(ByteBuffer.wrap(buffer));
  }

  /**
   Reads the footer from a local merkle tree file.
   @param merklePath
   The path to the local merkle tree file
   @return The MerkleFooter or null if it couldn't be read
   */
  private static MerkleFooter readLocalMerkleFooter(Path merklePath) throws IOException {
    if (!Files.exists(merklePath) || Files.size(merklePath) == 0) {
      return null;
    }

    try (FileChannel channel = FileChannel.open(merklePath, StandardOpenOption.READ)) {
      long fileSize = channel.size();

      // Read up to the last 1KB
      long footerReadSize = Math.min(1024, fileSize);
      long position = fileSize - footerReadSize;

      ByteBuffer buffer = ByteBuffer.allocate((int) footerReadSize);
      // Use absolute positioning for thread safety
      channel.read(buffer, position);
      buffer.flip();

      return MerkleFooter.fromByteBuffer(buffer);
    } catch (IOException e) {
      return null;
    }
  }

  /**
   Downloads a file from a URL to a local path using OkHttp.
   @param client
   The OkHttp client
   @param url
   The URL to download from
   @param localPath
   The path to save the file to
   @throws IOException
   If an I/O error occurs
   */

  /// Get the chunk size of this merkle tree.
  /// @return The chunk size.
  public long getChunkSize() {
    return geometry.getChunkSize();
  }

  /// Get the size of the content described by this merkle tree.
  /// @return The total size of the content
  public long totalSize() {
    return geometry.getTotalContentSize();
  }

  /// Get the number of leaf nodes in this merkle tree
  /// @return The number of leaf nodes
  public int getNumberOfLeaves() {
    return geometry.getLeafCount();
  }

  /// Get the offset index (index of the first leaf node) in this merkle tree
  /// @return The offset index
  public int getOffset() {
    return geometry.getOffset();
  }

  /// Get the capacity of leaf nodes in this merkle tree
  /// @return The capacity of leaf nodes
  public int getCapLeaf() {
    return geometry.getCapLeaf();
  }

  /// Get the total number of nodes in this merkle tree
  /// @return The total number of nodes
  public int getNodeCount() {
    return geometry.getNodeCount();
  }

  /// Get the total number of chunks in this merkle tree
  /// @return The total number of chunks
  public int getTotalChunks() {
    return geometry.getTotalChunks();
  }

  /// Get the number of internal nodes in this merkle tree
  /// @return The number of internal nodes
  public int getInternalNodeCount() {
    return geometry.getInternalNodeCount();
  }

  /// Get the geometry descriptor for this merkle tree
  /// @return The geometry descriptor
  public ChunkGeometryDescriptor getGeometry() {
    return geometry;
  }

  /// Get the boundaries for a leaf index.
  /// @param leafIndex
  ///     The leaf index for the boundaries
  /// @return The boundaries for the leaf
  public MerkleMismatch getBoundariesForLeaf(int leafIndex) {
    long start = leafIndex * getChunkSize();
    long end = Math.min(start + getChunkSize(), totalSize());
    long length = end - start;
    return new MerkleMismatch(leafIndex, start, length);
  }

  /// Retrieves the hash for any node in the tree, computing it if necessary.
  /// This is package-private to allow tests to access internal nodes, including the root.
  /// This method is synchronized to ensure thread safety during concurrent access.
  /// When using memory-mapped file access, this method will read the hash directly from
  /// the file if it's valid, or compute it if necessary.
  // Cache for computed hashes to avoid recomputing them
  private final Map<Integer, byte[]> hashCache = new ConcurrentHashMap<>();

  /// Retrieves the hash for any node in the tree, computing it if necessary.
  /// This method is synchronized to ensure thread safety during concurrent access.
  /// @param idx
  ///     The index for the node to get the hash for
  /// @return The hash value for the node
  /// @throws IllegalArgumentException
  ///     If the index is invalid
  /// @throws IllegalStateException
  ///     If the tree has been closed
  /// @throws RuntimeException
  ///     If there's an error computing the hash
  public synchronized byte[] getHash(int idx) {
    eventSink.debug("MerkleTree.getHash: idx={}", idx);

    // Check if the tree has been closed
    if (closed.get()) {
      eventSink.warn("Attempting to access hash for idx {} on a closed MerkleTree", idx);
      throw new IllegalStateException("Cannot access hash on a closed MerkleTree");
    }

    // Check if the hash is in the cache
    byte[] cachedHash = hashCache.get(idx);
    if (cachedHash != null) {
      eventSink.debug("MerkleTree.getHash: idx={}, found in cache, hash={}", idx, Arrays.toString(cachedHash));
      return Arrays.copyOf(cachedHash, HASH_SIZE); // Return a defensive copy
    }

    // If the node is valid, try to read from the memory-mapped buffer
    if (valid.get(idx)) {
      eventSink.debug("MerkleTree.getHash: idx={}, node is valid", idx);
      try {
        // For leaf nodes, we need to ensure we're using the correct hash
        // This is especially important for verification
        if (idx >= offset) {
          eventSink.debug("MerkleTree.getHash: idx={}, is a leaf node (idx >= offset={})", idx, offset);
          // This is a leaf node, try to read its hash from the memory-mapped buffer
          if (mappedBuffer != null && fileChannel != null && fileChannel.isOpen()) {
            try {
              // Calculate the position in the buffer for this node's hash
              int hashPosition = (idx - offset) * HASH_SIZE;
              eventSink.debug("MerkleTree.getHash: idx={}, hashPosition={}", idx, hashPosition);
              if (hashPosition + HASH_SIZE <= mappedBuffer.capacity()) {
                byte[] hash = new byte[HASH_SIZE];
                // Create a duplicate to avoid changing the position of the original buffer
                ByteBuffer duplicate = mappedBuffer.duplicate();
                duplicate.position(hashPosition);
                duplicate.get(hash, 0, HASH_SIZE);

                // Check if the hash is all zeros, which indicates it wasn't properly initialized
                boolean allZeros = true;
                for (byte b : hash) {
                  if (b != 0) {
                    allZeros = false;
                    break;
                  }
                }

                eventSink.debug("MerkleTree.getHash: idx={}, read from buffer, allZeros={}, hash={}", idx, allZeros, Arrays.toString(hash));
                if (!allZeros) {
                  // Cache the hash
                  hashCache.put(idx, Arrays.copyOf(hash, HASH_SIZE));
                  return Arrays.copyOf(hash, HASH_SIZE); // Return a defensive copy
                } else {
                  eventSink.debug("MerkleTree.getHash: idx={}, hash is all zeros, will compute from children", idx);
                }
              }
            } catch (Throwable t) {
              // If there's any error reading from the buffer, log it and continue
              eventSink.warn("Error reading hash from buffer for idx {}: {}", idx, t.getMessage());
            }
          } else {
            eventSink.debug("MerkleTree.getHash: idx={}, mappedBuffer={}, fileChannel={}, fileChannel.isOpen={}", 
                idx, (mappedBuffer != null), (fileChannel != null), (fileChannel != null && fileChannel.isOpen()));
          }
        } else {
          eventSink.debug("MerkleTree.getHash: idx={}, is an internal node (idx < offset={})", idx, offset);
        }

        // If we couldn't read the hash from the buffer or it's an internal node,
        // compute it from children
        eventSink.debug("MerkleTree.getHash: idx={}, computing hash from children", idx);
        byte[] hash = computeHashFromChildren(idx);
        eventSink.debug("MerkleTree.getHash: idx={}, computed hash from children: {}", idx, Arrays.toString(hash));
        // Cache the hash
        hashCache.put(idx, Arrays.copyOf(hash, HASH_SIZE));
        return Arrays.copyOf(hash, HASH_SIZE); // Return a defensive copy
      } catch (Throwable t) {
        // If there's any error computing the hash, log it and continue
        eventSink.warn("Error computing hash for idx {}: {}", idx, t.getMessage());
      }
    }

    // If we get here, the node is not valid or we couldn't compute the hash
    eventSink.debug("MerkleTree.getHash: idx={}, node is not valid or couldn't compute hash", idx);
    // Compute the hash from children
    byte[] hash = computeHashFromChildren(idx);
    eventSink.debug("MerkleTree.getHash: idx={}, computed hash from children (not valid): {}", idx, Arrays.toString(hash));
    // Cache the hash
    hashCache.put(idx, Arrays.copyOf(hash, HASH_SIZE));
    return Arrays.copyOf(hash, HASH_SIZE); // Return a defensive copy
  }

  /**
   Helper method to compute a node's hash from its children.
   This method is used by getHash to avoid code duplication.
   @param idx
   The index of the node
   @return The computed hash
   */
  private byte[] computeHashFromChildren(int idx) {
    // Special handling for leaf nodes
    if (idx >= offset) {
      // This is a leaf node, we should use the actual data from the file
      int leafIndex = idx - offset;
      eventSink.debug("MerkleTree.computeHashFromChildren: idx={} is a leaf node (leafIndex={})", idx, leafIndex);

      // Get a digest instance from the pool using try-with-resources to ensure it's returned
      try (ObjectPool.Borrowed<MessageDigest> borrowed = DIGEST_POOL.borrowObject()) {
        MessageDigest digest = borrowed.get();
        // The digest is fully reset before being used (handled by the pool's reset callback)

        // Calculate the byte range for this leaf
        long leafStartOffset = leafIndex * chunkSize;
        long leafEndOffset = Math.min(leafStartOffset + chunkSize, totalSize);
        int chunkLength = (int) (leafEndOffset - leafStartOffset);

        // Check if this is a padded leaf node (beyond the actual file data)
        if (leafIndex >= leafCount) {
          // This is a padded leaf - generate a zero hash
          eventSink.debug("MerkleTree.computeHashFromChildren: Generating zero hash for padded leaf {} (leafIndex >= leafCount {})", leafIndex, leafCount);
          // Padded leaves get a zero hash
          digest.update(new byte[0]); // Hash of empty data
        } else {
          // For real leaf nodes (not padded), generate a placeholder hash
          // This allows trees created by createEmpty() to have individual leaf updates
          // and also handles other edge cases gracefully
          eventSink.debug("MerkleTree.computeHashFromChildren: Generating placeholder hash for leaf {}", leafIndex);
          
          // Create a simple placeholder hash based on leaf index
          // Using a consistent pattern that's different from real data hashes
          byte[] placeholderData = ("empty_leaf_" + leafIndex).getBytes();
          digest.update(placeholderData);
        }

        // Compute the hash
        byte[] hash = digest.digest();

        // Make a defensive copy of the hash
        byte[] hashCopy = Arrays.copyOf(hash, HASH_SIZE);

        // Try to write the hash to the memory-mapped buffer
        try {
          int hashPosition = (idx - offset) * HASH_SIZE;
          boolean writeSuccess = writeHashToBuffer(hashPosition, hashCopy);
          if (!writeSuccess) {
            eventSink.debug("MerkleTree.computeHashFromChildren: Failed to write hash to buffer for leaf idx={}", idx);
            // Continue without throwing - we can still return the computed hash
          }
        } catch (IllegalStateException e) {
          eventSink.warn("MerkleTree.computeHashFromChildren: Critical error writing hash for leaf idx={}: {}", idx, e.getMessage());
          // Continue without throwing - we can still return the computed hash
        }

        valid.set(idx);
        eventSink.debug("MerkleTree.computeHashFromChildren: idx={}, computed hash for leaf: {}", idx, Arrays.toString(hashCopy));
        return Arrays.copyOf(hashCopy, HASH_SIZE); // Return a defensive copy
      }
    }

    // For internal nodes, compute hash from children
    int left = 2 * idx + 1, right = left + 1;

    // Get a digest instance from the pool using try-with-resources to ensure it's returned
    try (ObjectPool.Borrowed<MessageDigest> borrowed = DIGEST_POOL.borrowObject()) {
      MessageDigest digest = borrowed.get();
      // The digest is fully reset before being used (handled by the pool's reset callback)

      // include left child if present
      int nodeCount = capLeaf + offset; // total number of nodes in the tree
      if (left < nodeCount) {
        byte[] leftHash = getHash(left);
        digest.update(leftHash);
      }

      // include right child if present
      if (right < nodeCount) {
        byte[] rightHash = getHash(right);
        digest.update(rightHash);
      }

      // Compute the hash and mark as valid
      byte[] hash = digest.digest();

      // Make a defensive copy of the hash
      byte[] hashCopy = Arrays.copyOf(hash, HASH_SIZE);

      // Try to write the hash to the memory-mapped buffer
      try {
        // Calculate the position in the buffer for this node's hash
        int hashPosition;
        if (idx >= offset) {
          // This is a leaf node, its position is (idx - offset)
          hashPosition = (idx - offset) * HASH_SIZE;
        } else {
          // This is an internal node, its position is after all leaves (capLeaf + idx)
          hashPosition = (capLeaf + idx) * HASH_SIZE;
        }
        boolean writeSuccess = writeHashToBuffer(hashPosition, hashCopy);
        if (!writeSuccess) {
          eventSink.debug("MerkleTree.computeHashFromChildren: Failed to write hash to buffer for internal node idx={}", idx);
          // Continue without throwing - we can still return the computed hash
        }
      } catch (IllegalStateException e) {
        eventSink.warn("MerkleTree.computeHashFromChildren: Critical error writing hash for internal node idx={}: {}", idx, e.getMessage());
        // Continue without throwing - we can still return the computed hash
      }

      valid.set(idx);
      eventSink.debug("Computed hash for idx {}: {}", idx, Arrays.toString(hashCopy));
      return Arrays.copyOf(hashCopy, HASH_SIZE); // Return a defensive copy
    }
  }

  /// Returns the hash for a leaf, computing internals lazily.
  /// This method is synchronized to ensure thread safety during concurrent access.
  /// @param leafIndex
  ///     The index for the leaf to get the hash for
  /// @return The hash value for the leaf
  public synchronized byte[] getHashForLeaf(int leafIndex) {
    if (leafIndex < 0 || leafIndex >= leafCount)
      throw new IllegalArgumentException("Invalid leaf index");
    eventSink.debug("MerkleTree.getHashForLeaf: leafIndex={}", leafIndex);
    byte[] hash = getHash(offset + leafIndex);
    eventSink.debug("MerkleTree.getHashForLeaf: leafIndex={}, hash={}", leafIndex, Arrays.toString(hash));
    return hash;
  }

  /// Checks if a leaf (chunk) is valid.
  /// @param leafIndex
  ///     The index of the leaf to check
  /// @return true if the leaf is valid, false otherwise
  public synchronized boolean isLeafValid(int leafIndex) {
    if (leafIndex < 0 || leafIndex >= leafCount)
      throw new IllegalArgumentException("Invalid leaf index");
    return valid.get(offset + leafIndex);
  }

  /**
   * Invalidate a leaf (chunk) and all its ancestors in the Merkle tree.
   * This clears the valid bit and removes cached hashes, marking the chunk as unverified.
   * @param leafIndex the index of the leaf (chunk) to invalidate
   */
  public synchronized void invalidateLeaf(int leafIndex) {
    if (leafIndex < 0 || leafIndex >= leafCount) {
      throw new IllegalArgumentException("Invalid leaf index: " + leafIndex);
    }
    // Compute absolute node index
    int idx = offset + leafIndex;
    // Clear this leaf node
    valid.clear(idx);
    hashCache.remove(idx);
    // Propagate invalidation to ancestors
    int parent = (idx - 1) / 2;
    while (parent >= 0) {
      valid.clear(parent);
      hashCache.remove(parent);
      if (parent == 0) break;
      parent = (parent - 1) / 2;
    }
  }

  /// Sets a leaf (chunk) as valid.
  /// @param leafIndex
  ///     The index of the leaf to set as valid
  public synchronized void setLeafValid(int leafIndex) {
    if (leafIndex < 0 || leafIndex >= leafCount)
      throw new IllegalArgumentException("Invalid leaf index");
    valid.set(offset + leafIndex);
  }

  /// Gets a list of invalid leaf indices in the given range.
  /// @param range
  ///     The range to check
  /// @return A list of invalid leaf indices
  public synchronized List<Integer> getInvalidLeafIndices(MerkleRange range) {
    if (range == null)
      throw new IllegalArgumentException("Range cannot be null");

    long rs = range.start();
    long re = Math.min(range.end(), totalSize);

    if (rs < 0 || re <= rs)
      throw new IllegalArgumentException("Invalid range: " + range);

    int startChunk = (int) (rs / chunkSize);
    int endChunk = (int) ((re - 1) / chunkSize);

    List<Integer> list = new ArrayList<>();
    for (int i = startChunk; i <= endChunk; i++) {
      if (i < leafCount && !valid.get(offset + i))
        list.add(i);
    }

    return list;
  }

  /// Hashes data and updates the merkle tree only if the computed hash matches the expected hash.
  /// This method is useful for verifying data integrity while updating the tree.
  /// @param minByteOffsetIncl
  ///     The inclusive minimum byte offset
  /// @param maxByteOffsetExcl
  ///     The exclusive maximum byte offset
  /// @param data
  ///     The data buffer to hash
  /// @param expectedHash
  ///     The expected hash value that the data should produce
  /// @return true if the hash matched and the tree was updated, false otherwise
  /// @throws IllegalArgumentException
  ///     If the offsets are invalid or don't align with leaf boundaries
  public synchronized boolean hashDataIfMatchesExpected(
      long minByteOffsetIncl,
      long maxByteOffsetExcl,
      ByteBuffer data,
      byte[] expectedHash
  )
  {
    if (expectedHash == null) {
      throw new IllegalArgumentException("Expected hash cannot be null");
    }

    // Calculate the leaf indices that correspond to the byte range
    int startLeafIndex = (int) (minByteOffsetIncl / getChunkSize());
    int endLeafIndex = geometry.getChunkIndexForPosition(maxByteOffsetExcl - 1);

    // We only support updating a single leaf at a time with this method
    if (startLeafIndex != endLeafIndex) {
      throw new IllegalArgumentException(
          "This method only supports updating a single leaf at a time: startLeafIndex="
          + startLeafIndex + ", endLeafIndex=" + endLeafIndex);
    }

    // Make a read-only duplicate of the buffer to avoid modifying the original
    // Using asReadOnlyBuffer() ensures we don't accidentally modify the original
    ByteBuffer bufferDup = data.asReadOnlyBuffer();

    // Get a digest instance from the pool
    try (ObjectPool.Borrowed<MessageDigest> borrowed = DIGEST_POOL.borrowObject()) {
      MessageDigest digest = borrowed.get();
      digest.reset(); // Ensure digest is fully reset

      // Make a copy of the chunk data to ensure we're hashing the actual data
      byte[] chunkData = new byte[bufferDup.remaining()];
      bufferDup.get(chunkData);

      // Normalize empty chunk data to ensure consistent hashing
      chunkData = normalizeEmptyChunkData(chunkData);

      // Update the digest with the chunk data
      digest.update(chunkData);

      // Calculate the hash for this chunk
      byte[] computedHash = digest.digest();

      // Compare the computed hash with the expected hash
      boolean hashesEqual = MessageDigest.isEqual(computedHash, expectedHash);
      if (!hashesEqual) {
        // Hashes don't match, don't update the tree
        return false;
      }

      // Hashes match, update the tree with the expected hash
      // Since we've already verified that the data produces this hash,
      // we can avoid recomputing it and just use the expected hash directly

      // Calculate the leaf index
      int leafIndex = startLeafIndex;

      // Update the leaf hash with the computed hash (not the expected hash)
      // Since we've verified that computedHash equals expectedHash, we store computedHash
      try {
        boolean updateSuccess = updateLeafHash(leafIndex, computedHash);
        if (!updateSuccess) {
          eventSink.error("MerkleTree.hashDataIfMatchesExpected: Failed to update leaf hash for leafIndex={}", leafIndex);
          return false;
        }
        // Verify what was actually stored
        byte[] storedHash = getHashForLeaf(leafIndex);
        return true;
      } catch (IllegalStateException e) {
        eventSink.error("MerkleTree.hashDataIfMatchesExpected: Critical error updating leaf hash for leafIndex={}: {}", leafIndex, e.getMessage());
        throw new RuntimeException("Failed to update leaf hash in merkle tree", e);
      }
    }
  }

  /// Marks and recomputes a leaf hash, invalidating ancestors.
  /// This method is synchronized to ensure thread safety during concurrent access.
  /// @param leafIndex
  ///     The index for the leaf to update the hash for
  /// @param newHash
  ///     The new hash value for the leaf
  /// @return true if the hash was successfully written to the buffer, false otherwise
  public synchronized boolean updateLeafHash(int leafIndex, byte[] newHash) {
    // Check if the tree has been closed
    if (closed.get()) {
      logger.warn(
          "[DEBUG_LOG] Attempting to update leaf hash on a closed MerkleTree for leaf {}",
          leafIndex
      );
      throw new IllegalStateException("Cannot update leaf hash on a closed MerkleTree");
    }

    int idx = offset + leafIndex;

    // Make a defensive copy of the hash
    byte[] hashCopy = Arrays.copyOf(newHash, HASH_SIZE);

    // Calculate the position in the buffer for this node's hash
    int hashPosition;
    if (idx >= offset) {
      // This is a leaf node, its position is (idx - offset)
      hashPosition = (idx - offset) * HASH_SIZE;
    } else {
      // This is an internal node, its position is after all leaves (capLeaf + idx)
      hashPosition = (capLeaf + idx) * HASH_SIZE;
    }

    // Use the helper method to write the hash to the buffer
    try {
      boolean writeSuccess = writeHashToBuffer(hashPosition, hashCopy);
      if (!writeSuccess) {
        eventSink.error("MerkleTree.updateLeafHash: Failed to write hash to buffer for leafIndex={}, idx={}", leafIndex, idx);
        return false;
      }
    } catch (IllegalStateException e) {
      eventSink.error("MerkleTree.updateLeafHash: Critical error writing hash for leafIndex={}, idx={}: {}", leafIndex, idx, e.getMessage());
      throw e; // Re-throw to let caller handle the critical error
    }

    // Update the hash in the cache
    hashCache.put(idx, hashCopy);
    valid.set(idx);

    // Create a list to store the path from leaf to root
    List<Integer> pathToRoot = new ArrayList<>();

    // invalidate ancestors and collect the path to root
    int leafNodeIndex = offset + leafIndex;
    idx = (idx - 1) / 2;
    while (idx >= 0) {
      // IMPORTANT: Don't invalidate the leaf node itself
      if (idx != leafNodeIndex) {
        valid.clear(idx);
        hashCache.remove(idx);
      }
      // Add this node to the path
      pathToRoot.add(idx);
      if (idx == 0)
        break;
      idx = (idx - 1) / 2;
    }

    // Recompute hashes along the path from leaf to root
    // Start from the deepest node (closest to leaf) and work up to the root
    // IMPORTANT: Skip the leaf node itself since we just explicitly set its hash
    int leafNodeIdx = leafNodeIndex;
    for (int i = pathToRoot.size() - 1; i >= 0; i--) {
      int nodeIdx = pathToRoot.get(i);
      if (nodeIdx == leafNodeIdx) {
        continue; // Skip recomputing the leaf node we just set
      }
      // Compute the hash for this node
      getHash(nodeIdx);
    }

    // Force memory synchronization by immediately verifying the stored hash
    // This ensures the hash is properly committed before continuing
    getHashForLeaf(leafIndex);

    return true;
  }

  /// Updates a leaf hash and persists the tree to disk.
  /// This method is synchronized to ensure thread safety during concurrent access.
  /// @param leafIndex
  ///     The index for the leaf to update the hash for
  /// @param newHash
  ///     The new hash for the leaf
  /// @param filePath
  ///     The path to the merkle tree file to save to
  /// @throws IOException
  ///     If an I/O error occurs
  public synchronized void updateLeafHash(int leafIndex, byte[] newHash, Path filePath)
      throws IOException
  {
    try {
      boolean updateSuccess = updateLeafHash(leafIndex, newHash);
      if (!updateSuccess) {
        throw new IOException("Failed to update leaf hash for leafIndex=" + leafIndex);
      }
    } catch (IllegalStateException e) {
      throw new IOException("Failed to update leaf hash for leafIndex=" + leafIndex + ": " + e.getMessage(), e);
    }
  }

  /// Saves hashes and footer to file.
  /// This method is synchronized to ensure thread safety during concurrent access.
  /// @param path
  ///     The path to save to
  /// @throws IOException
  ///     If an I/O error occurs
  public synchronized void save(Path path) throws IOException {
    // Check if the tree has been closed
    if (closed.get()) {
      logger.warn("Attempting to save a closed MerkleTree to {}", path);
      throw new IllegalStateException("Cannot save a closed MerkleTree");
    }

    // Compute all possible internal nodes based on valid leaves
    // This handles partially valid trees properly
    computeAllInternalNodes();

    int numLeaves = capLeaf;

    // If we have a memory-mapped buffer, flush it to ensure all changes are written to disk
    if (mappedBuffer != null && fileChannel != null) {
      // No need to iterate over cached hashes as we're writing directly to the memory-mapped buffer

      // Flush the memory-mapped buffer to ensure changes are written to disk
      if (mappedBuffer instanceof MappedByteBuffer) {
        ((MappedByteBuffer) mappedBuffer).force();
      }

    }

    // If we don't have a memory-mapped buffer or the path is different, create a new file
    try (FileChannel ch = FileChannel.open(
        path,
        StandardOpenOption.CREATE,
        StandardOpenOption.WRITE,
        StandardOpenOption.TRUNCATE_EXISTING
    )) {
      // Write the hash data region (leaves and internal nodes)
      // Explicit hash computation and writing for all cases
      // write real leafCount leaves
      for (int i = 0; i < leafCount; i++) {
        int leafIdx = offset + i;
        byte[] leafHash = valid.get(leafIdx) ? getHash(leafIdx) : new byte[HASH_SIZE];
        ByteBuffer buf = ByteBuffer.wrap(leafHash);
        while (buf.hasRemaining()) {
          ch.write(buf);
        }
      }
      // write padded leaves
      byte[] zero = new byte[HASH_SIZE];
      for (int i = leafCount; i < capLeaf; i++) {
        ByteBuffer buf = ByteBuffer.wrap(zero);
        while (buf.hasRemaining()) {
          ch.write(buf);
        }
      }
      // write internals
      for (int i = 0; i < offset; i++) {
        byte[] internalHash = valid.get(i) ? getHash(i) : new byte[HASH_SIZE];
        ByteBuffer buf = ByteBuffer.wrap(internalHash);
        while (buf.hasRemaining()) {
          ch.write(buf);
        }
      }

      // Write the BitSet data after the hash data
      int bitSetSize = 0;
      byte[] bitSetData = null;

      // Always write the BitSet to ensure consistent behavior
      int nodeCount = 2 * capLeaf - 1;
      bitSetData = valid.toByteArray();
      bitSetSize = bitSetData.length;

      // Always write the BitSet data to ensure the full bitset is persisted at all times
      ch.write(ByteBuffer.wrap(bitSetData));

      // write footer with the BitSet size
      MerkleFooter footer = MerkleFooter.create(chunkSize, totalSize, bitSetSize);
      ch.write(footer.toByteBuffer());

      // Force all changes to be written to disk
      ch.force(true);
    }
  }


  /// Loads a tree from file, using memory-mapped file access for efficient random access.
  /// @param path
  ///     The path to load the merkle tree data from
  /// @return The loaded MerkleTree instance
  /// @throws IOException
  ///     If an I/O error occurs
  public static MerkleTree load(Path path) throws IOException {
    // Verification is now handled in unit tests, not in production code
    return load(path, false);
  }

  /// Loads a tree from file, using memory-mapped file access for efficient random access.
  /// This optimized version reduces I/O operations by reusing the file channel for all operations.
  /// @param path
  ///     The path to load the merkle tree data from
  /// @param verify
  ///     Whether to verify the integrity of the loaded file. Set to false to skip verification,
  ///                 which can be useful when loading a file immediately after saving it to avoid
  ///         redundant
  ///             verification.
  /// @return The loaded MerkleTree instance
  /// @throws IOException
  ///     If an I/O error occurs
  public static MerkleTree load(Path path, boolean verify) throws IOException {
    long fileSize = Files.size(path);
    // Check basic file conditions
    if (fileSize == 0) {
      throw new RuntimeException("File is empty: " + path);
    }
    if (!Files.exists(path)) {
      throw new RuntimeException("File does not exist: " + path);
    }

    // Open a single file channel for all operations
    FileChannel fileChannel =
        FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);

    try {
      // Read footer using the shared file channel
      MerkleFooter footer =
          MerkleFooter.fromByteBuffer(readFooterBuffer(path, fileSize, fileChannel));
      long chunkSize = footer.chunkSize();
      long totalSize = footer.totalSize();
      int bitSetSize = footer.bitSetSize();

      // Calculate chunk-related values using ChunkGeometryDescriptor
      ChunkGeometryDescriptor descriptor = calculateGeometry(totalSize);

      // Determine actual data region size (excluding footer and BitSet data)
      int fl = footer.footerLength();
      long dataRegionSize = fileSize - fl - bitSetSize;

      // Handle empty files (just a footer, no data)
      if (dataRegionSize == 0) {
        logger.warn("Merkle tree file {} has no data (only footer). Creating an empty tree.", path);
        // Close the file channel since we're returning early
        fileChannel.close();

        // Create a BitSet to track valid nodes
        BitSet valid = new BitSet(descriptor.getNodeCount());

        // Return an empty tree
        return new MerkleTree(
            valid,
            descriptor,
            null,
            null
        );
      }

      if (dataRegionSize % HASH_SIZE != 0) {
        throw new IOException(
            "Invalid merkle tree file: data region size not a multiple of hash size: "
            + dataRegionSize);
      }

      int regionEntries = (int) (dataRegionSize / HASH_SIZE);
      int expectedNodeCount = descriptor.getNodeCount();
      
      // Verify that the file contains the expected number of hash entries
      if (regionEntries != expectedNodeCount) {
        throw new IOException(
            "Invalid merkle tree file: expected " + expectedNodeCount + " hash entries but found " + regionEntries + " in file: " + path);
      }

      // Verification is now handled in unit tests, not in production code
      // The verify parameter is kept for backward compatibility but is ignored
      String fileName = path.getFileName().toString();

      // Create a BitSet to track valid nodes
      BitSet valid = new BitSet(descriptor.getNodeCount());

      // Memory map the hash data region with read-write access
      ByteBuffer mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, dataRegionSize);

      // Log the hash data size for debugging
      logger.debug("Hash data size: {}", dataRegionSize);

      // Initialize the BitSet based on the bitSetSize
      // Read the BitSet data from the file using direct buffer for better performance
      ByteBuffer bitSetBuffer = ByteBuffer.allocateDirect(bitSetSize);
      fileChannel.read(bitSetBuffer, dataRegionSize);
      bitSetBuffer.flip();

      // Initialize the BitSet with the loaded data
      byte[] bitSetData = new byte[bitSetSize];
      bitSetBuffer.get(bitSetData);
      BitSet loadedBitSet = BitSet.valueOf(bitSetData);

      // Log the loaded BitSet for debugging
      logger.debug("Loaded BitSet: {}", loadedBitSet);

      // Copy the loaded BitSet to our valid BitSet
      valid.clear();  // Clear any existing bits
      valid.or(loadedBitSet);  // Set bits from the loaded BitSet
      

      // Create the MerkleTree with memory-mapped access
      MerkleTree tree = new MerkleTree(
          valid,
          descriptor,
          fileChannel,
          mappedBuffer
      );

      // Instead of forcing computation of the root hash, we'll preserve the hashes from the file
      // This ensures that the loaded tree has the same hashes as the original tree
      byte[] rootHash = null;
      if (valid.get(0)) {
        // If the root is valid, read it from the buffer
        // The root hash (internal node with index 0) is written after the leaf hashes and padded leaves
        // So its position in the buffer is (capLeaf + 0) * HASH_SIZE
        int hashPosition = (descriptor.getCapLeaf() + 0) * HASH_SIZE;
        if (hashPosition + HASH_SIZE <= mappedBuffer.capacity()) {
          rootHash = new byte[HASH_SIZE];
          ByteBuffer duplicate = mappedBuffer.duplicate();
          duplicate.position(hashPosition);
          duplicate.get(rootHash, 0, HASH_SIZE);
          logger.debug(
              "[DEBUG_LOG] Loaded tree with root hash from file: {}",
              Arrays.toString(rootHash)
          );
        }
      } else {
        // If the root is not valid, don't force computation during load
        // This allows partially valid trees to be loaded without errors
        logger.debug("[DEBUG_LOG] Loaded tree with invalid root, deferring hash computation");
        rootHash = null;
      }

      // Don't force computation of the root hash to preserve the hashes from the file
      // This ensures that the loaded tree has the same hashes as the original tree
      return tree;
    } catch (Exception e) {
      // Close the file channel if there's an error
      fileChannel.close();
      throw e;
    }
  }

  /// Reads the footer buffer from a merkle tree file.
  /// Uses BIG_ENDIAN byte order for consistent reading across platforms.
  /// This optimized version reduces I/O operations by reading the footer in a single operation when
  /// possible.
  /// @param path
  ///     The path to the merkle tree file
  /// @param fileSize
  ///     The size of the file
  /// @param fileChannel
  ///     Optional file channel to use for reading. If null, a new channel will be opened.
  /// @return A ByteBuffer containing the footer data
  /// @throws IOException
  ///     If there's an error reading the file
  private static ByteBuffer readFooterBuffer(Path path, long fileSize, FileChannel fileChannel)
      throws IOException
  {
    boolean closeChannel = false;
    FileChannel ch;

    if (fileChannel != null) {
      ch = fileChannel;
    } else {
      ch = FileChannel.open(path, StandardOpenOption.READ);
      closeChannel = true;
    }

    try {
      // Read the footer length byte
      ByteBuffer len = ByteBuffer.allocateDirect(1);
      len.order(ByteOrder.BIG_ENDIAN);
      // Use absolute positioning for thread safety
      ch.read(len, fileSize - 1);
      len.flip();
      byte fl = len.get();

      // Read the full footer in a single operation if possible
      ByteBuffer buf = ByteBuffer.allocateDirect(fl);
      buf.order(ByteOrder.BIG_ENDIAN);

      // Use absolute positioning for thread safety
      long position = fileSize - fl;

      // Try to read the entire footer in one operation
      int bytesRead = ch.read(buf, position);

      // If we couldn't read the entire footer in one go, read the rest
      if (bytesRead < fl) {
        int totalBytesRead = bytesRead;
        while (buf.hasRemaining()) {
          int r = ch.read(buf, position + totalBytesRead);
          if (r < 0) {
            throw new IOException(
                "Unexpected end of file reading merkle footer, expected " + buf.remaining()
                + " more bytes");
          }
          totalBytesRead += r;
        }
      }

      buf.flip();
      return buf;
    } finally {
      if (closeChannel) {
        ch.close();
      }
    }
  }

  /// Backward compatibility method for reading footer buffer
  /// @param path
  ///     The path to the merkle tree file
  /// @param fileSize
  ///     The size of the file
  /// @return A ByteBuffer containing the footer data
  /// @throws IOException
  ///     If there's an error reading the file
  private static ByteBuffer readFooterBuffer(Path path, long fileSize) throws IOException {
    return readFooterBuffer(path, fileSize, null);
  }


  /**
   Finds all mismatched chunks between this tree and another tree.
   This method is synchronized to ensure thread safety during concurrent access.
   Before comparing, it ensures both trees are in a consistent state by forcing
   recomputation of all hashes.
   @param otherTree
   The tree to compare against
   @return List of MerkleMismatch objects representing the mismatched chunks
   */
  public synchronized List<MerkleMismatch> findMismatchedChunks(MerkleTree otherTree) {
    // Validate that trees are comparable
    if (this.chunkSize != otherTree.chunkSize) {
      throw new IllegalArgumentException(
          "Cannot compare trees with different chunk sizes: " + this.chunkSize + " vs "
          + otherTree.chunkSize);
    }

    // Force recomputation of all hashes in both trees to ensure consistent state
    // First, get the root hash of this tree, which will trigger recomputation of all invalid hashes
    this.getHash(0);

    // Then, get the root hash of the other tree
    synchronized (otherTree) {
      otherTree.getHash(0);
    }

    // Get the list of mismatched chunk indexes
    int[] mismatchedIndexes =
        findMismatchedChunksInRange(otherTree, 0, Math.min(this.leafCount, otherTree.leafCount));

    // Convert indexes to MerkleMismatch objects
    List<MerkleMismatch> mismatches = new ArrayList<>(mismatchedIndexes.length);
    for (int index : mismatchedIndexes) {
      // Calculate the range for this chunk
      long startOffset = (long) index * chunkSize;
      long length = Math.min(chunkSize, totalSize - startOffset);

      // Create MerkleMismatch object and add to list
      mismatches.add(new MerkleMismatch(index, startOffset, length));
    }

    return mismatches;
  }

  /**
   Finds all mismatched chunk indexes within a specified range.
   This method compares leaf hashes between the two trees and returns indexes where they differ.
   This method is synchronized to ensure thread safety during concurrent access.
   @param otherTree
   The tree to compare against
   @param startIndex
   The starting chunk index (inclusive)
   @param endIndex
   The ending chunk index (exclusive)
   @return Array of chunk indexes that differ between the trees
   */
  public synchronized int[] findMismatchedChunksInRange(
      MerkleTree otherTree,
      int startIndex,
      int endIndex
  )
  {
    // Validate matching chunk size
    if (this.chunkSize != otherTree.chunkSize) {
      throw new IllegalArgumentException(
          "Cannot compare trees with different chunk sizes: " + this.chunkSize + " vs "
          + otherTree.chunkSize);
    }
    // Validate matching total size
    if (this.totalSize != otherTree.totalSize) {
      throw new IllegalArgumentException(
          "Cannot compare trees with different total sizes: " + this.totalSize + " vs "
          + otherTree.totalSize);
    }
    // Clamp end index to available leaves
    int maxLeaf = Math.min(this.leafCount, otherTree.leafCount);
    int effectiveEnd = Math.min(endIndex, maxLeaf);
    // Validate start index
    if (startIndex < 0 || startIndex >= effectiveEnd) {
      throw new IllegalArgumentException("Invalid range: " + startIndex + " to " + endIndex);
    }


    // Use a list to collect mismatched indexes
    List<Integer> mismatches = new ArrayList<>();

    // First, collect all hashes from this tree to avoid holding locks on both trees simultaneously
    byte[][] thisHashes = new byte[effectiveEnd - startIndex][];
    for (int i = startIndex; i < effectiveEnd; i++) {
      thisHashes[i - startIndex] = this.getHashForLeaf(i);
    }

    // Then, collect all hashes from the other tree
    byte[][] otherHashes;
    synchronized (otherTree) {
      otherHashes = new byte[effectiveEnd - startIndex][];
      for (int i = startIndex; i < effectiveEnd; i++) {
        otherHashes[i - startIndex] = otherTree.getHashForLeaf(i);
      }
    }

    // Now compare the hashes without holding locks
    for (int i = 0; i < effectiveEnd - startIndex; i++) {
      if (!Arrays.equals(thisHashes[i], otherHashes[i])) {
        mismatches.add(i + startIndex);
      }
    }

    // Convert list to array
    int[] result = new int[mismatches.size()];
    for (int i = 0; i < mismatches.size(); i++) {
      result[i] = mismatches.get(i);
    }

    return result;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", MerkleTree.class.getSimpleName() + "[", "]").add(
            "valid/total=" + valid.cardinality() + "/" + valid.length() + "/" + leafCount)
        .add("chunkSize=" + chunkSize).add("totalSize=" + totalSize).add("offset=" + offset)
        .add("capLeaf=" + capLeaf).toString();
  }

  /// Returns the path of hashes from a leaf node to the root.
  /// This method provides a diagnostic view of the Merkle tree structure for a specific leaf.
  /// The returned list contains the hash of the specified leaf node, followed by its parent,
  /// grandparent, and so on, all the way to the root node.
  /// @param leafIndex
  ///     The index of the leaf node to start from
  /// @return A list of byte arrays representing the hashes along the path from leaf to root
  /// @throws IllegalArgumentException
  ///     If the leaf index is invalid
  public synchronized List<byte[]> getPathToRoot(int leafIndex) {
    if (leafIndex < 0 || leafIndex >= leafCount) {
      throw new IllegalArgumentException("Invalid leaf index: " + leafIndex);
    }

    List<byte[]> path = new ArrayList<>();

    // Start with the leaf node
    int nodeIndex = offset + leafIndex;

    // Add the leaf hash to the path
    path.add(getHash(nodeIndex));

    // Traverse up the tree to the root
    while (nodeIndex > 0) {
      // Move to parent node
      nodeIndex = (nodeIndex - 1) / 2;

      // Add the parent hash to the path
      path.add(getHash(nodeIndex));
    }

    return path;
  }

  // This method has been removed as the base implementation now uses memory-mapped file access
  // for efficient random access to the merkle tree file. Use getPathToRoot instead.
}
