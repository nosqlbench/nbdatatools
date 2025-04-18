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


import io.nosqlbench.vectordata.downloader.DownloadProgress;
import io.nosqlbench.vectordata.downloader.DownloadResult;
import io.nosqlbench.vectordata.status.NoOpDownloadEventSink;
import io.nosqlbench.vectordata.status.StdoutDownloadEventSink;
import io.nosqlbench.vectordata.downloader.ChunkedDownloader;
import io.nosqlbench.vectordata.status.EventSink;

import java.io.Closeable;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.nosqlbench.vectordata.merkle.MerklePane.MREF;
import static io.nosqlbench.vectordata.merkle.MerklePane.MRKL;

/// MerklePainter is an active wrapper around a MerklePane which knows how
/// to download and submit ("paint") chunks to the MerklePane as needed.
/// It relays minimal from MerklePane in the form of the intact chunks bitset.
/// This provides a simple and complete view of chunk state for callers.
public class MerklePainter implements Closeable {
  private final MerklePane pane;
  private final String sourcePath;
  private final Path localPath;
  private final Path merklePath;
  private final Path referenceTreePath;

  // Thread pool for parallel downloads
  private final ExecutorService downloadExecutor;

  // ChunkedDownloader for efficient downloads
  private final ChunkedDownloader chunkedDownloader;
  private final EventSink eventSink;

  // Map to track in-progress download tasks by chunk index
  private final Map<Integer, CompletableFuture<Boolean>> downloadTasks = new ConcurrentHashMap<>();

  /// Creates a new MerklePainter for the given local file and source URL
  /// @param localPath
  ///     Path where the local data file exists or will be stored
  /// @param sourcePath
  ///     URL of the source data file (merkle tree will be downloaded from
  ///     sourcePath + [MerklePane#MRKL])
  public MerklePainter(Path localPath, String sourcePath) {
    this(localPath, sourcePath, new StdoutDownloadEventSink());
  }

  /// Creates a new MerklePainter for the given local file and source URL with a custom event sink
  /// @param localPath
  ///     Path where the local data file exists or will be stored
  /// @param sourcePath
  ///     URL of the source data file (merkle tree will be downloaded from
  ///     sourcePath + [MerklePane#MRKL])
  /// @param eventSink
  ///     Event sink for logging download progress and events
  public MerklePainter(Path localPath, String sourcePath, EventSink eventSink) {
    this.localPath = localPath;
    this.sourcePath = sourcePath;
    this.merklePath = localPath.resolveSibling(localPath.getFileName().toString() + MRKL);
    this.referenceTreePath = localPath.resolveSibling(localPath.getFileName().toString() + MREF);
    this.eventSink = eventSink;

    // Initialize the pane with local paths, reference tree path, and source URL
    // MerklePane will handle downloading the reference tree if needed
    this.pane = new MerklePane(localPath, merklePath, referenceTreePath, sourcePath);

    // Initialize the download executor with a virtual thread executor
    this.downloadExecutor = Executors.newVirtualThreadPerTaskExecutor();

    // Initialize the chunked downloader if a source path is provided
    if (sourcePath != null) {
      try {
        URL url = new URL(sourcePath);
        String fileName = localPath.getFileName().toString();
        // Use a chunk size of 1MB and parallelism based on available processors
        int parallelism = Math.min(8, Runtime.getRuntime().availableProcessors());
        this.chunkedDownloader = new ChunkedDownloader(url, fileName, 1024 * 1024, parallelism, eventSink);
      } catch (MalformedURLException e) {
        throw new RuntimeException("Invalid source URL: " + sourcePath, e);
      }
    } else {
      // No source path provided, so no chunked downloader
      this.chunkedDownloader = null;
    }
  }

  /// Gets the source URL of the data file
  /// @return The source URL
  public String sourcePath() {
    return sourcePath;
  }

  /// Gets the local path where the data file is stored
  /// @return The local file path
  public Path localPath() {
    return localPath;
  }

  /**
   * Gets the MerklePane used by this painter.
   *
   * @return The MerklePane instance
   */
  public MerklePane pane() {
    return pane;
  }

  /**
   * Gets the total size of the file as reported by the merkle tree.
   *
   * @return The total size in bytes
   */
  public long totalSize() {
    return pane.getTotalSize();
  }

  /**
   * Downloads a range of data from the remote source using ChunkedDownloader.
   * If no source path is available, returns an empty buffer.
   *
   * @param start The starting position
   * @param length The number of bytes to download
   * @return A ByteBuffer containing the downloaded data, or an empty buffer if no source path is available
   * @throws IOException If there's an error downloading the data
   */
  public ByteBuffer downloadRange(long start, long length) throws IOException {
    // If no source path is available, return an empty buffer
    if (sourcePath == null) {
      return ByteBuffer.allocate(0);
    }

    eventSink.debug("Requesting range: bytes={}-{}", start, start + length - 1);

    // Create a temporary file to download the range
    Path tempFile = Files.createTempFile("merkle-range-", ".tmp");
    try {
      // Create a URL with range parameters
      URL rangeUrl;
      try {
        rangeUrl = new URL(sourcePath);
      } catch (MalformedURLException e) {
        throw new IOException("Invalid source URL: " + sourcePath, e);
      }

      // Use the chunked downloader to download the range
      // We'll create a new downloader for each range to avoid conflicts
      ChunkedDownloader rangeDownloader = new ChunkedDownloader(
          rangeUrl,
          tempFile.getFileName().toString(),
          length, // Use the range length as the chunk size
          1, // Single chunk download
          eventSink,
          start, // Starting position for the range
          length // Length of the range
      );

      // Download the range to the temporary file
      DownloadProgress progress = rangeDownloader.download(tempFile, true);

      try {
        // Wait for the download to complete
        DownloadResult result = progress.get(30, TimeUnit.SECONDS);

        if (!result.isSuccess()) {
          throw new IOException("Failed to download range: " +
              (result.error() != null ? result.error().getMessage() : "unknown error"));
        }

        // Read the downloaded data into a ByteBuffer
        ByteBuffer buffer = ByteBuffer.allocate((int) length);
        byte[] data = Files.readAllBytes(tempFile);
        buffer.put(data, 0, Math.min(data.length, (int) length));
        buffer.flip();

        eventSink.debug("Successfully downloaded range: bytes={}-{}, size={}",
                      start, start + length - 1, buffer.remaining());

        return buffer;
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        throw new IOException("Failed to download range", e);
      }
    } finally {
      // Clean up the temporary file
      try {
        Files.deleteIfExists(tempFile);
      } catch (IOException e) {
        // Ignore cleanup errors
      }
    }
  }

  /**
   * Downloads a specific chunk from the remote source and submits it to the MerklePane.
   * This automatically updates the merkle tree for the chunk.
   *
   * @param chunkIndex The index of the chunk to download
   * @return true if the chunk was successfully downloaded and processed, false otherwise
   * @throws IOException If there's an error downloading or processing the chunk
   */
  public boolean downloadAndSubmitChunk(int chunkIndex) throws IOException {
    // Get the chunk boundaries
    MerkleTree merkleTree = pane.getMerkleTree();
    MerkleTree.NodeBoundary bounds = merkleTree.getBoundariesForLeaf(chunkIndex);
    long start = bounds.start();
    long end = bounds.end();
    int length = (int)(end - start);

    // Download the chunk data
    ByteBuffer chunkData = downloadRange(start, length);

    // Submit the chunk to the MerklePane
    pane.submitChunk(chunkIndex, chunkData);

    return true;
  }

  /**
   * Ensures that all chunks in the specified range are downloaded and verified.
   * This method blocks until all chunks are available.
   *
   * @param startPosition The start position in the file
   * @param endPosition The end position in the file (exclusive)
   * @throws IOException If there's an error downloading or verifying the chunks
   */
  public void paint(long startPosition, long endPosition) throws IOException {
    try {
      // Wait for the asynchronous paint operation to complete
      paintAsync(startPosition, endPosition).join();
    } catch (Exception e) {
      // Unwrap the exception if it's an IOException
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      } else {
        throw new IOException("Failed to paint range", e);
      }
    }
  }

  /**
   * Finds optimal Merkle nodes for downloading based on the current state of the tree.
   * This method selects nodes at various levels of the tree to optimize transfer size.
   *
   * @param startChunk The starting chunk index
   * @param endChunk The ending chunk index (inclusive)
   * @param maxTransferSize The maximum number of chunks to include in a single transfer
   * @return A list of NodeTransfer objects representing optimal transfers
   */
  private List<NodeTransfer> findOptimalTransfers(int startChunk, int endChunk, int maxTransferSize) {
    List<NodeTransfer> transfers = new ArrayList<>();
    MerkleTree merkleTree = pane.getMerkleTree();
    MerkleNode root = merkleTree.root();
    long chunkSize = merkleTree.chunkSize();
    long totalSize = merkleTree.totalSize();

    // Start with the range of chunks we need
    findOptimalTransfersRecursive(root, startChunk, endChunk, maxTransferSize, transfers, chunkSize, totalSize);

    return transfers;
  }

  /**
   * Recursively finds optimal Merkle nodes for downloading.
   *
   * @param node The current node to examine
   * @param startChunk The starting chunk index
   * @param endChunk The ending chunk index (inclusive)
   * @param maxTransferSize The maximum number of chunks to include in a single transfer
   * @param transfers The list to add transfers to
   * @param chunkSize The size of each chunk
   * @param totalSize The total size of the file
   */
  private void findOptimalTransfersRecursive(MerkleNode node, int startChunk, int endChunk,
                                            int maxTransferSize, List<NodeTransfer> transfers,
                                            long chunkSize, long totalSize) {
    // If this is a leaf node, add it directly if it's in our range
    if (node.isLeaf()) {
      int leafIndex = node.index();
      if (leafIndex >= startChunk && leafIndex <= endChunk && !pane.isChunkIntact(leafIndex)) {
        // This is a leaf node in our range that needs downloading
        long start = node.startOffset(totalSize, chunkSize);
        long end = node.endOffset(totalSize, chunkSize);
        transfers.add(new NodeTransfer(node, start, end));
      }
      return;
    }

    // For internal nodes, we need to determine if this node covers chunks in our range
    // and if all those chunks need downloading

    // Find the range of chunks this node covers
    long nodeStart = node.startOffset(totalSize, chunkSize);
    long nodeEnd = node.endOffset(totalSize, chunkSize);
    int nodeStartChunk = (int)(nodeStart / chunkSize);
    int nodeEndChunk = (int)Math.min(nodeEnd / chunkSize, totalSize / chunkSize);

    // Check if this node is completely outside our range
    if (nodeEndChunk < startChunk || nodeStartChunk > endChunk) {
      return;
    }

    // Calculate the intersection of this node's range with our target range
    int intersectStart = Math.max(nodeStartChunk, startChunk);
    int intersectEnd = Math.min(nodeEndChunk, endChunk);
    int chunkCount = intersectEnd - intersectStart + 1;

    // Check if all chunks in this node's range need downloading
    boolean allNeedDownload = true;
    for (int i = intersectStart; i <= intersectEnd; i++) {
      if (pane.isChunkIntact(i)) {
        allNeedDownload = false;
        break;
      }
    }

    // If all chunks need downloading and the size is appropriate, download the entire node
    if (allNeedDownload && chunkCount <= maxTransferSize) {
        // Calculate the actual byte range for this node
      // Ensure the range aligns with chunk boundaries
      long start = Math.max(nodeStart, intersectStart * chunkSize);
      long end = Math.min(nodeEnd, (intersectEnd + 1) * chunkSize);

      // Verify that the range is at least one chunk in size
      if (end - start >= chunkSize) {
        transfers.add(new NodeTransfer(node, start, end));
      } else {
        eventSink.warn("Skipping transfer smaller than one chunk: {} bytes", end - start);
      }
    } else if (node.left() != null) {
      // Otherwise, recurse into children
      findOptimalTransfersRecursive(node.left(), startChunk, endChunk, maxTransferSize, transfers, chunkSize, totalSize);
      if (node.right() != null) {
        findOptimalTransfersRecursive(node.right(), startChunk, endChunk, maxTransferSize, transfers, chunkSize, totalSize);
      }
    }
  }

  /**
   * Record class to represent a transfer of a Merkle node.
   */
  private record NodeTransfer(MerkleNode node, long start, long end) {}

  /**
   * Asynchronously ensures that all chunks in the specified range are downloaded and verified.
   * This method returns immediately with a CompletableFuture that completes when all chunks are available.
   * It uses the Merkle tree structure to optimize transfers.
   *
   * @param startPosition The start position in the file
   * @param endPosition The end position in the file (exclusive)
   * @return A CompletableFuture that completes when all chunks are available
   */
  public CompletableFuture<Void> paintAsync(long startPosition, long endPosition) {
    try {
      // Get the merkle tree
      MerkleTree merkleTree = pane.getMerkleTree();

      // Calculate the chunk indices for the range
      int startChunk = merkleTree.getChunkIndexForPosition(startPosition);
      int endChunk = merkleTree.getChunkIndexForPosition(Math.min(endPosition - 1, merkleTree.totalSize() - 1));

      // Determine optimal transfer size based on active transfers
      int activeTransfers = downloadTasks.size();
      int optimalTransferSize = activeTransfers > 0 ? (1 << Math.max(0, 16 - activeTransfers)) : 16;

      // Find optimal transfers for this range
      List<NodeTransfer> transfers = findOptimalTransfers(startChunk, endChunk, optimalTransferSize);

      if (transfers.isEmpty()) {
        // If no transfers needed, return completed future
        return CompletableFuture.completedFuture(null);
      }

      // Create a list of futures for each transfer
      List<CompletableFuture<Void>> transferFutures = new ArrayList<>();

      for (NodeTransfer transfer : transfers) {
        CompletableFuture<Void> future = downloadNodeTransferAsync(transfer);
        transferFutures.add(future);
      }

      // Return a future that completes when all transfers are done
      return CompletableFuture.allOf(transferFutures.toArray(new CompletableFuture[0]));
    } catch (Exception e) {
      // If there's an exception, return a failed future
      CompletableFuture<Void> future = new CompletableFuture<>();
      future.completeExceptionally(e);
      return future;
    }
  }

  /**
   * Downloads a chunk if it hasn't been downloaded yet or if verification fails.
   *
   * @param chunkIndex The index of the chunk to download
   * @return true if the chunk was successfully downloaded or was already intact, false otherwise
   * @throws IOException If there's an error downloading or verifying the chunk
   */
  public boolean downloadChunkIfNeeded(int chunkIndex) throws IOException {
    // Check if the chunk is already intact
    if (pane.isChunkIntact(chunkIndex)) {
      return true;
    }

    // Get the chunk boundaries
    MerkleTree merkleTree = pane.getMerkleTree();
    MerkleTree.NodeBoundary bounds = merkleTree.getBoundariesForLeaf(chunkIndex);
    long start = bounds.start();
    long end = bounds.end();

    // Check if the chunk has any data (non-zero bytes)
    boolean hasData = false;
    try {
      // Open the file for reading
      try (FileChannel channel = FileChannel.open(localPath, StandardOpenOption.READ)) {
        // Seek to the start of the chunk
        channel.position(start);
        // Read a small portion of the chunk to check if it has data
        ByteBuffer buffer = ByteBuffer.allocate(Math.min(1024, (int)(end - start)));
        int bytesRead = channel.read(buffer);
        if (bytesRead > 0) {
          buffer.flip();
          for (int i = 0; i < buffer.limit(); i++) {
            if (buffer.get(i) != 0) {
              hasData = true;
              break;
            }
          }
        }
      }
    } catch (IOException e) {
      // Ignore exceptions when checking for data
    }

    // If the chunk has data, verify it
    if (hasData) {
      if (pane.verifyChunk(chunkIndex)) {
        // Chunk is valid and has been marked as intact by MerklePane
        return true;
      }
    }

    // Chunk needs to be downloaded
    return downloadAndSubmitChunk(chunkIndex);
  }

  /**
   * Downloads multiple chunks in parallel and submits them to the MerklePane.
   * This automatically updates the merkle tree for each chunk.
   *
   * @param chunkIndices The indices of the chunks to download
   * @return A list of chunk indices that were successfully downloaded and processed
   * @throws IOException If there's an error downloading or processing the chunks
   */
  public List<Integer> downloadAndSubmitChunks(List<Integer> chunkIndices) throws IOException {
    try {
      return downloadAndSubmitChunksAsync(chunkIndices).join();
    } catch (CompletionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else {
        throw new IOException("Failed to download chunks", e);
      }
    }
  }

  /**
   * Asynchronously downloads multiple chunks in parallel and submits them to the MerklePane.
   * This automatically updates the merkle tree for each chunk.
   *
   * @param chunkIndices The indices of the chunks to download
   * @return A CompletableFuture that completes with a list of chunk indices that were successfully downloaded and processed
   */
  public CompletableFuture<List<Integer>> downloadAndSubmitChunksAsync(List<Integer> chunkIndices) {
    if (chunkIndices.isEmpty()) {
      return CompletableFuture.completedFuture(new ArrayList<>());
    }

    // Filter out chunks that are already intact
    List<Integer> chunksToDownload = new ArrayList<>();
    for (int chunkIndex : chunkIndices) {
      if (!pane.isChunkIntact(chunkIndex)) {
        chunksToDownload.add(chunkIndex);
      }
    }

    if (chunksToDownload.isEmpty()) {
      return CompletableFuture.completedFuture(new ArrayList<>(chunkIndices)); // All chunks are already intact
    }

    // Download chunks in parallel
    List<CompletableFuture<Boolean>> futures = new ArrayList<>();

    for (int chunkIndex : chunksToDownload) {
      // Get or create a future for this chunk
      CompletableFuture<Boolean> future = downloadTasks.computeIfAbsent(chunkIndex, idx -> {
        return CompletableFuture.supplyAsync(() -> {
          try {
            boolean result = downloadAndSubmitChunk(idx);
            // Remove the task from the map when it completes
            downloadTasks.remove(idx);
            return result;
          } catch (IOException e) {
            throw new CompletionException(e);
          }
        }, downloadExecutor);
      });

      futures.add(future);
    }

    // Return a future that completes when all downloads are done
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
      .thenApply(v -> {
        // Collect successful chunk indices
        List<Integer> successfulChunks = new ArrayList<>();
        for (int i = 0; i < chunksToDownload.size(); i++) {
          int chunkIndex = chunksToDownload.get(i);
          try {
            boolean success = futures.get(i).get();
            if (success) {
              successfulChunks.add(chunkIndex);
            }
          } catch (InterruptedException | ExecutionException e) {
            // Skip this chunk if there was an error
          }
        }
        return successfulChunks;
      });
  }

  /// Gets the path to the local merkle tree file
  /// @return The merkle tree file path
  public Path merklePath() {
    return merklePath;
  }

  /// Gets the path to the downloaded reference merkle tree file
  /// @return The reference merkle tree file path
  public Path referenceTreePath() {
    return referenceTreePath;
  }

  /**
   * Downloads multiple chunks if they haven't been downloaded yet or if verification fails.
   *
   * @param chunkIndices The indices of the chunks to download
   * @return A list of chunk indices that were successfully downloaded or were already intact
   * @throws IOException If there's an error downloading or verifying the chunks
   */
  public List<Integer> downloadChunksIfNeeded(List<Integer> chunkIndices) throws IOException {
    try {
      // Wait for the asynchronous operation to complete
      return downloadChunksIfNeededAsync(chunkIndices).join();
    } catch (Exception e) {
      // Unwrap the exception if it's an IOException
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      } else {
        throw new IOException("Failed to download chunks", e);
      }
    }
  }

  /**
   * Asynchronously downloads multiple chunks if they haven't been downloaded yet or if verification fails.
   *
   * @param chunkIndices The indices of the chunks to download
   * @return A CompletableFuture that completes with a list of chunk indices that were successfully downloaded or were already intact
   */
  public CompletableFuture<List<Integer>> downloadChunksIfNeededAsync(List<Integer> chunkIndices) {
    if (chunkIndices.isEmpty()) {
      return CompletableFuture.completedFuture(new ArrayList<>());
    }

    // Create a future for the final result
    CompletableFuture<List<Integer>> resultFuture = new CompletableFuture<>();

    // Process the chunks asynchronously
    CompletableFuture.runAsync(() -> {
      try {
        // Check which chunks need to be downloaded
        List<Integer> chunksToDownload = new ArrayList<>();
        List<Integer> intactChunks = new ArrayList<>();

        for (int chunkIndex : chunkIndices) {
          if (pane.isChunkIntact(chunkIndex)) {
            intactChunks.add(chunkIndex);
          } else {
            // Check if the chunk has data and can be verified
            MerkleTree merkleTree = pane.getMerkleTree();
            MerkleTree.NodeBoundary bounds = merkleTree.getBoundariesForLeaf(chunkIndex);
            long start = bounds.start();
            long end = bounds.end();

            boolean hasData = false;
            try {
              // Open the file for reading
              try (FileChannel channel = FileChannel.open(localPath, StandardOpenOption.READ)) {
                // Seek to the start of the chunk
                channel.position(start);
                // Read a small portion of the chunk to check if it has data
                ByteBuffer buffer = ByteBuffer.allocate(Math.min(1024, (int)(end - start)));
                int bytesRead = channel.read(buffer);
                if (bytesRead > 0) {
                  buffer.flip();
                  for (int i = 0; i < buffer.limit(); i++) {
                    if (buffer.get(i) != 0) {
                      hasData = true;
                      break;
                    }
                  }
                }
              }
            } catch (IOException e) {
              // Ignore exceptions when checking for data
            }

            // If the chunk has data, verify it
            if (hasData && pane.verifyChunk(chunkIndex)) {
              intactChunks.add(chunkIndex);
            } else {
              chunksToDownload.add(chunkIndex);
            }
          }
        }

        // If all chunks are intact, return them
        if (chunksToDownload.isEmpty()) {
          resultFuture.complete(intactChunks);
          return;
        }

        // Download the chunks that need to be downloaded
        downloadAndSubmitChunksAsync(chunksToDownload).thenAccept(downloadedChunks -> {
          // Combine the intact chunks with the downloaded chunks
          List<Integer> result = new ArrayList<>(intactChunks);
          result.addAll(downloadedChunks);
          resultFuture.complete(result);
        }).exceptionally(ex -> {
          resultFuture.completeExceptionally(ex);
          return null;
        });
      } catch (Exception e) {
        resultFuture.completeExceptionally(e);
      }
    }, downloadExecutor).exceptionally(ex -> {
      resultFuture.completeExceptionally(ex);
      return null;
    });

    return resultFuture;
  }

  /**
   * Checks if a download task is in progress for the specified chunk.
   *
   * @param chunkIndex The index of the chunk to check
   * @return true if a download task is in progress for the chunk, false otherwise
   */
  public boolean isDownloadInProgress(int chunkIndex) {
    return downloadTasks.containsKey(chunkIndex);
  }

  /**
   * Checks if any download tasks are in progress for the specified range.
   *
   * @param startPosition The start position in the file
   * @param endPosition The end position in the file (exclusive)
   * @return true if any download tasks are in progress for the range, false otherwise
   */
  public boolean isDownloadInProgress(long startPosition, long endPosition) {
    MerkleTree merkleTree = pane.getMerkleTree();
    int startChunk = merkleTree.getChunkIndexForPosition(startPosition);
    int endChunk = merkleTree.getChunkIndexForPosition(Math.min(endPosition - 1, merkleTree.totalSize() - 1));

    for (int i = startChunk; i <= endChunk; i++) {
      if (isDownloadInProgress(i)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Asynchronously downloads a node transfer and processes the chunks it contains.
   *
   * @param transfer The NodeTransfer to download
   * @return A CompletableFuture that completes when the transfer is done
   */
  private CompletableFuture<Void> downloadNodeTransferAsync(NodeTransfer transfer) {
    CompletableFuture<Void> future = new CompletableFuture<>();

    // Calculate the length of the transfer
    final long originalStart = transfer.start();
    final long originalEnd = transfer.end();

    // Get the merkle tree
    final MerkleTree merkleTree = pane.getMerkleTree();
    final long chunkSize = merkleTree.chunkSize();

    // Ensure the transfer aligns with chunk boundaries
    final long alignedStart = (originalStart / chunkSize) * chunkSize;
    final long alignedEnd = ((originalEnd + chunkSize - 1) / chunkSize) * chunkSize;

    // Determine the final start and end values
    final long start;
    long end; // Not final because we might need to adjust it

    // If the alignment changed the range, log a warning
    if (alignedStart != originalStart || alignedEnd != originalEnd) {
      eventSink.warn("Adjusting transfer range to align with chunk boundaries: [{}-{}] -> [{}-{}]",
                     originalStart, originalEnd, alignedStart, alignedEnd);
      start = alignedStart;
      end = alignedEnd;
    } else {
      start = originalStart;
      end = originalEnd;
    }

    // Calculate initial length
    long length = end - start;

    // Verify that the range is at least one chunk in size
    if (length < chunkSize) {
      eventSink.warn("Transfer size {} is smaller than chunk size {}, adjusting to one chunk",
                   length, chunkSize);
      end = start + chunkSize;
      length = chunkSize;
    }

    // Now that we've finalized the end value, make it final for the lambda
    final long finalEnd = end;
    final long finalLength = length;

    // Calculate the chunk range this transfer covers
    int startChunk = (int)(start / chunkSize);
    int endChunk = (int)((end - 1) / chunkSize);

    // Create a set of chunk indices to track which chunks are being downloaded in this transfer
    Set<Integer> chunkIndices = new HashSet<>();
    for (int i = startChunk; i <= endChunk; i++) {
      if (!pane.isChunkIntact(i)) {
        chunkIndices.add(i);
        // Register this chunk as being downloaded
        downloadTasks.put(i, future.thenApply(v -> true));
      }
    }

    if (chunkIndices.isEmpty()) {
      // No chunks need downloading
      future.complete(null);
      return future;
    }

    // Log the transfer
    eventSink.info("Downloading node transfer: level=" + transfer.node().level() +
                   ", chunks=" + chunkIndices.size() + ", bytes=" + finalLength);

    // Download the range asynchronously
    CompletableFuture.supplyAsync(() -> {
      try {
        // Download the range
        ByteBuffer buffer = downloadRange(start, finalLength);

        // Process each chunk in the transfer
        for (int chunkIndex : chunkIndices) {
          // Calculate the chunk boundaries
          MerkleTree.NodeBoundary bounds = merkleTree.getBoundariesForLeaf(chunkIndex);
          long chunkStart = bounds.start();
          long chunkEnd = bounds.end();
          int chunkLength = (int)(chunkEnd - chunkStart);

          // Extract the chunk data from the buffer
          ByteBuffer chunkBuffer = ByteBuffer.allocate(chunkLength);
          final int bufferPosition = (int)(chunkStart - start);
          if (bufferPosition >= 0 && bufferPosition + chunkLength <= buffer.limit()) {
            buffer.position(bufferPosition);
            byte[] chunkData = new byte[chunkLength];
            buffer.get(chunkData, 0, chunkLength);
            chunkBuffer.put(chunkData);
            chunkBuffer.flip();

            // Submit the chunk to the MerklePane
            pane.submitChunk(chunkIndex, chunkBuffer);
          } else {
            eventSink.error("Chunk " + chunkIndex + " is outside the downloaded range");
          }
        }

        return null;
      } catch (Exception e) {
        throw new CompletionException(e);
      } finally {
        // Remove the download tasks
        for (int chunkIndex : chunkIndices) {
          downloadTasks.remove(chunkIndex);
        }
      }
    }, downloadExecutor).thenAccept(v -> {
      future.complete(null);
    }).exceptionally(ex -> {
      future.completeExceptionally(ex);
      return null;
    });

    return future;
  }

  /**
   * Gets a set of chunk indices that are currently being downloaded.
   *
   * @return A set of chunk indices that are currently being downloaded
   */
  public Set<Integer> getInProgressChunks() {
    return downloadTasks.keySet();
  }

  /**
   * Blocks the calling thread until all pending downloads have completed.
   * This method waits for all download tasks that are currently in progress.
   *
   * @param timeout The maximum time to wait
   * @param unit The time unit of the timeout argument
   * @return true if all downloads completed, false if the timeout was reached
   * @throws InterruptedException If the current thread was interrupted while waiting
   */
  public boolean awaitAllDownloads(long timeout, TimeUnit unit) throws InterruptedException {
    long endTime = System.nanoTime() + unit.toNanos(timeout);

    while (!downloadTasks.isEmpty()) {
      // Create a copy of the current tasks to avoid ConcurrentModificationException
      Set<CompletableFuture<Boolean>> tasks = new HashSet<>(downloadTasks.values());

      if (tasks.isEmpty()) {
        return true; // No tasks to wait for
      }

      // Create a combined future that completes when all tasks complete
      CompletableFuture<Void> allTasks = CompletableFuture.allOf(
          tasks.toArray(new CompletableFuture[0]));

      try {
        // Calculate remaining time
        long remainingNanos = endTime - System.nanoTime();
        if (remainingNanos <= 0) {
          return false; // Timeout reached
        }

        // Wait for all tasks to complete or timeout
        allTasks.get(remainingNanos, TimeUnit.NANOSECONDS);

        // If we get here and downloadTasks is still not empty,
        // it means new tasks were added while we were waiting
        if (downloadTasks.isEmpty()) {
          return true;
        }
      } catch (TimeoutException e) {
        return false; // Timeout reached
      } catch (ExecutionException e) {
        // Log the error but continue waiting for other tasks
        eventSink.error("Error while waiting for downloads: {}", e.getMessage());
      }
    }

    return true;
  }

  /**
   * Blocks the calling thread until all pending downloads have completed.
   * This method waits indefinitely for all download tasks that are currently in progress.
   *
   * @throws InterruptedException If the current thread was interrupted while waiting
   */
  public void awaitAllDownloads() throws InterruptedException {
    while (!downloadTasks.isEmpty()) {
      // Create a copy of the current tasks to avoid ConcurrentModificationException
      Set<CompletableFuture<Boolean>> tasks = new HashSet<>(downloadTasks.values());

      if (tasks.isEmpty()) {
        return; // No tasks to wait for
      }

      // Create a combined future that completes when all tasks complete
      CompletableFuture<Void> allTasks = CompletableFuture.allOf(
          tasks.toArray(new CompletableFuture[0]));

      try {
        // Wait for all tasks to complete
        allTasks.join();

        // If we get here and downloadTasks is still not empty,
        // it means new tasks were added while we were waiting
        if (downloadTasks.isEmpty()) {
          return;
        }
      } catch (Exception e) {
        // Log the error but continue waiting for other tasks
        eventSink.error("Error while waiting for downloads: {}", e.getMessage());
      }
    }
  }

  @Override
  public void close() throws IOException {
    pane.close();

    // Shut down the download executor
    if (downloadExecutor != null && !downloadExecutor.isShutdown()) {
      downloadExecutor.shutdown();
    }
  }
}
