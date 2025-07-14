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


import io.nosqlbench.nbdatatools.api.transport.ChunkedTransportClient;
import io.nosqlbench.nbdatatools.api.transport.ChunkedTransportIO;
import io.nosqlbench.vectordata.downloader.DownloadProgress;
import io.nosqlbench.vectordata.downloader.DownloadResult;
import io.nosqlbench.vectordata.status.EventSink;
import io.nosqlbench.vectordata.status.LogFileEventSink;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static io.nosqlbench.vectordata.merkle.MerklePane.MREF;
import static io.nosqlbench.vectordata.merkle.MerklePane.MRKL;

/// # REQUIREMENTS
/// MerklePainter is an active wrapper around a MerklePane which knows how
/// to download and submit ("paint") chunks to the MerklePane as needed.
/// MerklePainter knows nothing about the internal structure of MerklePane except for the chunk
/// size and chunk indexes. When MerklePainter is asked to ensure some part of the content backed
///  by MerklePane is valid, it simply asks MerklePane for a list of ranges which need to be
/// fetched. These ranges are described to MerklePainter as NodeTransfers, which remember the
/// merkle index and the bounds. When results are posted back to MerklePane, they are posted with
///  their associated NodeTransfer.
public class MerklePainter implements Closeable {
  /// A pre-allocated array of zeros used as a placeholder for zeroed-out hash values.
  /// This is used when invalidating a chunk before downloading it to ensure the merkle tree
  /// never indicates a chunk is consistent when it is not known to be.
  public static final byte[] ZEROED_HASH = new byte[MerkleTree.HASH_SIZE];
  private final MerklePane pane;
  private final String sourcePath;
  private final Path localPath;
  private final Path merklePath;
  private final Path referenceTreePath;
  private final Path logFilePath;

  // Default minimum and maximum download sizes
  private static final long DEFAULT_MIN_DOWNLOAD_SIZE = 4 * 1024 * 1024; // 4MB
  private static final long DEFAULT_MAX_DOWNLOAD_SIZE = 32 * 1024 * 1024; // 32MB

  // Auto-buffering threshold - after this many contiguous requests, enable read-ahead mode
  private static final int AUTOBUFFER_THRESHOLD = 10;

  // Configurable minimum and maximum download sizes
  private final long minDownloadSize;
  private final long maxDownloadSize;

  // Thread pool for parallel downloads
  private final ExecutorService downloadExecutor;

  // Fields for tracking contiguous requests
  private MerkleRange lastRequestRange;
  private int contiguousRequestCount = 0;
  private boolean autoBufferMode = false;

  // ChunkedTransportClient for efficient downloads
  private final ChunkedTransportClient transportClient;
  private final EventSink eventSink;

  // Map to track in-progress download tasks by chunk index
  private final Map<Integer, CompletableFuture<Boolean>> downloadTasks = new ConcurrentHashMap<>();

  // Lock for atomic scheduling of downloads
  private final Object downloadSchedulingLock = new Object();

  /// Creates a new MerklePainter for the given local file and source URL
  /// @param localPath
  ///     Path where the local data file exists or will be stored
  /// @param sourcePath
  ///     URL of the source data file (merkle tree will be downloaded from
  ///             sourcePath + [MerklePane#MRKL])
  public MerklePainter(Path localPath, String sourcePath) {
    // Create a LogFileEventSink with a log file adjacent to the mrkl file
    this(localPath, sourcePath, new LogFileEventSink(localPath.resolveSibling(localPath.getFileName().toString() + ".log")));
  }

  /// Creates a new MerklePainter for the given local file and source URL with a custom event sink
  /// @param localPath
  ///     Path where the local data file exists or will be stored
  /// @param sourcePath
  ///     URL of the source data file (merkle tree will be downloaded from
  ///             sourcePath + [MerklePane#MRKL])
  /// @param eventSink
  ///     Event sink for logging download progress and events
  public MerklePainter(Path localPath, String sourcePath, EventSink eventSink) {
    this(localPath, sourcePath, eventSink, DEFAULT_MIN_DOWNLOAD_SIZE, DEFAULT_MAX_DOWNLOAD_SIZE);
  }


  /// Creates a new MerklePainter for the given local file and source URL with a custom event sink
  /// and custom minimum and maximum download sizes
  /// @param localPath
  ///     Path where the local data file exists or will be stored
  /// @param sourcePath
  ///     URL of the source data file (merkle tree will be downloaded from
  ///             sourcePath + [MerklePane#MRKL])
  /// @param eventSink
  ///     Event sink for logging download progress and events
  /// @param minDownloadSize
  ///     Minimum download size in bytes
  /// @param maxDownloadSize
  ///     Maximum download size in bytes
  public MerklePainter(Path localPath, String sourcePath, EventSink eventSink, long minDownloadSize, long maxDownloadSize) {
    // According to requirements, it should be invalid to create a MerklePane without a remote content URL
    if (sourcePath == null) {
      throw new IllegalArgumentException("Source path (remote content URL) must be provided");
    }

    this.localPath = localPath;
    this.sourcePath = sourcePath;
    this.merklePath = localPath.resolveSibling(localPath.getFileName().toString() + MRKL);
    this.referenceTreePath = localPath.resolveSibling(localPath.getFileName().toString() + MREF);
    this.logFilePath = localPath.resolveSibling(localPath.getFileName().toString() + ".log");
    this.eventSink = eventSink;
    this.minDownloadSize = minDownloadSize;
    this.maxDownloadSize = maxDownloadSize;

    // Initialize the pane using MerklePaneSetup to centralize setup logic
    this.pane = MerklePaneSetup.setupMerklePane(localPath, merklePath, sourcePath);

    // Initialize the download executor with a virtual thread executor
    this.downloadExecutor = Executors.newVirtualThreadPerTaskExecutor();

    // Initialize the transport client (sourcePath is guaranteed to be non-null at this point)
    try {
      this.transportClient = ChunkedTransportIO.create(sourcePath);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create transport client for: " + sourcePath + ": " + e.getMessage(), e);
    }

    // Register a shutdown hook to ensure proper cleanup when the JVM shuts down
    Runtime.getRuntime().addShutdownHook(new Thread(this::handleShutdown));
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
   Gets the MerklePane used by this painter.
   @return The MerklePane instance
   */
  public MerklePane pane() {
    return pane;
  }

  /**
   Gets the chunk geometry used by this painter.
   This delegates to the underlying MerklePane for consistency.
   @return The ChunkGeometryDescriptor instance from the pane
   */
  public ChunkGeometryDescriptor getGeometry() {
    return pane.getGeometry();
  }

  /**
   Gets the total size of the file as reported by the merkle tree.
   @return The total size in bytes
   */
  public long totalSize() {
    return pane.getTotalSize();
  }

  /// Checks whether all chunks in the specified range are already valid.
  /// This method is used as an early-exit check for both synchronous and asynchronous paint methods.
  /// 
  /// @param startIncl The start value inclusive
  /// @param endExcl The end value exclusive
  /// @return true if all chunks in the range are valid, false otherwise
  public boolean isRangeValid(long startIncl, long endExcl) {
    long totalSize = pane.getTotalSize();
    if (totalSize <= 0) {
      return true; // No data to download
    }

    int startChunk = pane.getChunkIndexForPosition(startIncl);
    int endChunk = pane.getChunkIndexForPosition(Math.min(endExcl - 1, totalSize - 1));

    // Check if all chunks in the range are already intact
    for (int chunkIndex = startChunk; chunkIndex <= endChunk; chunkIndex++) {
      boolean isIntact = pane.isChunkIntact(chunkIndex);
      if (!isIntact) {
        return false; // At least one chunk is not valid
      }
    }

    return true; // All chunks are valid
  }

  /// Uses the pane to determine what chunks need to be fetched, and then submits them to the
  /// pane for reconciliation. This call blocks until the downloads have been done and the pane
  /// is updated.
  /// 
  /// This method first checks if all chunks in the range are already valid using the isRangeValid method.
  /// If they are, it returns immediately without scheduling any downloads.
  /// 
  /// @param startIncl The start value inclusive
  /// @param endExcl The end value exclusive
  public void paint(long startIncl, long endExcl) {
    // Early-exit check: if all chunks in the range are already valid, return immediately
    if (isRangeValid(startIncl, endExcl)) {
      return;
    }


    try {
      DownloadProgress progress = paintAsync(startIncl, endExcl);
      DownloadResult result = progress.get(); // Call the asynchronous method and wait for completion
      if (!result.isSuccess()) {
        throw new RuntimeException("Error during paint operation: " + 
                                  (result.error() != null ? result.error().getMessage() : "Unknown error"));
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Error during paint operation: " + e.getMessage(), e);
    }
  }


  /// Uses the pane to determine what chunks need to be fetched, and then submits them to the
  /// pane for reconciliation. This call returns a DownloadProgress that can be used to check the
  /// status of an active download or otherwise synchronously wait for the result.
  /// 
  /// This method first checks if all chunks in the range are already valid using the isRangeValid method.
  /// If they are, it returns a completed DownloadProgress without scheduling any downloads.
  ///
  /// This method automatically selects download sizes that align with chunk boundaries, respecting
  /// the minimum and maximum download size constraints. It also ensures that downloads are only
  /// scheduled for chunks that don't already have pending downloads.
  ///
  /// This method also implements auto-buffering behavior:
  /// - It tracks the most recent request range
  /// - It counts contiguous requests (requests that are logically contiguous from the last one)
  /// - When the contiguous request count exceeds AUTOBUFFER_THRESHOLD, it enables auto-buffer mode
  /// - In auto-buffer mode, it uses the maximum download size and keeps at least 4 additional
  ///   requests running in read-ahead mode from the last user-provided request
  ///
  /// @param startIncl the start value inclusive
  /// @param endExcl the end value exclusive
  /// @return a DownloadProgress that can be used to track the download progress
  public DownloadProgress paintAsync(long startIncl, long endExcl) {
    AtomicLong currentBytes = new AtomicLong(0);
    CompletableFuture<DownloadResult> future = new CompletableFuture<>();

    // Early-exit check: if all chunks in the range are already valid, return immediately
    if (isRangeValid(startIncl, endExcl)) {
      future.complete(DownloadResult.skipped(localPath, 0));
      return new DownloadProgress(localPath, 0, currentBytes, future);
    }

    long totalSize = pane.getTotalSize(); // Get the total size from the pane
    if (totalSize <= 0) {
      future.complete(DownloadResult.skipped(localPath, 0));
      return new DownloadProgress(localPath, 0, currentBytes, future);
    }

    int startChunk = pane.getChunkIndexForPosition(startIncl);
    int endChunk = pane.getChunkIndexForPosition(Math.min(endExcl - 1, totalSize - 1));

    // Create current request range
    MerkleRange currentRange = new MerkleRange(startIncl, endExcl);

    // Check if this request is contiguous with the last one
    if (lastRequestRange != null) {
      // A request is logically contiguous if it starts exactly where the last one ended
      if (currentRange.start() == lastRequestRange.end()) {
        contiguousRequestCount++;

        // Check if we should enable auto-buffer mode
        if (contiguousRequestCount >= AUTOBUFFER_THRESHOLD && !autoBufferMode) {
          autoBufferMode = true;
          eventSink.log(MerklePainterEvent.AUTO_BUFFER_ON, 
                       "count", contiguousRequestCount, 
                       "threshold", AUTOBUFFER_THRESHOLD);
        }
      } else {
        // Reset counter for non-contiguous request
        contiguousRequestCount = 0;
        autoBufferMode = false;
      }
    }

    // Update last request range
    lastRequestRange = currentRange;

    // Calculate total bytes to download for the requested range
    long bytesToDownload = 0;
    for (int chunkIndex = startChunk; chunkIndex <= endChunk; chunkIndex++) {
      if (!pane.isChunkIntact(chunkIndex) && !downloadTasks.containsKey(chunkIndex)) {
        MerkleMismatch boundary = pane.getChunkBoundary(chunkIndex);
        bytesToDownload += (boundary.endExclusive() - boundary.startInclusive());
      }
    }

    // Create DownloadProgress with the total bytes to download
    DownloadProgress progress = new DownloadProgress(localPath, bytesToDownload, currentBytes, future);

    // If there's nothing to download in the requested range, complete immediately
    if (bytesToDownload == 0) {
      future.complete(DownloadResult.skipped(localPath, 0));

      // If in auto-buffer mode, still check if we need to do read-ahead
      if (autoBufferMode) {
        scheduleReadAhead(endChunk);
      }

      return progress;
    }

    // Run the download asynchronously
    CompletableFuture.runAsync(() -> {
      try {
        // Group chunks into optimal download ranges based on min/max download size
        // In auto-buffer mode, prefer using maximum download size
        List<ChunkRange> downloadRanges = calculateOptimalDownloadRanges(startChunk, endChunk);

        for (ChunkRange range : downloadRanges) {
          // Download each range of chunks
          try {
            downloadAndSubmitChunkRange(range.startChunk(), range.endChunk(), currentBytes);
          } catch (ChunkDownloadException e) {
            future.complete(DownloadResult.failed(localPath, e));
            return;
          }
        }

        // If in auto-buffer mode, schedule read-ahead downloads
        if (autoBufferMode) {
          scheduleReadAhead(endChunk);
        }

        future.complete(DownloadResult.downloaded(localPath, currentBytes.get()));
      } catch (Exception e) {
        eventSink.log(MerklePainterEvent.ERROR_PAINTING, "text", e.getMessage());
        future.complete(DownloadResult.failed(localPath, e));
      }
    });

    return progress;
  }

  /**
   * Schedules read-ahead downloads starting from the specified chunk.
   * This method is used in auto-buffer mode to keep at least 4 additional requests
   * running in read-ahead mode from the last user-provided request.
   * 
   * This method ensures atomic scheduling of downloads using the downloadSchedulingLock.
   *
   * @param lastChunk The last chunk of the user-provided request
   */
  private void scheduleReadAhead(int lastChunk) {
    int readAheadCount = 4; // Number of read-ahead requests to keep running
    int totalChunks = pane.merkleTree().getNumberOfLeaves();
    int nextChunk = lastChunk + 1;

    // Don't read past the end of the file
    if (nextChunk >= totalChunks) {
      return;
    }

    // Find chunks that need downloading with proper synchronization
    List<Integer> chunksToDownload = new ArrayList<>();
    List<CompletableFuture<Boolean>> readAheadFutures = new ArrayList<>();

    synchronized (downloadSchedulingLock) {
      // Find up to readAheadCount chunks that need downloading
      int count = 0;
      for (int chunkIndex = nextChunk; chunkIndex < totalChunks && count < readAheadCount; chunkIndex++) {
        // Skip chunks that are already intact or have pending downloads
        if (pane.isChunkIntact(chunkIndex) || downloadTasks.containsKey(chunkIndex)) {
          continue;
        }

        // This chunk needs to be downloaded - reserve it immediately
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        downloadTasks.put(chunkIndex, future);
        chunksToDownload.add(chunkIndex);
        readAheadFutures.add(future);
        count++;
      }
    }

    // If we found chunks that need downloading, schedule read-ahead downloads
    if (!chunksToDownload.isEmpty()) {
      // Log read-ahead download
      eventSink.log(MerklePainterEvent.READ_AHEAD, 
                   "from", chunksToDownload.get(0), 
                   "to", chunksToDownload.get(chunksToDownload.size() - 1));

      // Schedule the download asynchronously
      CompletableFuture.runAsync(() -> {
        try {
          AtomicLong readAheadBytes = new AtomicLong(0);

          // Process each chunk individually - they've already been reserved in the downloadTasks map
          for (int i = 0; i < chunksToDownload.size(); i++) {
            int chunkIndex = chunksToDownload.get(i);
            CompletableFuture<Boolean> future = readAheadFutures.get(i);

            // Skip chunks that are already intact (final check)
            if (pane.isChunkIntact(chunkIndex)) {
              future.complete(true);
              synchronized (downloadSchedulingLock) {
                downloadTasks.remove(chunkIndex);
              }
              continue;
            }

            // Download and submit this chunk
            try {
              downloadAndSubmitChunkRange(chunkIndex, chunkIndex, readAheadBytes);
            } catch (ChunkDownloadException e) {
              // Log the error but continue with other chunks
              eventSink.log(MerklePainterEvent.ERROR_READ_AHEAD, 
                           "index", chunkIndex,
                           "text", e.getMessage());
              future.complete(false);
              synchronized (downloadSchedulingLock) {
                downloadTasks.remove(chunkIndex);
              }
            }
          }
        } catch (Exception e) {
          eventSink.log(MerklePainterEvent.ERROR_READ_AHEAD, 
                       "from", chunksToDownload.get(0), 
                       "to", chunksToDownload.get(chunksToDownload.size() - 1), 
                       "text", e.getMessage());
        }
      }, downloadExecutor);
    }
  }

  /// When a chunk is downloaded and submitted, a specific sequence of operations is required to
  /// ensure consistency. A key invariant to updating the merkle tree is that it should never
  /// indicate that a chunk is consistent when it is not known to be. This includes when a chunk
  /// is scheduled to be downloaded and when the hash for the chunk is being calculated. So when
  /// a chunk is going to be downloaded, the leaf for that chunk should first be invalidated,
  ///  and then the contents of its hash should be zeroed out. Only after the data for the chunk
  /// has been persisted to disk should the hash for the chunk be calculated and then updated.
  /// 
  /// Before persisting the chunk, it is verified against the reference merkle tree:
  /// 1. A hash for the downloaded content is computed
  /// 2. The hash is compared to the reference hash for the same chunk index
  /// 3. If the hashes match, the content is persisted and the merkle tree is updated
  /// 4. If the hashes don't match, the download is considered failed and retried up to a limit
  ///
  /// @param chunkIndex The index for the chunk to be downloaded and udpated in the merkle tree
  /// @return True if the chunk was downloaded and submitted successfully, false otherwise
  boolean downloadAndSubmitChunk(int chunkIndex) {
    final int MAX_RETRY_ATTEMPTS = 3;
    int retryAttempt = 0;

    try {
      // Step 1: Invalidate the leaf and its ancestors in the Merkle tree
      pane.getMerkleTree().invalidateLeaf(chunkIndex);

      // Step 3: Get the chunk boundaries
      MerkleMismatch boundary = pane.getChunkBoundary(chunkIndex);
      long start = boundary.startInclusive();
      long length = boundary.endExclusive() - start;

      // Retry loop for downloading and verifying the chunk
      while (retryAttempt <= MAX_RETRY_ATTEMPTS) {
        // Download the chunk data
        ByteBuffer chunkData = downloadRange(start, length);

        if (chunkData == null) {
          eventSink.log(MerklePainterEvent.DOWNLOAD_FAILED, 
              "index", chunkIndex,
              "start", start,
              "size", length,
              "reason", "Download returned null");
          return false;
        }

        // Step 4: Verify the downloaded chunk against the reference merkle tree
        eventSink.log(MerklePainterEvent.CHUNK_VFY_START, "index", chunkIndex);

        try (ObjectPool.Borrowed<MessageDigest> borrowedDigest = pane.merkleTree().withDigest()) {
          MessageDigest messageDigest = borrowedDigest.get();

          // Create a byte array copy of the buffer data to ensure consistent hashing
          // This matches the approach used in MerkleTree.hashData
          byte[] chunkDataArray = new byte[chunkData.remaining()];
          chunkData.duplicate().get(chunkDataArray);

          // If the chunk data is empty, use a single zero byte instead
          // This ensures that empty chunks are hashed consistently with MerkleTree
          if (chunkDataArray.length == 0) {
            chunkDataArray = new byte[1];
            chunkDataArray[0] = 0;
          }

          messageDigest.update(chunkDataArray);
          byte[] downloadedHash = messageDigest.digest();

          // Get the reference hash for this chunk index
          MerkleTree refTree = pane.getRefTree();
          if (refTree == null) {
            eventSink.log(MerklePainterEvent.CHUNK_VFY_FAIL, 
                "index", chunkIndex, 
                "text", "Reference merkle tree not available",
                "refHash", "",
                "compHash", bytesToHex(downloadedHash));
            return false;
          }

          byte[] referenceHash = refTree.getHashForLeaf(chunkIndex);

          // Compare the hashes
          if (MessageDigest.isEqual(downloadedHash, referenceHash)) {
            // Hashes match, verification successful
            eventSink.log(MerklePainterEvent.CHUNK_VFY_OK, "index", chunkIndex);

            // Step 5: Persist the data to disk and update the merkle tree
            try {
              // Use submitChunkWithHash to ensure the valid bit is set properly
              submitChunkWithHash(chunkIndex, chunkData, downloadedHash);
              return true;
            } catch (ChunkSubmissionException e) {
              eventSink.log(MerklePainterEvent.ERROR_CHUNK, "index", chunkIndex, "text", e.getMessage());
              return false;
            }
          } else {
            // Hashes don't match, verification failed
            if (retryAttempt < MAX_RETRY_ATTEMPTS) {
              // Retry the download
              retryAttempt++;
              eventSink.log(MerklePainterEvent.CHUNK_VFY_RETRY, 
                  "index", chunkIndex, 
                  "attempt", retryAttempt);
              continue;
            } else {
              // Max retries reached, verification failed
              eventSink.log(MerklePainterEvent.CHUNK_VFY_FAIL, 
                  "index", chunkIndex, 
                  "text", "Hash verification failed after " + MAX_RETRY_ATTEMPTS + " attempts",
                  "refHash", bytesToHex(referenceHash),
                  "compHash", bytesToHex(downloadedHash));
              return false;
            }
          }
        } catch (RuntimeException e) {
          eventSink.log(MerklePainterEvent.ERROR_HASH, "text", e.getMessage());
          return false;
        }
      }

      // If we get here, all verification attempts failed
      return false;
    } catch (Exception e) {
      eventSink.log(MerklePainterEvent.ERROR_CHUNK, "index", chunkIndex, "text", e.getMessage());
      return false;
    }
  }

  private ByteBuffer downloadRange(long start, long length) {
    try {
      // Check if transport client is null (which can happen if initialization failed)
      if (transportClient == null) {
        throw new IllegalStateException("ChunkedTransportClient is not initialized. Cannot download content.");
      }

      // Use the transport client to fetch the range
      CompletableFuture<ByteBuffer> future = transportClient.fetchRange(start, (int) length);
      ByteBuffer result = future.get();
      
      if (result == null) {
        throw new IOException("Transport client returned null for range " + start + "-" + (start + length - 1));
      }
      
      return result;
    } catch (Exception e) {
      String errorMessage = "Error downloading range " + start + "-" + (start + length - 1) + ": " + e.getMessage();
      eventSink.log(MerklePainterEvent.ERROR_DOWNLOAD, "start", start, "size", length, "text",
          e.getMessage(), "code", 0);
      throw new RuntimeException(errorMessage, e);
    }
  }

  /// Await all download streams which are pending before unblocking the caller.
  /// @throws InterruptedException if the current thread is interrupted while waiting
  public void awaitAllDownloads() throws InterruptedException {
    // Take a snapshot of the current tasks to avoid concurrent modification
    List<CompletableFuture<Boolean>> tasksToWait;
    synchronized (downloadSchedulingLock) {
      tasksToWait = new ArrayList<>(downloadTasks.values());
    }

    // Wait for all tasks to complete
    while (tasksToWait.stream().anyMatch(f -> !f.isDone())) {
      Thread.sleep(100); // Check periodically
    }

    // Check for exceptions in completed futures and rethrow if any
    for (CompletableFuture<Boolean> future : tasksToWait) {
      try {
        future.get(); // This will rethrow any exception that occurred during the download
      } catch (ExecutionException e) {
        if (e.getCause() instanceof IOException) {
          throw new RuntimeException("An IO exception occurred while waiting for downloads: " + e.getCause().getMessage(), e.getCause());
        } else {
          throw new RuntimeException("An unexpected exception occurred while waiting for downloads: " + e.getCause().getMessage(), e.getCause());
        }
      }
    }

    // Clear completed tasks from the map
    synchronized (downloadSchedulingLock) {
      downloadTasks.values().removeIf(CompletableFuture::isDone);
    }
  }

  /**
   * Waits for all pending downloads to complete, with a timeout.
   *
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   * @return true if all downloads completed successfully before the timeout, false if the timeout was reached
   * @throws InterruptedException if the current thread is interrupted while waiting
   */
  public boolean awaitAllDownloads(int timeout, TimeUnit unit) throws InterruptedException {
    if (downloadTasks.isEmpty()) {
      return true; // No downloads to wait for
    }

    try {
      // Convert all individual futures into a single future that completes when all complete
      CompletableFuture<Void> allFutures = CompletableFuture.allOf(
        downloadTasks.values().toArray(new CompletableFuture[0])
      );

      // Wait with timeout
      allFutures.get(timeout, unit);

      // If we reach here, all downloads completed before the timeout
      downloadTasks.clear(); // Clear the completed tasks
      return true;
    } catch (TimeoutException e) {
      // Timeout reached before all downloads completed
      return false;
    } catch (ExecutionException e) {
      // Handle exceptions that occurred during the download tasks
      if (e.getCause() instanceof IOException) {
        throw new RuntimeException("An IO exception occurred while waiting for downloads: " + e.getCause().getMessage(), e.getCause());
      } else {
        throw new RuntimeException("An unexpected exception occurred while waiting for downloads: " + e.getCause().getMessage(), e.getCause());
      }
    }
  }


  /**
   Record class to represent a transfer of a Merkle node.
   */
  private record NodeTransfer(int merkleNodeIndex, long byteRangeStartInclusive, long byteRangeEndExclusive) {
  }

  /**
   Record class to represent a range of chunks to download.
   */
  private record ChunkRange(int startChunk, int endChunk, long startByte, long endByte) {
  }

  /**
   * Calculates optimal download ranges based on chunk boundaries and min/max download size constraints.
   * This method groups chunks into ranges that respect the minimum and maximum download size constraints
   * while ensuring that downloads align with chunk boundaries.
   * 
   * This method checks for existing downloads in a synchronized manner to ensure consistency with
   * the atomic scheduling in downloadAndSubmitChunkRange.
   *
   * @param startChunk The starting chunk index (inclusive)
   * @param endChunk The ending chunk index (inclusive)
   * @return A list of ChunkRange objects representing the optimal download ranges
   */
  private List<ChunkRange> calculateOptimalDownloadRanges(int startChunk, int endChunk) {
    List<ChunkRange> ranges = new ArrayList<>();

    // If no chunks to download, return empty list
    if (startChunk > endChunk) {
      return ranges;
    }

    int currentStartChunk = -1;
    long currentRangeSize = 0;
    long currentStartByte = 0;
    int lastChunkInRange = -1;

    for (int chunkIndex = startChunk; chunkIndex <= endChunk; chunkIndex++) {
      // Check if chunk is intact
      boolean isIntact = pane.isChunkIntact(chunkIndex);

      // Check if chunk has a pending download in a synchronized manner
      boolean hasPendingDownload;
      synchronized (downloadSchedulingLock) {
        hasPendingDownload = downloadTasks.containsKey(chunkIndex);
      }

      // Skip chunks that are already intact or have pending downloads
      if (isIntact || hasPendingDownload) {
        // If we were building a range, finalize it before skipping this chunk
        if (currentStartChunk != -1 && currentRangeSize > 0) {
          // Only add the range if it meets the minimum size or it's the only range we have
          if (currentRangeSize >= minDownloadSize || ranges.isEmpty()) {
            ranges.add(new ChunkRange(currentStartChunk, lastChunkInRange, currentStartByte, 
                                      currentStartByte + currentRangeSize));
          }
          currentStartChunk = -1;
          currentRangeSize = 0;
        }
        continue;
      }

      // Get the chunk boundaries
      MerkleMismatch boundary = pane.getChunkBoundary(chunkIndex);
      long chunkSize = boundary.endExclusive() - boundary.startInclusive();

      // If this is the first chunk in a potential range, record its details
      if (currentStartChunk == -1) {
        currentStartChunk = chunkIndex;
        currentStartByte = boundary.startInclusive();
        currentRangeSize = chunkSize;
        lastChunkInRange = chunkIndex;
      } else {
        // Check if adding this chunk would exceed the maximum download size
        if (currentRangeSize + chunkSize > maxDownloadSize) {
          // If the current range meets the minimum size, add it
          if (currentRangeSize >= minDownloadSize) {
            ranges.add(new ChunkRange(currentStartChunk, lastChunkInRange, currentStartByte, 
                                      currentStartByte + currentRangeSize));
            // Start a new range with this chunk
            currentStartChunk = chunkIndex;
            currentStartByte = boundary.startInclusive();
            currentRangeSize = chunkSize;
            lastChunkInRange = chunkIndex;
          } else {
            // If the current range is too small, add this chunk anyway
            // This might exceed the maximum size, but it's better than having a range that's too small
            currentRangeSize += chunkSize;
            lastChunkInRange = chunkIndex;

            // Now the range should meet the minimum size, so add it
            ranges.add(new ChunkRange(currentStartChunk, lastChunkInRange, currentStartByte, 
                                      currentStartByte + currentRangeSize));
            // Reset for the next range
            currentStartChunk = -1;
            currentRangeSize = 0;
          }
        } else {
          // Add this chunk to the current range
          currentRangeSize += chunkSize;
          lastChunkInRange = chunkIndex;

          // Check if we've reached the maximum download size
          if (currentRangeSize >= maxDownloadSize) {
            ranges.add(new ChunkRange(currentStartChunk, lastChunkInRange, currentStartByte, 
                                      currentStartByte + currentRangeSize));
            // Reset for the next range
            currentStartChunk = -1;
            currentRangeSize = 0;
          }
        }
      }
    }

    // Add any remaining range
    if (currentStartChunk != -1 && currentRangeSize > 0) {
      // Only add if it meets the minimum size or it's the last range and we have no other ranges
      if (currentRangeSize >= minDownloadSize || ranges.isEmpty()) {
        ranges.add(new ChunkRange(currentStartChunk, lastChunkInRange, currentStartByte, 
                                  currentStartByte + currentRangeSize));
      } else {
        // If the last range is too small and we have previous ranges, try to merge it with the previous range
        if (!ranges.isEmpty()) {
          ChunkRange lastRange = ranges.remove(ranges.size() - 1);
          // Check if merging would exceed the maximum size
          long mergedSize = (lastRange.endByte - lastRange.startByte) + currentRangeSize;
          if (mergedSize <= maxDownloadSize) {
            // Merge with the previous range
            ranges.add(new ChunkRange(lastRange.startChunk, lastChunkInRange, lastRange.startByte, 
                                      currentStartByte + currentRangeSize));
          } else {
            // If merging would exceed the maximum size, add both ranges separately
            ranges.add(lastRange);
            ranges.add(new ChunkRange(currentStartChunk, lastChunkInRange, currentStartByte, 
                                      currentStartByte + currentRangeSize));
          }
        } else {
          // If there's no previous range, add it anyway
          ranges.add(new ChunkRange(currentStartChunk, lastChunkInRange, currentStartByte, 
                                    currentStartByte + currentRangeSize));
        }
      }
    }

    // Log the calculated ranges for debugging
    for (ChunkRange range : ranges) {
      long size = range.endByte - range.startByte;
      eventSink.debug("Calculated download range: chunks " + range.startChunk + "-" + range.endChunk + 
                     ", size " + size + " bytes");
    }

    return ranges;
  }

  /**
   * Downloads and submits a range of chunks.
   * This method downloads a range of chunks as a single HTTP request and then submits each chunk individually.
   * It ensures atomic scheduling of downloads and that callers requesting the same block share the same Future.
   *
   * @param startChunkIndex The starting chunk index (inclusive)
   * @param endChunkIndex The ending chunk index (inclusive)
   * @param currentBytes An AtomicLong to track the number of bytes downloaded
   * @throws ChunkDownloadException If any chunk in the range fails to download or verify
   */
  private void downloadAndSubmitChunkRange(int startChunkIndex, int endChunkIndex, AtomicLong currentBytes) throws ChunkDownloadException {
    // Check if any chunks in the range already have pending downloads
    // and collect futures for those chunks
    List<CompletableFuture<Boolean>> existingFutures = new ArrayList<>();
    List<Integer> chunksToDownload = new ArrayList<>();

    // First, determine which chunks need to be downloaded with proper synchronization
    synchronized (downloadSchedulingLock) {
      for (int chunkIndex = startChunkIndex; chunkIndex <= endChunkIndex; chunkIndex++) {
        // Skip chunks that are already intact
        if (pane.isChunkIntact(chunkIndex)) {
          continue;
        }

        // Check if this chunk already has a pending download
        CompletableFuture<Boolean> existingFuture = downloadTasks.get(chunkIndex);
        if (existingFuture != null) {
          existingFutures.add(existingFuture);
        } else {
          // This chunk needs to be downloaded
          chunksToDownload.add(chunkIndex);
        }
      }

      // If there are existing futures but no new chunks to download, wait for the futures to complete
      if (!existingFutures.isEmpty() && chunksToDownload.isEmpty()) {
        // Release the lock while waiting for futures
        eventSink.log(MerklePainterEvent.WAITING_DOWNLOADS, 
            "from", startChunkIndex, 
            "to", endChunkIndex);
      }
    }

    // If there are existing futures but no new chunks to download, wait for the futures to complete outside the lock
    if (!existingFutures.isEmpty() && chunksToDownload.isEmpty()) {
      try {
        CompletableFuture.allOf(existingFutures.toArray(new CompletableFuture[0])).get();
        // Check if all chunks are now intact
        boolean allIntact = true;
        for (int chunkIndex = startChunkIndex; chunkIndex <= endChunkIndex; chunkIndex++) {
          if (!pane.isChunkIntact(chunkIndex)) {
            allIntact = false;
            break;
          }
        }
        if (allIntact) {
          return;
        }
      } catch (InterruptedException | ExecutionException e) {
        eventSink.log(MerklePainterEvent.ERROR_WAITING, 
            "from", startChunkIndex, 
            "to", endChunkIndex, 
            "text", e.getMessage());
        throw new ChunkDownloadException("Error waiting for existing downloads: " + e.getMessage(), 
            e, startChunkIndex, endChunkIndex);
      }
    }

    // If there are no chunks to download, return (nothing to do)
    if (chunksToDownload.isEmpty()) {
      return;
    }

    // Process chunks that need to be downloaded
    for (int chunkIndex : chunksToDownload) {
      // Skip chunks that are already intact (double-check)
      if (pane.isChunkIntact(chunkIndex)) {
        continue;
      }

      CompletableFuture<Boolean> chunkFuture;

      // Atomically check and create futures for each chunk
      synchronized (downloadSchedulingLock) {
        // Check again if the chunk is already intact or has a pending download
        if (pane.isChunkIntact(chunkIndex) || downloadTasks.containsKey(chunkIndex)) {
          continue;
        }

        // Create a new future for this chunk
        chunkFuture = new CompletableFuture<>();
        downloadTasks.put(chunkIndex, chunkFuture);
      }

      try {
        // Get the chunk boundaries
        MerkleMismatch boundary = pane.getChunkBoundary(chunkIndex);
        long chunkStart = boundary.startInclusive();
        long chunkSize = boundary.endExclusive() - chunkStart;

        // Log the start of the download for this chunk
        eventSink.log(MerklePainterEvent.RANGE_START, 
            "from", chunkIndex, 
            "to", chunkIndex, 
            "begin", chunkStart, 
            "end", chunkStart + chunkSize, 
            "size", chunkSize);

        // Download just this chunk
        ByteBuffer chunkData = downloadRange(chunkStart, chunkSize);
        if (chunkData == null) {
          eventSink.log(MerklePainterEvent.RANGE_FAILED, 
              "from", chunkIndex, 
              "to", chunkIndex, 
              "begin", chunkStart, 
              "end", chunkStart + chunkSize, 
              "size", chunkSize);
          chunkFuture.complete(false);
          synchronized (downloadSchedulingLock) {
            downloadTasks.remove(chunkIndex);
          }
          throw new ChunkDownloadException("Failed to download chunk data", startChunkIndex, endChunkIndex);
        }

        // Log the completion of the download
        eventSink.log(MerklePainterEvent.RANGE_COMPLETE, 
            "from", chunkIndex, 
            "to", chunkIndex, 
            "begin", chunkStart, 
            "end", chunkStart + chunkSize, 
            "size", chunkSize);

        // Verify the chunk before submitting it
        eventSink.log(MerklePainterEvent.CHUNK_VFY_START, "index", chunkIndex);

        try (ObjectPool.Borrowed<MessageDigest> borrowedDigest = pane.merkleTree().withDigest()) {
          // Calculate the hash of the downloaded chunk using MerkleTree's digest pool
          MessageDigest digest = borrowedDigest.get();

          // Create a byte array copy of the buffer data to ensure consistent hashing
          // This matches the approach used in MerkleTree.hashData
          byte[] chunkDataArray = new byte[chunkData.remaining()];
          chunkData.duplicate().get(chunkDataArray);

          // If the chunk data is empty, use a single zero byte instead
          // This ensures that empty chunks are hashed consistently with MerkleTree
          if (chunkDataArray.length == 0) {
            chunkDataArray = new byte[1];
            chunkDataArray[0] = 0;
          }

          digest.update(chunkDataArray);
          byte[] downloadedHash = digest.digest();

          // Get the reference hash for this chunk index
          MerkleTree refTree = pane.getRefTree();
          if (refTree == null) {
            eventSink.log(MerklePainterEvent.CHUNK_VFY_FAIL, 
                "index", chunkIndex, 
                "text", "Reference merkle tree not available",
                "refHash", "",
                "compHash", bytesToHex(downloadedHash));
            chunkFuture.complete(false);
            synchronized (downloadSchedulingLock) {
              downloadTasks.remove(chunkIndex);
            }
            throw new ChunkDownloadException("Reference merkle tree not available for chunk verification", startChunkIndex, endChunkIndex);
          }

          byte[] referenceHash = refTree.getHashForLeaf(chunkIndex);

          // Compare the hashes
          if (MessageDigest.isEqual(downloadedHash, referenceHash)) {
            // Hashes match, verification successful
            eventSink.log(MerklePainterEvent.CHUNK_VFY_OK, "index", chunkIndex);

            // Submit the chunk with verification to ensure ShadowTree is updated
            try {
              // Use verification mode to ensure proper ShadowTree tracking
              boolean verified = pane.submitChunk(chunkIndex, chunkData, true);
              if (verified) {
                currentBytes.addAndGet(chunkSize);
                chunkFuture.complete(true);
              } else {
                chunkFuture.complete(false);
                synchronized (downloadSchedulingLock) {
                  downloadTasks.remove(chunkIndex);
                }
                throw new ChunkDownloadException("Chunk verification failed during submission", startChunkIndex, endChunkIndex);
              }
            } catch (IOException e) {
              chunkFuture.complete(false);
              synchronized (downloadSchedulingLock) {
                downloadTasks.remove(chunkIndex);
              }
              throw new ChunkDownloadException("Failed to submit chunk with verification: " + e.getMessage(), e, startChunkIndex, endChunkIndex);
            }
          } else {
            // Hashes don't match, verification failed
            eventSink.log(MerklePainterEvent.CHUNK_VFY_FAIL, 
                "index", chunkIndex, 
                "text", "Hash verification failed",
                "refHash", bytesToHex(referenceHash),
                "compHash", bytesToHex(downloadedHash));
            chunkFuture.complete(false);
            synchronized (downloadSchedulingLock) {
              downloadTasks.remove(chunkIndex);
            }
            throw new ChunkDownloadException("Hash verification failed for chunk " + chunkIndex, startChunkIndex, endChunkIndex, referenceHash, downloadedHash);
          }
        } catch (RuntimeException e) {
          eventSink.log(MerklePainterEvent.ERROR_HASH, "text", e.getMessage());
          chunkFuture.complete(false);
          synchronized (downloadSchedulingLock) {
            downloadTasks.remove(chunkIndex);
          }
          // If it's already a ChunkDownloadException, rethrow it directly
          if (e instanceof ChunkDownloadException) {
            throw e;
          }
          throw new ChunkDownloadException("Hash algorithm not available: " + e.getMessage(), e, startChunkIndex, endChunkIndex);
        }
      } catch (Exception e) {
        eventSink.log(MerklePainterEvent.CHUNK_PROC_ERROR, "index", chunkIndex, "text", e.getMessage());
        chunkFuture.completeExceptionally(e);
        synchronized (downloadSchedulingLock) {
          downloadTasks.remove(chunkIndex);
        }
        throw new ChunkDownloadException("Error processing chunk " + chunkIndex + ": " + e.getMessage(), e, startChunkIndex, endChunkIndex);
      }
    }

    // All chunks were processed successfully
    return;
  }

  /**
   * Submits a chunk to the pane for processing.
   * This method handles the invalidation, zeroing, and updating of the chunk in the merkle tree.
   *
   * @param chunkIndex The index of the chunk to submit
   * @param chunkData The data for the chunk
   * @throws ChunkSubmissionException if the chunk submission fails
   */
  private void submitChunk(int chunkIndex, ByteBuffer chunkData) throws ChunkSubmissionException {
    try {
      // Log the start of the chunk submission process
      eventSink.log(MerklePainterEvent.CHUNK_START, "index", chunkIndex);

      pane.submitChunk(chunkIndex, chunkData);
      eventSink.log(MerklePainterEvent.CHUNK_VALID, "index", chunkIndex);
    } catch (Exception e) {
      eventSink.log(MerklePainterEvent.CHUNK_FAILED, "index", chunkIndex, "text", e.getMessage());
      throw new ChunkSubmissionException("Failed to submit chunk " + chunkIndex + ": " + e.getMessage(), e);
    }
  }

  /**
   * Converts a byte array to a hexadecimal string representation.
   *
   * @param bytes The byte array to convert
   * @return The hexadecimal string representation of the byte array
   */
  private String bytesToHex(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return "";
    }
    StringBuilder hexString = new StringBuilder(2 * bytes.length);
    for (byte b : bytes) {
      String hex = Integer.toHexString(0xff & b);
      if (hex.length() == 1) {
        hexString.append('0');
      }
      hexString.append(hex);
    }
    return hexString.toString();
  }

  /**
   * Submits a chunk to the pane for processing with a pre-computed hash.
   * This method avoids recomputing the hash when it's already available.
   *
   * @param chunkIndex The index of the chunk to submit
   * @param chunkData The data for the chunk
   * @param precomputedHash The pre-computed hash of the chunk
   * @throws ChunkSubmissionException if the chunk submission fails
   */
  private void submitChunkWithHash(int chunkIndex, ByteBuffer chunkData, byte[] precomputedHash) throws ChunkSubmissionException {
    try {
      // Log the start of the chunk submission process
      eventSink.log(MerklePainterEvent.CHUNK_START, "index", chunkIndex);

      pane.submitChunkWithHash(chunkIndex, chunkData, precomputedHash);
      eventSink.log(MerklePainterEvent.CHUNK_VALID, "index", chunkIndex);
    } catch (Exception e) {
      eventSink.log(MerklePainterEvent.CHUNK_FAILED, "index", chunkIndex, "text", e.getMessage());
      throw new ChunkSubmissionException("Failed to submit chunk " + chunkIndex + " with hash: " + e.getMessage(), e);
    }
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


  /// Handles the shutdown sequence when the JVM is shutting down
  /// This method implements the requirements from package-info.java:
  /// 1. Stop or abandon any pending transfers
  /// 2. Compute any merkle tree hashes which are calculable
  /// 3. Flush the merkle tree data to disk, ensuring the merkle tree file mtime is at least one millisecond newer than the content
  private void handleShutdown() {
    try {
      eventSink.log(MerklePainterEvent.SHUTDOWN_INIT, "path", localPath.toString());

      // 1. Stop or abandon any pending transfers
      eventSink.log(MerklePainterEvent.SHUTDOWN_STOPPING);
      for (Map.Entry<Integer, CompletableFuture<Boolean>> entry : downloadTasks.entrySet()) {
        if (!entry.getValue().isDone()) {
          entry.getValue().cancel(true);
        }
      }
      downloadTasks.clear();

      if (downloadExecutor != null && !downloadExecutor.isShutdown()) {
        downloadExecutor.shutdownNow();
        try {
          // Wait a short time for tasks to terminate
          downloadExecutor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }

      // Close the transport client
      if (transportClient != null) {
        try {
          transportClient.close();
        } catch (Exception e) {
          eventSink.log(MerklePainterEvent.ERROR_CLOSE, "text", "Failed to close transport client: " + e.getMessage());
        }
      }

      // 2. Compute any merkle tree hashes which are calculable (breadth-first)
      eventSink.log(MerklePainterEvent.SHUTDOWN_HASHING);
      try {
        // Force recalculation of hashes by accessing the root hash
        // This will trigger a breadth-first recalculation of all calculable hashes
        MerkleTree tree = pane.merkleTree();
        if (tree != null) {
          // Get the root hash, which will recalculate all calculable hashes
          tree.getHash(0);
        }
      } catch (Exception e) {
        eventSink.log(MerklePainterEvent.ERROR_HASH, "text", e.getMessage());
      }

      // 3. Flush the merkle tree data to disk
      eventSink.log(MerklePainterEvent.SHUTDOWN_FLUSHING);
      try {
        // Get the last modified time of the content file
        long contentLastModified = Files.getLastModifiedTime(localPath).toMillis();

        // Close the pane, which will close the merkle trees
        try {
          pane.close();
        } catch (IOException e) {
          eventSink.log(MerklePainterEvent.ERROR_CLOSE, "text", e.getMessage());
        }

        // Ensure the merkle tree file mtime is at least one millisecond newer than the content
        if (Files.exists(merklePath)) {
          long merkleLastModified = Files.getLastModifiedTime(merklePath).toMillis();
          if (merkleLastModified <= contentLastModified) {
            // Set the last modified time to be contentLastModified + 1
            Files.setLastModifiedTime(merklePath, 
                FileTime.fromMillis(contentLastModified + 1));
          }
        }
      } catch (IOException e) {
        eventSink.log(MerklePainterEvent.ERROR_SAVE, "text", e.getMessage());
      }

      eventSink.log(MerklePainterEvent.SHUTDOWN_COMPLETE, "path", localPath.toString());
    } catch (Exception e) {
      eventSink.log(MerklePainterEvent.ERROR_GENERAL, "text", e.getMessage());
    }
  }

  @Override
  public void close() throws IOException {
    // Execute the shutdown sequence
    handleShutdown();
  }
}
