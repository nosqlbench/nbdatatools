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


import io.nosqlbench.vectordata.status.StdoutDownloadEventSink;
import io.nosqlbench.vectordata.downloader.ChunkedDownloader;
import io.nosqlbench.vectordata.status.EventSink;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okio.BufferedSource;
import okio.Buffer;

import java.io.IOException;
import java.nio.ByteBuffer;

import java.io.Closeable;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
  private final MerklePane pane;
  private final String sourcePath;
  private final Path localPath;
  private final Path merklePath;
  private final Path referenceTreePath;
  private final OkHttpClient client = new OkHttpClient();


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
  ///             sourcePath + [MerklePane#MRKL])
  public MerklePainter(Path localPath, String sourcePath) {
    this(localPath, sourcePath, new StdoutDownloadEventSink());
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
        this.chunkedDownloader =
            new ChunkedDownloader(url, fileName, 1024 * 1024, parallelism, eventSink);
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
   Gets the MerklePane used by this painter.
   @return The MerklePane instance
   */
  public MerklePane pane() {
    return pane;
  }

  /**
   Gets the total size of the file as reported by the merkle tree.
   @return The total size in bytes
   */
  public long totalSize() {
    return pane.getTotalSize();
  }

  /// Uses the pane to determine what chunks need to be fetched, and then submits them to the
  /// pane for reconciliation. This call blocks until the downloads have been done and the pane
  /// is updated.
  /// @param startIncl The start value inclusive
  /// @param endExcl The end value exclusive
  public void paint(long startIncl, long endExcl) {
    try {
      paintAsync(startIncl, endExcl).get(); // Call the asynchronous method and wait for completion
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Error during paint operation", e);
    }
  }


  /// Uses the pane to determine what chunks need to be fetched, and then submits them to the
  /// pane for reconciliation. This call returns a DownloadProgress that can be used to check the
  ///  status of an active download or otherwise synchronously wait for the result.
  /// @param startIncl the start value inclusive
  /// @param endExcl the end value exclusive
  /// @return a completable future of Void
  ///
  public CompletableFuture<Void> paintAsync(long startIncl, long endExcl) {
    return CompletableFuture.runAsync(() -> {
      try {
        long totalSize = pane.getTotalSize(); // Get the total size from the pane
        if (totalSize <= 0) {
          return; // Or handle as appropriate for an empty file
        }

        int startChunk = pane.getChunkIndexForPosition(startIncl);
        int endChunk = pane.getChunkIndexForPosition(Math.min(endExcl - 1, totalSize - 1));

        for (int chunkIndex = startChunk; chunkIndex <= endChunk; chunkIndex++) {
          if (!pane.isChunkIntact(chunkIndex)) {
            downloadAndSubmitChunk(chunkIndex);
          }
        }
      } catch (Exception e) {
        eventSink.error("Error painting chunks: " + e.getMessage());
        // Handle or rethrow the exception as needed
      }
    });
  }

  boolean downloadAndSubmitChunk(int chunkIndex) {
    try {
      // Assumes MerklePane has methods to provide chunk boundaries
      MerkleMismatch boundary = pane.getChunkBoundary(chunkIndex); // Placeholder, replace with
      // your
      // actual method call
      long start = boundary.startInclusive();
      long length = boundary.endExclusive() - start;
      ByteBuffer chunkData = downloadRange(start, length);

      if (chunkData != null) {
        pane.submitChunk(chunkIndex, chunkData);
        return true;
      } else {
        eventSink.error("Failed to download chunk " + chunkIndex);
        return false;
      }
    } catch (Exception e) {
      eventSink.error("Error downloading or submitting chunk " + chunkIndex + ": " + e.getMessage());
      return false; // Or re-throw the exception depending on desired behavior
    }

  }

  private ByteBuffer downloadRange(long start, long length) {
    try {

      Request request = new Request.Builder()
          .url(sourcePath)
          .addHeader("Range", "bytes=" + start + "-" + (start + length - 1))
          .get().build();


      try (Response response = client.newCall(request).execute()) {
        if (response.isSuccessful() || response.code()==206) {
          try (BufferedSource source = response.body().source()) {
            Buffer buffer = new Buffer();
            source.readAll(buffer);
            return ByteBuffer.wrap(buffer.readByteArray());
          }
        } else {
          String errorBody = "";
          try {
            if (response.body() != null) {
              errorBody = response.body().string();
            }
          } catch (Exception ignored) {
            // Ignore any errors reading the body
          }
          String errorMessage = "Error downloading range " + start + "-" + (start + length - 1) +
                          ", status code: " + response.code() + 
                          (errorBody.isEmpty() ? "" : ": " + errorBody);
          eventSink.error(errorMessage);
          throw new IOException(errorMessage);
        }
      }
    } catch (Exception e) {
      String errorMessage = "Error downloading range " + start + "-" + (start + length - 1) + ": " + e.getMessage();
      eventSink.error(errorMessage);
      throw new RuntimeException(errorMessage, e);
    }
  }

  /// Await all download streams which are pending before unblocking the caller.
  /// @throws InterruptedException if the current thread is interrupted while waiting
  public void awaitAllDownloads() throws InterruptedException {
    while (downloadTasks.values().stream().anyMatch(f -> !f.isDone())) {
      Thread.sleep(100); // Check periodically
    }

    // Check for exceptions in completed futures and rethrow if any
    for (CompletableFuture<Boolean> future : downloadTasks.values()) {
      try {
        future.get(); // This will rethrow any exception that occurred during the download
      } catch (ExecutionException e) {
        if (e.getCause() instanceof IOException) {
          throw new RuntimeException("An IO exception occurred while waiting for downloads", e.getCause());
        } else {
          throw new RuntimeException("An unexpected exception occurred while waiting for downloads", e.getCause());
        }
      }
    }
    downloadTasks.clear(); // Clear the completed tasks
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
        throw new RuntimeException("An IO exception occurred while waiting for downloads", e.getCause());
      } else {
        throw new RuntimeException("An unexpected exception occurred while waiting for downloads", e.getCause());
      }
    }
  }


  /**
   Record class to represent a transfer of a Merkle node.
   */
  private record NodeTransfer(int merkleNodeIndex, long byteRangeStartInclusive, long byteRangeEndExclusive) {
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


  @Override
  public void close() throws IOException {
    pane.close();

    // Shut down the download executor
    if (downloadExecutor != null && !downloadExecutor.isShutdown()) {
      downloadExecutor.shutdown();
    }
  }
}
