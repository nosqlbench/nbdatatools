package io.nosqlbench.vectordata.download.merkle;

import io.nosqlbench.vectordata.download.DownloadProgress;
import io.nosqlbench.vectordata.download.DownloadResult;
import io.nosqlbench.vectordata.download.NoOpDownloadEventSink;
import io.nosqlbench.vectordata.download.chunker.ChunkedDownloader;
import io.nosqlbench.vectordata.download.chunker.DownloadEventSink;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
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

import static io.nosqlbench.vectordata.download.merkle.MerklePane.MREF;
import static io.nosqlbench.vectordata.download.merkle.MerklePane.MRKL;

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
  private final DownloadEventSink eventSink;

  // Map to track in-progress download tasks by chunk index
  private final Map<Integer, CompletableFuture<Boolean>> downloadTasks = new ConcurrentHashMap<>();

  /// Creates a new MerklePainter for the given local file and source URL
  /// @param localPath
  ///     Path where the local data file exists or will be stored
  /// @param sourcePath
  ///     URL of the source data file (merkle tree will be downloaded from
  ///     sourcePath + [MerklePane#MRKL])
  /// @throws IOException
  ///     If there's an error opening the files or downloading reference data
  public MerklePainter(Path localPath, String sourcePath) {
    this(localPath, sourcePath, new NoOpDownloadEventSink());
  }

  /// Creates a new MerklePainter for the given local file and source URL with a custom event sink
  /// @param localPath
  ///     Path where the local data file exists or will be stored
  /// @param sourcePath
  ///     URL of the source data file (merkle tree will be downloaded from
  ///     sourcePath + [MerklePane#MRKL])
  /// @param eventSink
  ///     Event sink for logging download progress and events
  /// @throws IOException
  ///     If there's an error opening the files or downloading reference data
  public MerklePainter(Path localPath, String sourcePath, DownloadEventSink eventSink) {
    this.localPath = localPath;
    this.sourcePath = sourcePath;
    this.merklePath = localPath.resolveSibling(localPath.getFileName().toString() + MRKL);
    this.referenceTreePath = localPath.resolveSibling(localPath.getFileName().toString() + MREF);
    this.eventSink = eventSink;

    // Initialize the pane with local paths, reference tree path, and source URL
    // MerklePane will handle downloading the reference tree if needed
    this.pane = new MerklePane(localPath, merklePath, referenceTreePath, sourcePath);

    // Initialize the download executor with a fixed thread pool
    this.downloadExecutor = Executors.newFixedThreadPool(Math.min(8, Runtime.getRuntime().availableProcessors()));

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
    return pane.getMerkleTree().totalSize();
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

    // Create a temporary file to download the range
    Path tempFile = Files.createTempFile("merkle-range-", ".tmp");
    try {
      // Create a URL with range parameters
      URL rangeUrl;
      try {
        // We'll handle the range in the request headers
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
          eventSink
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
   * Asynchronously ensures that all chunks in the specified range are downloaded and verified.
   * This method returns immediately with a CompletableFuture that completes when all chunks are available.
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

      // Create a list of chunk indices to check
      List<Integer> chunkIndices = new ArrayList<>();
      for (int i = startChunk; i <= endChunk; i++) {
        chunkIndices.add(i);
      }

      // Download chunks if needed asynchronously
      return downloadChunksIfNeededAsync(chunkIndices).thenApply(result -> null);
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
   * Gets a set of chunk indices that are currently being downloaded.
   *
   * @return A set of chunk indices that are currently being downloaded
   */
  public Set<Integer> getInProgressChunks() {
    return downloadTasks.keySet();
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