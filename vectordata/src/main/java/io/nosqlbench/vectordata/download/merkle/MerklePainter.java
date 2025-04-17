package io.nosqlbench.vectordata.download.merkle;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

  /// Creates a new MerklePainter for the given local file and source URL
  /// @param localPath
  ///     Path where the local data file exists or will be stored
  /// @param sourcePath
  ///     URL of the source data file (merkle tree will be downloaded from
  ///     sourcePath + [MerklePane#MRKL])
  /// @throws IOException
  ///     If there's an error opening the files or downloading reference data
  public MerklePainter(Path localPath, String sourcePath) {
    this.localPath = localPath;
    this.sourcePath = sourcePath;
    this.merklePath = localPath.resolveSibling(localPath.getFileName().toString() + MRKL);
    this.referenceTreePath = localPath.resolveSibling(localPath.getFileName().toString() + MREF);

    // Initialize the pane with local paths, reference tree path, and source URL
    // MerklePane will handle downloading the reference tree if needed
    this.pane = new MerklePane(localPath, merklePath, referenceTreePath, sourcePath);

    // Initialize the download executor with a fixed thread pool
    this.downloadExecutor = Executors.newFixedThreadPool(Math.min(8, Runtime.getRuntime().availableProcessors()));
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
   * Downloads a range of data from the remote source.
   *
   * @param start The starting position
   * @param length The number of bytes to download
   * @return A ByteBuffer containing the downloaded data
   * @throws IOException If there's an error downloading the data
   */
  public ByteBuffer downloadRange(long start, long length) throws IOException {
    // Create URL for the range request
    URL url;
    try {
      url = new URL(sourcePath);
    } catch (MalformedURLException e) {
      throw new IOException("Invalid source URL: " + sourcePath, e);
    }

    // Open connection and set range header
    java.net.URLConnection connection = url.openConnection();
    if (connection instanceof java.net.HttpURLConnection) {
      java.net.HttpURLConnection httpConnection = (java.net.HttpURLConnection) connection;
      httpConnection.setRequestProperty("Range", "bytes=" + start + "-" + (start + length - 1));
    }

    // Download the data
    try (InputStream in = connection.getInputStream()) {
      ByteBuffer buffer = ByteBuffer.allocate((int) length);
      byte[] temp = new byte[8192]; // 8KB buffer
      int bytesRead;
      int totalRead = 0;

      while ((bytesRead = in.read(temp)) != -1 && totalRead < length) {
        int bytesToCopy = (int) Math.min(bytesRead, length - totalRead);
        buffer.put(temp, 0, bytesToCopy);
        totalRead += bytesToCopy;
      }

      buffer.flip();
      return buffer;
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
   * This method is non-blocking if all chunks are already intact.
   *
   * @param startPosition The start position in the file
   * @param endPosition The end position in the file (exclusive)
   * @throws IOException If there's an error downloading or verifying the chunks
   */
  public void paint(long startPosition, long endPosition) throws IOException {
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

    // Download chunks if needed
    downloadChunksIfNeeded(chunkIndices);
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
    if (chunkIndices.isEmpty()) {
      return new ArrayList<>();
    }

    // Filter out chunks that are already intact
    List<Integer> chunksToDownload = new ArrayList<>();
    for (int chunkIndex : chunkIndices) {
      if (!pane.isChunkIntact(chunkIndex)) {
        chunksToDownload.add(chunkIndex);
      }
    }

    if (chunksToDownload.isEmpty()) {
      return new ArrayList<>(chunkIndices); // All chunks are already intact
    }

    // Download chunks in parallel
    List<CompletableFuture<Boolean>> futures = new ArrayList<>();

    for (int chunkIndex : chunksToDownload) {
      CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(() -> {
        try {
          return downloadAndSubmitChunk(chunkIndex);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }, downloadExecutor);

      futures.add(future);
    }

    // Wait for all downloads to complete
    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

      // Collect successful chunk indices
      List<Integer> successfulChunks = new ArrayList<>();
      for (int i = 0; i < chunkIndices.size(); i++) {
        int chunkIndex = chunkIndices.get(i);
        boolean success = futures.get(i).get();

        if (success) {
          successfulChunks.add(chunkIndex);
        }
      }

      return successfulChunks;
    } catch (Exception e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else {
        throw new IOException("Failed to download chunks", e);
      }
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

  /**
   * Downloads multiple chunks if they haven't been downloaded yet or if verification fails.
   *
   * @param chunkIndices The indices of the chunks to download
   * @return A list of chunk indices that were successfully downloaded or were already intact
   * @throws IOException If there's an error downloading or verifying the chunks
   */
  public List<Integer> downloadChunksIfNeeded(List<Integer> chunkIndices) throws IOException {
    if (chunkIndices.isEmpty()) {
      return new ArrayList<>();
    }

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
      return intactChunks;
    }

    // Download the chunks that need to be downloaded
    List<Integer> downloadedChunks = downloadAndSubmitChunks(chunksToDownload);

    // Combine the intact chunks with the downloaded chunks
    List<Integer> result = new ArrayList<>(intactChunks);
    result.addAll(downloadedChunks);
    return result;
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