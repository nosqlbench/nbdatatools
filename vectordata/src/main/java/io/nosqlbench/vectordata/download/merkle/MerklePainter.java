package io.nosqlbench.vectordata.download.merkle;

import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static io.nosqlbench.vectordata.download.merkle.MerklePane.MREF;
import static io.nosqlbench.vectordata.download.merkle.MerklePane.MRKL;

/// Combines a MerklePane with source URL functionality for downloading and verifying data.
/// Downloads and manages the reference merkle tree from a source URL.
public class MerklePainter implements Closeable {
  private final MerklePane pane;
  private final String sourcePath;
  private final Path localPath;
  private final Path merklePath;
  private final Path referenceTreePath;

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
  }
}