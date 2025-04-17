package io.nosqlbench.vectordata.download.merkle;

import io.nosqlbench.vectordata.layout.manifest.DSView;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.concurrent.CompletableFuture;

/**
 * A RandomAccessFile implementation that uses MerklePainter to verify and download data as needed.
 * This class acts as a shell around its parent RandomAccessFile, presenting the size from the
 * reference merkle file and ensuring that content is downloaded and verified before any reads.
 */
public class MerkleRAF extends RandomAccessFile {
  private final MerklePainter painter;
  private final long virtualSize; // The total size according to the merkle tree

  /**
   * Creates a new MerkleRAF with a local path and source URL.
   *
   * @param localPath Path where the local data file exists or will be stored
   * @param remoteUrl URL of the source data file
   * @throws IOException If there's an error opening the files or downloading reference data
   */
  public MerkleRAF(Path localPath, String remoteUrl) throws IOException {
    super(touch(localPath), "rwd");
    // Create a MerklePainter with the given parameters
    this.painter = new MerklePainter(localPath, remoteUrl);
    this.virtualSize = painter.totalSize();

    // No initialization needed for intact chunks tracking
  }

  private static String touch(Path localPath) {
    try {
      Files.createDirectories(localPath.getParent());
      if (!Files.exists(localPath)) {
        Files.createFile(localPath);
      }
      return localPath.toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns the length of this file as reported by the merkle tree.
   * This may be different from the actual file length if not all chunks have been downloaded.
   *
   * @return The virtual length of the file
   */
  @Override
  public long length() {
    return virtualSize;
  }

  /**
   * Sets the file-pointer offset at which the next read or write occurs.
   * Also ensures that the chunk containing the new position is available.
   *
   * @param pos The offset position, measured in bytes from the beginning of the file
   * @throws IOException If an I/O error occurs or if pos is less than 0
   */
  @Override
  public void seek(long pos) throws IOException {
    if (pos < 0) {
      throw new IOException("Negative seek position: " + pos);
    }

    if (pos > virtualSize) {
      throw new IOException("Seek position beyond file size: " + pos + " > " + virtualSize);
    }

    // Ensure the chunk containing this position is available
    painter.paint(pos, pos + 1);

    // Perform the seek operation
    super.seek(pos);
  }

  /**
   * Reads a byte of data from this file. Also ensures that the chunk containing
   * the current position is available.
   *
   * @return The next byte of data, or -1 if the end of the file is reached
   * @throws IOException If an I/O error occurs
   */
  @Override
  public int read() throws IOException {
    // Get the current position
    long pos = getFilePointer();

    // Ensure the chunk containing this position is available
    painter.paint(pos, pos + 1);

    // Perform the read operation
    return super.read();
  }

  /**
   * Reads up to b.length bytes of data from this file. Also ensures that all chunks
   * containing the requested data are available.
   *
   * @param b The buffer into which the data is read
   * @return The total number of bytes read into the buffer, or -1 if the end of the file is reached
   * @throws IOException If an I/O error occurs
   */
  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  /**
   * Reads up to len bytes of data from this file. Also ensures that all chunks
   * containing the requested data are available.
   *
   * @param b The buffer into which the data is read
   * @param off The start offset in the buffer at which the data is written
   * @param len The maximum number of bytes to read
   * @return The total number of bytes read into the buffer, or -1 if the end of the file is reached
   * @throws IOException If an I/O error occurs
   */
  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    long currentPos = getFilePointer();

    // Ensure all chunks containing the requested data are available
    long endPos = currentPos + len - 1;
    painter.paint(currentPos, endPos + 1);

    // Perform the read operation
    return super.read(b, off, len);
  }

  /**
   * Asynchronously prebuffers a range of bytes by ensuring all chunks containing the range are downloaded.
   * This method does not block and returns immediately with a CompletableFuture that completes when
   * all chunks in the range are available.
   *
   * @param position The starting position in the file
   * @param length The number of bytes to prebuffer
   * @return A CompletableFuture that completes when all chunks in the range are available
   */
  public CompletableFuture<Void> prebuffer(long position, long length) {
    if (position < 0) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      future.completeExceptionally(new IOException("Negative position: " + position));
      return future;
    }

    if (length < 0) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      future.completeExceptionally(new IOException("Negative length: " + length));
      return future;
    }

    if (position >= virtualSize) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      future.completeExceptionally(new IOException("Position beyond file size: " + position + " >= " + virtualSize));
      return future;
    }

    // Adjust length if it would go beyond the end of the file
    long endPosition = Math.min(position + length, virtualSize);

    // Delegate to the painter's paintAsync method
    return painter.paintAsync(position, endPosition);
  }

  /**
   * Closes this random access file and releases any system resources associated with it.
   * Also closes the associated MerklePainter.
   *
   * @throws IOException If an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    // First close the RandomAccessFile
    super.close();

    // Close the painter
    if (painter != null) {
      painter.close();
    }
  }
}
