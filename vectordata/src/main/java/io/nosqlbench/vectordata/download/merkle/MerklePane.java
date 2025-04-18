package io.nosqlbench.vectordata.download.merkle;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;
import java.util.Arrays;

/**
 * MerklePane provides a window into a file with Merkle tree verification.
 * It allows for efficient random access to chunks of data with integrity checking.
 */
public class MerklePane implements AutoCloseable {
  // File extensions for merkle tree files
  /// File extension for merkle tree files
  public static final String MRKL = ".mrkl";
  /// File extension for reference merkle tree files
  public static final String MREF = ".mref";

  // The merkle tree for this pane
  private final MerkleTree merkleTree;

  // The data file and its channel
  private final Path filePath;
  private final Path merklePath;
  private final FileChannel channel;
  private final long fileSize;

  // Tracking which chunks are intact (verified)
  private final BitSet intactChunks;
  private final MerkleBits merkleBits;

  /// Creates a new MerklePane for the given file
  /// @param filePath
  ///     Path to the data file
  public MerklePane(Path filePath) {
    // For backward compatibility, throw an exception if the file doesn't exist
    if (!Files.exists(filePath)) {
      throw new RuntimeException("Data file does not exist: " + filePath);
    }

    // Initialize with default values for merklePath, referenceTreePath, and sourceUrl
    Path merklePath = filePath.resolveSibling(filePath.getFileName().toString() + MRKL);

    // Initialize fields
    this.filePath = filePath;
    this.merklePath = merklePath;

    // Initialize the merkle tree
    this.merkleTree = initializePane(filePath, merklePath, null, null);

    try {
      // Open the data file for reading and writing
      this.channel = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE);
      this.fileSize = channel.size();

      // Initialize the intact chunks tracking
      int numChunks = (int) Math.ceil((double) merkleTree.totalSize() / merkleTree.getChunkSize());
      this.intactChunks = new BitSet(numChunks);
      this.merkleBits = new MerkleBits(intactChunks);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /// Creates a new MerklePane for the given file and its associated Merkle tree
  /// @param filePath
  ///     Path to the data file
  /// @param merklePath
  ///     Path to the Merkle tree file
  public MerklePane(Path filePath, Path merklePath) {
    // Initialize fields
    this.filePath = filePath;
    this.merklePath = merklePath;

    // Initialize the merkle tree
    this.merkleTree = initializePane(filePath, merklePath, null, null);

    try {
      // Open the data file for reading and writing
      this.channel = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE);
      this.fileSize = channel.size();

      // Initialize the intact chunks tracking
      int numChunks = (int) Math.ceil((double) merkleTree.totalSize() / merkleTree.getChunkSize());
      this.intactChunks = new BitSet(numChunks);
      this.merkleBits = new MerkleBits(intactChunks);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /// Creates a new MerklePane for the given file and its associated Merkle tree
  /// @param filePath
  ///     Path to the data file
  /// @param merklePath
  ///     Path to the Merkle tree file
  /// @param referenceTreePath
  ///     Path to the reference merkle tree file (may be null)
  public MerklePane(Path filePath, Path merklePath, Path referenceTreePath) {
    // Initialize fields
    this.filePath = filePath;
    this.merklePath = merklePath;

    // Initialize the merkle tree
    this.merkleTree = initializePane(filePath, merklePath, referenceTreePath, null);

    try {
      // Open the data file for reading and writing
      this.channel = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE);
      this.fileSize = channel.size();

      // Initialize the intact chunks tracking
      int numChunks = (int) Math.ceil((double) merkleTree.totalSize() / merkleTree.getChunkSize());
      this.intactChunks = new BitSet(numChunks);
      this.merkleBits = new MerkleBits(intactChunks);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /// Creates a new MerklePane for the given file and its associated Merkle tree,
  /// with an optional source URL for downloading the reference merkle tree
  /// @param filePath
  ///     Path to the data file
  /// @param merklePath
  ///     Path to the Merkle tree file
  /// @param referenceTreePath
  ///     Path to the reference merkle tree file (may be null)
  /// @param sourceUrl
  ///     Source URL for downloading the reference merkle tree (may be null)
  public MerklePane(Path filePath, Path merklePath, Path referenceTreePath, String sourceUrl) {
    // Initialize fields
    this.filePath = filePath;
    this.merklePath = merklePath;

    // Initialize the merkle tree
    this.merkleTree = initializePane(filePath, merklePath, referenceTreePath, sourceUrl);

    try {
      // Open the data file for reading and writing
      this.channel = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE);
      this.fileSize = channel.size();

      // Initialize the intact chunks tracking
      int numChunks = (int) Math.ceil((double) merkleTree.totalSize() / merkleTree.getChunkSize());
      this.intactChunks = new BitSet(numChunks);
      this.merkleBits = new MerkleBits(intactChunks);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Initializes the MerklePane with the given parameters.
   * This method contains the common initialization logic for all constructors.
   *
   * @param filePath Path to the data file
   * @param merklePath Path to the merkle tree file
   * @param referenceTreePath Path to the reference merkle tree file (may be null)
   * @param sourceUrl Source URL for downloading the reference merkle tree (may be null)
   * @return The initialized MerkleTree
   */
  private MerkleTree initializePane(Path filePath, Path merklePath, Path referenceTreePath, String sourceUrl) {
    try {
      if (filePath==null) {
        throw new IOException("Data file path cannot be null");
      }
      Files.createDirectories(filePath.getParent());

      if (merklePath==null) {
        throw new IOException("Merkle tree path cannot be null");
      }
      Files.createDirectories(merklePath.getParent());

      // Only create directories for the reference tree path if it's not null
      if (referenceTreePath != null) {
        Files.createDirectories(referenceTreePath.getParent());
      }

      // Rule 1: Ensure that the local reference merkle tree (MREF file) is current with the remote reference merkle tree file.
      Path actualReferenceTreePath = referenceTreePath;
      boolean needToDownload = false;

      if (sourceUrl != null && (actualReferenceTreePath == null || !Files.exists(actualReferenceTreePath))) {
        // Determine the reference tree path if not provided
        if (actualReferenceTreePath == null) {
          actualReferenceTreePath = filePath.resolveSibling(filePath.getFileName().toString() + MREF);
        }
        needToDownload = true;
      } else if (sourceUrl != null && actualReferenceTreePath != null && Files.exists(actualReferenceTreePath)) {
        // Check if the local file is up-to-date with the remote file
        try {
          if (!isLocalMerkleTreeUpToDate(actualReferenceTreePath, sourceUrl)) {
            needToDownload = true;
          }
        } catch (Exception e) {
          // If there's any error during the comparison, download the file to be safe
          needToDownload = true;
        }
      }

      if (needToDownload && sourceUrl != null) {
        // Download the reference tree
        URL merkleUrl;
        try {
          merkleUrl = new URL(sourceUrl + MRKL);
        } catch (java.net.MalformedURLException e) {
          throw new IOException("Invalid source URL: " + sourceUrl, e);
        }

        // Create parent directories if needed
        Files.createDirectories(actualReferenceTreePath.getParent());

        // Download the file
        try (InputStream in = merkleUrl.openStream();
             FileOutputStream out = new FileOutputStream(actualReferenceTreePath.toFile()))
        {
          in.transferTo(out);
        } catch (IOException e) {
          throw new IOException("Failed to download reference merkle tree from " + merkleUrl, e);
        }
      }

      // Rule 2: Ensure that the local merkle tree file is present, or if it is not, create one with the createEmptyTreeLike(...) method
      if (actualReferenceTreePath != null && Files.exists(actualReferenceTreePath)) {
        if (!Files.exists(merklePath) || Files.size(merklePath) == 0) {
          // Create an empty merkle tree file with the same structure as the reference file
          MerkleTree.createEmptyTreeLike(actualReferenceTreePath, merklePath);
        }
      } else if (!Files.exists(merklePath)) {
        // If no reference tree is available and the merkle tree doesn't exist, throw an error
        throw new IOException("Merkle tree file does not exist and no reference tree available: " + merklePath);
      }

      // Rule 3: Ensure that at least the first chunk of the content file exists.
      // If the content file is not present, create an empty file for it.
      if (!Files.exists(filePath)) {
        Files.createFile(filePath);
      } else {
        // If the content file exists, check its timestamp relative to the merkle tree file
        long contentLastModified = Files.getLastModifiedTime(filePath).toMillis();
        long merkleLastModified = Files.getLastModifiedTime(merklePath).toMillis();

        // If content file is newer than the merkle tree file, refresh the merkle tree file
        if (contentLastModified > merkleLastModified) {
          // Load the reference tree to get chunk size and total size
          MerkleTree refTree = null;
          if (actualReferenceTreePath != null && Files.exists(actualReferenceTreePath)) {
            refTree = MerkleTree.load(actualReferenceTreePath);
          } else {
            // If no reference tree, use the existing merkle tree
            refTree = MerkleTree.load(merklePath);
          }

          long chunkSize = refTree.getChunkSize();
          long totalSize = refTree.totalSize();

          // Create a new merkle tree from the content file
          try (FileChannel contentChannel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            long fileSize = contentChannel.size();
            // Use the smaller of the file size or the total size from the reference tree
            long effectiveSize = Math.min(fileSize, totalSize);

            // Map the file into memory for efficient reading
            ByteBuffer fileData = ByteBuffer.allocate((int)effectiveSize);
            contentChannel.read(fileData);
            fileData.flip();

            // Create a new merkle tree from the file data
            MerkleTree newTree = MerkleTree.fromData(fileData, chunkSize, new MerkleRange(0, effectiveSize));

            // Save the new tree to the merkle file
            newTree.save(merklePath);
          }
        }
        // If content file is same timestamp or older than merkle tree file, just load them both as is
      }

      // Load the merkle tree
      MerkleTree merkleTree = MerkleTree.load(merklePath);

      // We no longer require the data file to match the merkle tree size
      // This allows for lazy loading of data
      return merkleTree;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /// Gets the Merkle tree associated with this window
  /// @return The MerkleTree instance
  public MerkleTree getMerkleTree() {
    return merkleTree;
  }

  /// Gets the Merkle tree associated with this window
  /// @return The MerkleTree instance
  public MerkleTree merkleTree() {
    return merkleTree;
  }

  /// Gets the data file path
  /// @return The data file path
  public Path getFilePath() {
    return filePath;
  }

  /// Gets the merkle tree file path
  /// @return The merkle tree file path
  public Path getMerklePath() {
    return merklePath;
  }

  /// Gets the file channel for the data file
  /// @return The file channel
  public FileChannel getChannel() {
    return channel;
  }

  /// Gets the size of the data file
  /// @return The file size
  public long getFileSize() {
    return fileSize;
  }

  /// Gets the size of the data file (alias for getFileSize)
  /// @return The file size
  public long fileSize() {
    return fileSize;
  }

  /// Gets the data file path (alias for getFilePath)
  /// @return The data file path
  public Path filePath() {
    return filePath;
  }

  /// Gets the BitSet tracking which chunks are intact
  /// @return The intact chunks BitSet
  public BitSet getIntactChunks() {
    return intactChunks;
  }

  /// Gets the MerkleBits wrapper for the intact chunks BitSet
  /// @return The MerkleBits wrapper
  public MerkleBits getMerkleBits() {
    return merkleBits;
  }

  /// Checks if a chunk is intact (verified)
  /// @param chunkIndex The index of the chunk to check
  /// @return true if the chunk is intact, false otherwise
  public boolean isChunkIntact(int chunkIndex) {
    return intactChunks.get(chunkIndex);
  }

  /// Reads a range of bytes from the data file
  /// @param start The starting position
  /// @param length The number of bytes to read
  /// @return A ByteBuffer containing the data
  /// @throws IOException If there's an error reading the data
  /// @throws IllegalArgumentException If the range is invalid
  public ByteBuffer readRange(int start, int length) throws IOException {
    // Validate the range
    if (length < 0) {
      throw new IllegalArgumentException("Range length cannot be negative: " + length);
    }

    if (start < 0) {
      throw new IllegalArgumentException("Range start cannot be negative: " + start);
    }

    if (start + length > fileSize) {
      throw new IllegalArgumentException("Range extends beyond file size: " + (start + length) + " > " + fileSize);
    }

    // Allocate a buffer for the data
    ByteBuffer buffer = ByteBuffer.allocate(length);

    // Read the data from the file
    channel.position(start);
    int bytesRead = channel.read(buffer);

    // Check if we read the expected number of bytes
    if (bytesRead < length) {
      // If the file is smaller than expected, pad with zeros
      buffer.position(bytesRead);
      buffer.put(new byte[length - bytesRead]);
    }

    // Prepare the buffer for reading
    buffer.flip();
    return buffer;
  }

  /// Reads a chunk from the data file
  /// @param chunkIndex The index of the chunk to read
  /// @return A ByteBuffer containing the chunk data
  /// @throws IOException If there's an error reading the chunk
  public ByteBuffer readChunk(int chunkIndex) throws IOException {
    // Get the chunk boundaries
    MerkleTree.NodeBoundary bounds = merkleTree.getBoundariesForLeaf(chunkIndex);
    long start = bounds.start();
    long end = bounds.end();
    int chunkSize = (int) (end - start);

    // Allocate a buffer for the chunk
    ByteBuffer buffer = ByteBuffer.allocate(chunkSize);

    // Read the chunk from the file
    channel.position(start);
    int bytesRead = channel.read(buffer);

    // Check if we read the expected number of bytes
    if (bytesRead < chunkSize) {
      // If the file is smaller than expected, pad with zeros
      buffer.position(bytesRead);
      buffer.put(new byte[chunkSize - bytesRead]);
    }

    // Prepare the buffer for reading
    buffer.flip();
    return buffer;
  }

  /// Verifies a chunk against its hash in the merkle tree
  /// @param chunkIndex The index of the chunk to verify
  /// @return true if the chunk is valid, false otherwise, or if the chunk hasn't been loaded yet
  /// @throws IOException If there's an error reading or verifying the chunk
  public boolean verifyChunk(int chunkIndex) throws IOException {
    // If the chunk is already marked as intact, return true
    if (intactChunks.get(chunkIndex)) {
      return true;
    }

    // Get the chunk boundaries
    MerkleTree.NodeBoundary bounds = merkleTree.getBoundariesForLeaf(chunkIndex);
    long start = bounds.start();
    long end = bounds.end();

    // If the chunk extends beyond the file size, it hasn't been loaded yet
    if (start >= fileSize || end > fileSize) {
      return false;
    }

    try {
      // Read the chunk
      ByteBuffer chunkData = readChunk(chunkIndex);

      // Get the expected hash from the merkle tree
      byte[] expectedHash = merkleTree.getHashForLeaf(chunkIndex);

      // Calculate the hash of the chunk
      byte[] actualHash;
      try {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update(chunkData.duplicate());
        actualHash = digest.digest();
      } catch (NoSuchAlgorithmException e) {
        return false; // If we can't calculate the hash, the chunk isn't valid
      }

      // Compare the hashes
      boolean valid = MessageDigest.isEqual(expectedHash, actualHash);

      // If the chunk is valid, mark it as intact
      if (valid) {
        intactChunks.set(chunkIndex);
      }

      return valid;
    } catch (Exception e) {
      // If there's any error reading or verifying the chunk, it hasn't been loaded properly
      return false;
    }
  }

  /// Writes a chunk to the data file and updates its hash in the merkle tree
  /// @param chunkIndex The index of the chunk to write
  /// @param data The data to write
  /// @throws IOException If there's an error writing the chunk
  public void writeChunk(int chunkIndex, ByteBuffer data) throws IOException {
    submitChunk(chunkIndex, data);
  }

  /// Submits a chunk to the data file and updates its hash in the merkle tree
  /// This is an alias for writeChunk for backward compatibility
  /// @param chunkIndex The index of the chunk to submit
  /// @param data The data to submit
  /// @throws IOException If there's an error submitting the chunk
  public void submitChunk(int chunkIndex, ByteBuffer data) throws IOException {
    // Get the chunk boundaries
    MerkleTree.NodeBoundary bounds = merkleTree.getBoundariesForLeaf(chunkIndex);
    long start = bounds.start();
    long end = bounds.end();
    int chunkSize = (int) (end - start);

    // Check if the data size matches the chunk size
    if (data.remaining() != chunkSize) {
      throw new IOException("Data size does not match chunk size: " + data.remaining() + " != " + chunkSize);
    }

    // Write the data to the file
    channel.position(start);
    channel.write(data.duplicate());

    // Calculate the hash of the chunk
    byte[] hash;
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(data.duplicate());
      hash = digest.digest();
    } catch (NoSuchAlgorithmException e) {
      throw new IOException("Failed to create message digest", e);
    }

    // Update the hash in the merkle tree
    merkleTree.updateLeafHash(chunkIndex, hash, merklePath);

    // Mark the chunk as intact
    intactChunks.set(chunkIndex);
  }

  /// Closes this MerklePane and releases any system resources associated with it
  /// @throws IOException If an I/O error occurs
  @Override
  public void close() throws IOException {
    if (channel != null && channel.isOpen()) {
      channel.close();
    }
  }



  /**
   * Checks if the local merkle tree file is up-to-date with the remote merkle tree file.
   * This is done by comparing the footers of both files.
   *
   * @param localMerklePath The path to the local merkle tree file
   * @param sourceUrl The URL of the remote merkle tree file
   * @return true if the local file is up-to-date, false otherwise
   * @throws IOException If there's an error reading the files or connecting to the remote server
   */
  private boolean isLocalMerkleTreeUpToDate(Path localMerklePath, String sourceUrl) throws IOException {
    if (!Files.exists(localMerklePath)) {
      return false;
    }

    try {
      // Create the URL for the remote merkle tree file
      URL merkleUrl = new URL(sourceUrl + MRKL);
      HttpURLConnection connection = (HttpURLConnection) merkleUrl.openConnection();

      // Set up the connection for a HEAD request to get the file size
      connection.setRequestMethod("HEAD");
      connection.setConnectTimeout(5000); // 5 second timeout
      connection.setReadTimeout(5000);    // 5 second timeout
      connection.connect();

      // Get the remote file size
      long remoteFileSize = connection.getContentLengthLong();
      if (remoteFileSize <= 0) {
        // If we can't determine the remote file size, assume it's not up-to-date
        return false;
      }

      // Get the local file size
      long localFileSize = Files.size(localMerklePath);
      if (localFileSize != remoteFileSize) {
        // If the file sizes are different, the local file is not up-to-date
        return false;
      }

      // For small files (less than 1MB), just compare the file sizes
      // This is a performance optimization to avoid unnecessary footer comparison
      if (localFileSize < 1024 * 1024) {
        return true;
      }

      // Read the footer from the local file
      MerkleFooter localFooter = readMerkleFooter(localMerklePath);
      if (localFooter == null) {
        return false;
      }

      // Read the footer from the remote file
      MerkleFooter remoteFooter = readRemoteMerkleFooter(merkleUrl, remoteFileSize);
      if (remoteFooter == null) {
        return false;
      }

      // Compare the footers
      return compareFooters(localFooter, remoteFooter);
    } catch (Exception e) {
      // If there's any error, assume the local file is not up-to-date
      return false;
    }
  }

  /**
   * Reads the footer from a local merkle tree file.
   *
   * @param merklePath The path to the merkle tree file
   * @return The MerkleFooter, or null if it couldn't be read
   */
  private MerkleFooter readMerkleFooter(Path merklePath) {
    try {
      // Get the file size
      long fileSize = Files.size(merklePath);
      if (fileSize < MerkleFooter.FIXED_FOOTER_SIZE + MerkleFooter.DIGEST_SIZE) {
        // File is too small to have a valid footer
        return null;
      }

      // Read the footer length (last byte)
      byte footerLength;
      try (FileChannel channel = FileChannel.open(merklePath, StandardOpenOption.READ)) {
        ByteBuffer lengthBuffer = ByteBuffer.allocate(1);
        channel.position(fileSize - 1);
        channel.read(lengthBuffer);
        lengthBuffer.flip();
        footerLength = lengthBuffer.get();
      }

      // Read the entire footer
      ByteBuffer footerBuffer = ByteBuffer.allocate(footerLength);
      try (FileChannel channel = FileChannel.open(merklePath, StandardOpenOption.READ)) {
        channel.position(fileSize - footerLength);
        channel.read(footerBuffer);
        footerBuffer.flip();
      }

      // Parse the footer
      return MerkleFooter.fromByteBuffer(footerBuffer);
    } catch (Exception e) {
      // If there's any error, return null
      return null;
    }
  }

  /**
   * Reads the footer from a remote merkle tree file.
   *
   * @param merkleUrl The URL of the merkle tree file
   * @param remoteFileSize The size of the remote file
   * @return The MerkleFooter, or null if it couldn't be read
   */
  private MerkleFooter readRemoteMerkleFooter(URL merkleUrl, long remoteFileSize) {
    try {
      // Determine how much of the file to read
      // We'll read the last 1KB or the entire file, whichever is smaller
      int readSize = (int) Math.min(1024, remoteFileSize);
      long startPosition = remoteFileSize - readSize;

      // Set up the connection for a ranged GET request
      HttpURLConnection connection = (HttpURLConnection) merkleUrl.openConnection();
      connection.setRequestMethod("GET");
      connection.setRequestProperty("Range", "bytes=" + startPosition + "-" + (remoteFileSize - 1));
      connection.setConnectTimeout(5000); // 5 second timeout
      connection.setReadTimeout(5000);    // 5 second timeout
      connection.connect();

      // Check if the server supports range requests
      int responseCode = connection.getResponseCode();
      if (responseCode != 206) { // 206 Partial Content
        // Server doesn't support range requests or there was an error
        return null;
      }

      // Read the data
      ByteBuffer buffer = ByteBuffer.allocate(readSize);
      try (InputStream in = connection.getInputStream()) {
        byte[] temp = new byte[readSize];
        int bytesRead = in.read(temp);
        if (bytesRead > 0) {
          buffer.put(temp, 0, bytesRead);
        }
      }
      buffer.flip();

      // Make sure we read enough data
      if (buffer.remaining() < MerkleFooter.FIXED_FOOTER_SIZE + MerkleFooter.DIGEST_SIZE) {
        return null;
      }

      // Read the footer length (last byte)
      buffer.position(buffer.limit() - 1);
      byte footerLength = buffer.get();
      buffer.position(0);

      // Validate the footer length
      if (footerLength <= 0 || footerLength > readSize) {
        // Invalid footer length
        return null;
      }

      // Position the buffer to the start of the footer
      buffer.position(readSize - footerLength);
      ByteBuffer footerBuffer = ByteBuffer.allocate(footerLength);
      footerBuffer.put(buffer);
      footerBuffer.flip();

      // Parse the footer
      return MerkleFooter.fromByteBuffer(footerBuffer);
    } catch (Exception e) {
      // If there's any error, return null
      return null;
    }
  }

  /**
   * Compares two MerkleFooter objects to see if they represent the same merkle tree.
   *
   * @param localFooter The local merkle tree footer
   * @param remoteFooter The remote merkle tree footer
   * @return true if the footers match, false otherwise
   */
  private boolean compareFooters(MerkleFooter localFooter, MerkleFooter remoteFooter) {
    if (localFooter == null || remoteFooter == null) {
      return false;
    }

    // Compare chunk size and total size
    if (localFooter.chunkSize() != remoteFooter.chunkSize() ||
        localFooter.totalSize() != remoteFooter.totalSize()) {
      return false;
    }

    // Compare digests
    return Arrays.equals(localFooter.digest(), remoteFooter.digest());
  }

  /// Returns the MerklePane as a string
  /// @return A string representation of the MerklePane
  @Override
  public String toString() {
    return "MerklePane{" +
        "filePath=" + filePath +
        ", merklePath=" + merklePath +
        ", fileSize=" + fileSize +
        ", merkleTree=" + merkleTree +
        '}';
  }

  /// A wrapper around BitSet for tracking which chunks have been downloaded.
  ///
  /// This class provides methods for getting, setting, and clearing bits in a BitSet,
  /// as well as for checking the cardinality and size of the BitSet.
  public static class MerkleBits {
    private final BitSet bits;

    /// Creates a new MerkleBits with the given BitSet.
    ///
    /// @param bits The BitSet to wrap
    public MerkleBits(BitSet bits) {
      this.bits = bits;
    }

    /// Gets the value of the bit at the specified index.
    ///
    /// @param index The index of the bit to get
    /// @return true if the bit is set, false otherwise
    public boolean get(int index) {
      return bits.get(index);
    }

    /// Sets the bit at the specified index to true.
    ///
    /// @param index The index of the bit to set
    public void set(int index) {
      bits.set(index);
    }

    /// Sets the bit at the specified index to false.
    ///
    /// @param index The index of the bit to clear
    public void clear(int index) {
      bits.clear(index);
    }

    /// Returns the number of bits set to true in this BitSet.
    ///
    /// @return The number of bits set to true
    public int cardinality() {
      return bits.cardinality();
    }

    /// Returns the number of bits in this BitSet.
    ///
    /// @return The number of bits
    public int size() {
      return bits.size();
    }

    @Override
    public String toString() {
      return bits.toString();
    }
  }
}
