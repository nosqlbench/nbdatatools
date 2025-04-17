package io.nosqlbench.vectordata.download.merkle;

import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import io.nosqlbench.vectordata.download.merkle.MerkleBits;

/// Provides windowed access to a file along with its associated Merkle tree.
/// Maintains an open random access channel to the file for efficient reading.
/// This class is primarily used to maintain a cohesive view of file state and associated merkle
/// tree state.
///
/// Apart form the initialization, during which it fetches a reference merkle tree,
/// It is effectively a passive data structure used by other classes to perform
/// more complex operations.
public class MerklePane implements Closeable {
  /// The merkle tree file extension for reference data, or the correct
  /// hash tree for the data, presumably fetched from remote first.
  public static final String MREF = ".mref";
  /// The merkle tree file extension for locally computed data
  /// that captures changing state and true-up status with respect to the reference.
  public static final String MRKL = ".mrkl";

  /// Path to the data file
  private final Path filePath;
  /// Path to the merkle tree file
  private final Path merklePath;
  /// The merkle tree for the file
  private final MerkleTree merkleTree;
  /// The file channel for reading the data file
  private final FileChannel channel;
  /// The size of the data file
  private final long fileSize;

  /// Track which chunks are intact (downloaded and verified)
  private final BitSet intactChunks;

  /// Listeners for changes to the intact chunks
  private final List<Consumer<BitSet>> intactChunksListeners = new CopyOnWriteArrayList<>();
  private final MerkleBits merkleBits;

  /// A merkle pane is a windowed view of a file and its associated merkle tree.
  /// @param filePath
  ///     The path to the data file
  public MerklePane(Path filePath) {
    this(filePath, filePath.resolveSibling(filePath.getFileName().toString() + MRKL));
  }

  /// Creates a new MerklePane for the given file and its associated Merkle tree
  /// @param filePath
  ///     Path to the data file
  /// @param merklePath
  ///     Path to the Merkle tree file
  public MerklePane(Path filePath, Path merklePath) {
    this(filePath, merklePath, null);
  }

  /// Creates a new MerklePane for the given file and its associated Merkle tree,
  /// with an optional reference merkle tree for initialization
  /// @param filePath
  ///     Path to the data file
  /// @param merklePath
  ///     Path to the Merkle tree file
  /// @param referenceTreePath
  ///     Path to the reference merkle tree file (may be null)
  public MerklePane(Path filePath, Path merklePath, Path referenceTreePath) {
    this(filePath, merklePath, referenceTreePath, null);
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
    this.filePath = filePath;
    this.merklePath = merklePath;
    try {
      // Download reference merkle tree if needed
      if (sourceUrl != null && (referenceTreePath == null || !Files.exists(referenceTreePath))) {
        referenceTreePath = downloadReferenceMerkleTree(sourceUrl, filePath);
      }

      // Initialize files if needed
      initializeFiles(filePath, merklePath, referenceTreePath);

      // Load the merkle tree first
      this.merkleTree = MerkleTree.load(merklePath);

      // Create the data file if it doesn't exist
      if (!Files.exists(filePath)) {
        Files.createFile(filePath);
      }

      // Open the data file for reading and writing
      this.channel = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE);
      this.fileSize = channel.size();

      // Initialize the intact chunks tracking
      int numChunks = (int) Math.ceil((double) merkleTree.totalSize() / merkleTree.getChunkSize());
      this.intactChunks = new BitSet(numChunks);
      this.merkleBits = new MerkleBits(intactChunks);

      // We no longer require the data file to match the merkle tree size
      // This allows for lazy loading of data
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /// Downloads a reference merkle tree file from a source URL
  /// @param sourceUrl
  ///     The source URL to download from
  /// @param filePath
  ///     The path to the data file (used to determine the reference tree path)
  /// @return The path to the downloaded reference merkle tree file
  /// @throws IOException
  ///     If there's an error downloading the file
  private Path downloadReferenceMerkleTree(String sourceUrl, Path filePath) throws IOException {
    // Determine the reference tree path
    Path referenceTreePath = filePath.resolveSibling(filePath.getFileName().toString() + MREF);

    // Create the URL for the merkle tree file
    URL merkleUrl;
    try {
      merkleUrl = new URL(sourceUrl + MRKL);
    } catch (java.net.MalformedURLException e) {
      throw new IOException("Invalid source URL: " + sourceUrl, e);
    }

    // Download the file
    try {
      // Create parent directories if needed
      Files.createDirectories(referenceTreePath.getParent());

      // Download the file
      try (InputStream in = merkleUrl.openStream();
           FileOutputStream out = new FileOutputStream(referenceTreePath.toFile()))
      {
        in.transferTo(out);
      }
    } catch (IOException e) {
      throw new IOException("Failed to download reference merkle tree from " + merkleUrl, e);
    }

    return referenceTreePath;
  }

  /// Initializes the necessary files for the MerklePane
  /// @param filePath
  ///     Path to the data file
  /// @param merklePath
  ///     Path to the merkle tree file
  /// @param referenceTreePath
  ///     Path to the reference merkle tree file (may be null)
  /// @throws IOException
  ///     If there's an error initializing the files
  private void initializeFiles(Path filePath, Path merklePath, Path referenceTreePath)
      throws IOException
  {
    // If we have a reference tree, use it to initialize the merkle file and data file
    if (referenceTreePath != null && Files.exists(referenceTreePath)) {
      // Create an empty merkle tree file if it doesn't exist or is empty
      if (!Files.exists(merklePath) || Files.size(merklePath) == 0) {
        // Create an empty merkle tree file with the same structure as the reference file
        MerkleTree.createEmptyTreeLike(referenceTreePath, merklePath);
      }

      // Get the total size from the reference merkle tree
      MerkleTree refTree = MerkleTree.load(referenceTreePath);
      long totalSize = refTree.totalSize();

      // Create the data file if it doesn't exist, but don't resize it
      // We no longer need to pre-allocate the entire file size
      if (!Files.exists(filePath)) {
        Files.createFile(filePath);
      }
    } else {
      // Without a reference tree, just check that the required files exist
      if (!Files.exists(filePath)) {
        throw new IOException("Data file does not exist: " + filePath);
      }

      if (!Files.exists(merklePath)) {
        throw new IOException("Merkle tree file does not exist: " + merklePath);
      }
    }
  }

  /// Gets the Merkle tree associated with this window
  /// @return The MerkleTree instance
  public MerkleTree merkleTree() {
    return merkleTree;
  }

  /// Gets the total size of the file
  /// @return The file size in bytes
  public long fileSize() {
    return fileSize;
  }

  /// Gets the path to the file
  /// @return The file path
  public Path filePath() {
    return filePath;
  }

  /// Reads a chunk of data from the file
  /// @param chunkIndex
  ///     The index of the chunk to read
  /// @return ByteBuffer containing the chunk data
  /// @throws IOException
  ///     If there's an error reading the file
  /// @throws IllegalArgumentException
  ///     If the chunk index is invalid
  public ByteBuffer readChunk(int chunkIndex) throws IOException {
    MerkleTree.NodeBoundary bounds = merkleTree.getBoundariesForLeaf(chunkIndex);
    long chunkSize = bounds.end() - bounds.start();

    // Check if the chunk is beyond the current file size
    if (bounds.start() >= fileSize) {
      // Return an empty buffer filled with zeros
      return ByteBuffer.allocate((int) chunkSize);
    }

    // Determine how much of the chunk we can actually read
    long availableBytes = Math.min(chunkSize, fileSize - bounds.start());

    // Create a buffer for the full chunk size
    ByteBuffer buffer = ByteBuffer.allocate((int) chunkSize);

    // Read the available data
    channel.position(bounds.start());
    channel.read(buffer.slice(0, (int) availableBytes));

    // The rest of the buffer remains filled with zeros
    buffer.position(0);
    buffer.limit((int) chunkSize);

    return buffer;
  }

  /// Reads a range of bytes from the file
  /// @param start
  ///     Starting byte offset
  /// @param length
  ///     Number of bytes to read
  /// @return ByteBuffer containing the requested data
  /// @throws IOException
  ///     If there's an error reading the file
  /// @throws IllegalArgumentException
  ///     If the range is invalid
  public ByteBuffer readRange(long start, int length) throws IOException {
    // Check for negative values
    if (start < 0) {
      throw new IllegalArgumentException(String.format(
          "Invalid negative start position: %d",
          start
      ));
    }

    if (length < 0) {
      throw new IllegalArgumentException(String.format("Invalid negative length: %d", length));
    }

    // Check if the range extends beyond the current file size
    // This maintains compatibility with existing tests
    if (start + length > fileSize) {
      throw new IllegalArgumentException(String.format(
          "Invalid range [%d, %d] for file of size %d",
          start,
          start + length,
          fileSize
      ));
    }

    // Create a buffer for the requested length
    ByteBuffer buffer = ByteBuffer.allocate(length);

    // Read the data
    channel.position(start);
    channel.read(buffer);
    buffer.flip();

    return buffer;
  }

  /// Verifies a specific chunk against the Merkle tree
  /// @param chunkIndex
  ///     The index of the chunk to verify
  /// @return true if the chunk matches the Merkle tree, false otherwise
  /// @throws IOException
  ///     If there's an error reading the file
  public boolean verifyChunk(int chunkIndex) throws IOException {
    ByteBuffer chunkData = readChunk(chunkIndex);

    // Hash the chunk data
    MessageDigest digest = merkleTree.getDigest();
    digest.reset();
    digest.update(chunkData);
    byte[] chunkHash = digest.digest();

    // Compare with the stored hash in the Merkle tree
    byte[] storedHash = merkleTree.getHashForLeaf(chunkIndex);

    boolean matches = Arrays.equals(chunkHash, storedHash);

    // If the chunk matches, mark it as intact
    if (matches) {
      synchronized (intactChunks) {
        intactChunks.set(chunkIndex);
      }

      // Notify listeners of the change
      notifyIntactChunksListeners();
    }

    return matches;
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }

  /**
   Gets the merkle tree associated with this pane.
   @return The merkle tree
   */
  public MerkleTree getMerkleTree() {
    return merkleTree;
  }

  /**
   Gets the path to the merkle tree file.
   @return The path to the merkle tree file
   */
  public Path getMerklePath() {
    return merklePath;
  }

  /**
   Accepts a chunk of data for a specific index and updates the merkle tree.
   This method writes the chunk data to the file and updates the merkle tree hash.
   @param chunkIndex
   The index of the chunk
   @param chunkData
   The data for the chunk
   @throws IOException
   If there's an error writing the data or updating the merkle tree
   */
  public void submitChunk(int chunkIndex, ByteBuffer chunkData) throws IOException {
    // Get the chunk boundaries
    MerkleTree.NodeBoundary bounds = merkleTree.getBoundariesForLeaf(chunkIndex);
    long start = bounds.start();
    long end = bounds.end();
    int length = (int) (end - start);

    // Ensure we got the expected amount of data
    if (chunkData.remaining() != length) {
      throw new IOException(
          "Chunk data size (" + chunkData.remaining() + ") does not match expected size (" + length
          + ")");
    }

    // Make a copy of the data for hashing (since we'll be writing it to the file)
    ByteBuffer hashData = ByteBuffer.allocate(chunkData.remaining());
    int originalPosition = chunkData.position();
    hashData.put(chunkData);
    chunkData.position(originalPosition); // Reset position for writing
    hashData.flip();

    // Write the chunk data to the file
    try (FileChannel writeChannel = FileChannel.open(filePath, StandardOpenOption.WRITE)) {
      writeChannel.position(start);
      writeChannel.write(chunkData);
    }

    // Hash the chunk data
    MessageDigest digest = merkleTree.getDigest();
    digest.reset();
    digest.update(hashData);
    byte[] chunkHash = digest.digest();

    // Update the merkle tree with the new hash
    merkleTree.updateLeafHash(chunkIndex, chunkHash, merklePath);

    // Mark the chunk as intact
    synchronized (intactChunks) {
      intactChunks.set(chunkIndex);
    }

    // Notify listeners of the change
    notifyIntactChunksListeners();
  }

  /// Gets a reference view of the intact chunks. This object may and should be retained and
  /// used, as it is a read-only view of the intact chunks and will be updated as needed.
  public BitSet getIntactChunks() {
    return merkleBits;
  }

  /**
   Checks if a specific chunk is intact (downloaded and verified).
   @param chunkIndex
   The index of the chunk to check
   @return true if the chunk is intact, false otherwise
   */
  public boolean isChunkIntact(int chunkIndex) {
    synchronized (intactChunks) {
      return intactChunks.get(chunkIndex);
    }
  }

  /**
   Adds a listener for changes to the intact chunks.
   @param listener
   The listener to add
   */
  public void addIntactChunksListener(Consumer<BitSet> listener) {
    intactChunksListeners.add(listener);
    // Notify the listener of the current state
    listener.accept(getIntactChunks());
  }

  /**
   Removes a listener for changes to the intact chunks.
   @param listener
   The listener to remove
   */
  public void removeIntactChunksListener(Consumer<BitSet> listener) {
    intactChunksListeners.remove(listener);
  }

  /**
   Notifies all listeners of changes to the intact chunks.
   */
  private void notifyIntactChunksListeners() {
    for (Consumer<BitSet> listener : intactChunksListeners) {
      listener.accept(merkleBits);
    }
  }
}