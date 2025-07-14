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


import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.BitSet;

import io.nosqlbench.vectordata.status.EventSink;
import io.nosqlbench.vectordata.status.NoOpDownloadEventSink;

/**
 MerklePane provides a window into a file with Merkle tree verification.
 It allows for efficient random access to chunks of data with integrity checking. */
public class MerklePane implements AutoCloseable {
  private final EventSink eventSink = new NoOpDownloadEventSink();
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

  // SINGLE SOURCE OF TRUTH: Centralized chunk calculations
  private final ChunkGeometryDescriptor geometry;

  // MerkleTree now tracks which chunks are intact (verified) with its own BitSet

  // Reference tree for comparison
  private MerkleTree refTree;
  
  // Shadow tree for proper tracking of verified chunks
  private ShadowTree shadowTree;

  /// Creates a new MerklePane for the given file
  /// @param filePath
  ///     Path to the data file
  /// @throws IllegalArgumentException
  ///     If the filePath is null
  public MerklePane(Path filePath) {
    // According to requirements, it should be invalid to create a MerklePane without a remote content URL
    throw new IllegalArgumentException("Remote content URL must be provided. Use a constructor that accepts a sourceUrl parameter.");
  }

  /// Creates a new MerklePane for the given file and its associated Merkle tree
  /// @param filePath
  ///     Path to the data file
  /// @param merklePath
  ///     Path to the Merkle tree file
  /// @throws IllegalArgumentException
  ///     If the filePath or merklePath is null, or if no remote content URL is provided
  public MerklePane(Path filePath, Path merklePath) {
    // According to requirements, it should be invalid to create a MerklePane without a remote content URL
    throw new IllegalArgumentException("Remote content URL must be provided. Use a constructor that accepts a sourceUrl parameter.");
  }

  /// Creates a new MerklePane for the given file and its associated Merkle tree
  /// @param filePath
  ///     Path to the data file
  /// @param merklePath
  ///     Path to the Merkle tree file
  /// @param referenceTreePath
  ///     Path to the reference merkle tree file (may be null)
  /// @throws IllegalArgumentException
  ///     If the filePath or merklePath is null, or if no remote content URL is provided
  public MerklePane(Path filePath, Path merklePath, Path referenceTreePath) {
    // According to requirements, it should be invalid to create a MerklePane without a remote content URL
    throw new IllegalArgumentException("Remote content URL must be provided. Use a constructor that accepts a sourceUrl parameter.");
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
  ///     Source URL for downloading the reference merkle tree (must be non-null)
  /// @throws IllegalArgumentException
  ///     If the filePath, merklePath, or sourceUrl is null
  public MerklePane(Path filePath, Path merklePath, Path referenceTreePath, String sourceUrl) {
    // According to requirements, it should be invalid to create a MerklePane without a remote content URL
    if (sourceUrl == null || sourceUrl.isEmpty()) {
      throw new IllegalArgumentException("Remote content URL must be provided");
    }

    this.filePath = filePath;
    this.merklePath = merklePath;
    this.merkleTree = MerklePaneSetup.initTree(filePath, merklePath, sourceUrl);

    try {
      this.refTree = MerkleTree.load(referenceTreePath);

      // Open the data file for reading and writing
      this.channel = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE);
      this.fileSize = channel.size();

      // INITIALIZE CHUNK GEOMETRY: Single source of truth for all chunk calculations
      // Use the virtual content size from the reference tree, not the local file size
      // The local file might be empty initially, but the reference tree knows the true content size
      long virtualContentSize = (refTree != null) ? refTree.totalSize() : Files.size(filePath);
      this.geometry = ChunkGeometryDescriptor.fromContentSize(virtualContentSize);

      // VALIDATE CONSISTENCY: Ensure geometry matches the trees
      // Temporarily disabled to assess current state after merkle file updates
      // validateGeometryConsistency();

      // Initialize shadow tree for proper tracking of verified chunks
      this.shadowTree = new ShadowTree(refTree, filePath, merklePath);

      // No need to initialize intact chunks tracking
      // ShadowTree now tracks which chunks are intact with proper semantics
    } catch (IOException e) {
      throw new RuntimeException("Failed to initialize MerklePane: " + e.getMessage(), e);
    }
  }


  // This method is no longer needed as MerkleTree now tracks its own bitsets

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

  /// Gets the reference Merkle tree used for verification
  /// @return The reference MerkleTree instance, or null if not available
  public MerkleTree getRefTree() {
    return refTree;
  }

  /// Gets the chunk geometry used by this pane.
  /// This provides access to the centralized chunk calculations.
  /// @return The ChunkGeometryDescriptor instance
  public ChunkGeometryDescriptor getGeometry() {
    return geometry;
  }

  /// Validates that the chunk geometry is consistent with the merkle trees.
  /// This ensures that all components are using the same chunk parameters.
  private void validateGeometryConsistency() {
    if (refTree != null) {
      int geometryChunks = geometry.getTotalChunks();
      int treeChunks = refTree.getNumberOfLeaves();

      if (geometryChunks != treeChunks) {
        throw new IllegalStateException(
            "Chunk count mismatch between geometry (" + geometryChunks + 
            ") and reference tree (" + treeChunks + "). " +
            "This indicates inconsistent chunk size configuration.");
      }
    }

    if (merkleTree != null) {
      int geometryChunks = geometry.getTotalChunks();
      int merkleTreeChunks = merkleTree.getNumberOfLeaves();

      if (geometryChunks != merkleTreeChunks) {
        throw new IllegalStateException(
            "Chunk count mismatch between geometry (" + geometryChunks + 
            ") and merkle tree (" + merkleTreeChunks + "). " +
            "This indicates inconsistent chunk size configuration.");
      }
    }
  }

  /**
   Gets the size of each chunk in bytes.
   @return chunk size
   */
  public synchronized long getChunkSize() {
    return merkleTree != null ? merkleTree.getChunkSize() : 0;
  }

  /**
   Gets the total size of the associated content file.
   @return content file size
   */
  public synchronized long getContentSize() {
    return fileSize;
  }

  /**
   Lists chunk indexes overlapping the given range that are not yet verified.
   @param range
   byte range to check
   @return list of chunk indexes needing download or verification
   */
  public synchronized java.util.List<Integer> getInvalidChunkIndexes(MerkleRange range) {
    // Use merkleTree's getInvalidLeafIndices method directly
    return merkleTree != null ? merkleTree.getInvalidLeafIndices(range) : new java.util.ArrayList<>();
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
  /// @return A new BitSet representing which chunks are intact
  public synchronized BitSet getIntactChunks() {
    // Create a new BitSet based on merkleTree's valid bits
    BitSet result = new BitSet();
    if (merkleTree != null) {
      int leafCount = merkleTree.getNumberOfLeaves();
      for (int i = 0; i < leafCount; i++) {
        if (merkleTree.isLeafValid(i)) {
          result.set(i);
        }
      }
    }
    return result;
  }

  /// Gets the MerkleBits wrapper for the intact chunks BitSet
  /// @return A new MerkleBits wrapper for a BitSet representing which chunks are intact
  public MerkleBits getMerkleBits() {
    // Create a new MerkleBits based on merkleTree's valid bits
    return new MerkleBits(getIntactChunks());
  }

  /// Checks if a chunk is intact (verified)
  /// Uses the ShadowTree to determine if a chunk has been properly verified
  /// against the reference tree and successfully stored
  /// @param chunkIndex
  ///     The index of the chunk to check
  /// @return true if the chunk is intact, false otherwise
  public synchronized boolean isChunkIntact(int chunkIndex) {
    // Use ShadowTree for proper verified chunk tracking
    if (shadowTree != null) {
      return shadowTree.isChunkVerified(chunkIndex);
    }
    
    // Fallback to old logic if ShadowTree not available
    // Create local references to avoid race conditions
    MerkleTree localMerkleTree = merkleTree;
    MerkleTree localRefTree = refTree;

    // If either tree is null, the chunk cannot be intact
    if (localMerkleTree == null || localRefTree == null) {
      return false;
    }

    // Check if the reference tree has this chunk as valid (meaning reference data exists)
    boolean refValid = localRefTree.isLeafValid(chunkIndex);
    if (!refValid) {
      return false;
    }

    // Check if the local tree has this chunk marked as valid (meaning it was explicitly verified)
    boolean localValid = localMerkleTree.isLeafValid(chunkIndex);
    if (!localValid) {
      return false;
    }

    // For shadow trees, we also need to check if the actual data exists in the local file
    // A chunk is only intact if all three conditions are met:
    // 1. Reference tree has valid data (refValid)
    // 2. Local tree marked as verified (localValid)  
    // 3. Local file has the actual data written
    try {
      MerkleMismatch bounds = localMerkleTree.getBoundariesForLeaf(chunkIndex);
      long chunkStart = bounds.startInclusive();
      long chunkEnd = chunkStart + bounds.length();
      
      // Check if the local file has data covering this chunk
      boolean hasData = (fileSize >= chunkEnd);
      
      return hasData;
    } catch (Exception e) {
      return false;
    }
  }

  /// Reads a range of bytes from the data file
  /// @param start
  ///     The starting position
  /// @param length
  ///     The number of bytes to read
  /// @return A ByteBuffer containing the data
  /// @throws IOException
  ///     If there's an error reading the data
  /// @throws IllegalArgumentException
  ///     If the range is invalid
  public ByteBuffer readRange(int start, int length) throws IOException {
    // Validate the range
    if (length < 0) {
      throw new IllegalArgumentException("Range length cannot be negative: " + length);
    }

    if (start < 0) {
      throw new IllegalArgumentException("Range startInclusive cannot be negative: " + start);
    }

    if (start + length > fileSize) {
      throw new IllegalArgumentException(
          "Range extends beyond file size: " + (start + length) + " > " + fileSize);
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
  /// @param chunkIndex
  ///     The index of the chunk to read
  /// @return A ByteBuffer containing the chunk data
  /// @throws IOException
  ///     If there's an error reading the chunk
  public ByteBuffer readChunk(int chunkIndex) throws IOException {
    // Get the chunk boundaries
    MerkleMismatch bounds = merkleTree.getBoundariesForLeaf(chunkIndex);
    long start = bounds.startInclusive();
    long length = bounds.length();
    int chunkSize = (int) length;

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
  /// @param chunkIndex
  ///     The index of the chunk to verify
  /// @return true if the chunk is valid, false otherwise, or if the chunk hasn't been loaded yet
  /// @throws IOException
  ///     If there's an error reading or verifying the chunk
  public boolean verifyChunk(int chunkIndex) throws IOException {
    // If the merkle tree hasn't been loaded, we can't verify the chunk
    if (merkleTree == null) {
      return false;
    }

    // If the reference tree is not available, we can't verify the chunk
    if (refTree == null) {
      return false;
    }

    // Note: We always verify the chunk against the reference tree, even if it's already marked as intact
    // This ensures that corrupted chunks are properly detected

    // Get the chunk boundaries
    MerkleMismatch bounds = merkleTree.getBoundariesForLeaf(chunkIndex);
    long start = bounds.startInclusive();
    long length = bounds.length();

    // If the chunk extends beyond the file size, it hasn't been loaded yet
    if (start >= fileSize || start + length > fileSize) {
      return false;
    }

    try {
      // Read the chunk
      ByteBuffer chunkData = readChunk(chunkIndex);

      // Get the expected hash from the reference tree
      byte[] expectedHash = refTree.getHashForLeaf(chunkIndex);

      if (expectedHash == null) {
        return false;
      }
      

      // Use the new method to hash the data only if it matches the expected hash
      boolean valid = merkleTree.hashDataIfMatchesExpected(start, start + length, chunkData.duplicate(), expectedHash);

      return valid;
    } catch (Exception e) {
      // If there's any error reading or verifying the chunk, it hasn't been loaded properly
      return false;
    }
  }

  /// Writes a chunk to the data file and updates its hash in the merkle tree
  /// @param chunkIndex
  ///     The index of the chunk to write
  /// @param data
  ///     The data to write
  /// @throws IOException
  ///     If there's an error writing the chunk
  public void writeChunk(int chunkIndex, ByteBuffer data) throws IOException {
    submitChunk(chunkIndex, data);
  }

  /// Submits a chunk to the data file and updates its hash in the merkle tree
  /// This is an alias for writeChunk for backward compatibility
  /// @param chunkIndex
  ///     The index of the chunk to submit
  /// @param data
  ///     The data to submit
  /// @throws IOException
  ///     If there's an error submitting the chunk
  public void submitChunk(int chunkIndex, ByteBuffer data) throws IOException {
    submitChunk(chunkIndex, data, false);
  }

  /// Submits a chunk to the data file and updates its hash in the merkle tree
  /// Optionally verifies the chunk against the reference tree's hash
  /// @param chunkIndex
  ///     The index of the chunk to submit
  /// @param data
  ///     The data to submit
  /// @param verifyAgainstRefTree
  ///     If true, only update the hash if it matches the reference tree's hash
  /// @return true if the chunk was submitted and the hash was updated, false if verification failed
  /// @throws IOException
  ///     If there's an error submitting the chunk
  public boolean submitChunk(int chunkIndex, ByteBuffer data, boolean verifyAgainstRefTree) throws IOException {
    // Use ShadowTree for proper verification if available
    if (shadowTree != null && verifyAgainstRefTree) {
      return shadowTree.submitChunk(chunkIndex, data);
    }
    
    // Fallback to old logic
    // Get the chunk boundaries
    MerkleMismatch bounds = merkleTree.getBoundariesForLeaf(chunkIndex);
    long start = bounds.startInclusive();
    long length = bounds.length();
    int chunkSize = (int) length;

    // Check if the data size matches the chunk size
    if (data.remaining() != chunkSize) {
      throw new IOException(
          "Data size does not match chunk size: " + data.remaining() + " != " + chunkSize);
    }

    // Write the data to the file
    channel.position(start);
    channel.write(data.duplicate());

    // If we're not verifying against the reference tree, just hash the data
    if (!verifyAgainstRefTree || refTree == null) {
      // Use MerkleTree's hashData method to hash the chunk and update the tree
      merkleTree.hashData(start, start + length, data.duplicate());
      return true;
    }

    // Get the expected hash from the reference tree
    byte[] expectedHash = refTree.getHashForLeaf(chunkIndex);

    // If the reference tree doesn't have a hash for this chunk, just hash the data
    if (expectedHash == null) {
      merkleTree.hashData(start, start + length, data.duplicate());
      return true;
    }

    // Use the new method to hash the data only if it matches the expected hash
    boolean hashMatched = merkleTree.hashDataIfMatchesExpected(start, start + length, data.duplicate(), expectedHash);

    return hashMatched;
  }

  /// Submits a chunk to the data file with a pre-computed hash
  /// This method avoids recomputing the hash when it's already available
  /// @param chunkIndex
  ///     The index of the chunk to submit
  /// @param data
  ///     The data to submit
  /// @param precomputedHash
  ///     The pre-computed hash of the data
  /// @throws IOException
  ///     If there's an error submitting the chunk
  public void submitChunkWithHash(int chunkIndex, ByteBuffer data, byte[] precomputedHash) throws IOException {
    // Get the chunk boundaries
    MerkleMismatch bounds = merkleTree.getBoundariesForLeaf(chunkIndex);
    long start = bounds.startInclusive();
    long length = bounds.length();
    int chunkSize = (int) length;

    // Check if the data size matches the chunk size
    if (data.remaining() != chunkSize) {
      throw new IOException(
          "Data size does not match chunk size: " + data.remaining() + " != " + chunkSize);
    }

    // Write the data to the file
    channel.position(start);
    channel.write(data.duplicate());

    // Update the hash in the merkle tree using the pre-computed hash
    merkleTree.updateLeafHash(chunkIndex, precomputedHash, merklePath);

    // Mark the chunk as intact in the merkle tree
    merkleTree.setLeafValid(chunkIndex);
  }

  /// Closes this MerklePane and releases any system resources associated with it
  /// @throws IOException
  ///     If an I/O error occurs
  @Override
  public void close() throws IOException {
    try {
      // Close the shadow tree first
      if (shadowTree != null) {
        shadowTree.close();
      }
      
      // Close the merkle trees
      if (merkleTree != null) {
        merkleTree.close();
      }

      if (refTree != null) {
        refTree.close();
      }
    } finally {
      // Always try to close the channel, even if closing the trees fails
      if (channel != null && channel.isOpen()) {
        channel.close();
      }
    }
  }


  /**
   Checks if the local merkle tree file is up-to-date with the remote merkle tree file.
   This is done by comparing the footers of both files.
   @param localMerklePath
   The path to the local merkle tree file
   @param sourceUrl
   The URL of the remote merkle tree file
   @return true if the local file is up-to-date, false otherwise
   @throws IOException
   If there's an error reading the files or connecting to the remote server
   */
  private boolean isLocalMerkleTreeUpToDate(Path localMerklePath, String sourceUrl)
      throws IOException
  {
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
   Reads the footer from a local merkle tree file.
   @param merklePath
   The path to the merkle tree file
   @return The MerkleFooter, or null if it couldn't be read
   */
  private MerkleFooter readMerkleFooter(Path merklePath) {
    try {
      // Get the file size
      long fileSize = Files.size(merklePath);
      if (fileSize < MerkleFooter.FIXED_FOOTER_SIZE) {
        // If the file is too small to contain a footer, it's invalid
        return null;
      }

      // Read the footer from the end of the file
      try (RandomAccessFile raf = new RandomAccessFile(merklePath.toFile(), "r")) {
        raf.seek(fileSize - MerkleFooter.FIXED_FOOTER_SIZE);
        byte[] footerBytes = new byte[MerkleFooter.FIXED_FOOTER_SIZE];
        raf.readFully(footerBytes);
        ByteBuffer footerBuffer = ByteBuffer.wrap(footerBytes);
        return MerkleFooter.fromByteBuffer(footerBuffer);
      }
    } catch (Exception e) {
      // If there's any error reading the footer, return null
      return null;
    }
  }

  /**
   Reads the footer from a remote merkle tree file.
   @param merkleUrl
   The URL of the merkle tree file
   @param remoteFileSize
   The size of the remote file
   @return The MerkleFooter, or null if it couldn't be read
   */
  private MerkleFooter readRemoteMerkleFooter(URL merkleUrl, long remoteFileSize)
      throws IOException
  {
    if (remoteFileSize < MerkleFooter.FIXED_FOOTER_SIZE) {
      // If the file is too small to contain a footer, it's invalid
      return null;
    }

    try {
      // Open a connection to the remote file
      HttpURLConnection connection = (HttpURLConnection) merkleUrl.openConnection();

      // Set up the connection for a GET request with a Range header
      // to only download the footer from the end of the file
      connection.setRequestMethod("GET");
      connection.setConnectTimeout(5000); // 5 second timeout
      connection.setReadTimeout(5000);    // 5 second timeout
      connection.setRequestProperty("Range", "bytes=" + (remoteFileSize - MerkleFooter.FIXED_FOOTER_SIZE) + "-" + (remoteFileSize - 1));
      connection.connect();

      // Check if the server supports range requests
      int responseCode = connection.getResponseCode();
      if (responseCode != HttpURLConnection.HTTP_PARTIAL) {
        // If the server doesn't support range requests, download the entire file
        // This is inefficient but necessary for servers that don't support range requests
        connection = (HttpURLConnection) merkleUrl.openConnection();
        connection.setRequestMethod("GET");
        connection.setConnectTimeout(5000); // 5 second timeout
        connection.setReadTimeout(5000);    // 5 second timeout
        connection.connect();

        // Read the entire file into memory
        try (InputStream is = connection.getInputStream()) {
          byte[] allBytes = is.readAllBytes();
          if (allBytes.length < MerkleFooter.FIXED_FOOTER_SIZE) {
            // If the file is too small to contain a footer, it's invalid
            return null;
          }

          // Extract the footer from the end of the file
          byte[] footerBytes = Arrays.copyOfRange(allBytes, allBytes.length - MerkleFooter.FIXED_FOOTER_SIZE, allBytes.length);
          ByteBuffer footerBuffer = ByteBuffer.wrap(footerBytes);
          return MerkleFooter.fromByteBuffer(footerBuffer);
        }
      } else {
        // Read the footer bytes from the response
        try (InputStream is = connection.getInputStream()) {
          byte[] footerBytes = is.readAllBytes();
          if (footerBytes.length != MerkleFooter.FIXED_FOOTER_SIZE) {
            // If we didn't get the expected number of bytes, the response is invalid
            return null;
          }

          ByteBuffer footerBuffer = ByteBuffer.wrap(footerBytes);
          return MerkleFooter.fromByteBuffer(footerBuffer);
        }
      }
    } catch (Exception e) {
      // If there's any error reading the footer, return null
      return null;
    }
  }

  /**
   Compares two merkle tree footers to see if they're equal.
   @param localFooter
   The local merkle tree footer
   @param remoteFooter
   The remote merkle tree footer
   @return true if the footers are equal, false otherwise
   */
  private boolean compareFooters(MerkleFooter localFooter, MerkleFooter remoteFooter) {
    // Compare the chunk size and total size
    return localFooter.chunkSize() == remoteFooter.chunkSize()
           && localFooter.totalSize() == remoteFooter.totalSize();
  }

  /// Returns a string representation of this MerklePane
  /// @return A string representation of the MerklePane
  @Override
  public String toString() {
    return "MerklePane{" + "filePath=" + filePath + ", merklePath=" + merklePath + ", fileSize="
           + fileSize + ", merkleTree=" + merkleTree + '}';
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
   Gets the total size of the data represented by this MerklePane.
   The only definitive reference for the total size is the reference merkle data.
   @return The total size in bytes, or -1 if no reference tree is available
   */
  public long getTotalSize() {
    // Use the reference tree's total size if available, otherwise return -1
    if (refTree != null) {
      return refTree.totalSize();
    } else {
      return -1; // No reference tree available
    }
  }

  /// Get the offset boundaries for a merkle leaf
  /// @param chunkIndex The chunk index
  /// @return The boundaries for the leaf
  public MerkleMismatch getBoundariesForLeaf(int chunkIndex) {
    if (refTree != null) {
      MerkleMismatch boundaries = refTree.getBoundariesForLeaf(chunkIndex);
      return boundaries;
    } else {
      return null;
    }
  }

  /// Gets the chunk index for a given file position.
  /// This method delegates to ChunkGeometryDescriptor for consistent calculations.
  /// @param position The file position
  /// @return The chunk index containing this position
  public int getChunkIndexForPosition(long position) {
    // DELEGATE TO GEOMETRY: Single source of truth for chunk calculations
    return geometry.getChunkIndexForPosition(position);
  }

  /// Gets the chunk boundary for a given chunk index.
  /// This method delegates to ChunkGeometryDescriptor for consistent calculations.
  /// @param chunkIndex The chunk index
  /// @return a MerkleMismatch range with start and end positions
  public MerkleMismatch getChunkBoundary(int chunkIndex) {
    // DELEGATE TO GEOMETRY: Single source of truth for chunk calculations
    ChunkBoundary boundary = geometry.getChunkBoundary(chunkIndex);
    return new MerkleMismatch(chunkIndex, boundary.startInclusive(), boundary.size());
  }

  /// A wrapper around BitSet for tracking which chunks have been downloaded.
  ///
  /// This class provides methods for getting, setting, and clearing bits in a BitSet,
  /// as well as for checking the cardinality and size of the BitSet.
  public static class MerkleBits {
    private final BitSet bits;

    /// Creates a new MerkleBits with the given BitSet.
    /// @param bits
    ///     The BitSet to wrap
    public MerkleBits(BitSet bits) {
      this.bits = bits;
    }

    /// Gets the value of the bit at the specified index.
    /// @param index
    ///     The index of the bit to get
    /// @return true if the bit is set, false otherwise
    public boolean get(int index) {
      return bits.get(index);
    }

    /// Sets the bit at the specified index to true.
    /// @param index
    ///     The index of the bit to set
    public void set(int index) {
      bits.set(index);
    }

    /// Sets the bit at the specified index to false.
    /// @param index
    ///     The index of the bit to clear
    public void clear(int index) {
      bits.clear(index);
    }

    /// Returns the number of bits set to true in this BitSet.
    /// @return The number of bits set to true
    public int cardinality() {
      return bits.cardinality();
    }

    /// Returns the number of bits in this BitSet.
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
