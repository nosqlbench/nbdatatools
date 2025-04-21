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



// Removed OkHttp dependencies to allow standard URLConnection for test stubbing

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.nio.file.StandardCopyOption;

/**
 MerkleTree stores all node hashes in a flat array and tracks staleness in a BitSet.
 It supports lazy recomputation of internal nodes when accessed. */

/// # REQUIREMENTS:
///
/// This merkle tree implementation stores all hashes in a flat array. It tracks whether a hash
/// for a node is stale with a bit set. When a merkle tree is loaded from disk, the bitset should
///  be set ot all true, indicating that all hashes are valid. However, when a chunk of data is
/// presented with a [MerkleMismatch] instance, the bitset should be set to false for the
/// affected nodes all the way to the root node.
///
/// The hash values for a node should only be updated when accessed, meaning it is possible to
/// update multipel leaf nodes and have the hash values be in some incorrect state while not
/// being observed. The bitset is responsible for tracking dirty hash values. When hash values
/// are updated right before access, the indices of the affected nodes should be marked as valid
/// after the path from the root to all affected leaves are updated with correct values.
///
/// Right before saving, the root hash should be accessed to force this computation, and the
/// bitset should be verified to be all true, i.e. all hashes are valid and computed.
///
/// When a merkle tree is saved to disk, it must have all of the hashes computed correctly first.
/// This also means that all the valid bits should have already been set.
/// Before the merkle tree is written to disk, the root node should be accessed to force all
/// hashes to be computed according to which ones are invalid according to the bitset.
/// Then, the digest of all hash values should be computed, and then stored in the footer of the
/// written file.
/// Then when the file is read back into a new merkle tree instance, the digest must still pass
/// to show that the file is representing the same base content.
///
/// The hashes are stored in root-first order, meaning that the leaf nodes start somewhere on the
///  interior of the hash data. This is represented by the offset value.

public class MerkleTree {
  /// SHA-256 hash size in bytes
  public static final int HASH_SIZE = 32;
  /// Shared MessageDigest instance for SHA-256 hashing
  private static final MessageDigest DIGEST;

  static {
    try {
      DIGEST = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  private final byte[][] hashes;
  private final BitSet valid;
  private final int leafCount;
  private final int capLeaf;
  private final int offset;
  private final long chunkSize;
  private final long totalSize;

  /**
   Builds a Merkle tree from raw data buffer.
   All nodes are fully computed and marked valid.
   */
  public static MerkleTree fromData(ByteBuffer data, long chunkSize, MerkleRange range) {
    // Validate chunkSize: must be positive power-of-two
    if (chunkSize <= 0 || Long.bitCount(chunkSize) != 1) {
      throw new IllegalArgumentException("Chunk size must be a power of two: " + chunkSize);
    }
    int leafCount = (int) Math.ceil((double) range.length() / chunkSize);
    int cap = 1;
    while (cap < leafCount)
      cap <<= 1;
    int nodeCount = 2 * cap - 1;
    byte[][] hashes = new byte[nodeCount][HASH_SIZE];
    BitSet valid = new BitSet(nodeCount);
    int offset = cap - 1;
    // compute leaf hashes (respecting range start offset)
    for (int i = 0; i < cap; i++) {
      ByteBuffer slice;
      if (i < leafCount) {
        long byteStart = range.start() + i * chunkSize;
        int start = (int) byteStart;
        int len = (int) Math.min(chunkSize, range.length() - i * chunkSize);
        // extract slice without modifying original buffer
        ByteBuffer dup = data.duplicate();
        dup.position(start).limit(start + len);
        slice = dup.slice();
      } else {
        slice = ByteBuffer.allocate(0);
      }
      DIGEST.reset();
      DIGEST.update(slice);
      hashes[offset + i] = DIGEST.digest();
      valid.set(offset + i);
    }
    // compute internal nodes
    for (int idx = offset - 1; idx >= 0; idx--) {
      int left = 2 * idx + 1;
      int right = left + 1;
      DIGEST.reset();
      DIGEST.update(hashes[left]);
      if (right < nodeCount)
        DIGEST.update(hashes[right]);
      hashes[idx] = DIGEST.digest();
      valid.set(idx);
    }
    return new MerkleTree(hashes, valid, leafCount, cap, offset, chunkSize, range.length());
  }

  /**
   Creates an empty (all stale) Merkle tree for given size.
   */
  public static MerkleTree createEmpty(long totalSize, long chunkSize) {
    // Validate chunkSize: must be positive power-of-two
    if (chunkSize <= 0 || Long.bitCount(chunkSize) != 1) {
      throw new IllegalArgumentException("Chunk size must be a power of two: " + chunkSize);
    }
    int leafCount = (int) Math.ceil((double) totalSize / chunkSize);
    int cap = 1;
    while (cap < leafCount)
      cap <<= 1;
    int nodeCount = 2 * cap - 1;
    byte[][] hashes = new byte[nodeCount][HASH_SIZE];
    BitSet valid = new BitSet(nodeCount);
    int offset = cap - 1;
    return new MerkleTree(hashes, valid, leafCount, cap, offset, chunkSize, totalSize);
  }

  private MerkleTree(
      byte[][] hashes,
      BitSet valid,
      int leafCount,
      int capLeaf,
      int offset,
      long chunkSize,
      long totalSize
  )
  {
    this.hashes = hashes;
    this.valid = valid;
    this.leafCount = leafCount;
    this.capLeaf = capLeaf;
    this.offset = offset;
    this.chunkSize = chunkSize;
    this.totalSize = totalSize;
  }

  /**
   Creates a new empty Merkle tree file with the same structure (chunk size and total size)
   as an existing Merkle tree file.
   @param merkleFile
   The source Merkle tree file to copy structure from
   @param emptyMerkleFile
   The path where the new empty Merkle tree file will be created
   @throws IOException
   If there is an error reading the source file or writing the target file
   */
  public static void createEmptyTreeLike(Path merkleFile, Path emptyMerkleFile) throws IOException {
    // Read the footer from the source file to get chunk size and total size
    long fileSize = Files.size(merkleFile);
    ByteBuffer footerBuffer = readFooterBuffer(merkleFile, fileSize);

    // Create a MerkleFooter object from the buffer
    MerkleFooter footer = MerkleFooter.fromByteBuffer(footerBuffer);

    // Extract chunk size and total size
    long chunkSize = footer.chunkSize();
    long totalSize = footer.totalSize();

    // Create a new empty tree with the same parameters
    MerkleTree emptyTree = createEmpty(totalSize, chunkSize);

    // Save the empty tree to the target file
    emptyTree.save(emptyMerkleFile);
  }

  /// This method is used to fetch a remote merkle tree file from a url
  /// to a local file. It occurs in a few steps:
  /// 1. The size of the remote merkle tree is determined by a head request with okhttp
  /// 2. The last up to 1k of the tree is fetched, and the end is decoded as a [MerkleFooter]
  /// 3. If the local file exists already and has the same size and has the same footer
  /// contents (determined by MerkleFooter.equals), then the local file is left as is.
  /// 4. In all other cases, the local file is downloaded again.
  /**
   Synchronizes a remote Merkle tree for a data file URL to local paths.
   Downloads the data file and its corresponding .mrkl tree if needed,
   then loads and returns the MerkleTree instance.
   */
  public static MerkleTree syncFromRemote(URL dataUrl, Path localDataPath) throws IOException {
    // Derive merkle URL and local merkle path
    String dataUrlStr = dataUrl.toString();
    URL merkleUrl = new URL(dataUrlStr + ".mrkl");
    Path localMerklePath = localDataPath.resolveSibling(localDataPath.getFileName() + ".mrkl");

    // Check if merkle file needs downloading by comparing its content to remote
    boolean download = true;
    if (Files.exists(localMerklePath)) {
      // Read remote merkle bytes
      byte[] remoteBytes;
      try (InputStream is = merkleUrl.openConnection().getInputStream()) {
        remoteBytes = is.readAllBytes();
      }
      // Read local merkle bytes
      byte[] localBytes = Files.readAllBytes(localMerklePath);
      if (Arrays.equals(remoteBytes, localBytes)) {
        download = false;
      }
    }

    if (download) {
      Files.createDirectories(localDataPath.getParent());
      // Download data file
      downloadFileHttp(dataUrl, localDataPath);
      // Download merkle file
      downloadFileHttp(merkleUrl, localMerklePath);
    }

    // Load and return the MerkleTree from local merkle file
    return MerkleTree.load(localMerklePath);
  }

  /**
   Reads the footer from a remote merkle tree file using OkHttp.
   @param client
   The OkHttp client
   @param merkleUrl
   The URL of the merkle tree file
   @param fileSize
   The size of the file
   @return The MerkleFooter or null if it couldn't be read
   */
  /**
   Reads the footer from a remote merkle tree file using HttpURLConnection.
   */
  private static MerkleFooter readRemoteMerkleFooterHttp(URL merkleUrl, long fileSize)
      throws IOException
  {
    int readSize = (int) Math.min(1024, fileSize);
    long start = fileSize - readSize;
    HttpURLConnection conn = (HttpURLConnection) merkleUrl.openConnection();
    conn.setRequestProperty("Range", "bytes=" + start + "-" + (fileSize - 1));
    conn.setConnectTimeout(5000);
    conn.setReadTimeout(5000);
    conn.connect();
    int code = conn.getResponseCode();
    byte[] buffer;
    if (code == HttpURLConnection.HTTP_PARTIAL) {
      try (InputStream is = conn.getInputStream()) {
        buffer = is.readAllBytes();
      }
    } else {
      conn.disconnect();
      conn = (HttpURLConnection) merkleUrl.openConnection();
      conn.setConnectTimeout(5000);
      conn.setReadTimeout(5000);
      conn.connect();
      if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
        return null;
      }
      try (InputStream is = conn.getInputStream()) {
        byte[] all = is.readAllBytes();
        buffer = (all.length > 1024 ? Arrays.copyOfRange(all, all.length - 1024, all.length) : all);
      }
    }
    return MerkleFooter.fromByteBuffer(ByteBuffer.wrap(buffer));
  }

  /**
   Reads the footer from a local merkle tree file.
   @param merklePath
   The path to the local merkle tree file
   @return The MerkleFooter or null if it couldn't be read
   */
  private static MerkleFooter readLocalMerkleFooter(Path merklePath) throws IOException {
    if (!Files.exists(merklePath) || Files.size(merklePath) == 0) {
      return null;
    }

    try (FileChannel channel = FileChannel.open(merklePath, StandardOpenOption.READ)) {
      long fileSize = channel.size();

      // Read up to the last 1KB
      long footerReadSize = Math.min(1024, fileSize);
      long position = fileSize - footerReadSize;

      ByteBuffer buffer = ByteBuffer.allocate((int) footerReadSize);
      channel.position(position);
      channel.read(buffer);
      buffer.flip();

      return MerkleFooter.fromByteBuffer(buffer);
    } catch (IOException e) {
      return null;
    }
  }

  /**
   Downloads a file from a URL to a local path using OkHttp.
   @param client
   The OkHttp client
   @param url
   The URL to download from
   @param localPath
   The path to save the file to
   @throws IOException
   If an I/O error occurs
   */
  /**
   Downloads a file from a URL to a local path using HttpURLConnection.
   */
  private static void downloadFileHttp(URL url, Path localPath) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setConnectTimeout(5000);
    conn.setReadTimeout(5000);
    conn.connect();
    int code = conn.getResponseCode();
    if (code != HttpURLConnection.HTTP_OK && code != HttpURLConnection.HTTP_PARTIAL) {
      throw new IOException("Failed to download file: " + url + ", HTTP response code: " + code);
    }
    Files.createDirectories(localPath.getParent());
    try (InputStream is = conn.getInputStream();
         OutputStream os = Files.newOutputStream(localPath))
    {
      is.transferTo(os);
    }
  }

  public long getChunkSize() {
    return chunkSize;
  }

  public long totalSize() {
    return totalSize;
  }

  public int getNumberOfLeaves() {
    return leafCount;
  }

  public MerkleMismatch getBoundariesForLeaf(int leafIndex) {
    long start = leafIndex * chunkSize;
    long end = Math.min(start + chunkSize, totalSize);
    long length = end - start;
    return new MerkleMismatch(leafIndex, start, length);
  }

  /**
   Retrieves the hash for any node in the tree, computing it if necessary.
   This is package-private to allow tests to access internal nodes, including the root.
   @param idx
   The index of the node in the hashes array
   @return The hash value for the node
   */
  byte[] getHash(int idx) {
    if (valid.get(idx))
      return hashes[idx];
    // compute children
    int left = 2 * idx + 1, right = left + 1;
    DIGEST.reset();
    // include left child if present
    if (left < hashes.length) {
      DIGEST.update(getHash(left));
    }
    // include right child if present
    if (right < hashes.length) {
      DIGEST.update(getHash(right));
    }
    hashes[idx] = DIGEST.digest();
    valid.set(idx);
    return hashes[idx];
  }

  /**
   Returns the hash for a leaf, computing internals lazily.
   */
  public byte[] getHashForLeaf(int leafIndex) {
    if (leafIndex < 0 || leafIndex >= leafCount)
      throw new IllegalArgumentException("Invalid leaf index");
    return getHash(offset + leafIndex);
  }

  /**
   Marks and recomputes a leaf hash, invalidating ancestors.
   */
  public void updateLeafHash(int leafIndex, byte[] newHash) {
    int idx = offset + leafIndex;
    hashes[idx] = newHash;
    valid.set(idx);
    // invalidate ancestors
    idx = (idx - 1) / 2;
    while (idx >= 0) {
      valid.clear(idx);
      if (idx == 0)
        break;
      idx = (idx - 1) / 2;
    }
    // recompute root lazily
    getHash(0);
  }

  /**
   Updates a leaf hash and persists the tree to disk.
   */
  public void updateLeafHash(int leafIndex, byte[] newHash, Path filePath) throws IOException {
    updateLeafHash(leafIndex, newHash);
    save(filePath);
  }

  /**
   Saves hashes and footer to file.
   */
  public void save(Path path) throws IOException {
    int numLeaves = capLeaf;
    // If an existing merkle file is present, verify integrity before overwriting
    if (Files.exists(path) && Files.size(path) > 0) {
      try {
        // verify existing Merkle file
        verifyWrittenMerkleFile(path);
      } catch (IOException ioe) {
        // existing file is corrupted: move it aside and abort save
        Path corrupted = path.resolveSibling(path.getFileName().toString() + ".corrupted");
        Files.move(path, corrupted, StandardCopyOption.REPLACE_EXISTING);
        throw new IOException("Merkle tree digest verification failed", ioe);
      }
    }
    // Write new merkle tree file
    try (FileChannel ch = FileChannel.open(
        path,
        StandardOpenOption.CREATE,
        StandardOpenOption.WRITE,
        StandardOpenOption.TRUNCATE_EXISTING
    ))
    {
      // write real leafCount leaves
      for (int i = 0; i < leafCount; i++) {
        ch.write(ByteBuffer.wrap(hashes[offset + i]));
      }
      // write padded leaves
      byte[] zero = new byte[HASH_SIZE];
      for (int i = leafCount; i < capLeaf; i++) {
        ch.write(ByteBuffer.wrap(zero));
      }
      // write internals
      for (int i = 0; i < offset; i++) {
        ch.write(ByteBuffer.wrap(hashes[i]));
      }
      // write footer
      MerkleFooter footer = MerkleFooter.create(chunkSize, totalSize, calculateDigest());
      ch.write(footer.toByteBuffer());
    }
  }

  private byte[] calculateDigest() {
    // digest over all tree data region in file order (leaves then internals)
    int totalLeaves = capLeaf;
    int offsetIndex = offset;
    int totalBytes = (totalLeaves + offsetIndex) * HASH_SIZE;
    ByteBuffer buf = ByteBuffer.allocate(totalBytes);
    // leaves region
    for (int i = offsetIndex; i < offsetIndex + totalLeaves; i++) {
      buf.put(hashes[i]);
    }
    // internal nodes region
    for (int i = 0; i < offsetIndex; i++) {
      buf.put(hashes[i]);
    }
    buf.flip();
    DIGEST.reset();
    DIGEST.update(buf);
    return DIGEST.digest();
  }

  /**
   Loads a tree from file, marking all nodes valid.
  */
  public static MerkleTree load(Path path) throws IOException {
    long fileSize = Files.size(path);
    // read footer
    MerkleFooter footer = MerkleFooter.fromByteBuffer(readFooterBuffer(path, fileSize));
    long chunkSize = footer.chunkSize();
    long totalSize = footer.totalSize();
    int leafCount = (int) Math.ceil((double) totalSize / chunkSize);
    // Determine tree shape based on data region entries
    int paddedCap = 1;
    while (paddedCap < leafCount) paddedCap <<= 1;
    int defaultOffset = paddedCap - 1;
    int defaultNodeCount = 2 * paddedCap - 1;
    // Determine actual data region size (excluding footer)
    int fl = footer.footerLength();
    long dataRegionSize = fileSize - fl;
    if (dataRegionSize % HASH_SIZE != 0) {
      throw new IOException("Invalid merkle tree file: data region size not a multiple of hash size: " + dataRegionSize);
    }
    int regionEntries = (int) (dataRegionSize / HASH_SIZE);
    // Choose capLeaf and offset based on padded or exact tree format
    int capLeaf;
    int offsetIndex;
    int nodeCount;
    if (regionEntries == defaultNodeCount) {
      // padded tree format
      capLeaf = paddedCap;
      offsetIndex = defaultOffset;
      nodeCount = defaultNodeCount;
    } else if (regionEntries == (2 * leafCount - 1)) {
      // exact complete tree format
      capLeaf = leafCount;
      offsetIndex = leafCount - 1;
      nodeCount = regionEntries;
    } else if (regionEntries == leafCount) {
      // only leaves stored, no internal nodes
      capLeaf = leafCount;
      offsetIndex = 0;
      nodeCount = 2 * leafCount - 1;
    } else {
      throw new IOException("Unexpected merkle tree data region entries: " + regionEntries
          + ", expected padded " + defaultNodeCount
          + " or exact complete " + (2 * leafCount - 1)
          + " or leaves-only " + leafCount);
    }
    byte[][] hashes = new byte[nodeCount][HASH_SIZE];
    BitSet valid = new BitSet(nodeCount);
    // read tree data region and populate hashes
    try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
      int footerRegion = offsetIndex * HASH_SIZE + capLeaf * HASH_SIZE;
      ByteBuffer buf = ByteBuffer.allocate(footerRegion);
      ch.position(0);
      while (buf.hasRemaining()) {
        int r = ch.read(buf);
        if (r < 0) {
          throw new IOException(
              "Unexpected end of file reading merkle tree data region, expected "
                  + buf.remaining() + " more bytes");
        }
      }
      buf.flip();
        // load leaves (or exact leaves) into correct positions
        for (int i = 0; i < capLeaf; i++) {
          buf.position(i * HASH_SIZE);
          buf.get(hashes[offsetIndex + i]);
        }
        // load internal nodes
        for (int i = 0; i < offsetIndex; i++) {
          buf.position(capLeaf * HASH_SIZE + i * HASH_SIZE);
          buf.get(hashes[i]);
        }
        // mark all nodes valid
        valid.set(0, nodeCount);
    }
    // verify integrity of loaded file
    verifyWrittenMerkleFile(path);
    // construct tree with actual capacity and offset based on file format
    return new MerkleTree(hashes, valid, leafCount, capLeaf, offsetIndex, chunkSize, totalSize);
  }

  private static ByteBuffer readFooterBuffer(Path path, long fileSize) throws IOException {
    try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
      ByteBuffer len = ByteBuffer.allocate(1);
      ch.position(fileSize - 1);
      ch.read(len);
      len.flip();
      byte fl = len.get();
      ByteBuffer buf = ByteBuffer.allocate(fl);
      ch.position(fileSize - fl);
      // read full footer into buffer
      while (buf.hasRemaining()) {
        int r = ch.read(buf);
        if (r < 0) {
          throw new IOException("Unexpected end of file reading merkle footer, expected "
              + buf.remaining() + " more bytes");
        }
      }
      buf.flip();
      return buf;
    }
  }

  /**
   Verifies the integrity of a written Merkle tree file by checking its footer digest.
   Throws IOException if the file is empty, truncated, or the digest does not match.
   */
  private static void verifyWrittenMerkleFile(Path path) throws IOException {
    // Ensure file exists and is non-empty
    long fileSize = Files.size(path);
    if (fileSize == 0) {
      throw new IOException("File is empty");
    }
    // Read footer buffer
    ByteBuffer footerBuf;
    try {
      footerBuf = readFooterBuffer(path, fileSize);
    } catch (IOException | RuntimeException e) {
      throw new IOException("Verification failed: Merkle tree digest verification failed", e);
    }
    MerkleFooter footer;
    try {
      footer = MerkleFooter.fromByteBuffer(footerBuf);
    } catch (IllegalArgumentException e) {
      throw new IOException("Verification failed: Merkle tree digest verification failed", e);
    }
    // Read tree data region (excluding footer)
    int fl = footer.footerLength();
    long dataSize = fileSize - fl;
    ByteBuffer dataBuf = ByteBuffer.allocate((int) dataSize);
    try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
      ch.position(0);
      // read full tree data region (excluding footer)
      while (dataBuf.hasRemaining()) {
        int r = ch.read(dataBuf);
        if (r < 0) {
          throw new IOException("Unexpected end of file reading merkle tree data for verification, expected "
              + dataBuf.remaining() + " more bytes");
        }
      }
      dataBuf.flip();
    }
    // Verify digest
    if (!footer.verifyDigest(dataBuf)) {
      throw new IOException("Verification failed: Merkle tree digest verification failed");
    }
  }

  /**
   Finds all mismatched chunks between this tree and another tree.
   @param otherTree
   The tree to compare against
   @return List of MerkleMismatch objects representing the mismatched chunks
   */
  public List<MerkleMismatch> findMismatchedChunks(MerkleTree otherTree) {
    // Validate that trees are comparable
    if (this.chunkSize != otherTree.chunkSize) {
      throw new IllegalArgumentException(
          "Cannot compare trees with different chunk sizes: " + this.chunkSize + " vs "
          + otherTree.chunkSize);
    }

    // Get the list of mismatched chunk indexes
    int[] mismatchedIndexes =
        findMismatchedChunksInRange(otherTree, 0, Math.min(this.leafCount, otherTree.leafCount));

    // Convert indexes to MerkleMismatch objects
    List<MerkleMismatch> mismatches = new ArrayList<>(mismatchedIndexes.length);
    for (int index : mismatchedIndexes) {
      // Calculate the range for this chunk
      long startOffset = (long) index * chunkSize;
      long length = Math.min(chunkSize, totalSize - startOffset);

      // Create MerkleMismatch object and add to list
      mismatches.add(new MerkleMismatch(index, startOffset, length));
    }

    return mismatches;
  }

  /**
   Finds all mismatched chunk indexes within a specified range.
   This method compares leaf hashes between the two trees and returns indexes where they differ.
   @param otherTree
   The tree to compare against
   @param startIndex
   The starting chunk index (inclusive)
   @param endIndex
   The ending chunk index (exclusive)
   @return Array of chunk indexes that differ between the trees
   */
  public int[] findMismatchedChunksInRange(MerkleTree otherTree, int startIndex, int endIndex) {
    // Validate matching chunk size
    if (this.chunkSize != otherTree.chunkSize) {
      throw new IllegalArgumentException(
          "Cannot compare trees with different chunk sizes: " + this.chunkSize + " vs "
          + otherTree.chunkSize);
    }
    // Validate matching total size
    if (this.totalSize != otherTree.totalSize) {
      throw new IllegalArgumentException(
          "Cannot compare trees with different total sizes: " + this.totalSize + " vs "
          + otherTree.totalSize);
    }
    // Clamp end index to available leaves
    int maxLeaf = Math.min(this.leafCount, otherTree.leafCount);
    int effectiveEnd = Math.min(endIndex, maxLeaf);
    // Validate start index
    if (startIndex < 0 || startIndex >= effectiveEnd) {
      throw new IllegalArgumentException("Invalid range: " + startIndex + " to " + endIndex);
    }

    // If the root hashes match and both trees are valid, there are no mismatches
    if (Arrays.equals(this.getHash(0), otherTree.getHash(0)) && this.valid.get(0)
        && otherTree.valid.get(0))
    {
      return new int[0];
    }

    // Use a list to collect mismatched indexes
    List<Integer> mismatches = new ArrayList<>();

    // Compare each leaf hash in the specified range
    for (int i = startIndex; i < effectiveEnd; i++) {
      byte[] thisHash = this.getHashForLeaf(i);
      byte[] otherHash = otherTree.getHashForLeaf(i);

      if (!Arrays.equals(thisHash, otherHash)) {
        mismatches.add(i);
      }
    }

    // Convert list to array
    int[] result = new int[mismatches.size()];
    for (int i = 0; i < mismatches.size(); i++) {
      result[i] = mismatches.get(i);
    }

    return result;
  }
}
