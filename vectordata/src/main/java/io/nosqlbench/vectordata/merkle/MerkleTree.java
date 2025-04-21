package io.nosqlbench.vectordata.merkle;


import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.BufferedSink;
import okio.BufferedSource;
import okio.Okio;

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
 ImplicitMerkleTree stores all node hashes in a flat array and tracks staleness in a BitSet.
 It supports lazy recomputation of internal nodes when accessed. */
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
    // compute leaf hashes
    for (int i = 0; i < cap; i++) {
      ByteBuffer slice;
      if (i < leafCount) {
        int start = i * (int) chunkSize;
        int len = (int) Math.min(chunkSize, range.length() - start);
        slice = ByteBuffer.allocate(len);
        data.position(start);
        data.limit(start + len);
        slice.put(data);
        slice.flip();
      } else {
        slice = ByteBuffer.allocate(0);
      }
      DIGEST.reset();
      DIGEST.update(slice.duplicate());
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
  public static MerkleTree syncFromRemote(URL merkleUrl, Path localPath) throws IOException {
    // Create OkHttp client
    OkHttpClient client = new OkHttpClient();

    // Check if we need to download the merkle tree file
    boolean downloadMerkleTree = true;
    long remoteSize = 0;
    MerkleFooter remoteFooter = null;

    // Send HEAD request to get merkle file size
    Request headRequest = new Request.Builder().url(merkleUrl.toString()).head().build();

    try (Response headResponse = client.newCall(headRequest).execute()) {
      if (headResponse.isSuccessful()) {
        // Get content length
        String contentLength = headResponse.header("Content-Length");
        if (contentLength != null) {
          remoteSize = Long.parseLong(contentLength);

          // If remote size is known and local file exists, check if they match
          if (remoteSize > 0 && Files.exists(localPath) && Files.size(localPath) == remoteSize) {

            // Read the remote footer
            remoteFooter = readRemoteMerkleFooter(client, merkleUrl.toString(), remoteSize);

            // Read the local footer
            MerkleFooter localFooter = readLocalMerkleFooter(localPath);

            // If footers match, no need to download
            if (localFooter != null && remoteFooter != null && localFooter.equals(remoteFooter)) {
              downloadMerkleTree = false;
            }
          }
        }
      }
    }

    // Download the merkle tree file if needed
    if (downloadMerkleTree) {
      // Create parent directories if needed
      Files.createDirectories(localPath.getParent());

      // Download the merkle tree file
      downloadFile(client, merkleUrl.toString(), localPath);

      // Derive data file URL by removing .mrkl extension and derive data file path
      String merkleUrlString = merkleUrl.toString();
      if (merkleUrlString.endsWith(".mrkl")) {
        String dataUrl = merkleUrlString.substring(0, merkleUrlString.length() - 5);
        // The data file path is derived by removing .mrkl from the localPath if it exists
        String localPathStr = localPath.toString();
        if (localPathStr.endsWith(".mrkl")) {
          Path dataPath = Path.of(localPathStr.substring(0, localPathStr.length() - 5));
          // Download the data file
          downloadFile(client, dataUrl, dataPath);
        }
      }
    }

    // Load and return the merkle tree
    return MerkleTree.load(localPath);
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
  private static MerkleFooter readRemoteMerkleFooter(
      OkHttpClient client,
      String merkleUrl,
      long fileSize
  ) throws IOException
  {
    // Determine how much to read from the end (up to 1KB)
    long footerReadSize = Math.min(1024, fileSize);
    long rangeStart = fileSize - footerReadSize;

    // Build a request with the range header
    Request request = new Request.Builder().url(merkleUrl)
        .header("Range", "bytes=" + rangeStart + "-" + (fileSize - 1)).build();

    try (Response response = client.newCall(request).execute()) {
      if (response.isSuccessful() && response.body() != null) {
        // Read the bytes
        byte[] buffer = response.body().bytes();

        // The last bytes contain the footer
        return MerkleFooter.fromByteBuffer(ByteBuffer.wrap(buffer));
      }
    }
    return null;
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
  private static void downloadFile(OkHttpClient client, String url, Path localPath)
      throws IOException
  {
    // Create the request
    Request request = new Request.Builder().url(url).build();

    // Execute the request
    try (Response response = client.newCall(request).execute()) {
      if (!response.isSuccessful() || response.body() == null) {
        throw new IOException(
            "Failed to download file: " + url + ", HTTP response code: " + response.code());
      }

      // Create parent directories if needed
      Files.createDirectories(localPath.getParent());

      // Download the file
      try (ResponseBody body = response.body();
           BufferedSource source = body.source();
           BufferedSink sink = Okio.buffer(Okio.sink(localPath)))
      {
        sink.writeAll(source);
      }
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
    try (FileChannel ch = FileChannel.open(
        path,
        StandardOpenOption.CREATE,
        StandardOpenOption.WRITE,
        StandardOpenOption.TRUNCATE_EXISTING
    ))
    {
      // write real leafCount leaves
      for (int i = 0; i < leafCount; i++)
        ch.write(ByteBuffer.wrap(hashes[offset + i]));
      // write padded leaves
      byte[] zero = new byte[HASH_SIZE];
      for (int i = leafCount; i < capLeaf; i++)
        ch.write(ByteBuffer.wrap(zero));
      // write internals
      for (int i = 0; i < offset; i++)
        ch.write(ByteBuffer.wrap(hashes[i]));
      // write footer
      MerkleFooter footer = MerkleFooter.create(chunkSize, totalSize, calculateDigest());
      ch.write(footer.toByteBuffer());
    }
    // verify written file and handle corruption
    try {
      verifyWrittenMerkleFile(path);
    } catch (IOException ioe) {
      // rename corrupted file
      Path corrupted = path.resolveSibling(path.getFileName().toString() + ".corrupted");
      Files.move(path, corrupted, StandardCopyOption.REPLACE_EXISTING);
      throw ioe;
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
    int cap = 1;
    while (cap < leafCount)
      cap <<= 1;
    int offset = cap - 1;
    int nodeCount = 2 * cap - 1;
    byte[][] hashes = new byte[nodeCount][HASH_SIZE];
    BitSet valid = new BitSet(nodeCount);
    try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
      // read all tree data (leaves then internals)
      int capLeaf = cap;
      int footerRegion = offset * HASH_SIZE + capLeaf * HASH_SIZE;
      ByteBuffer buf = ByteBuffer.allocate(footerRegion);
      ch.position(0);
      ch.read(buf);
      buf.flip();
      // load leaves (including padded leaves)
      for (int i = 0; i < capLeaf; i++) {
        buf.position(i * HASH_SIZE);
        buf.get(hashes[offset + i]);
      }
      // load internal nodes
      for (int i = 0; i < offset; i++) {
        buf.position(capLeaf * HASH_SIZE + i * HASH_SIZE);
        buf.get(hashes[i]);
      }
      // mark all valid
      valid.set(0, nodeCount);
    }
    // verify integrity of loaded file
    verifyWrittenMerkleFile(path);
    return new MerkleTree(hashes, valid, leafCount, cap, offset, chunkSize, totalSize);
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
      ch.read(buf);
      buf.flip();
      return buf;
    }
  }
  /**
   * Verifies the integrity of a written Merkle tree file by checking its footer digest.
   * Throws IOException if the file is empty, truncated, or the digest does not match.
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
      ch.read(dataBuf);
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