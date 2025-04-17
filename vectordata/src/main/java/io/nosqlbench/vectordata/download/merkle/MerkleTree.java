package io.nosqlbench.vectordata.download.merkle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.nosqlbench.vectordata.download.merkle.MerkleNode.HASH_SIZE;

/// Implements a Merkle tree (hash tree) for efficient data verification.
///
/// A Merkle tree is a binary tree where each leaf node contains a hash of a data chunk,
/// and each internal node contains a hash of its children's hashes. This structure allows
/// efficient verification of large data sets by comparing only the relevant branches.
public class MerkleTree {

    /// Shared MessageDigest instance for SHA-256 hashing
    private static final MessageDigest DIGEST;
    /// Root node of the Merkle tree
    private final MerkleNode root;
    /// Size of each data chunk in bytes (must be a power of 2)
    private final long chunkSize;
    /// Total size of the original data in bytes
    private final long totalSize;
    /// Range of data that this tree represents
    private final MerkleRange computedRange;

    static {
        try {
            DIGEST = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    /// Creates a new Merkle tree with the specified parameters.
    ///
    /// @param root The root node of the tree
    /// @param chunkSize The size of each data chunk in bytes (must be a power of 2)
    /// @param totalSize The total size of the original data in bytes
    /// @param computedRange The range of data that this tree represents
    /// @throws IllegalArgumentException if chunkSize is not a power of 2
    public MerkleTree(MerkleNode root, long chunkSize, long totalSize, MerkleRange computedRange) {
        if (!isPowerOfTwo(chunkSize)) {
            throw new IllegalArgumentException("Chunk size must be a power of two, got: " + chunkSize);
        }
        this.root = root;
        this.chunkSize = chunkSize;
        this.totalSize = totalSize;
        this.computedRange = computedRange;
    }

    /// Checks if a number is a power of 2.
    ///
    /// @param n The number to check
    /// @return true if n is a power of 2, false otherwise
    private static boolean isPowerOfTwo(long n) {
        return n > 0 && (n & (n - 1)) == 0;
    }

    /// Gets the root node of the tree.
    ///
    /// @return The root node
    public MerkleNode root() {
        return root;
    }

    /// Gets the chunk size used by this tree.
    ///
    /// @return The chunk size in bytes
    public long chunkSize() {
        return chunkSize;
    }

    /// Gets the total size of the original data.
    ///
    /// @return The total size in bytes
    public long totalSize() {
        return totalSize;
    }

    /// Gets the chunk size in bytes.
    ///
    /// @return The chunk size in bytes
    public long getChunkSize() {
        return chunkSize;
    }

    /// Gets the range of data that this tree represents.
    ///
    /// @return The computed range
    public MerkleRange computedRange() {
        return computedRange;
    }

    /// Returns the total number of leaf nodes in the complete tree
    public int getNumberOfLeaves() {
        return (int) ((totalSize + chunkSize - 1) >>> getChunkSizePower());
    }

    /// Converts a byte offset into its corresponding leaf node index
    /// @param offset The byte offset to convert
    /// @return The leaf node index
    public int getLeafIndex(long offset) {
        return (int) (offset >>> getChunkSizePower());
    }

    /// Returns the power of two for the chunk size
    private int getChunkSizePower() {
        return Long.numberOfTrailingZeros(chunkSize);
    }

    /// Returns the byte boundaries for a specific leaf node
    /// @param leafIndex The index of the leaf node
    /// @return The byte boundaries for the leaf node
    public NodeBoundary getBoundariesForLeaf(int leafIndex) {
        int numberOfLeaves = getNumberOfLeaves();
        if (leafIndex < 0 || leafIndex >= numberOfLeaves) {
            throw new IllegalArgumentException("Invalid leaf index: " + leafIndex);
        }

        long start = (long) leafIndex << getChunkSizePower();
        long end = Math.min(totalSize, ((long) (leafIndex + 1) << getChunkSizePower()));

        return new NodeBoundary(start, end);
    }

    /// Gets the leaf index (chunk index) for a given position in the file.
    ///
    /// @param position The position in the file
    /// @return The index of the leaf node (chunk) containing the position
    /// @throws IllegalArgumentException if the position is invalid
    public int getChunkIndexForPosition(long position) {
        if (position < 0 || position >= totalSize) {
            throw new IllegalArgumentException(
                "Invalid position: " + position + ", valid range is [0, " + (totalSize - 1) + "]"
            );
        }

        return (int)(position >> getChunkSizePower());
    }

    /// Represents the boundaries of a specific chunk in the tree
    public record NodeBoundary(long start, long end) {}

    /// Creates a new tree with an updated range
    /// @param newRange The new range to set for the tree
    /// @param treeData The raw data to build the tree from
    /// @return A new MerkleTree instance
    /// @throws IllegalArgumentException if the new range is invalid
    public MerkleTree withRange(MerkleRange newRange, ByteBuffer treeData) {
        if (computedRange.contains(newRange)) {
            return this;
        }

        treeData = treeData.duplicate();

        // Calculate leaf node indices for the new range
        int startLeaf = getLeafIndex(newRange.start());
        int endLeaf = getLeafIndex(newRange.end() - 1) + 1;

        // Create new leaf nodes for the range
        List<MerkleNode> leaves = new ArrayList<>(endLeaf - startLeaf);
        for (int i = startLeaf; i < endLeaf; i++) {
            treeData.position(i * HASH_SIZE);
            byte[] hash = new byte[HASH_SIZE];
            treeData.get(hash);
            leaves.add(MerkleNode.leaf(i - startLeaf, hash));
        }

        MerkleNode newRoot = buildTree(leaves, treeData, startLeaf);
        return new MerkleTree(newRoot, chunkSize, totalSize, newRange);
    }

    /// Builds a new tree from raw data
    /// @param data The raw data to build the tree from
    /// @param chunkSize The size of each data chunk in bytes (must be a power of 2)
    /// @param range The range of data that this tree represents
    /// @return A new MerkleTree instance
    public static MerkleTree fromData(ByteBuffer data, long chunkSize, MerkleRange range) {
        if (data == null || data.remaining() == 0) {
            throw new IllegalArgumentException("Data buffer cannot be null or empty");
        }

        List<MerkleNode> leaves = new ArrayList<>();
        long totalSize = data.capacity();
        int numLeaves = (int)((totalSize + chunkSize - 1) / chunkSize);

        // Create a duplicate to avoid modifying the original buffer
        ByteBuffer workingBuffer = data.duplicate();
        ByteBuffer treeData = ByteBuffer.allocate(numLeaves * HASH_SIZE);

        synchronized (DIGEST) {
            for (int leafIndex = 0; leafIndex < numLeaves; leafIndex++) {
                int bufsize = (int)Math.min(workingBuffer.remaining(), chunkSize);
                if (bufsize <= 0) break;

                byte[] chunk = new byte[bufsize];
                workingBuffer.get(chunk);

                DIGEST.reset();
                DIGEST.update(chunk);
                byte[] hash = DIGEST.digest();

                treeData.put(hash);
                leaves.add(MerkleNode.leaf(leafIndex, hash));
            }
        }

        if (leaves.isEmpty()) {
            throw new IllegalArgumentException("No leaf nodes created from input data");
        }

        MerkleNode root = buildTree(leaves);
        if (root == null) {
            throw new IllegalStateException("Failed to build tree: root node is null");
        }

        return new MerkleTree(root, chunkSize, totalSize, range);
    }

    /// Recursively builds a balanced binary tree from a list of nodes
    /// @param leaves The list of leaf nodes to build the tree from
    /// @return The root node of the built tree
    /// @throws IllegalArgumentException if the list of leaves is null or empty
    private static MerkleNode buildTree(List<MerkleNode> leaves) {
        if (leaves == null || leaves.isEmpty()) {
            return null;
        }

        if (leaves.size() == 1) {
            return leaves.get(0);
        }

        List<MerkleNode> parents = new ArrayList<>((leaves.size() + 1) / 2);

        for (int i = 0; i < leaves.size(); i += 2) {
            MerkleNode left = leaves.get(i);
            MerkleNode right = (i + 1 < leaves.size()) ? leaves.get(i + 1) : null;

            parents.add(MerkleNode.internal(
                parents.size(),
                left.hash(),
                right != null ? right.hash() : null,
                left,
                right
            ));
        }

        return buildTree(parents);
    }

    private static MerkleNode buildTree(List<MerkleNode> leaves, ByteBuffer treeData, int startLeaf) {
        return buildTree(leaves);  // Delegate to the simpler buildTree method
    }

    /// Saves the Merkle tree to a file, including the footer.
    ///
    /// The file format consists of all leaf node hashes followed by a footer
    /// containing the chunk size and total size.
    ///
    /// @param path The path to save the file to
    /// @throws IOException If there's an error writing to the file
    public void save(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING)) {

            // Get number of leaves
            int numLeaves = getNumberOfLeaves();

            // Allocate buffer for all leaf hashes
            ByteBuffer treeData = ByteBuffer.allocate(numLeaves * HASH_SIZE);

            // Collect all leaf hashes in order
            collectLeafHashes(root, treeData);
            treeData.flip();

            // Write tree data
            channel.write(treeData);

            // Write footer
            ByteBuffer footerBuffer = ByteBuffer.allocate(Long.BYTES * 2);
            footerBuffer.putLong(chunkSize);
            footerBuffer.putLong(totalSize);
            footerBuffer.flip();

            channel.write(footerBuffer);
        }
    }

    /// Recursively collects all leaf node hashes in order.
    ///
    /// @param node The current node to process
    /// @param buffer The buffer to store the hashes in
    private void collectLeafHashes(MerkleNode node, ByteBuffer buffer) {
        if (node == null) return;

        if (node.isLeaf()) {
            buffer.put(node.hash());
            return;
        }

        collectLeafHashes(node.left(), buffer);
        collectLeafHashes(node.right(), buffer);
    }

    /// Creates an empty merkle tree file with the same structure as a reference merkle tree file.
    /// The empty tree will have the same chunk size and total size as the reference tree,
    /// but all leaf node hashes will be initialized to zero bytes.
    ///
    /// @param referencePath The path to the reference merkle tree file
    /// @param outputPath The path where the empty merkle tree file will be created
    /// @throws IOException If there's an error reading or writing the files
    public static void createEmptyTreeLike(Path referencePath, Path outputPath) throws IOException {
        try (FileChannel referenceChannel = FileChannel.open(referencePath, StandardOpenOption.READ);
             FileChannel outputChannel = FileChannel.open(outputPath,
                 StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {

            // Read footer from reference file
            long referenceSize = referenceChannel.size();
            ByteBuffer footerBuffer = ByteBuffer.allocate(Long.BYTES * 2);
            referenceChannel.position(referenceSize - footerBuffer.capacity());
            referenceChannel.read(footerBuffer);
            footerBuffer.flip();

            // Get chunk size and total size
            long chunkSize = footerBuffer.getLong();
            long totalSize = footerBuffer.getLong();

            // Calculate number of leaves
            int numLeaves = (int)((totalSize + chunkSize - 1) / chunkSize);

            // Create empty leaf hashes (all zeros)
            ByteBuffer emptyTreeData = ByteBuffer.allocate(numLeaves * HASH_SIZE);

            // Fill with zeros (already zeroed by default in Java)
            // But we need to make sure we have at least one hash
            if (numLeaves > 0) {
                // Create a non-zero hash for the first leaf to ensure it can be read
                byte[] nonZeroHash = new byte[HASH_SIZE];
                for (int i = 0; i < HASH_SIZE; i++) {
                    nonZeroHash[i] = (byte) (i + 1); // Simple non-zero pattern
                }
                emptyTreeData.put(nonZeroHash);

                // Reset position to beginning for writing
                emptyTreeData.flip();
            }

            // Write empty tree data
            outputChannel.write(emptyTreeData);

            // Write the same footer as the reference file
            footerBuffer.flip();
            outputChannel.write(footerBuffer);
        }
    }

    /// Loads a Merkle tree from a file.
    ///
    /// Reads the leaf node hashes and footer information to reconstruct the tree.
    ///
    /// @param path The path to load the file from
    /// @return The loaded MerkleTree
    /// @throws IOException If there's an error reading the file
    public static MerkleTree load(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            // Read footer
            long fileSize = channel.size();
            ByteBuffer footerBuffer = ByteBuffer.allocate(Long.BYTES * 2);
            channel.position(fileSize - footerBuffer.capacity());
            channel.read(footerBuffer);
            footerBuffer.flip();

            // Get chunk size and total size
            long chunkSize = footerBuffer.getLong();
            long totalSize = footerBuffer.getLong();

            // Calculate number of leaves
            int numLeaves = (int)((totalSize + chunkSize - 1) / chunkSize);

            // Read tree data
            ByteBuffer treeData = ByteBuffer.allocate(numLeaves * HASH_SIZE);
            channel.position(0);
            int bytesRead = channel.read(treeData);
            if (bytesRead < HASH_SIZE) {
                throw new IOException("Failed to read tree data");
            }
            treeData.flip();

            // Create leaf nodes
            List<MerkleNode> leaves = new ArrayList<>(numLeaves);
            for (int i = 0; i < numLeaves && treeData.remaining() >= HASH_SIZE; i++) {
                byte[] hash = new byte[HASH_SIZE];
                treeData.get(hash);
                leaves.add(MerkleNode.leaf(i, hash));
            }

            if (leaves.isEmpty()) {
                throw new IOException("No leaf nodes found in merkle file");
            }

            // Build tree from leaves
            MerkleNode root = buildTree(leaves, treeData, 0);

            return new MerkleTree(root, chunkSize, totalSize,
                new MerkleRange(0, totalSize));
        }
    }

    /// Creates a new MerkleTree representing a subrange of this tree.
    ///
    /// Extracts a portion of the tree that covers only the specified range.
    ///
    /// @param newRange The range for the subtree
    /// @return A new MerkleTree covering only the specified range
    /// @throws IllegalArgumentException if the range is invalid or outside the tree's total size
    public MerkleTree subTree(MerkleRange newRange) {
        // Validate range
        if (newRange.start() < 0 || newRange.end() > totalSize || newRange.start() >= newRange.end()) {
            throw new IllegalArgumentException(
                "Invalid range [" + newRange.start() + "," + newRange.end() +
                "] for tree of size " + totalSize
            );
        }

        // If range matches current tree, return this
        if (newRange.equals(computedRange)) {
            return this;
        }

        // Calculate leaf indices for the new range
        int startLeaf = getLeafIndex(newRange.start());
        int endLeaf = getLeafIndex(newRange.end() - 1) + 1;

        // Collect relevant leaf nodes
        List<MerkleNode> leaves = new ArrayList<>();
        MerkleNode current = root;
        collectLeavesInRange(current, startLeaf, endLeaf, leaves);

        // Build new tree from collected leaves
        MerkleNode newRoot = buildTree(leaves, ByteBuffer.allocate(0), startLeaf);
        return new MerkleTree(newRoot, chunkSize, totalSize, newRange);
    }

    /// Helper method to collect leaf nodes within a specified range of indices.
    ///
    /// @param node The current node to process
    /// @param startLeaf The starting leaf index (inclusive)
    /// @param endLeaf The ending leaf index (exclusive)
    /// @param leaves The list to collect the leaves in
    private void collectLeavesInRange(MerkleNode node, int startLeaf, int endLeaf, List<MerkleNode> leaves) {
        if (node == null) {
            return;
        }

        if (node.isLeaf()) {
            if (node.index() >= startLeaf && node.index() < endLeaf) {
                leaves.add(node);
            }
            return;
        }

        // Recursively collect leaves from children
        collectLeavesInRange(node.left(), startLeaf, endLeaf, leaves);
        collectLeavesInRange(node.right(), startLeaf, endLeaf, leaves);
    }

    /// Compares this tree with another and returns details of chunks that don't match.
    ///
    /// Uses the Merkle tree structure to efficiently identify differences without
    /// comparing all chunks individually.
    ///
    /// @param other The tree to compare against
    /// @return List of MerkleMismatch records for differing chunks
    /// @throws IllegalArgumentException if trees have incompatible properties
    public List<MerkleMismatch> findMismatchedChunks(MerkleTree other) {
        // Validate compatibility
        if (this.chunkSize != other.chunkSize) {
            throw new IllegalArgumentException(
                "Cannot compare trees with different chunk sizes: " +
                this.chunkSize + " vs " + other.chunkSize
            );
        }
        if (this.totalSize != other.totalSize) {
            throw new IllegalArgumentException(
                "Cannot compare trees with different total sizes: " +
                this.totalSize + " vs " + other.totalSize
            );
        }

        List<MerkleMismatch> mismatches = new ArrayList<>();
        MerkleRange commonRange = computedRange.intersection(other.computedRange());
        if (commonRange == null) {
            // No overlap, all chunks in both ranges differ
            int startChunk = getLeafIndex(computedRange.start());
            int endChunk = getLeafIndex(computedRange.end() - 1) + 1;
            for (int i = startChunk; i < endChunk; i++) {
                NodeBoundary bounds = getBoundariesForLeaf(i);
                mismatches.add(new MerkleMismatch(
                    i,
                    bounds.start(),
                    bounds.end() - bounds.start()
                ));
            }
            return mismatches;
        }

        // Compare nodes recursively starting from roots
        compareTrees(this.root, other.root, mismatches);
        return mismatches;
    }

    /// Recursively compares two subtrees and collects mismatched chunks.
    ///
    /// @param node1 The first node to compare
    /// @param node2 The second node to compare
    /// @param mismatches The list to collect mismatches in
    private void compareTrees(MerkleNode node1, MerkleNode node2, List<MerkleMismatch> mismatches) {
        // If either node is null, consider their subtrees as mismatched
        if (node1 == null || node2 == null) {
            if (node1 != null) {
                collectLeafMismatches(node1, mismatches);
            }
            if (node2 != null) {
                collectLeafMismatches(node2, mismatches);
            }
            return;
        }

        // If hashes match, subtrees are identical
        if (Arrays.equals(node1.hash(), node2.hash())) {
            return;
        }

        // If we reach leaves, add their details if they don't match
        if (node1.isLeaf() && node2.isLeaf()) {
            NodeBoundary bounds = getBoundariesForLeaf(node1.index());
            mismatches.add(new MerkleMismatch(
                node1.index(),
                bounds.start(),
                bounds.end() - bounds.start()
            ));
            return;
        }

        // Recurse into children
        compareTrees(node1.left(), node2.left(), mismatches);
        compareTrees(node1.right(), node2.right(), mismatches);
    }

    /// Collects all leaf mismatches from a subtree.
    ///
    /// @param node The node to collect mismatches from
    /// @param mismatches The list to collect mismatches in
    private void collectLeafMismatches(MerkleNode node, List<MerkleMismatch> mismatches) {
        if (node == null) {
            return;
        }

        if (node.isLeaf()) {
            NodeBoundary bounds = getBoundariesForLeaf(node.index());
            mismatches.add(new MerkleMismatch(
                node.index(),
                bounds.start(),
                bounds.end() - bounds.start()
            ));
            return;
        }

        collectLeafMismatches(node.left(), mismatches);
        collectLeafMismatches(node.right(), mismatches);
    }

    /// Returns a new MessageDigest instance for SHA-256.
    ///
    /// @return MessageDigest configured for SHA-256
    /// @throws RuntimeException if SHA-256 is not available
    public MessageDigest getDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    /// Gets the stored hash for a specific leaf node.
    ///
    /// Navigates down the tree to find the leaf node with the given index.
    ///
    /// @param leafIndex The index of the leaf node
    /// @return The hash value for that leaf
    /// @throws IllegalArgumentException if the leaf index is invalid
    public byte[] getHashForLeaf(int leafIndex) {
        if (leafIndex < 0 || leafIndex >= getNumberOfLeaves()) {
            throw new IllegalArgumentException("Invalid leaf index: " + leafIndex);
        }

        MerkleNode node = root;
        int currentIndex = leafIndex;
        int totalLeaves = getNumberOfLeaves();

        // Navigate down to the leaf node
        while (!node.isLeaf()) {
            int leftSubtreeSize = totalLeaves / 2;
            if (currentIndex < leftSubtreeSize) {
                node = node.left();
                totalLeaves = leftSubtreeSize;
            } else {
                node = node.right();
                currentIndex -= leftSubtreeSize;
                totalLeaves -= leftSubtreeSize;
            }
        }

        return node.hash();
    }

    /// Updates the hash for a specific leaf node.
    ///
    /// @param leafIndex The index of the leaf node
    /// @param newHash The new hash for the leaf node
    /// @throws IllegalArgumentException if the leaf index is invalid
    public void updateLeafHash(int leafIndex, byte[] newHash) {
        if (leafIndex < 0 || leafIndex >= getNumberOfLeaves()) {
            throw new IllegalArgumentException("Invalid leaf index: " + leafIndex);
        }

        MerkleNode node = root;
        int currentIndex = leafIndex;
        int totalLeaves = getNumberOfLeaves();

        // Navigate down to the leaf node
        while (!node.isLeaf()) {
            int leftSubtreeSize = totalLeaves / 2;
            if (currentIndex < leftSubtreeSize) {
                node = node.left();
                totalLeaves = leftSubtreeSize;
            } else {
                node = node.right();
                currentIndex -= leftSubtreeSize;
                totalLeaves -= leftSubtreeSize;
            }
        }

        // Update the leaf node hash
        node.updateHash(newHash);
    }

    /// Updates the hash for a specific leaf node and writes it to the file.
    ///
    /// @param leafIndex The index of the leaf node
    /// @param newHash The new hash for the leaf node
    /// @param filePath The path to the merkle tree file
    /// @throws IllegalArgumentException if the leaf index is invalid
    /// @throws IOException if there's an error writing to the file
    public void updateLeafHash(int leafIndex, byte[] newHash, Path filePath) throws IOException {
        // Update the in-memory hash
        updateLeafHash(leafIndex, newHash);

        // Update the merkle tree file on disk
        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.WRITE)) {
            // Write the updated leaf hash to the file
            ByteBuffer buffer = ByteBuffer.wrap(newHash);
            channel.position(leafIndex * HASH_SIZE);
            channel.write(buffer);
        }
    }

    /// Calculates the height of the Merkle tree
    ///
    /// @return The height of the tree (0 for empty tree, 1 for just a root node)
    public int getTreeHeight() {
        if (root == null) {
            return 0;
        }
        return calculateHeight(root);
    }

    /// Recursively calculates the height of a subtree
    ///
    /// @param node The root of the subtree
    /// @return The height of the subtree
    private int calculateHeight(MerkleNode node) {
        if (node == null) {
            return 0;
        }

        int leftHeight = calculateHeight(node.left());
        int rightHeight = calculateHeight(node.right());

        return Math.max(leftHeight, rightHeight) + 1;
    }

    /// Counts the total number of nodes in the tree
    ///
    /// @return The total number of nodes
    public int getTotalNodeCount() {
        if (root == null) {
            return 0;
        }
        return countNodes(root);
    }

    /// Recursively counts nodes in a subtree
    ///
    /// @param node The root of the subtree
    /// @return The number of nodes in the subtree
    private int countNodes(MerkleNode node) {
        if (node == null) {
            return 0;
        }

        return 1 + countNodes(node.left()) + countNodes(node.right());
    }

    /// Formats a byte size into a human-readable string
    ///
    /// @param bytes The size in bytes
    /// @return A human-readable string representation
    private String formatByteSize(long bytes) {
        if (bytes < 1024) {
            return bytes + " bytes";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.2f KB", bytes / 1024.0);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.2f MB", bytes / (1024.0 * 1024.0));
        } else {
            return String.format("%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0));
        }
    }

    /// Converts a byte array to a hex string
    ///
    /// @param bytes The byte array to convert
    /// @return A hex string representation
    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    /// Returns a string representation of this Merkle tree
    ///
    /// @return A string containing a summary of the tree's properties
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Merkle Tree Summary:\n");

        // Basic properties
        sb.append(String.format("Original Data Size: %s\n", formatByteSize(totalSize)));
        sb.append(String.format("Chunk Size: %s\n", formatByteSize(chunkSize)));
        sb.append(String.format("Number of Chunks: %d\n", getNumberOfLeaves()));

        // Tree structure
        int height = getTreeHeight();
        int totalNodes = getTotalNodeCount();
        int internalNodes = totalNodes - getNumberOfLeaves();

        sb.append(String.format("Tree Height: %d\n", height));
        sb.append(String.format("Total Nodes: %d\n", totalNodes));
        sb.append(String.format("Leaf Nodes: %d\n", getNumberOfLeaves()));
        sb.append(String.format("Internal Nodes: %d\n", internalNodes));

        // Range information
        sb.append(String.format("Computed Range: %s\n", computedRange));

        // Root hash
        if (root != null) {
            String rootHashHex = bytesToHex(root.hash());
            sb.append(String.format("Root Hash: %s\n", rootHashHex));
        } else {
            sb.append("Root Hash: <none>\n");
        }

        // Estimated file size
        long estimatedFileSize = (getNumberOfLeaves() * HASH_SIZE) + (2 * Long.BYTES);
        sb.append(String.format("Estimated File Size: %s\n", formatByteSize(estimatedFileSize)));

        return sb.toString();
    }
}
