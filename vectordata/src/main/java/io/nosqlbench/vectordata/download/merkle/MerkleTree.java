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
}
