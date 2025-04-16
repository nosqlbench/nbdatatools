package restart;

import io.nosqlbench.vectordata.download.merkle.restart.MerkleNode;
import io.nosqlbench.vectordata.download.merkle.restart.MerkleRange;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/// # MerkleMeta
/// 
/// Represents a Merkle tree structure with metadata and dynamic boundary computation.
/// Uses SHA-256 as the standard hash algorithm.
/// 
/// ## Node Positioning
/// The tree follows standard Merkle tree node positioning:
/// - Leaf nodes represent fixed-size chunks of the original data
/// - Internal nodes are created by hashing pairs of child nodes
/// - Node indices are recalculated when the tree is modified
/// 
/// ## Usage Example
/// ```java
/// ByteBuffer treeData = ...;
/// Range initialRange = new Range(0, 1024);
/// MerkleMeta tree = MerkleMeta.fromBuffer(
///     treeData, totalSize, initialRange, 
///     chunkSize, version, fileDigest, footerLength
/// );
/// ```
public record MerkleMeta(
    byte version,              // Version of the Merkle tree format
    long totalDataSize,        // Total size of the data being hashed
    MerkleRange computedRange,       // Currently computed range within the total data
    byte[] fileDigest,         // SHA-256 digest of the entire file
    long chunkSize,           // Size of each chunk (must be power of 2)
    MerkleNode root,          // Root node of the Merkle tree
    short footerLength        // Length of footer metadata
) {
    // Minimum chunk size of 1KB to prevent excessive tree depth
    private static final long MIN_CHUNK_SIZE = 1L << 10;
    public static final byte CURRENT_VERSION = 0x01;

    /// Validates the MerkleMeta parameters during construction
    public MerkleMeta {
        // Ensure file digest is correct length (SHA-256 = 32 bytes)
        if (fileDigest.length != MerkleNode.HASH_SIZE) {
            throw new IllegalArgumentException(
                "Invalid file digest length: expected " + MerkleNode.HASH_SIZE +
                ", got " + fileDigest.length
            );
        }
        
        // Enforce minimum chunk size
        if (chunkSize < MIN_CHUNK_SIZE) {
            throw new IllegalArgumentException(
                "Chunk size must be at least " + MIN_CHUNK_SIZE + " bytes"
            );
        }
        
        // Round up chunk size to next power of 2 if needed
        if ((chunkSize & (chunkSize - 1)) != 0) {
            chunkSize = Long.highestOneBit(chunkSize) << 1;
        }

        if (computedRange.end() > totalDataSize) {
            throw new IllegalArgumentException("Range end exceeds total size");
        }

        if (version != CURRENT_VERSION) {
            throw new IllegalStateException("Unsupported version: 0x" + Integer.toHexString(version));
        }
    }

    /// Returns the total number of leaf nodes in the complete tree
    public int getNumberOfLeaves() {
        return (int) ((totalDataSize + chunkSize - 1) / chunkSize);
    }

    /// Converts a byte offset into its corresponding leaf node index
    public int getLeafIndex(long offset) {
        return (int) (offset / chunkSize);
    }

    /// Creates a new MerkleMeta with an updated range and recomputed tree
    public MerkleMeta withRange(MerkleRange newRange, ByteBuffer treeData) {
        if (newRange.end() > totalDataSize) {
            throw new IllegalArgumentException("Range end exceeds total size");
        }

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
            // Read the hash directly from treeData
            treeData.position(i * MerkleNode.HASH_SIZE);
            byte[] hash = new byte[MerkleNode.HASH_SIZE];
            treeData.get(hash);
            // Create new leaf node with index relative to this range
            leaves.add(MerkleNode.leaf(i - startLeaf, hash));
        }

        // Build new tree structure with renumbered nodes
        MerkleNode newRoot = buildTree(leaves, treeData, startLeaf);
        
        return new MerkleMeta(
            version,
            totalDataSize,
            newRange,
            fileDigest,
            chunkSize,
            newRoot,
            footerLength
        );
    }

    /// Recursively builds a balanced binary tree from a list of nodes
    private static MerkleNode buildTree(List<MerkleNode> leaves, ByteBuffer treeData, int startLeaf) {
        if (leaves.isEmpty()) {
            return null;
        }
        
        // Base case: single node
        if (leaves.size() == 1) {
            return leaves.get(0);
        }
        
        List<MerkleNode> currentLevel = new ArrayList<>(leaves);
        
        while (currentLevel.size() > 1) {
            List<MerkleNode> nextLevel = new ArrayList<>((currentLevel.size() + 1) / 2);
            
            for (int i = 0; i < currentLevel.size(); i += 2) {
                MerkleNode left = currentLevel.get(i);
                MerkleNode right = (i + 1 < currentLevel.size()) ? currentLevel.get(i + 1) : null;
                
                byte[] leftHash = left.hash();
                byte[] rightHash = right != null ? right.hash() : null;
                
                nextLevel.add(MerkleNode.internal(
                    nextLevel.size(),

                    leftHash,
                    rightHash,
                    left,
                    right
                ));
            }
            
            currentLevel = nextLevel;
        }
        
        return currentLevel.get(0);
    }

    /// Represents the boundaries of a specific chunk in the tree
    public record NodeBoundary(long start, long end) {}

    /// Returns the byte boundaries for a specific leaf node
    public NodeBoundary getBoundariesForLeaf(int leafIndex) {
        int numberOfLeaves = getNumberOfLeaves();
        if (leafIndex < 0 || leafIndex >= numberOfLeaves) {
            throw new IllegalArgumentException("Invalid leaf index: " + leafIndex);
        }
        
        long start = (long) leafIndex * chunkSize;
        long end = Math.min(totalDataSize, ((long) leafIndex + 1) * chunkSize);
        
        return new NodeBoundary(start, end);
    }

    /// Validates the version of this MerkleMeta instance
    public void validate() {
        if (version != CURRENT_VERSION) {
            throw new IllegalStateException("Unsupported version: 0x" + Integer.toHexString(version));
        }
    }

    /// Creates a new MerkleMeta instance from a buffer containing tree data
    public static MerkleMeta fromBuffer(
        ByteBuffer treeData,
        long totalSize,
        MerkleRange initialRange,
        long chunkSize,
        byte version,
        byte[] fileDigest,
        short footerLength
    ) {
        // Validate parameters first
        if (chunkSize < MIN_CHUNK_SIZE) {
            throw new IllegalArgumentException(
                "Chunk size must be at least " + MIN_CHUNK_SIZE + " bytes"
            );
        }

        if (fileDigest.length != MerkleNode.HASH_SIZE) {
            throw new IllegalArgumentException(
                "Invalid file digest length: expected " + MerkleNode.HASH_SIZE +
                ", got " + fileDigest.length
            );
        }

        if (version != CURRENT_VERSION) {
            throw new IllegalStateException("Unsupported version: 0x" + Integer.toHexString(version));
        }

        if (initialRange.end() > totalSize) {
            throw new IllegalArgumentException("Range end exceeds total size");
        }

        // Round up chunk size to next power of 2 if needed
        if ((chunkSize & (chunkSize - 1)) != 0) {
            chunkSize = Long.highestOneBit(chunkSize) << 1;
        }

        // Create initial tree structure for the specified range
        int startLeaf = (int) (initialRange.start() / chunkSize);
        int endLeaf = (int) ((initialRange.end() - 1) / chunkSize) + 1;
        
        List<MerkleNode> leaves = new ArrayList<>(endLeaf - startLeaf);
        for (int i = startLeaf; i < endLeaf; i++) {
            treeData.position(i * MerkleNode.HASH_SIZE);
            byte[] hash = new byte[MerkleNode.HASH_SIZE];
            treeData.get(hash);
            leaves.add(MerkleNode.leaf(i - startLeaf, hash));
        }
        
        MerkleNode root = buildTree(leaves, treeData, startLeaf);
        
        return new MerkleMeta(
            version,
            totalSize,
            initialRange,
            fileDigest,
            chunkSize,
            root,
            footerLength
        );
    }
}
