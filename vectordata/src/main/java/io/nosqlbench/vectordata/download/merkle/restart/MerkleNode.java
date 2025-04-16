package io.nosqlbench.vectordata.download.merkle.restart;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Represents a node in a Merkle tree structure.
 */
public record MerkleNode(
    int index,          // Index in logical tree structure
    int level,          // Level in the tree (0 = leaf)
    byte[] hash,        // SHA-256 hash of the section
    MerkleNode left,    // Left child node
    MerkleNode right    // Right child node
) {
    public static final int HASH_SIZE = 32; // SHA-256 hash size in bytes

    public MerkleNode {
        if (hash.length != HASH_SIZE) {
            throw new IllegalArgumentException(
                "Invalid hash length: expected " + HASH_SIZE + 
                ", got " + hash.length
            );
        }
    }

    /**
     * Create a leaf node with the given index and hash
     */
    public static MerkleNode leaf(int index, byte[] hash) {
        return new MerkleNode(index, 0, hash, null, null);
    }

    /**
     * Create an internal node with the given parameters
     */
    public static MerkleNode internal(int index, byte[] leftHash, byte[] rightHash, MerkleNode left, MerkleNode right) {
        // Create MessageDigest instance for SHA-256
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }

        // Combine and hash the child hashes
        byte[] combinedHash;
        if (rightHash != null) {
            // Concatenate the hashes before digesting
            byte[] combined = new byte[HASH_SIZE * 2];
            System.arraycopy(leftHash, 0, combined, 0, HASH_SIZE);
            System.arraycopy(rightHash, 0, combined, HASH_SIZE, HASH_SIZE);
            combinedHash = digest.digest(combined);
        } else {
            // If no right hash, hash the left hash alone
            combinedHash = digest.digest(leftHash);
        }
        
        int level = left != null ? left.level() + 1 : 1;
        return new MerkleNode(index, level, combinedHash, left, right);
    }

    /**
     * Check if this node is a leaf node
     */
    public boolean isLeaf() {
        return left == null && right == null;
    }

    /**
     * Compute the start offset for this node based on index and chunk size
     */
    public long startOffset(long totalSize, long chunkSize) {
        if (!isLeaf()) {
            return left.startOffset(totalSize, chunkSize);
        }
        return (long) index * chunkSize;
    }

    /**
     * Compute the end offset for this node based on index and chunk size
     */
    public long endOffset(long totalSize, long chunkSize) {
        if (!isLeaf()) {
            return right != null ? right.endOffset(totalSize, chunkSize) 
                               : left.endOffset(totalSize, chunkSize);
        }
        return Math.min(totalSize, ((long) index + 1) * chunkSize);
    }

    /**
     * Find the leaf node containing the given offset
     */
    public MerkleNode findLeafForOffset(long offset, long totalSize, long chunkSize) {
        if (isLeaf()) {
            long start = startOffset(totalSize, chunkSize);
            long end = endOffset(totalSize, chunkSize);
            return (offset >= start && offset < end) ? this : null;
        }
        
        if (left != null) {
            long leftEnd = left.endOffset(totalSize, chunkSize);
            if (offset < leftEnd) {
                return left.findLeafForOffset(offset, totalSize, chunkSize);
            }
        }
        
        if (right != null) {
            return right.findLeafForOffset(offset, totalSize, chunkSize);
        }
        
        return null;
    }
}