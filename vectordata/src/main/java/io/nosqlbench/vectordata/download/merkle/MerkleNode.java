package io.nosqlbench.vectordata.download.merkle;

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


import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/// Represents a node in a Merkle tree structure.
///
/// Each node contains a hash value and references to its child nodes (if any).
/// Leaf nodes represent chunks of the original data, while internal nodes
/// combine and hash their children's values.
public record MerkleNode(
    /// Index in logical tree structure
    int index,
    /// Level in the tree (0 = leaf)
    int level,
    /// SHA-256 hash of the section
    byte[] hash,
    /// Left child node
    MerkleNode left,
    /// Right child node
    MerkleNode right
) {
    /// SHA-256 hash size in bytes
    public static final int HASH_SIZE = 32;
    /// Shared MessageDigest instance for SHA-256 hashing
    private static final MessageDigest DIGEST;

    static {
        try {
            DIGEST = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    /// Compact constructor that validates the hash length
    public MerkleNode {
        if (hash.length != HASH_SIZE) {
            throw new IllegalArgumentException(
                "Invalid hash length: expected " + HASH_SIZE +
                ", got " + hash.length
            );
        }
    }

    /// Creates a leaf node with the given index and hash.
    ///
    /// @param index The index of the leaf node
    /// @param hash The SHA-256 hash of the data chunk
    /// @return A new leaf node
    public static MerkleNode leaf(int index, byte[] hash) {
        return new MerkleNode(index, 0, hash, null, null);
    }

    /// Creates an internal node by combining and hashing its children's values.
    ///
    /// @param index The index of the internal node
    /// @param leftHash The hash from the left child
    /// @param rightHash The hash from the right child (may be null)
    /// @param left The left child node
    /// @param right The right child node (may be null)
    /// @return A new internal node with combined hash
    public static MerkleNode internal(int index, byte[] leftHash, byte[] rightHash, MerkleNode left, MerkleNode right) {
        // Combine and hash the child hashes
        byte[] combinedHash;
        if (rightHash != null) {
            // Concatenate the hashes before digesting
            byte[] combined = new byte[HASH_SIZE * 2];
            System.arraycopy(leftHash, 0, combined, 0, HASH_SIZE);
            System.arraycopy(rightHash, 0, combined, HASH_SIZE, HASH_SIZE);
            combinedHash = DIGEST.digest(combined);
        } else {
            // If no right hash, hash the left hash alone
            combinedHash = DIGEST.digest(leftHash);
        }

        int level = left != null ? left.level() + 1 : 1;
        return new MerkleNode(index, level, combinedHash, left, right);
    }

    /// Checks if this node is a leaf node (has no children).
    ///
    /// @return true if this is a leaf node, false otherwise
    public boolean isLeaf() {
        return left == null && right == null;
    }

    /// Computes the start byte offset for this node in the original data.
    ///
    /// For leaf nodes, this is based on the index and chunk size.
    /// For internal nodes, it's the start offset of the leftmost leaf descendant.
    ///
    /// @param totalSize The total size of the original data
    /// @param chunkSize The size of each chunk
    /// @return The start byte offset
    public long startOffset(long totalSize, long chunkSize) {
        if (!isLeaf()) {
            return left.startOffset(totalSize, chunkSize);
        }
        return (long) index * chunkSize;
    }

    /// Computes the end byte offset for this node in the original data.
    ///
    /// For leaf nodes, this is based on the index and chunk size.
    /// For internal nodes, it's the end offset of the rightmost leaf descendant.
    ///
    /// @param totalSize The total size of the original data
    /// @param chunkSize The size of each chunk
    /// @return The end byte offset
    public long endOffset(long totalSize, long chunkSize) {
        if (!isLeaf()) {
            return right != null ? right.endOffset(totalSize, chunkSize)
                               : left.endOffset(totalSize, chunkSize);
        }
        return Math.min(totalSize, ((long) index + 1) * chunkSize);
    }

    /// Finds the leaf node containing the given byte offset in the original data.
    ///
    /// Traverses the tree to locate the appropriate leaf node.
    ///
    /// @param offset The byte offset to find
    /// @param totalSize The total size of the original data
    /// @param chunkSize The size of each chunk
    /// @return The leaf node containing the offset, or null if not found
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

    /// Updates the hash value of this node.
    ///
    /// @param newHash The new hash value to set
    /// @throws IllegalArgumentException if the hash length is invalid
    public void updateHash(byte[] newHash) {
        if (newHash.length != HASH_SIZE) {
            throw new IllegalArgumentException(
                "Invalid hash length: expected " + HASH_SIZE +
                ", got " + newHash.length
            );
        }

        // Copy the new hash to the existing hash array
        System.arraycopy(newHash, 0, hash, 0, HASH_SIZE);
    }
}
