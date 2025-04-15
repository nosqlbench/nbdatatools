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


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/// Utility class for building Merkle trees from files.
///
/// This class provides methods to create Merkle trees for files, which can be used
/// for efficient verification and partial downloads. It supports creating trees
/// for both regular files and zero-filled files.
///
public class MerkleTreeBuilder {
    private static final String HASH_ALGORITHM = "SHA-256";
    private static final int HASH_SIZE = 32; // SHA-256 produces 32 byte hashes

    // Pre-computed SHA-256 hash of an all-zero buffer of any size
    private static final byte[] ZERO_BLOCK_HASH = new byte[] {
        (byte)0xe3, (byte)0xb0, (byte)0xc4, (byte)0x42, (byte)0x98, (byte)0xfc, (byte)0x1c, (byte)0x14,
        (byte)0x9a, (byte)0xfb, (byte)0xf4, (byte)0xc8, (byte)0x99, (byte)0x6f, (byte)0xb9, (byte)0x24,
        (byte)0x27, (byte)0xae, (byte)0x41, (byte)0xe4, (byte)0x64, (byte)0x9b, (byte)0x93, (byte)0x4c,
        (byte)0xa4, (byte)0x95, (byte)0x99, (byte)0x1b, (byte)0x78, (byte)0x52, (byte)0xb8, (byte)0x55
    };

    /// Builds a Merkle tree for a zero-filled file of known size without reading the file.
    ///
    /// @param fileSize    the size of the zero-filled file in bytes
    /// @param minSection  minimum section size in bytes
    /// @param maxSection  maximum section size in bytes
    /// @return ByteBuffer containing the serialized Merkle tree
    public static ByteBuffer buildZeroFileMerkleTree(long fileSize, long minSection, long maxSection) {
        List<Long> offsets = MerkleTreeRanger.computeMerkleOffsets(fileSize, minSection, maxSection);

        // Calculate number of leaf nodes and total tree size
        int leafNodes = offsets.size() - 1;
        int totalNodes = 2 * leafNodes - 1;
        ByteBuffer treeBuffer = ByteBuffer.allocate(totalNodes * HASH_SIZE);

        // Create list of leaf hashes - all identical for zero blocks
        List<byte[]> leafHashes = new ArrayList<>(leafNodes);
        for (int i = 0; i < leafNodes; i++) {
            leafHashes.add(ZERO_BLOCK_HASH);
        }

        // Build the tree bottom-up
        buildTree(treeBuffer, leafHashes);

        treeBuffer.flip();
        return treeBuffer;
    }

    /// Computes a Merkle tree for the given file using the partition boundaries from MerkleTreeRanger.
    ///
    /// @param filePath    the path to the file to process
    /// @param minSection  minimum section size in bytes
    /// @param maxSection  maximum section size in bytes
    /// @return ByteBuffer containing the serialized Merkle tree
    /// @throws IOException if there are file operations errors
    public static ByteBuffer buildMerkleTree(Path filePath, long minSection, long maxSection) throws IOException {
        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            long fileSize = channel.size();
            List<Long> offsets = MerkleTreeRanger.computeMerkleOffsets(fileSize, minSection, maxSection);

            // Calculate number of leaf nodes and total tree size
            int leafNodes = offsets.size() - 1;
            int totalNodes = 2 * leafNodes - 1;
            ByteBuffer treeBuffer = ByteBuffer.allocate(totalNodes * HASH_SIZE);

            // Compute leaf node hashes
            List<byte[]> leafHashes = computeLeafHashes(channel, offsets);

            // Build the tree bottom-up
            buildTree(treeBuffer, leafHashes);

            treeBuffer.flip();
            return treeBuffer;
        }
    }

    /// Computes the hash values for each leaf section of the file.
    ///
    /// @param channel the file channel to read from
    /// @param offsets the list of section boundary offsets
    /// @return a list of hash values for each leaf section
    /// @throws IOException if there are file operations errors
    private static List<byte[]> computeLeafHashes(FileChannel channel, List<Long> offsets) throws IOException {
        List<byte[]> leafHashes = new ArrayList<>();
        MessageDigest digest = getMessageDigest();

        for (int i = 0; i < offsets.size() - 1; i++) {
            long start = offsets.get(i);
            long end = offsets.get(i + 1);
            long length = end - start;

            // Use a reasonable buffer size for reading file chunks
            ByteBuffer buffer = ByteBuffer.allocate((int) Math.min(length, 1024 * 1024));

            channel.position(start);
            digest.reset();

            while (channel.position() < end) {
                int bytesRead = channel.read(buffer);
                if (bytesRead == -1) break;

                buffer.flip();
                digest.update(buffer);
                buffer.clear();
            }

            leafHashes.add(digest.digest());
        }

        return leafHashes;
    }

    /// Builds the Merkle tree from the leaf hashes and writes it to the buffer.
    ///
    /// @param treeBuffer the buffer to write the tree to
    /// @param leafHashes the list of leaf node hashes
    private static void buildTree(ByteBuffer treeBuffer, List<byte[]> leafHashes) {
        List<byte[]> currentLevel = new ArrayList<>(leafHashes);
        MessageDigest digest = getMessageDigest();

        // Write leaf hashes to the buffer
        for (byte[] hash : leafHashes) {
            treeBuffer.put(hash);
        }

        // Build upper levels of the tree
        while (currentLevel.size() > 1) {
            List<byte[]> nextLevel = new ArrayList<>();

            for (int i = 0; i < currentLevel.size() - 1; i += 2) {
                digest.reset();
                digest.update(currentLevel.get(i));
                digest.update(currentLevel.get(i + 1));
                byte[] parentHash = digest.digest();
                nextLevel.add(parentHash);
                treeBuffer.put(parentHash);
            }

            // Handle odd number of nodes
            if (currentLevel.size() % 2 == 1) {
                byte[] lastHash = currentLevel.get(currentLevel.size() - 1);
                nextLevel.add(lastHash);
                treeBuffer.put(lastHash);
            }

            currentLevel = nextLevel;
        }
    }

    /// Gets a MessageDigest instance for the hash algorithm used by the Merkle tree.
    ///
    /// @return a MessageDigest instance
    /// @throws RuntimeException if the hash algorithm is not available
    private static MessageDigest getMessageDigest() {
        try {
            return MessageDigest.getInstance(HASH_ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to initialize SHA-256 digest", e);
        }
    }
}
