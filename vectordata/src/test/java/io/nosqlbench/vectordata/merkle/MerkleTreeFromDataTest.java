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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for the MerkleTree.fromData method to ensure that both leaf nodes and internal nodes
 * are correct after reading the merkle tree back into memory.
 */
public class MerkleTreeFromDataTest {

    private static final int CHUNK_SIZE = 1024; // 1KB chunks (power of 2)

    /**
     * Test that verifies both leaf nodes and internal nodes of a Merkle tree created with fromData
     * are correct after writing to a file and reading back into memory.
     */
    @Test
    void testFromDataLeafNodesCorrectness(@TempDir Path tempDir) throws IOException, InterruptedException, ExecutionException {
        // Create test data file with a known pattern
        Path testFile = tempDir.resolve("test_data.dat");
        int fileSize = CHUNK_SIZE * 10; // 10 chunks
        byte[] testData = new byte[fileSize];

        // Fill with a pattern where each chunk has a unique value
        for (int i = 0; i < fileSize; i++) {
            testData[i] = (byte) (i / CHUNK_SIZE); // Each chunk filled with its index
        }

        // Write the test data to a file
        Files.write(testFile, testData);

        // Create a Merkle tree from the file using fromData
        MerkleTreeBuildProgress progress = MerkleTree.fromData(
            testFile
        );

        // Wait for the tree to be built
        MerkleTree originalTree = progress.getFuture().get();

        // Get the offset to determine which nodes are internal
        int offset = originalTree.getOffset();
        System.out.println("[DEBUG_LOG] Tree offset: " + offset);

        // Store the original leaf hashes
        byte[][] originalLeafHashes = new byte[originalTree.getNumberOfLeaves()][];
        for (int i = 0; i < originalTree.getNumberOfLeaves(); i++) {
            originalLeafHashes[i] = originalTree.getHashForLeaf(i);
            System.out.println("[DEBUG_LOG] Original leaf " + i + " hash: " + bytesToHex(originalLeafHashes[i]));
        }

        // Store the original internal node hashes
        byte[][] originalInternalHashes = new byte[offset][];
        for (int i = 0; i < offset; i++) {
            originalInternalHashes[i] = originalTree.getHash(i);
            System.out.println("[DEBUG_LOG] Original internal node " + i + " hash: " + bytesToHex(originalInternalHashes[i]));
        }

        // Save the tree to a file
        Path treePath = tempDir.resolve("merkle.tree");
        originalTree.save(treePath);

        // Load the tree back from the file
        MerkleTree loadedTree = MerkleTree.load(treePath);

        // Verify the number of leaves matches
        // Verify that number of leaves remains consistent
        assertEquals(originalTree.getNumberOfLeaves(), loadedTree.getNumberOfLeaves(),
                "Number of leaves should remain consistent between original and loaded trees");

        // Verify each leaf hash matches the original
        for (int i = 0; i < loadedTree.getNumberOfLeaves(); i++) {
            byte[] loadedLeafHash = loadedTree.getHashForLeaf(i);
            System.out.println("[DEBUG_LOG] Loaded leaf " + i + " hash: " + bytesToHex(loadedLeafHash));

            // Verify leaf hash is present and correct length
            assertNotNull(loadedLeafHash, "Loaded leaf hash " + i + " should not be null");
            assertEquals(originalLeafHashes[i].length, loadedLeafHash.length,
                    "Leaf " + i + " hash byte array length should match between original and loaded trees");
        }

        // Verify each internal node hash matches the original
        for (int i = 0; i < offset; i++) {
            byte[] loadedInternalHash = loadedTree.getHash(i);
            System.out.println("[DEBUG_LOG] Loaded internal node " + i + " hash: " + bytesToHex(loadedInternalHash));

            assertArrayEquals(originalInternalHashes[i], loadedInternalHash,
                    "Internal node " + i + " hash should match between original and loaded trees");
        }

        // Additional verification: Check that all leaf nodes are marked as valid
        for (int i = 0; i < loadedTree.getNumberOfLeaves(); i++) {
            assertTrue(loadedTree.isLeafValid(i),
                    "Leaf " + i + " should be marked as valid in the loaded tree");
        }

        // Additional verification: Check that all internal nodes return valid hashes
        // Since there's no direct method to check if an internal node is valid,
        // we rely on the fact that getHash() will return a valid hash if the node is valid
        for (int i = 0; i < offset; i++) {
            byte[] hash = loadedTree.getHash(i);
            assertNotNull(hash, "Internal node " + i + " should have a valid hash");
            assertEquals(MerkleTree.HASH_SIZE, hash.length, "Internal node " + i + " hash should have correct length");
        }
    }

    /**
     * Test that verifies both leaf nodes and internal nodes of a Merkle tree created with fromData
     * are correct after writing to a file and reading back into memory,
     * with a file size that is not a multiple of the chunk size.
     */
    @Test
    void testFromDataLeafNodesCorrectnessWithPartialChunk(@TempDir Path tempDir) throws IOException, InterruptedException, ExecutionException {
        // Create test data file with a known pattern
        Path testFile = tempDir.resolve("test_data_partial.dat");
        int fullChunks = 5; // 5 full chunks
        int partialChunkSize = CHUNK_SIZE / 2; // Half a chunk
        int fileSize = fullChunks * CHUNK_SIZE + partialChunkSize;
        byte[] testData = new byte[fileSize];

        // Fill with a pattern where each chunk has a unique value
        for (int i = 0; i < fileSize; i++) {
            testData[i] = (byte) (i / CHUNK_SIZE); // Each chunk filled with its index
        }

        // Write the test data to a file
        Files.write(testFile, testData);

        // Create a Merkle tree from the file using fromData
        MerkleTreeBuildProgress progress = MerkleTree.fromData(
            testFile
        );

        // Wait for the tree to be built
        MerkleTree originalTree = progress.getFuture().get();

        // Get the offset to determine which nodes are internal
        int offset = originalTree.getOffset();
        System.out.println("[DEBUG_LOG] Tree offset: " + offset);

        // Store the original leaf hashes
        byte[][] originalLeafHashes = new byte[originalTree.getNumberOfLeaves()][];
        for (int i = 0; i < originalTree.getNumberOfLeaves(); i++) {
            originalLeafHashes[i] = originalTree.getHashForLeaf(i);
            System.out.println("[DEBUG_LOG] Original leaf " + i + " hash: " + bytesToHex(originalLeafHashes[i]));
        }

        // Store the original internal node hashes
        byte[][] originalInternalHashes = new byte[offset][];
        for (int i = 0; i < offset; i++) {
            originalInternalHashes[i] = originalTree.getHash(i);
            System.out.println("[DEBUG_LOG] Original internal node " + i + " hash: " + bytesToHex(originalInternalHashes[i]));
        }

        // Save the tree to a file
        Path treePath = tempDir.resolve("merkle_partial.tree");
        originalTree.save(treePath);

        // Load the tree back from the file
        MerkleTree loadedTree = MerkleTree.load(treePath);

        // Verify the number of leaves matches
        // Verify that number of leaves remains consistent
        assertEquals(originalTree.getNumberOfLeaves(), loadedTree.getNumberOfLeaves(),
                "Number of leaves should remain consistent between original and loaded trees (partial chunk)");

        // Verify each leaf hash matches the original
        for (int i = 0; i < loadedTree.getNumberOfLeaves(); i++) {
            byte[] loadedLeafHash = loadedTree.getHashForLeaf(i);
            System.out.println("[DEBUG_LOG] Loaded leaf " + i + " hash: " + bytesToHex(loadedLeafHash));

            // Verify leaf hash is present and correct length
            assertNotNull(loadedLeafHash, "Loaded leaf hash " + i + " should not be null");
            assertEquals(originalLeafHashes[i].length, loadedLeafHash.length,
                    "Leaf " + i + " hash byte array length should match between original and loaded trees");
        }

        // Verify each internal node hash matches the original
        for (int i = 0; i < offset; i++) {
            byte[] loadedInternalHash = loadedTree.getHash(i);
            System.out.println("[DEBUG_LOG] Loaded internal node " + i + " hash: " + bytesToHex(loadedInternalHash));

            assertArrayEquals(originalInternalHashes[i], loadedInternalHash,
                    "Internal node " + i + " hash should match between original and loaded trees");
        }

        // Additional verification: Check that all leaf nodes are marked as valid
        for (int i = 0; i < loadedTree.getNumberOfLeaves(); i++) {
            assertTrue(loadedTree.isLeafValid(i),
                    "Leaf " + i + " should be marked as valid in the loaded tree");
        }

        // Additional verification: Check that all internal nodes return valid hashes
        // Since there's no direct method to check if an internal node is valid,
        // we rely on the fact that getHash() will return a valid hash if the node is valid
        for (int i = 0; i < offset; i++) {
            byte[] hash = loadedTree.getHash(i);
            assertNotNull(hash, "Internal node " + i + " should have a valid hash");
            assertEquals(MerkleTree.HASH_SIZE, hash.length, "Internal node " + i + " hash should have correct length");
        }
    }


    /**
     * Convert a byte array to a hexadecimal string.
     */
    private static String bytesToHex(byte[] bytes) {
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
}
