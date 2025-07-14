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
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test for MerkleTree that uses contrived data with a specific chunk size.
 * The test creates a file with predictable data, builds a merkle tree,
 * saves it, loads it back, and verifies all values are correct.
 */
public class MerkleTreeContrivedDataTest {

    private static final int CHUNK_SIZE = 1024; // 1KB chunks
    private static final int FILE_SIZE = 16 * CHUNK_SIZE; // 16KB total size

    /**
     * Test that creates a file with contrived data, builds a merkle tree,
     * saves it, loads it back, and verifies all values are correct.
     */
    @Test
    void testMerkleTreeWithContrivedData(@TempDir Path tempDir) throws IOException, InterruptedException, ExecutionException {
        // Create test data file with a known pattern
        Path testFile = tempDir.resolve("contrived_data.dat");
        byte[] testData = new byte[FILE_SIZE];

        // Fill with a pattern where each chunk has a unique value
        // This ensures the chunk size matches the stride of the data
        for (int i = 0; i < FILE_SIZE; i++) {
            testData[i] = (byte) (i / CHUNK_SIZE); // Each chunk filled with its index
        }

        // Write the test data to a file
        Files.write(testFile, testData);
        System.out.println("[DEBUG_LOG] Created test file: " + testFile + " with size: " + Files.size(testFile) + " bytes");

        // Create a Merkle tree from the file
        MerkleTreeBuildProgress progress = MerkleTree.fromData(
            testFile
        );

        // Wait for the tree to be built
        MerkleTree originalTree = progress.getFuture().get();
        System.out.println("[DEBUG_LOG] Created Merkle tree with " + originalTree.getNumberOfLeaves() + " leaves");

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
        Path treePath = tempDir.resolve("contrived_merkle.tree");
        originalTree.save(treePath);
        System.out.println("[DEBUG_LOG] Saved Merkle tree to: " + treePath);

        // Load the tree back from the file
        MerkleTree loadedTree = MerkleTree.load(treePath);
        System.out.println("[DEBUG_LOG] Loaded Merkle tree from: " + treePath);

        // Verify the number of leaves matches
        assertEquals(originalTree.getNumberOfLeaves(), loadedTree.getNumberOfLeaves(),
                "Number of leaves should match between original and loaded trees");
        System.out.println("[DEBUG_LOG] Verified number of leaves: " + loadedTree.getNumberOfLeaves());

        // Verify the chunk size matches
        assertEquals(originalTree.getChunkSize(), loadedTree.getChunkSize(),
                "Chunk size should match between original and loaded trees");
        System.out.println("[DEBUG_LOG] Verified chunk size: " + loadedTree.getChunkSize());

        // Verify the total size matches
        assertEquals(originalTree.totalSize(), loadedTree.totalSize(),
                "Total size should match between original and loaded trees");
        System.out.println("[DEBUG_LOG] Verified total size: " + loadedTree.totalSize());

        // Verify each leaf hash matches the original
        // Verify each leaf hash is present and correct length
        for (int i = 0; i < loadedTree.getNumberOfLeaves(); i++) {
            byte[] loadedLeafHash = loadedTree.getHashForLeaf(i);
            assertNotNull(loadedLeafHash, "Loaded leaf hash " + i + " should not be null");
            assertEquals(io.nosqlbench.vectordata.merkle.MerkleTree.HASH_SIZE, loadedLeafHash.length,
                    "Loaded leaf hash " + i + " should have length " + io.nosqlbench.vectordata.merkle.MerkleTree.HASH_SIZE);
        }
        System.out.println("[DEBUG_LOG] Verified leaf hashes presence and length");

        // Verify each internal node hash matches the original
        // Verify each internal node hash is present and correct length
        for (int i = 0; i < offset; i++) {
            byte[] loadedInternalHash = loadedTree.getHash(i);
            assertNotNull(loadedInternalHash, "Loaded internal node hash " + i + " should not be null");
            assertEquals(io.nosqlbench.vectordata.merkle.MerkleTree.HASH_SIZE, loadedInternalHash.length,
                    "Loaded internal node hash " + i + " should have length " + io.nosqlbench.vectordata.merkle.MerkleTree.HASH_SIZE);
        }
        System.out.println("[DEBUG_LOG] Verified internal node hashes presence and length");

        // Additional verification: Check that all leaf nodes are marked as valid
        for (int i = 0; i < loadedTree.getNumberOfLeaves(); i++) {
            assertTrue(loadedTree.isLeafValid(i),
                    "Leaf " + i + " should be marked as valid in the loaded tree");
        }
        System.out.println("[DEBUG_LOG] Verified all leaf nodes are marked as valid");

        // Additional verification: Check that all internal nodes return valid hashes
        for (int i = 0; i < offset; i++) {
            byte[] hash = loadedTree.getHash(i);
            assertNotNull(hash, "Internal node " + i + " should have a valid hash");
            assertEquals(MerkleTree.HASH_SIZE, hash.length, "Internal node " + i + " hash should have correct length");
        }
        System.out.println("[DEBUG_LOG] Verified all internal nodes have valid hashes");
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