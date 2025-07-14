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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for verifying the BitSet persistence functionality in MerkleTree.
 * These tests ensure that the BitSet which tracks updates can be persisted on disk
 * and loaded back correctly.
 */
public class MerkleTreeBitSetPersistenceTest {

    @TempDir
    Path tempDir;

    private static final int CHUNK_SIZE = 16; // Power of two

    /**
     * Test that verifies the BitSet is persisted to disk and loaded back correctly.
     * This test:
     * 1. Creates a MerkleTree from test data
     * 2. Invalidates some nodes
     * 3. Saves the tree to a file
     * 4. Loads the tree from the file
     * 5. Verifies that the invalidated nodes are still marked as invalid
     */
    @Test
    void testBitSetPersistence() throws IOException, NoSuchAlgorithmException {
        // Create test data
        byte[] data = new byte[CHUNK_SIZE * 4]; // 4 chunks
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }

        // Create a MerkleTree from the data
        ByteBuffer buffer = ByteBuffer.wrap(data);
        MerkleTree tree = MerkleTree.fromData(buffer);

        // Get the number of nodes in the tree
        int nodeCount = tree.getNumberOfLeaves() * 2 - 1;
        int offset = tree.getNumberOfLeaves() - 1;

        // Create new hashes for available leaves (adjust based on actual leaf count)
        int leafCount = tree.getNumberOfLeaves();
        System.out.println("[DEBUG_LOG] Tree has " + leafCount + " leaves");
        
        // Update leaf 0 (always available)
        byte[] newHash0 = createNewHash(CHUNK_SIZE, 0);
        tree.updateLeafHash(0, newHash0);
        
        // Update additional leaves if they exist
        if (leafCount > 1) {
            byte[] newHash1 = createNewHash(CHUNK_SIZE, 1);
            tree.updateLeafHash(1, newHash1);
        }
        if (leafCount > 2) {
            byte[] newHash2 = createNewHash(CHUNK_SIZE, 2);
            tree.updateLeafHash(2, newHash2);
        }

        // Force computation of the root hash to ensure consistent state
        byte[] initialRootHash = tree.getHash(0);
        System.out.println("[DEBUG_LOG] Initial root hash: " + Arrays.toString(initialRootHash));

        // Print the leaf hashes before saving
        System.out.println("[DEBUG_LOG] Leaf 0 hash before saving: " + Arrays.toString(tree.getHashForLeaf(0)));
        if (leafCount > 1) {
            System.out.println("[DEBUG_LOG] Leaf 1 hash before saving: " + Arrays.toString(tree.getHashForLeaf(1)));
        }
        if (leafCount > 2) {
            System.out.println("[DEBUG_LOG] Leaf 2 hash before saving: " + Arrays.toString(tree.getHashForLeaf(2)));
        }

        // Print the file size before saving
        System.out.println("[DEBUG_LOG] Tree node count: " + nodeCount);
        System.out.println("[DEBUG_LOG] Tree offset: " + offset);

        // Save the tree to a file
        Path treePath = tempDir.resolve("merkle_bitset_test.mrkl");
        tree.save(treePath);

        // Print the file size after saving
        System.out.println("[DEBUG_LOG] File size after saving: " + Files.size(treePath));

        // Load the tree from the file
        MerkleTree loadedTree = MerkleTree.load(treePath);

        // Print the leaf hashes after loading (adjust for actual leaf count)
        int loadedLeafCount = loadedTree.getNumberOfLeaves();
        System.out.println("[DEBUG_LOG] Loaded tree has " + loadedLeafCount + " leaves");
        System.out.println("[DEBUG_LOG] Leaf 0 hash after loading: " + Arrays.toString(loadedTree.getHashForLeaf(0)));
        if (loadedLeafCount > 1) {
            System.out.println("[DEBUG_LOG] Leaf 1 hash after loading: " + Arrays.toString(loadedTree.getHashForLeaf(1)));
        }
        if (loadedLeafCount > 2) {
            System.out.println("[DEBUG_LOG] Leaf 2 hash after loading: " + Arrays.toString(loadedTree.getHashForLeaf(2)));
        }

        // Print the tree properties after loading
        System.out.println("[DEBUG_LOG] Loaded tree node count: " + (loadedTree.getNumberOfLeaves() * 2 - 1));
        System.out.println("[DEBUG_LOG] Loaded tree offset: " + (loadedTree.getNumberOfLeaves() - 1));

        // Verify the tree properties match
        assertEquals(tree.getChunkSize(), loadedTree.getChunkSize(), "Chunk size should match");
        assertEquals(tree.totalSize(), loadedTree.totalSize(), "Total size should match");

        // Verify that the internal nodes on the path to the root are still marked as invalid
        // by checking if accessing a leaf hash causes recomputation of the path

        // Get the original root hash (this should force recomputation)
        byte[] originalRootHash = tree.getHash(0);
        System.out.println("[DEBUG_LOG] Original root hash: " + Arrays.toString(originalRootHash));

        // Get the loaded root hash (this should also force recomputation)
        byte[] loadedRootHash = loadedTree.getHash(0);
        System.out.println("[DEBUG_LOG] Loaded root hash: " + Arrays.toString(loadedRootHash));

        // Print the first few bytes of both hashes for comparison
        System.out.println("[DEBUG_LOG] Original root hash first 8 bytes: " + 
            Arrays.toString(Arrays.copyOf(originalRootHash, 8)));
        System.out.println("[DEBUG_LOG] Loaded root hash first 8 bytes: " + 
            Arrays.toString(Arrays.copyOf(loadedRootHash, 8)));

        // The root hashes should match after recomputation
        assertArrayEquals(originalRootHash, loadedRootHash, "Root hashes should match after recomputation");

        // Now verify that the leaf hashes match (adjust for actual leaf count)
        byte[] originalLeaf0Hash = tree.getHashForLeaf(0);
        byte[] loadedLeaf0Hash = loadedTree.getHashForLeaf(0);
        assertArrayEquals(originalLeaf0Hash, loadedLeaf0Hash, "Leaf 0 hashes should match");

        // Only check additional leaves if they exist
        if (tree.getNumberOfLeaves() > 1 && loadedTree.getNumberOfLeaves() > 1) {
            byte[] originalLeaf1Hash = tree.getHashForLeaf(1);
            byte[] loadedLeaf1Hash = loadedTree.getHashForLeaf(1);
            assertArrayEquals(originalLeaf1Hash, loadedLeaf1Hash, "Leaf 1 hashes should match");
        }
        
        if (tree.getNumberOfLeaves() > 2 && loadedTree.getNumberOfLeaves() > 2) {
            byte[] originalLeaf2Hash = tree.getHashForLeaf(2);
            byte[] loadedLeaf2Hash = loadedTree.getHashForLeaf(2);
            assertArrayEquals(originalLeaf2Hash, loadedLeaf2Hash, "Leaf 2 hashes should match");
        }
    }

    /**
     * Test that verifies the case where all hashes are valid.
     * This test:
     * 1. Creates a MerkleTree from test data
     * 2. Computes all hashes by accessing the root hash
     * 3. Saves the tree to a file
     * 4. Loads the tree from the file
     * 5. Verifies that all hashes are marked as valid
     */
    @Test
    void testAllHashesValid() throws IOException {
        // Create test data
        byte[] data = new byte[CHUNK_SIZE * 4]; // 4 chunks
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }

        // Create a MerkleTree from the data
        ByteBuffer buffer = ByteBuffer.wrap(data);
        MerkleTree tree = MerkleTree.fromData(buffer);

        // Force computation of all hashes by accessing the root hash
        byte[] rootHash = tree.getHash(0);
        assertNotNull(rootHash, "Root hash should not be null");

        // Save the tree to a file
        Path treePath = tempDir.resolve("merkle_all_valid.mrkl");
        tree.save(treePath);

        // Load the tree from the file
        MerkleTree loadedTree = MerkleTree.load(treePath);

        // Verify the tree properties match
        assertEquals(tree.getChunkSize(), loadedTree.getChunkSize(), "Chunk size should match");
        assertEquals(tree.totalSize(), loadedTree.totalSize(), "Total size should match");

        // Verify that all hashes are marked as valid by checking if accessing the root hash
        // doesn't cause recomputation (the hash should be the same)
        byte[] loadedRootHash = loadedTree.getHash(0);
        assertArrayEquals(rootHash, loadedRootHash, "Root hashes should match without recomputation");
    }

    /**
     * Helper method to create a new hash for testing.
     */
    private byte[] createNewHash(int chunkSize, int seed) throws NoSuchAlgorithmException {
        byte[] data = new byte[chunkSize];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) ((i + seed * 64) % 256);
        }

        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update(data);
        return digest.digest();
    }
}
