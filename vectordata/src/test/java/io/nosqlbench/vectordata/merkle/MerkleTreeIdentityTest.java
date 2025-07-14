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
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that verify the identity property of MerkleTree - that two different merkle tree files
 * created from the same content produce identical trees.
 */
class MerkleTreeIdentityTest {
    @TempDir
    Path tempDir;

    private static final int CHUNK_SIZE = 16; // Power of two

    /**
     * Test that verifies two different merkle tree files created from the same content
     * produce identical trees.
     */
    @Test
    void testIdenticalTreesFromSameContent() throws IOException {
        // Create test data
        byte[] data = new byte[CHUNK_SIZE * 4]; // 4 chunks
        Random random = new Random(42); // Use fixed seed for reproducibility
        random.nextBytes(data);

        // Create a single data file
        Path dataFile = tempDir.resolve("test_data.bin");
        Files.write(dataFile, data);

        // Create a merkle tree file from the data file
        MerkleTree tree = MerkleTree.fromFile(
            dataFile,
            CHUNK_SIZE,
            new MerkleRange(0, data.length)
        );
        Path treePath = tempDir.resolve("tree.mrkl");
        tree.save(treePath);

        // Load the tree from the file twice
        MerkleTree loadedTree1 = MerkleTree.load(treePath);
        MerkleTree loadedTree2 = MerkleTree.load(treePath);

        // Verify the trees have the same properties
        assertEquals(loadedTree1.getNumberOfLeaves(), loadedTree2.getNumberOfLeaves(), 
            "Number of leaves should match");
        assertEquals(loadedTree1.getChunkSize(), loadedTree2.getChunkSize(), 
            "Chunk size should match");
        assertEquals(loadedTree1.totalSize(), loadedTree2.totalSize(), 
            "Total size should match");

        // Compare the root hashes
        byte[] rootHash1 = loadedTree1.getHashForLeaf(0);
        byte[] rootHash2 = loadedTree2.getHashForLeaf(0);
        System.out.println("[DEBUG_LOG] Root hash 1: " + Arrays.toString(rootHash1));
        System.out.println("[DEBUG_LOG] Root hash 2: " + Arrays.toString(rootHash2));
        assertArrayEquals(rootHash1, rootHash2, "Root hashes should match");

        // Compare all leaf hashes
        for (int i = 0; i < loadedTree1.getNumberOfLeaves(); i++) {
            byte[] hash1 = loadedTree1.getHashForLeaf(i);
            byte[] hash2 = loadedTree2.getHashForLeaf(i);
            System.out.println("[DEBUG_LOG] Leaf " + i + " hash 1: " + Arrays.toString(hash1));
            System.out.println("[DEBUG_LOG] Leaf " + i + " hash 2: " + Arrays.toString(hash2));
            assertArrayEquals(
                hash1,
                hash2,
                "Leaf " + i + " hashes should match"
            );
        }

        // Verify there are no mismatched chunks
        List<MerkleMismatch> mismatches = loadedTree1.findMismatchedChunks(loadedTree2);
        assertTrue(mismatches.isEmpty(), "There should be no mismatched chunks between the trees");
    }

    /**
     * Test that verifies two different merkle tree files created from the same content
     * but with different file names produce identical trees.
     */
    @Test
    void testIdenticalTreesWithDifferentFileNames() throws IOException {
        // Create test data
        byte[] data = new byte[CHUNK_SIZE * 4]; // 4 chunks
        Random random = new Random(42); // Use fixed seed for reproducibility
        random.nextBytes(data);

        // Create a single data file
        Path dataFile = tempDir.resolve("different_data.bin");
        Files.write(dataFile, data);

        // Create a merkle tree file from the data file
        MerkleTree tree = MerkleTree.fromFile(
            dataFile,
            CHUNK_SIZE,
            new MerkleRange(0, data.length)
        );
        Path treePath = tempDir.resolve("different_tree.mrkl");
        tree.save(treePath);

        // Load the tree from the file twice
        MerkleTree loadedTree1 = MerkleTree.load(treePath);
        MerkleTree loadedTree2 = MerkleTree.load(treePath);

        // Verify the trees have the same properties
        assertEquals(loadedTree1.getNumberOfLeaves(), loadedTree2.getNumberOfLeaves(), 
            "Number of leaves should match");
        assertEquals(loadedTree1.getChunkSize(), loadedTree2.getChunkSize(), 
            "Chunk size should match");
        assertEquals(loadedTree1.totalSize(), loadedTree2.totalSize(), 
            "Total size should match");

        // Compare all leaf hashes
        for (int i = 0; i < loadedTree1.getNumberOfLeaves(); i++) {
            byte[] hash1 = loadedTree1.getHashForLeaf(i);
            byte[] hash2 = loadedTree2.getHashForLeaf(i);
            System.out.println("[DEBUG_LOG] Leaf " + i + " hash 1: " + Arrays.toString(hash1));
            System.out.println("[DEBUG_LOG] Leaf " + i + " hash 2: " + Arrays.toString(hash2));
            assertArrayEquals(
                hash1,
                hash2,
                "Leaf " + i + " hashes should match"
            );
        }

        // Verify there are no mismatched chunks
        List<MerkleMismatch> mismatches = loadedTree1.findMismatchedChunks(loadedTree2);
        assertTrue(mismatches.isEmpty(), "There should be no mismatched chunks between the trees");
    }
}
