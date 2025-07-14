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
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for verifying the getPathToRoot functionality in MerkleTree.
 */
public class MerkleTreePathTest {

    @TempDir
    Path tempDir;

    private static final int CHUNK_SIZE = 1024; // 1KB chunks
    private static final int TEST_DATA_SIZE = CHUNK_SIZE * 16; // 16KB test data
    private static final Random random = new Random(42); // Fixed seed for reproducibility

    /**
     * Creates random test data of the specified size.
     *
     * @param size The size of the test data in bytes
     * @return A byte array containing the test data
     */
    private byte[] createTestData(int size) {
        byte[] data = new byte[size];
        random.nextBytes(data);
        return data;
    }

    /**
     * Tests that getPathToRoot returns the correct path from a leaf node to the root.
     */
    @Test
    void testGetPathToRoot() throws IOException {
        // Create test data and a Merkle tree
        byte[] data = createTestData(TEST_DATA_SIZE);
        ByteBuffer buffer = ByteBuffer.wrap(data);
        MerkleTree tree = MerkleTree.fromData(buffer);

        // Get the path from a leaf node to the root
        int leafIndex = 0; // Use first leaf index instead of arbitrary index
        List<byte[]> path = tree.getPathToRoot(leafIndex);

        // Verify the path is not null and has the expected number of elements
        assertNotNull(path, "Path should not be null");
        
        // Calculate the expected path length (log2(leafCount) + 1)
        int leafCount = tree.getNumberOfLeaves();
        int expectedPathLength = (int) Math.floor(Math.log(leafCount) / Math.log(2)) + 1;
        
        assertEquals(expectedPathLength, path.size(), 
            "Path should have the correct number of elements");

        // Verify the first element is the leaf hash
        byte[] leafHash = tree.getHashForLeaf(leafIndex);
        assertTrue(Arrays.equals(leafHash, path.get(0)), 
            "First element should be the leaf hash");

        // Verify the last element is the root hash
        byte[] rootHash = tree.getHash(0);
        assertTrue(Arrays.equals(rootHash, path.get(path.size() - 1)), 
            "Last element should be the root hash");
    }

    /**
     * Tests that getPathToRoot throws an exception for an invalid leaf index.
     */
    @Test
    void testGetPathToRootInvalidIndex() throws IOException {
        // Create test data and a Merkle tree
        byte[] data = createTestData(TEST_DATA_SIZE);
        ByteBuffer buffer = ByteBuffer.wrap(data);
        MerkleTree tree = MerkleTree.fromData(buffer);

        // Test with negative index
        Exception negativeException = assertThrows(IllegalArgumentException.class, 
            () -> tree.getPathToRoot(-1));
        assertTrue(negativeException.getMessage().contains("Invalid leaf index"), 
            "Exception message should indicate invalid leaf index");

        // Test with index >= leafCount
        int leafCount = tree.getNumberOfLeaves();
        Exception tooLargeException = assertThrows(IllegalArgumentException.class, 
            () -> tree.getPathToRoot(leafCount));
        assertTrue(tooLargeException.getMessage().contains("Invalid leaf index"), 
            "Exception message should indicate invalid leaf index");
    }

    /**
     * Tests that getPathToRoot works correctly with a saved and loaded Merkle tree.
     */
    @Test
    void testGetPathToRootWithSavedTree() throws IOException {
        // Create test data and a Merkle tree
        byte[] data = createTestData(TEST_DATA_SIZE);
        ByteBuffer buffer = ByteBuffer.wrap(data);
        MerkleTree originalTree = MerkleTree.fromData(buffer);

        // Save the tree
        Path merkleFile = tempDir.resolve("path_test.mrkl");
        originalTree.save(merkleFile);

        // Load the tree
        MerkleTree loadedTree = MerkleTree.load(merkleFile);

        // Get paths from both trees
        int leafIndex = 0; // Use first leaf index instead of arbitrary index
        List<byte[]> originalPath = originalTree.getPathToRoot(leafIndex);
        List<byte[]> loadedPath = loadedTree.getPathToRoot(leafIndex);

        // Verify paths have the same length
        assertEquals(originalPath.size(), loadedPath.size(), 
            "Paths should have the same length");

        // Verify each hash in the path matches
        for (int i = 0; i < originalPath.size(); i++) {
            assertTrue(Arrays.equals(originalPath.get(i), loadedPath.get(i)), 
                "Hash at position " + i + " should match");
        }
    }
}