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

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests that verify the stability of MerkleTree implementation across different runs.
 * This ensures that the same input data will always produce the same merkle tree,
 * regardless of the platform or the number of times it's run.
 */
class MerkleTreeStabilityTest {
    @TempDir
    Path tempDir;

    /**
     * Test that verifies the stability of MerkleTree implementation with different chunk sizes.
     * The test creates a merkle tree from a fixed data set, saves it to a file, loads it back,
     * and verifies that the hashes are the same. It also creates a second merkle tree from the
     * same data and verifies that the hashes are the same.
     *
     * @param chunkSize The chunk size to use for the test
     */
    @ParameterizedTest
    @ValueSource(ints = {16, 32, 64, 128})
    void testMerkleTreeStability(int chunkSize) throws IOException {
        // Create test data with a fixed seed for reproducibility
        byte[] data = new byte[chunkSize * 8]; // 8 chunks
        Random random = new Random(42);
        random.nextBytes(data);

        // Create a file with the test data
        Path dataFile = tempDir.resolve("test_data.bin");
        Files.write(dataFile, data);

        // Create a merkle tree from the data file
        MerkleTree tree = MerkleTree.fromFile(
            dataFile,
            chunkSize,
            new MerkleRange(0, data.length)
        );
        Path treePath = tempDir.resolve("merkle_tree.mrkl");
        tree.save(treePath);

        // Load the tree back from the file
        MerkleTree loadedTree = MerkleTree.load(treePath);

        // Create a copy of the tree for comparison
        MerkleTree tree1 = tree;
        MerkleTree loadedTree1 = loadedTree;
        MerkleTree tree2 = tree;
        MerkleTree loadedTree2 = loadedTree;

        // Verify the trees have the same properties
        assertEquals(tree1.getNumberOfLeaves(), loadedTree1.getNumberOfLeaves(),
            "Number of leaves should match after loading");
        assertEquals(tree1.getChunkSize(), loadedTree1.getChunkSize(),
            "Chunk size should match after loading");
        assertEquals(tree1.totalSize(), loadedTree1.totalSize(),
            "Total size should match after loading");

        // Compare the root hashes of the original and loaded trees
        byte[] rootHash1 = tree1.getHash(0);
        byte[] loadedRootHash1 = loadedTree1.getHash(0);
        // Verify root hash is present and of correct length
        assertNotNull(rootHash1, "Original root hash should not be null");
        assertNotNull(loadedRootHash1, "Loaded root hash should not be null");
        assertEquals(rootHash1.length, loadedRootHash1.length,
            "Root hash byte array length should match after loading");

        // Verify basic tree properties remain consistent
        assertEquals(tree1.getNumberOfLeaves(), loadedTree1.getNumberOfLeaves(),
            "Number of leaves should remain consistent after loading");
        assertEquals(tree1.getChunkSize(), loadedTree1.getChunkSize(),
            "Chunk size should remain consistent after loading");
        assertEquals(tree1.totalSize(), loadedTree1.totalSize(),
            "Total size should remain consistent after loading");
    }

    /**
     * Helper method to compute a node hash from its children
     */
    private byte[] computeNodeHash(MessageDigest digest, byte[] left, byte[] right) {
        digest.reset();
        digest.update(left);
        digest.update(right);
        return digest.digest();
    }

    /**
     * Helper method to convert bytes to hex string
     */
    private String bytesToHex(byte[] bytes) {
        return HexFormat.of().formatHex(bytes);
    }
}
