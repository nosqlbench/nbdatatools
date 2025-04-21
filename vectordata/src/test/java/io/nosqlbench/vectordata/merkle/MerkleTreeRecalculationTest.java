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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for verifying the hash recalculation functionality in MerkleTree.
 */
public class MerkleTreeRecalculationTest {

    @TempDir
    Path tempDir;

    private static final int CHUNK_SIZE = 1024; // 1KB chunks
    private static final int TEST_DATA_SIZE = CHUNK_SIZE * 8; // 8KB test data
    private static final Random random = new Random(42); // Fixed seed for reproducibility

    /**
     * Creates random test data of the specified size.
     *
     * @param size The size of the test data in bytes
     * @return A byte array containing the test data
     */
    private byte[] createTestData(int size) {
        byte[] data = new byte[size];
        // Use a fresh, fixed-seed RNG for test data to ensure consistent initial state
        new Random(42).nextBytes(data);
        return data;
    }

    /**
     * Creates a test Merkle tree from random data.
     *
     * @return A newly created Merkle tree
     */
    private MerkleTree createTestTree() {
        byte[] data = createTestData(TEST_DATA_SIZE);
        ByteBuffer buffer = ByteBuffer.wrap(data);
        return MerkleTree.fromData(
                buffer,
                CHUNK_SIZE,
                new MerkleRange(0, data.length)
        );
    }

    /**
     * Tests updating a leaf hash and verifying the change propagates correctly.
     */
    @Test
    void testUpdateLeafHashAndVerify() throws NoSuchAlgorithmException {
        // Create test data
        byte[] originalData = createTestData(TEST_DATA_SIZE);
        ByteBuffer originalBuffer = ByteBuffer.wrap(originalData);

        // Create a merkle tree from the original data
        MerkleTree tree = MerkleTree.fromData(
            originalBuffer,
            CHUNK_SIZE,
            new MerkleRange(0, originalData.length)
        );

        // Get original hash for a leaf
        int leafIndex = 3; // Use leaf index 3
        byte[] originalLeafHash = tree.getHashForLeaf(leafIndex);

        // Create a new hash (distinct constant data)
        byte[] newHashData = new byte[CHUNK_SIZE];
        Arrays.fill(newHashData, (byte) 0xFF);
        
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update(newHashData);
        byte[] newHash = digest.digest();
        
        // Update the tree with the new hash
        tree.updateLeafHash(leafIndex, newHash);
        
        // Verify the leaf hash was updated
        byte[] updatedLeafHash = tree.getHashForLeaf(leafIndex);
        assertArrayEquals(newHash, updatedLeafHash, "Leaf hash should be updated to new hash");
        assertFalse(Arrays.equals(originalLeafHash, updatedLeafHash), 
                   "Updated leaf hash should differ from original");
    }

    /**
     * Tests updating multiple leaf hashes and verifying all changes propagate.
     */
    @Test
    void testUpdateMultipleLeafHashes() throws NoSuchAlgorithmException {
        // Create a test tree
        MerkleTree tree = createTestTree();
        
        // Get the leaves and their original hashes
        int leafCount = tree.getNumberOfLeaves();
        byte[][] originalLeafHashes = new byte[leafCount][];
        
        for (int i = 0; i < leafCount; i++) {
            originalLeafHashes[i] = tree.getHashForLeaf(i);
        }
        
        // Create another tree to compare against later
        MerkleTree originalTree = createTestTree();
        
        // Update several leaf hashes
        int[] leavesToUpdate = {0, leafCount/2, leafCount-1};
        
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        for (int leafIndex : leavesToUpdate) {
            // Create a new hash (distinct constant data)
            byte[] newHashData = new byte[CHUNK_SIZE];
            Arrays.fill(newHashData, (byte) 0xFF);
            
            digest.reset();
            digest.update(newHashData);
            byte[] newHash = digest.digest();
            
            // Update the leaf hash
            tree.updateLeafHash(leafIndex, newHash);
            
            // Verify the update happened
            byte[] updatedHash = tree.getHashForLeaf(leafIndex);
            assertArrayEquals(newHash, updatedHash, 
                             "Leaf " + leafIndex + " should have been updated");
        }
        
        // Verify that only the targeted leaves were changed
        for (int i = 0; i < leafCount; i++) {
            byte[] currentHash = tree.getHashForLeaf(i);
            boolean shouldBeChanged = false;
            
            for (int leafIndex : leavesToUpdate) {
                if (i == leafIndex) {
                    shouldBeChanged = true;
                    break;
                }
            }
            
            if (shouldBeChanged) {
                assertFalse(Arrays.equals(originalLeafHashes[i], currentHash),
                           "Target leaf " + i + " hash should be changed");
            } else {
                assertArrayEquals(originalLeafHashes[i], currentHash,
                                 "Non-target leaf " + i + " hash should remain unchanged");
            }
        }
        
        // Compare trees using findMismatchedChunks
        List<MerkleMismatch> mismatches = tree.findMismatchedChunks(originalTree);
        
        // Should have exactly the number of mismatches as leaves we updated
        assertEquals(leavesToUpdate.length, mismatches.size(),
                    "Should have exactly " + leavesToUpdate.length + " mismatches");
        
        // Verify each mismatch corresponds to a leaf we updated
        for (MerkleMismatch mismatch : mismatches) {
            boolean isExpectedMismatch = false;
            for (int leafIndex : leavesToUpdate) {
                if (mismatch.chunkIndex() == leafIndex) {
                    isExpectedMismatch = true;
                    break;
                }
            }
            assertTrue(isExpectedMismatch, 
                      "Mismatch at chunk " + mismatch.chunkIndex() + " should be an expected one");
        }
    }

    /**
     * Tests saving and loading a tree with updated hashes.
     */
    @Test
    void testSaveAndLoadWithUpdatedHashes() throws IOException, NoSuchAlgorithmException {
        // Create a test tree
        MerkleTree tree = createTestTree();
        
        // Update a leaf hash
        int leafIndex = 2;
        byte[] newHashData = new byte[CHUNK_SIZE];
        random.nextBytes(newHashData);
        
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update(newHashData);
        byte[] newHash = digest.digest();
        
        tree.updateLeafHash(leafIndex, newHash);
        
        // Save the tree
        Path savePath = tempDir.resolve("updated_tree.mrkl");
        tree.save(savePath);
        
        // Load the tree
        MerkleTree loadedTree = MerkleTree.load(savePath);
        
        // Verify the loaded tree has the updated hash
        byte[] loadedHash = loadedTree.getHashForLeaf(leafIndex);
        assertArrayEquals(newHash, loadedHash, 
                         "Loaded tree should have the updated hash");
        
        // Verify no mismatches between the original and loaded tree
        List<MerkleMismatch> mismatches = tree.findMismatchedChunks(loadedTree);
        assertTrue(mismatches.isEmpty(), 
                  "There should be no mismatches between original and loaded tree");
    }

    /**
     * Tests that updating a leaf in an empty tree works correctly.
     */
    @Test
    void testUpdateEmptyTree() throws NoSuchAlgorithmException {
        // Create an empty tree
        MerkleTree emptyTree = MerkleTree.createEmpty(TEST_DATA_SIZE, CHUNK_SIZE);
        
        // Create test data for one chunk
        byte[] chunkData = new byte[CHUNK_SIZE];
        random.nextBytes(chunkData);
        
        // Hash the chunk
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update(chunkData);
        byte[] hash = digest.digest();
        
        // Update a leaf in the empty tree
        int leafIndex = 2;
        emptyTree.updateLeafHash(leafIndex, hash);
        
        // Verify the leaf was updated
        byte[] retrievedHash = emptyTree.getHashForLeaf(leafIndex);
        assertArrayEquals(hash, retrievedHash, 
                         "Leaf hash should be updated in empty tree");
    }

    /**
     * Tests that saving and loading a tree multiple times preserves tree integrity.
     */
    @Test
    void testSaveLoadMultipleTimes() throws IOException, NoSuchAlgorithmException {
        // Create a test tree
        MerkleTree originalTree = createTestTree();
        
        // Update some leaf hashes
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        for (int i = 0; i < 3; i++) {
            byte[] newData = new byte[CHUNK_SIZE];
            random.nextBytes(newData);
            digest.reset();
            digest.update(newData);
            byte[] newHash = digest.digest();
            originalTree.updateLeafHash(i, newHash);
        }
        
        // First save and load
        Path firstSavePath = tempDir.resolve("tree_save1.mrkl");
        originalTree.save(firstSavePath);
        MerkleTree firstLoadedTree = MerkleTree.load(firstSavePath);
        
        // Verify integrity after first load
        List<MerkleMismatch> firstMismatches = originalTree.findMismatchedChunks(firstLoadedTree);
        assertTrue(firstMismatches.isEmpty(), 
                  "There should be no mismatches after first load");
        
        // Second save and load
        Path secondSavePath = tempDir.resolve("tree_save2.mrkl");
        firstLoadedTree.save(secondSavePath);
        MerkleTree secondLoadedTree = MerkleTree.load(secondSavePath);
        
        // Verify integrity after second load
        List<MerkleMismatch> secondMismatches = firstLoadedTree.findMismatchedChunks(secondLoadedTree);
        assertTrue(secondMismatches.isEmpty(), 
                  "There should be no mismatches after second load");
        
        // Verify integrity compared to original
        List<MerkleMismatch> originalMismatches = originalTree.findMismatchedChunks(secondLoadedTree);
        assertTrue(originalMismatches.isEmpty(), 
                  "There should be no mismatches between original and twice-loaded tree");
    }

    /**
     * Tests creating an empty tree like an existing tree.
     */
    @Test
    void testCreateEmptyTreeLike() throws IOException, NoSuchAlgorithmException {
        // Create and save a test tree
        MerkleTree originalTree = createTestTree();
        Path originalPath = tempDir.resolve("original.mrkl");
        originalTree.save(originalPath);
        
        // Create an empty tree like the original
        Path emptyPath = tempDir.resolve("empty.mrkl");
        MerkleTree.createEmptyTreeLike(originalPath, emptyPath);
        
        // Load the empty tree
        MerkleTree emptyTree = MerkleTree.load(emptyPath);
        
        // Verify the empty tree has the same structure
        assertEquals(originalTree.getChunkSize(), emptyTree.getChunkSize(),
                    "Chunk size should match");
        assertEquals(originalTree.totalSize(), emptyTree.totalSize(),
                    "Total size should match");
        assertEquals(originalTree.getNumberOfLeaves(), emptyTree.getNumberOfLeaves(),
                    "Number of leaves should match");
        
        // Update a leaf in the empty tree to match the original
        int leafIndex = 3;
        byte[] originalHash = originalTree.getHashForLeaf(leafIndex);
        emptyTree.updateLeafHash(leafIndex, originalHash);
        
        // Save the updated empty tree
        Path updatedEmptyPath = tempDir.resolve("updated_empty.mrkl");
        emptyTree.save(updatedEmptyPath);
        
        // Load the updated empty tree
        MerkleTree updatedEmptyTree = MerkleTree.load(updatedEmptyPath);
        
        // Verify the leaf was updated
        byte[] updatedHash = updatedEmptyTree.getHashForLeaf(leafIndex);
        assertArrayEquals(originalHash, updatedHash,
                         "Leaf hash should match after update");
    }
    
    /**
     * Tests that the chunk boundaries are correctly computed.
     */
    @Test
    void testGetBoundariesForLeaf() {
        // Create a test tree
        MerkleTree tree = createTestTree();
        
        // Check first leaf boundaries
        MerkleMismatch firstLeaf = tree.getBoundariesForLeaf(0);
        assertEquals(0, firstLeaf.chunkIndex(), "First leaf should have index 0");
        assertEquals(0, firstLeaf.startInclusive(), "First leaf should start at 0");
        assertEquals(CHUNK_SIZE, firstLeaf.length(), "First leaf should have length of CHUNK_SIZE");
        
        // Check middle leaf boundaries
        int middleIndex = tree.getNumberOfLeaves() / 2;
        MerkleMismatch middleLeaf = tree.getBoundariesForLeaf(middleIndex);
        assertEquals(middleIndex, middleLeaf.chunkIndex(), 
                    "Middle leaf should have correct index");
        assertEquals(middleIndex * CHUNK_SIZE, middleLeaf.startInclusive(), 
                    "Middle leaf should start at correct offset");
        assertEquals(CHUNK_SIZE, middleLeaf.length(),
                    "Middle leaf should have length of CHUNK_SIZE");
        
        // Check last leaf boundaries (might be partial)
        int lastIndex = tree.getNumberOfLeaves() - 1;
        MerkleMismatch lastLeaf = tree.getBoundariesForLeaf(lastIndex);
        assertEquals(lastIndex, lastLeaf.chunkIndex(),
                    "Last leaf should have correct index");
        assertEquals(lastIndex * CHUNK_SIZE, lastLeaf.startInclusive(),
                    "Last leaf should start at correct offset");
        
        // Last leaf length could be CHUNK_SIZE or less if totalSize is not a multiple of CHUNK_SIZE
        long expectedLength = Math.min(CHUNK_SIZE, tree.totalSize() - (lastIndex * CHUNK_SIZE));
        assertEquals(expectedLength, lastLeaf.length(),
                    "Last leaf should have correct length");
    }
}
