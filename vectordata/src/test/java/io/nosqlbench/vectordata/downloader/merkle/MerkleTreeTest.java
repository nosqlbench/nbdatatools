package io.nosqlbench.vectordata.downloader.merkle;

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

import io.nosqlbench.vectordata.merkle.MerkleFooter;
import io.nosqlbench.vectordata.merkle.MerkleMismatch;
import io.nosqlbench.vectordata.merkle.MerkleRange;
import io.nosqlbench.vectordata.merkle.MerkleTree;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MerkleTreeTest {
    @TempDir
    Path tempDir;

    private static final int CHUNK_SIZE = 16; // Power of two

    private MerkleTree createTestTree(byte[] data, int chunkSize) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        return MerkleTree.fromData(
            buffer,
            chunkSize,
            new MerkleRange(0, data.length)
        );
    }

    @Test
    void testCreation() {
        // Test creating a tree from data
        byte[] data = new byte[CHUNK_SIZE * 8]; // 8 chunks
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }

        MerkleTree tree = createTestTree(data, CHUNK_SIZE);
        assertEquals(8, tree.getNumberOfLeaves(), "Tree should have 8 leaves");
        assertEquals(CHUNK_SIZE, tree.getChunkSize(), "Chunk size should match");
        assertEquals(data.length, tree.totalSize(), "Total size should match");
    }

    @Test
    void testLeafHashing() throws NoSuchAlgorithmException {
        // Create test data
        byte[] chunkData = new byte[CHUNK_SIZE];
        for (int i = 0; i < chunkData.length; i++) {
            chunkData[i] = (byte) i;
        }

        // Create a one-chunk tree
        MerkleTree tree = createTestTree(chunkData, CHUNK_SIZE);

        // Manually hash the chunk
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update(chunkData);
        byte[] expectedHash = digest.digest();

        // Get the hash from the tree's leaf
        byte[] actualHash = tree.getHashForLeaf(0);

        // Compare the hashes
        assertArrayEquals(expectedHash, actualHash, "Leaf hash should match manual hash");
    }

    @Test
    void testSaveAndLoad() throws IOException {
        // Create test data
        byte[] data = new byte[CHUNK_SIZE * 4]; // 4 chunks
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }

        // Create a tree
        MerkleTree tree = createTestTree(data, CHUNK_SIZE);

        // Save the tree to a file
        Path treePath = tempDir.resolve("merkle.tree");
        tree.save(treePath);

        // Load the tree back
        MerkleTree loadedTree = MerkleTree.load(treePath);

        // Compare the trees
        assertEquals(tree.getNumberOfLeaves(), loadedTree.getNumberOfLeaves(), "Number of leaves should match");
        assertEquals(tree.getChunkSize(), loadedTree.getChunkSize(), "Chunk size should match");
        assertEquals(tree.totalSize(), loadedTree.totalSize(), "Total size should match");

        // Compare leaf hashes
        for (int i = 0; i < tree.getNumberOfLeaves(); i++) {
            assertArrayEquals(tree.getHashForLeaf(i), loadedTree.getHashForLeaf(i),
                    "Leaf " + i + " hash should match");
        }
    }

    @Test
    void testEmptyTreeCreation() {
        // Test creating an empty tree
        long totalSize = CHUNK_SIZE * 8;
        MerkleTree tree = MerkleTree.createEmpty(totalSize, CHUNK_SIZE);
        
        assertEquals(8, tree.getNumberOfLeaves(), "Empty tree should have correct number of leaves");
        assertEquals(CHUNK_SIZE, tree.getChunkSize(), "Chunk size should match");
        assertEquals(totalSize, tree.totalSize(), "Total size should match");
    }
    
    @Test
    void testCreateEmptyTreeLike() throws IOException {
        // Create test data
        byte[] data = new byte[CHUNK_SIZE * 4]; // 4 chunks
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }

        // Create a tree
        MerkleTree tree = createTestTree(data, CHUNK_SIZE);

        // Save the tree to a file
        Path treePath = tempDir.resolve("merkle.tree");
        tree.save(treePath);
        
        // Create empty tree like the original
        Path emptyTreePath = tempDir.resolve("empty_tree.tree");
        MerkleTree.createEmptyTreeLike(treePath, emptyTreePath);
        
        // Load the empty tree
        MerkleTree emptyTree = MerkleTree.load(emptyTreePath);
        
        // Verify properties match
        assertEquals(tree.getChunkSize(), emptyTree.getChunkSize(), "Chunk size should match");
        assertEquals(tree.totalSize(), emptyTree.totalSize(), "Total size should match");
        assertEquals(tree.getNumberOfLeaves(), emptyTree.getNumberOfLeaves(), "Number of leaves should match");
    }

    @Test
    void testFooterWriteRead() throws IOException {
        // Create test data
        byte[] data = new byte[CHUNK_SIZE * 4]; // 4 chunks
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }

        // Create a tree
        MerkleTree tree = createTestTree(data, CHUNK_SIZE);

        // Save the tree to a file
        Path treePath = tempDir.resolve("merkle.tree");
        tree.save(treePath);

        // Read the footer manually
        long fileSize = Files.size(treePath);
        ByteBuffer lenBuffer = ByteBuffer.allocate(1);
        try (FileChannel fc = FileChannel.open(treePath, StandardOpenOption.READ)) {
            fc.position(fileSize - 1);
            fc.read(lenBuffer);
        }
        lenBuffer.flip();
        byte footerLength = lenBuffer.get();

        ByteBuffer footerBuffer = ByteBuffer.allocate(footerLength);
        try (FileChannel fc = FileChannel.open(treePath, StandardOpenOption.READ)) {
            fc.position(fileSize - footerLength);
            fc.read(footerBuffer);
        }
        footerBuffer.flip();

        // Parse the footer values
        MerkleFooter footer = MerkleFooter.fromByteBuffer(footerBuffer);

        // Check values
        assertEquals(CHUNK_SIZE, footer.chunkSize(), "Chunk size in footer should match");
        assertEquals(data.length, footer.totalSize(), "Total size in footer should match");
    }
    
    @Test
    void testFindMismatchedChunks() {
        // Create test data
        byte[] data1 = new byte[CHUNK_SIZE * 4]; // 4 chunks
        byte[] data2 = new byte[CHUNK_SIZE * 4]; // 4 chunks
        
        // Fill with test data
        for (int i = 0; i < data1.length; i++) {
            data1[i] = (byte) (i % 256);
            data2[i] = (byte) (i % 256); // initially the same
        }
        
        // Modify chunk 2 in data2
        for (int i = CHUNK_SIZE * 2; i < CHUNK_SIZE * 3; i++) {
            data2[i] = (byte) ((data2[i] + 128) % 256); // modify data
        }
        
        // Create trees
        MerkleTree tree1 = createTestTree(data1, CHUNK_SIZE);
        MerkleTree tree2 = createTestTree(data2, CHUNK_SIZE);
        
        // Find mismatched chunks
        List<MerkleMismatch> mismatches = tree1.findMismatchedChunks(tree2);
        
        // Verify exactly one mismatch found
        assertEquals(1, mismatches.size(), "Should find exactly one mismatched chunk");
        
        // Verify mismatch is at the correct position
        MerkleMismatch mismatch = mismatches.get(0);
        assertEquals(2, mismatch.chunkIndex(), "Mismatch should be at chunk index 2");
        assertEquals(CHUNK_SIZE * 2, mismatch.startInclusive(), "Mismatch should start at the right offset");
        assertEquals(CHUNK_SIZE, mismatch.length(), "Mismatch should have the correct length");
    }
    
    @Test
    void testUpdateLeafHash() throws NoSuchAlgorithmException {
        // Create test data
        byte[] data = new byte[CHUNK_SIZE * 4]; // 4 chunks
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        
        // Create tree
        MerkleTree tree = createTestTree(data, CHUNK_SIZE);
        
        // Get original leaf hash
        byte[] originalHash = tree.getHashForLeaf(1);
        
        // Create a new hash
        byte[] newData = new byte[CHUNK_SIZE];
        for (int i = 0; i < newData.length; i++) {
            newData[i] = (byte) ((i + 128) % 256); // different data
        }
        
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update(newData);
        byte[] newHash = digest.digest();
        
        // Update the leaf hash
        tree.updateLeafHash(1, newHash);
        
        // Verify the leaf hash was updated
        byte[] updatedHash = tree.getHashForLeaf(1);
        assertFalse(Arrays.equals(originalHash, updatedHash), "Hash should have changed");
        assertArrayEquals(newHash, updatedHash, "Hash should match the new hash");
    }
    
    @Test
    void testGetBoundariesForLeaf() {
        // Create a tree with a non-power-of-two size
        long totalSize = CHUNK_SIZE * 3 + 5; // 3 full chunks plus 5 bytes
        MerkleTree tree = MerkleTree.createEmpty(totalSize, CHUNK_SIZE);
        
        // Check first chunk boundaries
        MerkleMismatch chunk0 = tree.getBoundariesForLeaf(0);
        assertEquals(0, chunk0.chunkIndex(), "First chunk should have index 0");
        assertEquals(0, chunk0.startInclusive(), "First chunk should start at offset 0");
        assertEquals(CHUNK_SIZE, chunk0.length(), "First chunk should have full chunk size length");
        
        // Check middle chunk boundaries
        MerkleMismatch chunk1 = tree.getBoundariesForLeaf(1);
        assertEquals(1, chunk1.chunkIndex(), "Middle chunk should have index 1");
        assertEquals(CHUNK_SIZE, chunk1.startInclusive(), "Middle chunk should start at offset CHUNK_SIZE");
        assertEquals(CHUNK_SIZE, chunk1.length(), "Middle chunk should have full chunk size length");
        
        // Check last (partial) chunk boundaries
        MerkleMismatch chunk3 = tree.getBoundariesForLeaf(3);
        assertEquals(3, chunk3.chunkIndex(), "Last chunk should have index 3");
        assertEquals(CHUNK_SIZE * 3, chunk3.startInclusive(), "Last chunk should start at offset CHUNK_SIZE*3");
        assertEquals(5, chunk3.length(), "Last chunk should have partial length of 5");
    }
    
    @Test
    void testNonPowerOfTwoChunkSize() {
        byte[] data = new byte[100];
        
        // Test with non-power-of-two chunk size
        assertThrows(IllegalArgumentException.class, () -> 
            createTestTree(data, 10),
            "Should throw exception for non-power-of-two chunk size"
        );
        
        // Test with power-of-two chunk size (should work)
        MerkleTree tree = createTestTree(data, 16);
        assertNotNull(tree, "Tree should be created with power-of-two chunk size");
    }
    
    @Test
    void testVerifyChunk() throws NoSuchAlgorithmException {
        // Create test data
        byte[] data = new byte[CHUNK_SIZE * 4]; // 4 chunks
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        
        // Create a tree
        MerkleTree tree = createTestTree(data, CHUNK_SIZE);
        
        // Extract the second chunk
        byte[] chunk = Arrays.copyOfRange(data, CHUNK_SIZE, CHUNK_SIZE * 2);
        
        // Hash the chunk
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update(chunk);
        byte[] chunkHash = digest.digest();
        
        // Get the leaf hash from the tree
        byte[] treeHash = tree.getHashForLeaf(1);
        
        // Verify the hashes match
        assertArrayEquals(chunkHash, treeHash, "Tree's leaf hash should match direct chunk hash");
        
        // Modify the chunk
        chunk[0] = (byte) (chunk[0] + 1);
        
        // Hash the modified chunk
        digest.reset();
        digest.update(chunk);
        byte[] modifiedChunkHash = digest.digest();
        
        // Verify the hashes don't match
        assertFalse(Arrays.equals(modifiedChunkHash, treeHash), 
                "Modified chunk hash should not match tree's leaf hash");
    }
}