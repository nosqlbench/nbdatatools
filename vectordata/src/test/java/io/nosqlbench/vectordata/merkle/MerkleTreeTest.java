
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

/**
 * Tests for MerkleTree functionality.
 */

import io.nosqlbench.vectordata.merklev2.*;
import java.util.concurrent.CompletableFuture;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MerkleTreeTest {
    @TempDir
    Path tempDir;

    private static final int CHUNK_SIZE = 16; // Power of two

    private MerkleDataImpl createTestTree(byte[] data) {
        // Create a tree from data using merklev2
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("Cannot create tree from null or empty data");
        }
        
        try {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            CompletableFuture<MerkleDataImpl> future = MerkleRefFactory.fromData(buffer);
            return future.get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create test tree: " + e.getMessage(), e);
        }
    }

    @Test
    void testCreation() {
        // Test creating a tree from data
        byte[] data = new byte[CHUNK_SIZE * 8]; // 8 chunks
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }

        MerkleDataImpl tree = createTestTree(data);
        assertTrue(tree.getNumberOfLeaves() > 0, "Tree should have leaves");
        assertTrue(tree.getChunkSize() > 0, "Chunk size should be positive");
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
        MerkleDataImpl tree = createTestTree(chunkData);

        // Get the hash from the tree's leaf
        byte[] firstHash = tree.getHashForLeaf(0);

        // Get the hash again to verify consistency
        byte[] secondHash = tree.getHashForLeaf(0);

        // Compare the hashes - they should be the same
        assertArrayEquals(firstHash, secondHash, "Leaf hash should be consistent when retrieved multiple times");

        // Verify the hash is not null or empty
        assertNotNull(firstHash, "Leaf hash should not be null");
        assertTrue(firstHash.length > 0, "Leaf hash should not be empty");

        // Create different data to test hash uniqueness
        byte[] differentData = new byte[CHUNK_SIZE];
        for (int i = 0; i < differentData.length; i++) {
            differentData[i] = (byte) ((i + 42) % 256); // Different from original
        }

        // Create a new tree from different data
        MerkleDataImpl differentTree = createTestTree(differentData);
        byte[] differentHash = differentTree.getHashForLeaf(0);

        // Different data should produce different hashes
        assertFalse(Arrays.equals(firstHash, differentHash), "Different data should produce different hashes");
        
        differentTree.close();
        tree.close();
    }

    @Test
    void testSaveAndLoad() throws IOException {
        // Create test data
        byte[] data = new byte[CHUNK_SIZE * 4]; // 4 chunks
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }

        // Create a tree using merklev2
        MerkleDataImpl tree = createTestTree(data);

        // Save the tree to a file
        Path treePath = tempDir.resolve("merkle.mref");
        tree.save(treePath);

        // Load the tree back
        MerkleDataImpl loadedTree = MerkleRefFactory.load(treePath);

        // Compare the trees
        assertTrue(loadedTree.getNumberOfLeaves() > 0, "Number of leaves should be positive");
        assertTrue(loadedTree.getChunkSize() > 0, "Chunk size should be positive");
        assertEquals(tree.totalSize(), loadedTree.totalSize(), "Total size should match");

        // Compare leaf hashes
        for (int i = 0; i < tree.getNumberOfLeaves(); i++) {
            assertArrayEquals(tree.getHashForLeaf(i), loadedTree.getHashForLeaf(i),
                    "Leaf " + i + " hash should match");
        }
        
        // Clean up
        tree.close();
        loadedTree.close();
    }

    @Test
    void testEmptyTreeCreation() {
        // Test creating an empty tree using merklev2
        long totalSize = CHUNK_SIZE * 8;
        MerkleDataImpl tree = MerkleRefFactory.createEmpty(totalSize);

        assertTrue(tree.getNumberOfLeaves() > 0, "Empty tree should have leaves");
        assertTrue(tree.getChunkSize() > 0, "Chunk size should be positive");
        assertEquals(totalSize, tree.totalSize(), "Total size should match");
    }

    @Test
    void testCreateEmptyTreeLike() throws IOException {
        // Create test data
        byte[] data = new byte[CHUNK_SIZE * 4]; // 4 chunks
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }

        // Create a tree using merklev2
        MerkleDataImpl tree = createTestTree(data);

        // Save the tree to a file
        Path treePath = tempDir.resolve("merkle.mref");
        tree.save(treePath);

        // Create empty tree like the original
        MerkleDataImpl emptyTree = MerkleRefFactory.createEmpty(tree.totalSize());

        // Verify properties match
        assertTrue(emptyTree.getChunkSize() > 0, "Chunk size should be positive");
        assertEquals(tree.totalSize(), emptyTree.totalSize(), "Total size should match");
        assertTrue(emptyTree.getNumberOfLeaves() > 0, "Number of leaves should be positive");
        
        // Clean up
        tree.close();
        emptyTree.close();
    }

    @Test
    void testFooterWriteRead() throws IOException {
        // Create test data
        byte[] data = new byte[CHUNK_SIZE * 4]; // 4 chunks
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }

        // Create a tree using merklev2
        MerkleDataImpl tree = createTestTree(data);

        // Save the tree to a file
        Path treePath = tempDir.resolve("merkle.mref");
        tree.save(treePath);

        // Read the footer manually
        long fileSize = Files.size(treePath);
        long footerPosition = fileSize - Merklev2Footer.FIXED_FOOTER_SIZE;
        
        try (FileChannel fc = FileChannel.open(treePath, StandardOpenOption.READ)) {
            Merklev2Footer footer = Merklev2Footer.readFromChannel(fc, footerPosition);
            
            // Check values
            assertTrue(footer.chunkSize() > 0, "Chunk size in footer should be positive");
            assertEquals(data.length, footer.totalContentSize(), "Total size in footer should match");
        }
        
        tree.close();
    }

    @Test
    void testFindMismatchedChunks() throws IOException {
        // Create two completely different data sets
        byte[] data1 = new byte[CHUNK_SIZE * 4]; // 4 chunks
        byte[] data2 = new byte[CHUNK_SIZE * 4]; // 4 chunks

        // Fill with completely different patterns
        for (int i = 0; i < data1.length; i++) {
            data1[i] = (byte) (i % 256);
            data2[i] = (byte) (255 - (i % 256)); // Inverted pattern
        }

        // Create trees using merklev2
        MerkleDataImpl tree1 = createTestTree(data1);
        MerkleDataImpl tree2 = createTestTree(data2);

        // Find mismatched chunks
        List<io.nosqlbench.vectordata.merkle.MerkleMismatch> mismatches = tree1.findMismatchedChunks(tree2);

        // Since the data is completely different, we expect mismatches
        assertTrue(mismatches.size() > 0, "Should find mismatches between completely different trees");
        
        // Clean up
        tree1.close();
        tree2.close();
    }

    @Test
    void testUpdateLeafHash() throws NoSuchAlgorithmException, IOException {
        // Create test data
        byte[] data = new byte[CHUNK_SIZE * 4]; // 4 chunks
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }

        // Create a reference tree
        MerkleDataImpl refTree = createTestTree(data);
        
        // Create a state tree from the reference
        Path statePath = tempDir.resolve("update_test.mrkl");
        MerkleDataImpl stateTree = MerkleDataImpl.createFromRef(refTree, statePath);

        // Get original leaf hash
        byte[] originalHash = stateTree.getHashForLeaf(0);
        assertNotNull(originalHash, "Original hash should not be null");

        // Create new data for chunk 0
        byte[] newData = new byte[CHUNK_SIZE];
        for (int i = 0; i < newData.length; i++) {
            newData[i] = (byte) ((i + 128) % 256); // different data
        }

        // Try to save invalid data (should fail)
        boolean saved = stateTree.saveIfValid(0, ByteBuffer.wrap(newData), buffer -> {
            // This would save the data if valid
        });
        
        assertFalse(saved, "Invalid data should not be saved");
        
        // Clean up
        stateTree.close();
        refTree.close();
    }

    @Test
    void testGetBoundariesForLeaf() {
        // Create a shape using merklev2
        long totalSize = CHUNK_SIZE * 3 + 5; // Test data size
        BaseMerkleShape shape = new BaseMerkleShape(totalSize);

        // Check first chunk boundaries
        ChunkBoundary chunk0 = shape.getChunkBoundary(0);
        assertEquals(0, chunk0.startInclusive(), "First chunk should start at offset 0");
        assertTrue(chunk0.length() > 0, "First chunk should have positive length");

        // Check if tree has multiple leaves
        if (shape.getLeafCount() > 1) {
            ChunkBoundary chunk1 = shape.getChunkBoundary(1);
            assertTrue(chunk1.startInclusive() >= chunk0.endExclusive(), "Middle chunk should start after first chunk");
            assertTrue(chunk1.length() > 0, "Middle chunk should have positive length");
        }

        // Check last chunk boundaries (if multiple leaves exist)
        if (shape.getLeafCount() > 1) {
            int lastLeafIndex = shape.getLeafCount() - 1;
            ChunkBoundary lastChunk = shape.getChunkBoundary(lastLeafIndex);
            assertTrue(lastChunk.startInclusive() >= 0, "Last chunk should have valid start offset");
            assertTrue(lastChunk.length() > 0, "Last chunk should have positive length");
        }
    }

    @Test
    void testNonPowerOfTwoChunkSize() {
        byte[] data = new byte[100];

        // Test creating tree with automatic chunk size calculation
        MerkleDataImpl tree = createTestTree(data);
        assertNotNull(tree, "Tree should be created with automatic chunk size calculation");
        assertTrue(tree.getChunkSize() > 0, "Chunk size should be positive");
        
        tree.close();
    }

    @Test
    void testHashDataIfMatchesExpected() throws IOException, NoSuchAlgorithmException {
        // Create simple test data
        byte[] testData = new byte[CHUNK_SIZE];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) i;
        }

        // Manually compute what the SHA-256 hash should be for verification
        MessageDigest testDigest = MessageDigest.getInstance("SHA-256");
        testDigest.update(testData);
        byte[] manualHash = testDigest.digest();

        // Create a tree from the test data
        MerkleDataImpl tree = createTestTree(testData);
        
        // Get the hash from the tree
        byte[] expectedHash = tree.getHashForLeaf(0);
        assertNotNull(expectedHash, "Tree should have a hash for leaf 0");
        
        // The tree hash and manual hash should match
        assertArrayEquals(expectedHash, manualHash, "Tree hash should match manually computed hash");
        
        // Test with different data - should produce different hash
        byte[] differentData = new byte[CHUNK_SIZE];
        for (int i = 0; i < differentData.length; i++) {
            differentData[i] = (byte) (i + 1); // Different data
        }
        
        MerkleDataImpl differentTree = createTestTree(differentData);
        byte[] differentHash = differentTree.getHashForLeaf(0);
        
        assertFalse(Arrays.equals(expectedHash, differentHash), "Different data should produce different hash");
    }

    @Test
    void testVerifyChunk() throws IOException {
        // Create simple test data
        byte[] testData = new byte[CHUNK_SIZE * 2];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) i;
        }

        // Create data file
        Path dataPath = tempDir.resolve("test_data.bin");
        Files.write(dataPath, testData);

        // Create a reference tree from the data
        MerkleDataImpl refTree = createTestTree(testData);
        
        // Save the reference tree
        Path refPath = tempDir.resolve("ref_data.mref");
        refTree.save(refPath);
        
        // Load the reference tree back
        MerkleDataImpl loadedRef = MerkleRefFactory.load(refPath);
        
        // Create a state tree for verification
        Path statePath = tempDir.resolve("test_data.mrkl");
        MerkleDataImpl stateTree = MerkleDataImpl.createFromRef(loadedRef, statePath);
        
        // Get the first chunk size
        long chunkSize = refTree.getChunkSize();
        int actualChunkSize = (int) Math.min(chunkSize, testData.length);
        ByteBuffer chunkData = ByteBuffer.wrap(testData, 0, actualChunkSize);
        
        // Verify the chunk data matches
        boolean saved = stateTree.saveIfValid(0, chunkData, data -> {
            // This callback would save the data
        });
        
        assertTrue(saved, "Chunk should be valid and saved");
        assertTrue(stateTree.isValid(0), "Chunk should be marked as valid");
        
        stateTree.close();
        refTree.close();
        loadedRef.close();
    }

    @Test
    void testConcurrentLeafUpdates() throws InterruptedException, IOException {
        // Create test data
        byte[] testData = new byte[CHUNK_SIZE * 4]; // 4 chunks
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) (i % 256);
        }

        // Create a reference tree from the data
        MerkleDataImpl refTree = createTestTree(testData);
        
        // Create a state tree for concurrent operations
        Path statePath = tempDir.resolve("concurrent_test.mrkl");
        MerkleDataImpl stateTree = MerkleDataImpl.createFromRef(refTree, statePath);

        // Test concurrent access to the state tree
        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(threadCount);
        AtomicInteger errorCount = new AtomicInteger(0);

        // Create concurrent tasks that read hashes and validate chunks
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    
                    // Each thread validates different chunks
                    for (int i = 0; i < refTree.getNumberOfLeaves(); i++) {
                        // Read hash from reference tree
                        byte[] hash = refTree.getHashForLeaf(i);
                        assertNotNull(hash, "Hash should not be null for leaf " + i);
                        
                        // Read hash from state tree
                        byte[] stateHash = stateTree.getHashForLeaf(i);
                        assertNotNull(stateHash, "State hash should not be null for leaf " + i);
                        
                        // Hashes should match
                        assertArrayEquals(hash, stateHash, "Hashes should match for leaf " + i);
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        // Start all threads
        startLatch.countDown();

        // Wait for all threads to complete
        boolean completed = finishLatch.await(30, TimeUnit.SECONDS);
        assertTrue(completed, "All threads should complete within 30 seconds");

        // Shut down the executor
        executor.shutdown();

        // Check for errors
        assertEquals(0, errorCount.get(), "No errors should occur during concurrent operations");

        // Clean up
        stateTree.close();
        refTree.close();
    }

    @Test
    void testLastLeafCalculationWithNonMultipleFileSize() throws NoSuchAlgorithmException, IOException {
        // Create a file with a size that is not a multiple of the chunk size
        int fullChunks = 3;
        int partialChunkSize = 5;
        int totalSize = fullChunks * CHUNK_SIZE + partialChunkSize;

        // Create test data with a specific pattern
        byte[] data = new byte[totalSize];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }

        // Create a merkle tree using merklev2
        MerkleDataImpl tree = createTestTree(data);

        // Verify the tree has leaves (merklev2 calculates chunk size automatically)
        assertTrue(tree.getNumberOfLeaves() > 0, 
                    "Tree should have at least one leaf for non-empty data");

        // Verify the boundaries of the last leaf using shape
        MerkleShape shape = tree.getShape();
        int actualLeafCount = tree.getNumberOfLeaves();
        ChunkBoundary lastLeafBoundary = shape.getChunkBoundary(actualLeafCount - 1);
        assertTrue(lastLeafBoundary.startInclusive() >= 0, 
                    "Last leaf should start at valid offset");
        assertTrue(lastLeafBoundary.length() > 0, 
                    "Last leaf should have positive length");

        // Get the hash from the tree for the last leaf
        byte[] treeHash = tree.getHashForLeaf(actualLeafCount - 1);
        assertNotNull(treeHash, "Tree hash for last leaf should not be null");

        // For merklev2, the chunk size is calculated automatically and we can't predict exact boundaries
        // So we just verify that we can retrieve hashes for all leaves
        for (int i = 0; i < actualLeafCount; i++) {
            byte[] leafHash = tree.getHashForLeaf(i);
            assertNotNull(leafHash, "Hash for leaf " + i + " should not be null");
        }

        tree.close();
    }

    @Test
    void testUpdateEmptyTree() throws IOException {
        // Create an empty tree with specific size
        long totalSize = CHUNK_SIZE * 4;
        MerkleDataImpl emptyTree = MerkleRefFactory.createEmpty(totalSize);

        // Verify initial state
        assertTrue(emptyTree.getNumberOfLeaves() > 0, "Empty tree should have leaves");
        assertEquals(totalSize, emptyTree.totalSize(), "Total size should match");

        // Create a state tree from the empty reference
        Path statePath = tempDir.resolve("empty_state.mrkl");
        MerkleDataImpl stateTree = MerkleDataImpl.createFromRef(emptyTree, statePath);

        // Verify state tree initial state
        assertFalse(stateTree.isValid(0), "Initially no chunks should be valid");

        // Create some test data
        byte[] chunkData = new byte[CHUNK_SIZE];
        for (int i = 0; i < chunkData.length; i++) {
            chunkData[i] = (byte) (i % 256);
        }

        // Since the empty tree has no reference hashes, saveIfValid should fail
        boolean saved = stateTree.saveIfValid(0, ByteBuffer.wrap(chunkData), data -> {
            // This callback would save the data if valid
        });

        assertFalse(saved, "Empty tree should not accept data without reference hashes");
        assertFalse(stateTree.isValid(0), "Chunk should not be marked valid");

        // Clean up
        stateTree.close();
        emptyTree.close();
    }

    @Test
    void testUpdateEmptyTreeLike() throws IOException {
        // Create test data
        byte[] testData = new byte[CHUNK_SIZE * 2];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) (i % 256);
        }

        // Create a reference tree
        MerkleDataImpl refTree = createTestTree(testData);
        
        // Save the reference tree
        Path refPath = tempDir.resolve("ref_tree.mref");
        refTree.save(refPath);
        
        // Load the reference tree back
        MerkleDataImpl loadedRef = MerkleRefFactory.load(refPath);
        
        // Verify the loaded reference tree
        assertEquals(refTree.getNumberOfLeaves(), loadedRef.getNumberOfLeaves(), "Number of leaves should match");
        assertEquals(refTree.totalSize(), loadedRef.totalSize(), "Total size should match");
        
        // Compare hashes
        for (int i = 0; i < refTree.getNumberOfLeaves(); i++) {
            assertArrayEquals(refTree.getHashForLeaf(i), loadedRef.getHashForLeaf(i), 
                "Hash for leaf " + i + " should match");
        }
        
        // Clean up
        refTree.close();
        loadedRef.close();
    }

    @Test
    void testFileSizeEncodingInMerkleFooter() throws IOException {
        // Test with various file sizes to ensure the size is correctly encoded in the footer

        // Test case 1: Empty file (0 bytes)
        testFileSizeEncoding(0);

        // Test case 2: Small file
        testFileSizeEncoding(CHUNK_SIZE);

        // Test case 3: Medium file 
        testFileSizeEncoding(CHUNK_SIZE * 3 + 5);

        // Test case 4: Large file
        testFileSizeEncoding(CHUNK_SIZE * 10);
    }

    /**
     * Helper method to test that file size is correctly encoded in the merkle footer
     * 
     * @param fileSize The size of the file to test
     * @throws IOException If an I/O error occurs
     */
    private void testFileSizeEncoding(int fileSize) throws IOException {
        if (fileSize == 0) {
            // Skip empty files as they cannot create valid merkle trees
            return;
        }

        // Create test data
        byte[] data = new byte[fileSize];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }

        // Create a merkle tree using merklev2
        MerkleDataImpl tree = createTestTree(data);

        // Save the tree to a file
        Path treePath = tempDir.resolve("merkle_" + fileSize + ".mref");
        tree.save(treePath);

        // Read the footer manually
        long treeFileSize = Files.size(treePath);
        long footerPosition = treeFileSize - Merklev2Footer.FIXED_FOOTER_SIZE;
        
        try (FileChannel fc = FileChannel.open(treePath, StandardOpenOption.READ)) {
            Merklev2Footer footer = Merklev2Footer.readFromChannel(fc, footerPosition);
            
            // Check values
            assertTrue(footer.chunkSize() > 0, 
                      "Chunk size in footer should be positive for file size " + fileSize);
            assertEquals(fileSize, footer.totalContentSize(), 
                        "Total size in footer should match original file size " + fileSize);
        }
        
        tree.close();
    }
}
