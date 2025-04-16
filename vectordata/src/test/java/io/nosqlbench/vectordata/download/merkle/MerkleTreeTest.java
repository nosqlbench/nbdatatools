package io.nosqlbench.vectordata.download.merkle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MerkleTreeTest {
    @TempDir
    Path tempDir;

    private MerkleTree createTestTree(byte[] data, int chunkSize) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        return MerkleTree.fromData(
            buffer,
            chunkSize,
            new MerkleRange(0, data.length)
        );
    }

    @Test
    void testBasicTreeOperations() {
        byte[] data = new byte[100];
        Arrays.fill(data, (byte)1);
        
        MerkleTree tree = createTestTree(data, 10);
        
        assertEquals(10, tree.chunkSize());
        assertEquals(100, tree.totalSize());
        assertEquals(10, tree.getNumberOfLeaves());
        
        // Test leaf index calculation
        assertEquals(0, tree.getLeafIndex(0));
        assertEquals(1, tree.getLeafIndex(15));
        assertEquals(9, tree.getLeafIndex(95));
        
        // Test boundary calculation
        MerkleTree.NodeBoundary firstChunk = tree.getBoundariesForLeaf(0);
        assertEquals(0, firstChunk.start());
        assertEquals(10, firstChunk.end());
        
        MerkleTree.NodeBoundary lastChunk = tree.getBoundariesForLeaf(9);
        assertEquals(90, lastChunk.start());
        assertEquals(100, lastChunk.end());
    }

    @Test
    void testTreeComparison() {
        // Create two identical trees
        byte[] data1 = new byte[100];
        Arrays.fill(data1, (byte)1);
        MerkleTree tree1 = createTestTree(data1, 10);
        
        byte[] data2 = data1.clone();
        MerkleTree tree2 = createTestTree(data2, 10);
        
        // Trees should be identical
        List<MerkleMismatch> mismatches = tree1.findMismatchedChunks(tree2);
        assertTrue(mismatches.isEmpty());
        
        // Modify one chunk in second tree
        data2[15] = 2; // Modify second chunk
        tree2 = createTestTree(data2, 10);
        
        mismatches = tree1.findMismatchedChunks(tree2);
        assertEquals(1, mismatches.size());
        
        MerkleMismatch mismatch = mismatches.get(0);
        assertEquals(1, mismatch.chunkIndex());
        assertEquals(10, mismatch.start());
        assertEquals(10, mismatch.length());
    }

    @Test
    void testSubTreeCreation() {
        byte[] data = new byte[100];
        Arrays.fill(data, (byte)1);
        MerkleTree fullTree = createTestTree(data, 10);
        
        // Create subtree for middle portion
        MerkleRange subRange = new MerkleRange(30, 60);
        MerkleTree subTree = fullTree.subTree(subRange);
        
        assertEquals(10, subTree.chunkSize());
        assertEquals(100, subTree.totalSize());
        assertEquals(subRange, subTree.computedRange());
    }

    @Test
    void testTreeSerialization() throws IOException {
        // Create test data with known content
        byte[] data = new byte[100];
        Arrays.fill(data, (byte)1);
        
        // Create and verify original tree
        ByteBuffer buffer = ByteBuffer.wrap(data);
        MerkleTree originalTree = MerkleTree.fromData(
            buffer,
            10,  // chunk size
            new MerkleRange(0, data.length)
        );
        
        assertNotNull(originalTree, "Original tree should not be null");
        assertNotNull(originalTree.root(), "Original tree should have a root node");
        
        // Save tree to file
        Path merkleFile = tempDir.resolve("test.merkle");
        originalTree.save(merkleFile);
        
        // Verify file was created and has content
        assertTrue(Files.exists(merkleFile), "Merkle file should exist");
        assertTrue(Files.size(merkleFile) > 0, "Merkle file should not be empty");
        
        // Load and verify tree
        MerkleTree loadedTree = MerkleTree.load(merkleFile);
        assertNotNull(loadedTree, "Loaded tree should not be null");
        assertNotNull(loadedTree.root(), "Loaded tree should have a root node");
        
        // Compare properties
        assertEquals(originalTree.chunkSize(), loadedTree.chunkSize());
        assertEquals(originalTree.totalSize(), loadedTree.totalSize());
        assertEquals(originalTree.computedRange(), loadedTree.computedRange());
        
        // Compare root hashes
        assertArrayEquals(
            originalTree.root().hash(),
            loadedTree.root().hash()
        );
    }

    @Test
    void testTreeCreation() {
        // Test with small data set
        byte[] data = new byte[20];
        Arrays.fill(data, (byte)1);
        
        MerkleTree tree = createTestTree(data, 5);  // 4 chunks of 5 bytes each
        
        assertNotNull(tree.root(), "Tree should have a root node");
        assertEquals(5, tree.chunkSize(), "Chunk size should be 5");
        assertEquals(20, tree.totalSize(), "Total size should be 20");
        assertEquals(4, tree.getNumberOfLeaves(), "Should have 4 leaves");
    }

    @Test
    void testInvalidOperations() {
        byte[] data = new byte[100];
        MerkleTree tree = createTestTree(data, 10);
        
        // Test invalid leaf index
        assertThrows(IllegalArgumentException.class, () -> tree.getBoundariesForLeaf(-1));
        assertThrows(IllegalArgumentException.class, () -> tree.getBoundariesForLeaf(10));
        
        // Test invalid subtree range
        assertThrows(IllegalArgumentException.class, () -> tree.subTree(new MerkleRange(-1, 50)));
        assertThrows(IllegalArgumentException.class, () -> tree.subTree(new MerkleRange(0, 101)));
        assertThrows(IllegalArgumentException.class, () -> tree.subTree(new MerkleRange(50, 40)));
        
        // Test incompatible tree comparison
        MerkleTree differentChunkSize = createTestTree(data, 20);
        assertThrows(IllegalArgumentException.class, () -> 
            tree.findMismatchedChunks(differentChunkSize)
        );
        
        byte[] differentSize = new byte[200];
        MerkleTree differentTotalSize = createTestTree(differentSize, 10);
        assertThrows(IllegalArgumentException.class, () -> 
            tree.findMismatchedChunks(differentTotalSize)
        );
    }
}
