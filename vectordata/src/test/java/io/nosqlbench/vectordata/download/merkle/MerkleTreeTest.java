package io.nosqlbench.vectordata.download.merkle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void testBasicTreeOperations() {
        byte[] data = new byte[128];
        Arrays.fill(data, (byte)1);
        
        MerkleTree tree = createTestTree(data, CHUNK_SIZE);
        
        assertEquals(CHUNK_SIZE, tree.chunkSize());
        assertEquals(128, tree.totalSize());
        assertEquals(8, tree.getNumberOfLeaves());
        
        assertEquals(0, tree.getLeafIndex(0));
        assertEquals(1, tree.getLeafIndex(CHUNK_SIZE + 1));
        assertEquals(7, tree.getLeafIndex(127));
        
        MerkleTree.NodeBoundary firstChunk = tree.getBoundariesForLeaf(0);
        assertEquals(0, firstChunk.start());
        assertEquals(CHUNK_SIZE, firstChunk.end());
        
        MerkleTree.NodeBoundary lastChunk = tree.getBoundariesForLeaf(7);
        assertEquals(112, lastChunk.start());
        assertEquals(128, lastChunk.end());
    }

    @Test
    void testInvalidChunkSize() {
        byte[] data = new byte[128];
        Arrays.fill(data, (byte)1);
        
        assertThrows(IllegalArgumentException.class,
            () -> createTestTree(data, 10),
            "Should throw exception for non-power-of-two chunk size"
        );
    }

    @Test
    void testTreeComparison() {
        byte[] data1 = new byte[128];
        Arrays.fill(data1, (byte)1);
        MerkleTree tree1 = createTestTree(data1, CHUNK_SIZE);
        
        byte[] data2 = data1.clone();
        MerkleTree tree2 = createTestTree(data2, CHUNK_SIZE);
        
        List<MerkleMismatch> mismatches = tree1.findMismatchedChunks(tree2);
        assertTrue(mismatches.isEmpty());
        
        data2[CHUNK_SIZE + 1] = 2;
        tree2 = createTestTree(data2, CHUNK_SIZE);
        
        mismatches = tree1.findMismatchedChunks(tree2);
        assertEquals(1, mismatches.size());
        
        MerkleMismatch mismatch = mismatches.get(0);
        assertEquals(1, mismatch.chunkIndex());
        assertEquals(CHUNK_SIZE, mismatch.start());
        assertEquals(CHUNK_SIZE, mismatch.length());
    }

    @Test
    void testSubTreeCreation() {
        byte[] data = new byte[128];
        Arrays.fill(data, (byte)1);
        MerkleTree fullTree = createTestTree(data, CHUNK_SIZE);
        
        MerkleRange subRange = new MerkleRange(32, 64);
        MerkleTree subTree = fullTree.subTree(subRange);
        
        assertEquals(CHUNK_SIZE, subTree.chunkSize());
        assertEquals(128, subTree.totalSize());
        assertEquals(subRange, subTree.computedRange());
    }

    @Test
    void testTreeSerialization() throws IOException {
        byte[] data = new byte[128];
        Arrays.fill(data, (byte)1);
        
        ByteBuffer buffer = ByteBuffer.wrap(data);
        MerkleTree originalTree = MerkleTree.fromData(
            buffer,
            CHUNK_SIZE,
            new MerkleRange(0, data.length)
        );
        
        Path merkleFile = tempDir.resolve("test" + MerklePane.MRKL);
        originalTree.save(merkleFile);
        
        MerkleTree loadedTree = MerkleTree.load(merkleFile);
        
        assertEquals(originalTree.chunkSize(), loadedTree.chunkSize());
        assertEquals(originalTree.totalSize(), loadedTree.totalSize());
        assertEquals(originalTree.computedRange(), loadedTree.computedRange());
        
        assertArrayEquals(
            originalTree.root().hash(),
            loadedTree.root().hash()
        );
    }

    @Test
    void testTreeCreation() {
        // Test with small data set
        byte[] data = new byte[32];  // Multiple of chunk size
        Arrays.fill(data, (byte)1);
        
        MerkleTree tree = createTestTree(data, 4);  // Power of 2 chunk size
        
        assertNotNull(tree.root(), "Tree should have a root node");
        assertEquals(4, tree.chunkSize(), "Chunk size should be 4");
        assertEquals(32, tree.totalSize(), "Total size should be 32");
        assertEquals(8, tree.getNumberOfLeaves(), "Should have 8 leaves");
    }

    @Test
    void testTreeCreationWithNonPowerOfTwoChunkSize() {
        byte[] data = new byte[20];
        Arrays.fill(data, (byte)1);
        
        assertThrows(IllegalArgumentException.class,
            () -> createTestTree(data, 5),  // Non-power-of-two chunk size
            "Should throw exception for non-power-of-two chunk size"
        );
    }

    @Test
    void testInvalidOperations() {
        byte[] data = new byte[100];
        MerkleTree tree = createTestTree(data, 16);
        
        // Test invalid leaf index
        assertThrows(IllegalArgumentException.class, () -> tree.getBoundariesForLeaf(-1));
        assertThrows(IllegalArgumentException.class, () -> tree.getBoundariesForLeaf(10));
        
        // Test invalid subtree range
        assertThrows(IllegalArgumentException.class, () -> tree.subTree(new MerkleRange(-1, 50)));
        assertThrows(IllegalArgumentException.class, () -> tree.subTree(new MerkleRange(0, 101)));
        assertThrows(IllegalArgumentException.class, () -> tree.subTree(new MerkleRange(50, 40)));
        
        // Test incompatible tree comparison
        MerkleTree differentChunkSize = createTestTree(data, 32);
        assertThrows(IllegalArgumentException.class, () -> 
            tree.findMismatchedChunks(differentChunkSize)
        );
        
        byte[] differentSize = new byte[200];
        MerkleTree differentTotalSize = createTestTree(differentSize, 32);
        assertThrows(IllegalArgumentException.class, () -> 
            tree.findMismatchedChunks(differentTotalSize)
        );
    }
}
