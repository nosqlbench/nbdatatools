package io.nosqlbench.vectordata.download.merkle.restart;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class MerkleMetaTest {
    private static final int HASH_SIZE = 32; // SHA-256 hash size

    @Test
    void testBasicConstruction() {
        byte[] fileDigest = new byte[HASH_SIZE];
        MerkleRange range = new MerkleRange(0, 1024);
        
        restart.MerkleMeta meta = new restart.MerkleMeta(
            restart.MerkleMeta.CURRENT_VERSION,
            1024L,
            range,
            fileDigest,
            1024,
            null,
            (short) 58
        );
        
        assertEquals(restart.MerkleMeta.CURRENT_VERSION, meta.version());
        assertEquals(1024L, meta.totalDataSize());
        assertEquals(range, meta.computedRange());
        assertArrayEquals(fileDigest, meta.fileDigest());
        assertEquals(1024, meta.chunkSize());
        assertNull(meta.root());
        assertEquals(58, meta.footerLength());
    }

    @Test
    void testInvalidFileDigestLength() {
        byte[] invalidDigest = new byte[16]; // Wrong size
        MerkleRange range = new MerkleRange(0, 1024);
        
        assertThrows(IllegalArgumentException.class, () -> new restart.MerkleMeta(
            restart.MerkleMeta.CURRENT_VERSION,
            1024L,
            range,
            invalidDigest,
            1024,
            null,
            (short) 58
        ));
    }

    @Test
    void testInvalidChunkSize() {
        byte[] fileDigest = new byte[HASH_SIZE];
        MerkleRange range = new MerkleRange(0, 1024);
        
        assertThrows(IllegalArgumentException.class, () -> new restart.MerkleMeta(
            restart.MerkleMeta.CURRENT_VERSION,
            1024L,
            range,
            fileDigest,
            512, // Less than MIN_CHUNK_SIZE
            null,
            (short) 58
        ));
    }

    @Test
    void testInvalidVersion() {
        byte[] fileDigest = new byte[HASH_SIZE];
        MerkleRange range = new MerkleRange(0, 1024);
        
        assertThrows(IllegalStateException.class, () -> new restart.MerkleMeta(
            (byte) 0x99, // Invalid version
            1024L,
            range,
            fileDigest,
            1024,
            null,
            (short) 58
        ));
    }

    @Test
    void testRangeExceedsTotalSize() {
        byte[] fileDigest = new byte[HASH_SIZE];
        MerkleRange range = new MerkleRange(0, 2048); // Exceeds totalDataSize
        
        assertThrows(IllegalArgumentException.class, () -> new restart.MerkleMeta(
            restart.MerkleMeta.CURRENT_VERSION,
            1024L,
            range,
            fileDigest,
            1024,
            null,
            (short) 58
        ));
    }

    @ParameterizedTest
    @ValueSource(longs = {1500, 2048, 3000}) // Non-power-of-2 values
    void testChunkSizeRounding(long initialChunkSize) {
        byte[] fileDigest = new byte[HASH_SIZE];
        MerkleRange range = new MerkleRange(0, 4096);
        
        restart.MerkleMeta meta = new restart.MerkleMeta(
            restart.MerkleMeta.CURRENT_VERSION,
            4096L,
            range,
            fileDigest,
            initialChunkSize,
            null,
            (short) 58
        );
        
        // Should be rounded up to next power of 2
        long highestBit = Long.highestOneBit(initialChunkSize);
        long expectedChunkSize = (initialChunkSize > highestBit) ? highestBit << 1 : highestBit;
        assertEquals(expectedChunkSize, meta.chunkSize());
        assertTrue((meta.chunkSize() & (meta.chunkSize() - 1)) == 0); // Verify power of 2
    }

    @Test
    void testGetNumberOfLeaves() {
        byte[] fileDigest = new byte[HASH_SIZE];
        MerkleRange range = new MerkleRange(0, 4096);
        
        restart.MerkleMeta meta = new restart.MerkleMeta(
            restart.MerkleMeta.CURRENT_VERSION,
            4096L,
            range,
            fileDigest,
            1024,
            null,
            (short) 58
        );
        
        assertEquals(4, meta.getNumberOfLeaves()); // 4096/1024 = 4 leaves
    }

    @Test
    void testGetLeafIndex() {
        byte[] fileDigest = new byte[HASH_SIZE];
        MerkleRange range = new MerkleRange(0, 4096);
        
        restart.MerkleMeta meta = new restart.MerkleMeta(
            restart.MerkleMeta.CURRENT_VERSION,
            4096L,
            range,
            fileDigest,
            1024,
            null,
            (short) 58
        );
        
        assertEquals(0, meta.getLeafIndex(0));
        assertEquals(1, meta.getLeafIndex(1024));
        assertEquals(2, meta.getLeafIndex(2048));
        assertEquals(3, meta.getLeafIndex(3072));
    }

    @Test
    void testGetBoundariesForLeaf() {
        byte[] fileDigest = new byte[HASH_SIZE];
        MerkleRange range = new MerkleRange(0, 4096);
        
        restart.MerkleMeta meta = new restart.MerkleMeta(
            restart.MerkleMeta.CURRENT_VERSION,
            4096L,
            range,
            fileDigest,
            1024,
            null,
            (short) 58
        );
        
        restart.MerkleMeta.NodeBoundary boundary = meta.getBoundariesForLeaf(1);
        assertEquals(1024, boundary.start());
        assertEquals(2048, boundary.end());
        
        assertThrows(IllegalArgumentException.class, () -> meta.getBoundariesForLeaf(-1));
        assertThrows(IllegalArgumentException.class, () -> meta.getBoundariesForLeaf(4));
    }

    @Test
    void testWithRange() throws NoSuchAlgorithmException {
        // Create sample tree data
        byte[] fileDigest = new byte[HASH_SIZE];
        MerkleRange initialRange = new MerkleRange(0, 2048);
        int totalLeaves = 4;
        ByteBuffer treeData = createSampleTreeData(totalLeaves);
        
        restart.MerkleMeta meta = restart.MerkleMeta.fromBuffer(
            treeData,
            4096L,
            initialRange,
            1024,
            restart.MerkleMeta.CURRENT_VERSION,
            fileDigest,
            (short) 58
        );
        
        // Test updating range
        MerkleRange newRange = new MerkleRange(1024, 3072);
        restart.MerkleMeta updatedMeta = meta.withRange(newRange, treeData.duplicate());
        
        assertNotNull(updatedMeta.root());
        assertEquals(newRange, updatedMeta.computedRange());
        
        // Test range containment optimization
        restart.MerkleMeta sameRangeMeta = updatedMeta.withRange(newRange, treeData.duplicate());
        assertSame(updatedMeta, sameRangeMeta);
    }

    private ByteBuffer createSampleTreeData(int totalLeaves) throws NoSuchAlgorithmException {
        ByteBuffer buffer = ByteBuffer.allocate(totalLeaves * HASH_SIZE);
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        Random random = new Random(42);
        
        for (int i = 0; i < totalLeaves; i++) {
            byte[] data = new byte[32];
            random.nextBytes(data);
            byte[] hash = digest.digest(data);
            buffer.put(hash);
        }
        
        buffer.flip();
        return buffer;
    }
}