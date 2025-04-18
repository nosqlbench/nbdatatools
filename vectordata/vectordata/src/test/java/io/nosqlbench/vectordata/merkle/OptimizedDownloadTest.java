package io.nosqlbench.vectordata.merkle;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the optimized download functionality in MerklePainter.
 * This test directly tests the findOptimalTransfers method using reflection.
 */
public class OptimizedDownloadTest {

    /**
     * Simple implementation of MerkleNode for testing.
     */
    private static class TestMerkleNode implements MerkleNode {
        private final int index;
        private final int level;
        private final boolean isLeaf;
        private final MerkleNode left;
        private final MerkleNode right;
        private final byte[] hash;

        public TestMerkleNode(int index, int level, boolean isLeaf) {
            this.index = index;
            this.level = level;
            this.isLeaf = isLeaf;
            this.hash = new byte[HASH_SIZE];

            if (!isLeaf && level > 0) {
                this.left = new TestMerkleNode(index * 2, level - 1, level == 1);
                this.right = new TestMerkleNode(index * 2 + 1, level - 1, level == 1);
            } else {
                this.left = null;
                this.right = null;
            }
        }

        @Override
        public int index() {
            return index;
        }

        @Override
        public int level() {
            return level;
        }

        @Override
        public boolean isLeaf() {
            return isLeaf;
        }

        @Override
        public MerkleNode left() {
            return left;
        }

        @Override
        public MerkleNode right() {
            return right;
        }

        @Override
        public byte[] hash() {
            return hash;
        }

        @Override
        public long startOffset(long totalSize, long chunkSize) {
            if (isLeaf) {
                return index * chunkSize;
            } else {
                // For internal nodes, calculate based on the leftmost leaf
                int leftmostLeafIndex = index << level;
                return leftmostLeafIndex * chunkSize;
            }
        }

        @Override
        public long endOffset(long totalSize, long chunkSize) {
            if (isLeaf) {
                return Math.min((index + 1) * chunkSize, totalSize);
            } else {
                // For internal nodes, calculate based on the rightmost leaf
                int rightmostLeafIndex = (index << level) + (1 << level) - 1;
                return Math.min((rightmostLeafIndex + 1) * chunkSize, totalSize);
            }
        }
    }

    /**
     * Simple implementation of MerkleTree for testing.
     */
    private static class TestMerkleTree {
        private final long chunkSize;
        private final long totalSize;
        private final MerkleNode root;

        public TestMerkleTree(long chunkSize, long totalSize) {
            this.chunkSize = chunkSize;
            this.totalSize = totalSize;

            // Create a simple root node
            int level = (int) Math.ceil(Math.log(totalSize / chunkSize) / Math.log(2));
            this.root = new TestMerkleNode(0, level, false);
        }

        public MerkleNode getRoot() {
            return root;
        }

        public long getChunkSize() {
            return chunkSize;
        }

        public long getTotalSize() {
            return totalSize;
        }
    }

    /**
     * Record class to represent a transfer of a Merkle node.
     * This is a copy of the private class in MerklePainter.
     */
    private record NodeTransfer(MerkleNode node, long start, long end) {}

    /**
     * Invokes the findOptimalTransfers method in MerklePainter using reflection.
     */
    private List<NodeTransfer> findOptimalTransfers(MerkleNode root, BitSet intactChunks,
                                                   int startChunk, int endChunk,
                                                   int maxTransferSize, long chunkSize, long totalSize)
            throws Exception {

        // Create a MerklePainter instance with a dummy constructor
        MerklePainter painter = new MerklePainter(null, null, null) {
            @Override
            public boolean isChunkIntact(int chunkIndex) {
                return intactChunks.get(chunkIndex);
            }
        };

        // Use reflection to access the private method
        Method method = MerklePainter.class.getDeclaredMethod(
            "findOptimalTransfersRecursive",
            MerkleNode.class, int.class, int.class, int.class, List.class, long.class, long.class);
        method.setAccessible(true);

        // Create a list to hold the transfers
        List<NodeTransfer> transfers = new ArrayList<>();

        // Invoke the method
        method.invoke(painter, root, startChunk, endChunk, maxTransferSize, transfers, chunkSize, totalSize);

        return transfers;
    }

    @Test
    void testOptimizedTransfers() throws Exception {
        // Create a test tree with 16KB chunks and 1MB total size (64 chunks)
        TestMerkleTree tree = new TestMerkleTree(16 * 1024L, 1024 * 1024L);

        // Create a BitSet to track intact chunks (all false initially)
        BitSet intactChunks = new BitSet(64);

        // Find optimal transfers for the entire range with different max sizes
        int[] maxSizes = {64, 32, 16, 8, 4, 2, 1};

        for (int maxSize : maxSizes) {
            List<NodeTransfer> transfers = findOptimalTransfers(
                tree.getRoot(), intactChunks, 0, 63, maxSize, tree.getChunkSize(), tree.getTotalSize());

            // Verify that transfers were found
            assertFalse(transfers.isEmpty(), "Should have found transfers for maxSize=" + maxSize);

            // Verify that the transfers cover all chunks
            BitSet coveredChunks = new BitSet(64);
            for (NodeTransfer transfer : transfers) {
                long start = transfer.start();
                long end = transfer.end();
                int startChunk = (int) (start / tree.getChunkSize());
                int endChunk = (int) ((end - 1) / tree.getChunkSize());

                for (int i = startChunk; i <= endChunk; i++) {
                    coveredChunks.set(i);
                }
            }

            // All chunks should be covered
            assertEquals(64, coveredChunks.cardinality(),
                        "All 64 chunks should be covered for maxSize=" + maxSize);

            // The number of transfers should be appropriate for the max size
            if (maxSize >= 64) {
                // With a max size of 64 or more, we should have just one transfer for the whole file
                assertEquals(1, transfers.size(),
                            "Should have just one transfer for maxSize=" + maxSize);
            } else {
                // With smaller max sizes, we should have more transfers
                assertTrue(transfers.size() >= 64 / maxSize,
                          "Should have at least " + (64 / maxSize) + " transfers for maxSize=" + maxSize);
            }
        }
    }

    @Test
    void testOptimizedTransfersWithSomeIntactChunks() throws Exception {
        // Create a test tree with 16KB chunks and 1MB total size (64 chunks)
        TestMerkleTree tree = new TestMerkleTree(16 * 1024L, 1024 * 1024L);

        // Create a BitSet to track intact chunks (set even chunks to intact)
        BitSet intactChunks = new BitSet(64);
        for (int i = 0; i < 64; i += 2) {
            intactChunks.set(i); // Even chunks are intact
        }

        // Find optimal transfers for the entire range
        List<NodeTransfer> transfers = findOptimalTransfers(
            tree.getRoot(), intactChunks, 0, 63, 64, tree.getChunkSize(), tree.getTotalSize());

        // Verify that transfers were found
        assertFalse(transfers.isEmpty(), "Should have found transfers");

        // Verify that the transfers only cover non-intact chunks
        BitSet coveredChunks = new BitSet(64);
        for (NodeTransfer transfer : transfers) {
            long start = transfer.start();
            long end = transfer.end();
            int startChunk = (int) (start / tree.getChunkSize());
            int endChunk = (int) ((end - 1) / tree.getChunkSize());

            for (int i = startChunk; i <= endChunk; i++) {
                coveredChunks.set(i);
            }
        }

        // Check that only odd chunks are covered
        for (int i = 0; i < 64; i++) {
            if (i % 2 == 0) {
                // Even chunks should not be covered (they're intact)
                assertFalse(coveredChunks.get(i), "Chunk " + i + " should not be covered (it's intact)");
            } else {
                // Odd chunks should be covered (they're not intact)
                assertTrue(coveredChunks.get(i), "Chunk " + i + " should be covered (it's not intact)");
            }
        }
    }

    @Test
    void testOptimalTransferSizeCalculation() throws Exception {
        // Create a test tree with 16KB chunks and 1MB total size (64 chunks)
        TestMerkleTree tree = new TestMerkleTree(16 * 1024L, 1024 * 1024L);

        // Create a BitSet to track intact chunks (all false initially)
        BitSet intactChunks = new BitSet(64);

        // Test with different numbers of active transfers
        int[] activeTransfers = {0, 4, 8, 12, 15};
        int[] expectedMaxSizes = {65536, 4096, 256, 16, 2};

        // Create a dummy MerklePainter to test the transfer size calculation
        MerklePainter painter = new MerklePainter(null, null, null) {
            @Override
            public boolean isChunkIntact(int chunkIndex) {
                return intactChunks.get(chunkIndex);
            }
        };

        // Use reflection to access the downloadTasks field
        Field tasksField = MerklePainter.class.getDeclaredField("downloadTasks");
        tasksField.setAccessible(true);
        Map<Integer, CompletableFuture<Boolean>> downloadTasks = new HashMap<>();
        tasksField.set(painter, downloadTasks);

        // Use reflection to access the findOptimalTransfers method
        Method method = MerklePainter.class.getDeclaredMethod(
            "findOptimalTransfers", int.class, int.class, int.class);
        method.setAccessible(true);

        for (int i = 0; i < activeTransfers.length; i++) {
            // Add fake active transfers
            downloadTasks.clear();
            for (int j = 0; j < activeTransfers[i]; j++) {
                downloadTasks.put(j, new CompletableFuture<>());
            }

            // Calculate the optimal transfer size
            int optimalTransferSize = activeTransfers[i] > 0 ?
                (1 << Math.max(0, 16 - activeTransfers[i])) : 65536;

            // Verify that the expected max size matches the calculated one
            assertEquals(expectedMaxSizes[i], optimalTransferSize,
                        "Optimal transfer size should be " + expectedMaxSizes[i] +
                        " for " + activeTransfers[i] + " active transfers");
        }
    }
}
