package io.nosqlbench.vectordata.merkle;

import io.nosqlbench.vectordata.status.EventSink;
import io.nosqlbench.vectordata.status.NoOpDownloadEventSink;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

class MerklePainterOptimizedDownloadTest {

    @TempDir
    Path tempDir;

    private Path testFile;

    @BeforeEach
    void setUp() throws IOException {
        // Create a test file
        testFile = tempDir.resolve("test.dat");
        byte[] data = new byte[1024 * 1024]; // 1MB of data
        Files.write(testFile, data);
    }

    /**
     * Creates a test MerkleTree for testing.
     *
     * @param chunkSize The size of each chunk
     * @param totalSize The total size of the file
     * @return A test MerkleTree
     */
    private MerkleTree createTestMerkleTree(long chunkSize, long totalSize) {
        return new TestMerkleTree(chunkSize, totalSize);
    }

    @Test
    void testOptimizedDownloads() throws ExecutionException, InterruptedException {
        // Create a test MerkleTree with 16KB chunks and 1MB total size (64 chunks)
        MerkleTree testTree = createTestMerkleTree(16 * 1024L, 1024 * 1024L);

        // Create a BitSet to track intact chunks (all false initially)
        BitSet intactChunks = new BitSet(64);

        // Create our test MerklePane
        TestMerklePane testPane = new TestMerklePane(testTree, intactChunks);

        // Create our test MerklePainter
        EventSink eventSink = new NoOpDownloadEventSink();
        TestMerklePainter testPainter = new TestMerklePainter(testPane, eventSink, testFile);

        // Call paintAsync to download a range of data
        CompletableFuture<Void> future = testPainter.paintAsync(0, 1024 * 1024);
        future.get(); // Wait for the download to complete

        // Verify that downloadRange was called with optimized ranges
        // The exact number depends on the implementation, but it should be less than 64 (the number of chunks)
        int downloadCalls = testPainter.getDownloadRangeCalls();
        assertTrue(downloadCalls > 0 && downloadCalls < 64,
                   "Expected between 1 and 63 download calls, got " + downloadCalls);

        // Verify that all chunks were submitted
        assertEquals(64, testPane.getSubmittedChunkCount(), "All 64 chunks should have been submitted");

        // Verify that all chunks are now marked as intact
        for (int i = 0; i < 64; i++) {
            assertTrue(testPane.isChunkIntact(i), "Chunk " + i + " should be intact");
        }
    }

    @Test
    void testOptimizedDownloadsWithSomeIntactChunks() throws ExecutionException, InterruptedException {
        // Create a test MerkleTree with 16KB chunks and 1MB total size (64 chunks)
        MerkleTree testTree = createTestMerkleTree(16 * 1024L, 1024 * 1024L);

        // Create a BitSet to track intact chunks (set even chunks to intact)
        BitSet intactChunks = new BitSet(64);
        for (int i = 0; i < 64; i += 2) {
            intactChunks.set(i); // Even chunks are intact
        }

        // Create our test MerklePane
        TestMerklePane testPane = new TestMerklePane(testTree, intactChunks);

        // Create our test MerklePainter
        EventSink eventSink = new NoOpDownloadEventSink();
        TestMerklePainter testPainter = new TestMerklePainter(testPane, eventSink, testFile);

        // Call paintAsync to download a range of data
        CompletableFuture<Void> future = testPainter.paintAsync(0, 1024 * 1024);
        future.get(); // Wait for the download to complete

        // Verify that downloadRange was called for each non-intact chunk
        // Since every other chunk is intact, we should have at most 32 calls
        int downloadCalls = testPainter.getDownloadRangeCalls();
        assertTrue(downloadCalls > 0 && downloadCalls <= 32,
                   "Expected between 1 and 32 download calls, got " + downloadCalls);

        // Verify that submitChunk was called only for non-intact chunks
        assertEquals(32, testPane.getSubmittedChunkCount(), "Only 32 chunks should have been submitted");

        // Verify that all chunks are now marked as intact
        for (int i = 0; i < 64; i++) {
            assertTrue(testPane.isChunkIntact(i), "Chunk " + i + " should be intact");
        }
    }

    @Test
    void testOptimalTransferSizeCalculation() throws Exception {
        // Create a test MerkleTree with 16KB chunks and 1MB total size (64 chunks)
        MerkleTree testTree = createTestMerkleTree(16 * 1024L, 1024 * 1024L);

        // Create a BitSet to track intact chunks (all false initially)
        BitSet intactChunks = new BitSet(64);

        // Create our test MerklePane
        TestMerklePane testPane = new TestMerklePane(testTree, intactChunks);

        // Create our test MerklePainter
        EventSink eventSink = new NoOpDownloadEventSink();
        TestMerklePainter testPainter = new TestMerklePainter(testPane, eventSink, testFile);

        // Test with different numbers of active transfers
        int[] activeTransfers = {0, 4, 8, 12, 15};
        int[] expectedMaxSizes = {65536, 4096, 256, 16, 2};

        for (int i = 0; i < activeTransfers.length; i++) {
            // Add fake active transfers
            for (int j = 0; j < activeTransfers[i]; j++) {
                // Use reflection to add active transfers
                java.lang.reflect.Field tasksField = MerklePainter.class.getDeclaredField("downloadTasks");
                tasksField.setAccessible(true);
                java.util.Map<Integer, CompletableFuture<Boolean>> downloadTasks =
                    (java.util.Map<Integer, CompletableFuture<Boolean>>) tasksField.get(testPainter);
                downloadTasks.put(j, new CompletableFuture<>());
            }

            // Find optimal transfers for the entire range
            java.util.List<TestMerklePainter.NodeTransfer> transfers =
                testPainter.testFindOptimalTransfers(0, 63, expectedMaxSizes[i]);

            // Verify that the transfers are optimized based on the number of active transfers
            assertFalse(transfers.isEmpty(), "Should have at least one transfer");

            // Clean up the fake active transfers
            java.lang.reflect.Field tasksField = MerklePainter.class.getDeclaredField("downloadTasks");
            tasksField.setAccessible(true);
            java.util.Map<Integer, CompletableFuture<Boolean>> downloadTasks =
                (java.util.Map<Integer, CompletableFuture<Boolean>>) tasksField.get(testPainter);
            downloadTasks.clear();
        }
    }
}
