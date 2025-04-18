package io.nosqlbench.vectordata.merkle;

import io.nosqlbench.vectordata.status.EventSink;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;

/**
 * A test-specific subclass of MerklePainter that allows direct testing of the optimized download functionality.
 */
public class TestMerklePainter extends MerklePainter {
    private final TestMerklePane testPane;
    private int downloadRangeCalls = 0;

    public TestMerklePainter(TestMerklePane testPane, EventSink eventSink, Path testFile) {
        super(testFile, "http://example.com/test.dat", eventSink);
        this.testPane = testPane;

        // Create a custom MerklePane that delegates to our TestMerklePane
        MerklePane delegatePane = new MerklePane(testFile, null, null, null) {
            @Override
            public MerkleTree getMerkleTree() {
                return testPane.getMerkleTree();
            }

            @Override
            public boolean isChunkIntact(int chunkIndex) {
                return testPane.isChunkIntact(chunkIndex);
            }

            @Override
            public void submitChunk(int chunkIndex, ByteBuffer chunkData) {
                testPane.submitChunk(chunkIndex, chunkData);
            }

            @Override
            public boolean verifyChunk(int chunkIndex) {
                return testPane.verifyChunk(chunkIndex);
            }
        };

        // Replace the pane field with our delegate pane using reflection
        try {
            java.lang.reflect.Field paneField = MerklePainter.class.getDeclaredField("pane");
            paneField.setAccessible(true);
            paneField.set(this, delegatePane);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set test pane", e);
        }
    }

    @Override
    public ByteBuffer downloadRange(long start, long length) {
        downloadRangeCalls++;

        // Create a buffer with the requested data
        ByteBuffer buffer = ByteBuffer.allocate((int) length);
        byte[] data = new byte[(int) length];
        // Fill with a pattern based on the start position
        for (int i = 0; i < length; i++) {
            data[i] = (byte) ((start + i) % 256);
        }
        buffer.put(data);
        buffer.flip();
        return buffer;
    }

    /**
     * Exposes the findOptimalTransfers method for testing.
     */
    public List<NodeTransfer> testFindOptimalTransfers(int startChunk, int endChunk, int maxTransferSize) {
        try {
            // Use reflection to access the private method
            java.lang.reflect.Method method = MerklePainter.class.getDeclaredMethod(
                "findOptimalTransfers", int.class, int.class, int.class);
            method.setAccessible(true);
            return (List<NodeTransfer>) method.invoke(this, startChunk, endChunk, maxTransferSize);
        } catch (Exception e) {
            throw new RuntimeException("Failed to call findOptimalTransfers", e);
        }
    }

    /**
     * Get the number of times downloadRange was called.
     */
    public int getDownloadRangeCalls() {
        return downloadRangeCalls;
    }

    /**
     * Record class to represent a transfer of a Merkle node.
     * This is a copy of the private class in MerklePainter.
     */
    public record NodeTransfer(MerkleNode node, long start, long end) {}
}
