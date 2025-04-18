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


import io.nosqlbench.vectordata.status.EventSink;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A test-specific subclass of MerklePainter that allows direct testing of the optimized download functionality.
 */
public class TestMerklePainter extends MerklePainter {
    private final TestMerklePane testPane;
    private final TestMerklePaneImpl testPaneImpl;
    private int downloadRangeCalls = 0;

    public TestMerklePainter(TestMerklePane testPane, EventSink eventSink, Path testFile, Path merkleFile) {
        // Use a null URL to avoid trying to download from a real URL
        super(testFile, null, eventSink);
        this.testPane = testPane;
        this.testPaneImpl = new TestMerklePaneImpl(testPane, testFile, merkleFile);

        // Create a custom MerklePane adapter that delegates to our TestMerklePaneImpl
        MerklePane delegatePane = new MerklePane(testFile) {
            @Override
            public MerkleTree getMerkleTree() {
                return testPaneImpl.getMerkleTree();
            }

            @Override
            public boolean isChunkIntact(int chunkIndex) {
                return testPaneImpl.isChunkIntact(chunkIndex);
            }

            @Override
            public void submitChunk(int chunkIndex, ByteBuffer chunkData) {
                testPaneImpl.submitChunk(chunkIndex, chunkData);
            }

            @Override
            public boolean verifyChunk(int chunkIndex) {
                return testPaneImpl.verifyChunk(chunkIndex);
            }

            @Override
            public Path getFilePath() {
                return testPaneImpl.getFilePath();
            }

            @Override
            public Path getMerklePath() {
                return testPaneImpl.getMerklePath();
            }

            @Override
            public FileChannel getChannel() {
                return testPaneImpl.getChannel();
            }

            @Override
            public long getFileSize() {
                return testPaneImpl.getFileSize();
            }

            @Override
            public BitSet getIntactChunks() {
                return testPaneImpl.getIntactChunks();
            }

            @Override
            public MerklePane.MerkleBits getMerkleBits() {
                return testPaneImpl.getMerkleBits();
            }

            @Override
            public ByteBuffer readChunk(int chunkIndex) throws IOException {
                return testPaneImpl.readChunk(chunkIndex);
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
     * Override the paintAsync method to ensure it properly submits chunks
     * This implementation optimizes downloads by combining adjacent chunks
     */
    @Override
    public CompletableFuture<Void> paintAsync(long startPosition, long endPosition) {
        // Get the merkle tree
        MerkleTree merkleTree = testPane.getMerkleTree();

        // Calculate the chunk indices for the range
        int startChunk = merkleTree.getChunkIndexForPosition(startPosition);
        int endChunk = merkleTree.getChunkIndexForPosition(Math.min(endPosition - 1, merkleTree.totalSize() - 1));

        // Optimize downloads by combining adjacent chunks
        int i = startChunk;
        while (i <= endChunk) {
            // Skip chunks that are already intact
            if (testPane.isChunkIntact(i)) {
                i++;
                continue;
            }

            // Find the end of the current run of non-intact chunks
            int runEnd = i;
            while (runEnd < endChunk && !testPane.isChunkIntact(runEnd + 1)) {
                runEnd++;
            }

            // Calculate the boundaries for this run
            MerkleTree.NodeBoundary startBounds = merkleTree.getBoundariesForLeaf(i);
            MerkleTree.NodeBoundary endBounds = merkleTree.getBoundariesForLeaf(runEnd);
            long runStart = startBounds.start();
            long runEndPos = endBounds.end();
            int runLength = (int)(runEndPos - runStart);

            // Download the entire run at once
            ByteBuffer buffer = downloadRange(runStart, runLength);

            // Submit each chunk in the run
            for (int j = i; j <= runEnd; j++) {
                MerkleTree.NodeBoundary bounds = merkleTree.getBoundariesForLeaf(j);
                long chunkStart = bounds.start();
                long chunkEnd = bounds.end();
                int chunkLength = (int)(chunkEnd - chunkStart);

                // Extract the chunk data from the buffer
                ByteBuffer chunkBuffer = ByteBuffer.allocate(chunkLength);
                buffer.position((int)(chunkStart - runStart));
                byte[] chunkData = new byte[chunkLength];
                buffer.get(chunkData, 0, chunkLength);
                chunkBuffer.put(chunkData);
                chunkBuffer.flip();

                // Submit the chunk
                testPane.submitChunk(j, chunkBuffer);
            }

            // Move to the next chunk after this run
            i = runEnd + 1;
        }

        return CompletableFuture.completedFuture(null);
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
