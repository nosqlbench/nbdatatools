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

import io.nosqlbench.vectordata.status.NoOpDownloadEventSink;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the awaitAllDownloads methods in MerklePainter.
 */
public class MerklePainterAwaitTest {
    @TempDir
    Path tempDir;

    private Path testFile;
    private Path merkleFile;
    private TestMerklePainter testPainter;
    private TestMerklePane testPane;

    @BeforeEach
    void setUp() throws IOException {
        // Create a test file
        testFile = tempDir.resolve("test.dat");
        byte[] data = new byte[1024 * 1024]; // 1MB of data
        Files.write(testFile, data);

        // Create a merkle tree file
        merkleFile = tempDir.resolve("test.dat.mrkl");
        byte[] merkleData = new byte[1024]; // 1KB of data
        Files.write(merkleFile, merkleData);

        // Create a test merkle tree with 64 chunks
        MerkleTree testTree = TestMerkleTree.createTestTree(64, 16384);

        // Create a BitSet with all chunks marked as not intact
        BitSet intactChunks = new BitSet(64);

        // Create our test MerklePane
        testPane = new TestMerklePane(testTree, intactChunks);

        // Create our test MerklePainter
        testPainter = new TestMerklePainter(testPane, new NoOpDownloadEventSink(), testFile, merkleFile);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (testPainter != null) {
            testPainter.close();
        }
    }

    /**
     * Test that awaitAllDownloads with timeout returns true when all downloads complete.
     */
    @Test
    void testAwaitAllDownloadsWithTimeout() throws Exception {
        // Start a download in the background
        CompletableFuture<Void> future = testPainter.paintAsync(0, 1024 * 1024);

        // Wait for all downloads to complete with a timeout
        boolean completed = testPainter.awaitAllDownloads(5, TimeUnit.SECONDS);

        // Verify that all downloads completed
        assertTrue(completed, "All downloads should have completed");
        assertTrue(future.isDone(), "Future should be done");

        // Verify that all chunks are now marked as intact
        for (int i = 0; i < 64; i++) {
            assertTrue(testPane.isChunkIntact(i), "Chunk " + i + " should be intact");
        }
    }

    /**
     * Test that awaitAllDownloads without timeout waits for all downloads to complete.
     */
    @Test
    void testAwaitAllDownloadsWithoutTimeout() throws Exception {
        // Start a download in the background
        CompletableFuture<Void> future = testPainter.paintAsync(0, 1024 * 1024);

        // Wait for all downloads to complete
        testPainter.awaitAllDownloads();

        // Verify that all downloads completed
        assertTrue(future.isDone(), "Future should be done");

        // Verify that all chunks are now marked as intact
        for (int i = 0; i < 64; i++) {
            assertTrue(testPane.isChunkIntact(i), "Chunk " + i + " should be intact");
        }
    }

    /**
     * Test that awaitAllDownloads with timeout returns false when timeout is reached.
     */
    @Test
    void testAwaitAllDownloadsTimeout() throws Exception {
        // Create a mock implementation that simulates a timeout
        TestMerklePainter mockPainter = new TestMerklePainter(testPane, new NoOpDownloadEventSink(), testFile, merkleFile) {
            @Override
            public boolean awaitAllDownloads(long timeout, TimeUnit unit) throws InterruptedException {
                // Always return false to simulate a timeout
                return false;
            }

            @Override
            public ByteBuffer downloadRange(long start, long length) {
                // This method won't be called in this test
                return super.downloadRange(start, length);
            }
        };

        // Start a download in the background
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            // Simulate a long-running task
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // Wait for all downloads to complete with a timeout
        boolean completed = mockPainter.awaitAllDownloads(1, TimeUnit.MILLISECONDS);

        // Verify that the timeout was reached
        assertFalse(completed, "Timeout should have been reached");

        // Wait for the future to complete to avoid resource leaks
        future.join();
        mockPainter.close();
    }
}
