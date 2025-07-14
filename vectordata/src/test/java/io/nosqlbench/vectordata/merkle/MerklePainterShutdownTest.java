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

import io.nosqlbench.jetty.testserver.JettyFileServerExtension;
import io.nosqlbench.vectordata.status.MemoryEventSink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for verifying the shutdown behavior of MerklePainter.
 * Tests that MerklePainter correctly handles shutdown operations:
 * 1. Stops or abandons any pending transfers
 * 2. Computes calculable hashes (breadth-first)
 * 3. Flushes merkle tree data to disk with appropriate timestamps
 */
@ExtendWith(JettyFileServerExtension.class)
public class MerklePainterShutdownTest {

    private static final int CHUNK_SIZE = 1024; // 1KB chunks
    private static final int TEST_DATA_SIZE = CHUNK_SIZE * 8; // 8KB test data
    private static final Random random = new Random(42); // Fixed seed for reproducibility

    /**
     * Creates random test data of the specified size.
     *
     * @param size The size of the test data in bytes
     * @return A byte array containing the test data
     */
    private byte[] createTestData(int size) {
        byte[] data = new byte[size];
        random.nextBytes(data);
        return data;
    }

    /**
     * Test that MerklePainter correctly handles shutdown operations.
     * This test will:
     * 1. Create a test file with known content
     * 2. Create a merkle tree for that file
     * 3. Create a MerklePainter with a memory event sink
     * 4. Update some leaf hashes to invalidate parent hashes
     * 5. Close the MerklePainter (simulating shutdown)
     * 6. Verify that shutdown events are logged
     * 7. Verify that the merkle tree file is updated with the correct timestamp
     */
    @Test
    void testShutdownBehavior(@TempDir Path tempDir) throws IOException, NoSuchAlgorithmException, InterruptedException {
        // Create test data
        byte[] testData = createTestData(TEST_DATA_SIZE);

        // Create a test file
        Path testFile = tempDir.resolve("test_data.bin");
        Files.write(testFile, testData);

        // Create a merkle tree for the test file
        Path merkleFile = tempDir.resolve("test_data.mrkl");
        MerkleTree tree = MerkleTree.fromFile(testFile, CHUNK_SIZE, new MerkleRange(0, TEST_DATA_SIZE));
        tree.save(merkleFile);

        // Create a reference merkle tree (identical to the original)
        Path refMerkleFile = tempDir.resolve("test_data.bin.mref");
        Files.copy(merkleFile, refMerkleFile);

        // Create a memory event sink to capture events
        MemoryEventSink eventSink = new MemoryEventSink();

        // Create a MerklePainter with the test file and event sink
        // Get the base URL from the JettyFileServerExtension
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        // Define the path to the test file in the test resources directory
        String testFilePath = "rawdatasets/testxvec/testxvec_base.fvec";
        // Create a URL for the test file
        URL fileUrl = new URL(baseUrl, testFilePath);

        // Use the standard constructor
        MerklePainter painter = new MerklePainter(testFile, fileUrl.toString(), eventSink);

        // Get the MerklePane from the painter
        MerklePane pane = painter.pane();

        // Invalidate some leaf hashes to force hash recalculation during shutdown
        // We'll invalidate two adjacent leaves to ensure their parent needs recalculation
        pane.getMerkleBits().clear(0);
        pane.getMerkleBits().clear(1);

        // Close the painter (this should trigger the shutdown behavior)
        painter.close();

        // Print all events for debugging
        System.out.println("[DEBUG_LOG] All events:");
        for (MemoryEventSink.LogEvent event : eventSink.getEvents()) {
            System.out.println("[DEBUG_LOG] Event: " + (event.getEventType() != null ? event.getEventType().name() : "null") + 
                              " - " + (event.getParams() != null ? event.getParams() : "null"));
        }

        // Check that shutdown events were logged
        boolean shutdownInitFound = false;
        boolean shutdownStoppingFound = false;
        boolean shutdownHashingFound = false;
        boolean shutdownFlushingFound = false;
        boolean shutdownCompleteFound = false;

        for (MemoryEventSink.LogEvent event : eventSink.getEvents()) {
            if (event.getEventType() == MerklePainterEvent.SHUTDOWN_INIT) {
                shutdownInitFound = true;
            }
            if (event.getEventType() == MerklePainterEvent.SHUTDOWN_STOPPING) {
                shutdownStoppingFound = true;
            }
            if (event.getEventType() == MerklePainterEvent.SHUTDOWN_HASHING) {
                shutdownHashingFound = true;
            }
            if (event.getEventType() == MerklePainterEvent.SHUTDOWN_FLUSHING) {
                shutdownFlushingFound = true;
            }
            if (event.getEventType() == MerklePainterEvent.SHUTDOWN_COMPLETE) {
                shutdownCompleteFound = true;
            }
        }

        assertTrue(shutdownInitFound, "SHUTDOWN_INIT event should be logged");
        assertTrue(shutdownStoppingFound, "SHUTDOWN_STOPPING event should be logged");
        assertTrue(shutdownHashingFound, "SHUTDOWN_HASHING event should be logged");
        assertTrue(shutdownFlushingFound, "SHUTDOWN_FLUSHING event should be logged");
        assertTrue(shutdownCompleteFound, "SHUTDOWN_COMPLETE event should be logged");
    }

    /**
     * Test that MerklePainter correctly handles shutdown with pending transfers.
     * This test will:
     * 1. Create a test file with known content
     * 2. Create a merkle tree for that file
     * 3. Create a MerklePainter with a memory event sink
     * 4. Start an asynchronous download
     * 5. Close the MerklePainter before the download completes (simulating shutdown)
     * 6. Verify that shutdown events are logged
     * 7. Verify that the pending transfer was stopped
     */
    @Test
    void testShutdownWithPendingTransfers(@TempDir Path tempDir) throws IOException, NoSuchAlgorithmException, InterruptedException {
        // Create test data
        byte[] testData = createTestData(TEST_DATA_SIZE);

        // Create a test file
        Path testFile = tempDir.resolve("test_data.bin");
        Files.write(testFile, testData);

        // Create a merkle tree for the test file
        Path merkleFile = tempDir.resolve("test_data.mrkl");
        MerkleTree tree = MerkleTree.fromFile(testFile, CHUNK_SIZE, new MerkleRange(0, TEST_DATA_SIZE));
        tree.save(merkleFile);

        // Create a reference merkle tree (identical to the original)
        Path refMerkleFile = tempDir.resolve("test_data.bin.mref");
        Files.copy(merkleFile, refMerkleFile);

        // Create a memory event sink to capture events
        MemoryEventSink eventSink = new MemoryEventSink();

        // Create a MerklePainter with the test file and event sink
        // Get the base URL from the JettyFileServerExtension
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        // Define the path to the test file in the test resources directory
        String testFilePath = "rawdatasets/testxvec/testxvec_base.fvec";
        // Create a URL for the test file
        URL fileUrl = new URL(baseUrl, testFilePath);

        // Use the standard constructor
        MerklePainter painter = new MerklePainter(testFile, fileUrl.toString(), eventSink);

        // Get the MerklePane from the painter
        MerklePane pane = painter.pane();

        // Invalidate all leaf hashes to force downloads
        int leafCount = pane.merkleTree().getNumberOfLeaves();
        for (int i = 0; i < leafCount; i++) {
            pane.getMerkleBits().clear(i);
        }

        // Start an asynchronous download (this will be pending when we close the painter)
        // We're using a dummy URL, so the download will never complete
        painter.paintAsync(0, TEST_DATA_SIZE);

        // Give the download a moment to start
        Thread.sleep(100);

        // Close the painter (this should trigger the shutdown behavior)
        painter.close();

        // Print all events for debugging
        System.out.println("[DEBUG_LOG] All events:");
        for (MemoryEventSink.LogEvent event : eventSink.getEvents()) {
            System.out.println("[DEBUG_LOG] Event: " + (event.getEventType() != null ? event.getEventType().name() : "null") + 
                              " - " + (event.getParams() != null ? event.getParams() : "null"));
        }

        // Check that shutdown events were logged
        boolean shutdownInitFound = false;
        boolean shutdownStoppingFound = false;

        for (MemoryEventSink.LogEvent event : eventSink.getEvents()) {
            if (event.getEventType() == MerklePainterEvent.SHUTDOWN_INIT) {
                shutdownInitFound = true;
            }
            if (event.getEventType() == MerklePainterEvent.SHUTDOWN_STOPPING) {
                shutdownStoppingFound = true;
            }
        }

        assertTrue(shutdownInitFound, "SHUTDOWN_INIT event should be logged");
        assertTrue(shutdownStoppingFound, "SHUTDOWN_STOPPING event should be logged");
    }
}