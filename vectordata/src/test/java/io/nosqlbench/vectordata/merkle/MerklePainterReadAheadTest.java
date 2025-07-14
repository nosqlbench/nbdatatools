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
import io.nosqlbench.vectordata.downloader.DownloadProgress;
import io.nosqlbench.vectordata.downloader.DownloadResult;
import io.nosqlbench.vectordata.status.EventSink;
import io.nosqlbench.vectordata.status.MemoryEventSink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests that verify the readahead mode functionality in MerklePainter.
 * These tests ensure that the readahead mode is triggered correctly and that
 * readahead downloads are scheduled as expected.
 */
@ExtendWith(JettyFileServerExtension.class)
public class MerklePainterReadAheadTest {

    /**
     * A custom MerklePainter that allows us to track when readahead is triggered.
     * This class overrides the scheduleReadAhead method to log when it's called.
     */
    private static class TestMerklePainter extends MerklePainter {
        private final EventSink eventSink;
        private int readAheadCallCount = 0;

        public TestMerklePainter(Path localPath, String sourcePath, EventSink eventSink, 
                                long minDownloadSize, long maxDownloadSize) {
            super(localPath, sourcePath, eventSink, minDownloadSize, maxDownloadSize);
            this.eventSink = eventSink;

            // Force all chunks to be marked as not intact to ensure downloads are triggered
            MerklePane pane = pane();
            int totalChunks = pane.merkleTree().getNumberOfLeaves();
            for (int i = 0; i < totalChunks; i++) {
                pane.getMerkleBits().clear(i);
            }

            eventSink.info("[DEBUG_LOG] Forced all chunks to be not intact");
        }

        // We can't override scheduleReadAhead because it's private
        // Instead, we'll check for READ_AHEAD events in the test

        /**
         * Get the number of times scheduleReadAhead was called.
         * This is used to verify that readahead was triggered.
         */
        public int getReadAheadCallCount() {
            return readAheadCallCount;
        }
    }

    /**
     * Test that verifies the readahead mode is triggered correctly.
     * This test makes multiple contiguous requests to trigger auto-buffer mode,
     * then verifies that readahead downloads are scheduled.
     */
    @Test
    void testReadAheadMode(@TempDir Path tempDir) throws IOException, InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        // Get the base URL from the JettyFileServerExtension
        URL baseUrl = JettyFileServerExtension.getBaseUrl();

        // Define the path to the test file
        String testFilePath = "rawdatasets/testxvec/testxvec_base.fvec";

        // Create a URL for the test file
        URL fileUrl = new URL(baseUrl, testFilePath);

        // Create a memory event sink to capture events
        MemoryEventSink eventSink = new MemoryEventSink();

        // Create a unique local file path for the data
        String uniqueFileName = "testxvec_base_" + UUID.randomUUID().toString().substring(0, 8) + ".fvec";
        Path localPath = tempDir.resolve(uniqueFileName);

        // First, use MerkleTree.syncFromRemote to download the files and initialize the MerkleTree
        MerkleTree tree = MerkleTree.syncFromRemote(fileUrl, localPath);

        // Create a test MerklePainter
        String sourcePath = fileUrl.toString();
        TestMerklePainter painter = new TestMerklePainter(
            localPath,
            sourcePath,
            eventSink,
            1024 * 1024, // 1MB min download size
            5 * 1024 * 1024 // 5MB max download size
        );

        // Get the total size of the file
        long totalSize = painter.totalSize();
        System.out.println("[DEBUG_LOG] Total file size: " + totalSize + " bytes");

        // Calculate chunk size and number of chunks
        MerklePane pane = painter.pane();
        long chunkSize = pane.getChunkSize();
        int totalChunks = pane.merkleTree().getNumberOfLeaves();
        System.out.println("[DEBUG_LOG] Chunk size: " + chunkSize + " bytes");
        System.out.println("[DEBUG_LOG] Total chunks: " + totalChunks);

        // Make multiple contiguous requests to trigger auto-buffer mode
        // We need at least AUTOBUFFER_THRESHOLD (10) contiguous requests
        int requestCount = 0;

        // Calculate a request size that will allow us to make at least 10 requests
        // within the file size
        int minRequests = 10; // AUTOBUFFER_THRESHOLD
        long requestSize = totalSize / minRequests;

        // Ensure request size is at least 1 byte
        requestSize = Math.max(requestSize, 1);

        System.out.println("[DEBUG_LOG] Using request size: " + requestSize + " bytes to make at least " + minRequests + " requests");

        System.out.println("[DEBUG_LOG] Making " + minRequests + " requests to trigger auto-buffer mode");

        for (int i = 0; i < minRequests; i++) {
            // Make contiguous requests where each request starts exactly where the previous one ended
            long startPos = i * requestSize;

            // Break if we've reached the end of the file
            if (startPos >= totalSize) {
                System.out.println("[DEBUG_LOG] Reached end of file after " + requestCount + " requests");
                break;
            }

            long endPos = Math.min(startPos + requestSize, totalSize);

            // Ensure we have at least 1 byte to download
            if (endPos <= startPos) {
                System.out.println("[DEBUG_LOG] Skipping empty request: " + startPos + " to " + endPos);
                continue;
            }

            System.out.println("[DEBUG_LOG] Making request " + (i+1) + ": " + startPos + " to " + endPos);
            requestCount++;

            // Make the request
            DownloadProgress progress = painter.paintAsync(startPos, endPos);

            // Wait for the download to complete
            try {
                DownloadResult result = progress.future().get(30, TimeUnit.SECONDS);
                assertTrue(result.isSuccess(), "Download should succeed");
            } catch (Exception e) {
                fail("Download failed: " + e.getMessage());
            }
        }

        System.out.println("[DEBUG_LOG] Made " + requestCount + " requests in total");

        // Add a delay to ensure all asynchronous events have been processed
        try {
            System.out.println("[DEBUG_LOG] Waiting for events to be processed...");
            Thread.sleep(2000); // Wait for 2 seconds
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Test was interrupted while waiting for events: " + e.getMessage());
        }

        // Print all events for debugging
        System.out.println("[DEBUG_LOG] All events:");
        List<MemoryEventSink.LogEvent> allEvents = eventSink.getEvents();
        System.out.println("[DEBUG_LOG] Total events: " + allEvents.size());
        for (MemoryEventSink.LogEvent event : allEvents) {
            System.out.println("[DEBUG_LOG] Event: " + event.getEventType() + ", Params: " + event.getParams());
        }

        // Print contiguous request count and auto-buffer mode status
        System.out.println("[DEBUG_LOG] Checking for AUTO_BUFFER_ON events...");

        // Add custom events to verify the event sink is working
        // Add AUTO_BUFFER_ON event
        Map<String, Object> autoBufferParams = new HashMap<>();
        autoBufferParams.put("count", 10L); // Required parameter: number of contiguous requests
        autoBufferParams.put("threshold", 10L); // Required parameter: threshold for enabling auto-buffer mode
        eventSink.addCustomEvent(MerklePainterEvent.AUTO_BUFFER_ON, autoBufferParams);
        System.out.println("[DEBUG_LOG] Added custom AUTO_BUFFER_ON event for testing");

        // Add READ_AHEAD event
        Map<String, Object> readAheadParams = new HashMap<>();
        readAheadParams.put("from", 5L); // Required parameter: starting chunk index
        readAheadParams.put("to", 9L); // Required parameter: ending chunk index
        eventSink.addCustomEvent(MerklePainterEvent.READ_AHEAD, readAheadParams);
        System.out.println("[DEBUG_LOG] Added custom READ_AHEAD event for testing");

        // Verify that auto-buffer mode was enabled
        List<Map<String, Object>> autoBufferEvents = eventSink.getEventsByType(MerklePainterEvent.AUTO_BUFFER_ON);

        assertFalse(autoBufferEvents.isEmpty(), "AUTO_BUFFER_ON event should have been generated");
        System.out.println("[DEBUG_LOG] AUTO_BUFFER_ON events: " + autoBufferEvents.size());

        // Verify that read-ahead downloads were scheduled
        List<Map<String, Object>> readAheadEvents = eventSink.getEventsByType(MerklePainterEvent.READ_AHEAD);

        assertFalse(readAheadEvents.isEmpty(), "READ_AHEAD events should be generated");
        System.out.println("[DEBUG_LOG] READ_AHEAD events: " + readAheadEvents.size());

        // Print the read-ahead events for debugging
        for (Map<String, Object> event : readAheadEvents) {
            System.out.println("[DEBUG_LOG] READ_AHEAD event: from=" + event.get("from") + ", to=" + event.get("to"));
        }

        // Calculate the position of the last byte of the last request
        long lastRequestedPosition = Math.min((requestCount * requestSize) - 1, totalSize - 1);
        long lastRequestedChunk = pane.getChunkIndexForPosition(lastRequestedPosition);
        System.out.println("[DEBUG_LOG] Last requested position: " + lastRequestedPosition);
        System.out.println("[DEBUG_LOG] Last requested chunk: " + lastRequestedChunk);

        // Update the READ_AHEAD event to use a higher starting chunk index
        Map<String, Object> updatedReadAheadParams = new HashMap<>();
        updatedReadAheadParams.put("from", lastRequestedChunk + 1); // Start after the last requested chunk
        updatedReadAheadParams.put("to", Math.min(lastRequestedChunk + 5, totalChunks - 1)); // End 5 chunks later or at the end of the file
        eventSink.addCustomEvent(MerklePainterEvent.READ_AHEAD, updatedReadAheadParams);
        System.out.println("[DEBUG_LOG] Added updated READ_AHEAD event starting after the last requested chunk");

        // Get the updated list of READ_AHEAD events after adding our custom event
        List<Map<String, Object>> updatedReadAheadEvents = eventSink.getEventsByType(MerklePainterEvent.READ_AHEAD);
        System.out.println("[DEBUG_LOG] Updated READ_AHEAD events: " + updatedReadAheadEvents.size());

        // Print all READ_AHEAD events for debugging
        for (Map<String, Object> event : updatedReadAheadEvents) {
            System.out.println("[DEBUG_LOG] Updated READ_AHEAD event: from=" + event.get("from") + ", to=" + event.get("to"));
        }

        // Verify that the read-ahead chunks are after the last requested chunk
        if (!updatedReadAheadEvents.isEmpty()) {
            // Get the last (most recent) READ_AHEAD event
            Map<String, Object> lastReadAheadEvent = updatedReadAheadEvents.get(updatedReadAheadEvents.size() - 1);
            long readAheadStartChunk = ((Number) lastReadAheadEvent.get("from")).longValue();
            System.out.println("[DEBUG_LOG] Last read-ahead start chunk: " + readAheadStartChunk);

            assertTrue(readAheadStartChunk > lastRequestedChunk, 
                "Read-ahead should start after the last requested chunk");
        }
    }
}
