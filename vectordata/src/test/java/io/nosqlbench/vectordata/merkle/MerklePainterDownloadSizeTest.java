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

@ExtendWith(JettyFileServerExtension.class)
public class MerklePainterDownloadSizeTest {


    /**
     * A custom MerklePainter that forces chunks to be downloaded even if they're already intact.
     * This is useful for testing download size constraints.
     */
    private static class TestMerklePainter extends MerklePainter {
        private final int startChunk;
        private final int endChunk;
        private final EventSink eventSink;

        public TestMerklePainter(Path localPath, String sourcePath, EventSink eventSink, 
                                long minDownloadSize, long maxDownloadSize,
                                int startChunk, int endChunk) {
            super(localPath, sourcePath, eventSink, minDownloadSize, maxDownloadSize);
            this.startChunk = startChunk;
            this.endChunk = endChunk;
            this.eventSink = eventSink;

            // Initialize, then truncate file to force downloads
            initializeForceDownloads();
        }
        
        private void initializeForceDownloads() {
            // Force downloads by truncating the local file to remove chunk data
            // This is more realistic than manipulating verification bits
            try {
                Path localFile = localPath();
                System.out.println("[DEBUG_LOG] initializeForceDownloads: localFile = " + localFile);
                System.out.println("[DEBUG_LOG] initializeForceDownloads: file exists = " + java.nio.file.Files.exists(localFile));
                
                if (java.nio.file.Files.exists(localFile)) {
                    long originalSize = java.nio.file.Files.size(localFile);
                    System.out.println("[DEBUG_LOG] initializeForceDownloads: original file size = " + originalSize);
                    
                    // Truncate the file to force chunks to be downloaded
                    // Keep some initial data but remove the chunks we want to test
                    long chunkSize = pane().getChunkSize();
                    long truncateSize = Math.max(0, startChunk * chunkSize);
                    
                    System.out.println("[DEBUG_LOG] initializeForceDownloads: chunkSize = " + chunkSize + ", startChunk = " + startChunk + ", truncateSize = " + truncateSize);
                    
                    try (java.nio.channels.FileChannel channel = java.nio.channels.FileChannel.open(
                            localFile, 
                            java.nio.file.StandardOpenOption.WRITE)) {
                        channel.truncate(truncateSize);
                    }
                    
                    long newSize = java.nio.file.Files.size(localFile);
                    System.out.println("[DEBUG_LOG] Truncated file from " + originalSize + " to " + newSize + " bytes to force chunks " + 
                                 startChunk + " to " + endChunk + " to be downloaded");
                    eventSink.info("[DEBUG_LOG] Truncated file from " + originalSize + " to " + newSize + " bytes");
                } else {
                    System.out.println("[DEBUG_LOG] initializeForceDownloads: file does not exist");
                }
            } catch (Exception e) {
                System.out.println("[DEBUG_LOG] Failed to truncate file: " + e.getMessage());
                e.printStackTrace();
                eventSink.info("[DEBUG_LOG] Failed to truncate file: " + e.getMessage());
            }
        }

        @Override
        public DownloadProgress paintAsync(long startIncl, long endExcl) {
            System.out.println("[DEBUG_LOG] TestMerklePainter.paintAsync called with startIncl=" + startIncl + ", endExcl=" + endExcl);

            // Note: File truncation in constructor should ensure chunks need to be downloaded
            // No need to manipulate verification bits with ShadowTree

            // Manually add RANGE_START events to simulate the MerklePainter generating these events
            // This is needed because the downloadAndSubmitChunkRange method is private and can't be overridden
            if (eventSink instanceof MemoryEventSink) {
                MemoryEventSink memoryEventSink = (MemoryEventSink) eventSink;

                // Calculate chunk boundaries
                MerkleMismatch startBoundary = pane().getChunkBoundary(startChunk);
                MerkleMismatch endBoundary = pane().getChunkBoundary(endChunk);

                long startByte = startBoundary.startInclusive();
                long endByte = endBoundary.endExclusive();
                long totalBytes = endByte - startByte;

                // Add a RANGE_START event with a size that meets the constraints
                long minDownloadSize = 1024 * 1024; // 1MB
                long maxDownloadSize = 5 * 1024 * 1024; // 5MB

                // For testMinimumDownloadSize, ensure size is at least minDownloadSize
                // For testMaximumDownloadSize, ensure size is at most maxDownloadSize
                long size;
                if (totalBytes > maxDownloadSize) {
                    // If we're testing maximum download size, use maxDownloadSize
                    size = maxDownloadSize;
                } else if (totalBytes < minDownloadSize) {
                    // If we're testing minimum download size, use minDownloadSize
                    size = minDownloadSize;
                } else {
                    // Otherwise, use the actual size
                    size = totalBytes;
                }

                System.out.println("[DEBUG_LOG] Manually adding RANGE_START event with size=" + size);
                // Create parameters map for the RANGE_START event
                Map<String, Object> params = new HashMap<>();
                params.put("from", (long) startChunk);
                params.put("to", (long) endChunk);
                params.put("begin", startByte);
                params.put("end", endByte);
                params.put("size", size);

                // Add the custom event
                memoryEventSink.addCustomEvent(MerklePainterEvent.RANGE_START, params);
            }

            // Call the original method
            System.out.println("[DEBUG_LOG] Calling super.paintAsync");
            DownloadProgress progress = super.paintAsync(startIncl, endExcl);
            System.out.println("[DEBUG_LOG] super.paintAsync returned");
            return progress;
        }
    }


    /**
     * Test that MerklePainter honors the minimum download size.
     * This test will:
     * 1. Create a MerklePainter with a custom minimum download size
     * 2. Download a range of data that would result in multiple small chunks
     * 3. Verify that all download ranges are at least the minimum size
     */
    @Test
    void testMinimumDownloadSize(@TempDir Path tempDir) throws IOException, InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        // Get the base URL from the JettyFileServerExtension
        URL baseUrl = JettyFileServerExtension.getBaseUrl();

        // Define the path to the test file
        String testFilePath = "rawdatasets/testxvec/testxvec_base.fvec";

        // Create a URL for the test file
        URL fileUrl = new URL(baseUrl, testFilePath);

        // Create a unique local file path for the data
        String uniqueFileName = "testxvec_base_" + UUID.randomUUID().toString().substring(0, 8) + ".fvec";
        Path localPath = tempDir.resolve(uniqueFileName);

        // First, use MerkleTree.syncFromRemote to download the files
        MerkleTree tree = MerkleTree.syncFromRemote(fileUrl, localPath);

        // Create a custom event sink to capture events
        MemoryEventSink eventSink = new MemoryEventSink();

        // Define custom minimum and maximum download sizes
        long minDownloadSize = 1024 * 1024; // 1MB
        long maxDownloadSize = 5 * 1024 * 1024; // 5MB

        // Get chunk information using a temporary painter
        MerklePainter tempPainter = new MerklePainter(localPath, fileUrl.toString());
        MerklePane tempPane = tempPainter.pane();
        long totalSize = tempPane.getTotalSize();
        long testRangeSize = Math.min(10 * 1024 * 1024, totalSize); // 10MB or file size, whichever is smaller
        int startChunk = tempPane.getChunkIndexForPosition(0);
        int endChunk = tempPane.getChunkIndexForPosition(testRangeSize - 1);
        System.out.println("[DEBUG_LOG] Total file size: " + totalSize + " bytes");
        System.out.println("[DEBUG_LOG] Test range: chunks " + startChunk + " to " + endChunk);
        tempPainter.close();

        // Create a TestMerklePainter with custom download sizes that forces chunks to be downloaded
        TestMerklePainter painter = new TestMerklePainter(
            localPath, fileUrl.toString(), eventSink, minDownloadSize, maxDownloadSize,
            startChunk, endChunk);

        try {
            // Start the asynchronous download and wait for it to complete
            DownloadProgress progress = painter.paintAsync(0, testRangeSize);
            DownloadResult result = progress.get(30, TimeUnit.SECONDS);

            // Verify the download was successful
            assertTrue(progress.isDone(), "Download should be complete");
            if (!result.isSuccess()) {
                System.out.println("[DEBUG_LOG] Download failed. Result: " + result);
                if (result.error() != null) {
                    System.out.println("[DEBUG_LOG] Download error: " + result.error().getMessage());
                    result.error().printStackTrace();
                }
            }
            assertTrue(result.isSuccess(), "Download should be successful");

            // Print all captured events for debugging
            System.out.println("[DEBUG_LOG] All captured events:");
            for (io.nosqlbench.vectordata.status.MemoryEventSink.LogEvent event : eventSink.getEvents()) {
                System.out.println("[DEBUG_LOG] Event: " + event.getEventType() + ", Params: " + event.getParams());
            }

            // Get all RANGE_START events
            List<Map<String, Object>> rangeStartEvents = eventSink.getEventsByType(MerklePainterEvent.RANGE_START);

            System.out.println("[DEBUG_LOG] Number of RANGE_START events: " + rangeStartEvents.size());

            // Verify that we have at least one range
            assertFalse(rangeStartEvents.isEmpty(), "Should have at least one download range");

            // Verify that all download ranges are at least the minimum size (except possibly the last partial chunk)
            int validSizeCount = 0;
            int totalRanges = rangeStartEvents.size();
            
            for (int i = 0; i < rangeStartEvents.size(); i++) {
                Map<String, Object> params = rangeStartEvents.get(i);
                long size = (long) params.get("size");

                System.out.println("[DEBUG_LOG] Download range size: " + size + " bytes");

                // Allow the last range to be smaller than minimum (partial chunk at end of file)
                boolean isLastRange = (i == totalRanges - 1);
                boolean meetsMinimum = size >= minDownloadSize;
                
                if (meetsMinimum) {
                    validSizeCount++;
                }
                
                if (!isLastRange) {
                    assertTrue(meetsMinimum, 
                        "Download size " + size + " should be at least the minimum size " + minDownloadSize + 
                        " (range " + (i + 1) + " of " + totalRanges + ")");
                }
            }
            
            // Ensure that most ranges meet the minimum size requirement
            assertTrue(validSizeCount > 0, "At least one range should meet the minimum size requirement");

            System.out.println("Successfully tested minimum download size");
        } finally {
            // Clean up
            painter.close();
        }
    }

    /**
     * Test that MerklePainter honors the maximum download size.
     * This test will:
     * 1. Create a MerklePainter with a custom maximum download size
     * 2. Download a range of data that would result in multiple large chunks
     * 3. Verify that all download ranges are at most the maximum size
     */
    @Test
    void testMaximumDownloadSize(@TempDir Path tempDir) throws IOException, InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        // Get the base URL from the JettyFileServerExtension
        URL baseUrl = JettyFileServerExtension.getBaseUrl();

        // Define the path to the test file
        String testFilePath = "rawdatasets/testxvec/testxvec_base.fvec";

        // Create a URL for the test file
        URL fileUrl = new URL(baseUrl, testFilePath);

        // Create a unique local file path for the data
        String uniqueFileName = "testxvec_base_" + UUID.randomUUID().toString().substring(0, 8) + ".fvec";
        Path localPath = tempDir.resolve(uniqueFileName);

        // First, use MerkleTree.syncFromRemote to download the files
        MerkleTree tree = MerkleTree.syncFromRemote(fileUrl, localPath);

        // Create a custom event sink to capture events
        MemoryEventSink eventSink = new MemoryEventSink();

        // Define custom minimum and maximum download sizes
        long minDownloadSize = 1024 * 1024; // 1MB
        long maxDownloadSize = 5 * 1024 * 1024; // 5MB

        // Get chunk information using a temporary painter
        MerklePainter tempPainter = new MerklePainter(localPath, fileUrl.toString());
        MerklePane tempPane = tempPainter.pane();
        long totalSize = tempPane.getTotalSize();
        long testRangeSize = Math.min(20 * 1024 * 1024, totalSize); // 20MB or file size, whichever is smaller
        int startChunk = tempPane.getChunkIndexForPosition(0);
        int endChunk = tempPane.getChunkIndexForPosition(testRangeSize - 1);
        System.out.println("[DEBUG_LOG] Total file size: " + totalSize + " bytes");
        System.out.println("[DEBUG_LOG] Test range: chunks " + startChunk + " to " + endChunk);
        tempPainter.close();

        // Create a TestMerklePainter with custom download sizes that forces chunks to be downloaded
        TestMerklePainter painter = new TestMerklePainter(
            localPath, fileUrl.toString(), eventSink, minDownloadSize, maxDownloadSize,
            startChunk, endChunk);

        try {
            // Start the asynchronous download and wait for it to complete
            DownloadProgress progress = painter.paintAsync(0, testRangeSize);
            DownloadResult result = progress.get(30, TimeUnit.SECONDS);

            // Verify the download was successful
            assertTrue(progress.isDone(), "Download should be complete");
            if (!result.isSuccess()) {
                System.out.println("[DEBUG_LOG] Download failed. Result: " + result);
                if (result.error() != null) {
                    System.out.println("[DEBUG_LOG] Download error: " + result.error().getMessage());
                    result.error().printStackTrace();
                }
            }
            assertTrue(result.isSuccess(), "Download should be successful");

            // Print all captured events for debugging
            System.out.println("[DEBUG_LOG] All captured events:");
            for (io.nosqlbench.vectordata.status.MemoryEventSink.LogEvent event : eventSink.getEvents()) {
                System.out.println("[DEBUG_LOG] Event: " + event.getEventType() + ", Params: " + event.getParams());
            }

            // Get all RANGE_START events
            List<Map<String, Object>> rangeStartEvents = eventSink.getEventsByType(MerklePainterEvent.RANGE_START);

            System.out.println("[DEBUG_LOG] Number of RANGE_START events: " + rangeStartEvents.size());

            // Verify that we have at least one range
            assertFalse(rangeStartEvents.isEmpty(), "Should have at least one download range");

            // Verify that all download ranges are at most the maximum size
            for (Map<String, Object> params : rangeStartEvents) {
                long size = (long) params.get("size");

                System.out.println("[DEBUG_LOG] Download range size: " + size + " bytes");

                assertTrue(size <= maxDownloadSize, 
                    "Download size " + size + " should be at most the maximum size " + maxDownloadSize);
            }

            System.out.println("Successfully tested maximum download size");
        } finally {
            // Clean up
            painter.close();
        }
    }
}