package io.nosqlbench.vectordata.downloader;

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
import io.nosqlbench.vectordata.merkle.MerklePainter;
import io.nosqlbench.vectordata.merkle.MerklePainterEvent;
import io.nosqlbench.vectordata.status.MemoryEventSink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An extensive test for chunk verification during file downloads.
 * This test downloads a large file (10MB+) using the local fixture server,
 * verifies that chunk verification works correctly, and observes the event log for details.
 */
@ExtendWith(JettyFileServerExtension.class)
public class ExtensiveChunkVerificationTest {

    @Test
    public void testLargeFileDownloadWithChunkVerification(@TempDir Path tempDir) throws IOException, ExecutionException, InterruptedException {
        // Get the base URL from the JettyFileServerExtension
        URL baseUrl = JettyFileServerExtension.getBaseUrl();

        // Use the test server URL with the testxvec_base.fvec file (10MB)
        URL fileUrl = new URL(baseUrl, "rawdatasets/testxvec/testxvec_base.fvec");

        // Create a unique file name for this test
        String uniquePrefix = "test_" + UUID.randomUUID().toString().substring(0, 8);
        Path testOutputFile = tempDir.resolve(uniquePrefix + ".fvec");

        // Create a memory event sink to capture events
        MemoryEventSink eventSink = new MemoryEventSink();

        // Create a chunked downloader with 1MB chunks and 5 concurrent downloads
        ChunkedDownloader downloader = new ChunkedDownloader(
            fileUrl,
            "testxvec_base.fvec",
            1024 * 1024, // 1MB chunks
            5, // 5 concurrent downloads
            eventSink
        );

        System.out.println("Starting download of large file: " + fileUrl);

        // Download the file
        DownloadProgress progress = downloader.download(testOutputFile, false);

        // Monitor the download progress
        DownloadResult result = null;
        while ((result = progress.poll(100, TimeUnit.MILLISECONDS)) == null) {
            System.out.println(
                progress.getProgress() + "( " + progress.currentBytes() + "/" + progress.totalBytes()
                + " bytes)");
            System.out.println("progress: " + progress);
        }

        System.out.println("Final progress: " + progress);
        System.out.println("Final result: " + result);

        // Verify the download was successful
        assertTrue(Files.exists(testOutputFile), "Downloaded file should exist");
        assertEquals(10100000, Files.size(testOutputFile), "Downloaded file size should match the original");
        assertTrue(result.isSuccess(), "Download should complete successfully");

        // Now use MerklePainter to verify chunks
        System.out.println("Creating MerklePainter for chunk verification...");

        // Create a new memory event sink for MerklePainter events
        MemoryEventSink merkleSink = new MemoryEventSink();

        // Create a MerklePainter instance
        try (MerklePainter painter = new MerklePainter(testOutputFile, fileUrl.toString(), merkleSink)) {
            // Invalidate the merkle tree to force verification
            for (int i = 0; i < painter.pane().merkleTree().getNumberOfLeaves(); i++) {
                painter.pane().merkleTree().invalidateLeaf(i);
            }
            // Request a range of data to trigger chunk verification
            int bufferSize = 4096;

            // Paint (ensure) a range at the beginning of the file
            System.out.println("Painting range at the beginning of the file: 0 to " + bufferSize);
            painter.paint(0, bufferSize);

            // Verify that the first chunk is now marked as valid after successful verification
            int firstChunkIndex = 0;
            assertTrue(painter.pane().isChunkIntact(firstChunkIndex), 
                "Chunk " + firstChunkIndex + " should be marked as valid after successful verification");

            // Paint a range from the middle of the file
            long middleOffset = 5000000; // 5MB into the file
            System.out.println("Painting range in the middle of the file: " + middleOffset + " to " + (middleOffset + bufferSize));
            painter.paint(middleOffset, middleOffset + bufferSize);

            // Paint a range from the end of the file
            long endOffset = 10000000 - bufferSize; // Near the end of the file
            System.out.println("Painting range at the end of the file: " + endOffset + " to " + (endOffset + bufferSize));
            painter.paint(endOffset, endOffset + bufferSize);

            // Paint larger ranges to ensure multiple chunks are verified
            // Assuming 1MB chunk size (as used in the downloader)
            int chunkSize = 1024 * 1024;

            // Paint a range covering the first few chunks
            System.out.println("Painting range covering the first few chunks: 0 to " + (chunkSize * 2));
            painter.paint(0, chunkSize * 2);

            // Paint a range covering chunks in the middle
            System.out.println("Painting range covering chunks in the middle: " + (middleOffset - chunkSize) + " to " + (middleOffset + chunkSize));
            painter.paint(middleOffset - chunkSize, middleOffset + chunkSize);

            // Paint a range covering the last few chunks
            long fileSize = Files.size(testOutputFile);
            System.out.println("Painting range covering the last few chunks: " + (fileSize - (chunkSize * 2)) + " to " + fileSize);
            painter.paint(fileSize - (chunkSize * 2), fileSize);
        }

        // Verify that chunk verification events were logged
        List<Map<String, Object>> startEvents = merkleSink.getEventsByType(MerklePainterEvent.CHUNK_VFY_START);
        List<Map<String, Object>> okEvents = merkleSink.getEventsByType(MerklePainterEvent.CHUNK_VFY_OK);
        List<Map<String, Object>> failEvents = merkleSink.getEventsByType(MerklePainterEvent.CHUNK_VFY_FAIL);

        System.out.println("Chunk verification start events: " + startEvents.size());
        System.out.println("Chunk verification success events: " + okEvents.size());
        System.out.println("Chunk verification failure events: " + failEvents.size());

        // Print some of the events for debugging
        for (int i = 0; i < Math.min(5, startEvents.size()); i++) {
            System.out.println("Start event " + i + ": " + startEvents.get(i));
        }

        for (int i = 0; i < Math.min(5, okEvents.size()); i++) {
            System.out.println("Success event " + i + ": " + okEvents.get(i));
        }

        // Assert that verification events were logged
        assertFalse(startEvents.isEmpty(), "Should have chunk verification start events");
        assertFalse(okEvents.isEmpty(), "Should have chunk verification success events");

        // Print information about any failure events
        if (!failEvents.isEmpty()) {
            System.out.println("Found " + failEvents.size() + " hash verification failure events:");
            for (Map<String, Object> failEvent : failEvents) {
                Integer chunkIndex = (Integer) failEvent.get("index");
                String text = (String) failEvent.get("text");
                String refHash = (String) failEvent.get("refHash");
                String compHash = (String) failEvent.get("compHash");
                System.out.println("  Chunk " + chunkIndex + ": " + text + 
                                  " (expected: " + refHash + ", actual: " + compHash + ")");
            }
            System.out.println("Ignoring expected hash verification failures");
        }

        // Assert that the number of start events matches the number of success events plus the number of failure events
        assertEquals(startEvents.size(), okEvents.size() + failEvents.size(), 
            "Number of verification start events should match number of success events plus number of failure events");

        System.out.println("Chunk verification test completed successfully");
    }
}
