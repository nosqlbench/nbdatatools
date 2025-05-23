package io.nosqlbench.vectordata.downloader.merkle;

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


import io.nosqlbench.vectordata.downloader.testserver.TestWebServerFixture;
import io.nosqlbench.vectordata.merkle.MerklePainter;
import io.nosqlbench.vectordata.merkle.MerklePane;
import io.nosqlbench.vectordata.merkle.MerkleTree;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class MerklePainterAsyncTest {

    /**
     * Test that MerklePainter can asynchronously download and submit chunks.
     * This test will:
     * 1. Create a MerklePainter with a local path and remote URL
     * 2. Call paintAsync to download a range of chunks
     * 3. Verify that the download is in progress
     * 4. Wait for the download to complete
     * 5. Verify that the chunks are intact
     */
    @Test
    void testPaintAsync(@TempDir Path tempDir) throws Exception {
        // Create a unique resource path for this test
        Path uniqueResourceRoot = Paths.get("src/test/resources/testserver");

        // Start a dedicated web server for this test with the unique resource path
        try (TestWebServerFixture server = new TestWebServerFixture(uniqueResourceRoot)) {
            server.start();
            URL baseUrl = server.getBaseUrl();

            // Define the path to the test file
            String testFilePath = "rawdatasets/testxvec/testxvec_base.fvec";

            // Create a URL for the test file
            URL fileUrl = new URL(baseUrl, testFilePath);

            // Create a unique local file path for the data
            String uniqueFileName = "testxvec_base_" + UUID.randomUUID().toString().substring(0, 8) + ".fvec";
            Path localPath = tempDir.resolve(uniqueFileName);

            // Create a MerklePainter instance
            MerklePainter painter = new MerklePainter(localPath, fileUrl.toString());

            try {
                // Verify that the files exist
                assertTrue(Files.exists(localPath), "Local file should exist");
                Path merklePath = painter.merklePath();
                assertTrue(Files.exists(merklePath), "Merkle file should exist");
                Path referenceTreePath = painter.referenceTreePath();
                assertTrue(Files.exists(referenceTreePath), "Reference merkle file should exist");

                // Get the MerklePane from the painter
                MerklePane pane = painter.pane();

                // Define a range to download
                long startPosition = 0;
                long endPosition = 1024; // Download first 1KB

                // Start the asynchronous download
                CompletableFuture<Void> future = painter.paintAsync(startPosition, endPosition);

                // Give the download a moment to start
                Thread.sleep(100);

                // Wait for the download to complete (with timeout)
                future.get(5, TimeUnit.SECONDS);

                // Now the future should be completed
                assertTrue(future.isDone(), "Download should be complete");

                // Verify that the chunks are intact
                MerkleTree merkleTree = pane.getMerkleTree();
                int startChunk = pane.getChunkIndexForPosition(startPosition);
                int endChunk = pane.getChunkIndexForPosition(endPosition - 1);

                for (int i = startChunk; i <= endChunk; i++) {
                    assertTrue(pane.isChunkIntact(i), "Chunk " + i + " should be intact");

                    // Read the chunk and verify it has data
                    ByteBuffer chunk = pane.readChunk(i);
                    assertTrue(chunk.remaining() > 0, "Chunk " + i + " should have data");
                }

                System.out.println("Successfully tested MerklePainter.paintAsync");
            } catch (IOException e) {
                System.out.println("Error during test: " + e.getMessage());
                throw e;
            } finally {
                // Clean up
                painter.close();
            }
        }
    }
//
//    /**
//     * Test that MerklePainter properly deduplicates download tasks.
//     * This test will:
//     * 1. Create a MerklePainter with a local path and remote URL
//     * 2. Start two overlapping downloads
//     * 3. Verify that the overlapping chunks are only downloaded once
//     *
//     * Note: This test is tagged as "integration" since it requires internet access
//     * and downloads real data, which may take longer than a typical unit test.
//     */
//    @Test
//    @Tag("integration")
//    void testTaskDeduplication() throws Exception {
//        // Skip this test for now as it requires internet access
//        assumeTrue(false, "Skipping test that requires internet access");
//        // Define the remote URL for the dataset
//        String remoteUrl = "https://jvector-datasets-shared.s3.us-east-1.amazonaws.com/faed719b5520a075f2281efb8c820834/ANN_SIFT1B/bigann_query.bvecs";
//
//        // Create a local file path for the data
//        Path localPath = tempDir.resolve("bigann_query_dedup.bvecs");
//
//        try {
//            // Create a MerklePainter instance
//            MerklePainter painter = new MerklePainter(localPath, remoteUrl);
//
//            // Define two overlapping ranges
//            long startPosition1 = 0;
//            long endPosition1 = 2048; // 2KB
//            long startPosition2 = 1024;
//            long endPosition2 = 3072; // 3KB
//
//            // Start the first download
//            CompletableFuture<Void> future1 = painter.paintAsync(startPosition1, endPosition1);
//
//            // Give the download a moment to startInclusive
//            Thread.sleep(100);
//
//            // Record the in-progress chunks after the first download starts
//            Set<Integer> inProgressChunks1 = painter.getInProgressChunks();
//
//            // Start the second download
//            CompletableFuture<Void> future2 = painter.paintAsync(startPosition2, endPosition2);
//
//            // Give the download a moment to startInclusive
//            Thread.sleep(100);
//
//            // Record the in-progress chunks after the second download starts
//            Set<Integer> inProgressChunks2 = painter.getInProgressChunks();
//
//            // The second set should not be much larger than the first if deduplication is working
//            // (it should only add chunks that weren't already being downloaded)
//            MerkleTree merkleTree = painter.pane().getMerkleTree();
//            int startChunk1 = merkleTree.getChunkIndexForPosition(startPosition1);
//            int endChunk1 = merkleTree.getChunkIndexForPosition(endPosition1 - 1);
//            int startChunk2 = merkleTree.getChunkIndexForPosition(startPosition2);
//            int endChunk2 = merkleTree.getChunkIndexForPosition(endPosition2 - 1);
//
//            int expectedOverlap = Math.max(0, Math.min(endChunk1, endChunk2) - Math.max(startChunk1, startChunk2) + 1);
//            int expectedTotalChunks = (endChunk1 - startChunk1 + 1) + (endChunk2 - startChunk2 + 1) - expectedOverlap;
//
//            // The total number of in-progress chunks should be approximately equal to the expected total
//            assertTrue(Math.abs(inProgressChunks2.size() - expectedTotalChunks) <= 1,
//                "Number of in-progress chunks should match expected total");
//
//            // Wait for both downloads to complete
//            CompletableFuture.allOf(future1, future2).get(60, TimeUnit.SECONDS);
//
//            // Verify that all chunks in both ranges are intact
//            for (int i = startChunk1; i <= endChunk2; i++) {
//                assertTrue(painter.pane().isChunkIntact(i), "Chunk " + i + " should be intact");
//            }
//
//            // Clean up
//            painter.close();
//
//            System.out.println("Successfully tested MerklePainter task deduplication");
//        } catch (java.io.FileNotFoundException e) {
//            // This might happen if the remote file doesn't exist
//            System.out.println("Remote file not found: " + e.getMessage());
//            // Skip the test rather than fail it
//            assumeTrue(false, "Remote file not available");
//        } catch (java.io.IOException e) {
//            System.out.println("Error during test: " + e.getMessage());
//            throw e;
//        }
//    }
}
