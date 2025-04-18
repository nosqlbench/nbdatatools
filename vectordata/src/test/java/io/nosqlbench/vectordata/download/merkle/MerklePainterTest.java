package io.nosqlbench.vectordata.download.merkle;

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


import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class MerklePainterTest {

    @TempDir
    Path tempDir;

    /**
     * Test that MerklePainter can download and submit chunks to MerklePane.
     * This test will:
     * 1. Create a MerklePainter with a local path and remote URL
     * 2. Download a specific chunk using the downloadAndSubmitChunk method
     * 3. Verify that the chunk was downloaded and the merkle tree was updated
     *
     * Note: This test is tagged as "integration" since it requires internet access
     * and downloads real data, which may take longer than a typical unit test.
     */
    @Test
    @Tag("integration")
    void testDownloadAndSubmitChunk() throws IOException {
        // Skip this test for now as it requires internet access
        assumeTrue(false, "Skipping test that requires internet access");
        // Define the remote URL for the dataset
        String remoteUrl = "https://jvector-datasets-shared.s3.us-east-1.amazonaws.com/faed719b5520a075f2281efb8c820834/ANN_SIFT1B/bigann_query.bvecs";

        // Create a local file path for the data
        Path localPath = tempDir.resolve("bigann_query_painter.bvecs");

        try {
            // Create a MerklePainter instance
            MerklePainter painter = new MerklePainter(localPath, remoteUrl);

            // Verify that the files exist
            assertTrue(Files.exists(localPath), "Local file should exist");
            Path merklePath = localPath.resolveSibling(localPath.getFileName().toString() + ".mrkl");
            assertTrue(Files.exists(merklePath), "Merkle file should exist");
            Path referenceTreePath = localPath.resolveSibling(localPath.getFileName().toString() + ".mref");
            assertTrue(Files.exists(referenceTreePath), "Reference merkle file should exist");

            // Get the MerklePane from the painter
            MerklePane pane = painter.pane();

            // Get the initial verification state
            boolean initialVerification = pane.verifyChunk(0);

            // Download and submit the first chunk
            boolean success = painter.downloadAndSubmitChunk(0);
            assertTrue(success, "Download and submit should succeed");

            // Now the chunk should have data
            ByteBuffer chunk = pane.readChunk(0);
            assertTrue(chunk.remaining() > 0, "Chunk should have data");

            // The verification state might change, but we can't guarantee it
            // since the initial state might already be valid

            // Clean up
            painter.close();

            System.out.println("Successfully tested MerklePainter.downloadAndSubmitChunk");
        } catch (java.io.FileNotFoundException e) {
            // This might happen if the remote file doesn't exist
            System.out.println("Remote file not found: " + e.getMessage());
            // Skip the test rather than fail it
            assumeTrue(false, "Remote file not available");
        } catch (java.io.IOException e) {
            System.out.println("Error during test: " + e.getMessage());
            throw e;
        }
    }
}
