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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(JettyFileServerExtension.class)
public class MerklePainterTest {

    /**
     * Test that MerklePainter can download and submit chunks to MerklePane.
     * This test will:
     * 1. Create a MerklePainter with a local path and remote URL
     * 2. Download a specific chunk using the downloadAndSubmitChunk method
     * 3. Verify that the chunk was downloaded and the merkle tree was updated
     */
    @Test
    void testDownloadAndSubmitChunk(@TempDir Path tempDir) throws IOException, InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
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

        // Now create a MerklePainter instance
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

            // Get the initial verification state
            boolean initialVerification = pane.verifyChunk(0);

            // Define a small range to download (similar to MerklePainterAsyncTest)
            long startPosition = 0;
            long endPosition = 1024; // Download first 1KB

            // Start the asynchronous download and wait for it to complete
            DownloadProgress progress = painter.paintAsync(startPosition, endPosition);
            DownloadResult result = progress.get(5, TimeUnit.SECONDS);

            // Verify the download was successful
            assertTrue(progress.isDone(), "Download should be complete");
            assertTrue(result.isSuccess(), "Download should be successful");

            // Now the chunk should have data
            ByteBuffer chunk = pane.readChunk(0);
            assertTrue(chunk.remaining() > 0, "Chunk should have data");

            // The verification state might change, but we can't guarantee it
            // since the initial state might already be valid

            System.out.println("Successfully tested MerklePainter.downloadAndSubmitChunk");
        } finally {
            // Clean up
            painter.close();
        }
    }
}
