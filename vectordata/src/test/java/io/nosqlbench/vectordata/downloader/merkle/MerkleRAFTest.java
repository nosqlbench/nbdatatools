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
import io.nosqlbench.vectordata.merkle.MerkleBRAF;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MerkleRAFTest {

    /**
     * Test that MerkleRAF can create its own MerklePainter instance.
     * This test will:
     * 1. Use the new constructor that takes a localPath and remoteUrl
     * 2. Verify that MerkleRAF creates and manages its own MerklePainter
     * 3. Test that reading and writing work correctly
     */
    @Test
    void testMerkleRAFWithInternalPainter(@TempDir Path tempDir) throws IOException {
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

            // Create a MerkleRAF instance with its own internal MerklePainter
            try (MerkleBRAF merkleRAF = new MerkleBRAF(localPath, fileUrl.toString())) {
                // Verify that the file exists
                assertTrue(Files.exists(localPath), "Local file should exist");

                // Verify that the merkle file exists
                Path merklePath = localPath.resolveSibling(localPath.getFileName().toString() + ".mrkl");
                assertTrue(Files.exists(merklePath), "Merkle file should exist");

                // Verify that the reference merkle file exists
                Path referenceTreePath = localPath.resolveSibling(localPath.getFileName().toString() + ".mref");
                assertTrue(Files.exists(referenceTreePath), "Reference merkle file should exist");

                // Test writing and reading back
                merkleRAF.seek(0);
                byte[] writeData = new byte[100];
                for (int i = 0; i < writeData.length; i++) {
                    writeData[i] = (byte) (i + 10);
                }
                merkleRAF.write(writeData);

                // Read back what we wrote
                merkleRAF.seek(0);
                byte[] readBack = new byte[100];
                int bytesRead = merkleRAF.read(readBack);
                assertEquals(100, bytesRead, "Should read the requested number of bytes");
                assertArrayEquals(writeData, readBack, "Should read back what was written");

                // Print some information about the file
                System.out.println("Successfully tested MerkleRAF with internal MerklePainter");
                System.out.println("File size: " + merkleRAF.length() + " bytes");
            }
        }
    }

    /**
     * Test that MerkleRAF can prebuffer data asynchronously.
     * This test will:
     * 1. Create a MerkleRAF instance
     * 2. Call prebuffer to asynchronously download a range of data
     * 3. Verify that the prebuffer operation completes successfully
     * 4. Verify that the data can be read without additional downloads
     */
    @Test
    void testPrebuffer(@TempDir Path tempDir) throws Exception {
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
            String uniqueFileName = "testxvec_base_prebuffer_" + UUID.randomUUID().toString().substring(0, 8) + ".fvec";
            Path localPath = tempDir.resolve(uniqueFileName);

            // Create a MerkleRAF instance with its own internal MerklePainter
            try (MerkleBRAF merkleRAF = new MerkleBRAF(localPath, fileUrl.toString())) {
                // Verify that the file exists
                assertTrue(Files.exists(localPath), "Local file should exist");

                // Define a range to prebuffer
                long startPosition = 0;
                long length = 1024; // 1KB

                // Call prebuffer to asynchronously download the range
                CompletableFuture<Void> future = merkleRAF.prebuffer(startPosition, length);

                // Wait for the prebuffer operation to complete
                future.get(5, TimeUnit.SECONDS);

                // Now read from the prebuffered range - this should not trigger any downloads
                merkleRAF.seek(startPosition);
                byte[] buffer = new byte[(int)length];
                int bytesRead = merkleRAF.read(buffer);

                // Verify that we read the expected number of bytes
                assertEquals(length, bytesRead, "Should read the requested number of bytes");

                // Verify that the buffer contains data (not all zeros)
                boolean hasNonZeroData = false;
                for (byte b : buffer) {
                    if (b != 0) {
                        hasNonZeroData = true;
                        break;
                    }
                }
                assertTrue(hasNonZeroData, "Buffer should contain non-zero data");

                System.out.println("Successfully tested MerkleRAF.prebuffer");
            }
        }
    }
}
