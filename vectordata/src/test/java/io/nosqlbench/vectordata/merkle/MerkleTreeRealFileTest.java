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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for loading a merkle file. */
@ExtendWith(JettyFileServerExtension.class)
public class MerkleTreeRealFileTest {

    @TempDir
    Path tempDir;

    @Tag("performance")
    @Test
    void testLoadMerkleFile() throws Exception {
        // Get the base URL from the JettyFileServerExtension
        URL baseUrl = JettyFileServerExtension.getBaseUrl();

        // Path to a merkle file in the test resources
        String merklePath = "rawdatasets/testxvec/testxvec_base.fvec.mrkl";

        // Create a URL for the merkle file
        URL merkleUrl = new URL(baseUrl, merklePath);

        // Create a unique local file path for the data
        String uniqueFileName = "testxvec_base_" + UUID.randomUUID().toString().substring(0, 8) + ".fvec.mrkl";
        Path localPath = tempDir.resolve(uniqueFileName);

        // Use HttpURLConnection to check status code
        java.net.HttpURLConnection connection = (java.net.HttpURLConnection) merkleUrl.openConnection();
        try {
            // Check the HTTP status code
            int statusCode = connection.getResponseCode();
            assertEquals(200, statusCode, "HTTP status code should be 200");

            // Download the file
            try (var in = connection.getInputStream()) {
                Files.copy(in, localPath);
            }
        } finally {
            connection.disconnect();
        }

        // Verify the file was downloaded
        assertTrue(Files.exists(localPath), "Merkle file should exist");

        // Measure the performance of loading the merkle tree
        System.out.println("[DEBUG_LOG] Starting merkle tree load performance test");
        System.out.println("[DEBUG_LOG] File size: " + Files.size(localPath) + " bytes");

        // Warm up the JVM
        System.out.println("[DEBUG_LOG] Warming up JVM...");
        for (int i = 0; i < 3; i++) {
            MerkleTree warmupTree = MerkleTree.load(localPath, false);
            warmupTree.close();
        }

        // Measure load time
        // Note: The verify parameter is kept for backward compatibility but is ignored in production code
        System.out.println("[DEBUG_LOG] Measuring load time...");
        long startTime = System.nanoTime();
        MerkleTree tree = MerkleTree.load(localPath, false);
        long endTime = System.nanoTime();
        long durationNanos = endTime - startTime;
        double durationMs = durationNanos / 1_000_000.0;

        // Print performance results
        System.out.println("[DEBUG_LOG] Load time: " + durationMs + " ms");

        // Verify the tree is not null
        assertNotNull(tree, "Loaded MerkleTree should not be null");

        // Clean up
        tree.close();

        System.out.println("MerkleTree.load succeeded for file: " + localPath);
    }
}
