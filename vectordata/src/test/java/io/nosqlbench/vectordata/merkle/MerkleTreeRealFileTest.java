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

import io.nosqlbench.vectordata.downloader.testserver.TestWebServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for loading a merkle file. */
public class MerkleTreeRealFileTest {

    @TempDir
    Path tempDir;

    @Test
    void testLoadMerkleFile() throws Exception {
        // Get the base URL from the TestWebServerExtension
        URL baseUrl = TestWebServerExtension.getBaseUrl();

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

        // Load and verify the Merkle tree
        MerkleTree tree = MerkleTree.load(localPath);
        assertNotNull(tree, "Loaded MerkleTree should not be null");
        System.out.println("MerkleTree.load succeeded for file: " + localPath);
    }
}
