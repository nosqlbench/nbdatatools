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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the MerkleTree.sync method.
 */
@ExtendWith(JettyFileServerExtension.class)
public class MerkleTreeSyncTest {

    @TempDir
    Path tempDir;

    // Test file paths relative to the test web server root
    private static final String TEST_FILE_PATH = "rawdatasets/testxvec/testxvec_base.fvec";
    private static final String TEST_MERKLE_PATH = "rawdatasets/testxvec/testxvec_base.fvec.mrkl";

    /**
     * Test that the sync method correctly downloads a file when it doesn't exist locally.
     */
    @Test
    void testSyncWhenFileDoesNotExist() throws Exception {
        // Get URLs from the test web server
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        URL fileUrl = new URL(baseUrl, TEST_FILE_PATH);

        // Call the method under test
        Path localPath = tempDir.resolve("test.dat");
        MerkleTree tree = MerkleTree.syncFromRemote(fileUrl, localPath);

        // Verify the result
        assertNotNull(tree);
        assertTrue(Files.exists(localPath));
        assertTrue(Files.exists(tempDir.resolve("test.dat.mrkl")));

        // Verify file content by comparing file sizes
        // We don't compare the entire content because the files might be large
        assertEquals(Files.size(localPath), 10100000); // Size of testxvec_base.fvec
    }

    /**
     * Test that the sync method correctly skips download when the local file is identical.
     */
    @Test
    void testSyncWhenFileIsIdentical() throws Exception {
        // Get URLs from the test web server
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        URL fileUrl = new URL(baseUrl, TEST_FILE_PATH);
        URL merkleUrl = new URL(baseUrl, TEST_MERKLE_PATH);

        // Create local files by downloading them first
        Path localPath = tempDir.resolve("test.dat");
        Path localMerklePath = tempDir.resolve("test.dat.mrkl");

        // Download the files directly
        try (var is = fileUrl.openStream()) {
            Files.copy(is, localPath, StandardCopyOption.REPLACE_EXISTING);
        }

        try (var is = merkleUrl.openStream()) {
            Files.copy(is, localMerklePath, StandardCopyOption.REPLACE_EXISTING);
        }

        // Record file modification times before sync
        long fileModTime = Files.getLastModifiedTime(localPath).toMillis();
        long merkleModTime = Files.getLastModifiedTime(localMerklePath).toMillis();

        // Wait a moment to ensure modification times would be different if files were updated
        Thread.sleep(100);

        // Call the method under test
        MerkleTree tree = MerkleTree.syncFromRemote(fileUrl, localPath);

        // Verify the result
        assertNotNull(tree);
        assertEquals(fileModTime, Files.getLastModifiedTime(localPath).toMillis());
        assertEquals(merkleModTime, Files.getLastModifiedTime(localMerklePath).toMillis());
    }

    /**
     * Test that the sync method correctly downloads when the local file is different.
     */
    @Test
    void testSyncWhenFileIsDifferent() throws Exception {
        // Get URLs from the test web server
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        URL fileUrl = new URL(baseUrl, TEST_FILE_PATH);
        URL merkleUrl = new URL(baseUrl, TEST_MERKLE_PATH);

        // Create local files with modified content
        Path localPath = tempDir.resolve("test.dat");
        Path localMerklePath = tempDir.resolve("test.dat.mrkl");

        // Create a small file with different content
        Files.write(localPath, new byte[1000]);
        Files.write(localMerklePath, new byte[100]);

        // Call the method under test
        MerkleTree tree = MerkleTree.syncFromRemote(fileUrl, localPath);

        // Verify the result
        assertNotNull(tree);

        // Verify file sizes match the expected sizes
        assertEquals(Files.size(localPath), 10100000); // Size of testxvec_base.fvec
        assertEquals(Files.size(localMerklePath), 1017); // Size of testxvec_base.fvec.mrkl
    }
}