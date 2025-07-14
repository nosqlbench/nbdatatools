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

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the MerklePaneSetup class.
 */
@ExtendWith(JettyFileServerExtension.class)
public class MerklePaneSetupTest {

    // Create test files in the test web server's temp directory
    private void createTestFile(String filename, byte[] content) throws IOException {
        Path tempDir = JettyFileServerExtension.TEMP_RESOURCES_ROOT;
        Path testFile = tempDir.resolve(filename);
        Files.createDirectories(testFile.getParent());
        Files.write(testFile, content);

        // Create a merkle file for the test file
        MerkleTree tree = MerkleTree.fromFile(testFile, 1024, new MerkleRange(0, content.length));
        Path merklePath = testFile.resolveSibling(testFile.getFileName().toString() + ".mrkl");
        tree.save(merklePath);
    }

    /**
     * Test initializing a MerkleTree with a remote HTTP URL.
     * This test will:
     * 1. Create a temporary content file
     * 2. Initialize a MerkleTree using MerklePaneSetup.initTree with an HTTP URL
     * 3. Verify that the MerkleTree is created correctly
     */
    @Test
    void testInitTreeWithHttpUrl(@TempDir Path tempDir) throws IOException {
        // Create a test content file with known content
        Path contentFile = tempDir.resolve("test_content.dat");
        byte[] testData = new byte[4096];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) (i % 256);
        }
        Files.write(contentFile, testData);

        // Define the merkle file path
        Path merklePath = tempDir.resolve("test_content.dat.mrkl");

        // Create the test file in the test web server's temp directory
        String filename = "test_content.dat";
        createTestFile(filename, testData);

        // Use the test web server URL for remote content
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        String remoteUrl = baseUrl.toString() + "temp/" + filename;

        // Initialize the tree with a remote HTTP URL
        MerkleTree tree = MerklePaneSetup.initTree(contentFile, merklePath, remoteUrl);

        // Verify that the tree was created
        assertNotNull(tree, "Tree should not be null");

        // Verify that the merkle file was created
        assertTrue(Files.exists(merklePath), "Merkle file should exist");

        // Verify that the reference merkle file was created
        Path mrefPath = contentFile.resolveSibling(contentFile.getFileName().toString() + ".mref");
        assertTrue(Files.exists(mrefPath), "Reference merkle file should exist");

        // Verify that the tree has the correct properties
        assertEquals(Files.size(contentFile), tree.totalSize(), "Tree total size should match content file size");
        assertTrue(tree.getChunkSize() > 0, "Tree chunk size should be positive");
    }

    /**
     * Test that MerklePaneSetup rebuilds the merkle tree when the content file is newer.
     * This test will:
     * 1. Create a content file and initialize a merkle tree
     * 2. Modify the content file and update its timestamp to be newer
     * 3. Initialize the tree again
     * 4. Verify that the merkle tree was rebuilt
     */
    @Test
    void testRebuildMerkleWhenContentNewer(@TempDir Path tempDir) throws IOException, InterruptedException {
        // Create a test content file
        Path contentFile = tempDir.resolve("content_newer.dat");
        byte[] initialData = new byte[4096];
        Files.write(contentFile, initialData);

        // Define the merkle file path
        Path merklePath = tempDir.resolve("content_newer.dat.mrkl");

        // Create the test file in the test web server's temp directory
        String filename = "content_newer.dat";
        createTestFile(filename, initialData);

        // Use the test web server URL for remote content
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        String remoteUrl = baseUrl.toString() + "temp/" + filename;

        // Initialize the tree
        MerkleTree initialTree = MerklePaneSetup.initTree(contentFile, merklePath, remoteUrl);
        assertNotNull(initialTree, "Initial tree should not be null");

        // Get the initial merkle file timestamp
        FileTime initialMerkleTime = Files.getLastModifiedTime(merklePath);

        // Wait a moment to ensure timestamps will be different
        Thread.sleep(1000);

        // Modify the content file with different data
        byte[] modifiedData = new byte[4096];
        for (int i = 0; i < modifiedData.length; i++) {
            modifiedData[i] = (byte) (i % 128);
        }
        Files.write(contentFile, modifiedData);

        // Set the content file's timestamp to be newer than the merkle file
        Files.setLastModifiedTime(contentFile, FileTime.from(Instant.now()));

        // Initialize the tree again
        MerkleTree rebuiltTree = MerklePaneSetup.initTree(contentFile, merklePath, remoteUrl);
        assertNotNull(rebuiltTree, "Rebuilt tree should not be null");

        // Verify that the merkle file was updated (timestamp should be newer)
        FileTime rebuiltMerkleTime = Files.getLastModifiedTime(merklePath);
        assertTrue(rebuiltMerkleTime.compareTo(initialMerkleTime) > 0, 
                "Rebuilt merkle file should have a newer timestamp");
    }

    /**
     * Test that MerklePaneSetup creates necessary files if they don't exist.
     * This test will:
     * 1. Define paths for non-existent files
     * 2. Initialize a tree with a remote HTTP URL
     * 3. Verify that all necessary files were created
     */
    @Test
    void testCreateNecessaryFiles(@TempDir Path tempDir) throws IOException {
        // Define paths for files that don't exist yet
        Path contentFile = tempDir.resolve("nonexistent_content.dat");
        Path merklePath = tempDir.resolve("nonexistent_content.dat.mrkl");

        // Create the test file in the test web server's temp directory
        String filename = "nonexistent_content.dat";
        byte[] testData = new byte[4096];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) (i % 256);
        }
        createTestFile(filename, testData);

        // Use the test web server URL for remote content
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        String remoteUrl = baseUrl.toString() + "temp/" + filename;

        // Initialize the tree
        MerkleTree tree = MerklePaneSetup.initTree(contentFile, merklePath, remoteUrl);
        assertNotNull(tree, "Tree should not be null");

        // Verify that all necessary files were created
        assertTrue(Files.exists(contentFile), "Content file should have been created");
        assertTrue(Files.exists(merklePath), "Merkle file should have been created");

        Path mrefPath = contentFile.resolveSibling(contentFile.getFileName().toString() + ".mref");
        assertTrue(Files.exists(mrefPath), "Reference merkle file should have been created");
    }

    /**
     * Test that MerklePaneSetup validates input arguments correctly.
     * This test will:
     * 1. Call initTree with null arguments
     * 2. Verify that appropriate exceptions are thrown
     */
    @Test
    void testValidateArguments(@TempDir Path tempDir) {
        // Test with null content path
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            MerklePaneSetup.initTree(null, tempDir.resolve("merkle.mrkl"), null);
        });
        assertTrue(exception.getMessage().contains("Content path and Merkle path must be non-null"), 
                "Exception message should mention null content path");

        // Test with null merkle path
        exception = assertThrows(IllegalArgumentException.class, () -> {
            MerklePaneSetup.initTree(tempDir.resolve("content.dat"), null, null);
        });
        assertTrue(exception.getMessage().contains("Content path and Merkle path must be non-null"), 
                "Exception message should mention null merkle path");
    }
}