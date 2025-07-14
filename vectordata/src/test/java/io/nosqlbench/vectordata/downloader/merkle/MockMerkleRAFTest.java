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


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Mock tests for the MerkleRAF functionality.
 * 
 * This test class isolates the errors with MerkleRAFTest by using a simple RandomAccessFile
 * instead of the legacy async file channel, focusing on the basic file operations without relying on the
 * complex download mechanism.
 */
public class MockMerkleRAFTest {

    /**
     * Test that a RandomAccessFile can be used for basic file operations.
     * This test simulates the functionality of MerkleRAF without using the actual implementation.
     */
    @Test
    void testMockMerkleRAFWithInternalPainter(@TempDir Path tempDir) throws IOException {
        // Create a unique local file path for the data
        String uniqueFileName = "testxvec_base_" + UUID.randomUUID().toString().substring(0, 8) + ".fvec";
        Path localPath = tempDir.resolve(uniqueFileName);
        System.out.println("[DEBUG_LOG] Local path: " + localPath);

        // Create the file and write some test data to it
        Files.createFile(localPath);
        byte[] testData = new byte[100];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) (i + 10);
        }
        Files.write(localPath, testData);

        // Create a RandomAccessFile instance
        try (RandomAccessFile raf = new RandomAccessFile(localPath.toFile(), "rw")) {
            // Verify that the file exists
            assertTrue(Files.exists(localPath), "Local file should exist");

            // Test reading back what we wrote directly to the file
            raf.seek(0);
            byte[] readBack = new byte[100];
            int bytesRead = raf.read(readBack);
            assertEquals(100, bytesRead, "Should read the requested number of bytes");
            assertArrayEquals(testData, readBack, "Should read back what was written");

            // Print some information about the file
            System.out.println("[DEBUG_LOG] Successfully tested mock MerkleRAF functionality");
            System.out.println("[DEBUG_LOG] File size: " + raf.length() + " bytes");
        }
    }

    /**
     * Test that a RandomAccessFile can be used for reading a range of data.
     * This test simulates the prebuffer functionality of MerkleRAF without using the actual implementation.
     */
    @Test
    void testMockPrebuffer(@TempDir Path tempDir) throws Exception {
        // Create a unique local file path for the data
        String uniqueFileName = "testxvec_base_prebuffer_" + UUID.randomUUID().toString().substring(0, 8) + ".fvec";
        Path localPath = tempDir.resolve(uniqueFileName);
        System.out.println("[DEBUG_LOG] Local path: " + localPath);

        // Create the file and write some test data to it
        Files.createFile(localPath);
        byte[] testData = new byte[1024]; // 1KB
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) (i % 256);
        }
        Files.write(localPath, testData);

        // Create a RandomAccessFile instance
        try (RandomAccessFile raf = new RandomAccessFile(localPath.toFile(), "rw")) {
            // Verify that the file exists
            assertTrue(Files.exists(localPath), "Local file should exist");

            // Define a range to read
            long startPosition = 0;
            long length = 1024; // 1KB

            // Read directly from the file
            raf.seek(startPosition);
            byte[] buffer = new byte[(int)length];
            int bytesRead = raf.read(buffer);

            // Verify that we read the expected number of bytes
            assertEquals(length, bytesRead, "Should read the requested number of bytes");

            // Verify that the buffer contains the expected data
            assertArrayEquals(testData, buffer, "Should read back what was written");

            System.out.println("[DEBUG_LOG] Successfully tested mock prebuffer functionality");
        }
    }
}
