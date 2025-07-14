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


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the AsyncRandomAccessFile class.
 */
public class AsyncRandomAccessFileTest {

    /**
     * Test that AsyncRandomAccessFile can read from a local file correctly.
     * This test will:
     * 1. Create a temporary file with known content
     * 2. Create an AsyncRandomAccessFile instance for the file
     * 3. Verify that reading from the file returns the expected content
     */
    @Test
    void testReadFromLocalFile(@TempDir Path tempDir) throws IOException {
        // Create a test file with known content
        Path testFile = tempDir.resolve("test_file.dat");
        byte[] testData = new byte[1024];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) (i % 256);
        }
        Files.write(testFile, testData);

        // Create an AsyncRandomAccessFile instance
        try (AsyncRandomAccessFile raf = new AsyncRandomAccessFile(testFile)) {
            // Verify the file length
            assertEquals(testData.length, raf.length(), "File length should match the written data length");

            // Read the entire file
            raf.seek(0);
            byte[] readData = new byte[testData.length];
            int bytesRead = raf.read(readData);

            // Verify that we read the expected number of bytes
            assertEquals(testData.length, bytesRead, "Should read the expected number of bytes");

            // Verify that the read data matches the written data
            assertArrayEquals(testData, readData, "Read data should match written data");

            // Read a portion of the file
            raf.seek(100);
            byte[] partialData = new byte[100];
            bytesRead = raf.read(partialData);

            // Verify that we read the expected number of bytes
            assertEquals(100, bytesRead, "Should read the requested number of bytes");

            // Verify that the read data matches the expected portion of the written data
            byte[] expectedPartialData = Arrays.copyOfRange(testData, 100, 200);
            assertArrayEquals(expectedPartialData, partialData, "Partial read data should match expected portion");
        }
    }

    /**
     * Test that the prebuffer method returns a completed future.
     * This test will:
     * 1. Create a temporary file
     * 2. Create an AsyncRandomAccessFile instance for the file
     * 3. Call prebuffer and verify that it returns a completed future
     */
    @Test
    void testPrebufferReturnsCompletedFuture(@TempDir Path tempDir) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        // Create a test file
        Path testFile = tempDir.resolve("test_file.dat");
        byte[] testData = new byte[1024];
        Files.write(testFile, testData);

        // Create an AsyncRandomAccessFile instance
        try (AsyncRandomAccessFile raf = new AsyncRandomAccessFile(testFile)) {
            // Call prebuffer
            CompletableFuture<Void> future = raf.prebuffer(0, 100);

            // Verify that the future is already completed
            assertTrue(future.isDone(), "Future should be completed immediately");

            // Verify that the future completes without exception
            future.get(1, TimeUnit.SECONDS); // This should not throw an exception
        }
    }

    /**
     * Test behavior when reading beyond the end of the file.
     * This test will:
     * 1. Create a small temporary file
     * 2. Create an AsyncRandomAccessFile instance for the file
     * 3. Attempt to read beyond the end of the file
     * 4. Verify that the read returns the expected number of bytes
     */
    @Test
    void testReadBeyondEndOfFile(@TempDir Path tempDir) throws IOException {
        // Create a small test file
        Path testFile = tempDir.resolve("small_file.dat");
        byte[] testData = new byte[10];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) i;
        }
        Files.write(testFile, testData);

        // Create an AsyncRandomAccessFile instance
        try (AsyncRandomAccessFile raf = new AsyncRandomAccessFile(testFile)) {
            // Verify the file length
            assertEquals(10, raf.length(), "File length should be 10 bytes");

            // Try to read more bytes than are in the file
            raf.seek(0);
            byte[] readData = new byte[20];
            int bytesRead = raf.read(readData);

            // Verify that we only read the available bytes
            assertEquals(10, bytesRead, "Should only read the available bytes");

            // Verify that the read data matches the written data
            for (int i = 0; i < 10; i++) {
                assertEquals(testData[i], readData[i], "Byte at position " + i + " should match");
            }

            // Try to read from beyond the end of the file
            raf.seek(10);
            bytesRead = raf.read(readData);

            // Verify that read returns -1 when at end of file
            assertEquals(-1, bytesRead, "Read should return -1 when at end of file");
        }
    }
}