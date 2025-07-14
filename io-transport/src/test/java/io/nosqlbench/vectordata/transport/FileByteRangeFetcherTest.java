package io.nosqlbench.vectordata.transport;

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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/// Unit tests for FileByteRangeFetcher.
public class FileByteRangeFetcherTest {

    @TempDir
    Path tempDir;

    @Test
    void testBasicRangeRead() throws Exception {
        // Create test file with known content
        byte[] testData = new byte[1000];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) (i % 256);
        }
        Path testFile = tempDir.resolve("test.dat");
        Files.write(testFile, testData);
        
        try (FileByteRangeFetcher fetcher = new FileByteRangeFetcher(testFile)) {
            // Test reading from the beginning
            CompletableFuture<ByteBuffer> future = fetcher.fetchRange(0, 10);
            ByteBuffer result = future.get();
            
            assertNotNull(result);
            assertEquals(10, result.remaining());
            
            byte[] resultBytes = new byte[result.remaining()];
            result.get(resultBytes);
            
            for (int i = 0; i < 10; i++) {
                assertEquals((byte) i, resultBytes[i]);
            }
        }
    }

    @Test
    void testRangeReadFromOffset() throws Exception {
        // Create test file with known content
        byte[] testData = new byte[1000];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) (i % 256);
        }
        Path testFile = tempDir.resolve("test.dat");
        Files.write(testFile, testData);
        
        try (FileByteRangeFetcher fetcher = new FileByteRangeFetcher(testFile)) {
            // Test reading from offset 100
            CompletableFuture<ByteBuffer> future = fetcher.fetchRange(100, 50);
            ByteBuffer result = future.get();
            
            assertNotNull(result);
            assertEquals(50, result.remaining());
            
            byte[] resultBytes = new byte[result.remaining()];
            result.get(resultBytes);
            
            for (int i = 0; i < 50; i++) {
                assertEquals((byte) ((100 + i) % 256), resultBytes[i]);
            }
        }
    }

    @Test
    void testLargeRangeRead() throws Exception {
        // Create a larger test file to trigger memory mapping
        byte[] testData = new byte[50000]; // 50KB to trigger memory mapping
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) (i % 256);
        }
        Path testFile = tempDir.resolve("large_test.dat");
        Files.write(testFile, testData);
        
        try (FileByteRangeFetcher fetcher = new FileByteRangeFetcher(testFile)) {
            // Test reading a large range (should use memory mapping)
            CompletableFuture<ByteBuffer> future = fetcher.fetchRange(1000, 10000);
            ByteBuffer result = future.get();
            
            assertNotNull(result);
            assertEquals(10000, result.remaining());
            
            // Verify first and last bytes
            assertEquals((byte) ((1000) % 256), result.get(0));
            assertEquals((byte) ((1000 + 9999) % 256), result.get(9999));
        }
    }

    @Test
    void testGetSize() throws Exception {
        byte[] testData = new byte[1234];
        Path testFile = tempDir.resolve("test.dat");
        Files.write(testFile, testData);
        
        try (FileByteRangeFetcher fetcher = new FileByteRangeFetcher(testFile)) {
            CompletableFuture<Long> future = fetcher.getSize();
            Long size = future.get();
            
            assertNotNull(size);
            assertEquals(1234L, size.longValue());
        }
    }

    @Test
    void testSupportsRangeRequests() throws Exception {
        byte[] testData = new byte[100];
        Path testFile = tempDir.resolve("test.dat");
        Files.write(testFile, testData);
        
        try (FileByteRangeFetcher fetcher = new FileByteRangeFetcher(testFile)) {
            assertTrue(fetcher.supportsRangeRequests());
        }
    }

    @Test
    void testGetSource() throws Exception {
        byte[] testData = new byte[100];
        Path testFile = tempDir.resolve("test.dat");
        Files.write(testFile, testData);
        
        try (FileByteRangeFetcher fetcher = new FileByteRangeFetcher(testFile)) {
            String source = fetcher.getSource();
            assertNotNull(source);
            assertEquals(testFile.toUri().toString(), source);
        }
    }

    @Test
    void testReadBeyondFileSize() throws Exception {
        byte[] testData = new byte[100];
        Path testFile = tempDir.resolve("test.dat");
        Files.write(testFile, testData);
        
        try (FileByteRangeFetcher fetcher = new FileByteRangeFetcher(testFile)) {
            // Try to read beyond file size
            CompletableFuture<ByteBuffer> future = fetcher.fetchRange(150, 50);
            
            assertThrows(Exception.class, () -> future.get());
        }
    }

    @Test
    void testReadPartiallyBeyondFileSize() throws Exception {
        byte[] testData = new byte[100];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) i;
        }
        Path testFile = tempDir.resolve("test.dat");
        Files.write(testFile, testData);
        
        try (FileByteRangeFetcher fetcher = new FileByteRangeFetcher(testFile)) {
            // Try to read more than available, should return only available bytes
            CompletableFuture<ByteBuffer> future = fetcher.fetchRange(90, 50);
            ByteBuffer result = future.get();
            
            assertNotNull(result);
            assertEquals(10, result.remaining()); // Only 10 bytes available from offset 90
            
            byte[] resultBytes = new byte[result.remaining()];
            result.get(resultBytes);
            
            for (int i = 0; i < 10; i++) {
                assertEquals((byte) (90 + i), resultBytes[i]);
            }
        }
    }

    @Test
    void testNegativeOffset() throws Exception {
        byte[] testData = new byte[100];
        Path testFile = tempDir.resolve("test.dat");
        Files.write(testFile, testData);
        
        try (FileByteRangeFetcher fetcher = new FileByteRangeFetcher(testFile)) {
            assertThrows(IllegalArgumentException.class, () -> {
                fetcher.fetchRange(-1, 10);
            });
        }
    }

    @Test
    void testZeroLength() throws Exception {
        byte[] testData = new byte[100];
        Path testFile = tempDir.resolve("test.dat");
        Files.write(testFile, testData);
        
        try (FileByteRangeFetcher fetcher = new FileByteRangeFetcher(testFile)) {
            assertThrows(IllegalArgumentException.class, () -> {
                fetcher.fetchRange(0, 0);
            });
        }
    }

    @Test
    void testNegativeLength() throws Exception {
        byte[] testData = new byte[100];
        Path testFile = tempDir.resolve("test.dat");
        Files.write(testFile, testData);
        
        try (FileByteRangeFetcher fetcher = new FileByteRangeFetcher(testFile)) {
            assertThrows(IllegalArgumentException.class, () -> {
                fetcher.fetchRange(0, -10);
            });
        }
    }

    @Test
    void testNullPath() {
        assertThrows(IllegalArgumentException.class, () -> {
            new FileByteRangeFetcher(null);
        });
    }

    @Test
    void testNonExistentFile() {
        Path nonExistentFile = tempDir.resolve("nonexistent.dat");
        
        assertThrows(IOException.class, () -> {
            new FileByteRangeFetcher(nonExistentFile);
        });
    }

    @Test
    void testDirectory() throws Exception {
        Path directory = tempDir.resolve("testdir");
        Files.createDirectory(directory);
        
        assertThrows(IOException.class, () -> {
            new FileByteRangeFetcher(directory);
        });
    }

    @Test
    void testUsageAfterClose() throws Exception {
        byte[] testData = new byte[100];
        Path testFile = tempDir.resolve("test.dat");
        Files.write(testFile, testData);
        
        FileByteRangeFetcher fetcher = new FileByteRangeFetcher(testFile);
        fetcher.close();
        
        // Operations after close should throw IOException
        assertThrows(IOException.class, () -> {
            fetcher.fetchRange(0, 10);
        });
        
        assertThrows(IOException.class, () -> {
            fetcher.getSize();
        });
    }

    @Test
    void testConcurrentAccess() throws Exception {
        // Create test file with known content
        byte[] testData = new byte[10000];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) (i % 256);
        }
        Path testFile = tempDir.resolve("concurrent_test.dat");
        Files.write(testFile, testData);
        
        try (FileByteRangeFetcher fetcher = new FileByteRangeFetcher(testFile)) {
            // Start multiple concurrent reads
            CompletableFuture<ByteBuffer> future1 = fetcher.fetchRange(0, 100);
            CompletableFuture<ByteBuffer> future2 = fetcher.fetchRange(1000, 100);
            CompletableFuture<ByteBuffer> future3 = fetcher.fetchRange(5000, 100);
            
            // Wait for all to complete
            ByteBuffer result1 = future1.get();
            ByteBuffer result2 = future2.get();
            ByteBuffer result3 = future3.get();
            
            // Verify all results are correct
            assertNotNull(result1);
            assertNotNull(result2);
            assertNotNull(result3);
            
            assertEquals(100, result1.remaining());
            assertEquals(100, result2.remaining());
            assertEquals(100, result3.remaining());
            
            assertEquals((byte) 0, result1.get(0));
            assertEquals((byte) (1000 % 256), result2.get(0));
            assertEquals((byte) (5000 % 256), result3.get(0));
        }
    }
}