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

import io.nosqlbench.jetty.testserver.JettyFileServerExtension;
import io.nosqlbench.jetty.testserver.JettyFileServerFixture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/// Unit tests for HttpByteRangeFetcher.
@ExtendWith(JettyFileServerExtension.class)
public class HttpByteRangeFetcherTest {

    private URL baseUrl;
    private JettyFileServerFixture serverFixture;
    private String testFileUrl;
    private Path testFile;

    @BeforeEach
    void setUp() throws IOException {
        baseUrl = JettyFileServerExtension.getBaseUrl();
        serverFixture = JettyFileServerExtension.getServer();
        
        // Create a test file in the server root directory
        Path serverRoot = serverFixture.getRootDirectory();
        testFile = serverRoot.resolve("test_fvec_file.bin");
        
        // Create a simple test file that mimics the structure we're testing
        // First 4 bytes: dimension count (100 as little-endian int)
        // Then: some test data
        ByteBuffer buffer = ByteBuffer.allocate(10100000); // Same size as expected
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(100); // dimensions
        
        // Fill with test data
        for (int i = 4; i < buffer.capacity(); i++) {
            buffer.put((byte) (i % 256));
        }
        
        Files.write(testFile, buffer.array());
        testFileUrl = baseUrl.toString() + "test_fvec_file.bin";
    }

    @Test
    void testBasicRangeRead() throws Exception {
        try (HttpByteRangeFetcher fetcher = new HttpByteRangeFetcher(testFileUrl)) {
            // Test reading the first 16 bytes (4 dimensions as float)
            CompletableFuture<ByteBuffer> future = fetcher.fetchRange(0, 16);
            ByteBuffer result = future.get();
            
            assertNotNull(result);
            assertEquals(16, result.remaining());
            
            // The first 4 bytes should represent the dimension count (100 in little-endian)
            // Read as little-endian since fvec format uses little-endian byte order
            result.order(ByteOrder.LITTLE_ENDIAN);
            int dimensions = result.getInt(0);
            assertEquals(100, dimensions);
        }
    }

    @Test
    void testRangeReadFromOffset() throws Exception {
        try (HttpByteRangeFetcher fetcher = new HttpByteRangeFetcher(testFileUrl)) {
            // Test reading from a specific offset
            CompletableFuture<ByteBuffer> future = fetcher.fetchRange(1000, 100);
            ByteBuffer result = future.get();
            
            assertNotNull(result);
            assertEquals(100, result.remaining());
        }
    }

    @Test
    void testGetSize() throws Exception {
        try (HttpByteRangeFetcher fetcher = new HttpByteRangeFetcher(testFileUrl)) {
            CompletableFuture<Long> future = fetcher.getSize();
            Long size = future.get();
            
            assertNotNull(size);
            assertEquals(10100000L, size.longValue()); // Expected size of testxvec_base.fvec
        }
    }

    @Test
    void testSupportsRangeRequests() throws Exception {
        try (HttpByteRangeFetcher fetcher = new HttpByteRangeFetcher(testFileUrl)) {
            // The test server should support range requests
            assertTrue(fetcher.supportsRangeRequests());
        }
    }

    @Test
    void testGetSource() throws Exception {
        try (HttpByteRangeFetcher fetcher = new HttpByteRangeFetcher(testFileUrl)) {
            String source = fetcher.getSource();
            assertNotNull(source);
            assertEquals(testFileUrl, source);
        }
    }

    @Test
    void testConcurrentAccess() throws Exception {
        try (HttpByteRangeFetcher fetcher = new HttpByteRangeFetcher(testFileUrl)) {
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
        }
    }

    @Test
    void testNegativeOffset() throws Exception {
        try (HttpByteRangeFetcher fetcher = new HttpByteRangeFetcher(testFileUrl)) {
            assertThrows(IllegalArgumentException.class, () -> {
                fetcher.fetchRange(-1, 10);
            });
        }
    }

    @Test
    void testZeroLength() throws Exception {
        try (HttpByteRangeFetcher fetcher = new HttpByteRangeFetcher(testFileUrl)) {
            assertThrows(IllegalArgumentException.class, () -> {
                fetcher.fetchRange(0, 0);
            });
        }
    }

    @Test
    void testNegativeLength() throws Exception {
        try (HttpByteRangeFetcher fetcher = new HttpByteRangeFetcher(testFileUrl)) {
            assertThrows(IllegalArgumentException.class, () -> {
                fetcher.fetchRange(0, -10);
            });
        }
    }

    @Test
    void testNullUrl() {
        assertThrows(IllegalArgumentException.class, () -> {
            new HttpByteRangeFetcher(null);
        });
    }

    @Test
    void testEmptyUrl() {
        assertThrows(IllegalArgumentException.class, () -> {
            new HttpByteRangeFetcher("");
        });
    }

    @Test
    void testNonHttpUrl() {
        assertThrows(IllegalArgumentException.class, () -> {
            new HttpByteRangeFetcher("file://path/to/file");
        });
    }

    @Test
    void testUsageAfterClose() throws Exception {
        HttpByteRangeFetcher fetcher = new HttpByteRangeFetcher(testFileUrl);
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
    void testReadBeyondFileSize() throws Exception {
        try (HttpByteRangeFetcher fetcher = new HttpByteRangeFetcher(testFileUrl)) {
            // Try to read beyond file size
            long fileSize = fetcher.getSize().get();
            CompletableFuture<ByteBuffer> future = fetcher.fetchRange(fileSize + 1000, 100);
            
            assertThrows(Exception.class, () -> future.get());
        }
    }

    @Test
    void testSmallAndLargeRanges() throws Exception {
        try (HttpByteRangeFetcher fetcher = new HttpByteRangeFetcher(testFileUrl)) {
            // Test small range
            CompletableFuture<ByteBuffer> smallFuture = fetcher.fetchRange(0, 16);
            ByteBuffer smallResult = smallFuture.get();
            assertNotNull(smallResult);
            assertEquals(16, smallResult.remaining());
            
            // Test larger range
            CompletableFuture<ByteBuffer> largeFuture = fetcher.fetchRange(1000, 8192);
            ByteBuffer largeResult = largeFuture.get();
            assertNotNull(largeResult);
            assertEquals(8192, largeResult.remaining());
        }
    }
}