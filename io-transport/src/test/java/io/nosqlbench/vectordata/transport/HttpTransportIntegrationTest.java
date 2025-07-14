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
import io.nosqlbench.nbdatatools.api.transport.ChunkedTransportClient;
import io.nosqlbench.nbdatatools.api.transport.ChunkedTransportIO;
import io.nosqlbench.nbdatatools.api.transport.FetchResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for HTTP transport using the Jetty test server.
 */
@ExtendWith(JettyFileServerExtension.class)
public class HttpTransportIntegrationTest {

    private URL baseUrl;
    private JettyFileServerFixture serverFixture;

    @BeforeEach
    public void setUp() {
        baseUrl = JettyFileServerExtension.getBaseUrl();
        serverFixture = JettyFileServerExtension.getServer();
    }

    @Test
    public void testHttpTransportWithTestServer() throws IOException {
        // Get the server root directory and create a test file there
        Path serverRoot = serverFixture.getRootDirectory();
        Path testDir = serverRoot.resolve("httptest");
        Files.createDirectories(testDir);
        
        String uniqueId = String.valueOf(System.currentTimeMillis());
        Path testFile = testDir.resolve("http_test_" + uniqueId + ".txt");
        String testContent = "HTTP transport test content\nLine 2\nLine 3";
        Files.writeString(testFile, testContent);
        
        try {
            // Create HTTP URL for the test file (relative to server root)
            String fileUrl = baseUrl.toString() + "httptest/http_test_" + uniqueId + ".txt";
            
            // Create transport using ChunkedTransportIO
            try (ChunkedTransportClient transport = ChunkedTransportIO.create(fileUrl)) {
                assertNotNull(transport);
                assertTrue(transport instanceof HttpByteRangeFetcher);
                
                // Test getting file size
                CompletableFuture<Long> sizeFuture = transport.getSize();
                Long size = sizeFuture.get();
                assertNotNull(size);
                assertTrue(size > 0);
                assertEquals(testContent.getBytes().length, size.longValue());
                
                // Test range request  
                CompletableFuture<? extends FetchResult<?>> rangeFuture = transport.fetchRange(0, 10);
                FetchResult<?> result = rangeFuture.get();
                ByteBuffer buffer = result.getData();
                assertNotNull(buffer);
                assertEquals(10, buffer.remaining());
                
                byte[] data = new byte[buffer.remaining()];
                buffer.get(data);
                String receivedContent = new String(data);
                assertEquals(testContent.substring(0, 10), receivedContent);
                
                // Test range requests support
                assertTrue(transport.supportsRangeRequests());
                
                // Test source URL
                assertEquals(fileUrl, transport.getSource());
            }
        } catch (Exception e) {
            fail("Test failed with exception: " + e.getMessage(), e);
        } finally {
            Files.deleteIfExists(testFile);
            try {
                Files.deleteIfExists(testDir);
            } catch (java.nio.file.DirectoryNotEmptyException e) {
                // Ignore if directory not empty
            }
        }
    }
}