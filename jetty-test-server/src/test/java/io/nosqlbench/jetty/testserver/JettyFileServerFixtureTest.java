package io.nosqlbench.jetty.testserver;

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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the JettyFileServerFixture class.
 * This test verifies basic HTTP server functionality including file serving and range requests.
 */
@ExtendWith(JettyFileServerExtension.class)
public class JettyFileServerFixtureTest {

    private URL baseUrl;

    @BeforeEach
    public void setUp() throws IOException {
        // Use the shared server instance from JettyFileServerExtension
        baseUrl = JettyFileServerExtension.getBaseUrl();
    }

    @Test
    public void testServerStartsAndServesFiles() throws IOException {
        // Test that the server is running and can serve files
        URL testUrl = new URL(baseUrl, "basic.txt");
        HttpURLConnection connection = (HttpURLConnection) testUrl.openConnection();
        try {
            // Check the HTTP status code
            int statusCode = connection.getResponseCode();
            assertEquals(200, statusCode, "HTTP status code should be 200");

            try (InputStream in = connection.getInputStream()) {
                assertNotNull(in);
                byte[] data = in.readAllBytes();
                assertTrue(data.length > 0, "Server should return non-empty content");
            }
        } finally {
            connection.disconnect();
        }
    }


    @Test
    public void testRangeRequests() throws IOException {
        // Test that the server correctly handles HTTP Range requests
        URL dataUrl = new URL(baseUrl, "basic.txt");

        // First, get the total file size
        HttpURLConnection headConnection = (HttpURLConnection) dataUrl.openConnection();
        headConnection.setRequestMethod("HEAD");
        try {
            int statusCode = headConnection.getResponseCode();
            assertEquals(200, statusCode, "HTTP status code should be 200 for HEAD request");

            // Get the content length
            String contentLengthStr = headConnection.getHeaderField("Content-Length");
            assertNotNull(contentLengthStr, "Content-Length header should be present");
            long contentLength = Long.parseLong(contentLengthStr);
            assertTrue(contentLength > 0, "Content length should be greater than 0");

            long rangeEnd = Math.min(contentLength - 1, 99);
            String rangeHeader = "bytes=0-" + rangeEnd;

            HttpURLConnection rangeConnection = (HttpURLConnection) dataUrl.openConnection();
            rangeConnection.setRequestProperty("Range", rangeHeader);
            try {
                statusCode = rangeConnection.getResponseCode();
                assertEquals(206, statusCode, "HTTP status code should be 206 for partial content");

                String contentRange = rangeConnection.getHeaderField("Content-Range");
                assertNotNull(contentRange, "Content-Range header should be present");
                assertTrue(contentRange.startsWith("bytes 0-"), "Content-Range should start with 'bytes 0-'");

                long expectedLength = rangeEnd + 1;
                assertEquals(expectedLength, rangeConnection.getContentLength(), "Content length should match the range size");

                try (InputStream in = rangeConnection.getInputStream()) {
                    byte[] data = in.readAllBytes();
                    assertEquals(expectedLength, data.length, "Response should contain the expected number of bytes");
                }
            } finally {
                rangeConnection.disconnect();
            }
        } finally {
            headConnection.disconnect();
        }
    }

    @Test
    public void testBasicFileServing() throws IOException {
        Path tempDir = JettyFileServerExtension.TEMP_RESOURCES_ROOT.resolve("basicfiletest");
        Files.createDirectories(tempDir);
        
        String uniqueId = String.valueOf(System.currentTimeMillis());
        Path testFile = tempDir.resolve("basic_test_" + uniqueId + ".txt");
        String testContent = "Hello from the test server!\nTimestamp: " + uniqueId;
        Files.writeString(testFile, testContent);
        
        try {
            String fileUrl = baseUrl.toString() + "temp/basicfiletest/basic_test_" + uniqueId + ".txt";
            URI uri = URI.create(fileUrl);
            HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
            try {
                int statusCode = connection.getResponseCode();
                assertEquals(200, statusCode, "HTTP status code should be 200");
                
                String contentType = connection.getContentType();
                assertNotNull(contentType, "Content-Type header should be present");
                assertTrue(contentType.startsWith("text/"));
                
                try (InputStream in = connection.getInputStream()) {
                    byte[] data = in.readAllBytes();
                    String receivedContent = new String(data);
                    assertEquals(testContent, receivedContent);
                }
            } finally {
                connection.disconnect();
            }
        } finally {
            Files.deleteIfExists(testFile);
            try {
                Files.deleteIfExists(tempDir);
            } catch (java.nio.file.DirectoryNotEmptyException e) {
                // Ignore if directory not empty
            }
        }
    }
}