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
import java.io.RandomAccessFile;
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

    /// Test that verifies the jetty test fixture can download small files from both non-temp and temp directories
    @Test
    public void testDownloadFromNonTempAndTempDirectories() throws IOException {
        // Test downloading from non-temp directory (static resources)
        testDownloadFromNonTempDirectory();
        
        // Test downloading from temp directory (dynamic resources)
        testDownloadFromTempDirectory();
    }

    /// Test downloading a small file from the non-temp directory path
    private void testDownloadFromNonTempDirectory() throws IOException {
        // Download the existing basic.txt file from the static resources
        URL staticFileUrl = new URL(baseUrl, "basic.txt");
        HttpURLConnection connection = (HttpURLConnection) staticFileUrl.openConnection();
        
        try {
            int statusCode = connection.getResponseCode();
            assertEquals(200, statusCode, "Should successfully download from non-temp directory");
            
            try (InputStream in = connection.getInputStream()) {
                byte[] data = in.readAllBytes();
                assertTrue(data.length > 0, "Downloaded file should not be empty");
                
                String content = new String(data);
                assertTrue(content.contains("basic test file"), "Content should match expected static file");
            }
        } finally {
            connection.disconnect();
        }
    }

    /// Test downloading a small file from the temp directory path
    private void testDownloadFromTempDirectory() throws IOException {
        // Create a small test file in the temp directory
        Path tempDir = JettyFileServerExtension.TEMP_RESOURCES_ROOT.resolve("downloadtest");
        Files.createDirectories(tempDir);
        
        String uniqueId = String.valueOf(System.currentTimeMillis());
        Path testFile = tempDir.resolve("temp_download_test_" + uniqueId + ".txt");
        String testContent = "Small test file for download verification\nCreated at: " + uniqueId + "\nSize: 64 bytes";
        Files.writeString(testFile, testContent);
        
        try {
            // Download the file via HTTP
            URL tempFileUrl = new URL(baseUrl, "temp/downloadtest/temp_download_test_" + uniqueId + ".txt");
            HttpURLConnection connection = (HttpURLConnection) tempFileUrl.openConnection();
            
            try {
                int statusCode = connection.getResponseCode();
                assertEquals(200, statusCode, "Should successfully download from temp directory");
                
                // Verify content length header
                String contentLengthHeader = connection.getHeaderField("Content-Length");
                assertNotNull(contentLengthHeader, "Content-Length header should be present");
                int contentLength = Integer.parseInt(contentLengthHeader);
                assertEquals(testContent.length(), contentLength, "Content-Length should match file size");
                
                try (InputStream in = connection.getInputStream()) {
                    byte[] data = in.readAllBytes();
                    assertEquals(testContent.length(), data.length, "Downloaded data should match expected size");
                    
                    String downloadedContent = new String(data);
                    assertEquals(testContent, downloadedContent, "Downloaded content should match original file");
                }
            } finally {
                connection.disconnect();
            }
        } finally {
            // Clean up
            Files.deleteIfExists(testFile);
            try {
                Files.deleteIfExists(tempDir);
            } catch (java.nio.file.DirectoryNotEmptyException e) {
                // Ignore if directory not empty
            }
        }
    }

    /// Test that verifies HEAD response for files larger than 2GB in temp directory
    @Test
    public void testHeadResponseForLargeFile() throws IOException {
        // Create a temp directory for the large file test
        Path tempDir = JettyFileServerExtension.TEMP_RESOURCES_ROOT.resolve("largefile");
        Files.createDirectories(tempDir);
        
        String uniqueId = String.valueOf(System.currentTimeMillis());
        Path largeTestFile = tempDir.resolve("large_test_file_" + uniqueId + ".bin");
        
        // Create a sparse file larger than 2GB (2.1GB = 2,252,341,248 bytes)
        long fileSize = 2L * 1024 * 1024 * 1024 + 100 * 1024 * 1024; // 2.1GB
        
        try {
            // Use RandomAccessFile to create a sparse file
            try (RandomAccessFile raf = new RandomAccessFile(largeTestFile.toFile(), "rw")) {
                // Set the file length to create a sparse file
                raf.setLength(fileSize);
                // Write a small amount of data at the beginning and end to ensure the file has some content
                raf.seek(0);
                raf.write("START_OF_LARGE_FILE".getBytes());
                raf.seek(fileSize - 20);
                raf.write("END_OF_LARGE_FILE".getBytes());
            }
            
            // Verify the file was created with the expected size
            long actualSize = Files.size(largeTestFile);
            assertEquals(fileSize, actualSize, "Created file should have expected size");
            
            // Test HEAD request to the large file
            URL largeFileUrl = new URL(baseUrl, "temp/largefile/large_test_file_" + uniqueId + ".bin");
            HttpURLConnection headConnection = (HttpURLConnection) largeFileUrl.openConnection();
            headConnection.setRequestMethod("HEAD");
            
            try {
                int statusCode = headConnection.getResponseCode();
                assertEquals(200, statusCode, "HEAD request should return 200 for large file");
                
                // Verify Content-Length header for files larger than 2GB
                String contentLengthHeader = headConnection.getHeaderField("Content-Length");
                assertNotNull(contentLengthHeader, "Content-Length header should be present for large files");
                
                long reportedSize = Long.parseLong(contentLengthHeader);
                assertEquals(fileSize, reportedSize, "Content-Length should correctly report size for 2GB+ files");
                
                // Verify other expected headers
                String acceptRanges = headConnection.getHeaderField("Accept-Ranges");
                assertEquals("bytes", acceptRanges, "Accept-Ranges header should indicate byte range support");
                
                // Verify that HEAD request doesn't return a body
                assertEquals(0, headConnection.getContentLength() == -1 ? 0 : headConnection.getContentLength(), 
                    "HEAD request should not return content length as content length for body (should be 0 or -1)");
                
                // Test that we can make a small range request to verify the file is actually accessible
                HttpURLConnection rangeConnection = (HttpURLConnection) largeFileUrl.openConnection();
                rangeConnection.setRequestProperty("Range", "bytes=0-18");
                try {
                    int rangeStatusCode = rangeConnection.getResponseCode();
                    assertEquals(206, rangeStatusCode, "Range request should return 206 for partial content");
                    
                    try (InputStream in = rangeConnection.getInputStream()) {
                        byte[] data = in.readAllBytes();
                        String content = new String(data);
                        assertEquals("START_OF_LARGE_FILE", content, "Range request should return expected content from start of file");
                    }
                } finally {
                    rangeConnection.disconnect();
                }
                
            } finally {
                headConnection.disconnect();
            }
            
        } finally {
            // Clean up the large file
            Files.deleteIfExists(largeTestFile);
            try {
                Files.deleteIfExists(tempDir);
            } catch (java.nio.file.DirectoryNotEmptyException e) {
                // Ignore if directory not empty
            }
        }
    }
}