package io.nosqlbench.vectordata.downloader;

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
import io.nosqlbench.vectordata.events.EventSink;
import io.nosqlbench.vectordata.events.NoOpEventSink;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for ResourceTransportService to ensure it works correctly for
 * both resource metadata queries and downloads.
 */
@ExtendWith(JettyFileServerExtension.class)
public class ResourceTransportServiceTest {
    
    @BeforeAll
    public static void setUp() throws IOException {
        // Create test data files in the ephemeral temp directory
        Path tempTestServerDir = JettyFileServerExtension.TEMP_RESOURCES_ROOT.resolve("httptest");
        Files.createDirectories(tempTestServerDir);
        
        // Create test files with different extensions that the tests expect
        createTestFile(tempTestServerDir.resolve("test_http.fvecs"), "This is test fvecs data for HTTP transport testing.");
        createTestFile(tempTestServerDir.resolve("test_http.bvecs"), "This is test bvecs data for HTTP download testing.");
        
        System.out.println("Created test files in: " + tempTestServerDir);
    }
    
    private static void createTestFile(Path file, String content) throws IOException {
        Files.write(file, content.getBytes());
        System.out.println("  Created: " + file.getFileName() + " (" + content.length() + " bytes)");
    }

    @Test
    public void testResourceMetadataForHttpResources() throws Exception {
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        URL testFileUrl = new URL(baseUrl, "temp/httptest/test_http.fvecs");
        
        ResourceTransportService service = new ChunkedResourceTransportService();
        
        // Test getting metadata
        ResourceMetadata metadata = service.getResourceMetadata(testFileUrl).get();
        
        assertTrue(metadata.exists(), "Resource should exist");
        assertTrue(metadata.hasValidSize(), "Resource should have valid size");
        assertTrue(metadata.size() > 0, "Resource size should be positive");
        assertTrue(metadata.supportsRanges(), "HTTP server should support ranges");
        
        System.out.println("HTTP Resource metadata:");
        System.out.println("  Size: " + metadata.size() + " bytes");
        System.out.println("  Supports ranges: " + metadata.supportsRanges());
        System.out.println("  Content type: " + metadata.contentType());
    }
    
    @Test 
    public void testResourceMetadataForFileResources(@TempDir Path tempDir) throws Exception {
        // Create a test file with unique prefix
        Path testFile = tempDir.resolve("resource_transport_test.dat");
        byte[] testData = "Hello, World!".getBytes();
        Files.write(testFile, testData);
        
        URL fileUrl = new URL("file://" + testFile.toAbsolutePath());
        ResourceTransportService service = new ChunkedResourceTransportService();
        
        // Test getting metadata
        ResourceMetadata metadata = service.getResourceMetadata(fileUrl).get();
        
        assertTrue(metadata.exists(), "File resource should exist");
        assertTrue(metadata.hasValidSize(), "File resource should have valid size");
        assertEquals(testData.length, metadata.size(), "File size should match");
        assertTrue(metadata.supportsRanges(), "File resources should support ranges");
        
        System.out.println("File Resource metadata:");
        System.out.println("  Size: " + metadata.size() + " bytes");
        System.out.println("  Supports ranges: " + metadata.supportsRanges());
    }
    
    @Test
    public void testResourceExists() throws Exception {
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        URL existingUrl = new URL(baseUrl, "temp/httptest/test_http.fvecs");
        URL nonExistingUrl = new URL(baseUrl, "temp/httptest/nonexistent_file.dat");
        
        ResourceTransportService service = new ChunkedResourceTransportService();
        
        assertTrue(service.resourceExists(existingUrl).get(), "Existing resource should return true");
        assertFalse(service.resourceExists(nonExistingUrl).get(), "Non-existing resource should return false");
    }
    
    @Test
    public void testDownloadResource(@TempDir Path tempDir) throws Exception {
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        URL testFileUrl = new URL(baseUrl, "temp/httptest/test_http.bvecs");
        
        Path targetFile = tempDir.resolve("downloaded_transport_test.bvecs");
        
        ResourceTransportService service = new ChunkedResourceTransportService();
        
        // Download the resource
        DownloadProgress progress = service.downloadResource(testFileUrl, targetFile, true);
        DownloadResult result = progress.get();
        
        assertTrue(result.isSuccess(), "Download should succeed");
        assertTrue(Files.exists(targetFile), "Downloaded file should exist");
        assertTrue(Files.size(targetFile) > 0, "Downloaded file should have content");
        
        System.out.println("Download completed:");
        System.out.println("  File: " + targetFile);
        System.out.println("  Size: " + Files.size(targetFile) + " bytes");
    }
    
    
    @Test
    public void testLocalMatchesRemote(@TempDir Path tempDir) throws Exception {
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        URL remoteUrl = new URL(baseUrl, "temp/httptest/test_http.fvecs");
        
        ResourceTransportService service = new ChunkedResourceTransportService();
        
        // First download the file
        Path localFile = tempDir.resolve("local_transport_copy.fvecs");
        DownloadProgress progress = service.downloadResource(remoteUrl, localFile, true);
        progress.get(); // Wait for completion
        
        // Now test if local matches remote
        assertTrue(service.localMatchesRemote(localFile, remoteUrl).get(), 
                  "Local file should match remote after download");
        
        // Test with non-existing local file
        Path nonExistentFile = tempDir.resolve("transport_does_not_exist.fvecs");
        assertFalse(service.localMatchesRemote(nonExistentFile, remoteUrl).get(),
                   "Non-existing local file should not match remote");
    }
}