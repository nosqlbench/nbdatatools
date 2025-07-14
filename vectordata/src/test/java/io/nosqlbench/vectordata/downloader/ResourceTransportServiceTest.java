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
import io.nosqlbench.vectordata.merkle.MerkleTree;
import io.nosqlbench.vectordata.status.NoOpDownloadEventSink;
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

    @Test
    public void testResourceMetadataForHttpResources() throws Exception {
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        URL testFileUrl = new URL(baseUrl, "rawdatasets/testxvec/testxvec_base.fvec");
        
        ResourceTransportService service = new ChunkedResourceTransportService();
        
        // Test getting metadata
        ResourceMetadata metadata = service.getResourceMetadata(testFileUrl).get();
        
        assertTrue(metadata.exists(), "Resource should exist");
        assertTrue(metadata.hasValidSize(), "Resource should have valid size");
        assertEquals(10100000, metadata.size(), "Resource size should match expected");
        assertTrue(metadata.supportsRanges(), "HTTP server should support ranges");
        
        System.out.println("HTTP Resource metadata:");
        System.out.println("  Size: " + metadata.size() + " bytes");
        System.out.println("  Supports ranges: " + metadata.supportsRanges());
        System.out.println("  Content type: " + metadata.contentType());
    }
    
    @Test 
    public void testResourceMetadataForFileResources(@TempDir Path tempDir) throws Exception {
        // Create a test file
        Path testFile = tempDir.resolve("test.dat");
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
        URL existingUrl = new URL(baseUrl, "rawdatasets/testxvec/testxvec_base.fvec");
        URL nonExistingUrl = new URL(baseUrl, "rawdatasets/nonexistent/file.dat");
        
        ResourceTransportService service = new ChunkedResourceTransportService();
        
        assertTrue(service.resourceExists(existingUrl).get(), "Existing resource should return true");
        assertFalse(service.resourceExists(nonExistingUrl).get(), "Non-existing resource should return false");
    }
    
    @Test
    public void testDownloadResource(@TempDir Path tempDir) throws Exception {
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        URL testFileUrl = new URL(baseUrl, "rawdatasets/testxvec/testxvec_query.fvec");
        
        Path targetFile = tempDir.resolve("downloaded_query.fvec");
        
        ResourceTransportService service = new ChunkedResourceTransportService();
        
        // Download the resource
        DownloadProgress progress = service.downloadResource(testFileUrl, targetFile, true);
        DownloadResult result = progress.get();
        
        assertTrue(result.isSuccess(), "Download should succeed");
        assertTrue(Files.exists(targetFile), "Downloaded file should exist");
        assertEquals(4040000, Files.size(targetFile), "Downloaded file size should match expected");
        
        System.out.println("Download completed:");
        System.out.println("  File: " + targetFile);
        System.out.println("  Size: " + Files.size(targetFile) + " bytes");
    }
    
    @Test
    public void testMerkleTreeSyncUsingResourceTransport(@TempDir Path tempDir) throws Exception {
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        URL dataUrl = new URL(baseUrl, "rawdatasets/testxvec/testxvec_base.fvec");
        
        Path localDataPath = tempDir.resolve("synced_base.fvec");
        
        // Create a custom transport service for testing
        ResourceTransportService transportService = new ChunkedResourceTransportService(
            1024 * 1024, // 1MB chunks
            2, // 2 parallel downloads
            new NoOpDownloadEventSink(),
            java.util.concurrent.ForkJoinPool.commonPool()
        );
        
        System.out.println("Testing MerkleTree.syncFromRemote with ResourceTransportService");
        System.out.println("Data URL: " + dataUrl);
        System.out.println("Local path: " + localDataPath);
        
        // Sync from remote using the new transport service
        MerkleTree merkleTree = MerkleTree.syncFromRemote(dataUrl, localDataPath, transportService);
        
        // Verify the result
        assertNotNull(merkleTree, "MerkleTree should be loaded");
        assertTrue(Files.exists(localDataPath), "Data file should be downloaded");
        assertEquals(10100000, Files.size(localDataPath), "Data file size should match");
        
        // Verify merkle file was also downloaded
        Path merkleFile = tempDir.resolve("synced_base.fvec.mrkl");
        assertTrue(Files.exists(merkleFile), "Merkle file should be downloaded");
        
        // Verify reference file was created
        Path refFile = tempDir.resolve("synced_base.fvec.mref");
        assertTrue(Files.exists(refFile), "Reference file should be created");
        
        System.out.println("MerkleTree sync completed successfully:");
        System.out.println("  Data file: " + Files.size(localDataPath) + " bytes");
        System.out.println("  Merkle file: " + Files.size(merkleFile) + " bytes");
        System.out.println("  Reference file: " + Files.size(refFile) + " bytes");
        System.out.println("  Tree chunks: " + merkleTree.getNumberOfLeaves());
        System.out.println("  Chunk size: " + merkleTree.getChunkSize() + " bytes");
    }
    
    @Test
    public void testLocalMatchesRemote(@TempDir Path tempDir) throws Exception {
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        URL remoteUrl = new URL(baseUrl, "rawdatasets/testxvec/testxvec_base.fvec");
        
        ResourceTransportService service = new ChunkedResourceTransportService();
        
        // First download the file
        Path localFile = tempDir.resolve("local_copy.fvec");
        DownloadProgress progress = service.downloadResource(remoteUrl, localFile, true);
        progress.get(); // Wait for completion
        
        // Now test if local matches remote
        assertTrue(service.localMatchesRemote(localFile, remoteUrl).get(), 
                  "Local file should match remote after download");
        
        // Test with non-existing local file
        Path nonExistentFile = tempDir.resolve("does_not_exist.fvec");
        assertFalse(service.localMatchesRemote(nonExistentFile, remoteUrl).get(),
                   "Non-existing local file should not match remote");
    }
}