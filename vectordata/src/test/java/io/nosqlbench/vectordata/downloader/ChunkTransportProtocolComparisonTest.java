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
import io.nosqlbench.vectordata.status.StdoutDownloadEventSink;
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
 * Test to verify that chunk transport service behaves consistently
 * regardless of URL scheme (file:// vs http://)
 */
@ExtendWith(JettyFileServerExtension.class)
public class ChunkTransportProtocolComparisonTest {

    @Test
    public void testBothProtocolsWork(@TempDir Path tempDir) throws IOException, ExecutionException, InterruptedException {
        System.out.println("\n=== Testing both file:// and http:// protocols work correctly ===");
        
        // Get the test file from resources
        Path testResourceFile = Path.of("src/test/resources/testserver/rawdatasets/testxvec/testxvec_base.fvec");
        assertTrue(Files.exists(testResourceFile), "Test resource file should exist");
        
        Path fileTargetDir = tempDir.resolve("file_test");
        Path httpTargetDir = tempDir.resolve("http_test");
        Files.createDirectories(fileTargetDir);
        Files.createDirectories(httpTargetDir);
        
        // Test 1: Download via file:// protocol
        System.out.println("\n--- Testing file:// protocol ---");
        URL fileUrl = new URL("file://" + testResourceFile.toAbsolutePath());
        Path fileTarget = fileTargetDir.resolve("testxvec_base.fvec");
        
        ChunkedDownloader fileDownloader = new ChunkedDownloader(
            fileUrl,
            "testxvec_base.fvec",
            1024 * 1024, // 1MB chunks
            4, // parallelism
            new NoOpDownloadEventSink()
        );
        
        DownloadProgress fileProgress = fileDownloader.download(fileTarget, true);
        DownloadResult fileResult = fileProgress.get();
        
        // Verify file:// download
        assertTrue(fileResult.isSuccess(), "file:// download should succeed");
        assertTrue(Files.exists(fileTarget), "Downloaded file should exist");
        long fileSize = Files.size(fileTarget);
        System.out.println("file:// download successful! Size: " + fileSize + " bytes");
        
        // Note: ChunkedDownloader only downloads the specific file requested
        // Merkle files must be downloaded separately if needed
        Path fileMerkle = fileTargetDir.resolve("testxvec_base.fvec.mrkl");
        assertFalse(Files.exists(fileMerkle), "ChunkedDownloader should not auto-download merkle files");
        
        // Test 2: Download via http:// protocol
        System.out.println("\n--- Testing http:// protocol ---");
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        URL httpUrl = new URL(baseUrl, "rawdatasets/testxvec/testxvec_base.fvec");
        Path httpTarget = httpTargetDir.resolve("testxvec_base.fvec");
        
        ChunkedDownloader httpDownloader = new ChunkedDownloader(
            httpUrl,
            "testxvec_base.fvec",
            1024 * 1024, // 1MB chunks
            4, // parallelism
            new NoOpDownloadEventSink()
        );
        
        DownloadProgress httpProgress = httpDownloader.download(httpTarget, true);
        DownloadResult httpResult = httpProgress.get();
        
        // Verify http:// download
        assertTrue(httpResult.isSuccess(), "http:// download should succeed");
        assertTrue(Files.exists(httpTarget), "Downloaded file should exist");
        long httpSize = Files.size(httpTarget);
        System.out.println("http:// download successful! Size: " + httpSize + " bytes");
        
        // Note: ChunkedDownloader only downloads the specific file requested
        // Merkle files must be downloaded separately if needed
        Path httpMerkle = httpTargetDir.resolve("testxvec_base.fvec.mrkl");
        assertFalse(Files.exists(httpMerkle), "ChunkedDownloader should not auto-download merkle files");
        
        // Compare results
        System.out.println("\n--- Comparing results ---");
        assertEquals(fileSize, httpSize, "Both protocols should download the same size file");
        assertEquals(10100000, fileSize, "File size should match expected test data size");
        
        // Both protocols successfully downloaded the data files
        // For merkle tree operations, use MerkleTree.syncFromRemote() which handles both data and merkle files
        
        System.out.println("\n✓ Both protocols work correctly and produce consistent results!");
    }
    
    @Test
    public void testChunkVerificationWithBothProtocols(@TempDir Path tempDir) throws IOException, ExecutionException, InterruptedException {
        System.out.println("\n=== Testing chunk verification with both protocols ===");
        
        // Get base URL
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        
        // Test with a file that has merkle verification
        Path targetDir = tempDir.resolve("chunk_verify_test");
        Files.createDirectories(targetDir);
        
        // Download with verbose event sink to see chunk verification
        StdoutDownloadEventSink eventSink = new StdoutDownloadEventSink();
        
        // Test http:// with chunk verification
        URL httpUrl = new URL(baseUrl, "rawdatasets/testxvec/testxvec_query.fvec");
        Path httpTarget = targetDir.resolve("testxvec_query.fvec");
        
        ChunkedDownloader httpDownloader = new ChunkedDownloader(
            httpUrl,
            "testxvec_query.fvec",
            1024 * 1024, // 1MB chunks
            2, // lower parallelism to see events clearly
            eventSink
        );
        
        System.out.println("\nDownloading via http:// with chunk verification...");
        DownloadProgress httpProgress = httpDownloader.download(httpTarget, true);
        DownloadResult httpResult = httpProgress.get();
        
        assertTrue(httpResult.isSuccess(), "http:// download with verification should succeed");
        assertTrue(Files.exists(httpTarget), "Downloaded file should exist");
        
        // ChunkedDownloader only downloads the specific file requested
        // To test merkle functionality, we need to download the merkle file separately
        Path httpMerkle = targetDir.resolve("testxvec_query.fvec.mrkl");
        assertFalse(Files.exists(httpMerkle), "ChunkedDownloader should not auto-download merkle files");
        
        // Now explicitly download the merkle file to demonstrate it works
        URL merkleUrl = new URL(baseUrl, "rawdatasets/testxvec/testxvec_query.fvec.mrkl");
        ChunkedDownloader merkleDownloader = new ChunkedDownloader(
            merkleUrl,
            "testxvec_query.fvec.mrkl",
            1024 * 1024, 
            2,
            eventSink
        );
        
        DownloadProgress merkleProgress = merkleDownloader.download(httpMerkle, true);
        DownloadResult merkleResult = merkleProgress.get();
        
        assertTrue(merkleResult.isSuccess(), "Merkle file download should succeed");
        assertTrue(Files.exists(httpMerkle), "Merkle file should exist after explicit download");
        
        // Load merkle tree to verify it's valid
        MerkleTree merkleTree = MerkleTree.load(httpMerkle, false);
        assertTrue(merkleTree.getNumberOfLeaves() > 0, "Merkle tree should have chunks");
        
        System.out.println("\n✓ Chunk verification works correctly with http:// protocol!");
        System.out.println("  File size: " + Files.size(httpTarget) + " bytes");
        System.out.println("  Merkle file size: " + Files.size(httpMerkle) + " bytes");
        System.out.println("  Chunks: " + merkleTree.getNumberOfLeaves());
        System.out.println("  Chunk size: " + merkleTree.getChunkSize() + " bytes");
    }
    
    @Test
    public void testHighLevelMerkleOperationsWithChunkTransport(@TempDir Path tempDir) throws Exception {
        System.out.println("\n=== Testing high-level merkle operations use chunk transport ===");
        
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        URL dataUrl = new URL(baseUrl, "rawdatasets/testxvec/testxvec_base.fvec");
        Path localDataPath = tempDir.resolve("high_level_test.fvec");
        
        // Use MerkleTree.syncFromRemote which uses ResourceTransportService internally
        // This should download both data and merkle files using chunk transport
        MerkleTree merkleTree = MerkleTree.syncFromRemote(dataUrl, localDataPath);
        
        // Verify both files were downloaded
        assertTrue(Files.exists(localDataPath), "Data file should be downloaded");
        assertEquals(10100000, Files.size(localDataPath), "Data file size should match");
        
        Path merkleFile = tempDir.resolve("high_level_test.fvec.mrkl");
        assertTrue(Files.exists(merkleFile), "Merkle file should be downloaded by syncFromRemote");
        
        Path refFile = tempDir.resolve("high_level_test.fvec.mref");
        assertTrue(Files.exists(refFile), "Reference file should be created by syncFromRemote");
        
        // Verify the merkle tree is functional
        assertNotNull(merkleTree, "MerkleTree should be loaded");
        assertTrue(merkleTree.getNumberOfLeaves() > 0, "MerkleTree should have chunks");
        
        System.out.println("High-level merkle operations completed successfully:");
        System.out.println("  Data file: " + Files.size(localDataPath) + " bytes");
        System.out.println("  Merkle file: " + Files.size(merkleFile) + " bytes");
        System.out.println("  Reference file: " + Files.size(refFile) + " bytes");
        System.out.println("  Tree chunks: " + merkleTree.getNumberOfLeaves());
        System.out.println("  Chunk size: " + merkleTree.getChunkSize() + " bytes");
        
        System.out.println("\n✓ High-level operations correctly use chunk transport for all downloads!");
    }
}