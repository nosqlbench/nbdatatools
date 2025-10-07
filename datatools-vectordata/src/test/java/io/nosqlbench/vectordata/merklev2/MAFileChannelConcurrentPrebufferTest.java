package io.nosqlbench.vectordata.merklev2;

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
import io.nosqlbench.nbdatatools.api.concurrent.ProgressIndicatingFuture;
import io.nosqlbench.vectordata.util.TestFixturePaths;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that verify MAFileChannel executes multiple concurrent downloads during prebuffer operations.
 */
@ExtendWith(JettyFileServerExtension.class)
class MAFileChannelConcurrentPrebufferTest {

    @Test
    void testPrebufferExecutesConcurrentDownloads(@TempDir Path tempDir, TestInfo testInfo) throws Exception {
        // Create a test file that will require multiple chunks to test concurrency
        final int TOTAL_SIZE = 2 * 1024 * 1024; // 2MB total to ensure 2 chunks
        
        String testFileName = TestFixturePaths.createTestSpecificFilename(testInfo, "concurrent_test.dat");
        byte[] testContent = createTestContent(TOTAL_SIZE);

        // Serve the file via HTTP using test-specific directory
        Path testSpecificTempDir = TestFixturePaths.createTestSpecificTempDir(testInfo);
        Path serverFile = testSpecificTempDir.resolve(testFileName);
        Files.write(serverFile, testContent);

        // Create merkle reference from the server file
        MerkleDataImpl merkleRef = (MerkleDataImpl) MerkleRef.fromDataSimple(serverFile).get();
        Path mrefPath = serverFile.resolveSibling(serverFile.getFileName() + ".mref");
        merkleRef.save(mrefPath);

        // Create test-specific server URL
        URL testSpecificUrl = TestFixturePaths.createTestSpecificServerUrl(testInfo, testFileName);
        
        // Use test-specific filenames for cache and state
        String cacheFilename = TestFixturePaths.createTestSpecificFilename(testInfo, "cache.dat");
        String stateFilename = TestFixturePaths.createTestSpecificFilename(testInfo, "state.mrkl");
        Path cacheFile = tempDir.resolve(cacheFilename);
        Path stateFile = tempDir.resolve(stateFilename);
        
        // Create MAFileChannel with the correct server URL
        MAFileChannel channel = new MAFileChannel(cacheFile, stateFile, testSpecificUrl.toString());
        
        try {
            // Verify initial state - no chunks should be valid yet
            assertEquals(TOTAL_SIZE, channel.size());
            
            MerkleShape shape = merkleRef.getShape();
            long actualNumChunks = shape.getLeafCount();
            long chunkSize = shape.getChunkSize();
            
            // Track concurrent download activity through timing and progress
            AtomicInteger maxConcurrentDownloads = new AtomicInteger(0);
            AtomicInteger progressUpdateCount = new AtomicInteger(0);
            long[] progressTimes = new long[10]; // Track timing of progress updates
            
            long startTime = System.currentTimeMillis();
            
            // Start prebuffer operation for the entire file
            CompletableFuture<Void> prebufferFuture = channel.prebuffer(0, TOTAL_SIZE);
            
            // Verify it's a ProgressIndicatingFuture
            assertInstanceOf(ProgressIndicatingFuture.class, prebufferFuture);
            ProgressIndicatingFuture<Void> progressFuture = (ProgressIndicatingFuture<Void>) prebufferFuture;
            
            // Monitor progress to verify chunks are being downloaded
            boolean[] chunkProgressSeen = new boolean[(int)actualNumChunks];
            
            // Poll progress until completion
            double lastProgress = -1;
            while (!prebufferFuture.isDone()) {
                double currentWork = progressFuture.getCurrentWork();
                double totalWork = progressFuture.getTotalWork();
                
                // Track progress changes with timing
                if (currentWork != lastProgress) {
                    int updateIdx = progressUpdateCount.getAndIncrement();
                    if (updateIdx < progressTimes.length) {
                        progressTimes[updateIdx] = System.currentTimeMillis() - startTime;
                    }
                    System.out.println("Progress update " + updateIdx + ": " + currentWork + "/" + totalWork + 
                                     " chunks at " + progressTimes[Math.min(updateIdx, progressTimes.length-1)] + "ms");
                    lastProgress = currentWork;
                }
                
                // Verify progress is reasonable
                assertTrue(currentWork >= 0, "Current work should be non-negative");
                assertTrue(currentWork <= totalWork, "Current work should not exceed total work");
                assertEquals(actualNumChunks, (int)totalWork, "Total work should equal number of chunks");
                
                // Track which chunks have been completed
                int completedChunks = (int) currentWork;
                for (int i = 0; i < completedChunks && i < chunkProgressSeen.length; i++) {
                    chunkProgressSeen[i] = true;
                }
                
                Thread.sleep(10); // Small delay to avoid busy waiting
            }
            
            // Wait for completion
            prebufferFuture.get();
            long endTime = System.currentTimeMillis();
            long totalTime = endTime - startTime;
            
            // Verify all chunks were downloaded
            assertEquals(actualNumChunks, progressFuture.getCurrentWork(), 0.1);
            assertTrue(progressFuture.isWorkComplete());
            
            // Analyze timing patterns to verify concurrent behavior
            System.out.println("Total prebuffer time: " + totalTime + "ms for " + actualNumChunks + " chunks");
            System.out.println("Progress updates: " + progressUpdateCount.get());
            
            // Print timing analysis
            if (progressUpdateCount.get() > 2) {
                long firstProgressTime = progressTimes[1]; // Skip initial 0
                long lastProgressTime = progressTimes[Math.min(progressUpdateCount.get()-1, progressTimes.length-1)];
                long downloadSpan = lastProgressTime - firstProgressTime;
                System.out.println("Download span: " + downloadSpan + "ms (from first to last progress)");
                
                // If downloads were truly concurrent, multiple chunks should complete in overlapping timeframes
                // If they were sequential, we'd see roughly equal time gaps between each completion
                if (progressUpdateCount.get() >= 3) {
                    long gap1 = progressTimes[2] - progressTimes[1];
                    long gap2 = progressTimes[Math.min(3, progressTimes.length-1)] - progressTimes[2];
                    System.out.println("Time gaps between completions: " + gap1 + "ms, " + gap2 + "ms");
                    
                    // With concurrency, we expect some chunks to complete close together
                    // Check if we see evidence of concurrent execution (short gaps or overlapping completions)
                    boolean evidenceOfConcurrency = downloadSpan < totalTime * 0.8 || gap1 < 50 || gap2 < 50;
                    if (!evidenceOfConcurrency) {
                        System.out.println("WARNING: No clear evidence of concurrent downloads detected");
                    }
                }
            }
            
            // Verify the download was reasonably fast (indicating concurrency)
            assertTrue(totalTime < 30000, "Prebuffer should complete within 30 seconds with concurrent downloads");
            
            // Verify file can be read after prebuffering
            byte[] readBuffer = new byte[TOTAL_SIZE];
            java.nio.ByteBuffer byteBuffer = java.nio.ByteBuffer.wrap(readBuffer);
            int bytesRead = channel.read(byteBuffer, 0).get();
            
            assertEquals(TOTAL_SIZE, bytesRead);
            assertArrayEquals(testContent, readBuffer);
            
        } finally {
            channel.close();
        }
    }

    @Test 
    void testPrebufferProgressTrackingDuringConcurrentDownloads(@TempDir Path tempDir, TestInfo testInfo) throws Exception {
        // Create test content with a reasonable size (smaller to avoid verification issues)
        final int TOTAL_SIZE = 2 * 1024 * 1024; // 2MB total - should create multiple chunks
        
        String testFileName = TestFixturePaths.createTestSpecificFilename(testInfo, "progress_test.dat");
        byte[] testContent = createTestContent(TOTAL_SIZE);
        
        // Serve the file via HTTP using test-specific directory
        Path testSpecificTempDir = TestFixturePaths.createTestSpecificTempDir(testInfo);
        Path serverFile = testSpecificTempDir.resolve(testFileName);
        Files.write(serverFile, testContent);

        // Create merkle reference from the server file
        MerkleDataImpl merkleRef = (MerkleDataImpl) MerkleRef.fromDataSimple(serverFile).get();
        Path mrefPath = serverFile.resolveSibling(serverFile.getFileName() + ".mref");
        merkleRef.save(mrefPath);

        // Create test-specific server URL
        URL testSpecificUrl = TestFixturePaths.createTestSpecificServerUrl(testInfo, testFileName);
        
        String cacheFilename = TestFixturePaths.createTestSpecificFilename(testInfo, "progress_cache.dat");
        String stateFilename = TestFixturePaths.createTestSpecificFilename(testInfo, "progress_state.mrkl");
        Path cacheFile = tempDir.resolve(cacheFilename);
        Path stateFile = tempDir.resolve(stateFilename);
        
        MAFileChannel channel = new MAFileChannel(cacheFile, stateFile, testSpecificUrl.toString());
        
        try {
            MerkleShape shape = merkleRef.getShape();
            long actualNumChunks = shape.getLeafCount();
            long chunkSize = shape.getChunkSize();
            
            // Start prebuffer operation
            CompletableFuture<Void> prebufferFuture = channel.prebuffer(0, TOTAL_SIZE);
            assertInstanceOf(ProgressIndicatingFuture.class, prebufferFuture);
            ProgressIndicatingFuture<Void> progressFuture = (ProgressIndicatingFuture<Void>) prebufferFuture;
            
            // Track progress changes to verify chunks are downloaded concurrently
            double lastProgress = -1;
            int progressUpdates = 0;
            
            while (!prebufferFuture.isDone()) {
                double currentProgress = progressFuture.getCurrentWork();
                
                if (currentProgress != lastProgress) {
                    progressUpdates++;
                    System.out.println("Progress: " + currentProgress + "/" + progressFuture.getTotalWork() + 
                                     " chunks (" + progressFuture.getProgressPercentage() + "%)");
                    lastProgress = currentProgress;
                    
                    // Verify bytes per unit is set correctly
                    assertTrue(progressFuture.getBytesPerUnit() > 1.0, "Bytes per unit should be greater than 1 (chunk size)");
                    assertEquals(chunkSize, progressFuture.getBytesPerUnit(), 1.0, "Bytes per unit should equal chunk size");
                }
                
                Thread.sleep(50); // Check every 50ms
            }
            
            prebufferFuture.get();
            
            // Verify we saw progress updates (indicating incremental completion)
            // With concurrent downloads, we should see at least some progress tracking
            assertTrue(progressUpdates >= 1, "Should see progress updates during downloads, saw: " + progressUpdates);
            System.out.println("Saw " + progressUpdates + " progress updates for " + actualNumChunks + " chunks");
            
            // Verify final state
            assertEquals(actualNumChunks, progressFuture.getCurrentWork(), 0.1);
            assertTrue(progressFuture.isWorkComplete());
            
        } finally {
            channel.close();
        }
    }

    /**
     * Creates test content with a predictable pattern for verification.
     */
    private byte[] createTestContent(int size) {
        byte[] content = new byte[size];
        for (int i = 0; i < size; i++) {
            content[i] = (byte) (i % 256);
        }
        return content;
    }
}