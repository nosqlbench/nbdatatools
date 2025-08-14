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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

///
/// Simple test to validate the redesigned prebuffer method that properly calculates
/// minimum node coverage and uses the existing download infrastructure.
///
@ExtendWith(JettyFileServerExtension.class)
class PrebufferSimpleTest {

    @Test
    void testSimplePrebuffer(@TempDir Path tempDir) throws IOException, ExecutionException, InterruptedException {
        // Create a 3MB test file that will result in 3 chunks
        int fileSize = 3 * 1024 * 1024; // 3MB
        Path sourceFile = createTestFile(tempDir, "prebuffer_simple.dat", fileSize);
        
        // Serve file via HTTP
        String uniqueId = String.valueOf(System.currentTimeMillis());
        Path serverDir = JettyFileServerExtension.TEMP_RESOURCES_ROOT.resolve("prebuffer_simple_" + uniqueId);
        Files.createDirectories(serverDir);
        Path serverFile = serverDir.resolve("prebuffer_simple.dat");
        Files.copy(sourceFile, serverFile);
        
        // Create merkle reference
        MerkleDataImpl merkleRef = (MerkleDataImpl) MerkleRef.fromDataSimple(serverFile).get();
        Path mrefPath = serverFile.resolveSibling(serverFile.getFileName() + ".mref");
        merkleRef.save(mrefPath);
        
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        String remoteUrl = baseUrl + "temp/prebuffer_simple_" + uniqueId + "/prebuffer_simple.dat";
        
        Path localCache = tempDir.resolve("cache.dat");
        Path merkleState = tempDir.resolve("state.mrkl");
        
        try (MAFileChannel channel = new MAFileChannel(localCache, merkleState, remoteUrl)) {
            MerkleShape shape = merkleRef.getShape();
            
            System.out.println("=== Simple Prebuffer Test ===");
            System.out.printf("File size: %d bytes%n", fileSize);
            System.out.printf("Chunk size: %d%n", shape.getChunkSize());
            System.out.printf("Total chunks: %d%n", shape.getLeafCount());
            
            // Test 1: Prebuffer a single chunk
            System.out.println("Testing single chunk prebuffer...");
            long chunkPosition = shape.getChunkSize(); // Position for chunk 1
            int chunkLength = 1024; // Small amount within the chunk
            
            CompletableFuture<Void> prebufferFuture = channel.prebuffer(chunkPosition, chunkLength);
            prebufferFuture.get(); // Wait for completion
            
            System.out.println("✓ Single chunk prebuffer completed");
            
            // Verify we can read from the prebuffered range
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            int bytesRead = channel.read(buffer, chunkPosition).get();
            assertTrue(bytesRead > 0, "Should be able to read from prebuffered chunk");
            System.out.printf("✓ Successfully read %d bytes from prebuffered chunk%n", bytesRead);
            
            // Test 2: Prebuffer a range spanning multiple chunks
            System.out.println("Testing multi-chunk prebuffer...");
            long spanStart = shape.getChunkSize() - 512; // Start in chunk 1
            int spanLength = (int) (shape.getChunkSize() + 1024); // Span into chunk 2
            
            CompletableFuture<Void> spanFuture = channel.prebuffer(spanStart, spanLength);
            spanFuture.get();
            
            System.out.println("✓ Multi-chunk prebuffer completed");
            
            // Verify both chunks are accessible
            buffer.clear();
            bytesRead = channel.read(buffer, spanStart).get();
            assertTrue(bytesRead > 0, "Should be able to read from span start");
            
            buffer.clear();
            bytesRead = channel.read(buffer, spanStart + spanLength - 100).get();
            assertTrue(bytesRead > 0, "Should be able to read from span end");
            
            System.out.println("✓ Multi-chunk span reading successful");
            
            // Test 3: Prebuffer already downloaded data (should complete immediately)
            System.out.println("Testing prebuffer of already valid data...");
            long start = System.nanoTime();
            CompletableFuture<Void> cachedFuture = channel.prebuffer(chunkPosition, chunkLength);
            cachedFuture.get();
            long duration = System.nanoTime() - start;
            
            System.out.printf("✓ Cached prebuffer completed in %d ns%n", duration);
            assertTrue(duration < 1_000_000, "Prebuffer of cached data should be very fast");
            
            System.out.println("✅ All prebuffer tests passed");
        }
    }
    
    @Test
    void testPrebufferBoundaryConditions(@TempDir Path tempDir) throws IOException, ExecutionException, InterruptedException {
        // Create a file size that creates boundary conditions
        int fileSize = 2_500_000; // 2.5MB - not evenly divisible by chunk size
        Path sourceFile = createTestFile(tempDir, "boundary.dat", fileSize);
        
        // Serve file via HTTP
        String uniqueId = String.valueOf(System.currentTimeMillis());
        Path serverDir = JettyFileServerExtension.TEMP_RESOURCES_ROOT.resolve("boundary_" + uniqueId);
        Files.createDirectories(serverDir);
        Path serverFile = serverDir.resolve("boundary.dat");
        Files.copy(sourceFile, serverFile);
        
        // Create merkle reference
        MerkleDataImpl merkleRef = (MerkleDataImpl) MerkleRef.fromDataSimple(serverFile).get();
        Path mrefPath = serverFile.resolveSibling(serverFile.getFileName() + ".mref");
        merkleRef.save(mrefPath);
        
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        String remoteUrl = baseUrl + "temp/boundary_" + uniqueId + "/boundary.dat";
        
        Path localCache = tempDir.resolve("boundary_cache.dat");
        Path merkleState = tempDir.resolve("boundary_state.mrkl");
        
        try (MAFileChannel channel = new MAFileChannel(localCache, merkleState, remoteUrl)) {
            MerkleShape shape = merkleRef.getShape();
            
            System.out.println("=== Boundary Conditions Test ===");
            System.out.printf("File size: %d bytes%n", fileSize);
            System.out.printf("Total chunks: %d%n", shape.getLeafCount());
            
            // Test prebuffering the last partial chunk
            long lastChunkStart = (shape.getLeafCount() - 1) * shape.getChunkSize();
            int lastChunkSize = (int) (fileSize - lastChunkStart);
            
            System.out.printf("Prebuffering last chunk: start=%d, size=%d%n", lastChunkStart, lastChunkSize);
            
            CompletableFuture<Void> lastChunkFuture = channel.prebuffer(lastChunkStart, lastChunkSize);
            lastChunkFuture.get();
            
            System.out.println("✓ Last chunk prebuffer completed");
            
            // Verify we can read from the end of the file
            ByteBuffer buffer = ByteBuffer.allocate(100);
            int bytesRead = channel.read(buffer, fileSize - 100).get();
            assertTrue(bytesRead > 0, "Should be able to read from end of file");
            System.out.printf("✓ Successfully read %d bytes from end of file%n", bytesRead);
            
            // Test prebuffering exactly to end of file
            CompletableFuture<Void> endFuture = channel.prebuffer(fileSize - 1000, 1000);
            endFuture.get();
            
            System.out.println("✓ End-of-file prebuffer completed");
            
            System.out.println("✅ All boundary condition tests passed");
        }
    }

    private Path createTestFile(Path dir, String name, int size) throws IOException {
        Path file = dir.resolve(name);
        byte[] data = new byte[size];
        Random random = new Random(12345); // Fixed seed for reproducible test data
        random.nextBytes(data);
        Files.write(file, data);
        return file;
    }
}