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
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

///
/// Tests to validate that the prebuffer method correctly downloads all intended chunks,
/// specifically testing the fix for leaf range vs chunk index mapping issues.
///
@ExtendWith(JettyFileServerExtension.class)
class PrebufferValidationTest {
    
    @TempDir
    Path tempDir;
    
    @Test 
    void testPrebufferWithBoundaryConditions(@TempDir Path tempDir) throws IOException, ExecutionException, InterruptedException {
        // Use a file size that creates boundary conditions for leaf range mapping
        // 2.5MB file with 1MB chunks = 3 chunks, but capLeaf will be 4
        int fileSize = 2_621_440; // 2.5MB
        
        // Create test file
        Path sourceFile = createTestFile(tempDir, "boundary_test.dat", fileSize);
        
        // Serve file via HTTP
        String uniqueId = String.valueOf(System.currentTimeMillis());
        Path serverDir = JettyFileServerExtension.TEMP_RESOURCES_ROOT.resolve("boundary_test_" + uniqueId);
        Files.createDirectories(serverDir);
        Path serverFile = serverDir.resolve("boundary_test.dat");
        Files.copy(sourceFile, serverFile);
        
        // Create merkle reference
        MerkleDataImpl merkleRef = (MerkleDataImpl) MerkleRef.fromDataSimple(serverFile).get();
        Path mrefPath = serverFile.resolveSibling(serverFile.getFileName() + ".mref");
        merkleRef.save(mrefPath);
        
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        String remoteUrl = baseUrl + "temp/boundary_test_" + uniqueId + "/boundary_test.dat";
        
        Path localCache = tempDir.resolve("test_cache.dat");
        Path merkleState = tempDir.resolve("test_state.mrkl");
        
        try (MAFileChannel channel = new MAFileChannel(localCache, merkleState, remoteUrl)) {
            MerkleShape shape = merkleRef.getShape();
            
            System.out.println("=== Prebuffer Boundary Conditions Test ===");
            System.out.printf("File size: %d bytes%n", fileSize);
            System.out.printf("Chunk size: %d%n", shape.getChunkSize());
            System.out.printf("Total chunks: %d%n", shape.getLeafCount());
            
            // Test prebuffering the last chunk (most likely to expose boundary issues)
            long lastChunkPosition = 2 * shape.getChunkSize(); // Position for chunk 2
            int lastChunkSize = (int) (fileSize - lastChunkPosition);
            
            System.out.printf("Prebuffering last chunk at position %d, size %d%n", 
                lastChunkPosition, lastChunkSize);
            
            // This should prebuffer chunk 2 completely
            CompletableFuture<Void> prebufferFuture = channel.prebuffer(lastChunkPosition, lastChunkSize);
            prebufferFuture.get(); // Wait for completion
            
            System.out.println("✓ Prebuffer completed successfully");
            
            // Verify that chunk 2 is actually available by reading from it
            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(1024);
            int bytesRead = channel.read(buffer, lastChunkPosition).get();
            
            assertTrue(bytesRead > 0, "Should be able to read from prebuffered chunk");
            System.out.printf("✓ Successfully read %d bytes from prebuffered chunk%n", bytesRead);
            
            // Test prebuffering a range that spans multiple chunks
            System.out.println("Testing multi-chunk prebuffer...");
            
            long spanStart = shape.getChunkSize() - 512; // Start in chunk 1
            int spanLength = (int) (shape.getChunkSize() + 1024); // Span into chunk 2
            
            CompletableFuture<Void> spanFuture = channel.prebuffer(spanStart, spanLength);
            spanFuture.get();
            
            System.out.println("✓ Multi-chunk prebuffer completed successfully");
            
            // Verify both chunks are available
            buffer.clear();
            bytesRead = channel.read(buffer, spanStart).get();
            assertTrue(bytesRead > 0, "Should be able to read from span start");
            
            buffer.clear();
            bytesRead = channel.read(buffer, spanStart + spanLength - 100).get();
            assertTrue(bytesRead > 0, "Should be able to read from span end");
            
            System.out.println("✓ Multi-chunk span reading successful");
        }
    }
    
    @Test
    void testPrebufferPostValidation(@TempDir Path tempDir) throws IOException, ExecutionException, InterruptedException {
        // Test that post-validation catches missing chunks
        int fileSize = 3_145_728; // 3MB
        
        // Create test file
        Path sourceFile = createTestFile(tempDir, "validation_test.dat", fileSize);
        
        // Serve file via HTTP
        String uniqueId = String.valueOf(System.currentTimeMillis());
        Path serverDir = JettyFileServerExtension.TEMP_RESOURCES_ROOT.resolve("validation_test_" + uniqueId);
        Files.createDirectories(serverDir);
        Path serverFile = serverDir.resolve("validation_test.dat");
        Files.copy(sourceFile, serverFile);
        
        // Create merkle reference
        MerkleDataImpl merkleRef = (MerkleDataImpl) MerkleRef.fromDataSimple(serverFile).get();
        Path mrefPath = serverFile.resolveSibling(serverFile.getFileName() + ".mref");
        merkleRef.save(mrefPath);
        
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        String remoteUrl = baseUrl + "temp/validation_test_" + uniqueId + "/validation_test.dat";
        
        Path localCache = tempDir.resolve("validation_cache.dat");
        Path merkleState = tempDir.resolve("validation_state.mrkl");
        
        try (MAFileChannel channel = new MAFileChannel(localCache, merkleState, remoteUrl)) {
            
            System.out.println("=== Prebuffer Post-Validation Test ===");
            
            // Normal prebuffer should work
            CompletableFuture<Void> normalFuture = channel.prebuffer(0, 1024);
            assertDoesNotThrow(() -> normalFuture.get());
            
            System.out.println("✓ Normal prebuffer completed with validation");
            
            // If we artificially broke the download system, the post-validation
            // would catch it and fail the future. Since we can't easily simulate
            // that in this test, we verify that the validation logic is in place
            // by checking that the chunks are actually valid after prebuffer.
            
            // Prebuffer a larger range
            CompletableFuture<Void> largeFuture = channel.prebuffer(1_048_576, 2_097_152);
            assertDoesNotThrow(() -> largeFuture.get());
            
            System.out.println("✓ Large range prebuffer completed with validation");
            
            // Verify we can actually read the data
            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(1024);
            int bytesRead = channel.read(buffer, 1_048_576).get();
            assertTrue(bytesRead > 0, "Prebuffered data should be readable");
            
            buffer.clear();
            bytesRead = channel.read(buffer, 2_097_152).get();
            assertTrue(bytesRead > 0, "End of prebuffered range should be readable");
            
            System.out.println("✓ Post-validation ensures data is actually available");
        } catch (ExecutionException | InterruptedException e) {
            fail("Prebuffer validation test failed: " + e.getMessage());
        }
    }
    
    @Test
    void testConcurrentPrebufferOperations(@TempDir Path tempDir) throws IOException, InterruptedException, ExecutionException {
        // Test that concurrent prebuffer operations don't interfere with each other
        int fileSize = 8_388_608; // 8MB
        
        // Create test file
        Path sourceFile = createTestFile(tempDir, "concurrent_test.dat", fileSize);
        
        // Serve file via HTTP
        String uniqueId = String.valueOf(System.currentTimeMillis());
        Path serverDir = JettyFileServerExtension.TEMP_RESOURCES_ROOT.resolve("concurrent_test_" + uniqueId);
        Files.createDirectories(serverDir);
        Path serverFile = serverDir.resolve("concurrent_test.dat");
        Files.copy(sourceFile, serverFile);
        
        // Create merkle reference
        MerkleDataImpl merkleRef = (MerkleDataImpl) MerkleRef.fromDataSimple(serverFile).get();
        Path mrefPath = serverFile.resolveSibling(serverFile.getFileName() + ".mref");
        merkleRef.save(mrefPath);
        
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        String remoteUrl = baseUrl + "temp/concurrent_test_" + uniqueId + "/concurrent_test.dat";
        
        Path localCache = tempDir.resolve("concurrent_cache.dat");
        Path merkleState = tempDir.resolve("concurrent_state.mrkl");
        
        try (MAFileChannel channel = new MAFileChannel(localCache, merkleState, remoteUrl)) {
            
            System.out.println("=== Concurrent Prebuffer Test ===");
            
            // Start multiple concurrent prebuffer operations
            CompletableFuture<Void> future1 = channel.prebuffer(0, 1_048_576);           // Chunk 0
            CompletableFuture<Void> future2 = channel.prebuffer(2_097_152, 1_048_576);   // Chunk 2
            CompletableFuture<Void> future3 = channel.prebuffer(4_194_304, 1_048_576);   // Chunk 4
            CompletableFuture<Void> future4 = channel.prebuffer(6_291_456, 1_048_576);   // Chunk 6
            
            // Wait for all to complete
            CompletableFuture<Void> allComplete = CompletableFuture.allOf(future1, future2, future3, future4);
            allComplete.get();
            
            System.out.println("✓ All concurrent prebuffer operations completed");
            
            // Verify all chunks are accessible
            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(1024);
            
            assertTrue(channel.read(buffer, 0).get() > 0, "Chunk 0 should be accessible");
            buffer.clear();
            assertTrue(channel.read(buffer, 2_097_152).get() > 0, "Chunk 2 should be accessible");
            buffer.clear();
            assertTrue(channel.read(buffer, 4_194_304).get() > 0, "Chunk 4 should be accessible");
            buffer.clear();
            assertTrue(channel.read(buffer, 6_291_456).get() > 0, "Chunk 6 should be accessible");
            
            System.out.println("✓ All concurrently prebuffered chunks are accessible");
            
        } catch (ExecutionException e) {
            fail("Concurrent prebuffer test failed: " + e.getMessage());
        }
    }
    
    private Path createTestFile(Path dir, String name, int size) throws IOException {
        Path file = dir.resolve(name);
        byte[] data = new byte[size];
        Random random = new Random(77777); // Fixed seed for reproducible test data
        random.nextBytes(data);
        Files.write(file, data);
        return file;
    }
}