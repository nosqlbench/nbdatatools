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
import java.util.BitSet;
import java.util.Random;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to verify the correct MAFileChannel cache and validation behavior:
 * 
 * 1. When a chunk is first accessed, it should be downloaded from remote source
 * 2. The downloaded data should be verified against the merkle hash
 * 3. If hash matches, the data should be stored in local cache and chunk marked as valid
 * 4. Subsequent reads from the same chunk should use the cached data (no re-download)
 * 5. When reopening with existing cache and state files, valid chunks should remain valid
 */
@ExtendWith(JettyFileServerExtension.class)
class MAFileChannelCacheVerificationTest {

    @Test
    void testChunkDownloadCacheAndValidation(@TempDir Path tempDir) throws Exception {
        // Setup: Create a test file that will result in multiple chunks
        int fileSize = 3 * 1024 * 1024; // 3MB file (3 chunks with 1MB default chunk size)
        Path sourceFile = createTestFile(tempDir, "cache_test.bin", fileSize);
        
        // Serve file via HTTP
        String uniqueId = String.valueOf(System.currentTimeMillis());
        Path serverDir = JettyFileServerExtension.TEMP_RESOURCES_ROOT.resolve("cache_test_" + uniqueId);
        Files.createDirectories(serverDir);
        Path serverFile = serverDir.resolve("cache_test.bin");
        Files.copy(sourceFile, serverFile);
        
        // Create merkle reference
        MerkleDataImpl merkleRef = (MerkleDataImpl) MerkleRef.fromDataSimple(serverFile).get();
        Path mrefPath = serverFile.resolveSibling(serverFile.getFileName() + ".mref");
        merkleRef.save(mrefPath);
        
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        String remoteUrl = baseUrl + "temp/cache_test_" + uniqueId + "/cache_test.bin";
        
        Path localCache = tempDir.resolve("cache.dat");
        Path merkleStatePath = tempDir.resolve("state.mrkl");
        
        MerkleShape shape = merkleRef.getShape();
        System.out.println("=== Test Setup ===");
        System.out.println("File size: " + fileSize + " bytes");
        System.out.println("Chunk size: " + shape.getChunkSize());
        System.out.println("Total chunks: " + shape.getLeafCount());
        
        // Test 1: First access to chunk 0 (Case 1 initialization)
        System.out.println("\n=== Test 1: First access (Case 1 initialization) ===");
        try (MAFileChannel channel1 = new MAFileChannel(localCache, merkleStatePath, remoteUrl)) {
            
            // Initially, no chunks should be valid and cache should be empty or non-existent
            MerkleState initialState = MerkleState.load(merkleStatePath);
            BitSet initialValid = initialState.getValidChunks();
            System.out.println("Initial valid chunks: " + initialValid);
            assertEquals(0, initialValid.cardinality(), "No chunks should be valid initially");
            initialState.close();
            
            long initialCacheSize = Files.exists(localCache) ? Files.size(localCache) : 0;
            System.out.println("Initial cache size: " + initialCacheSize + " bytes");
            
            // Read from chunk 0 - this should trigger download, verification, and caching
            System.out.println("\nAccessing chunk 0...");
            ByteBuffer buffer = ByteBuffer.allocate(1000);
            Future<Integer> readFuture = channel1.read(buffer, 500); // Read from middle of chunk 0
            int bytesRead = readFuture.get();
            assertTrue(bytesRead > 0, "Should successfully read data");
            
            // Force state persistence
            channel1.force(true);
            Thread.sleep(100); // Allow async operations to complete
            
            // Verify chunk 0 is now marked as valid (download + verification completed)
            MerkleState afterRead = MerkleState.load(merkleStatePath);
            assertTrue(afterRead.isValid(0), "Chunk 0 should be valid after download and verification");
            assertEquals(1, afterRead.getValidChunks().cardinality(), "Only chunk 0 should be valid");
            afterRead.close();
            
            // Verify cache file has grown (data was stored)
            long cacheAfterDownload = Files.size(localCache);
            System.out.println("Cache size after download: " + cacheAfterDownload + " bytes");
            assertTrue(cacheAfterDownload > initialCacheSize, "Cache should contain downloaded data");
            
            System.out.println("✓ Chunk 0 downloaded, verified, and cached");
        }
        
        // Test 2: Access same chunk from existing cache (Case 2 initialization)
        System.out.println("\n=== Test 2: Access cached chunk (Case 2 initialization) ===");
        
        // Record cache state before second access
        long cacheBeforeReopen = Files.size(localCache);
        MerkleState stateBeforeReopen = MerkleState.load(merkleStatePath);
        BitSet validBeforeReopen = stateBeforeReopen.getValidChunks();
        System.out.println("Valid chunks before reopen: " + validBeforeReopen);
        stateBeforeReopen.close();
        
        try (MAFileChannel channel2 = new MAFileChannel(localCache, merkleStatePath, remoteUrl)) {
            
            // Read the same region of chunk 0 - should use cache, not re-download
            System.out.println("Accessing chunk 0 again (should use cache)...");
            ByteBuffer buffer = ByteBuffer.allocate(1000);
            Future<Integer> readFuture = channel2.read(buffer, 500); // Same position as before
            int bytesRead = readFuture.get();
            assertTrue(bytesRead > 0, "Should successfully read cached data");
            
            // Verify no changes to cache or state (no re-download occurred)
            long cacheAfterReopen = Files.size(localCache);
            assertEquals(cacheBeforeReopen, cacheAfterReopen, "Cache size should not change (no re-download)");
            
            MerkleState stateAfterReopen = MerkleState.load(merkleStatePath);
            BitSet validAfterReopen = stateAfterReopen.getValidChunks();
            assertEquals(validBeforeReopen, validAfterReopen, "Valid chunks should not change");
            stateAfterReopen.close();
            
            System.out.println("✓ Chunk 0 accessed from cache without re-download");
        }
        
        // Test 3: Access different chunk (should trigger new download)
        System.out.println("\n=== Test 3: Access different chunk ===");
        try (MAFileChannel channel3 = new MAFileChannel(localCache, merkleStatePath, remoteUrl)) {
            
            // Access chunk 2 (skipping chunk 1)
            long chunk2Position = 2L * shape.getChunkSize() + 1000;
            System.out.println("Accessing chunk 2 at position " + chunk2Position + "...");
            
            ByteBuffer buffer = ByteBuffer.allocate(1000);
            Future<Integer> readFuture = channel3.read(buffer, chunk2Position);
            int bytesRead = readFuture.get();
            assertTrue(bytesRead > 0, "Should successfully read from chunk 2");
            
            // Force state persistence
            channel3.force(true);
            Thread.sleep(100);
            
            // Verify chunk 2 is now also valid
            MerkleState afterChunk2 = MerkleState.load(merkleStatePath);
            assertTrue(afterChunk2.isValid(0), "Chunk 0 should still be valid");
            assertFalse(afterChunk2.isValid(1), "Chunk 1 should still be invalid (not accessed)");
            assertTrue(afterChunk2.isValid(2), "Chunk 2 should now be valid");
            assertEquals(2, afterChunk2.getValidChunks().cardinality(), "2 chunks should be valid");
            afterChunk2.close();
            
            System.out.println("✓ Chunk 2 downloaded, verified, and cached");
        }
        
        // Test 4: Final verification - both cached chunks accessible
        System.out.println("\n=== Test 4: Final verification ===");
        try (MAFileChannel channel4 = new MAFileChannel(localCache, merkleStatePath, remoteUrl)) {
            
            // Access both chunks - should use cache for both
            ByteBuffer buffer1 = ByteBuffer.allocate(500);
            Future<Integer> read1 = channel4.read(buffer1, 100); // Chunk 0
            assertTrue(read1.get() > 0, "Should read from cached chunk 0");
            
            ByteBuffer buffer2 = ByteBuffer.allocate(500);
            Future<Integer> read2 = channel4.read(buffer2, 2L * shape.getChunkSize() + 500); // Chunk 2
            assertTrue(read2.get() > 0, "Should read from cached chunk 2");
            
            // Verify data consistency - read same data should match
            ByteBuffer verify1 = ByteBuffer.allocate(500);
            Future<Integer> verify1Read = channel4.read(verify1, 100);
            verify1Read.get();
            
            buffer1.flip();
            verify1.flip();
            assertEquals(buffer1, verify1, "Repeated reads should return identical data");
            
            System.out.println("✓ Both cached chunks accessible with consistent data");
        }
        
        System.out.println("\n✅ All cache verification tests passed:");
        System.out.println("  ✓ Chunks are downloaded and verified on first access");
        System.out.println("  ✓ Valid chunks are marked in persistent state");
        System.out.println("  ✓ Cached chunks are not re-downloaded");
        System.out.println("  ✓ Cache provides consistent data across accesses");
        
        // Cleanup
        Files.deleteIfExists(serverFile);
        Files.deleteIfExists(mrefPath);
        merkleRef.close();
    }
    
    @Test
    void testMerkleValidationMechanism(@TempDir Path tempDir) throws Exception {
        /*
         * This test specifically verifies that chunk validation happens
         * during the download process, not during the read process.
         */
        
        System.out.println("\n=== Merkle Validation Mechanism Test ===");
        
        // Create source file
        Path sourceFile = createTestFile(tempDir, "validation_test.bin", 2 * 1024 * 1024);
        
        // Create merkle reference
        MerkleDataImpl merkleRef = (MerkleDataImpl) MerkleRef.fromDataSimple(sourceFile).get();
        
        // Create merkle state
        Path statePath = tempDir.resolve("validation_state.mrkl");
        MerkleState state = MerkleState.fromRef(merkleRef, statePath);
        
        MerkleShape shape = state.getMerkleShape();
        System.out.println("Total chunks: " + shape.getLeafCount());
        
        // Verify initially no chunks are valid
        assertEquals(0, state.getValidChunks().cardinality(), "Initially no chunks should be valid");
        
        // Manually simulate the download and validation process for chunk 0
        System.out.println("\nSimulating download and validation for chunk 0...");
        
        // Read actual chunk data from source file
        long chunkStart = 0;
        int chunkSize = (int) Math.min(shape.getChunkSize(), Files.size(sourceFile));
        byte[] chunkData = new byte[chunkSize];
        
        try (FileChannel sourceChannel = FileChannel.open(sourceFile, StandardOpenOption.READ)) {
            ByteBuffer buffer = ByteBuffer.wrap(chunkData);
            sourceChannel.read(buffer, chunkStart);
        }
        
        // Attempt to validate and save the chunk
        boolean saved = state.saveIfValid(0, ByteBuffer.wrap(chunkData), data -> {
            System.out.println("Chunk 0 hash validation passed - data saved to cache");
        });
        
        assertTrue(saved, "Chunk 0 should be successfully validated and saved");
        assertTrue(state.isValid(0), "Chunk 0 should now be marked as valid");
        
        // Simulate validation of corrupted data
        System.out.println("\nSimulating validation with corrupted data...");
        byte[] corruptedData = new byte[chunkSize];
        new Random().nextBytes(corruptedData); // Random data that won't match hash
        
        boolean corruptedSaved = state.saveIfValid(1, ByteBuffer.wrap(corruptedData), data -> {
            fail("Save callback should not be called for corrupted data");
        });
        
        assertFalse(corruptedSaved, "Corrupted data should fail validation");
        assertFalse(state.isValid(1), "Chunk 1 should remain invalid after failed validation");
        
        System.out.println("✓ Validation mechanism working correctly:");
        System.out.println("  ✓ Valid data passes hash verification and is saved");
        System.out.println("  ✓ Invalid data fails hash verification and is rejected");
        System.out.println("  ✓ Only verified chunks are marked as valid");
        
        state.close();
        merkleRef.close();
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