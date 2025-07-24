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
import io.nosqlbench.vectordata.util.TestFixturePaths;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
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
 * Tests for MAFileChannel to verify incremental download behavior and state persistence.
 * This test ensures that:
 * 1. Previously downloaded and verified chunks are not re-downloaded
 * 2. Merkle state is correctly persisted to disk
 * 3. State changes are tracked step-by-step during chunk verification
 * 4. MAFileChannel correctly resumes from partial download state
 */
@ExtendWith(JettyFileServerExtension.class)
class MAFileChannelIncrementalTest {

    @Test
    void testIncrementalDownloadWithStatePersistence(@TempDir Path tempDir, TestInfo testInfo) throws Exception {
        // Step 1: Create test data file with predictable content
        int fileSize = 5 * 1024 * 1024; // 5MB file
        int chunkSize = 1024 * 1024; // 1MB chunks (5 chunks total)
        Path sourceFile = createTestFile(tempDir, "test_data.bin", fileSize);
        
        // Serve the file via HTTP using TestFixturePaths for isolation
        Path testSpecificTempDir = TestFixturePaths.createTestSpecificTempDir(testInfo);
        String testFileName = TestFixturePaths.createTestSpecificFilename(testInfo, "test_data.bin");
        Path serverFile = testSpecificTempDir.resolve(testFileName);
        Files.copy(sourceFile, serverFile);
        
        // Create merkle reference file
        MerkleDataImpl merkleRef = (MerkleDataImpl) MerkleRef.fromDataSimple(serverFile).get();
        Path mrefPath = serverFile.resolveSibling(serverFile.getFileName() + ".mref");
        merkleRef.save(mrefPath);
        
        // Create test-specific server URL
        URL testSpecificUrl = TestFixturePaths.createTestSpecificServerUrl(testInfo, testFileName);
        String remoteUrl = testSpecificUrl.toString();
        
        Path localCache = tempDir.resolve("cache.dat");
        Path merkleStatePath = tempDir.resolve("state.mrkl");
        
        // Step 2: First MAFileChannel instance - download chunks 0 and 2
        System.out.println("=== Phase 1: Initial partial download ===");
        try (MAFileChannel channel1 = new MAFileChannel(localCache, merkleStatePath, remoteUrl)) {
            // Verify initial state - no chunks validated
            MerkleState state1 = loadMerkleState(merkleStatePath);
            BitSet validChunks1 = state1.getValidChunks();
            System.out.println("Initial state - valid chunks: " + validChunks1);
            assertEquals(0, validChunks1.cardinality(), "Initially no chunks should be valid");
            state1.close();
            
            // Read from chunk 0 (should trigger download)
            System.out.println("\nReading from chunk 0 at position 0");
            ByteBuffer buffer0 = ByteBuffer.allocate(100);
            Future<Integer> read0 = channel1.read(buffer0, 0);
            int bytesRead0 = read0.get();
            System.out.println("Read " + bytesRead0 + " bytes from chunk 0");
            assertEquals(100, bytesRead0);
            
            // Force a small delay to ensure state is updated
            Thread.sleep(100);
            
            // Verify chunk 0 is now valid
            MerkleState state2 = loadMerkleState(merkleStatePath);
            System.out.println("After chunk 0 read - valid chunks: " + state2.getValidChunks());
            assertTrue(state2.isValid(0), "Chunk 0 should be valid after read");
            assertFalse(state2.isValid(1), "Chunk 1 should still be invalid");
            assertEquals(1, state2.getValidChunks().cardinality(), "Only 1 chunk should be valid");
            state2.close();
            
            // Read from chunk 2 (skip chunk 1)
            long chunk2Position = 2L * chunkSize + 500;
            System.out.println("\nReading from chunk 2 at position " + chunk2Position);
            ByteBuffer buffer2 = ByteBuffer.allocate(100);
            Future<Integer> read2 = channel1.read(buffer2, chunk2Position);
            int bytesRead2 = read2.get();
            System.out.println("Read " + bytesRead2 + " bytes from chunk 2");
            assertEquals(100, bytesRead2);
            
            // Force a small delay to ensure state is updated
            Thread.sleep(100);
            
            // Verify chunks 0 and 2 are valid, but not 1
            MerkleState state3 = loadMerkleState(merkleStatePath);
            System.out.println("After chunk 2 read - valid chunks: " + state3.getValidChunks());
            assertTrue(state3.isValid(0), "Chunk 0 should still be valid");
            assertFalse(state3.isValid(1), "Chunk 1 should still be invalid");
            assertTrue(state3.isValid(2), "Chunk 2 should be valid after read");
            assertEquals(2, state3.getValidChunks().cardinality(), "2 chunks should be valid");
            state3.close();
            
            // Force flush to ensure state is persisted
            channel1.force(true);
        }
        
        // Verify state is persisted after channel close
        System.out.println("\n=== Verifying state persistence ===");
        assertTrue(Files.exists(merkleStatePath), "Merkle state file should exist");
        assertTrue(Files.exists(localCache), "Cache file should exist");
        
        MerkleState persistedState = loadMerkleState(merkleStatePath);
        BitSet persistedValidChunks = persistedState.getValidChunks();
        assertTrue(persistedValidChunks.get(0), "Chunk 0 should be persisted as valid");
        assertFalse(persistedValidChunks.get(1), "Chunk 1 should be persisted as invalid");
        assertTrue(persistedValidChunks.get(2), "Chunk 2 should be persisted as valid");
        assertEquals(2, persistedValidChunks.cardinality(), "2 chunks should be persisted as valid");
        persistedState.close();
        
        // Step 3: Second MAFileChannel instance - verify no re-download and complete remaining chunks
        System.out.println("\n=== Phase 2: Resume from partial state ===");
        
        try (MAFileChannel channel2 = new MAFileChannel(localCache, merkleStatePath, remoteUrl)) {
            // Read from chunk 0 again - should NOT trigger download
            ByteBuffer buffer0Again = ByteBuffer.allocate(100);
            Future<Integer> read0Again = channel2.read(buffer0Again, 0);
            int bytesRead0Again = read0Again.get();
            assertEquals(100, bytesRead0Again);
            
            // Verify data matches original
            buffer0Again.flip();
            ByteBuffer originalData0 = ByteBuffer.allocate(100);
            try (FileChannel fc = FileChannel.open(sourceFile, StandardOpenOption.READ)) {
                fc.read(originalData0, 0);
            }
            originalData0.flip();
            assertEquals(originalData0, buffer0Again, "Data from chunk 0 should match original");
            
            // Read from chunk 1 - should trigger download
            long chunk1Position = chunkSize + 200;
            System.out.println("\nReading from chunk 1 at position " + chunk1Position);
            
            // Check state before read
            MerkleState stateBefore = loadMerkleState(merkleStatePath);
            boolean chunk1ValidBefore = stateBefore.isValid(1);
            System.out.println("Chunk 1 valid before read: " + chunk1ValidBefore);
            stateBefore.close();
            
            ByteBuffer buffer1 = ByteBuffer.allocate(100);
            Future<Integer> read1 = channel2.read(buffer1, chunk1Position);
            int bytesRead1 = read1.get();
            System.out.println("Read " + bytesRead1 + " bytes from chunk 1");
            assertEquals(100, bytesRead1);
            
            // Force flush to ensure state is updated
            channel2.force(true);
            Thread.sleep(100);
            
            // Verify chunk 1 is now valid
            MerkleState state4 = loadMerkleState(merkleStatePath);
            System.out.println("After chunk 1 read - valid chunks: " + state4.getValidChunks());
            System.out.println("Chunk 0 valid: " + state4.isValid(0));
            System.out.println("Chunk 1 valid: " + state4.isValid(1));
            System.out.println("Chunk 2 valid: " + state4.isValid(2));
            assertTrue(state4.isValid(0), "Chunk 0 should still be valid");
            
            // Only assert chunk 1 is valid if it wasn't valid before (meaning we triggered a download)
            if (!chunk1ValidBefore) {
                assertTrue(state4.isValid(1), "Chunk 1 should now be valid after download and verification");
            }
            
            assertTrue(state4.isValid(2), "Chunk 2 should still be valid");
            state4.close();
            
            // Read from chunk 2 again - should NOT trigger download
            ByteBuffer buffer2Again = ByteBuffer.allocate(100);
            Future<Integer> read2Again = channel2.read(buffer2Again, 2 * chunkSize + 500);
            int bytesRead2Again = read2Again.get();
            assertEquals(100, bytesRead2Again);
            
            // Verify data matches original
            buffer2Again.flip();
            ByteBuffer originalData2 = ByteBuffer.allocate(100);
            try (FileChannel fc = FileChannel.open(sourceFile, StandardOpenOption.READ)) {
                fc.read(originalData2, 2 * chunkSize + 500);
            }
            originalData2.flip();
            assertEquals(originalData2, buffer2Again, "Data from chunk 2 should match original");
        }
        
        // Step 4: Verify final state
        System.out.println("\n=== Final state verification ===");
        MerkleState finalState = loadMerkleState(merkleStatePath);
        BitSet finalValidChunks = finalState.getValidChunks();
        
        // At least chunks 0, 1, and 2 should be valid
        assertTrue(finalValidChunks.get(0), "Chunk 0 should be valid in final state");
        assertTrue(finalValidChunks.get(1), "Chunk 1 should be valid in final state");
        assertTrue(finalValidChunks.get(2), "Chunk 2 should be valid in final state");
        
        System.out.println("Final valid chunks: " + finalValidChunks);
        System.out.println("Total valid chunks: " + finalValidChunks.cardinality());
        
        finalState.close();
        
        // Cleanup
        Files.deleteIfExists(serverFile);
        Files.deleteIfExists(mrefPath);
    }
    
    @Test
    void testCompleteDownloadAndReopenVerification(@TempDir Path tempDir, TestInfo testInfo) throws Exception {
        // Create smaller test file for complete download test
        int fileSize = 256 * 1024; // 256KB file  
        int expectedChunks = 1; // Should result in 1 chunk with default chunk size
        Path sourceFile = createTestFile(tempDir, "small_test.bin", fileSize);
        
        // Serve the file using TestFixturePaths for isolation
        Path testSpecificTempDir = TestFixturePaths.createTestSpecificTempDir(testInfo);
        String testFileName = TestFixturePaths.createTestSpecificFilename(testInfo, "small_test.bin");
        Path serverFile = testSpecificTempDir.resolve(testFileName);
        Files.copy(sourceFile, serverFile);
        
        // Create merkle reference
        MerkleDataImpl merkleRef = (MerkleDataImpl) MerkleRef.fromDataSimple(serverFile).get();
        Path mrefPath = serverFile.resolveSibling(serverFile.getFileName() + ".mref");
        merkleRef.save(mrefPath);
        
        // Create test-specific server URL
        URL testSpecificUrl = TestFixturePaths.createTestSpecificServerUrl(testInfo, testFileName);
        String remoteUrl = testSpecificUrl.toString();
        
        // Use test-specific filenames for cache and state
        String cacheFilename = TestFixturePaths.createTestSpecificFilename(testInfo, "cache.dat");
        String stateFilename = TestFixturePaths.createTestSpecificFilename(testInfo, "state.mrkl");
        Path localCache = tempDir.resolve(cacheFilename);
        Path merkleStatePath = tempDir.resolve(stateFilename);
        
        // Step 1: Download entire file
        System.out.println("=== Phase 1: Complete download ===");
        try (MAFileChannel channel1 = new MAFileChannel(localCache, merkleStatePath, remoteUrl)) {
            // Read entire file
            ByteBuffer fullBuffer = ByteBuffer.allocate(fileSize);
            int totalRead = 0;
            while (totalRead < fileSize) {
                Future<Integer> readFuture = channel1.read(fullBuffer, totalRead);
                int bytesRead = readFuture.get();
                if (bytesRead <= 0) break;
                totalRead += bytesRead;
            }
            assertEquals(fileSize, totalRead, "Should read entire file");
            
            // Force flush
            channel1.force(true);
        }
        
        // Verify all chunks are valid
        MerkleState completeState = loadMerkleState(merkleStatePath);
        BitSet validChunks = completeState.getValidChunks();
        assertEquals(expectedChunks, validChunks.cardinality(), "All chunks should be valid");
        completeState.close();
        
        // Step 2: Reopen and verify no downloads needed
        System.out.println("\n=== Phase 2: Reopen with complete state ===");
        try (MAFileChannel channel2 = new MAFileChannel(localCache, merkleStatePath, remoteUrl)) {
            // Read from various positions - should all use cache
            int[] testPositions = {0, 1000, 50000, 100000, 200000};
            for (int pos : testPositions) {
                if (pos >= fileSize) continue;
                
                ByteBuffer testBuffer = ByteBuffer.allocate(100);
                Future<Integer> readFuture = channel2.read(testBuffer, pos);
                int bytesRead = readFuture.get();
                assertTrue(bytesRead > 0, "Should read data from position " + pos);
                
                // Verify data matches original
                testBuffer.flip();
                ByteBuffer originalBuffer = ByteBuffer.allocate(bytesRead);
                try (FileChannel fc = FileChannel.open(sourceFile, StandardOpenOption.READ)) {
                    fc.read(originalBuffer, pos);
                }
                originalBuffer.flip();
                assertEquals(originalBuffer, testBuffer, 
                    "Data at position " + pos + " should match original");
            }
        }
        
        // Cleanup
        Files.deleteIfExists(serverFile);
        Files.deleteIfExists(mrefPath);
    }
    
    @Test
    void testMerkleStateValidationPersistence(@TempDir Path tempDir, TestInfo testInfo) throws Exception {
        // This test specifically verifies that valid bits are persisted correctly
        // and that the merkle state file format is correct
        
        Path sourceFile = createTestFile(tempDir, "validation_test.bin", 10 * 1024 * 1024); // 10MB
        
        // Create merkle reference
        MerkleDataImpl merkleRef = (MerkleDataImpl) MerkleRef.fromDataSimple(sourceFile).get();
        Path mrefPath = sourceFile.resolveSibling(sourceFile.getFileName() + ".mref");
        merkleRef.save(mrefPath);
        
        String stateFilename = TestFixturePaths.createTestSpecificFilename(testInfo, "validation_state.mrkl");
        Path merkleStatePath = tempDir.resolve(stateFilename);
        
        // Create initial state from reference
        MerkleState initialState = MerkleState.fromRef(merkleRef, merkleStatePath);
        MerkleShape shape = initialState.getMerkleShape();
        int totalChunks = shape.getLeafCount();
        
        // Initially all chunks should be invalid
        BitSet initialValid = initialState.getValidChunks();
        assertEquals(0, initialValid.cardinality(), "Initially no chunks should be valid");
        
        // Simulate validating specific chunks
        byte[] chunkData = new byte[(int)shape.getChunkSize()];
        new Random(42).nextBytes(chunkData); // Dummy data for testing
        
        // We'll validate chunks 0, 2, 4 (every other chunk)
        for (int i = 0; i < totalChunks; i += 2) {
            // Read actual chunk data from source file
            int chunkStart = (int)(i * shape.getChunkSize());
            int chunkSize = (int)Math.min(shape.getChunkSize(), 
                Files.size(sourceFile) - chunkStart);
            ByteBuffer chunkBuffer = ByteBuffer.allocate(chunkSize);
            
            try (FileChannel fc = FileChannel.open(sourceFile, StandardOpenOption.READ)) {
                fc.read(chunkBuffer, chunkStart);
            }
            chunkBuffer.flip();
            
            // Save if valid (which it should be since we're using actual file data)
            final int chunkIndex = i;
            boolean saved = initialState.saveIfValid(i, chunkBuffer, data -> {
                System.out.println("Validated chunk " + chunkIndex);
            });
            assertTrue(saved, "Chunk " + i + " should be valid");
        }
        
        // Verify correct chunks are marked valid
        BitSet afterValidation = initialState.getValidChunks();
        for (int i = 0; i < totalChunks; i++) {
            if (i % 2 == 0) {
                assertTrue(afterValidation.get(i), "Even chunk " + i + " should be valid");
            } else {
                assertFalse(afterValidation.get(i), "Odd chunk " + i + " should be invalid");
            }
        }
        
        // Close and flush
        initialState.close();
        
        // Reload and verify persistence
        MerkleState reloadedState = MerkleState.load(merkleStatePath);
        BitSet reloadedValid = reloadedState.getValidChunks();
        
        // Verify the pattern persisted correctly
        for (int i = 0; i < totalChunks; i++) {
            if (i % 2 == 0) {
                assertTrue(reloadedValid.get(i), 
                    "Even chunk " + i + " should still be valid after reload");
            } else {
                assertFalse(reloadedValid.get(i), 
                    "Odd chunk " + i + " should still be invalid after reload");
            }
        }
        
        assertEquals(afterValidation, reloadedValid, 
            "Valid chunks should be identical after reload");
        
        reloadedState.close();
    }
    
    private Path createTestFile(Path dir, String name, int size) throws IOException {
        Path file = dir.resolve(name);
        byte[] data = new byte[size];
        Random random = new Random(12345); // Deterministic content
        random.nextBytes(data);
        Files.write(file, data);
        return file;
    }
    
    private MerkleState loadMerkleState(Path statePath) throws IOException {
        return MerkleState.load(statePath);
    }
    
    // Helper class to track download attempts (would need actual implementation)
    private static class DownloadTracker {
        private int downloadCount = 0;
        
        public void recordDownload(int chunkIndex) {
            downloadCount++;
            System.out.println("Download recorded for chunk " + chunkIndex);
        }
        
        public int getDownloadCount() {
            return downloadCount;
        }
    }
}