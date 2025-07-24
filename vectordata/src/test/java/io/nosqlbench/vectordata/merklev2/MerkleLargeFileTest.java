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

import io.nosqlbench.vectordata.datagen.TestDataFiles;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for merklev2 with large files to ensure it handles real-world data sizes.
 */
@Tag("largedata")
public class MerkleLargeFileTest {

    @Test
    public void testLargeFileWithMerkleRef(@TempDir Path tempDir) throws Exception {
        // Create test data
        Path dataFile = getOrCreateLargeTestFile(tempDir);

        long fileSize = Files.size(dataFile);
        System.out.println("Created test file: " + dataFile + " (" + fileSize + " bytes)");

        // Create merkle reference from the data
        MerkleRefBuildProgress progress = MerkleRefFactory.fromData(dataFile);
        
        // Monitor progress
        CompletableFuture<MerkleDataImpl> future = progress.getFuture();
        while (!future.isDone()) {
            System.out.println("Progress: " + progress.getProcessedChunks() + "/" + progress.getTotalChunks() + " chunks");
            Thread.sleep(100);
        }

        MerkleDataImpl merkleRef = future.get();
        assertNotNull(merkleRef);

        // The merkle data is automatically saved during creation
        // Just flush to ensure persistence
        merkleRef.flush();

        // Verify the merkle shape
        MerkleShape shape = merkleRef.getShape();
        assertEquals(fileSize, shape.getTotalContentSize());
        System.out.println("Total chunks: " + shape.getLeafCount());
        System.out.println("Chunk size: " + shape.getChunkSize());

        // Close and verify
        merkleRef.close();
    }

    @Test
    public void testLargeFileWithMerkleState(@TempDir Path tempDir) throws Exception {
        // Test with a 50MB file
        int vectorCount = 50_000;
        int dimensions = 128;
        long seed = 123L;

        // Create test data
        Path dataFile = tempDir.resolve("state_test.fvec");
        createLargeVectorFile(dataFile, vectorCount, dimensions, seed);

        // Create merkle reference
        MerkleDataImpl merkleRef = MerkleRefFactory.fromData(dataFile).getFuture().get();
        
        // Create merkle state for tracking downloads
        Path statePath = tempDir.resolve("state_test.fvec.mrkl");
        MerkleState merkleState = MerkleState.fromRef(merkleRef, statePath);

        // Simulate downloading chunks
        int totalChunks = merkleRef.getShape().getLeafCount();
        System.out.println("Simulating download of " + totalChunks + " chunks");

        // Read the data file for chunk verification
        try (FileChannel channel = FileChannel.open(dataFile, StandardOpenOption.READ)) {
            for (int i = 0; i < Math.min(10, totalChunks); i++) { // Test first 10 chunks
                // Get chunk boundaries
                long chunkStart = i * merkleRef.getShape().getChunkSize();
                int chunkSize = (int) Math.min(
                    merkleRef.getShape().getChunkSize(),
                    channel.size() - chunkStart
                );

                // Read chunk data
                ByteBuffer chunkData = ByteBuffer.allocate(chunkSize);
                channel.read(chunkData, chunkStart);
                chunkData.flip();

                // Verify and save chunk
                final int chunkIndex = i;
                boolean saved = merkleState.saveIfValid(i, chunkData, data -> {
                    System.out.println("Chunk " + chunkIndex + " validated and saved");
                });

                assertTrue(saved, "Chunk " + i + " should be valid");
                assertTrue(merkleState.isValid(i), "Chunk " + i + " should be marked valid");
            }
        }

        // Close and reload state
        merkleState.close();
        merkleRef.close();
        
        MerkleState reloadedState = MerkleState.load(statePath);
        for (int i = 0; i < Math.min(10, totalChunks); i++) {
            assertTrue(reloadedState.isValid(i), "Chunk " + i + " should still be valid after reload");
        }
        reloadedState.close();
    }

    @Test
    public void testPerformanceWithVeryLargeFile(@TempDir Path tempDir) throws Exception {
        // Test with 1GB worth of data (simulation using smaller file)
        int vectorCount = 10_000; // Reduced for test speed
        int dimensions = 1024; // Larger dimensions
        long seed = 999L;

        Path dataFile = tempDir.resolve("perf_test.fvec");
        createLargeVectorFile(dataFile, vectorCount, dimensions, seed);

        long startTime = System.currentTimeMillis();
        
        // Create merkle reference
        MerkleDataImpl merkleRef = MerkleRefFactory.fromData(dataFile).getFuture().get();
        
        long refCreationTime = System.currentTimeMillis() - startTime;
        System.out.println("Reference creation time: " + refCreationTime + "ms");
        
        // Flush reference
        startTime = System.currentTimeMillis();
        merkleRef.flush();
        long saveTime = System.currentTimeMillis() - startTime;
        System.out.println("Reference flush time: " + saveTime + "ms");

        merkleRef.close();

        // Performance assertions
        assertTrue(refCreationTime < 30000, "Reference creation should complete within 30 seconds");
        assertTrue(saveTime < 5000, "Reference flush should complete within 5 seconds");
    }

    private void createLargeVectorFile(Path file, int vectorCount, int dimensions, long seed) throws IOException {
        createLargeVectorFileInternal(file, vectorCount, dimensions, seed);
    }

    /// Gets or creates a large test file for use in tests.
    /// This creates a 100MB file with 100,000 vectors of 128 dimensions using a fixed seed.
    /// The file is cached and reused if it already exists in the temp directory.
    ///
    /// @param tempDir The temporary directory to store the file
    /// @return Path to the large test file
    /// @throws IOException If file creation fails
    public static Path getOrCreateLargeTestFile(Path tempDir) throws IOException {
        Path dataFile = tempDir.resolve("large_test.fvec");
        
        // Check if file already exists and is the expected size
        if (Files.exists(dataFile)) {
            long expectedSize = calculateExpectedFileSize(100_000, 128);
            long actualSize = Files.size(dataFile);
            if (actualSize == expectedSize) {
                System.out.println("Reusing existing large test file: " + dataFile + " (" + actualSize + " bytes)");
                return dataFile;
            } else {
                System.out.println("Existing file size mismatch, recreating: expected=" + expectedSize + ", actual=" + actualSize);
                Files.deleteIfExists(dataFile);
            }
        }
        
        // Create new file with standard parameters
        int vectorCount = 100_000;
        int dimensions = 128;
        long seed = 42L;
        
        System.out.println("Creating large test file: " + dataFile);
        createLargeVectorFileInternal(dataFile, vectorCount, dimensions, seed);
        return dataFile;
    }

    /// Calculate the expected file size for an fvec file
    /// @param vectorCount Number of vectors
    /// @param dimensions Number of dimensions per vector
    /// @return Expected file size in bytes
    private static long calculateExpectedFileSize(int vectorCount, int dimensions) {
        // fvec format: 4 bytes for dimension count + (dimensions * 4 bytes per float)
        return vectorCount * (4 + dimensions * 4L);
    }

    /// Internal method for creating vector files (renamed to avoid confusion)
    /// @param file Output file path
    /// @param vectorCount Number of vectors to generate
    /// @param dimensions Number of dimensions per vector
    /// @param seed Random seed for reproducible data
    /// @throws IOException If file creation fails
    private static void createLargeVectorFileInternal(Path file, int vectorCount, int dimensions, long seed) throws IOException {
        System.out.println("Creating vector file with " + vectorCount + " vectors of " + dimensions + " dimensions");
        
        Random random = new Random(seed);
        float[][] vectors = new float[vectorCount][dimensions];
        
        for (int i = 0; i < vectorCount; i++) {
            for (int j = 0; j < dimensions; j++) {
                vectors[i][j] = random.nextFloat();
            }
        }

        TestDataFiles.saveToFile(vectors, file, TestDataFiles.Format.fvec);
        
        long fileSize = Files.size(file);
        System.out.println("Created file: " + file + " (" + fileSize + " bytes)");
    }
}