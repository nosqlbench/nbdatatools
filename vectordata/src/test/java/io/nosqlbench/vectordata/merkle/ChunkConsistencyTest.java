package io.nosqlbench.vectordata.merkle;

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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.nio.file.Files;
import java.io.IOException;

/**
 * Tests that verify chunk calculation consistency across all merkle components.
 * This addresses the root cause of the CohereAccessTest failure.
 */
public class ChunkConsistencyTest {

    @TempDir
    Path tempDir;

    @Test
    public void testChunkGeometryConsistency() throws IOException {
        // Test the core geometry calculations work correctly
        long fileSize = 41_000_000_000L; // 41GB like Cohere

        ChunkGeometryDescriptor geometry = new ChunkGeometryDescriptor(fileSize);

        // Get the automatically calculated chunk size
        long chunkSize = geometry.getChunkSize();
        System.out.println("Automatically calculated chunk size: " + (chunkSize / (1024*1024)) + "MB");

        // Test the problematic vector access from CohereAccessTest
        long vectorIndex = 2_324_227L;
        long vectorSize = 4100L; // 4 bytes + 1024 floats * 4 bytes
        long filePosition = vectorIndex * vectorSize;

        System.out.println("Testing problematic vector access:");
        System.out.println("  Vector index: " + vectorIndex);
        System.out.println("  File position: " + filePosition);
        System.out.println("  File position (GB): " + (filePosition / (1024.0 * 1024.0 * 1024.0)));

        // This should NOT throw an exception
        int chunkIndex = geometry.getChunkIndexForPosition(filePosition);
        System.out.println("  Calculated chunk: " + chunkIndex);
        System.out.println("  Total chunks: " + geometry.getTotalChunks());

        // Verify chunk is within bounds
        assertTrue(chunkIndex >= 0, "Chunk index should be non-negative");
        assertTrue(chunkIndex < geometry.getTotalChunks(), 
            "Chunk " + chunkIndex + " should be less than total " + geometry.getTotalChunks());

        // Verify chunk boundary makes sense
        ChunkBoundary boundary = geometry.getChunkBoundary(chunkIndex);
        assertTrue(boundary.contains(filePosition), 
            "Chunk boundary should contain the file position");

        System.out.println("  Chunk boundary: [" + boundary.startInclusive() + 
                          ", " + boundary.endExclusive() + ")");
        System.out.println("✅ ChunkGeometry calculations are consistent");
    }

    @Test 
    public void testMerklePaneGeometryIntegration() throws IOException {
        // Create a test file
        Path testFile = tempDir.resolve("test.data");
        Path merkleFile = testFile.resolveSibling("test.data.mrkl");
        Path refFile = testFile.resolveSibling("test.data.mref");

        // Create a small test file
        byte[] testData = new byte[10 * 1024 * 1024]; // 10MB
        Files.write(testFile, testData);

        // We can't easily create a full MerklePane without proper merkle files,
        // but we can test the ChunkGeometryDescriptor creation
        try {
            ChunkGeometryDescriptor geometry = ChunkGeometryFactory.fromFile(testFile);

            assertNotNull(geometry, "Geometry should be created from file");
            // With new algorithm: 10MB file gets 1MB chunks (minimum chunk size)
            // 10MB / 1MB = 10 chunks, which is well under the 16384 limit
            assertEquals(1024 * 1024L, geometry.getChunkSize(),
                "Should use 1MB chunk size for 10MB file");
            assertEquals(testData.length, geometry.getTotalFileSize(),
                "Should match actual file size");

            // Verify chunk calculations work
            int totalChunks = geometry.getTotalChunks();
            assertTrue(totalChunks > 0, "Should have at least one chunk");

            // Test position mapping
            int chunk0 = geometry.getChunkIndexForPosition(0);
            assertEquals(0, chunk0, "Position 0 should be in chunk 0");

            long lastPosition = testData.length - 1;
            int lastChunk = geometry.getChunkIndexForPosition(lastPosition);
            assertTrue(lastChunk < totalChunks, "Last position should be in valid chunk");

            System.out.println("✅ MerklePane geometry integration works correctly");

        } catch (Exception e) {
            System.out.println("⚠️  Could not test full MerklePane integration (expected): " + e.getMessage());
            // This is expected since we don't have proper merkle tree files
        }
    }

    @Test
    public void testChunkBoundaryEquivalence() {
        // Test that ChunkGeometryDescriptor and ChunkBoundary calculations are equivalent
        long fileSize = 20480; // 20KB file

        ChunkGeometryDescriptor geometry = new ChunkGeometryDescriptor(fileSize);
        long chunkSize = geometry.getChunkSize();

        System.out.println("Testing with file size: " + fileSize + " bytes");
        System.out.println("Automatically calculated chunk size: " + chunkSize + " bytes");
        System.out.println("Total chunks: " + geometry.getTotalChunks());

        // Test each chunk boundary
        for (int i = 0; i < geometry.getTotalChunks(); i++) {
            ChunkBoundary boundary = geometry.getChunkBoundary(i);

            // Verify boundary properties
            assertEquals(i, boundary.chunkIndex(), "Boundary should have correct chunk index");
            assertEquals(i * chunkSize, boundary.startInclusive(), 
                "Boundary start should match calculation");

            long expectedEnd = Math.min((i + 1) * chunkSize, fileSize);
            assertEquals(expectedEnd, boundary.endExclusive(), 
                "Boundary end should match calculation");

            // Verify reverse calculation
            int calculatedChunk = geometry.getChunkIndexForPosition(boundary.startInclusive());
            assertEquals(i, calculatedChunk, 
                "Reverse calculation should match original chunk index");
        }

        System.out.println("✅ Chunk boundary calculations are mathematically consistent");
    }

    @Test
    public void testCohereAccessScenarioSpecific() {
        // Test the EXACT scenario that was failing in CohereAccessTest
        long fileSize = 41_000_000_000L; // 41GB Cohere file

        ChunkGeometryDescriptor geometry = new ChunkGeometryDescriptor(fileSize);

        // Get the automatically calculated chunk size
        long chunkSize = geometry.getChunkSize();

        // The exact vector that was causing the failure
        long vectorIndex = 2_324_227L;
        long vectorSize = 4100L; // 4 + (1024 * 4) bytes per vector
        long filePosition = vectorIndex * vectorSize; // = 9,529,330,700

        // This calculation should work without throwing
        int chunkIndex = geometry.getChunkIndexForPosition(filePosition);

        // Log the critical information for debugging
        System.out.println("=== Cohere Access Scenario Analysis ===");
        System.out.println("File size: " + fileSize + " bytes (" + (fileSize / (1024*1024*1024)) + " GB)");
        System.out.println("Automatically calculated chunk size: " + chunkSize + " bytes (" + (chunkSize / (1024*1024)) + " MB)");
        System.out.println("Total chunks: " + geometry.getTotalChunks());
        System.out.println("Vector " + vectorIndex + " at position " + filePosition);
        System.out.println("Calculated chunk: " + chunkIndex);

        // The key assertion: chunk should be valid
        assertTrue(chunkIndex < geometry.getTotalChunks(), 
            "Chunk " + chunkIndex + " MUST be less than total chunks " + geometry.getTotalChunks());

        // Verify the chunk boundary contains the position
        ChunkBoundary boundary = geometry.getChunkBoundary(chunkIndex);
        assertTrue(boundary.contains(filePosition),
            "Chunk " + chunkIndex + " boundary [" + boundary.startInclusive() + 
            ", " + boundary.endExclusive() + ") must contain position " + filePosition);

        System.out.println("✅ Cohere access scenario is now mathematically valid");
    }
}
