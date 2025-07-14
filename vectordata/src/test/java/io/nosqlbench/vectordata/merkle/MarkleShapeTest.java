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

import io.nosqlbench.vectordata.merklev2.BaseMerkleShape;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class MarkleShapeTest {

    @Test
    public void testBasicChunkCalculations() {
        // Test with Cohere-like parameters: 41GB file
        long fileSize = 41_000_000_000L; // 41GB

        BaseMerkleShape geometry = new BaseMerkleShape(fileSize);

        // Get the automatically calculated chunk size
        long chunkSize = geometry.getChunkSize();

        // Verify total chunks calculation
        int expectedChunks = (int) Math.ceil((double) fileSize / chunkSize);
        assertEquals(expectedChunks, geometry.getTotalChunks());

        // Verify chunk size is a power of 2
        assertEquals(1, Long.bitCount(chunkSize), "Chunk size should be a power of 2");

        // Verify chunk count is reasonable (should be less than MAX_PREFERRED_CHUNKS which is 4096)
        assertTrue(geometry.getTotalChunks() <= 4096, "Total chunks should be <= 4096");

        System.out.println("Total chunks for 41GB file with " + (chunkSize / (1024*1024)) + "MB chunks: " + geometry.getTotalChunks());
    }

    @Test
    public void testChunkBoundaryCalculations() {
        long fileSize = 10 * 1024 * 1024; // 10MB

        BaseMerkleShape geometry = new BaseMerkleShape(fileSize);
        long chunkSize = geometry.getChunkSize();

        // Calculate expected chunks
        int expectedChunks = (int) Math.ceil((double) fileSize / chunkSize);
        assertEquals(expectedChunks, geometry.getTotalChunks());

        // Test first chunk
        ChunkBoundary firstChunk = geometry.getChunkBoundary(0);
        assertEquals(0, firstChunk.chunkIndex());
        assertEquals(0, firstChunk.startInclusive());
        assertEquals(chunkSize, firstChunk.endExclusive());
        assertEquals(chunkSize, firstChunk.size());

        // Test last chunk
        ChunkBoundary lastChunk = geometry.getChunkBoundary(geometry.getTotalChunks() - 1);
        assertEquals(geometry.getTotalChunks() - 1, lastChunk.chunkIndex());
        assertEquals((geometry.getTotalChunks() - 1) * chunkSize, lastChunk.startInclusive());
        assertEquals(fileSize, lastChunk.endExclusive());
        assertEquals(fileSize - ((geometry.getTotalChunks() - 1) * chunkSize), lastChunk.size());
    }

    @Test
    public void testPositionToChunkMapping() {
        // Use a small file size to test position mapping
        long fileSize = 5120; // 5KB file

        BaseMerkleShape geometry = new BaseMerkleShape(fileSize);
        long chunkSize = geometry.getChunkSize();

        // For small files, the minimum chunk size (1MB) will be used
        // So we'll have just 1 chunk for this small file

        // Position 0 should be in chunk 0
        assertEquals(0, geometry.getChunkIndexForPosition(0));

        // Last position should be in the last chunk
        assertEquals(geometry.getTotalChunks() - 1, geometry.getChunkIndexForPosition(fileSize - 1));

        // Test a position in the middle
        long middlePosition = fileSize / 2;
        int expectedChunk = (int)(middlePosition / chunkSize);
        assertEquals(expectedChunk, geometry.getChunkIndexForPosition(middlePosition));
    }

    @Test
    public void testCohereScenario() {
        // Test the specific scenario from CohereAccessTest failure
        long fileSize = 41_000_000_000L; // 41GB file

        BaseMerkleShape geometry = new BaseMerkleShape(fileSize);

        // Vector index 2,324,227 with 4100 bytes per vector
        long vectorIndex = 2_324_227L;
        long vectorSize = 4100L;
        long filePosition = vectorIndex * vectorSize;

        System.out.println("Vector index: " + vectorIndex);
        System.out.println("File position: " + filePosition);
        System.out.println("File position (GB): " + (filePosition / (1024.0 * 1024.0 * 1024.0)));
        System.out.println("Chunk size: " + (geometry.getChunkSize() / (1024.0 * 1024.0)) + " MB");

        // Calculate which chunk this should be in
        int chunkIndex = geometry.getChunkIndexForPosition(filePosition);
        int totalChunks = geometry.getTotalChunks();

        System.out.println("Calculated chunk: " + chunkIndex);
        System.out.println("Total chunks: " + totalChunks);

        // The chunk index should be valid
        assertTrue(chunkIndex < totalChunks, 
            "Chunk " + chunkIndex + " should be less than total " + totalChunks);

        // Verify the chunk boundary makes sense
        ChunkBoundary boundary = geometry.getChunkBoundary(chunkIndex);
        assertTrue(boundary.contains(filePosition), 
            "Chunk boundary should contain the calculated position");
    }

    @Test
    public void testValidationErrors() {
        long fileSize = 10240;

        BaseMerkleShape geometry = new BaseMerkleShape(fileSize);

        // Test invalid chunk index
        assertThrows(IllegalArgumentException.class, () -> {
            geometry.getChunkBoundary(-1);
        });

        assertThrows(IllegalArgumentException.class, () -> {
            geometry.getChunkBoundary(geometry.getTotalChunks());
        });

        // Test invalid file position
        assertThrows(IllegalArgumentException.class, () -> {
            geometry.getChunkIndexForPosition(-1);
        });

        assertThrows(IllegalArgumentException.class, () -> {
            geometry.getChunkIndexForPosition(fileSize);
        });
    }

    @Test
    public void testContentSizeValidation() {
        // Test invalid content sizes
        assertThrows(IllegalArgumentException.class, () -> {
            new BaseMerkleShape(-1); // Negative content size
        });

        // Test valid content sizes
        assertDoesNotThrow(() -> {
            new BaseMerkleShape(0); // Zero content size is valid (empty file)
            new BaseMerkleShape(1024); // Small content size
            new BaseMerkleShape(1024 * 1024); // 1MB
            new BaseMerkleShape(1024 * 1024 * 1024); // 1GB
        });

        // Test that chunk size is always a power of 2
        BaseMerkleShape geometry = new BaseMerkleShape(1000);
        assertEquals(1, Long.bitCount(geometry.getChunkSize()), 
            "Chunk size should be a power of 2");
    }
}
