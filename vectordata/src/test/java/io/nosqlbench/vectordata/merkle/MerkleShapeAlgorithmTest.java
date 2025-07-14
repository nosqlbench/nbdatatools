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

/**
 * Tests the MerkleShape chunk size calculation algorithm.
 */
public class MerkleShapeAlgorithmTest {

    @Test
    public void testChunkSizeCalculationAlgorithm() {
        // Test minimum chunk size (1MB) for small files
        BaseMerkleShape small = new BaseMerkleShape(1024 * 1024); // 1MB file
        assertEquals(1024 * 1024L, small.getChunkSize(), "1MB file should use 1MB chunks");
        assertEquals(1, small.getTotalChunks(), "1MB file should have 1 chunk");

        // Test that we stay at 1MB for files that don't exceed 4096 chunks
        long fourGB = 4L * 1024L * 1024L * 1024L; // 4GB
        BaseMerkleShape medium = new BaseMerkleShape(fourGB);
        assertEquals(1024 * 1024L, medium.getChunkSize(), "4GB file should use 1MB chunks");
        assertEquals(4096, medium.getTotalChunks(), "4GB file should have exactly 4096 chunks");

        // Test that we scale up when chunks would exceed 4096
        long fiveGB = 5L * 1024L * 1024L * 1024L; // 5GB  
        BaseMerkleShape large = new BaseMerkleShape(fiveGB);
        assertEquals(2 * 1024 * 1024L, large.getChunkSize(), "5GB file should use 2MB chunks");
        assertTrue(large.getTotalChunks() <= 4096, "5GB file should have <= 4096 chunks");

        // Test maximum chunk size (64MB)
        long very_large = 1024L * 1024L * 1024L * 1024L; // 1TB
        BaseMerkleShape veryLarge = new BaseMerkleShape(very_large);
        assertEquals(64 * 1024 * 1024L, veryLarge.getChunkSize(), "1TB file should use 64MB chunks");
        // At 1TB with 64MB chunks, we'll have more than 4096 chunks, but that's allowed

        // Test powers of 2
        BaseMerkleShape test1 = new BaseMerkleShape(32L * 1024L * 1024L * 1024L); // 32GB
        assertTrue(Long.bitCount(test1.getChunkSize()) == 1, "Chunk size should be power of 2");

        // Test zero content size
        BaseMerkleShape empty = new BaseMerkleShape(0);
        assertEquals(1024 * 1024L, empty.getChunkSize(), "Empty content should use minimum chunk size");
        assertEquals(0, empty.getTotalChunks(), "Empty content should have 0 chunks");
    }

    @Test
    public void testChunkSizeScaling() {
        // Test that chunk size scales up as expected

        // At 4GB exactly: 1MB chunks, 4096 chunks
        BaseMerkleShape at4GB = new BaseMerkleShape(4L * 1024L * 1024L * 1024L);
        assertEquals(1024 * 1024L, at4GB.getChunkSize());
        assertEquals(4096, at4GB.getTotalChunks());

        // Just over 4GB: should scale to 2MB chunks
        BaseMerkleShape over4GB = new BaseMerkleShape(4L * 1024L * 1024L * 1024L + 1);
        assertEquals(2 * 1024 * 1024L, over4GB.getChunkSize());
        assertTrue(over4GB.getTotalChunks() <= 4096);

        // At 8GB: 2MB chunks, 4096 chunks
        BaseMerkleShape at8GB = new BaseMerkleShape(8L * 1024L * 1024L * 1024L);
        assertEquals(2 * 1024 * 1024L, at8GB.getChunkSize());
        assertEquals(4096, at8GB.getTotalChunks());

        // Continue scaling pattern up to 64MB max
        BaseMerkleShape at256GB = new BaseMerkleShape(256L * 1024L * 1024L * 1024L);
        assertEquals(64 * 1024 * 1024L, at256GB.getChunkSize());
        assertEquals(4096, at256GB.getTotalChunks());

        // Beyond 64MB max chunk size, chunks can exceed 4096
        BaseMerkleShape at1TB = new BaseMerkleShape(1024L * 1024L * 1024L * 1024L);
        assertEquals(64 * 1024 * 1024L, at1TB.getChunkSize());
        assertTrue(at1TB.getTotalChunks() > 4096); // 16384 chunks
    }

    @Test
    public void testCohereScenario() {
        // Test the original Cohere file size scenario
        long cohereSize = 41_000_000_000L; // 41GB
        BaseMerkleShape cohere = new BaseMerkleShape(cohereSize);

        // 41GB should use 16MB chunks to keep under 4096 chunks
        // 41GB / 16MB ≈ 2560 chunks
        assertEquals(16 * 1024 * 1024L, cohere.getChunkSize(), "41GB file should use 16MB chunks");
        assertTrue(cohere.getTotalChunks() <= 4096, "Should keep chunks under 4096");

        // Verify the chunk that was originally problematic (9087) would not exist with new chunking
        // With 16MB chunks, we'd have ~2560 chunks, so chunk 9087 would be invalid
        assertTrue(9087 >= cohere.getTotalChunks(), "Chunk 9087 should not exist with new chunking scheme");
    }
}
