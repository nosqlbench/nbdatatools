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

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the MerkleTree.findMismatchedChunksInRange method.
 */
public class MerkleTreeRangeComparisonTest {

    @TempDir
    Path tempDir;

    /**
     * Test that the findMismatchedChunksInRange method correctly identifies mismatches
     * within a specific range of chunk indexes.
     */
    @Test
    void testFindMismatchedChunksInRange() {
        // Create test data
        int dataSize = 1024 * 1024; // 1MB
        int chunkSize = 4096; // 4KB chunks
        ByteBuffer data1 = ByteBuffer.allocate(dataSize);
        ByteBuffer data2 = ByteBuffer.allocate(dataSize);

        // Fill with random data
        Random random = new Random(42); // Use fixed seed for reproducibility
        random.nextBytes(data1.array());

        // Copy data1 to data2
        data2.put(data1.array());
        data1.position(0);
        data2.position(0);

        // Modify specific chunks in data2
        // We'll modify chunks 10, 20, 30, 40, and 50
        int[] modifiedChunks = {10, 20, 30, 40, 50};
        for (int chunkIndex : modifiedChunks) {
            int offset = chunkIndex * chunkSize;
            // Only modify if within bounds
            if (offset + chunkSize <= dataSize) {
                for (int i = 0; i < chunkSize; i++) {
                    data2.put(offset + i, (byte) (data2.get(offset + i) ^ 0xFF)); // Flip bits
                }
            }
        }

        // Create MerkleTrees
        MerkleRange fullRange = new MerkleRange(0, dataSize);
        MerkleTree tree1 = MerkleTree.fromData(data1, chunkSize, fullRange);
        MerkleTree tree2 = MerkleTree.fromData(data2, chunkSize, fullRange);

        // Test finding mismatches in the full range
        int[] mismatches = tree1.findMismatchedChunksInRange(tree2, 0, dataSize / chunkSize);
        assertEquals(modifiedChunks.length, mismatches.length);
        Arrays.sort(modifiedChunks); // Sort for comparison
        assertArrayEquals(modifiedChunks, mismatches);

        // Test finding mismatches in a partial range (chunks 15-45)
        int[] partialMismatches = tree1.findMismatchedChunksInRange(tree2, 15, 45);
        int[] expectedPartialMismatches = {20, 30, 40};
        assertArrayEquals(expectedPartialMismatches, partialMismatches);

        // Test finding mismatches in a range with no differences (chunks 0-5)
        int[] noMismatches = tree1.findMismatchedChunksInRange(tree2, 0, 5);
        assertEquals(0, noMismatches.length);
    }

    /**
     * Test that the findMismatchedChunksInRange method correctly validates input parameters.
     */
    @Test
    void testFindMismatchedChunksInRangeValidation() {
        // Create test data
        int dataSize = 1024 * 1024; // 1MB
        int chunkSize = 4096; // 4KB chunks
        ByteBuffer data1 = ByteBuffer.allocate(dataSize);
        ByteBuffer data2 = ByteBuffer.allocate(dataSize);

        // Fill with random data
        Random random = new Random(42);
        random.nextBytes(data1.array());
        random.nextBytes(data2.array());

        // Create MerkleTrees
        MerkleRange fullRange = new MerkleRange(0, dataSize);
        MerkleTree tree1 = MerkleTree.fromData(data1, chunkSize, fullRange);
        MerkleTree tree2 = MerkleTree.fromData(data2, chunkSize, fullRange);

        // Test with negative start index
        assertThrows(IllegalArgumentException.class, () -> {
            tree1.findMismatchedChunksInRange(tree2, -1, 10);
        });

        // Test with end index <= start index
        assertThrows(IllegalArgumentException.class, () -> {
            tree1.findMismatchedChunksInRange(tree2, 10, 10);
        });

        // Test with start index out of range
        assertThrows(IllegalArgumentException.class, () -> {
            tree1.findMismatchedChunksInRange(tree2, dataSize / chunkSize + 1, dataSize / chunkSize + 10);
        });

        // Test with different chunk sizes
        MerkleTree tree3 = MerkleTree.fromData(data1, chunkSize * 2, fullRange);
        assertThrows(IllegalArgumentException.class, () -> {
            tree1.findMismatchedChunksInRange(tree3, 0, 10);
        });

        // Test with different total sizes
        ByteBuffer data3 = ByteBuffer.allocate(dataSize * 2);
        MerkleTree tree4 = MerkleTree.fromData(data3, chunkSize, new MerkleRange(0, dataSize * 2));
        assertThrows(IllegalArgumentException.class, () -> {
            tree1.findMismatchedChunksInRange(tree4, 0, 10);
        });
    }

    /**
     * Test that the findMismatchedChunksInRange method correctly handles edge cases.
     */
    @Test
    void testFindMismatchedChunksInRangeEdgeCases() {
        // Create test data
        int dataSize = 1024 * 1024; // 1MB
        int chunkSize = 4096; // 4KB chunks
        ByteBuffer data1 = ByteBuffer.allocate(dataSize);
        ByteBuffer data2 = ByteBuffer.allocate(dataSize);

        // Fill with random data
        Random random = new Random(42);
        random.nextBytes(data1.array());

        // Copy data1 to data2
        data2.put(data1.array());
        data1.position(0);
        data2.position(0);

        // Create MerkleTrees with different ranges
        MerkleRange range1 = new MerkleRange(0, dataSize / 2);
        MerkleRange range2 = new MerkleRange(dataSize / 2, dataSize);
        MerkleTree tree1 = MerkleTree.fromData(data1, chunkSize, range1);
        MerkleTree tree2 = MerkleTree.fromData(data2, chunkSize, range2);

        // Test with non-overlapping trees
        int[] mismatches = tree1.findMismatchedChunksInRange(tree2, 0, 10);
        assertEquals(10, mismatches.length);
        for (int i = 0; i < 10; i++) {
            assertEquals(i, mismatches[i]);
        }

        // Test with end index exceeding maximum
        int maxChunks = dataSize / chunkSize;
        int[] allMismatches = tree1.findMismatchedChunksInRange(tree2, 0, maxChunks + 10);
        // The expected number of mismatches is the number of chunks in the first tree's range
        // Since tree1 has a range of (0, dataSize/2), the number of chunks is dataSize/2/chunkSize
        assertEquals(dataSize / 2 / chunkSize, allMismatches.length);
    }
}
