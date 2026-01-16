package io.nosqlbench.vshapes.stream;

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

import static org.junit.jupiter.api.Assertions.*;

class ChunkSizeCalculatorTest {

    @Test
    void constructor_validDimensions() {
        ChunkSizeCalculator calc = new ChunkSizeCalculator(768);
        assertEquals(768, calc.getDimensions());
        assertEquals(ChunkSizeCalculator.DEFAULT_BUDGET_FRACTION, calc.getBudgetFraction());
        assertEquals(ChunkSizeCalculator.DEFAULT_OVERHEAD_FACTOR, calc.getOverheadFactor());
    }

    @Test
    void constructor_invalidDimensions_throws() {
        assertThrows(IllegalArgumentException.class, () -> new ChunkSizeCalculator(0));
        assertThrows(IllegalArgumentException.class, () -> new ChunkSizeCalculator(-1));
    }

    @Test
    void constructor_customSettings() {
        ChunkSizeCalculator calc = new ChunkSizeCalculator(768, 0.8, 1.5);
        assertEquals(768, calc.getDimensions());
        assertEquals(0.8, calc.getBudgetFraction(), 0.001);
        assertEquals(1.5, calc.getOverheadFactor(), 0.001);
    }

    @Test
    void constructor_invalidBudgetFraction_throws() {
        assertThrows(IllegalArgumentException.class, () -> new ChunkSizeCalculator(768, 0.0, 1.2));
        assertThrows(IllegalArgumentException.class, () -> new ChunkSizeCalculator(768, -0.5, 1.2));
        assertThrows(IllegalArgumentException.class, () -> new ChunkSizeCalculator(768, 1.5, 1.2));
    }

    @Test
    void constructor_invalidOverheadFactor_throws() {
        assertThrows(IllegalArgumentException.class, () -> new ChunkSizeCalculator(768, 0.6, 0.5));
    }

    @Test
    void calculateForBudget_basicCalculation() {
        ChunkSizeCalculator calc = new ChunkSizeCalculator(768, 1.0, 1.0);

        // 768 dims * 4 bytes = 3072 bytes per vector
        // 1MB budget = 1,048,576 bytes
        // Expected: 1,048,576 / 3072 = 341 vectors
        long oneMB = 1024 * 1024;
        int chunkSize = calc.calculateForBudget(oneMB);

        // With no overhead (factor=1.0), 1MB / (768*4) = ~341 vectors
        // But MIN_CHUNK_SIZE is 1000, so it should return MIN_CHUNK_SIZE
        assertEquals(ChunkSizeCalculator.MIN_CHUNK_SIZE, chunkSize,
            "Chunk size should be clamped to MIN_CHUNK_SIZE for small budgets");
    }

    @Test
    void calculateForBudget_clampsToMinimum() {
        ChunkSizeCalculator calc = new ChunkSizeCalculator(768);

        // Very small budget should return minimum chunk size
        int chunkSize = calc.calculateForBudget(100);
        assertEquals(ChunkSizeCalculator.MIN_CHUNK_SIZE, chunkSize);
    }

    @Test
    void calculateForBudget_clampsToMaximum() {
        ChunkSizeCalculator calc = new ChunkSizeCalculator(1);  // 1 dimension

        // Very large budget with 1 dimension
        long hugeBudget = 100L * 1024 * 1024 * 1024;  // 100GB
        int chunkSize = calc.calculateForBudget(hugeBudget);
        assertEquals(ChunkSizeCalculator.MAX_CHUNK_SIZE, chunkSize);
    }

    @Test
    void calculateForBudget_zeroBudget() {
        ChunkSizeCalculator calc = new ChunkSizeCalculator(768);
        int chunkSize = calc.calculateForBudget(0);
        assertEquals(ChunkSizeCalculator.MIN_CHUNK_SIZE, chunkSize);
    }

    @Test
    void calculateForBudget_negativeBudget() {
        ChunkSizeCalculator calc = new ChunkSizeCalculator(768);
        int chunkSize = calc.calculateForBudget(-1000);
        assertEquals(ChunkSizeCalculator.MIN_CHUNK_SIZE, chunkSize);
    }

    @Test
    void calculate_returnsPositiveValue() {
        ChunkSizeCalculator calc = new ChunkSizeCalculator(768);
        int chunkSize = calc.calculate();

        assertTrue(chunkSize >= ChunkSizeCalculator.MIN_CHUNK_SIZE);
        assertTrue(chunkSize <= ChunkSizeCalculator.MAX_CHUNK_SIZE);
    }

    @Test
    void estimateMemoryBytes_basicCalculation() {
        ChunkSizeCalculator calc = new ChunkSizeCalculator(768, 1.0, 1.0);

        // 768 dims * 1000 vectors * 4 bytes = 3,072,000 bytes
        long estimate = calc.estimateMemoryBytes(1000);
        assertEquals(3_072_000, estimate);
    }

    @Test
    void estimateMemoryBytes_withOverhead() {
        ChunkSizeCalculator calc = new ChunkSizeCalculator(768, 1.0, 1.2);

        // 768 dims * 1000 vectors * 4 bytes * 1.2 = 3,686,400 bytes
        long estimate = calc.estimateMemoryBytes(1000);
        assertEquals(3_686_400, estimate);
    }

    @Test
    void bytesPerVector_basicCalculation() {
        ChunkSizeCalculator calc = new ChunkSizeCalculator(768);
        assertEquals(768 * 4, calc.bytesPerVector());
    }

    @Test
    void parseMemorySpec_gigabytes() {
        long bytes = ChunkSizeCalculator.parseMemorySpec("4g");
        assertEquals(4L * 1024 * 1024 * 1024, bytes);

        bytes = ChunkSizeCalculator.parseMemorySpec("4G");
        assertEquals(4L * 1024 * 1024 * 1024, bytes);
    }

    @Test
    void parseMemorySpec_megabytes() {
        long bytes = ChunkSizeCalculator.parseMemorySpec("512m");
        assertEquals(512L * 1024 * 1024, bytes);

        bytes = ChunkSizeCalculator.parseMemorySpec("512M");
        assertEquals(512L * 1024 * 1024, bytes);
    }

    @Test
    void parseMemorySpec_kilobytes() {
        long bytes = ChunkSizeCalculator.parseMemorySpec("1024k");
        assertEquals(1024L * 1024, bytes);

        bytes = ChunkSizeCalculator.parseMemorySpec("1024K");
        assertEquals(1024L * 1024, bytes);
    }

    @Test
    void parseMemorySpec_fraction() {
        // Fraction should return a portion of available heap
        long bytes = ChunkSizeCalculator.parseMemorySpec("0.5");
        Runtime rt = Runtime.getRuntime();
        long availableHeap = rt.maxMemory() - rt.totalMemory() + rt.freeMemory();
        long expected = (long) (availableHeap * 0.5);

        // Allow some variance due to GC between calls
        assertTrue(Math.abs(bytes - expected) < availableHeap * 0.01);
    }

    @Test
    void parseMemorySpec_nullOrBlank_throws() {
        assertThrows(IllegalArgumentException.class, () -> ChunkSizeCalculator.parseMemorySpec(null));
        assertThrows(IllegalArgumentException.class, () -> ChunkSizeCalculator.parseMemorySpec(""));
        assertThrows(IllegalArgumentException.class, () -> ChunkSizeCalculator.parseMemorySpec("   "));
    }

    @Test
    void parseMemorySpec_invalid_throws() {
        assertThrows(IllegalArgumentException.class, () -> ChunkSizeCalculator.parseMemorySpec("abc"));
        assertThrows(IllegalArgumentException.class, () -> ChunkSizeCalculator.parseMemorySpec("12x"));
    }

    @Test
    void toString_containsRelevantInfo() {
        ChunkSizeCalculator calc = new ChunkSizeCalculator(768);
        String str = calc.toString();

        assertTrue(str.contains("768"));
        assertTrue(str.contains("60%"));
        assertTrue(str.contains("1.2x"));
    }
}
