package io.nosqlbench.readers;

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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

class UniformIvecStreamerTest {

    @TempDir
    Path tempDir;

    /**
     * Helper method to create a test ivec file with the given vectors
     */
    private Path createTestIvecFile(String filename, int[][] vectors) throws IOException {
        Path testFile = tempDir.resolve(filename);
        
        try (var outputStream = Files.newOutputStream(testFile)) {
            for (int[] vector : vectors) {
                // Write dimension (4 bytes)
                ByteBuffer dimBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
                dimBuffer.putInt(vector.length);
                outputStream.write(dimBuffer.array());
                
                // Write vector values (4 bytes each)
                ByteBuffer valueBuffer = ByteBuffer.allocate(vector.length * 4).order(ByteOrder.LITTLE_ENDIAN);
                for (int value : vector) {
                    valueBuffer.putInt(value);
                }
                outputStream.write(valueBuffer.array());
            }
        }
        
        return testFile;
    }

    @Test
    void testBasicIteration() throws IOException {
        // Create test data
        int[][] testVectors = {
            {1, 2, 3},
            {4, 5, 6},
            {7, 8, 9}
        };
        
        Path testFile = createTestIvecFile("basic_iteration.ivec", testVectors);
        
        // Test the streamer
        UniformIvecStreamer streamer = new UniformIvecStreamer();
        streamer.open(testFile);
        
        // Check size
        assertEquals(3, streamer.getSize(), "Streamer should report 3 vectors");
        
        // Check iteration
        Iterator<int[]> iterator = streamer.iterator();
        
        assertTrue(iterator.hasNext(), "Iterator should have first element");
        int[] vector1 = iterator.next();
        assertArrayEquals(new int[]{1, 2, 3}, vector1, "First vector should match");
        
        assertTrue(iterator.hasNext(), "Iterator should have second element");
        int[] vector2 = iterator.next();
        assertArrayEquals(new int[]{4, 5, 6}, vector2, "Second vector should match");
        
        assertTrue(iterator.hasNext(), "Iterator should have third element");
        int[] vector3 = iterator.next();
        assertArrayEquals(new int[]{7, 8, 9}, vector3, "Third vector should match");
        
        assertFalse(iterator.hasNext(), "Iterator should be exhausted");
        
        // Test exception when trying to read past the end
        assertThrows(NoSuchElementException.class, iterator::next, "Should throw NoSuchElementException when reading past the end");
        
        // Close the streamer
        streamer.close();
    }

    @Test
    void testMultipleIterators() throws IOException {
        // Create test data
        int[][] testVectors = {
            {1, 2},
            {3, 4},
            {5, 6}
        };
        
        Path testFile = createTestIvecFile("multiple_iterators.ivec", testVectors);
        
        // Test the streamer
        UniformIvecStreamer streamer = new UniformIvecStreamer();
        streamer.open(testFile);
        
        // First iterator should get all the data
        Iterator<int[]> iterator1 = streamer.iterator();
        assertTrue(iterator1.hasNext());
        assertArrayEquals(new int[]{1, 2}, iterator1.next());
        assertTrue(iterator1.hasNext());
        assertArrayEquals(new int[]{3, 4}, iterator1.next());
        assertTrue(iterator1.hasNext());
        assertArrayEquals(new int[]{5, 6}, iterator1.next());
        assertFalse(iterator1.hasNext());
        
        // Second iterator should start from the beginning again
        Iterator<int[]> iterator2 = streamer.iterator();
        assertTrue(iterator2.hasNext());
        assertArrayEquals(new int[]{1, 2}, iterator2.next());
        assertTrue(iterator2.hasNext());
        assertArrayEquals(new int[]{3, 4}, iterator2.next());
        assertTrue(iterator2.hasNext());
        assertArrayEquals(new int[]{5, 6}, iterator2.next());
        assertFalse(iterator2.hasNext());
        
        // Close the streamer
        streamer.close();
    }

    @Test
    void testReadVector() throws IOException {
        // Create test data
        int[][] testVectors = {
            {10, 20, 30},
            {40, 50, 60},
            {70, 80, 90}
        };
        
        Path testFile = createTestIvecFile("read_vector.ivec", testVectors);
        
        // Test the streamer
        UniformIvecStreamer streamer = new UniformIvecStreamer();
        streamer.open(testFile);
        
        // Read vectors by index
        assertArrayEquals(new int[]{10, 20, 30}, streamer.readVector(0), "Vector at index 0 should match");
        assertArrayEquals(new int[]{40, 50, 60}, streamer.readVector(1), "Vector at index 1 should match");
        assertArrayEquals(new int[]{70, 80, 90}, streamer.readVector(2), "Vector at index 2 should match");
        
        // Test out of bounds
        assertThrows(IndexOutOfBoundsException.class, () -> streamer.readVector(-1), "Should throw IndexOutOfBoundsException for negative index");
        assertThrows(IndexOutOfBoundsException.class, () -> streamer.readVector(3), "Should throw IndexOutOfBoundsException for index >= size");
        
        // Close the streamer
        streamer.close();
    }

    @Test
    void testEmptyFile() throws IOException {
        // Create an empty file
        Path emptyFile = tempDir.resolve("empty.ivec");
        Files.createFile(emptyFile);
        
        // Test the streamer
        UniformIvecStreamer streamer = new UniformIvecStreamer();
        
        // Opening an empty file should throw an exception
        assertThrows(RuntimeException.class, () -> streamer.open(emptyFile), "Opening an empty file should throw an exception");
    }

    @Test
    void testInvalidFile() throws IOException {
        // Create a file with invalid content
        Path invalidFile = tempDir.resolve("invalid.ivec");
        Files.write(invalidFile, new byte[]{1, 2, 3}); // Too short to be valid
        
        // Test the streamer
        UniformIvecStreamer streamer = new UniformIvecStreamer();
        
        // Opening an invalid file should throw an exception
        assertThrows(RuntimeException.class, () -> streamer.open(invalidFile), "Opening an invalid file should throw an exception");
    }

    @Test
    void testCollectAllVectors() throws IOException {
        // Create test data with different vector dimensions
        int[][] testVectors = {
            {1, 2, 3, 4},
            {5, 6, 7, 8},
            {9, 10, 11, 12}
        };
        
        Path testFile = createTestIvecFile("collect_all.ivec", testVectors);
        
        // Test the streamer
        UniformIvecStreamer streamer = new UniformIvecStreamer();
        streamer.open(testFile);
        
        // Collect all vectors
        List<int[]> vectors = new ArrayList<>();
        Iterator<int[]> iterator = streamer.iterator();
        while (iterator.hasNext()) {
            vectors.add(iterator.next());
        }
        
        // Verify results
        assertEquals(3, vectors.size(), "Should have collected 3 vectors");
        assertArrayEquals(new int[]{1, 2, 3, 4}, vectors.get(0), "First vector should match");
        assertArrayEquals(new int[]{5, 6, 7, 8}, vectors.get(1), "Second vector should match");
        assertArrayEquals(new int[]{9, 10, 11, 12}, vectors.get(2), "Third vector should match");
        
        // Close the streamer
        streamer.close();
    }

    @Test
    void testGetName() throws IOException {
        // Create test data
        int[][] testVectors = {{1, 2, 3}};
        Path testFile = createTestIvecFile("name_test.ivec", testVectors);
        
        // Test the streamer
        UniformIvecStreamer streamer = new UniformIvecStreamer();
        streamer.open(testFile);
        
        // Check name
        assertEquals("name_test.ivec", streamer.getName(), "Name should match the filename");
        
        // Close the streamer
        streamer.close();
    }
}