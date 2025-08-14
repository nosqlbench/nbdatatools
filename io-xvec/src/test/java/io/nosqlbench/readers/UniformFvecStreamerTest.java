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

class UniformFvecStreamerTest {

    @TempDir
    Path tempDir;

    /**
     * Helper method to create a test fvec file with the given vectors
     */
    private Path createTestFvecFile(String filename, float[][] vectors) throws IOException {
        Path testFile = tempDir.resolve(filename);
        
        try (var outputStream = Files.newOutputStream(testFile)) {
            for (float[] vector : vectors) {
                // Write dimension (4 bytes)
                ByteBuffer dimBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
                dimBuffer.putInt(vector.length);
                outputStream.write(dimBuffer.array());
                
                // Write vector values (4 bytes each for float)
                ByteBuffer valueBuffer = ByteBuffer.allocate(vector.length * 4).order(ByteOrder.LITTLE_ENDIAN);
                for (float value : vector) {
                    valueBuffer.putFloat(value);
                }
                outputStream.write(valueBuffer.array());
            }
        }
        
        return testFile;
    }

    @Test
    void testBasicIteration() throws IOException {
        // Create test data
        float[][] testVectors = {
            {1.1f, 2.2f, 3.3f},
            {4.4f, 5.5f, 6.6f},
            {7.7f, 8.8f, 9.9f}
        };
        
        Path testFile = createTestFvecFile("basic_iteration.fvec", testVectors);
        
        // Test the streamer
        UniformFvecStreamer streamer = new UniformFvecStreamer();
        streamer.open(testFile);
        
        // Check size
        assertEquals(3, streamer.getSize(), "Streamer should report 3 vectors");
        
        // Check iteration
        Iterator<float[]> iterator = streamer.iterator();
        
        assertTrue(iterator.hasNext(), "Iterator should have first element");
        float[] vector1 = iterator.next();
        assertArrayEquals(new float[]{1.1f, 2.2f, 3.3f}, vector1, 0.0001f, "First vector should match");
        
        assertTrue(iterator.hasNext(), "Iterator should have second element");
        float[] vector2 = iterator.next();
        assertArrayEquals(new float[]{4.4f, 5.5f, 6.6f}, vector2, 0.0001f, "Second vector should match");
        
        assertTrue(iterator.hasNext(), "Iterator should have third element");
        float[] vector3 = iterator.next();
        assertArrayEquals(new float[]{7.7f, 8.8f, 9.9f}, vector3, 0.0001f, "Third vector should match");
        
        assertFalse(iterator.hasNext(), "Iterator should be exhausted");
        
        // Test exception when trying to read past the end
        assertThrows(NoSuchElementException.class, iterator::next, "Should throw NoSuchElementException when reading past the end");
        
        // Close the streamer
        streamer.close();
    }

    @Test
    void testMultipleIterators() throws IOException {
        // Create test data
        float[][] testVectors = {
            {1.1f, 2.2f},
            {3.3f, 4.4f},
            {5.5f, 6.6f}
        };
        
        Path testFile = createTestFvecFile("multiple_iterators.fvec", testVectors);
        
        // Test the streamer
        UniformFvecStreamer streamer = new UniformFvecStreamer();
        streamer.open(testFile);
        
        // First iterator should get all the data
        Iterator<float[]> iterator1 = streamer.iterator();
        assertTrue(iterator1.hasNext());
        assertArrayEquals(new float[]{1.1f, 2.2f}, iterator1.next(), 0.0001f);
        assertTrue(iterator1.hasNext());
        assertArrayEquals(new float[]{3.3f, 4.4f}, iterator1.next(), 0.0001f);
        assertTrue(iterator1.hasNext());
        assertArrayEquals(new float[]{5.5f, 6.6f}, iterator1.next(), 0.0001f);
        assertFalse(iterator1.hasNext());
        
        // Second iterator should start from the beginning again
        Iterator<float[]> iterator2 = streamer.iterator();
        assertTrue(iterator2.hasNext());
        assertArrayEquals(new float[]{1.1f, 2.2f}, iterator2.next(), 0.0001f);
        assertTrue(iterator2.hasNext());
        assertArrayEquals(new float[]{3.3f, 4.4f}, iterator2.next(), 0.0001f);
        assertTrue(iterator2.hasNext());
        assertArrayEquals(new float[]{5.5f, 6.6f}, iterator2.next(), 0.0001f);
        assertFalse(iterator2.hasNext());
        
        // Close the streamer
        streamer.close();
    }

    @Test
    void testReadVector() throws IOException {
        // Create test data
        float[][] testVectors = {
            {10.1f, 20.2f, 30.3f},
            {40.4f, 50.5f, 60.6f},
            {70.7f, 80.8f, 90.9f}
        };
        
        Path testFile = createTestFvecFile("read_vector.fvec", testVectors);
        
        // Test the streamer
        UniformFvecStreamer streamer = new UniformFvecStreamer();
        streamer.open(testFile);
        
        // Read vectors by index
        assertArrayEquals(new float[]{10.1f, 20.2f, 30.3f}, streamer.readVector(0), 0.0001f, "Vector at index 0 should match");
        assertArrayEquals(new float[]{40.4f, 50.5f, 60.6f}, streamer.readVector(1), 0.0001f, "Vector at index 1 should match");
        assertArrayEquals(new float[]{70.7f, 80.8f, 90.9f}, streamer.readVector(2), 0.0001f, "Vector at index 2 should match");
        
        // Test out of bounds
        assertThrows(IndexOutOfBoundsException.class, () -> streamer.readVector(-1), "Should throw IndexOutOfBoundsException for negative index");
        assertThrows(IndexOutOfBoundsException.class, () -> streamer.readVector(3), "Should throw IndexOutOfBoundsException for index >= size");
        
        // Close the streamer
        streamer.close();
    }

    @Test
    void testEmptyFile() throws IOException {
        // Create an empty file
        Path emptyFile = tempDir.resolve("empty.fvec");
        Files.createFile(emptyFile);
        
        // Test the streamer
        UniformFvecStreamer streamer = new UniformFvecStreamer();
        
        // Opening an empty file should throw an exception
        assertThrows(RuntimeException.class, () -> streamer.open(emptyFile), "Opening an empty file should throw an exception");
    }

    @Test
    void testInvalidFile() throws IOException {
        // Create a file with invalid content
        Path invalidFile = tempDir.resolve("invalid.fvec");
        Files.write(invalidFile, new byte[]{1, 2, 3}); // Too short to be valid
        
        // Test the streamer
        UniformFvecStreamer streamer = new UniformFvecStreamer();
        
        // Opening an invalid file should throw an exception
        assertThrows(RuntimeException.class, () -> streamer.open(invalidFile), "Opening an invalid file should throw an exception");
    }

    @Test
    void testCollectAllVectors() throws IOException {
        // Create test data with different vector dimensions
        float[][] testVectors = {
            {1.1f, 2.2f, 3.3f, 4.4f},
            {5.5f, 6.6f, 7.7f, 8.8f},
            {9.9f, 10.1f, 11.1f, 12.2f}
        };
        
        Path testFile = createTestFvecFile("collect_all.fvec", testVectors);
        
        // Test the streamer
        UniformFvecStreamer streamer = new UniformFvecStreamer();
        streamer.open(testFile);
        
        // Collect all vectors
        List<float[]> vectors = new ArrayList<>();
        Iterator<float[]> iterator = streamer.iterator();
        while (iterator.hasNext()) {
            vectors.add(iterator.next());
        }
        
        // Verify results
        assertEquals(3, vectors.size(), "Should have collected 3 vectors");
        assertArrayEquals(new float[]{1.1f, 2.2f, 3.3f, 4.4f}, vectors.get(0), 0.0001f, "First vector should match");
        assertArrayEquals(new float[]{5.5f, 6.6f, 7.7f, 8.8f}, vectors.get(1), 0.0001f, "Second vector should match");
        assertArrayEquals(new float[]{9.9f, 10.1f, 11.1f, 12.2f}, vectors.get(2), 0.0001f, "Third vector should match");
        
        // Close the streamer
        streamer.close();
    }

    @Test
    void testGetName() throws IOException {
        // Create test data
        float[][] testVectors = {{1.1f, 2.2f, 3.3f}};
        Path testFile = createTestFvecFile("name_test.fvec", testVectors);
        
        // Test the streamer
        UniformFvecStreamer streamer = new UniformFvecStreamer();
        streamer.open(testFile);
        
        // Check name
        assertEquals("name_test.fvec", streamer.getName(), "Name should match the filename");
        
        // Close the streamer
        streamer.close();
    }
}