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

class UniformDvecStreamerTest {

    @TempDir
    Path tempDir;

    /**
     * Helper method to create a test dvec file with the given vectors
     */
    private Path createTestDvecFile(String filename, double[][] vectors) throws IOException {
        Path testFile = tempDir.resolve(filename);
        
        try (var outputStream = Files.newOutputStream(testFile)) {
            for (double[] vector : vectors) {
                // Write dimension (4 bytes)
                ByteBuffer dimBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
                dimBuffer.putInt(vector.length);
                outputStream.write(dimBuffer.array());
                
                // Write vector values (8 bytes each for double)
                ByteBuffer valueBuffer = ByteBuffer.allocate(vector.length * 8).order(ByteOrder.LITTLE_ENDIAN);
                for (double value : vector) {
                    valueBuffer.putDouble(value);
                }
                outputStream.write(valueBuffer.array());
            }
        }
        
        return testFile;
    }

    @Test
    void testBasicIteration() throws IOException {
        // Create test data
        double[][] testVectors = {
            {1.1, 2.2, 3.3},
            {4.4, 5.5, 6.6},
            {7.7, 8.8, 9.9}
        };
        
        Path testFile = createTestDvecFile("basic_iteration.dvec", testVectors);
        
        // Test the streamer
        UniformDvecStreamer streamer = new UniformDvecStreamer();
        streamer.open(testFile);
        
        // Check size
        assertEquals(3, streamer.getSize(), "Streamer should report 3 vectors");
        
        // Check iteration
        Iterator<double[]> iterator = streamer.iterator();
        
        assertTrue(iterator.hasNext(), "Iterator should have first element");
        double[] vector1 = iterator.next();
        assertArrayEquals(new double[]{1.1, 2.2, 3.3}, vector1, 0.0001, "First vector should match");
        
        assertTrue(iterator.hasNext(), "Iterator should have second element");
        double[] vector2 = iterator.next();
        assertArrayEquals(new double[]{4.4, 5.5, 6.6}, vector2, 0.0001, "Second vector should match");
        
        assertTrue(iterator.hasNext(), "Iterator should have third element");
        double[] vector3 = iterator.next();
        assertArrayEquals(new double[]{7.7, 8.8, 9.9}, vector3, 0.0001, "Third vector should match");
        
        assertFalse(iterator.hasNext(), "Iterator should be exhausted");
        
        // Test exception when trying to read past the end
        assertThrows(NoSuchElementException.class, iterator::next, "Should throw NoSuchElementException when reading past the end");
        
        // Close the streamer
        streamer.close();
    }

    @Test
    void testMultipleIterators() throws IOException {
        // Create test data
        double[][] testVectors = {
            {1.1, 2.2},
            {3.3, 4.4},
            {5.5, 6.6}
        };
        
        Path testFile = createTestDvecFile("multiple_iterators.dvec", testVectors);
        
        // Test the streamer
        UniformDvecStreamer streamer = new UniformDvecStreamer();
        streamer.open(testFile);
        
        // First iterator should get all the data
        Iterator<double[]> iterator1 = streamer.iterator();
        assertTrue(iterator1.hasNext());
        assertArrayEquals(new double[]{1.1, 2.2}, iterator1.next(), 0.0001);
        assertTrue(iterator1.hasNext());
        assertArrayEquals(new double[]{3.3, 4.4}, iterator1.next(), 0.0001);
        assertTrue(iterator1.hasNext());
        assertArrayEquals(new double[]{5.5, 6.6}, iterator1.next(), 0.0001);
        assertFalse(iterator1.hasNext());
        
        // Second iterator should start from the beginning again
        Iterator<double[]> iterator2 = streamer.iterator();
        assertTrue(iterator2.hasNext());
        assertArrayEquals(new double[]{1.1, 2.2}, iterator2.next(), 0.0001);
        assertTrue(iterator2.hasNext());
        assertArrayEquals(new double[]{3.3, 4.4}, iterator2.next(), 0.0001);
        assertTrue(iterator2.hasNext());
        assertArrayEquals(new double[]{5.5, 6.6}, iterator2.next(), 0.0001);
        assertFalse(iterator2.hasNext());
        
        // Close the streamer
        streamer.close();
    }

    @Test
    void testReadVector() throws IOException {
        // Create test data
        double[][] testVectors = {
            {10.1, 20.2, 30.3},
            {40.4, 50.5, 60.6},
            {70.7, 80.8, 90.9}
        };
        
        Path testFile = createTestDvecFile("read_vector.dvec", testVectors);
        
        // Test the streamer
        UniformDvecStreamer streamer = new UniformDvecStreamer();
        streamer.open(testFile);
        
        // Read vectors by index
        assertArrayEquals(new double[]{10.1, 20.2, 30.3}, streamer.readVector(0), 0.0001, "Vector at index 0 should match");
        assertArrayEquals(new double[]{40.4, 50.5, 60.6}, streamer.readVector(1), 0.0001, "Vector at index 1 should match");
        assertArrayEquals(new double[]{70.7, 80.8, 90.9}, streamer.readVector(2), 0.0001, "Vector at index 2 should match");
        
        // Test out of bounds
        assertThrows(IndexOutOfBoundsException.class, () -> streamer.readVector(-1), "Should throw IndexOutOfBoundsException for negative index");
        assertThrows(IndexOutOfBoundsException.class, () -> streamer.readVector(3), "Should throw IndexOutOfBoundsException for index >= size");
        
        // Close the streamer
        streamer.close();
    }

    @Test
    void testEmptyFile() throws IOException {
        // Create an empty file
        Path emptyFile = tempDir.resolve("empty.dvec");
        Files.createFile(emptyFile);
        
        // Test the streamer
        UniformDvecStreamer streamer = new UniformDvecStreamer();
        
        // Opening an empty file should throw an exception
        assertThrows(RuntimeException.class, () -> streamer.open(emptyFile), "Opening an empty file should throw an exception");
    }

    @Test
    void testInvalidFile() throws IOException {
        // Create a file with invalid content
        Path invalidFile = tempDir.resolve("invalid.dvec");
        Files.write(invalidFile, new byte[]{1, 2, 3}); // Too short to be valid
        
        // Test the streamer
        UniformDvecStreamer streamer = new UniformDvecStreamer();
        
        // Opening an invalid file should throw an exception
        assertThrows(RuntimeException.class, () -> streamer.open(invalidFile), "Opening an invalid file should throw an exception");
    }

    @Test
    void testCollectAllVectors() throws IOException {
        // Create test data with different vector dimensions
        double[][] testVectors = {
            {1.1, 2.2, 3.3, 4.4},
            {5.5, 6.6, 7.7, 8.8},
            {9.9, 10.1, 11.1, 12.2}
        };
        
        Path testFile = createTestDvecFile("collect_all.dvec", testVectors);
        
        // Test the streamer
        UniformDvecStreamer streamer = new UniformDvecStreamer();
        streamer.open(testFile);
        
        // Collect all vectors
        List<double[]> vectors = new ArrayList<>();
        Iterator<double[]> iterator = streamer.iterator();
        while (iterator.hasNext()) {
            vectors.add(iterator.next());
        }
        
        // Verify results
        assertEquals(3, vectors.size(), "Should have collected 3 vectors");
        assertArrayEquals(new double[]{1.1, 2.2, 3.3, 4.4}, vectors.get(0), 0.0001, "First vector should match");
        assertArrayEquals(new double[]{5.5, 6.6, 7.7, 8.8}, vectors.get(1), 0.0001, "Second vector should match");
        assertArrayEquals(new double[]{9.9, 10.1, 11.1, 12.2}, vectors.get(2), 0.0001, "Third vector should match");
        
        // Close the streamer
        streamer.close();
    }

    @Test
    void testGetName() throws IOException {
        // Create test data
        double[][] testVectors = {{1.1, 2.2, 3.3}};
        Path testFile = createTestDvecFile("name_test.dvec", testVectors);
        
        // Test the streamer
        UniformDvecStreamer streamer = new UniformDvecStreamer();
        streamer.open(testFile);
        
        // Check name
        assertEquals("name_test.dvec", streamer.getName(), "Name should match the filename");
        
        // Close the streamer
        streamer.close();
    }
}