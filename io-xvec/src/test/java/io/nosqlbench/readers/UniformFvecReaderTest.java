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
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

class UniformFvecReaderTest {

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
                ByteBuffer dimBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN); // Use little-endian byte order
                dimBuffer.putInt(vector.length);
                outputStream.write(dimBuffer.array());

                // Write vector values (4 bytes each for float)
                ByteBuffer valueBuffer = ByteBuffer.allocate(vector.length * 4).order(ByteOrder.LITTLE_ENDIAN); // Use little-endian byte order
                for (float value : vector) {
                    valueBuffer.putFloat(value);
                }
                outputStream.write(valueBuffer.array());
            }
        }

        return testFile;
    }

    @Test
    void testBasicAccess() throws IOException {
        // Create test data
        float[][] testVectors = {
            {1.1f, 2.2f, 3.3f},
            {4.4f, 5.5f, 6.6f},
            {7.7f, 8.8f, 9.9f}
        };

        Path testFile = createTestFvecFile("basic_access.fvec", testVectors);

        // Test the reader
        UniformFvecReader reader = new UniformFvecReader();
        reader.open(testFile);

        // Check size
        assertEquals(3, reader.size(), "Reader should report 3 vectors");
        assertEquals(3, reader.getSize(), "getSize() should match size()");

        // Check get method
        assertArrayEquals(new float[]{1.1f, 2.2f, 3.3f}, reader.get(0), 0.0001f, "Vector at index 0 should match");
        assertArrayEquals(new float[]{4.4f, 5.5f, 6.6f}, reader.get(1), 0.0001f, "Vector at index 1 should match");
        assertArrayEquals(new float[]{7.7f, 8.8f, 9.9f}, reader.get(2), 0.0001f, "Vector at index 2 should match");

        // Test out of bounds
        assertThrows(IndexOutOfBoundsException.class, () -> reader.get(-1), "Should throw IndexOutOfBoundsException for negative index");
        assertThrows(IndexOutOfBoundsException.class, () -> reader.get(3), "Should throw IndexOutOfBoundsException for index >= size");

        // Close the reader
        reader.close();
    }

    @Test
    void testListMethods() throws IOException {
        // Create test data
        float[][] testVectors = {
            {1.1f, 2.2f, 3.3f},
            {4.4f, 5.5f, 6.6f},
            {7.7f, 8.8f, 9.9f}
        };

        Path testFile = createTestFvecFile("list_methods.fvec", testVectors);

        // Test the reader
        UniformFvecReader reader = new UniformFvecReader();
        reader.open(testFile);

        // Test isEmpty
        assertFalse(reader.isEmpty(), "Reader should not be empty");

        // Test contains
        assertTrue(reader.contains(new float[]{1.1f, 2.2f, 3.3f}), "Reader should contain [1.1f, 2.2f, 3.3f]");
        assertTrue(reader.contains(new float[]{4.4f, 5.5f, 6.6f}), "Reader should contain [4.4f, 5.5f, 6.6f]");
        assertTrue(reader.contains(new float[]{7.7f, 8.8f, 9.9f}), "Reader should contain [7.7f, 8.8f, 9.9f]");
        assertFalse(reader.contains(new float[]{10.1f, 11.2f, 12.3f}), "Reader should not contain [10.1f, 11.2f, 12.3f]");
        assertFalse(reader.contains(new float[]{1.1f, 2.2f}), "Reader should not contain [1.1f, 2.2f] (wrong dimension)");

        // Test indexOf
        assertEquals(0, reader.indexOf(new float[]{1.1f, 2.2f, 3.3f}), "indexOf should find [1.1f, 2.2f, 3.3f] at index 0");
        assertEquals(1, reader.indexOf(new float[]{4.4f, 5.5f, 6.6f}), "indexOf should find [4.4f, 5.5f, 6.6f] at index 1");
        assertEquals(2, reader.indexOf(new float[]{7.7f, 8.8f, 9.9f}), "indexOf should find [7.7f, 8.8f, 9.9f] at index 2");
        assertEquals(-1, reader.indexOf(new float[]{10.1f, 11.2f, 12.3f}), "indexOf should return -1 for non-existent vector");

        // Test lastIndexOf
        assertEquals(0, reader.lastIndexOf(new float[]{1.1f, 2.2f, 3.3f}), "lastIndexOf should find [1.1f, 2.2f, 3.3f] at index 0");
        assertEquals(1, reader.lastIndexOf(new float[]{4.4f, 5.5f, 6.6f}), "lastIndexOf should find [4.4f, 5.5f, 6.6f] at index 1");
        assertEquals(2, reader.lastIndexOf(new float[]{7.7f, 8.8f, 9.9f}), "lastIndexOf should find [7.7f, 8.8f, 9.9f] at index 2");
        assertEquals(-1, reader.lastIndexOf(new float[]{10.1f, 11.2f, 12.3f}), "lastIndexOf should return -1 for non-existent vector");

        // Test containsAll
        assertTrue(reader.containsAll(Arrays.asList(new float[]{1.1f, 2.2f, 3.3f}, new float[]{4.4f, 5.5f, 6.6f})), 
                  "containsAll should return true for contained vectors");
        assertFalse(reader.containsAll(Arrays.asList(new float[]{1.1f, 2.2f, 3.3f}, new float[]{10.1f, 11.2f, 12.3f})), 
                   "containsAll should return false if any vector is not contained");

        // Close the reader
        reader.close();
    }

    @Test
    void testIterator() throws IOException {
        // Create test data
        float[][] testVectors = {
            {1.1f, 2.2f, 3.3f},
            {4.4f, 5.5f, 6.6f},
            {7.7f, 8.8f, 9.9f}
        };

        Path testFile = createTestFvecFile("iterator.fvec", testVectors);

        // Test the reader
        UniformFvecReader reader = new UniformFvecReader();
        reader.open(testFile);

        // Test iterator
        Iterator<float[]> iterator = reader.iterator();

        assertTrue(iterator.hasNext(), "Iterator should have first element");
        assertArrayEquals(new float[]{1.1f, 2.2f, 3.3f}, iterator.next(), 0.0001f, "First vector should match");

        assertTrue(iterator.hasNext(), "Iterator should have second element");
        assertArrayEquals(new float[]{4.4f, 5.5f, 6.6f}, iterator.next(), 0.0001f, "Second vector should match");

        assertTrue(iterator.hasNext(), "Iterator should have third element");
        assertArrayEquals(new float[]{7.7f, 8.8f, 9.9f}, iterator.next(), 0.0001f, "Third vector should match");

        assertFalse(iterator.hasNext(), "Iterator should be exhausted");
        assertThrows(NoSuchElementException.class, iterator::next, "Should throw NoSuchElementException when reading past the end");

        // Close the reader
        reader.close();
    }

    @Test
    void testListIterator() throws IOException {
        // Create test data
        float[][] testVectors = {
            {1.1f, 2.2f, 3.3f},
            {4.4f, 5.5f, 6.6f},
            {7.7f, 8.8f, 9.9f}
        };

        Path testFile = createTestFvecFile("list_iterator.fvec", testVectors);

        // Test the reader
        UniformFvecReader reader = new UniformFvecReader();
        reader.open(testFile);

        // Test list iterator
        ListIterator<float[]> iterator = reader.listIterator();

        // Forward iteration
        assertTrue(iterator.hasNext(), "Iterator should have next element");
        assertFalse(iterator.hasPrevious(), "Iterator should not have previous element at start");
        assertEquals(0, iterator.nextIndex(), "Next index should be 0");
        assertEquals(-1, iterator.previousIndex(), "Previous index should be -1");

        assertArrayEquals(new float[]{1.1f, 2.2f, 3.3f}, iterator.next(), 0.0001f, "First vector should match");

        assertTrue(iterator.hasNext(), "Iterator should have next element");
        assertTrue(iterator.hasPrevious(), "Iterator should have previous element");
        assertEquals(1, iterator.nextIndex(), "Next index should be 1");
        assertEquals(0, iterator.previousIndex(), "Previous index should be 0");

        assertArrayEquals(new float[]{4.4f, 5.5f, 6.6f}, iterator.next(), 0.0001f, "Second vector should match");

        assertTrue(iterator.hasNext(), "Iterator should have next element");
        assertTrue(iterator.hasPrevious(), "Iterator should have previous element");
        assertEquals(2, iterator.nextIndex(), "Next index should be 2");
        assertEquals(1, iterator.previousIndex(), "Previous index should be 1");

        assertArrayEquals(new float[]{7.7f, 8.8f, 9.9f}, iterator.next(), 0.0001f, "Third vector should match");

        assertFalse(iterator.hasNext(), "Iterator should be exhausted");
        assertTrue(iterator.hasPrevious(), "Iterator should have previous element");
        assertEquals(3, iterator.nextIndex(), "Next index should be 3");
        assertEquals(2, iterator.previousIndex(), "Previous index should be 2");

        // Backward iteration
        assertArrayEquals(new float[]{7.7f, 8.8f, 9.9f}, iterator.previous(), 0.0001f, "Third vector should match");
        assertArrayEquals(new float[]{4.4f, 5.5f, 6.6f}, iterator.previous(), 0.0001f, "Second vector should match");
        assertArrayEquals(new float[]{1.1f, 2.2f, 3.3f}, iterator.previous(), 0.0001f, "First vector should match");

        assertFalse(iterator.hasPrevious(), "Iterator should be exhausted in reverse");
        assertTrue(iterator.hasNext(), "Iterator should have next element");

        // Close the reader
        reader.close();
    }

    @Test
    void testSubList() throws IOException {
        // Create test data
        float[][] testVectors = {
            {1.1f, 2.2f, 3.3f},
            {4.4f, 5.5f, 6.6f},
            {7.7f, 8.8f, 9.9f},
            {10.1f, 11.2f, 12.3f},
            {13.4f, 14.5f, 15.6f}
        };

        Path testFile = createTestFvecFile("sublist.fvec", testVectors);

        // Test the reader
        UniformFvecReader reader = new UniformFvecReader();
        reader.open(testFile);

        // Test subList
        List<float[]> subList = reader.subList(1, 4);

        assertEquals(3, subList.size(), "Sublist should have 3 elements");
        assertArrayEquals(new float[]{4.4f, 5.5f, 6.6f}, subList.get(0), 0.0001f, "First element of sublist should match");
        assertArrayEquals(new float[]{7.7f, 8.8f, 9.9f}, subList.get(1), 0.0001f, "Second element of sublist should match");
        assertArrayEquals(new float[]{10.1f, 11.2f, 12.3f}, subList.get(2), 0.0001f, "Third element of sublist should match");

        // Test subList of subList
        List<float[]> subSubList = subList.subList(1, 3);

        assertEquals(2, subSubList.size(), "Sub-sublist should have 2 elements");
        assertArrayEquals(new float[]{7.7f, 8.8f, 9.9f}, subSubList.get(0), 0.0001f, "First element of sub-sublist should match");
        assertArrayEquals(new float[]{10.1f, 11.2f, 12.3f}, subSubList.get(1), 0.0001f, "Second element of sub-sublist should match");

        // Test invalid subList parameters
        assertThrows(IndexOutOfBoundsException.class, () -> reader.subList(-1, 3), "Should throw for negative fromIndex");
        assertThrows(IndexOutOfBoundsException.class, () -> reader.subList(0, 6), "Should throw for toIndex > size");
        assertThrows(IndexOutOfBoundsException.class, () -> reader.subList(3, 2), "Should throw for fromIndex > toIndex");

        // Close the reader
        reader.close();
    }

    @Test
    void testToArray() throws IOException {
        // Create test data
        float[][] testVectors = {
            {1.1f, 2.2f, 3.3f},
            {4.4f, 5.5f, 6.6f},
            {7.7f, 8.8f, 9.9f}
        };

        Path testFile = createTestFvecFile("to_array.fvec", testVectors);

        // Test the reader
        UniformFvecReader reader = new UniformFvecReader();
        reader.open(testFile);

        // Test toArray()
        Object[] array = reader.toArray();

        assertEquals(3, array.length, "Array should have 3 elements");
        assertArrayEquals(new float[]{1.1f, 2.2f, 3.3f}, (float[]) array[0], 0.0001f, "First element should match");
        assertArrayEquals(new float[]{4.4f, 5.5f, 6.6f}, (float[]) array[1], 0.0001f, "Second element should match");
        assertArrayEquals(new float[]{7.7f, 8.8f, 9.9f}, (float[]) array[2], 0.0001f, "Third element should match");

        // Test toArray(T[]) with array of correct size
        float[][] typedArray = new float[3][];
        float[][] result = reader.toArray(typedArray);

        assertSame(typedArray, result, "Should return the same array instance when size matches");
        assertArrayEquals(new float[]{1.1f, 2.2f, 3.3f}, result[0], 0.0001f, "First element should match");
        assertArrayEquals(new float[]{4.4f, 5.5f, 6.6f}, result[1], 0.0001f, "Second element should match");
        assertArrayEquals(new float[]{7.7f, 8.8f, 9.9f}, result[2], 0.0001f, "Third element should match");

        // Test toArray(T[]) with array that's too small
        float[][] smallArray = new float[2][];
        float[][] expandedResult = reader.toArray(smallArray);

        assertNotSame(smallArray, expandedResult, "Should return a new array when input is too small");
        assertEquals(3, expandedResult.length, "Result array should have correct length");
        assertArrayEquals(new float[]{1.1f, 2.2f, 3.3f}, expandedResult[0], 0.0001f, "First element should match");
        assertArrayEquals(new float[]{4.4f, 5.5f, 6.6f}, expandedResult[1], 0.0001f, "Second element should match");
        assertArrayEquals(new float[]{7.7f, 8.8f, 9.9f}, expandedResult[2], 0.0001f, "Third element should match");

        // Test toArray(T[]) with array that's too large
        float[][] largeArray = new float[4][];
        float[][] paddedResult = reader.toArray(largeArray);

        assertSame(largeArray, paddedResult, "Should return the same array instance when size is larger");
        assertArrayEquals(new float[]{1.1f, 2.2f, 3.3f}, paddedResult[0], 0.0001f, "First element should match");
        assertArrayEquals(new float[]{4.4f, 5.5f, 6.6f}, paddedResult[1], 0.0001f, "Second element should match");
        assertArrayEquals(new float[]{7.7f, 8.8f, 9.9f}, paddedResult[2], 0.0001f, "Third element should match");
        assertNull(paddedResult[3], "Extra element should be null");

        // Close the reader
        reader.close();
    }

    @Test
    void testUnsupportedOperations() throws IOException {
        // Create test data
        float[][] testVectors = {{1.1f, 2.2f, 3.3f}};
        Path testFile = createTestFvecFile("unsupported.fvec", testVectors);

        // Test the reader
        UniformFvecReader reader = new UniformFvecReader();
        reader.open(testFile);

        // Test unsupported operations
        assertThrows(UnsupportedOperationException.class, () -> reader.add(new float[]{4.4f, 5.5f, 6.6f}), 
                    "add should throw UnsupportedOperationException");

        assertThrows(UnsupportedOperationException.class, () -> reader.add(0, new float[]{4.4f, 5.5f, 6.6f}), 
                    "add at index should throw UnsupportedOperationException");

        assertThrows(UnsupportedOperationException.class, () -> reader.addAll(Arrays.asList(new float[]{4.4f, 5.5f, 6.6f})), 
                    "addAll should throw UnsupportedOperationException");

        assertThrows(UnsupportedOperationException.class, () -> reader.addAll(0, Arrays.asList(new float[]{4.4f, 5.5f, 6.6f})), 
                    "addAll at index should throw UnsupportedOperationException");

        assertThrows(UnsupportedOperationException.class, () -> reader.remove(0), 
                    "remove by index should throw UnsupportedOperationException");

        assertThrows(UnsupportedOperationException.class, () -> reader.remove(new float[]{1.1f, 2.2f, 3.3f}), 
                    "remove by object should throw UnsupportedOperationException");

        assertThrows(UnsupportedOperationException.class, () -> reader.removeAll(Arrays.asList(new float[]{1.1f, 2.2f, 3.3f})), 
                    "removeAll should throw UnsupportedOperationException");

        assertThrows(UnsupportedOperationException.class, () -> reader.retainAll(Arrays.asList(new float[]{1.1f, 2.2f, 3.3f})), 
                    "retainAll should throw UnsupportedOperationException");

        assertThrows(UnsupportedOperationException.class, () -> reader.clear(), 
                    "clear should throw UnsupportedOperationException");

        assertThrows(UnsupportedOperationException.class, () -> reader.set(0, new float[]{4.4f, 5.5f, 6.6f}), 
                    "set should throw UnsupportedOperationException");

        // Close the reader
        reader.close();
    }

    @Test
    void testGetName() throws IOException {
        // Create test data
        float[][] testVectors = {{1.1f, 2.2f, 3.3f}};
        Path testFile = createTestFvecFile("name_test.fvec", testVectors);

        // Test the reader
        UniformFvecReader reader = new UniformFvecReader();
        reader.open(testFile);

        // Check name
        assertEquals("name_test.fvec", reader.getName(), "Name should match the filename");

        // Close the reader
        reader.close();
    }

    @Test
    void testGetDimension() throws IOException {
        // Create test data
        float[][] testVectors = {{1.1f, 2.2f, 3.3f, 4.4f, 5.5f}};
        Path testFile = createTestFvecFile("dimension_test.fvec", testVectors);

        // Test the reader
        UniformFvecReader reader = new UniformFvecReader();
        reader.open(testFile);

        // Check dimension
        assertEquals(5, reader.getDimension(), "Dimension should match the vector length");

        // Close the reader
        reader.close();
    }
}
