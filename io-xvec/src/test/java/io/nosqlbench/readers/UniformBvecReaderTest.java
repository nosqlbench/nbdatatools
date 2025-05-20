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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UniformBvecReaderTest {

    @TempDir
    Path tempDir;

    /**
     * Helper method to create a test bvec file with the given vectors
     */
    private Path createTestBvecFile(String filename, int[][] vectors) throws IOException {
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
    void testBasicAccess() throws IOException {
        // Create test data
        int[][] testVectors = {
            {1, 2, 3},
            {4, 5, 6},
            {7, 8, 9}
        };
        
        Path testFile = createTestBvecFile("basic_access.bvec", testVectors);
        
        // Test the reader
        UniformBvecReader reader = new UniformBvecReader();
        reader.open(testFile);
        
        // Check size
        assertEquals(3, reader.size(), "Reader should report 3 vectors");
        assertEquals(3, reader.getSize(), "getSize() should match size()");
        
        // Check get method
        assertArrayEquals(new int[]{1, 2, 3}, reader.get(0), "Vector at index 0 should match");
        assertArrayEquals(new int[]{4, 5, 6}, reader.get(1), "Vector at index 1 should match");
        assertArrayEquals(new int[]{7, 8, 9}, reader.get(2), "Vector at index 2 should match");
        
        // Test out of bounds
        assertThrows(IndexOutOfBoundsException.class, () -> reader.get(-1), "Should throw IndexOutOfBoundsException for negative index");
        assertThrows(IndexOutOfBoundsException.class, () -> reader.get(3), "Should throw IndexOutOfBoundsException for index >= size");
        
        // Close the reader
        reader.close();
    }

    @Test
    void testListMethods() throws IOException {
        // Create test data
        int[][] testVectors = {
            {1, 2, 3},
            {4, 5, 6},
            {7, 8, 9}
        };
        
        Path testFile = createTestBvecFile("list_methods.bvec", testVectors);
        
        // Test the reader
        UniformBvecReader reader = new UniformBvecReader();
        reader.open(testFile);
        
        // Test isEmpty
        assertFalse(reader.isEmpty(), "Reader should not be empty");
        
        // Test contains
        assertTrue(reader.contains(new int[]{1, 2, 3}), "Reader should contain [1, 2, 3]");
        assertTrue(reader.contains(new int[]{4, 5, 6}), "Reader should contain [4, 5, 6]");
        assertTrue(reader.contains(new int[]{7, 8, 9}), "Reader should contain [7, 8, 9]");
        assertFalse(reader.contains(new int[]{10, 11, 12}), "Reader should not contain [10, 11, 12]");
        assertFalse(reader.contains(new int[]{1, 2}), "Reader should not contain [1, 2] (wrong dimension)");
        
        // Test indexOf
        assertEquals(0, reader.indexOf(new int[]{1, 2, 3}), "indexOf should find [1, 2, 3] at index 0");
        assertEquals(1, reader.indexOf(new int[]{4, 5, 6}), "indexOf should find [4, 5, 6] at index 1");
        assertEquals(2, reader.indexOf(new int[]{7, 8, 9}), "indexOf should find [7, 8, 9] at index 2");
        assertEquals(-1, reader.indexOf(new int[]{10, 11, 12}), "indexOf should return -1 for non-existent vector");
        
        // Test lastIndexOf
        assertEquals(0, reader.lastIndexOf(new int[]{1, 2, 3}), "lastIndexOf should find [1, 2, 3] at index 0");
        assertEquals(1, reader.lastIndexOf(new int[]{4, 5, 6}), "lastIndexOf should find [4, 5, 6] at index 1");
        assertEquals(2, reader.lastIndexOf(new int[]{7, 8, 9}), "lastIndexOf should find [7, 8, 9] at index 2");
        assertEquals(-1, reader.lastIndexOf(new int[]{10, 11, 12}), "lastIndexOf should return -1 for non-existent vector");
        
        // Test containsAll
        assertTrue(reader.containsAll(Arrays.asList(new int[]{1, 2, 3}, new int[]{4, 5, 6})), 
                  "containsAll should return true for contained vectors");
        assertFalse(reader.containsAll(Arrays.asList(new int[]{1, 2, 3}, new int[]{10, 11, 12})), 
                   "containsAll should return false if any vector is not contained");
        
        // Close the reader
        reader.close();
    }

    @Test
    void testIterator() throws IOException {
        // Create test data
        int[][] testVectors = {
            {1, 2, 3},
            {4, 5, 6},
            {7, 8, 9}
        };
        
        Path testFile = createTestBvecFile("iterator.bvec", testVectors);
        
        // Test the reader
        UniformBvecReader reader = new UniformBvecReader();
        reader.open(testFile);
        
        // Test iterator
        Iterator<int[]> iterator = reader.iterator();
        
        assertTrue(iterator.hasNext(), "Iterator should have first element");
        assertArrayEquals(new int[]{1, 2, 3}, iterator.next(), "First vector should match");
        
        assertTrue(iterator.hasNext(), "Iterator should have second element");
        assertArrayEquals(new int[]{4, 5, 6}, iterator.next(), "Second vector should match");
        
        assertTrue(iterator.hasNext(), "Iterator should have third element");
        assertArrayEquals(new int[]{7, 8, 9}, iterator.next(), "Third vector should match");
        
        assertFalse(iterator.hasNext(), "Iterator should be exhausted");
        assertThrows(NoSuchElementException.class, iterator::next, "Should throw NoSuchElementException when reading past the end");
        
        // Close the reader
        reader.close();
    }

    @Test
    void testListIterator() throws IOException {
        // Create test data
        int[][] testVectors = {
            {1, 2, 3},
            {4, 5, 6},
            {7, 8, 9}
        };
        
        Path testFile = createTestBvecFile("list_iterator.bvec", testVectors);
        
        // Test the reader
        UniformBvecReader reader = new UniformBvecReader();
        reader.open(testFile);
        
        // Test list iterator
        ListIterator<int[]> iterator = reader.listIterator();
        
        // Forward iteration
        assertTrue(iterator.hasNext(), "Iterator should have next element");
        assertFalse(iterator.hasPrevious(), "Iterator should not have previous element at start");
        assertEquals(0, iterator.nextIndex(), "Next index should be 0");
        assertEquals(-1, iterator.previousIndex(), "Previous index should be -1");
        
        assertArrayEquals(new int[]{1, 2, 3}, iterator.next(), "First vector should match");
        
        assertTrue(iterator.hasNext(), "Iterator should have next element");
        assertTrue(iterator.hasPrevious(), "Iterator should have previous element");
        assertEquals(1, iterator.nextIndex(), "Next index should be 1");
        assertEquals(0, iterator.previousIndex(), "Previous index should be 0");
        
        assertArrayEquals(new int[]{4, 5, 6}, iterator.next(), "Second vector should match");
        
        assertTrue(iterator.hasNext(), "Iterator should have next element");
        assertTrue(iterator.hasPrevious(), "Iterator should have previous element");
        assertEquals(2, iterator.nextIndex(), "Next index should be 2");
        assertEquals(1, iterator.previousIndex(), "Previous index should be 1");
        
        assertArrayEquals(new int[]{7, 8, 9}, iterator.next(), "Third vector should match");
        
        assertFalse(iterator.hasNext(), "Iterator should be exhausted");
        assertTrue(iterator.hasPrevious(), "Iterator should have previous element");
        assertEquals(3, iterator.nextIndex(), "Next index should be 3");
        assertEquals(2, iterator.previousIndex(), "Previous index should be 2");
        
        // Backward iteration
        assertArrayEquals(new int[]{7, 8, 9}, iterator.previous(), "Third vector should match");
        assertArrayEquals(new int[]{4, 5, 6}, iterator.previous(), "Second vector should match");
        assertArrayEquals(new int[]{1, 2, 3}, iterator.previous(), "First vector should match");
        
        assertFalse(iterator.hasPrevious(), "Iterator should be exhausted in reverse");
        assertTrue(iterator.hasNext(), "Iterator should have next element");
        
        // Close the reader
        reader.close();
    }

    @Test
    void testSubList() throws IOException {
        // Create test data
        int[][] testVectors = {
            {1, 2, 3},
            {4, 5, 6},
            {7, 8, 9},
            {10, 11, 12},
            {13, 14, 15}
        };
        
        Path testFile = createTestBvecFile("sublist.bvec", testVectors);
        
        // Test the reader
        UniformBvecReader reader = new UniformBvecReader();
        reader.open(testFile);
        
        // Test subList
        List<int[]> subList = reader.subList(1, 4);
        
        assertEquals(3, subList.size(), "Sublist should have 3 elements");
        assertArrayEquals(new int[]{4, 5, 6}, subList.get(0), "First element of sublist should match");
        assertArrayEquals(new int[]{7, 8, 9}, subList.get(1), "Second element of sublist should match");
        assertArrayEquals(new int[]{10, 11, 12}, subList.get(2), "Third element of sublist should match");
        
        // Test subList of subList
        List<int[]> subSubList = subList.subList(1, 3);
        
        assertEquals(2, subSubList.size(), "Sub-sublist should have 2 elements");
        assertArrayEquals(new int[]{7, 8, 9}, subSubList.get(0), "First element of sub-sublist should match");
        assertArrayEquals(new int[]{10, 11, 12}, subSubList.get(1), "Second element of sub-sublist should match");
        
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
        int[][] testVectors = {
            {1, 2, 3},
            {4, 5, 6},
            {7, 8, 9}
        };
        
        Path testFile = createTestBvecFile("to_array.bvec", testVectors);
        
        // Test the reader
        UniformBvecReader reader = new UniformBvecReader();
        reader.open(testFile);
        
        // Test toArray()
        Object[] array = reader.toArray();
        
        assertEquals(3, array.length, "Array should have 3 elements");
        assertArrayEquals(new int[]{1, 2, 3}, (int[]) array[0], "First element should match");
        assertArrayEquals(new int[]{4, 5, 6}, (int[]) array[1], "Second element should match");
        assertArrayEquals(new int[]{7, 8, 9}, (int[]) array[2], "Third element should match");
        
        // Test toArray(T[]) with array of correct size
        int[][] typedArray = new int[3][];
        int[][] result = reader.toArray(typedArray);
        
        assertSame(typedArray, result, "Should return the same array instance when size matches");
        assertArrayEquals(new int[]{1, 2, 3}, result[0], "First element should match");
        assertArrayEquals(new int[]{4, 5, 6}, result[1], "Second element should match");
        assertArrayEquals(new int[]{7, 8, 9}, result[2], "Third element should match");
        
        // Test toArray(T[]) with array that's too small
        int[][] smallArray = new int[2][];
        int[][] expandedResult = reader.toArray(smallArray);
        
        assertNotSame(smallArray, expandedResult, "Should return a new array when input is too small");
        assertEquals(3, expandedResult.length, "Result array should have correct length");
        assertArrayEquals(new int[]{1, 2, 3}, expandedResult[0], "First element should match");
        assertArrayEquals(new int[]{4, 5, 6}, expandedResult[1], "Second element should match");
        assertArrayEquals(new int[]{7, 8, 9}, expandedResult[2], "Third element should match");
        
        // Test toArray(T[]) with array that's too large
        int[][] largeArray = new int[4][];
        int[][] paddedResult = reader.toArray(largeArray);
        
        assertSame(largeArray, paddedResult, "Should return the same array instance when size is larger");
        assertArrayEquals(new int[]{1, 2, 3}, paddedResult[0], "First element should match");
        assertArrayEquals(new int[]{4, 5, 6}, paddedResult[1], "Second element should match");
        assertArrayEquals(new int[]{7, 8, 9}, paddedResult[2], "Third element should match");
        assertNull(paddedResult[3], "Extra element should be null");
        
        // Close the reader
        reader.close();
    }

    @Test
    void testUnsupportedOperations() throws IOException {
        // Create test data
        int[][] testVectors = {{1, 2, 3}};
        Path testFile = createTestBvecFile("unsupported.bvec", testVectors);
        
        // Test the reader
        UniformBvecReader reader = new UniformBvecReader();
        reader.open(testFile);
        
        // Test unsupported operations
        assertThrows(UnsupportedOperationException.class, () -> reader.add(new int[]{4, 5, 6}), 
                    "add should throw UnsupportedOperationException");
        
        assertThrows(UnsupportedOperationException.class, () -> reader.add(0, new int[]{4, 5, 6}), 
                    "add at index should throw UnsupportedOperationException");
        
        assertThrows(UnsupportedOperationException.class, () -> reader.addAll(Arrays.asList(new int[]{4, 5, 6})), 
                    "addAll should throw UnsupportedOperationException");
        
        assertThrows(UnsupportedOperationException.class, () -> reader.addAll(0, Arrays.asList(new int[]{4, 5, 6})), 
                    "addAll at index should throw UnsupportedOperationException");
        
        assertThrows(UnsupportedOperationException.class, () -> reader.remove(0), 
                    "remove by index should throw UnsupportedOperationException");
        
        assertThrows(UnsupportedOperationException.class, () -> reader.remove(new int[]{1, 2, 3}), 
                    "remove by object should throw UnsupportedOperationException");
        
        assertThrows(UnsupportedOperationException.class, () -> reader.removeAll(Arrays.asList(new int[]{1, 2, 3})), 
                    "removeAll should throw UnsupportedOperationException");
        
        assertThrows(UnsupportedOperationException.class, () -> reader.retainAll(Arrays.asList(new int[]{1, 2, 3})), 
                    "retainAll should throw UnsupportedOperationException");
        
        assertThrows(UnsupportedOperationException.class, () -> reader.clear(), 
                    "clear should throw UnsupportedOperationException");
        
        assertThrows(UnsupportedOperationException.class, () -> reader.set(0, new int[]{4, 5, 6}), 
                    "set should throw UnsupportedOperationException");
        
        // Close the reader
        reader.close();
    }

    @Test
    void testGetName() throws IOException {
        // Create test data
        int[][] testVectors = {{1, 2, 3}};
        Path testFile = createTestBvecFile("name_test.bvec", testVectors);
        
        // Test the reader
        UniformBvecReader reader = new UniformBvecReader();
        reader.open(testFile);
        
        // Check name
        assertEquals("name_test.bvec", reader.getName(), "Name should match the filename");
        
        // Close the reader
        reader.close();
    }

    @Test
    void testGetDimension() throws IOException {
        // Create test data
        int[][] testVectors = {{1, 2, 3, 4, 5}};
        Path testFile = createTestBvecFile("dimension_test.bvec", testVectors);
        
        // Test the reader
        UniformBvecReader reader = new UniformBvecReader();
        reader.open(testFile);
        
        // Check dimension
        assertEquals(5, reader.getDimension(), "Dimension should match the vector length");
        
        // Close the reader
        reader.close();
    }
}