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

class UniformDvecReaderTest {

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
    void testBasicAccess() throws IOException {
        // Create test data
        double[][] testVectors = {
            {1.1, 2.2, 3.3},
            {4.4, 5.5, 6.6},
            {7.7, 8.8, 9.9}
        };
        
        Path testFile = createTestDvecFile("basic_access.dvec", testVectors);
        
        // Test the reader
        UniformDvecReader reader = new UniformDvecReader();
        reader.open(testFile);
        
        // Check size
        assertEquals(3, reader.size(), "Reader should report 3 vectors");
        assertEquals(3, reader.getSize(), "getSize() should match size()");
        
        // Check get method
        assertArrayEquals(new double[]{1.1, 2.2, 3.3}, reader.get(0), 0.0001, "Vector at index 0 should match");
        assertArrayEquals(new double[]{4.4, 5.5, 6.6}, reader.get(1), 0.0001, "Vector at index 1 should match");
        assertArrayEquals(new double[]{7.7, 8.8, 9.9}, reader.get(2), 0.0001, "Vector at index 2 should match");
        
        // Test out of bounds
        assertThrows(IndexOutOfBoundsException.class, () -> reader.get(-1), "Should throw IndexOutOfBoundsException for negative index");
        assertThrows(IndexOutOfBoundsException.class, () -> reader.get(3), "Should throw IndexOutOfBoundsException for index >= size");
        
        // Close the reader
        reader.close();
    }

    @Test
    void testListMethods() throws IOException {
        // Create test data
        double[][] testVectors = {
            {1.1, 2.2, 3.3},
            {4.4, 5.5, 6.6},
            {7.7, 8.8, 9.9}
        };
        
        Path testFile = createTestDvecFile("list_methods.dvec", testVectors);
        
        // Test the reader
        UniformDvecReader reader = new UniformDvecReader();
        reader.open(testFile);
        
        // Test isEmpty
        assertFalse(reader.isEmpty(), "Reader should not be empty");
        
        // Test contains
        assertTrue(reader.contains(new double[]{1.1, 2.2, 3.3}), "Reader should contain [1.1, 2.2, 3.3]");
        assertTrue(reader.contains(new double[]{4.4, 5.5, 6.6}), "Reader should contain [4.4, 5.5, 6.6]");
        assertTrue(reader.contains(new double[]{7.7, 8.8, 9.9}), "Reader should contain [7.7, 8.8, 9.9]");
        assertFalse(reader.contains(new double[]{10.1, 11.2, 12.3}), "Reader should not contain [10.1, 11.2, 12.3]");
        assertFalse(reader.contains(new double[]{1.1, 2.2}), "Reader should not contain [1.1, 2.2] (wrong dimension)");
        
        // Test indexOf
        assertEquals(0, reader.indexOf(new double[]{1.1, 2.2, 3.3}), "indexOf should find [1.1, 2.2, 3.3] at index 0");
        assertEquals(1, reader.indexOf(new double[]{4.4, 5.5, 6.6}), "indexOf should find [4.4, 5.5, 6.6] at index 1");
        assertEquals(2, reader.indexOf(new double[]{7.7, 8.8, 9.9}), "indexOf should find [7.7, 8.8, 9.9] at index 2");
        assertEquals(-1, reader.indexOf(new double[]{10.1, 11.2, 12.3}), "indexOf should return -1 for non-existent vector");
        
        // Test lastIndexOf
        assertEquals(0, reader.lastIndexOf(new double[]{1.1, 2.2, 3.3}), "lastIndexOf should find [1.1, 2.2, 3.3] at index 0");
        assertEquals(1, reader.lastIndexOf(new double[]{4.4, 5.5, 6.6}), "lastIndexOf should find [4.4, 5.5, 6.6] at index 1");
        assertEquals(2, reader.lastIndexOf(new double[]{7.7, 8.8, 9.9}), "lastIndexOf should find [7.7, 8.8, 9.9] at index 2");
        assertEquals(-1, reader.lastIndexOf(new double[]{10.1, 11.2, 12.3}), "lastIndexOf should return -1 for non-existent vector");
        
        // Test containsAll
        assertTrue(reader.containsAll(Arrays.asList(new double[]{1.1, 2.2, 3.3}, new double[]{4.4, 5.5, 6.6})), 
                  "containsAll should return true for contained vectors");
        assertFalse(reader.containsAll(Arrays.asList(new double[]{1.1, 2.2, 3.3}, new double[]{10.1, 11.2, 12.3})), 
                   "containsAll should return false if any vector is not contained");
        
        // Close the reader
        reader.close();
    }

    @Test
    void testIterator() throws IOException {
        // Create test data
        double[][] testVectors = {
            {1.1, 2.2, 3.3},
            {4.4, 5.5, 6.6},
            {7.7, 8.8, 9.9}
        };
        
        Path testFile = createTestDvecFile("iterator.dvec", testVectors);
        
        // Test the reader
        UniformDvecReader reader = new UniformDvecReader();
        reader.open(testFile);
        
        // Test iterator
        Iterator<double[]> iterator = reader.iterator();
        
        assertTrue(iterator.hasNext(), "Iterator should have first element");
        assertArrayEquals(new double[]{1.1, 2.2, 3.3}, iterator.next(), 0.0001, "First vector should match");
        
        assertTrue(iterator.hasNext(), "Iterator should have second element");
        assertArrayEquals(new double[]{4.4, 5.5, 6.6}, iterator.next(), 0.0001, "Second vector should match");
        
        assertTrue(iterator.hasNext(), "Iterator should have third element");
        assertArrayEquals(new double[]{7.7, 8.8, 9.9}, iterator.next(), 0.0001, "Third vector should match");
        
        assertFalse(iterator.hasNext(), "Iterator should be exhausted");
        assertThrows(NoSuchElementException.class, iterator::next, "Should throw NoSuchElementException when reading past the end");
        
        // Close the reader
        reader.close();
    }

    @Test
    void testListIterator() throws IOException {
        // Create test data
        double[][] testVectors = {
            {1.1, 2.2, 3.3},
            {4.4, 5.5, 6.6},
            {7.7, 8.8, 9.9}
        };
        
        Path testFile = createTestDvecFile("list_iterator.dvec", testVectors);
        
        // Test the reader
        UniformDvecReader reader = new UniformDvecReader();
        reader.open(testFile);
        
        // Test list iterator
        ListIterator<double[]> iterator = reader.listIterator();
        
        // Forward iteration
        assertTrue(iterator.hasNext(), "Iterator should have next element");
        assertFalse(iterator.hasPrevious(), "Iterator should not have previous element at start");
        assertEquals(0, iterator.nextIndex(), "Next index should be 0");
        assertEquals(-1, iterator.previousIndex(), "Previous index should be -1");
        
        assertArrayEquals(new double[]{1.1, 2.2, 3.3}, iterator.next(), 0.0001, "First vector should match");
        
        assertTrue(iterator.hasNext(), "Iterator should have next element");
        assertTrue(iterator.hasPrevious(), "Iterator should have previous element");
        assertEquals(1, iterator.nextIndex(), "Next index should be 1");
        assertEquals(0, iterator.previousIndex(), "Previous index should be 0");
        
        assertArrayEquals(new double[]{4.4, 5.5, 6.6}, iterator.next(), 0.0001, "Second vector should match");
        
        assertTrue(iterator.hasNext(), "Iterator should have next element");
        assertTrue(iterator.hasPrevious(), "Iterator should have previous element");
        assertEquals(2, iterator.nextIndex(), "Next index should be 2");
        assertEquals(1, iterator.previousIndex(), "Previous index should be 1");
        
        assertArrayEquals(new double[]{7.7, 8.8, 9.9}, iterator.next(), 0.0001, "Third vector should match");
        
        assertFalse(iterator.hasNext(), "Iterator should be exhausted");
        assertTrue(iterator.hasPrevious(), "Iterator should have previous element");
        assertEquals(3, iterator.nextIndex(), "Next index should be 3");
        assertEquals(2, iterator.previousIndex(), "Previous index should be 2");
        
        // Backward iteration
        assertArrayEquals(new double[]{7.7, 8.8, 9.9}, iterator.previous(), 0.0001, "Third vector should match");
        assertArrayEquals(new double[]{4.4, 5.5, 6.6}, iterator.previous(), 0.0001, "Second vector should match");
        assertArrayEquals(new double[]{1.1, 2.2, 3.3}, iterator.previous(), 0.0001, "First vector should match");
        
        assertFalse(iterator.hasPrevious(), "Iterator should be exhausted in reverse");
        assertTrue(iterator.hasNext(), "Iterator should have next element");
        
        // Close the reader
        reader.close();
    }

    @Test
    void testSubList() throws IOException {
        // Create test data
        double[][] testVectors = {
            {1.1, 2.2, 3.3},
            {4.4, 5.5, 6.6},
            {7.7, 8.8, 9.9},
            {10.1, 11.2, 12.3},
            {13.4, 14.5, 15.6}
        };
        
        Path testFile = createTestDvecFile("sublist.dvec", testVectors);
        
        // Test the reader
        UniformDvecReader reader = new UniformDvecReader();
        reader.open(testFile);
        
        // Test subList
        List<double[]> subList = reader.subList(1, 4);
        
        assertEquals(3, subList.size(), "Sublist should have 3 elements");
        assertArrayEquals(new double[]{4.4, 5.5, 6.6}, subList.get(0), 0.0001, "First element of sublist should match");
        assertArrayEquals(new double[]{7.7, 8.8, 9.9}, subList.get(1), 0.0001, "Second element of sublist should match");
        assertArrayEquals(new double[]{10.1, 11.2, 12.3}, subList.get(2), 0.0001, "Third element of sublist should match");
        
        // Test subList of subList
        List<double[]> subSubList = subList.subList(1, 3);
        
        assertEquals(2, subSubList.size(), "Sub-sublist should have 2 elements");
        assertArrayEquals(new double[]{7.7, 8.8, 9.9}, subSubList.get(0), 0.0001, "First element of sub-sublist should match");
        assertArrayEquals(new double[]{10.1, 11.2, 12.3}, subSubList.get(1), 0.0001, "Second element of sub-sublist should match");
        
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
        double[][] testVectors = {
            {1.1, 2.2, 3.3},
            {4.4, 5.5, 6.6},
            {7.7, 8.8, 9.9}
        };
        
        Path testFile = createTestDvecFile("to_array.dvec", testVectors);
        
        // Test the reader
        UniformDvecReader reader = new UniformDvecReader();
        reader.open(testFile);
        
        // Test toArray()
        Object[] array = reader.toArray();
        
        assertEquals(3, array.length, "Array should have 3 elements");
        assertArrayEquals(new double[]{1.1, 2.2, 3.3}, (double[]) array[0], 0.0001, "First element should match");
        assertArrayEquals(new double[]{4.4, 5.5, 6.6}, (double[]) array[1], 0.0001, "Second element should match");
        assertArrayEquals(new double[]{7.7, 8.8, 9.9}, (double[]) array[2], 0.0001, "Third element should match");
        
        // Test toArray(T[]) with array of correct size
        double[][] typedArray = new double[3][];
        double[][] result = reader.toArray(typedArray);
        
        assertSame(typedArray, result, "Should return the same array instance when size matches");
        assertArrayEquals(new double[]{1.1, 2.2, 3.3}, result[0], 0.0001, "First element should match");
        assertArrayEquals(new double[]{4.4, 5.5, 6.6}, result[1], 0.0001, "Second element should match");
        assertArrayEquals(new double[]{7.7, 8.8, 9.9}, result[2], 0.0001, "Third element should match");
        
        // Test toArray(T[]) with array that's too small
        double[][] smallArray = new double[2][];
        double[][] expandedResult = reader.toArray(smallArray);
        
        assertNotSame(smallArray, expandedResult, "Should return a new array when input is too small");
        assertEquals(3, expandedResult.length, "Result array should have correct length");
        assertArrayEquals(new double[]{1.1, 2.2, 3.3}, expandedResult[0], 0.0001, "First element should match");
        assertArrayEquals(new double[]{4.4, 5.5, 6.6}, expandedResult[1], 0.0001, "Second element should match");
        assertArrayEquals(new double[]{7.7, 8.8, 9.9}, expandedResult[2], 0.0001, "Third element should match");
        
        // Test toArray(T[]) with array that's too large
        double[][] largeArray = new double[4][];
        double[][] paddedResult = reader.toArray(largeArray);
        
        assertSame(largeArray, paddedResult, "Should return the same array instance when size is larger");
        assertArrayEquals(new double[]{1.1, 2.2, 3.3}, paddedResult[0], 0.0001, "First element should match");
        assertArrayEquals(new double[]{4.4, 5.5, 6.6}, paddedResult[1], 0.0001, "Second element should match");
        assertArrayEquals(new double[]{7.7, 8.8, 9.9}, paddedResult[2], 0.0001, "Third element should match");
        assertNull(paddedResult[3], "Extra element should be null");
        
        // Close the reader
        reader.close();
    }

    @Test
    void testUnsupportedOperations() throws IOException {
        // Create test data
        double[][] testVectors = {{1.1, 2.2, 3.3}};
        Path testFile = createTestDvecFile("unsupported.dvec", testVectors);
        
        // Test the reader
        UniformDvecReader reader = new UniformDvecReader();
        reader.open(testFile);
        
        // Test unsupported operations
        assertThrows(UnsupportedOperationException.class, () -> reader.add(new double[]{4.4, 5.5, 6.6}), 
                    "add should throw UnsupportedOperationException");
        
        assertThrows(UnsupportedOperationException.class, () -> reader.add(0, new double[]{4.4, 5.5, 6.6}), 
                    "add at index should throw UnsupportedOperationException");
        
        assertThrows(UnsupportedOperationException.class, () -> reader.addAll(Arrays.asList(new double[]{4.4, 5.5, 6.6})), 
                    "addAll should throw UnsupportedOperationException");
        
        assertThrows(UnsupportedOperationException.class, () -> reader.addAll(0, Arrays.asList(new double[]{4.4, 5.5, 6.6})), 
                    "addAll at index should throw UnsupportedOperationException");
        
        assertThrows(UnsupportedOperationException.class, () -> reader.remove(0), 
                    "remove by index should throw UnsupportedOperationException");
        
        assertThrows(UnsupportedOperationException.class, () -> reader.remove(new double[]{1.1, 2.2, 3.3}), 
                    "remove by object should throw UnsupportedOperationException");
        
        assertThrows(UnsupportedOperationException.class, () -> reader.removeAll(Arrays.asList(new double[]{1.1, 2.2, 3.3})), 
                    "removeAll should throw UnsupportedOperationException");
        
        assertThrows(UnsupportedOperationException.class, () -> reader.retainAll(Arrays.asList(new double[]{1.1, 2.2, 3.3})), 
                    "retainAll should throw UnsupportedOperationException");
        
        assertThrows(UnsupportedOperationException.class, () -> reader.clear(), 
                    "clear should throw UnsupportedOperationException");
        
        assertThrows(UnsupportedOperationException.class, () -> reader.set(0, new double[]{4.4, 5.5, 6.6}), 
                    "set should throw UnsupportedOperationException");
        
        // Close the reader
        reader.close();
    }

    @Test
    void testGetName() throws IOException {
        // Create test data
        double[][] testVectors = {{1.1, 2.2, 3.3}};
        Path testFile = createTestDvecFile("name_test.dvec", testVectors);
        
        // Test the reader
        UniformDvecReader reader = new UniformDvecReader();
        reader.open(testFile);
        
        // Check name
        assertEquals("name_test.dvec", reader.getName(), "Name should match the filename");
        
        // Close the reader
        reader.close();
    }

    @Test
    void testGetDimension() throws IOException {
        // Create test data
        double[][] testVectors = {{1.1, 2.2, 3.3, 4.4, 5.5}};
        Path testFile = createTestDvecFile("dimension_test.dvec", testVectors);
        
        // Test the reader
        UniformDvecReader reader = new UniformDvecReader();
        reader.open(testFile);
        
        // Check dimension
        assertEquals(5, reader.getDimension(), "Dimension should match the vector length");
        
        // Close the reader
        reader.close();
    }
}