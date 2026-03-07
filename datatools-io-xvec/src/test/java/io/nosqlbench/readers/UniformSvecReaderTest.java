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

import static org.junit.jupiter.api.Assertions.*;

class UniformSvecReaderTest {

    @TempDir
    Path tempDir;

    /// Helper method to create a test svec file with the given vectors.
    private Path createTestSvecFile(String filename, short[][] vectors) throws IOException {
        Path testFile = tempDir.resolve(filename);

        try (var outputStream = Files.newOutputStream(testFile)) {
            for (short[] vector : vectors) {
                // Write dimension (4 bytes)
                ByteBuffer dimBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
                dimBuffer.putInt(vector.length);
                outputStream.write(dimBuffer.array());

                // Write vector values (2 bytes each)
                ByteBuffer valueBuffer = ByteBuffer.allocate(vector.length * 2).order(ByteOrder.LITTLE_ENDIAN);
                for (short value : vector) {
                    valueBuffer.putShort(value);
                }
                outputStream.write(valueBuffer.array());
            }
        }

        return testFile;
    }

    @Test
    void testBasicAccess() throws IOException {
        short[][] testVectors = {
            {1, 2, 3},
            {4, 5, 6},
            {7, 8, 9}
        };

        Path testFile = createTestSvecFile("basic_access.svec", testVectors);

        UniformSvecReader reader = new UniformSvecReader();
        reader.open(testFile);

        assertEquals(3, reader.size());
        assertEquals(3, reader.getSize());

        assertArrayEquals(new short[]{1, 2, 3}, reader.get(0));
        assertArrayEquals(new short[]{4, 5, 6}, reader.get(1));
        assertArrayEquals(new short[]{7, 8, 9}, reader.get(2));

        assertThrows(IndexOutOfBoundsException.class, () -> reader.get(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> reader.get(3));

        reader.close();
    }

    @Test
    void testNegativeValues() throws IOException {
        short[][] testVectors = {
            {-1, -32768, 32767},
            {0, -100, 100}
        };

        Path testFile = createTestSvecFile("negative.svec", testVectors);

        UniformSvecReader reader = new UniformSvecReader();
        reader.open(testFile);

        assertArrayEquals(new short[]{-1, -32768, 32767}, reader.get(0));
        assertArrayEquals(new short[]{0, -100, 100}, reader.get(1));

        reader.close();
    }

    @Test
    void testListMethods() throws IOException {
        short[][] testVectors = {
            {1, 2, 3},
            {4, 5, 6},
            {7, 8, 9}
        };

        Path testFile = createTestSvecFile("list_methods.svec", testVectors);

        UniformSvecReader reader = new UniformSvecReader();
        reader.open(testFile);

        assertFalse(reader.isEmpty());

        assertTrue(reader.contains(new short[]{1, 2, 3}));
        assertTrue(reader.contains(new short[]{4, 5, 6}));
        assertFalse(reader.contains(new short[]{10, 11, 12}));
        assertFalse(reader.contains(new short[]{1, 2}));

        assertEquals(0, reader.indexOf(new short[]{1, 2, 3}));
        assertEquals(1, reader.indexOf(new short[]{4, 5, 6}));
        assertEquals(-1, reader.indexOf(new short[]{10, 11, 12}));

        assertEquals(2, reader.lastIndexOf(new short[]{7, 8, 9}));
        assertEquals(-1, reader.lastIndexOf(new short[]{10, 11, 12}));

        assertTrue(reader.containsAll(Arrays.asList(new short[]{1, 2, 3}, new short[]{4, 5, 6})));
        assertFalse(reader.containsAll(Arrays.asList(new short[]{1, 2, 3}, new short[]{10, 11, 12})));

        reader.close();
    }

    @Test
    void testIterator() throws IOException {
        short[][] testVectors = {
            {1, 2, 3},
            {4, 5, 6},
            {7, 8, 9}
        };

        Path testFile = createTestSvecFile("iterator.svec", testVectors);

        UniformSvecReader reader = new UniformSvecReader();
        reader.open(testFile);

        Iterator<short[]> iterator = reader.iterator();

        assertTrue(iterator.hasNext());
        assertArrayEquals(new short[]{1, 2, 3}, iterator.next());
        assertTrue(iterator.hasNext());
        assertArrayEquals(new short[]{4, 5, 6}, iterator.next());
        assertTrue(iterator.hasNext());
        assertArrayEquals(new short[]{7, 8, 9}, iterator.next());
        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::next);

        reader.close();
    }

    @Test
    void testListIterator() throws IOException {
        short[][] testVectors = {
            {1, 2, 3},
            {4, 5, 6},
            {7, 8, 9}
        };

        Path testFile = createTestSvecFile("list_iterator.svec", testVectors);

        UniformSvecReader reader = new UniformSvecReader();
        reader.open(testFile);

        ListIterator<short[]> iterator = reader.listIterator();

        assertTrue(iterator.hasNext());
        assertFalse(iterator.hasPrevious());

        assertArrayEquals(new short[]{1, 2, 3}, iterator.next());
        assertArrayEquals(new short[]{4, 5, 6}, iterator.next());
        assertArrayEquals(new short[]{7, 8, 9}, iterator.next());

        assertFalse(iterator.hasNext());
        assertTrue(iterator.hasPrevious());

        // Backward
        assertArrayEquals(new short[]{7, 8, 9}, iterator.previous());
        assertArrayEquals(new short[]{4, 5, 6}, iterator.previous());
        assertArrayEquals(new short[]{1, 2, 3}, iterator.previous());

        assertFalse(iterator.hasPrevious());
        assertTrue(iterator.hasNext());

        reader.close();
    }

    @Test
    void testSubList() throws IOException {
        short[][] testVectors = {
            {1, 2, 3},
            {4, 5, 6},
            {7, 8, 9},
            {10, 11, 12},
            {13, 14, 15}
        };

        Path testFile = createTestSvecFile("sublist.svec", testVectors);

        UniformSvecReader reader = new UniformSvecReader();
        reader.open(testFile);

        List<short[]> subList = reader.subList(1, 4);

        assertEquals(3, subList.size());
        assertArrayEquals(new short[]{4, 5, 6}, subList.get(0));
        assertArrayEquals(new short[]{7, 8, 9}, subList.get(1));
        assertArrayEquals(new short[]{10, 11, 12}, subList.get(2));

        List<short[]> subSubList = subList.subList(1, 3);
        assertEquals(2, subSubList.size());
        assertArrayEquals(new short[]{7, 8, 9}, subSubList.get(0));
        assertArrayEquals(new short[]{10, 11, 12}, subSubList.get(1));

        assertThrows(IndexOutOfBoundsException.class, () -> reader.subList(-1, 3));
        assertThrows(IndexOutOfBoundsException.class, () -> reader.subList(0, 6));
        assertThrows(IndexOutOfBoundsException.class, () -> reader.subList(3, 2));

        reader.close();
    }

    @Test
    void testToArray() throws IOException {
        short[][] testVectors = {
            {1, 2, 3},
            {4, 5, 6},
            {7, 8, 9}
        };

        Path testFile = createTestSvecFile("to_array.svec", testVectors);

        UniformSvecReader reader = new UniformSvecReader();
        reader.open(testFile);

        Object[] array = reader.toArray();
        assertEquals(3, array.length);
        assertArrayEquals(new short[]{1, 2, 3}, (short[]) array[0]);
        assertArrayEquals(new short[]{4, 5, 6}, (short[]) array[1]);
        assertArrayEquals(new short[]{7, 8, 9}, (short[]) array[2]);

        short[][] typedArray = new short[3][];
        short[][] result = (short[][]) reader.toArray(typedArray);
        assertSame(typedArray, result);
        assertArrayEquals(new short[]{1, 2, 3}, result[0]);

        short[][] smallArray = new short[1][];
        short[][] expandedResult = (short[][]) reader.toArray(smallArray);
        assertNotSame(smallArray, expandedResult);
        assertEquals(3, expandedResult.length);

        reader.close();
    }

    @Test
    void testUnsupportedOperations() throws IOException {
        short[][] testVectors = {{1, 2, 3}};
        Path testFile = createTestSvecFile("unsupported.svec", testVectors);

        UniformSvecReader reader = new UniformSvecReader();
        reader.open(testFile);

        assertThrows(UnsupportedOperationException.class, () -> reader.add(new short[]{4, 5, 6}));
        assertThrows(UnsupportedOperationException.class, () -> reader.remove(0));
        assertThrows(UnsupportedOperationException.class, () -> reader.clear());
        assertThrows(UnsupportedOperationException.class, () -> reader.set(0, new short[]{4, 5, 6}));

        reader.close();
    }

    @Test
    void testGetName() throws IOException {
        short[][] testVectors = {{1, 2, 3}};
        Path testFile = createTestSvecFile("name_test.svec", testVectors);

        UniformSvecReader reader = new UniformSvecReader();
        reader.open(testFile);

        assertEquals("name_test.svec", reader.getName());

        reader.close();
    }

    @Test
    void testGetDimension() throws IOException {
        short[][] testVectors = {{1, 2, 3, 4, 5}};
        Path testFile = createTestSvecFile("dimension_test.svec", testVectors);

        UniformSvecReader reader = new UniformSvecReader();
        reader.open(testFile);

        assertEquals(5, reader.getDimension());

        reader.close();
    }
}
