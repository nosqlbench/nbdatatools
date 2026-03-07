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

class UniformSvecStreamerTest {

    @TempDir
    Path tempDir;

    /// Helper method to create a test svec file with the given vectors.
    private Path createTestSvecFile(String filename, short[][] vectors) throws IOException {
        Path testFile = tempDir.resolve(filename);

        try (var outputStream = Files.newOutputStream(testFile)) {
            for (short[] vector : vectors) {
                ByteBuffer dimBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
                dimBuffer.putInt(vector.length);
                outputStream.write(dimBuffer.array());

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
    void testBasicIteration() throws IOException {
        short[][] testVectors = {
            {1, 2, 3},
            {4, 5, 6},
            {7, 8, 9}
        };

        Path testFile = createTestSvecFile("basic_iteration.svec", testVectors);

        UniformSvecStreamer streamer = new UniformSvecStreamer();
        streamer.open(testFile);

        assertEquals(3, streamer.getSize());

        Iterator<short[]> iterator = streamer.iterator();

        assertTrue(iterator.hasNext());
        assertArrayEquals(new short[]{1, 2, 3}, iterator.next());
        assertTrue(iterator.hasNext());
        assertArrayEquals(new short[]{4, 5, 6}, iterator.next());
        assertTrue(iterator.hasNext());
        assertArrayEquals(new short[]{7, 8, 9}, iterator.next());
        assertFalse(iterator.hasNext());

        assertThrows(NoSuchElementException.class, iterator::next);

        streamer.close();
    }

    @Test
    void testMultipleIterators() throws IOException {
        short[][] testVectors = {
            {1, 2},
            {3, 4},
            {5, 6}
        };

        Path testFile = createTestSvecFile("multiple_iterators.svec", testVectors);

        UniformSvecStreamer streamer = new UniformSvecStreamer();
        streamer.open(testFile);

        // First iterator
        Iterator<short[]> iter1 = streamer.iterator();
        assertArrayEquals(new short[]{1, 2}, iter1.next());
        assertArrayEquals(new short[]{3, 4}, iter1.next());
        assertArrayEquals(new short[]{5, 6}, iter1.next());
        assertFalse(iter1.hasNext());

        // Second iterator should restart
        Iterator<short[]> iter2 = streamer.iterator();
        assertArrayEquals(new short[]{1, 2}, iter2.next());
        assertArrayEquals(new short[]{3, 4}, iter2.next());
        assertArrayEquals(new short[]{5, 6}, iter2.next());
        assertFalse(iter2.hasNext());

        streamer.close();
    }

    @Test
    void testReadVector() throws IOException {
        short[][] testVectors = {
            {10, 20, 30},
            {40, 50, 60},
            {70, 80, 90}
        };

        Path testFile = createTestSvecFile("read_vector.svec", testVectors);

        UniformSvecStreamer streamer = new UniformSvecStreamer();
        streamer.open(testFile);

        assertArrayEquals(new short[]{10, 20, 30}, streamer.readVector(0));
        assertArrayEquals(new short[]{40, 50, 60}, streamer.readVector(1));
        assertArrayEquals(new short[]{70, 80, 90}, streamer.readVector(2));

        assertThrows(IndexOutOfBoundsException.class, () -> streamer.readVector(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> streamer.readVector(3));

        streamer.close();
    }

    @Test
    void testNegativeValues() throws IOException {
        short[][] testVectors = {
            {-1, -32768, 32767},
            {0, -100, 100}
        };

        Path testFile = createTestSvecFile("negative.svec", testVectors);

        UniformSvecStreamer streamer = new UniformSvecStreamer();
        streamer.open(testFile);

        assertArrayEquals(new short[]{-1, -32768, 32767}, streamer.readVector(0));
        assertArrayEquals(new short[]{0, -100, 100}, streamer.readVector(1));

        streamer.close();
    }

    @Test
    void testEmptyFile() {
        Path emptyFile = tempDir.resolve("empty.svec");
        try {
            Files.createFile(emptyFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        UniformSvecStreamer streamer = new UniformSvecStreamer();
        assertThrows(RuntimeException.class, () -> streamer.open(emptyFile));
    }

    @Test
    void testCollectAllVectors() throws IOException {
        short[][] testVectors = {
            {1, 2, 3, 4},
            {5, 6, 7, 8},
            {9, 10, 11, 12}
        };

        Path testFile = createTestSvecFile("collect_all.svec", testVectors);

        UniformSvecStreamer streamer = new UniformSvecStreamer();
        streamer.open(testFile);

        List<short[]> vectors = new ArrayList<>();
        for (short[] v : streamer) {
            vectors.add(v);
        }

        assertEquals(3, vectors.size());
        assertArrayEquals(new short[]{1, 2, 3, 4}, vectors.get(0));
        assertArrayEquals(new short[]{5, 6, 7, 8}, vectors.get(1));
        assertArrayEquals(new short[]{9, 10, 11, 12}, vectors.get(2));

        streamer.close();
    }

    @Test
    void testGetName() throws IOException {
        short[][] testVectors = {{1, 2, 3}};
        Path testFile = createTestSvecFile("name_test.svec", testVectors);

        UniformSvecStreamer streamer = new UniformSvecStreamer();
        streamer.open(testFile);

        assertEquals("name_test.svec", streamer.getName());

        streamer.close();
    }
}
