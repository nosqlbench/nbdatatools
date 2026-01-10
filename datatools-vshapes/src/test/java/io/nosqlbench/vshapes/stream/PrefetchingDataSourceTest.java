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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/// Unit tests for PrefetchingDataSource.
@Tag("unit")
class PrefetchingDataSourceTest {

    @Test
    void constructor_rejectsNullDelegate() {
        assertThrows(IllegalArgumentException.class, () -> new PrefetchingDataSource(null));
    }

    @Test
    void constructor_rejectsZeroPrefetchCount() {
        DataSource mock = createMockSource(10, 4, 5);
        assertThrows(IllegalArgumentException.class, () -> new PrefetchingDataSource(mock, 0));
    }

    @Test
    void constructor_rejectsNegativePrefetchCount() {
        DataSource mock = createMockSource(10, 4, 5);
        assertThrows(IllegalArgumentException.class, () -> new PrefetchingDataSource(mock, -1));
    }

    @Test
    void getShape_delegatesToSource() {
        DataSource mock = createMockSource(100, 16, 10);
        PrefetchingDataSource prefetching = new PrefetchingDataSource(mock);

        DataspaceShape shape = prefetching.getShape();

        assertEquals(100, shape.cardinality());
        assertEquals(16, shape.dimensionality());
    }

    @Test
    void getId_prefixesDelegate() {
        DataSource mock = new MockDataSource("test-source", 10, 4, 5);
        PrefetchingDataSource prefetching = new PrefetchingDataSource(mock);

        assertEquals("prefetch:test-source", prefetching.getId());
    }

    @Test
    void chunks_returnsAllChunks() {
        int totalVectors = 100;
        int dimensions = 4;
        int chunkSize = 25;
        DataSource mock = createMockSource(totalVectors, dimensions, chunkSize);
        PrefetchingDataSource prefetching = new PrefetchingDataSource(mock);

        List<float[][]> chunks = new ArrayList<>();
        for (float[][] chunk : prefetching.chunks(chunkSize)) {
            chunks.add(chunk);
        }

        assertEquals(4, chunks.size()); // 100 / 25 = 4 chunks
        for (float[][] chunk : chunks) {
            assertEquals(dimensions, chunk.length);
            assertEquals(chunkSize, chunk[0].length);
        }
    }

    @Test
    void chunks_prefetchesAhead() throws InterruptedException {
        int totalVectors = 50;
        int dimensions = 2;
        int chunkSize = 10;
        CountDownLatch firstChunkLoaded = new CountDownLatch(1);
        CountDownLatch secondChunkLoaded = new CountDownLatch(1);
        AtomicInteger chunksLoaded = new AtomicInteger(0);

        // Create a source that signals when chunks are loaded
        DataSource slowSource = new DataSource() {
            @Override
            public DataspaceShape getShape() {
                return new DataspaceShape(totalVectors, dimensions);
            }

            @Override
            public Iterable<float[][]> chunks(int size) {
                return () -> new Iterator<>() {
                    private int loaded = 0;

                    @Override
                    public boolean hasNext() {
                        return loaded < 5;
                    }

                    @Override
                    public float[][] next() {
                        loaded++;
                        int count = chunksLoaded.incrementAndGet();
                        if (count == 1) firstChunkLoaded.countDown();
                        if (count == 2) secondChunkLoaded.countDown();

                        float[][] chunk = new float[dimensions][chunkSize];
                        return chunk;
                    }
                };
            }

            @Override
            public String getId() {
                return "slow-source";
            }
        };

        PrefetchingDataSource prefetching = new PrefetchingDataSource(slowSource, 2);

        Iterator<float[][]> iter = prefetching.chunks(chunkSize).iterator();

        // Request first chunk - this should trigger prefetching
        assertTrue(iter.hasNext());
        iter.next();

        // Wait for prefetching to load second chunk
        assertTrue(secondChunkLoaded.await(2, TimeUnit.SECONDS),
            "Second chunk should be prefetched");

        // Should have loaded at least 2 chunks (current + prefetched)
        assertTrue(chunksLoaded.get() >= 2, "Should prefetch at least one chunk ahead");
    }

    @Test
    void chunks_handlesEmptySource() {
        DataSource empty = new DataSource() {
            @Override
            public DataspaceShape getShape() {
                return new DataspaceShape(0, 4);
            }

            @Override
            public Iterable<float[][]> chunks(int chunkSize) {
                return java.util.Collections::emptyIterator;
            }

            @Override
            public String getId() {
                return "empty";
            }
        };

        PrefetchingDataSource prefetching = new PrefetchingDataSource(empty);

        List<float[][]> chunks = new ArrayList<>();
        for (float[][] chunk : prefetching.chunks(10)) {
            chunks.add(chunk);
        }

        assertTrue(chunks.isEmpty());
    }

    @Test
    void builder_createsValidInstance() {
        DataSource mock = createMockSource(100, 8, 10);

        PrefetchingDataSource prefetching = PrefetchingDataSource.builder()
            .source(mock)
            .prefetchCount(3)
            .build();

        assertEquals(3, prefetching.getPrefetchCount());
        assertSame(mock, prefetching.getDelegate());
    }

    @Test
    void builder_failsWithoutSource() {
        assertThrows(IllegalStateException.class, () ->
            PrefetchingDataSource.builder().prefetchCount(2).build()
        );
    }

    @Test
    void wrap_createsDefaultPrefetchingSource() {
        DataSource mock = createMockSource(50, 4, 10);

        PrefetchingDataSource prefetching = PrefetchingDataSource.wrap(mock);

        assertEquals(PrefetchingDataSource.DEFAULT_PREFETCH_COUNT, prefetching.getPrefetchCount());
        assertSame(mock, prefetching.getDelegate());
    }

    @Test
    void chunks_propagatesErrors() {
        RuntimeException expected = new RuntimeException("Test error");
        DataSource failingSource = new DataSource() {
            @Override
            public DataspaceShape getShape() {
                return new DataspaceShape(100, 4);
            }

            @Override
            public Iterable<float[][]> chunks(int chunkSize) {
                return () -> new Iterator<>() {
                    private int count = 0;

                    @Override
                    public boolean hasNext() {
                        return count < 3;
                    }

                    @Override
                    public float[][] next() {
                        count++;
                        if (count == 2) throw expected;
                        return new float[4][10];
                    }
                };
            }

            @Override
            public String getId() {
                return "failing";
            }
        };

        PrefetchingDataSource prefetching = new PrefetchingDataSource(failingSource);
        Iterator<float[][]> iter = prefetching.chunks(10).iterator();

        // First chunk should work
        assertTrue(iter.hasNext());
        iter.next();

        // Eventually should propagate the error
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> {
            while (iter.hasNext()) {
                iter.next();
            }
        });
        assertEquals("Error during chunk prefetch", thrown.getMessage());
        assertSame(expected, thrown.getCause());
    }

    /// Creates a mock data source that generates random columnar chunks.
    private DataSource createMockSource(int totalVectors, int dimensions, int chunkSize) {
        return new MockDataSource("mock-" + totalVectors, totalVectors, dimensions, chunkSize);
    }

    /// Mock DataSource implementation for testing.
    private static class MockDataSource implements DataSource {
        private final String id;
        private final int totalVectors;
        private final int dimensions;
        private final int chunkSize;

        MockDataSource(String id, int totalVectors, int dimensions, int chunkSize) {
            this.id = id;
            this.totalVectors = totalVectors;
            this.dimensions = dimensions;
            this.chunkSize = chunkSize;
        }

        @Override
        public DataspaceShape getShape() {
            return new DataspaceShape(totalVectors, dimensions);
        }

        @Override
        public Iterable<float[][]> chunks(int requestedChunkSize) {
            int size = requestedChunkSize > 0 ? requestedChunkSize : chunkSize;
            return () -> new Iterator<>() {
                private int vectorsReturned = 0;

                @Override
                public boolean hasNext() {
                    return vectorsReturned < totalVectors;
                }

                @Override
                public float[][] next() {
                    int remaining = totalVectors - vectorsReturned;
                    int thisChunkSize = Math.min(size, remaining);
                    vectorsReturned += thisChunkSize;

                    float[][] chunk = new float[dimensions][thisChunkSize];
                    for (int d = 0; d < dimensions; d++) {
                        for (int v = 0; v < thisChunkSize; v++) {
                            chunk[d][v] = (float) Math.random();
                        }
                    }
                    return chunk;
                }
            };
        }

        @Override
        public String getId() {
            return id;
        }
    }
}
