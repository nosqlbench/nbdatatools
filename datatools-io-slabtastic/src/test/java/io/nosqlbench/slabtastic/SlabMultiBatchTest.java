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

package io.nosqlbench.slabtastic;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;

/// Tests for the multi-batch read API ({@link SlabReader#getAll}).
///
/// Covers order sensitivity, partial success, cross-namespace requests,
/// unknown namespaces, edge cases, and concurrent access.
class SlabMultiBatchTest implements SlabConstants {

    @TempDir
    Path tempDir;

    @Test
    void allPresent() throws IOException {
        Path file = tempDir.resolve("allpresent.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            for (int i = 0; i < 20; i++) {
                writer.write(i, ("rec-" + i).getBytes());
            }
        }
        try (SlabReader reader = new SlabReader(file)) {
            List<Long> ordinals = new ArrayList<>();
            for (int i = 0; i < 20; i++) ordinals.add((long) i);
            Collections.shuffle(ordinals, new Random(99));

            List<BatchRequest> requests = ordinals.stream()
                .map(BatchRequest::of)
                .toList();

            BatchResult result = reader.getAll(requests);
            assertThat(result.size()).isEqualTo(20);
            assertThat(result.isComplete()).isTrue();
            assertThat(result.hasPartialFailure()).isFalse();

            for (int i = 0; i < 20; i++) {
                long ord = ordinals.get(i);
                Optional<ByteBuffer> slot = result.get(i);
                assertThat(slot).as("slot %d (ordinal %d)", i, ord).isPresent();
                assertThat(bufToString(slot.get())).isEqualTo("rec-" + ord);
            }
        }
    }

    @Test
    void partialEmpty() throws IOException {
        Path file = tempDir.resolve("partial.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, "zero".getBytes());
            writer.write(1, "one".getBytes());
            // gap at 2-9
            writer.write(10, "ten".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            BatchResult result = reader.getAll(0, 1, 5, 10, 99);
            assertThat(result.size()).isEqualTo(5);
            assertThat(result.get(0)).isPresent();
            assertThat(result.get(1)).isPresent();
            assertThat(result.get(2)).isEmpty();   // ordinal 5 not present
            assertThat(result.get(3)).isPresent();
            assertThat(result.get(4)).isEmpty();   // ordinal 99 not present
            assertThat(result.presentCount()).isEqualTo(3);
            assertThat(result.emptyCount()).isEqualTo(2);
            assertThat(result.hasPartialFailure()).isTrue();
            assertThat(result.isComplete()).isFalse();
        }
    }

    @Test
    void crossNamespace() throws IOException {
        Path file = tempDir.resolve("crossns.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write("alpha", 0, "a0".getBytes());
            writer.write("alpha", 1, "a1".getBytes());
            writer.write("beta", 0, "b0".getBytes());
            writer.write("beta", 1, "b1".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            List<BatchRequest> requests = List.of(
                BatchRequest.of("alpha", 0),
                BatchRequest.of("beta", 1),
                BatchRequest.of("alpha", 1),
                BatchRequest.of("beta", 0)
            );
            BatchResult result = reader.getAll(requests);
            assertThat(result.isComplete()).isTrue();
            assertThat(bufToString(result.get(0).get())).isEqualTo("a0");
            assertThat(bufToString(result.get(1).get())).isEqualTo("b1");
            assertThat(bufToString(result.get(2).get())).isEqualTo("a1");
            assertThat(bufToString(result.get(3).get())).isEqualTo("b0");
        }
    }

    @Test
    void unknownNamespace() throws IOException {
        Path file = tempDir.resolve("unknownns.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write("existing", 0, "data".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            List<BatchRequest> requests = List.of(
                BatchRequest.of("existing", 0),
                BatchRequest.of("nonexistent", 0),
                BatchRequest.of("also_missing", 42)
            );
            BatchResult result = reader.getAll(requests);
            assertThat(result.get(0)).isPresent();
            assertThat(result.get(1)).isEmpty();
            assertThat(result.get(2)).isEmpty();
            assertThat(result.presentCount()).isEqualTo(1);
            assertThat(result.emptyCount()).isEqualTo(2);
        }
    }

    @Test
    void emptyRequestList() throws IOException {
        Path file = tempDir.resolve("empty.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, "data".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            BatchResult result = reader.getAll(List.of());
            assertThat(result.size()).isZero();
            assertThat(result.isComplete()).isTrue();
            assertThat(result.hasPartialFailure()).isFalse();
            assertThat(result.emptyCount()).isZero();
            assertThat(result.presentCount()).isZero();
        }
    }

    @Test
    void orderSensitivity_nonMonotonic() throws IOException {
        Path file = tempDir.resolve("nonmono.slab");
        try (SlabWriter writer = new SlabWriter(file, 512)) {
            for (int i = 0; i < 100; i++) {
                writer.write(i, ("v" + i).getBytes());
            }
        }
        try (SlabReader reader = new SlabReader(file)) {
            // Request in non-monotonic order: 50, 10, 90, 30, 70, 0, 99
            long[] ordinals = {50, 10, 90, 30, 70, 0, 99};
            BatchResult result = reader.getAll(ordinals);
            assertThat(result.isComplete()).isTrue();
            for (int i = 0; i < ordinals.length; i++) {
                assertThat(bufToString(result.get(i).get()))
                    .as("slot %d (ordinal %d)", i, ordinals[i])
                    .isEqualTo("v" + ordinals[i]);
            }
        }
    }

    @Test
    void orderSensitivity_reversed() throws IOException {
        Path file = tempDir.resolve("reversed.slab");
        try (SlabWriter writer = new SlabWriter(file, 512)) {
            for (int i = 0; i < 20; i++) {
                writer.write(i, ("r" + i).getBytes());
            }
        }
        try (SlabReader reader = new SlabReader(file)) {
            long[] ordinals = new long[20];
            for (int i = 0; i < 20; i++) ordinals[i] = 19 - i;
            BatchResult result = reader.getAll(ordinals);
            assertThat(result.isComplete()).isTrue();
            for (int i = 0; i < 20; i++) {
                assertThat(bufToString(result.get(i).get()))
                    .as("slot %d", i)
                    .isEqualTo("r" + (19 - i));
            }
        }
    }

    @Test
    void orderSensitivity_duplicates() throws IOException {
        Path file = tempDir.resolve("duplicates.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, "hello".getBytes());
            writer.write(1, "world".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            BatchResult result = reader.getAll(0, 0, 1, 0);
            assertThat(result.size()).isEqualTo(4);
            assertThat(result.isComplete()).isTrue();
            assertThat(bufToString(result.get(0).get())).isEqualTo("hello");
            assertThat(bufToString(result.get(1).get())).isEqualTo("hello");
            assertThat(bufToString(result.get(2).get())).isEqualTo("world");
            assertThat(bufToString(result.get(3).get())).isEqualTo("hello");
        }
    }

    @Test
    void batchResultHelpers() throws IOException {
        Path file = tempDir.resolve("helpers.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, "a".getBytes());
            writer.write(1, "b".getBytes());
            writer.write(2, "c".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            // All present
            BatchResult allPresent = reader.getAll(0, 1, 2);
            assertThat(allPresent.isComplete()).isTrue();
            assertThat(allPresent.hasPartialFailure()).isFalse();
            assertThat(allPresent.presentCount()).isEqualTo(3);
            assertThat(allPresent.emptyCount()).isZero();

            // Mixed
            BatchResult mixed = reader.getAll(0, 99, 2);
            assertThat(mixed.isComplete()).isFalse();
            assertThat(mixed.hasPartialFailure()).isTrue();
            assertThat(mixed.presentCount()).isEqualTo(2);
            assertThat(mixed.emptyCount()).isEqualTo(1);

            // All empty
            BatchResult allEmpty = reader.getAll(88, 99);
            assertThat(allEmpty.isComplete()).isFalse();
            assertThat(allEmpty.hasPartialFailure()).isTrue();
            assertThat(allEmpty.presentCount()).isZero();
            assertThat(allEmpty.emptyCount()).isEqualTo(2);
        }
    }

    @Test
    void concurrentCalls() throws Exception {
        Path file = tempDir.resolve("concurrent.slab");
        try (SlabWriter writer = new SlabWriter(file, 512)) {
            for (int i = 0; i < 200; i++) {
                writer.write(i, ("val" + i).getBytes());
            }
        }
        try (SlabReader reader = new SlabReader(file)) {
            int threadCount = 8;
            int batchesPerThread = 100;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            List<Future<Void>> futures = new ArrayList<>();

            for (int t = 0; t < threadCount; t++) {
                final int seed = t;
                futures.add(executor.submit(() -> {
                    Random rng = new Random(seed);
                    for (int b = 0; b < batchesPerThread; b++) {
                        int batchSize = 1 + rng.nextInt(16);
                        long[] ordinals = new long[batchSize];
                        for (int i = 0; i < batchSize; i++) {
                            ordinals[i] = rng.nextInt(200);
                        }
                        BatchResult result = reader.getAll(ordinals);
                        assertThat(result.size()).isEqualTo(batchSize);
                        for (int i = 0; i < batchSize; i++) {
                            Optional<ByteBuffer> slot = result.get(i);
                            assertThat(slot).as("thread=%d batch=%d slot=%d ord=%d",
                                seed, b, i, ordinals[i]).isPresent();
                            assertThat(bufToString(slot.get()))
                                .isEqualTo("val" + ordinals[i]);
                        }
                    }
                    return null;
                }));
            }
            executor.shutdown();
            for (Future<Void> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void ordinalBeyondEnd() throws IOException {
        Path file = tempDir.resolve("beyondend.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, "only".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            BatchResult result = reader.getAll(0, 1, 1000, Long.MAX_VALUE / 2);
            assertThat(result.get(0)).isPresent();
            assertThat(result.get(1)).isEmpty();
            assertThat(result.get(2)).isEmpty();
            assertThat(result.get(3)).isEmpty();
        }
    }

    private static String bufToString(ByteBuffer buf) {
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return new String(bytes);
    }
}
