package io.nosqlbench.vectordata.spec.datasets.types;

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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/// Tests for [MappedDatasetView], verifying that every [DatasetView] method
/// correctly delegates to the source and applies the mapping function.
class MappedDatasetViewTest {

    private static final int SIZE = 20;

    /// A list-backed [DatasetView] of [Integer] values 0..SIZE-1, used as the
    /// source for all tests.
    private ListDatasetView source;

    /// The mapped view under test: maps Integer to its String representation.
    private MappedDatasetView<Integer, String> mapped;

    @BeforeEach
    void setup() {
        source = new ListDatasetView(SIZE);
        mapped = new MappedDatasetView<>(source, String::valueOf);
    }

    @Test
    void getReturnsMappedElement() {
        assertThat(mapped.get(0)).isEqualTo("0");
        assertThat(mapped.get(7)).isEqualTo("7");
        assertThat(mapped.get(SIZE - 1)).isEqualTo(String.valueOf(SIZE - 1));
    }

    @Test
    void getAsyncReturnsMappedElement() throws Exception {
        Future<String> future = mapped.getAsync(5);
        assertThat(future.get()).isEqualTo("5");
    }

    @Test
    void getAsyncWithTimeoutReturnsMappedElement() throws Exception {
        Future<String> future = mapped.getAsync(5);
        assertThat(future.get(1, TimeUnit.SECONDS)).isEqualTo("5");
    }

    @Test
    void getAsyncDelegatesCancelAndStatus() {
        Future<String> future = mapped.getAsync(5);
        assertThat(future.isDone()).isTrue();
        assertThat(future.isCancelled()).isFalse();
    }

    @Test
    void getRangeReturnsMappedElements() {
        Object[] range = mapped.getRange(3, 7);
        assertThat(range).containsExactly("3", "4", "5", "6");
    }

    @Test
    void getRangeAsyncReturnsMappedElements() throws Exception {
        Future<String[]> future = mapped.getRangeAsync(0, 3);
        Object[] result = future.get();
        assertThat(result).containsExactly("0", "1", "2");
    }

    @Test
    void getRangeAsyncWithTimeoutReturnsMappedElements() throws Exception {
        Future<String[]> future = mapped.getRangeAsync(0, 3);
        Object[] result = future.get(1, TimeUnit.SECONDS);
        assertThat(result).containsExactly("0", "1", "2");
    }

    @Test
    void getIndexedReturnsMappedValueWithCorrectIndex() {
        Indexed<String> indexed = mapped.getIndexed(9);
        assertThat(indexed.index()).isEqualTo(9);
        assertThat(indexed.value()).isEqualTo("9");
    }

    @Test
    void getIndexedAsyncReturnsMappedValueWithCorrectIndex() throws Exception {
        Future<Indexed<String>> future = mapped.getIndexedAsync(4);
        Indexed<String> indexed = future.get();
        assertThat(indexed.index()).isEqualTo(4);
        assertThat(indexed.value()).isEqualTo("4");
    }

    @Test
    void getIndexedAsyncWithTimeoutReturnsMappedValue() throws Exception {
        Future<Indexed<String>> future = mapped.getIndexedAsync(4);
        Indexed<String> indexed = future.get(1, TimeUnit.SECONDS);
        assertThat(indexed.index()).isEqualTo(4);
        assertThat(indexed.value()).isEqualTo("4");
    }

    @Test
    void getIndexedRangeReturnsMappedValuesWithCorrectIndices() {
        Indexed<String>[] range = mapped.getIndexedRange(10, 13);
        assertThat(range).hasSize(3);
        for (int i = 0; i < 3; i++) {
            assertThat(range[i].index()).isEqualTo(10 + i);
            assertThat(range[i].value()).isEqualTo(String.valueOf(10 + i));
        }
    }

    @Test
    void getIndexedRangeAsyncReturnsMappedValuesWithCorrectIndices() throws Exception {
        Future<Indexed<String>[]> future = mapped.getIndexedRangeAsync(10, 13);
        Indexed<String>[] range = future.get();
        assertThat(range).hasSize(3);
        for (int i = 0; i < 3; i++) {
            assertThat(range[i].index()).isEqualTo(10 + i);
            assertThat(range[i].value()).isEqualTo(String.valueOf(10 + i));
        }
    }

    @Test
    void getIndexedRangeAsyncWithTimeoutReturnsMappedValues() throws Exception {
        Future<Indexed<String>[]> future = mapped.getIndexedRangeAsync(10, 13);
        Indexed<String>[] range = future.get(1, TimeUnit.SECONDS);
        assertThat(range).hasSize(3);
        for (int i = 0; i < 3; i++) {
            assertThat(range[i].index()).isEqualTo(10 + i);
            assertThat(range[i].value()).isEqualTo(String.valueOf(10 + i));
        }
    }

    @Test
    void getCountDelegatesToSource() {
        assertThat(mapped.getCount()).isEqualTo(SIZE);
    }

    @Test
    void getDataTypeDelegatesToSource() {
        assertThat(mapped.getDataType()).isEqualTo(Integer.class);
    }

    @Test
    void prebufferRangeDelegatesToSource() throws Exception {
        CompletableFuture<Void> future = mapped.prebuffer(0, 10);
        assertThat(future.isDone()).isTrue();
        assertThat(future.get()).isNull();
    }

    @Test
    void prebufferFullDelegatesToSource() throws Exception {
        CompletableFuture<Void> future = mapped.prebuffer();
        assertThat(future.isDone()).isTrue();
        assertThat(future.get()).isNull();
    }

    @Test
    void toListReturnsMappedElements() {
        List<String> list = mapped.toList();
        assertThat(list).hasSize(SIZE);
        for (int i = 0; i < SIZE; i++) {
            assertThat(list.get(i)).isEqualTo(String.valueOf(i));
        }
    }

    @Test
    void toListWithTransformChainsMappings() {
        List<Integer> lengths = mapped.toList(String::length);
        assertThat(lengths).hasSize(SIZE);
        for (int i = 0; i < SIZE; i++) {
            assertThat(lengths.get(i)).isEqualTo(String.valueOf(i).length());
        }
    }

    @Test
    void iteratorReturnsMappedElements() {
        List<String> collected = new ArrayList<>();
        for (String s : mapped) {
            collected.add(s);
        }
        assertThat(collected).hasSize(SIZE);
        for (int i = 0; i < SIZE; i++) {
            assertThat(collected.get(i)).isEqualTo(String.valueOf(i));
        }
    }

    @Test
    void getConsistentWithGetRange() {
        Object[] range = mapped.getRange(0, SIZE);
        for (int i = 0; i < SIZE; i++) {
            assertThat(range[i]).isEqualTo(mapped.get(i));
        }
    }

    @Test
    void getConsistentWithGetIndexed() {
        for (int i = 0; i < SIZE; i++) {
            Indexed<String> indexed = mapped.getIndexed(i);
            assertThat(indexed.value()).isEqualTo(mapped.get(i));
            assertThat(indexed.index()).isEqualTo(i);
        }
    }

    @Test
    void chainedMappingWorks() {
        MappedDatasetView<String, Integer> doubleMapper =
            new MappedDatasetView<>(mapped, s -> s.length() * 2);
        assertThat(doubleMapper.get(5)).isEqualTo(2);   // "5".length() * 2
        assertThat(doubleMapper.get(15)).isEqualTo(4);   // "15".length() * 2
    }

    // ---- Minimal list-backed DatasetView used as the source ----

    /// A trivial in-memory [DatasetView] backed by a list of integers `0..size-1`.
    ///
    /// All methods are implemented with direct list access. Async methods return
    /// already-completed futures. This avoids any file I/O or external dependencies.
    private static class ListDatasetView implements DatasetView<Integer> {
        private final List<Integer> data;

        ListDatasetView(int size) {
            data = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                data.add(i);
            }
        }

        @Override
        public CompletableFuture<Void> prebuffer(long startIncl, long endExcl) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> prebuffer() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public int getCount() {
            return data.size();
        }

        @Override
        public Class<?> getDataType() {
            return Integer.class;
        }

        @Override
        public Integer get(long index) {
            return data.get((int) index);
        }

        @Override
        public Future<Integer> getAsync(long index) {
            return CompletableFuture.completedFuture(get(index));
        }

        @Override
        public Integer[] getRange(long startInclusive, long endExclusive) {
            return data.subList((int) startInclusive, (int) endExclusive).toArray(new Integer[0]);
        }

        @Override
        public Future<Integer[]> getRangeAsync(long startInclusive, long endExclusive) {
            return CompletableFuture.completedFuture(getRange(startInclusive, endExclusive));
        }

        @Override
        public Indexed<Integer> getIndexed(long index) {
            return new Indexed<>(index, get(index));
        }

        @Override
        public Future<Indexed<Integer>> getIndexedAsync(long index) {
            return CompletableFuture.completedFuture(getIndexed(index));
        }

        @SuppressWarnings("unchecked")
        @Override
        public Indexed<Integer>[] getIndexedRange(long startInclusive, long endExclusive) {
            int len = (int) (endExclusive - startInclusive);
            Indexed<Integer>[] result = new Indexed[len];
            for (int i = 0; i < len; i++) {
                result[i] = new Indexed<>(startInclusive + i, get(startInclusive + i));
            }
            return result;
        }

        @Override
        public Future<Indexed<Integer>[]> getIndexedRangeAsync(long startInclusive, long endExclusive) {
            return CompletableFuture.completedFuture(getIndexedRange(startInclusive, endExclusive));
        }

        @Override
        public List<Integer> toList() {
            return List.copyOf(data);
        }

        @Override
        public <U> List<U> toList(Function<Integer, U> f) {
            List<U> result = new ArrayList<>(data.size());
            for (Integer i : data) {
                result.add(f.apply(i));
            }
            return result;
        }

        @Override
        public Iterator<Integer> iterator() {
            return data.iterator();
        }
    }
}
