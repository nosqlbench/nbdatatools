package io.nosqlbench.vectordata.discovery.metadata.views;

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

import io.nosqlbench.vectordata.discovery.metadata.PredicateStoreBackend;
import io.nosqlbench.vectordata.discovery.metadata.ResultIndices;
import io.nosqlbench.vectordata.spec.datasets.types.Indexed;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Function;

/// Adapts a {@link PredicateStoreBackend} to the {@link ResultIndices}
/// dataset view interface, decoding raw bytes into {@code int[]}
/// arrays.
///
/// ## Wire format
///
/// Each result indices record is encoded as:
/// ```
/// [count:4][index0:4][index1:4]...
/// ```
/// All values are little-endian 32-bit integers.
public class ResultIndicesDatasetView implements ResultIndices {

    private final PredicateStoreBackend backend;

    /// Creates a new view over the given backend.
    ///
    /// @param backend the predicate store backend
    public ResultIndicesDatasetView(PredicateStoreBackend backend) {
        this.backend = backend;
    }

    @Override
    public int getVectorDimensions() {
        // Dimensions vary per record; return max K from first record as estimate
        if (getCount() == 0) return 0;
        return get(0).length;
    }

    @Override
    public int getCount() {
        return (int) Math.min(backend.getResultIndicesCount(), Integer.MAX_VALUE);
    }

    @Override
    public Class<?> getDataType() {
        return int[].class;
    }

    @Override
    public int[] get(long index) {
        ByteBuffer buf = backend.getResultIndices(index)
            .orElseThrow(() -> new IndexOutOfBoundsException("No result indices at ordinal " + index));
        buf.order(ByteOrder.LITTLE_ENDIAN);
        int count = buf.getInt();
        int[] result = new int[count];
        for (int i = 0; i < count; i++) {
            result[i] = buf.getInt();
        }
        return result;
    }

    @Override
    public Future<int[]> getAsync(long index) {
        return CompletableFuture.completedFuture(get(index));
    }

    @Override
    public int[][] getRange(long startInclusive, long endExclusive) {
        int len = (int) (endExclusive - startInclusive);
        int[][] result = new int[len][];
        for (int i = 0; i < len; i++) {
            result[i] = get(startInclusive + i);
        }
        return result;
    }

    @Override
    public Future<int[][]> getRangeAsync(long startInclusive, long endExclusive) {
        return CompletableFuture.completedFuture(getRange(startInclusive, endExclusive));
    }

    @Override
    public Indexed<int[]> getIndexed(long index) {
        return new Indexed<>(index, get(index));
    }

    @Override
    public Future<Indexed<int[]>> getIndexedAsync(long index) {
        return CompletableFuture.completedFuture(getIndexed(index));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Indexed<int[]>[] getIndexedRange(long startInclusive, long endExclusive) {
        int len = (int) (endExclusive - startInclusive);
        Indexed<int[]>[] result = new Indexed[len];
        for (int i = 0; i < len; i++) {
            long idx = startInclusive + i;
            result[i] = new Indexed<>(idx, get(idx));
        }
        return result;
    }

    @Override
    public Future<Indexed<int[]>[]> getIndexedRangeAsync(long startInclusive, long endExclusive) {
        return CompletableFuture.completedFuture(getIndexedRange(startInclusive, endExclusive));
    }

    @Override
    public List<int[]> toList() {
        int count = getCount();
        List<int[]> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(get(i));
        }
        return list;
    }

    @Override
    public <U> List<U> toList(Function<int[], U> f) {
        int count = getCount();
        List<U> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(f.apply(get(i)));
        }
        return list;
    }

    @Override
    public CompletableFuture<Void> prebuffer(long startIncl, long endExcl) {
        return backend.prebuffer();
    }

    @Override
    public CompletableFuture<Void> prebuffer() {
        return backend.prebuffer();
    }

    @Override
    public Iterator<int[]> iterator() {
        return new Iterator<>() {
            private int idx = 0;
            private final int count = getCount();

            @Override
            public boolean hasNext() {
                return idx < count;
            }

            @Override
            public int[] next() {
                return get(idx++);
            }
        };
    }
}
