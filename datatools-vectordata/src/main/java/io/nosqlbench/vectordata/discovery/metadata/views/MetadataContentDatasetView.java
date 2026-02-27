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

import io.nosqlbench.vectordata.discovery.metadata.MetadataContent;
import io.nosqlbench.vectordata.discovery.metadata.MetadataLayout;
import io.nosqlbench.vectordata.discovery.metadata.MetadataRecordCodec;
import io.nosqlbench.vectordata.discovery.metadata.PredicateStoreBackend;
import io.nosqlbench.vectordata.spec.datasets.types.Indexed;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Function;

/// Adapts a {@link PredicateStoreBackend} to the {@link MetadataContent}
/// dataset view interface, decoding raw bytes into typed record maps
/// using {@link MetadataRecordCodec}.
public class MetadataContentDatasetView implements MetadataContent {

    private final PredicateStoreBackend backend;
    private final MetadataLayout layout;

    /// Creates a new view over the given backend.
    ///
    /// @param backend the predicate store backend
    /// @param layout  the metadata layout used for decoding records
    public MetadataContentDatasetView(PredicateStoreBackend backend, MetadataLayout layout) {
        this.backend = backend;
        this.layout = layout;
    }

    @Override
    public int getCount() {
        return (int) Math.min(backend.getMetadataContentCount(), Integer.MAX_VALUE);
    }

    @Override
    public Class<?> getDataType() {
        return Map.class;
    }

    @Override
    public Map<String, Object> get(long index) {
        ByteBuffer buf = backend.getMetadataContent(index)
            .orElseThrow(() -> new IndexOutOfBoundsException("No metadata content at ordinal " + index));
        return MetadataRecordCodec.decode(layout, buf);
    }

    @Override
    public Future<Map<String, Object>> getAsync(long index) {
        return CompletableFuture.completedFuture(get(index));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object>[] getRange(long startInclusive, long endExclusive) {
        int len = (int) (endExclusive - startInclusive);
        Map<String, Object>[] result = new Map[len];
        for (int i = 0; i < len; i++) {
            result[i] = get(startInclusive + i);
        }
        return result;
    }

    @Override
    public Future<Map<String, Object>[]> getRangeAsync(long startInclusive, long endExclusive) {
        return CompletableFuture.completedFuture(getRange(startInclusive, endExclusive));
    }

    @Override
    public Indexed<Map<String, Object>> getIndexed(long index) {
        return new Indexed<>(index, get(index));
    }

    @Override
    public Future<Indexed<Map<String, Object>>> getIndexedAsync(long index) {
        return CompletableFuture.completedFuture(getIndexed(index));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Indexed<Map<String, Object>>[] getIndexedRange(long startInclusive, long endExclusive) {
        int len = (int) (endExclusive - startInclusive);
        Indexed<Map<String, Object>>[] result = new Indexed[len];
        for (int i = 0; i < len; i++) {
            long idx = startInclusive + i;
            result[i] = new Indexed<>(idx, get(idx));
        }
        return result;
    }

    @Override
    public Future<Indexed<Map<String, Object>>[]> getIndexedRangeAsync(long startInclusive, long endExclusive) {
        return CompletableFuture.completedFuture(getIndexedRange(startInclusive, endExclusive));
    }

    @Override
    public List<Map<String, Object>> toList() {
        int count = getCount();
        List<Map<String, Object>> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(get(i));
        }
        return list;
    }

    @Override
    public <U> List<U> toList(Function<Map<String, Object>, U> f) {
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
    public Iterator<Map<String, Object>> iterator() {
        return new Iterator<>() {
            private int idx = 0;
            private final int count = getCount();

            @Override
            public boolean hasNext() {
                return idx < count;
            }

            @Override
            public Map<String, Object> next() {
                return get(idx++);
            }
        };
    }
}
