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
import io.nosqlbench.vectordata.discovery.metadata.Predicates;
import io.nosqlbench.vectordata.spec.datasets.types.Indexed;
import io.nosqlbench.vectordata.spec.predicates.PNode;
import io.nosqlbench.vectordata.spec.predicates.PredicateContext;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Function;

/// Adapts a {@link PredicateStoreBackend} to the {@link Predicates}
/// dataset view interface, decoding raw bytes into {@link PNode}
/// instances.
///
/// When a {@link PredicateContext} is provided, predicates are decoded
/// using context-aware decoding that supports both indexed and named
/// field modes. Without a context, the legacy indexed format is assumed.
public class PredicatesDatasetView implements Predicates<PNode<?>> {

    private final PredicateStoreBackend backend;
    private final PredicateContext context;

    /// Creates a new view over the given backend using context-aware decoding.
    ///
    /// @param backend the predicate store backend
    /// @param context the predicate context for decoding
    public PredicatesDatasetView(PredicateStoreBackend backend, PredicateContext context) {
        this.backend = backend;
        this.context = context;
    }

    /// Creates a new view over the given backend using legacy indexed decoding.
    ///
    /// @param backend the predicate store backend
    public PredicatesDatasetView(PredicateStoreBackend backend) {
        this(backend, null);
    }

    @Override
    public int getCount() {
        return (int) Math.min(backend.getPredicateCount(), Integer.MAX_VALUE);
    }

    @Override
    public Class<?> getDataType() {
        return PNode.class;
    }

    /// Returns the predicate context.
    /// @return the predicate context used for decoding, or null if using legacy indexed decoding
    public PredicateContext getContext() {
        return context;
    }

    @Override
    public PNode<?> get(long index) {
        ByteBuffer buf = backend.getPredicate(index)
            .orElseThrow(() -> new IndexOutOfBoundsException("No predicate at ordinal " + index));
        if (context != null) {
            return context.decode(buf);
        }
        return PNode.fromBuffer(buf);
    }

    @Override
    public Future<PNode<?>> getAsync(long index) {
        return CompletableFuture.completedFuture(get(index));
    }

    @Override
    @SuppressWarnings("unchecked")
    public PNode<?>[] getRange(long startInclusive, long endExclusive) {
        int len = (int) (endExclusive - startInclusive);
        PNode<?>[] result = new PNode[len];
        for (int i = 0; i < len; i++) {
            result[i] = get(startInclusive + i);
        }
        return result;
    }

    @Override
    public Future<PNode<?>[]> getRangeAsync(long startInclusive, long endExclusive) {
        return CompletableFuture.completedFuture(getRange(startInclusive, endExclusive));
    }

    @Override
    public Indexed<PNode<?>> getIndexed(long index) {
        return new Indexed<>(index, get(index));
    }

    @Override
    public Future<Indexed<PNode<?>>> getIndexedAsync(long index) {
        return CompletableFuture.completedFuture(getIndexed(index));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Indexed<PNode<?>>[] getIndexedRange(long startInclusive, long endExclusive) {
        int len = (int) (endExclusive - startInclusive);
        Indexed<PNode<?>>[] result = new Indexed[len];
        for (int i = 0; i < len; i++) {
            long idx = startInclusive + i;
            result[i] = new Indexed<>(idx, get(idx));
        }
        return result;
    }

    @Override
    public Future<Indexed<PNode<?>>[]> getIndexedRangeAsync(long startInclusive, long endExclusive) {
        return CompletableFuture.completedFuture(getIndexedRange(startInclusive, endExclusive));
    }

    @Override
    public List<PNode<?>> toList() {
        int count = getCount();
        List<PNode<?>> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(get(i));
        }
        return list;
    }

    @Override
    public <U> List<U> toList(Function<PNode<?>, U> f) {
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
    public Iterator<PNode<?>> iterator() {
        return new Iterator<>() {
            private int idx = 0;
            private final int count = getCount();

            @Override
            public boolean hasNext() {
                return idx < count;
            }

            @Override
            public PNode<?> next() {
                return get(idx++);
            }
        };
    }
}
