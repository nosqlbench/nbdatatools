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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/// A [DatasetView] adapter that lazily transforms every element of a source
/// [DatasetView] through a mapping function.
///
/// All element-access methods (`get`, `getAsync`, `getRange`, etc.) delegate to the
/// source view and apply the mapping function to each result. Structural methods
/// (`getCount`, `getDataType`, `prebuffer`) delegate directly without transformation.
///
/// Async methods return lightweight [Future] wrappers that defer the mapping until
/// [Future#get] is called, preserving the non-blocking nature of the source.
///
/// @param <S> the source element type
/// @param <T> the target element type after mapping
public class MappedDatasetView<S, T> implements DatasetView<T> {

    private final DatasetView<S> source;
    private final Function<S, T> mapper;

    /// Creates a new mapped view over the given source.
    ///
    /// @param source the underlying dataset view to delegate to
    /// @param mapper the function applied to each element from the source
    public MappedDatasetView(DatasetView<S> source, Function<S, T> mapper) {
        this.source = source;
        this.mapper = mapper;
    }

    /// Returns the underlying source dataset view.
    ///
    /// @return the source dataset view
    protected DatasetView<S> source() {
        return source;
    }

    /// Returns the mapping function applied to each element.
    ///
    /// @return the element mapping function
    protected Function<S, T> mapper() {
        return mapper;
    }

    @Override
    public T get(long index) {
        return mapper.apply(source.get(index));
    }

    @Override
    public Future<T> getAsync(long index) {
        return new MappedFuture<>(source.getAsync(index), mapper);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T[] getRange(long startInclusive, long endExclusive) {
        S[] raw = source.getRange(startInclusive, endExclusive);
        T[] result = (T[]) new Object[raw.length];
        for (int i = 0; i < raw.length; i++) {
            result[i] = mapper.apply(raw[i]);
        }
        return result;
    }

    @Override
    public Future<T[]> getRangeAsync(long startInclusive, long endExclusive) {
        return new MappedArrayFuture<>(source.getRangeAsync(startInclusive, endExclusive), mapper);
    }

    @Override
    public Indexed<T> getIndexed(long index) {
        Indexed<S> indexed = source.getIndexed(index);
        return new Indexed<>(indexed.index(), mapper.apply(indexed.value()));
    }

    @Override
    public Future<Indexed<T>> getIndexedAsync(long index) {
        return new MappedIndexedFuture<>(source.getIndexedAsync(index), mapper);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Indexed<T>[] getIndexedRange(long startInclusive, long endExclusive) {
        Indexed<S>[] raw = source.getIndexedRange(startInclusive, endExclusive);
        Indexed<T>[] result = new Indexed[raw.length];
        for (int i = 0; i < raw.length; i++) {
            result[i] = new Indexed<>(raw[i].index(), mapper.apply(raw[i].value()));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Future<Indexed<T>[]> getIndexedRangeAsync(long startInclusive, long endExclusive) {
        return new MappedIndexedArrayFuture<>(source.getIndexedRangeAsync(startInclusive, endExclusive), mapper);
    }

    @Override
    public int getCount() {
        return source.getCount();
    }

    @Override
    public Class<?> getDataType() {
        return source.getDataType();
    }

    @Override
    public CompletableFuture<Void> prebuffer(long startIncl, long endExcl) {
        return source.prebuffer(startIncl, endExcl);
    }

    @Override
    public CompletableFuture<Void> prebuffer() {
        return source.prebuffer();
    }

    @Override
    public List<T> toList() {
        return source.toList(mapper::apply);
    }

    @Override
    public <U> List<U> toList(Function<T, U> f) {
        return source.toList(element -> f.apply(mapper.apply(element)));
    }

    @Override
    public Iterator<T> iterator() {
        Iterator<S> it = source.iterator();
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public T next() {
                return mapper.apply(it.next());
            }
        };
    }

    /// A [Future] wrapper that lazily maps the result of a delegate through a function.
    ///
    /// @param <S> the source type
    /// @param <T> the target type
    private static class MappedFuture<S, T> implements Future<T> {
        private final Future<S> delegate;
        private final Function<S, T> mapper;

        MappedFuture(Future<S> delegate, Function<S, T> mapper) {
            this.delegate = delegate;
            this.mapper = mapper;
        }

        @Override public boolean cancel(boolean mayInterruptIfRunning) { return delegate.cancel(mayInterruptIfRunning); }
        @Override public boolean isCancelled() { return delegate.isCancelled(); }
        @Override public boolean isDone() { return delegate.isDone(); }

        @Override
        public T get() throws InterruptedException, ExecutionException {
            return mapper.apply(delegate.get());
        }

        @Override
        public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return mapper.apply(delegate.get(timeout, unit));
        }
    }

    /// A [Future] wrapper that lazily maps each element of a source array.
    ///
    /// @param <S> the source element type
    /// @param <T> the target element type
    private static class MappedArrayFuture<S, T> implements Future<T[]> {
        private final Future<S[]> delegate;
        private final Function<S, T> mapper;

        MappedArrayFuture(Future<S[]> delegate, Function<S, T> mapper) {
            this.delegate = delegate;
            this.mapper = mapper;
        }

        @Override public boolean cancel(boolean mayInterruptIfRunning) { return delegate.cancel(mayInterruptIfRunning); }
        @Override public boolean isCancelled() { return delegate.isCancelled(); }
        @Override public boolean isDone() { return delegate.isDone(); }

        @SuppressWarnings("unchecked")
        @Override
        public T[] get() throws InterruptedException, ExecutionException {
            return mapArray(delegate.get());
        }

        @SuppressWarnings("unchecked")
        @Override
        public T[] get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return mapArray(delegate.get(timeout, unit));
        }

        @SuppressWarnings("unchecked")
        private T[] mapArray(S[] raw) {
            T[] result = (T[]) new Object[raw.length];
            for (int i = 0; i < raw.length; i++) { result[i] = mapper.apply(raw[i]); }
            return result;
        }
    }

    /// A [Future] wrapper that lazily maps the value inside an [Indexed] result.
    ///
    /// @param <S> the source value type
    /// @param <T> the target value type
    private static class MappedIndexedFuture<S, T> implements Future<Indexed<T>> {
        private final Future<Indexed<S>> delegate;
        private final Function<S, T> mapper;

        MappedIndexedFuture(Future<Indexed<S>> delegate, Function<S, T> mapper) {
            this.delegate = delegate;
            this.mapper = mapper;
        }

        @Override public boolean cancel(boolean mayInterruptIfRunning) { return delegate.cancel(mayInterruptIfRunning); }
        @Override public boolean isCancelled() { return delegate.isCancelled(); }
        @Override public boolean isDone() { return delegate.isDone(); }

        @Override
        public Indexed<T> get() throws InterruptedException, ExecutionException {
            Indexed<S> indexed = delegate.get();
            return new Indexed<>(indexed.index(), mapper.apply(indexed.value()));
        }

        @Override
        public Indexed<T> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            Indexed<S> indexed = delegate.get(timeout, unit);
            return new Indexed<>(indexed.index(), mapper.apply(indexed.value()));
        }
    }

    /// A [Future] wrapper that lazily maps each element of an [Indexed] array result.
    ///
    /// @param <S> the source value type
    /// @param <T> the target value type
    private static class MappedIndexedArrayFuture<S, T> implements Future<Indexed<T>[]> {
        private final Future<Indexed<S>[]> delegate;
        private final Function<S, T> mapper;

        MappedIndexedArrayFuture(Future<Indexed<S>[]> delegate, Function<S, T> mapper) {
            this.delegate = delegate;
            this.mapper = mapper;
        }

        @Override public boolean cancel(boolean mayInterruptIfRunning) { return delegate.cancel(mayInterruptIfRunning); }
        @Override public boolean isCancelled() { return delegate.isCancelled(); }
        @Override public boolean isDone() { return delegate.isDone(); }

        @SuppressWarnings("unchecked")
        @Override
        public Indexed<T>[] get() throws InterruptedException, ExecutionException {
            return mapArray(delegate.get());
        }

        @SuppressWarnings("unchecked")
        @Override
        public Indexed<T>[] get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return mapArray(delegate.get(timeout, unit));
        }

        @SuppressWarnings("unchecked")
        private Indexed<T>[] mapArray(Indexed<S>[] raw) {
            Indexed<T>[] result = new Indexed[raw.length];
            for (int i = 0; i < raw.length; i++) {
                result[i] = new Indexed<>(raw[i].index(), mapper.apply(raw[i].value()));
            }
            return result;
        }
    }
}
