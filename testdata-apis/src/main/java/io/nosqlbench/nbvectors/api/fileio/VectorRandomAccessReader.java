package io.nosqlbench.nbvectors.api.fileio;

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


import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.RandomAccess;

/// A sized reader interface that provides immutable list access to elements.
///
/// This interface extends List<T> but overrides all mutation methods to throw
/// UnsupportedOperationException, making it effectively immutable.
/// @param <T> the type of data to read
public interface VectorRandomAccessReader<T> extends List<T>, RandomAccess, Sized, Named {

    @Override
    default T set(int index, T element) {
        throw new UnsupportedOperationException("This reader is immutable.");
    }
    
    @Override
    default boolean add(T t) {
        throw new UnsupportedOperationException("This reader is immutable.");
    }
    
    @Override
    default void add(int index, T element) {
        throw new UnsupportedOperationException("This reader is immutable.");
    }
    
    @Override
    default boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException("This reader is immutable.");
    }
    
    @Override
    default boolean addAll(int index, Collection<? extends T> c) {
        throw new UnsupportedOperationException("This reader is immutable.");
    }
    
    @Override
    default T remove(int index) {
        throw new UnsupportedOperationException("This reader is immutable.");
    }
    
    @Override
    default boolean remove(Object o) {
        throw new UnsupportedOperationException("This reader is immutable.");
    }
    
    @Override
    default boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException("This reader is immutable.");
    }
    
    @Override
    default boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("This reader is immutable.");
    }
    
    @Override
    default void clear() {
        throw new UnsupportedOperationException("This reader is immutable.");
    }
    
    @Override
    default Iterator<T> iterator() {
        return listIterator();
    }
    
    @Override
    default ListIterator<T> listIterator() {
        return listIterator(0);
    }
    
    @Override
    default ListIterator<T> listIterator(int index) {
        return new ImmutableListIterator<>(this, index);
    }
}

