/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.nosqlbench.nbvectors.api.fileio;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/// An abstract base class for sized readers that enforces immutability.
///
/// This class implements SizedReader<T> and makes all mutation methods final to ensure
/// that subclasses cannot override them and break the immutability contract.
/// Subclasses only need to implement the abstract methods to provide read access to the data.
/// @param <T> the type of data to read
public abstract class ImmutableSizedReader<T> implements VectorRandomAccessReader<T> {

    /// The error message used when mutation is attempted
    private static final String IMMUTABLE_ERROR = "This reader is immutable.";

    @Override
    public final T set(int index, T element) {
        throw new UnsupportedOperationException(IMMUTABLE_ERROR);
    }
    
    @Override
    public final boolean add(T t) {
        throw new UnsupportedOperationException(IMMUTABLE_ERROR);
    }
    
    @Override
    public final void add(int index, T element) {
        throw new UnsupportedOperationException(IMMUTABLE_ERROR);
    }
    
    @Override
    public final boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException(IMMUTABLE_ERROR);
    }
    
    @Override
    public final boolean addAll(int index, Collection<? extends T> c) {
        throw new UnsupportedOperationException(IMMUTABLE_ERROR);
    }
    
    @Override
    public final T remove(int index) {
        throw new UnsupportedOperationException(IMMUTABLE_ERROR);
    }
    
    @Override
    public final boolean remove(Object o) {
        throw new UnsupportedOperationException(IMMUTABLE_ERROR);
    }
    
    @Override
    public final boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException(IMMUTABLE_ERROR);
    }
    
    @Override
    public final boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException(IMMUTABLE_ERROR);
    }
    
    @Override
    public final void clear() {
        throw new UnsupportedOperationException(IMMUTABLE_ERROR);
    }
    
    @Override
    public final Iterator<T> iterator() {
        return listIterator();
    }
    
    @Override
    public final ListIterator<T> listIterator() {
        return listIterator(0);
    }
    
    @Override
    public final ListIterator<T> listIterator(int index) {
        return new ImmutableListIterator<>(this, index);
    }
    
    /// Returns a string representation of this reader.
    ///
    /// @return A string representation including the class name and size
    @Override
    public String toString() {
        return getClass().getSimpleName() + "[size=" + getSize() + "]";
    }
    
    /// Abstract methods from List interface that subclasses must implement
    
    @Override
    public abstract T get(int index);
    
    @Override
    public abstract boolean contains(Object o);
    
    @Override
    public abstract int indexOf(Object o);
    
    @Override
    public abstract int lastIndexOf(Object o);
    
    @Override
    public abstract List<T> subList(int fromIndex, int toIndex);
    
    @Override
    public abstract boolean isEmpty();
    
    @Override
    public abstract int size();
    
    @Override
    public abstract boolean containsAll(Collection<?> c);
    
    @Override
    public abstract Object[] toArray();
    
    @Override
    public abstract <E> E[] toArray(E[] a);
}
