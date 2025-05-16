package io.nosqlbench.nbvectors.api.noncore;

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


import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * An immutable list iterator implementation that provides read-only access to a list.
 * All mutation operations throw UnsupportedOperationException.
 * 
 * @param <T> the type of elements returned by this iterator
 */
public class ImmutableListIterator<T> implements ListIterator<T> {
    private final List<T> list;
    private int cursor;
    
    /**
     * Creates a new ImmutableListIterator for the specified list, starting at the specified index.
     *
     * @param list the list to iterate over
     * @param index the initial position of the cursor
     * @throws IndexOutOfBoundsException if the index is out of range
     */
    public ImmutableListIterator(List<T> list, int index) {
        this.list = Objects.requireNonNull(list, "List cannot be null");
        if (index < 0 || index > list.size()) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + list.size());
        }
        this.cursor = index;
    }
    
    @Override
    public boolean hasNext() {
        return cursor < list.size();
    }
    
    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return list.get(cursor++);
    }
    
    @Override
    public boolean hasPrevious() {
        return cursor > 0;
    }
    
    @Override
    public T previous() {
        if (!hasPrevious()) {
            throw new NoSuchElementException();
        }
        return list.get(--cursor);
    }
    
    @Override
    public int nextIndex() {
        return cursor;
    }
    
    @Override
    public int previousIndex() {
        return cursor - 1;
    }

    // All mutation operations throw UnsupportedOperationException
    
    @Override
    public void remove() {
        throw new UnsupportedOperationException("This iterator is immutable");
    }
    
    @Override
    public void set(T t) {
        throw new UnsupportedOperationException("This iterator is immutable");
    }
    
    @Override
    public void add(T t) {
        throw new UnsupportedOperationException("This iterator is immutable");
    }
}
