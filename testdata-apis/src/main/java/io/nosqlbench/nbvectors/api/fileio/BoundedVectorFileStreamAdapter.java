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

import java.nio.file.Path;
import java.util.*;

/**
 * Adapter class to convert BoundedVectorFileStream to VectorFileArray.
 * This allows using a BoundedVectorFileStream as a VectorFileArray.
 * 
 * @param <T> The type of vector elements
 */
public class BoundedVectorFileStreamAdapter<T> implements VectorFileArray<T> {
    private final BoundedVectorFileStream<T> stream;
    private final Class<T> elementType;

    /**
     * Creates a new adapter for the given stream and element type.
     * 
     * @param stream The stream to adapt
     * @param elementType The type of elements in the stream
     */
    public BoundedVectorFileStreamAdapter(BoundedVectorFileStream<?> stream, Class<T> elementType) {
        @SuppressWarnings("unchecked")
        BoundedVectorFileStream<T> typedStream = (BoundedVectorFileStream<T>) stream;
        this.stream = typedStream;
        this.elementType = elementType;
    }

    @Override
    public void open(Path filePath) {
        stream.open(filePath);
    }

    @Override
    public void close() {
        stream.close();
    }

    @Override
    public T get(int index) {
        Iterator<T> iterator = stream.iterator();
        for (int i = 0; i < index; i++) {
            if (!iterator.hasNext()) {
                throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + stream.getSize());
            }
            iterator.next();
        }

        if (!iterator.hasNext()) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + stream.getSize());
        }

        return iterator.next();
    }

    @Override
    public int size() {
        return stream.getSize();
    }

    @Override
    public int getSize() {
        return stream.getSize();
    }

    @Override
    public String getName() {
        return stream.getName();
    }

    @Override
    public boolean isEmpty() {
        return stream.getSize() == 0;
    }

    @Override
    public boolean contains(Object o) {
        for (T item : stream) {
            if (Objects.equals(item, o)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Iterator<T> iterator() {
        return stream.iterator();
    }

    @Override
    public Object[] toArray() {
        List<T> list = new ArrayList<>(stream.getSize());
        for (T item : stream) {
            list.add(item);
        }
        return list.toArray();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E> E[] toArray(E[] a) {
        List<T> list = new ArrayList<>(stream.getSize());
        for (T item : stream) {
            list.add(item);
        }
        return list.toArray(a);
    }

    @Override
    public boolean add(T t) {
        throw new UnsupportedOperationException("This reader is immutable.");
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException("This reader is immutable.");
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object o : c) {
            if (!contains(o)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException("This reader is immutable.");
    }

    @Override
    public boolean addAll(int index, Collection<? extends T> c) {
        throw new UnsupportedOperationException("This reader is immutable.");
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException("This reader is immutable.");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("This reader is immutable.");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("This reader is immutable.");
    }

    @Override
    public void add(int index, T element) {
        throw new UnsupportedOperationException("This reader is immutable.");
    }

    @Override
    public T remove(int index) {
        throw new UnsupportedOperationException("This reader is immutable.");
    }

    @Override
    public int indexOf(Object o) {
        int index = 0;
        for (T item : stream) {
            if (Objects.equals(item, o)) {
                return index;
            }
            index++;
        }
        return -1;
    }

    @Override
    public int lastIndexOf(Object o) {
        int index = 0;
        int lastFound = -1;
        for (T item : stream) {
            if (Objects.equals(item, o)) {
                lastFound = index;
            }
            index++;
        }
        return lastFound;
    }

    @Override
    public ListIterator<T> listIterator() {
        throw new UnsupportedOperationException("ListIterator not supported");
    }

    @Override
    public ListIterator<T> listIterator(int index) {
        throw new UnsupportedOperationException("ListIterator not supported");
    }

    @Override
    public List<T> subList(int fromIndex, int toIndex) {
        if (fromIndex < 0 || toIndex > stream.getSize() || fromIndex > toIndex) {
            throw new IndexOutOfBoundsException("fromIndex: " + fromIndex + ", toIndex: " + toIndex + ", size: " + stream.getSize());
        }

        List<T> result = new ArrayList<>(toIndex - fromIndex);
        int index = 0;
        for (T item : stream) {
            if (index >= fromIndex && index < toIndex) {
                result.add(item);
            }
            index++;
            if (index >= toIndex) {
                break;
            }
        }
        return result;
    }

    @Override
    public T set(int index, T element) {
        throw new UnsupportedOperationException("This reader is immutable.");
    }
}