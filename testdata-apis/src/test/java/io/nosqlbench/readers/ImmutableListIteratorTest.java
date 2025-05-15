package io.nosqlbench.readers;

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


import io.nosqlbench.nbvectors.api.fileio.ImmutableListIterator;
import io.nosqlbench.nbvectors.api.fileio.VectorRandomAccessReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ImmutableListIteratorTest {

    private List<String> testList;
    private ListIterator<String> iterator;

    @BeforeEach
    void setUp() {
        // Create a simple SizedReader implementation for testing
        testList = new TestSizedReader<>(Arrays.asList("A", "B", "C"));
        iterator = new ImmutableListIterator<>(testList, 0);
    }

    @Test
    void testHasNextAndNext() {
        assertTrue(iterator.hasNext());
        assertEquals("A", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("B", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("C", iterator.next());
        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, () -> iterator.next());
    }

    @Test
    void testHasPreviousAndPrevious() {
        // Move to end first
        iterator.next(); // A
        iterator.next(); // B
        iterator.next(); // C
        
        assertTrue(iterator.hasPrevious());
        assertEquals("C", iterator.previous());
        assertTrue(iterator.hasPrevious());
        assertEquals("B", iterator.previous());
        assertTrue(iterator.hasPrevious());
        assertEquals("A", iterator.previous());
        assertFalse(iterator.hasPrevious());
        assertThrows(NoSuchElementException.class, () -> iterator.previous());
    }

    @Test
    void testNextIndexAndPreviousIndex() {
        assertEquals(0, iterator.nextIndex());
        assertEquals(-1, iterator.previousIndex());
        
        iterator.next(); // A
        
        assertEquals(1, iterator.nextIndex());
        assertEquals(0, iterator.previousIndex());
        
        iterator.next(); // B
        iterator.next(); // C
        
        assertEquals(3, iterator.nextIndex());
        assertEquals(2, iterator.previousIndex());
    }

    @Test
    void testStartingFromMiddle() {
        iterator = new ImmutableListIterator<>(testList, 1);
        
        assertEquals(1, iterator.nextIndex());
        assertEquals(0, iterator.previousIndex());
        assertTrue(iterator.hasPrevious());
        assertTrue(iterator.hasNext());
        assertEquals("B", iterator.next());
    }

    @Test
    void testUnsupportedOperations() {
        assertThrows(UnsupportedOperationException.class, () -> iterator.remove());
        assertThrows(UnsupportedOperationException.class, () -> iterator.set("Z"));
        assertThrows(UnsupportedOperationException.class, () -> iterator.add("Z"));
    }

    // A simple implementation of SizedReader for testing
    private static class TestSizedReader<T> implements VectorRandomAccessReader<T> {
        private final List<T> elements;

        public TestSizedReader(List<T> elements) {
            this.elements = elements;
        }

        @Override
        public T get(int index) {
            return elements.get(index);
        }

        @Override
        public int size() {
            return elements.size();
        }

        @Override
        public boolean isEmpty() {
            return elements.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return elements.contains(o);
        }

        @Override
        public Object[] toArray() {
            return elements.toArray();
        }

        @Override
        public <T1> T1[] toArray(T1[] a) {
            return elements.toArray(a);
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return elements.containsAll(c);
        }

        @Override
        public int indexOf(Object o) {
            return elements.indexOf(o);
        }

        @Override
        public int lastIndexOf(Object o) {
            return elements.lastIndexOf(o);
        }

        @Override
        public List<T> subList(int fromIndex, int toIndex) {
            return elements.subList(fromIndex, toIndex);
        }

        @Override
        public int getSize() {
            return size();
        }

        @Override
        public String getName() {
            return "TestSizedReader";
        }
    }
}
