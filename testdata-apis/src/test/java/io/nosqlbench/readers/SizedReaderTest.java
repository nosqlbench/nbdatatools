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


import io.nosqlbench.nbvectors.api.fileio.VectorRandomAccessReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class SizedReaderTest {

    private VectorRandomAccessReader<String> sizedReader;

    @BeforeEach
    void setUp() {
        // Create a concrete implementation of SizedReader for testing
        sizedReader = new TestSizedReader<>(Arrays.asList("A", "B", "C"));
    }

    @Test
    void testSizedReaderImmutabilityMethods() {
        // Test all mutation methods throw UnsupportedOperationException
        assertThrows(UnsupportedOperationException.class, () -> sizedReader.add("D"));
        assertThrows(UnsupportedOperationException.class, () -> sizedReader.add(0, "D"));
        assertThrows(UnsupportedOperationException.class, () -> sizedReader.addAll(Arrays.asList("D", "E")));
        assertThrows(UnsupportedOperationException.class, () -> sizedReader.addAll(0, Arrays.asList("D", "E")));
        assertThrows(UnsupportedOperationException.class, () -> sizedReader.set(0, "Z"));
        assertThrows(UnsupportedOperationException.class, () -> sizedReader.remove(0));
        assertThrows(UnsupportedOperationException.class, () -> sizedReader.remove("A"));
        assertThrows(UnsupportedOperationException.class, () -> sizedReader.removeAll(Arrays.asList("A", "B")));
        assertThrows(UnsupportedOperationException.class, () -> sizedReader.retainAll(Arrays.asList("A")));
        assertThrows(UnsupportedOperationException.class, () -> sizedReader.clear());
    }

    @Test
    void testListAccessMethods() {
        // Test basic List accessor methods
        assertEquals(3, sizedReader.size());
        assertFalse(sizedReader.isEmpty());
        assertEquals("A", sizedReader.get(0));
        assertEquals("B", sizedReader.get(1));
        assertEquals("C", sizedReader.get(2));
        assertTrue(sizedReader.contains("B"));
        assertFalse(sizedReader.contains("Z"));
        assertEquals(1, sizedReader.indexOf("B"));
        assertEquals(1, sizedReader.lastIndexOf("B"));
        assertArrayEquals(new String[]{"A", "B", "C"}, sizedReader.toArray());
        
        List<String> subList = sizedReader.subList(0, 2);
        assertEquals(2, subList.size());
        assertEquals("A", subList.get(0));
        assertEquals("B", subList.get(1));
    }

    @Test
    void testIterator() {
        // Test that iterator works correctly
        Iterator<String> iterator = sizedReader.iterator();
        assertTrue(iterator.hasNext());
        assertEquals("A", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("B", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("C", iterator.next());
        assertFalse(iterator.hasNext());
        
        // Iterator is immutable in SizedReader implementation
        assertThrows(UnsupportedOperationException.class, () -> {
            Iterator<String> it = sizedReader.iterator();
            it.next();
            it.remove();
        });
    }

    @Test
    void testListIterator() {
        // Test that list iterator works correctly
        ListIterator<String> listIterator = sizedReader.listIterator();
        assertTrue(listIterator.hasNext());
        assertFalse(listIterator.hasPrevious());
        
        assertEquals("A", listIterator.next());
        assertTrue(listIterator.hasNext());
        assertTrue(listIterator.hasPrevious());
        
        // Test ListIterator is immutable
        assertThrows(UnsupportedOperationException.class, () -> {
            ListIterator<String> it = sizedReader.listIterator();
            it.next();
            it.remove();
        });
        assertThrows(UnsupportedOperationException.class, () -> {
            ListIterator<String> it = sizedReader.listIterator();
            it.next();
            it.set("Z");
        });
        assertThrows(UnsupportedOperationException.class, () -> {
            ListIterator<String> it = sizedReader.listIterator();
            it.add("Z");
        });
    }
    
    @Test
    void testSizedAndNamedInterfaces() {
        // Test Sized interface method
        assertEquals(3, sizedReader.getSize());
        
        // Test Named interface method
        assertEquals("TestSizedReader", sizedReader.getName());
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
