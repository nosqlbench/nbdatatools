package io.nosqlbench.vectordata.merkle;

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


import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ObjectPoolTest {

    @Test
    void testObjectCreation() {
        AtomicInteger creationCounter = new AtomicInteger(0);

        ObjectPool<ByteBuffer> pool = new ObjectPool<>(
            () -> {
                creationCounter.incrementAndGet();
                return ByteBuffer.allocate(1024);
            },
            ByteBuffer::clear,
            null
        );

        assertEquals(0, pool.size(), "Pool should be empty initially");

        // Borrow an object, which should create a new one
        ByteBuffer buffer;
        try (ObjectPool.Borrowed<ByteBuffer> borrowed = pool.borrowObject()) {
            buffer = borrowed.get();
            assertNotNull(buffer, "Borrowed object should not be null");
            assertEquals(1, creationCounter.get(), "Factory should have been called once");
            assertEquals(0, pool.size(), "Pool should still be empty after borrowing");
        } // Buffer should be automatically returned here

        assertEquals(1, pool.size(), "Pool should have one object after returning");

        // Borrow again, should reuse the existing object
        ByteBuffer buffer2;
        try (ObjectPool.Borrowed<ByteBuffer> borrowed = pool.borrowObject()) {
            buffer2 = borrowed.get();
            assertNotNull(buffer2, "Borrowed object should not be null");
            assertEquals(1, creationCounter.get(), "Factory should not have been called again");
            assertEquals(0, pool.size(), "Pool should be empty after borrowing again");
        } // Buffer should be automatically returned here

        // Borrow a second object, should create a new one
        try (ObjectPool.Borrowed<ByteBuffer> borrowed1 = pool.borrowObject()) {
            ByteBuffer buffer3 = borrowed1.get();
            assertNotNull(buffer3, "Borrowed object should not be null");

            // Borrow another object, should create a new one
            try (ObjectPool.Borrowed<ByteBuffer> borrowed2 = pool.borrowObject()) {
                ByteBuffer buffer4 = borrowed2.get();
                assertNotNull(buffer4, "Borrowed object should not be null");
                assertEquals(2, creationCounter.get(), "Factory should have been called again");
            }
        }
    }

    @Test
    void testResetCallback() {
        AtomicInteger resetCounter = new AtomicInteger(0);

        ObjectPool<ByteBuffer> pool = new ObjectPool<>(
            () -> ByteBuffer.allocate(1024),
            buffer -> {
                resetCounter.incrementAndGet();
                buffer.clear();
            },
            null
        );

        // Borrow and use a buffer
        ByteBuffer buffer;
        try (ObjectPool.Borrowed<ByteBuffer> borrowed = pool.borrowObject()) {
            buffer = borrowed.get();
            buffer.put("test".getBytes());
            assertEquals(4, buffer.position(), "Buffer position should be updated");
        } // Buffer should be automatically returned here

        assertEquals(1, resetCounter.get(), "Reset callback should have been called once");
        assertEquals(1, pool.size(), "Pool should have one object after returning");

        // Borrow the buffer again and verify it was reset
        try (ObjectPool.Borrowed<ByteBuffer> borrowed = pool.borrowObject()) {
            ByteBuffer buffer2 = borrowed.get();
            assertEquals(0, buffer2.position(), "Buffer should have been reset");
        }
    }

    @Test
    void testDisposeCallback() {
        List<ByteBuffer> disposedBuffers = new ArrayList<>();

        ObjectPool<ByteBuffer> pool = new ObjectPool<>(
            () -> ByteBuffer.allocate(1024),
            ByteBuffer::clear,
            disposedBuffers::add
        );

        // Borrow and store 5 distinct buffers
        List<ObjectPool.Borrowed<ByteBuffer>> borrowedBuffers = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            ObjectPool.Borrowed<ByteBuffer> borrowed = pool.borrowObject();
            ByteBuffer buffer = borrowed.get();
            // Put some unique data in each buffer to ensure they're distinct
            buffer.put(("buffer" + i).getBytes());
            borrowedBuffers.add(borrowed);
        }

        assertEquals(0, pool.size(), "Pool should be empty while objects are borrowed");

        // Return all the buffers to the pool
        for (ObjectPool.Borrowed<ByteBuffer> borrowed : borrowedBuffers) {
            borrowed.close();
        }

        assertEquals(5, pool.size(), "Pool should have 5 objects after returning");
        assertEquals(0, disposedBuffers.size(), "No buffers should have been disposed yet");

        // Clear the pool, which should dispose all buffers
        pool.clear();
        assertEquals(0, pool.size(), "Pool should be empty after clearing");
        assertEquals(5, disposedBuffers.size(), "All 5 buffers should have been disposed");
    }

    @Test
    void testAutoCloseableWrapper() {
        AtomicInteger resetCounter = new AtomicInteger(0);

        ObjectPool<ByteBuffer> pool = new ObjectPool<>(
            () -> ByteBuffer.allocate(1024),
            buffer -> {
                resetCounter.incrementAndGet();
                buffer.clear();
            },
            null
        );

        // Use try-with-resources
        try (ObjectPool.Borrowed<ByteBuffer> borrowed = pool.borrowObject()) {
            ByteBuffer buffer = borrowed.get();
            assertNotNull(buffer, "Borrowed object should not be null");
            buffer.put("test".getBytes());
            assertEquals(4, buffer.position(), "Buffer position should be updated");
        } // Buffer should be automatically returned here

        assertEquals(1, resetCounter.get(), "Reset callback should have been called once");
        assertEquals(1, pool.size(), "Pool should have one object after try-with-resources");

        // Borrow the buffer again and verify it was reset
        try (ObjectPool.Borrowed<ByteBuffer> borrowed = pool.borrowObject()) {
            ByteBuffer buffer2 = borrowed.get();
            assertEquals(0, buffer2.position(), "Buffer should have been reset");
        }
    }

    @Test
    void testMultipleReturns() {
        AtomicInteger resetCounter = new AtomicInteger(0);

        ObjectPool<ByteBuffer> pool = new ObjectPool<>(
            () -> ByteBuffer.allocate(1024),
            buffer -> resetCounter.incrementAndGet(),
            null
        );

        // Use try-with-resources with explicit close
        ObjectPool.Borrowed<ByteBuffer> borrowed = pool.borrowObject();
        ByteBuffer buffer = borrowed.get();

        // Close explicitly
        borrowed.close();
        assertEquals(1, resetCounter.get(), "Reset callback should have been called once");

        // Close again, should not call reset again
        borrowed.close();
        assertEquals(1, resetCounter.get(), "Reset callback should not have been called again");

        // Trying to get the object after it's returned should throw an exception
        assertThrows(IllegalStateException.class, borrowed::get);
    }

    @Test
    void testNullObjectReturn() {
        ObjectPool<ByteBuffer> pool = new ObjectPool<>(
            () -> ByteBuffer.allocate(1024),
            ByteBuffer::clear,
            null
        );

        // Returning null should not throw an exception
        pool.returnObject(null);
        assertEquals(0, pool.size(), "Pool size should not change when returning null");
    }

    @Test
    void testNullFactoryThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> new ObjectPool<>(null, null, null));
    }

    @Test
    void testNullResetCallbackThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> new ObjectPool<>(() -> ByteBuffer.allocate(1024), null, null));
    }

    @Test
    void testDefaultConstructor() {
        // Test the constructor that takes only the objectFactory parameter
        ObjectPool<ByteBuffer> pool = new ObjectPool<>(() -> ByteBuffer.allocate(1024));

        // Verify that the pool works correctly
        try (ObjectPool.Borrowed<ByteBuffer> borrowed = pool.borrowObject()) {
            ByteBuffer buffer = borrowed.get();
            assertNotNull(buffer, "Borrowed object should not be null");
            buffer.put("test".getBytes());
            assertEquals(4, buffer.position(), "Buffer position should be updated");
        } // Buffer should be automatically returned here

        assertEquals(1, pool.size(), "Pool should have one object after try-with-resources");

        // Borrow the buffer again and verify it was returned (but not reset since we used the no-op reset callback)
        try (ObjectPool.Borrowed<ByteBuffer> borrowed = pool.borrowObject()) {
            ByteBuffer buffer = borrowed.get();
            assertEquals(4, buffer.position(), "Buffer position should not have been reset");
        }
    }
}
