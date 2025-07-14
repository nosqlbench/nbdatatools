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


import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.function.Supplier;

/// A generic object pool implementation that manages a pool of reusable objects.
/// This class provides methods to borrow objects from the pool and return them when done.
/// It uses a supplier callback to create new objects when needed and lifecycle callbacks
/// to manage object state.
///
/// Example usage:
/// ```
/// // Create a pool of ByteBuffers with explicit reset callback
/// ObjectPool<ByteBuffer> bufferPool = new ObjectPool<>(
///     () -> ByteBuffer.allocate(1024),  // supplier
///     buffer -> buffer.clear(),         // reset callback (required)
///     null                              // dispose callback (optional)
/// );
///
/// // Or create a pool with default no-op reset callback
/// ObjectPool<ByteBuffer> simplePool = new ObjectPool<>(
///     () -> ByteBuffer.allocate(1024)   // supplier
/// );
///
/// // Use the try-with-resources pattern (recommended)
/// try (ObjectPool.Borrowed<ByteBuffer> borrowed = bufferPool.borrowObject()) {
///     ByteBuffer buffer = borrowed.get();
///     // Use the buffer...
/// } // Automatically returned to the pool
/// ```
///
/// @param <T> The type of objects managed by this pool
public class ObjectPool<T> {
    private final ConcurrentLinkedQueue<T> pool;
    private final Supplier<T> objectFactory;
    private final Consumer<T> resetCallback;
    private final Consumer<T> disposeCallback;

    /// Creates a new object pool with the specified factory and callbacks.
    ///
    /// @param objectFactory A supplier that creates new objects when the pool is empty
    /// @param resetCallback A callback that resets an object before returning it to the pool
    /// @param disposeCallback A callback that disposes of an object when it's removed from the pool (can be null)
    public ObjectPool(Supplier<T> objectFactory, Consumer<T> resetCallback, Consumer<T> disposeCallback) {
        if (objectFactory == null) {
            throw new IllegalArgumentException("Object factory cannot be null");
        }
        if (resetCallback == null) {
            throw new IllegalArgumentException("Reset callback cannot be null");
        }
        this.pool = new ConcurrentLinkedQueue<>();
        this.objectFactory = objectFactory;
        this.resetCallback = resetCallback;
        this.disposeCallback = disposeCallback;
    }

    /// Creates a new object pool with the specified factory and a no-op reset callback.
    ///
    /// @param objectFactory A supplier that creates new objects when the pool is empty
    public ObjectPool(Supplier<T> objectFactory) {
        this(objectFactory, obj -> {}, null);
    }

    // Private method to borrow an object from the pool
    private T borrow() {
        T object = pool.poll();
        if (object == null) {
            object = objectFactory.get();
        }
        resetCallback.accept(object);
        return object;
    }

    /// Returns an object to the pool after applying the reset callback.
    ///
    /// @param object The object to return to the pool
    public void returnObject(T object) {
        if (object == null) {
            return;
        }

        pool.offer(object);
    }

    /// Clears the pool, applying the dispose callback to each object if provided.
    public void clear() {
        if (disposeCallback != null) {
            T object;
            while ((object = pool.poll()) != null) {
                disposeCallback.accept(object);
            }
        } else {
            pool.clear();
        }
    }

    /// Returns the current size of the pool.
    ///
    /// @return The number of objects currently in the pool
    public int size() {
        return pool.size();
    }

    /// Borrows an object from the pool and wraps it in an AutoCloseable wrapper.
    /// This allows for automatic return of the object when used with try-with-resources.
    ///
    /// @return A Borrowed wrapper containing an object from the pool
    public Borrowed<T> borrowObject() {
        return new Borrowed<>(this, borrow());
    }

    /// An AutoCloseable wrapper for objects borrowed from the pool.
    /// This class allows for automatic return of objects to the pool when used with try-with-resources.
    ///
    /// @param <T> The type of object being borrowed from the pool
    public static class Borrowed<T> implements AutoCloseable {
        private final ObjectPool<T> pool;
        private T object;
        private boolean returned = false;

        private Borrowed(ObjectPool<T> pool, T object) {
            this.pool = pool;
            this.object = object;
        }

        /// Gets the borrowed object.
        ///
        /// @return The borrowed object
        /// @throws IllegalStateException if the object has already been returned to the pool
        public T get() {
            if (returned) {
                throw new IllegalStateException("Object has already been returned to the pool");
            }
            return object;
        }

        /// Returns the object to the pool.
        /// This method is automatically called when used with try-with-resources.
        @Override
        public void close() {
            if (!returned) {
                pool.returnObject(object);
                returned = true;
                object = null;
            }
        }
    }
}
