package io.nosqlbench.writers;

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

import io.nosqlbench.readers.DataType;
import io.nosqlbench.readers.Encoding;

import java.nio.file.Path;

/**
 * Interface for components that can write data of a specific type.
 * Implementations should be annotated with {@link DataType} to indicate
 * the type of data they can write and {@link Encoding} to indicate the format.
 *
 * @param <T> The type of data that this writer can write
 */
public interface Writer<T> {
    
    /**
     * Write the given data
     *
     * @param data The data to write
     */
    void write(T data);
    
    /**
     * Get the name of this writer implementation
     *
     * @return The writer name
     */
    default String getName() {
        return this.getClass().getSimpleName();
    }
    
    /**
     * Initialize the writer with a path
     *
     * @param path The path to initialize the writer with
     */
    default void initialize(Path path) {
        // Default implementation is a no-op
        // Implementations should override this to perform initialization
    }

    void close();
}
