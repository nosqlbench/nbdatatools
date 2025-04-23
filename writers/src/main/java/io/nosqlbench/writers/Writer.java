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

package io.nosqlbench.writers;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;

/**
 * Interface for writing data to a destination.
 *
 * @param <T> The type of data to be written
 */
public interface Writer<T> {
    
    /**
     * Writes a single item to the destination.
     *
     * @param item The item to write
     * @return This writer instance for method chaining
     * @throws IOException If an I/O error occurs during writing
     */
    Writer<T> write(T item) throws IOException;
    
    /**
     * Writes multiple items to the destination.
     *
     * @param items The collection of items to write
     * @return This writer instance for method chaining
     * @throws IOException If an I/O error occurs during writing
     */
    Writer<T> writeAll(Collection<T> items) throws IOException;
    
    /**
     * Returns the name of this writer.
     *
     * @return A name representing this writer
     */
    String getName();
    
    /**
     * Flushes any buffered data to the destination.
     *
     * @return This writer instance for method chaining
     * @throws IOException If an I/O error occurs during flushing
     */
    Writer<T> flush() throws IOException;
}
