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

import io.nosqlbench.nbvectors.api.services.DataType;
import io.nosqlbench.nbvectors.api.services.Encoding;

import java.nio.file.Path;
import java.util.List;

/**
 Interface for components that can write data of a specific type.
 Implementations should be annotated with {@link DataType} to indicate
 the type of data they can write and {@link Encoding} to indicate the format.
 @param <T>
 The type of data that this writer can write */
public interface VectorWriter<T> extends AutoCloseable  {

    /**
     Write the given T data.
     @param data
     The data to write
     */
    void write(T data);

    /// Bulk write an array of data.
    /// The default implementation is not necessarily efficient, although it allows hand-over to
    ///  the writer in bulk form if you already have one handy.
    /// A more efficient implementation will implement the bulk write directly without traversing the
    /// array.
    /// @param data
    ///     an array of T
    default void writeBulk(T[] data) {
        for (T datum : data) {
            write(datum);
        }
    }

    /// Bulk write a list of data.
    /// The default implementation is not necessarily efficient, although it allows hand-over to
    ///  the writer in bulk form if you already have one handy.
    /// A more efficient implementation will implement the bulk write directly
    /// without traversing the list.
    /// @param data
    ///     an array of T
    default void writeBulk(List<T> data) {
        for (T datum : data) {
            write(datum);
        }
    }

    /**
     Get the name of this writer implementation
     @return The writer name
     */
    default String getName() {
        return this.getClass().getSimpleName();
    }

    /**
     Initialize the writer with a path
     @param path
     The path to initialize the writer with
     */
    public void open(Path path) throws Exception;

    /// If a writer has a meaningful way of flushing buffered data before [#close()] is called,
    /// then implement thius method.
    default void flush() {}

    /// This should be called at the end of the writer lifecycle.
    void close();

}
