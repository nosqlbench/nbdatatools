package io.nosqlbench.nbdatatools.api.fileio;

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
 * Adapter class that wraps a BoundedVectorFileStream.
 * 
 * @param <T> The type of vector elements
 */
public class BoundedVectorFileStreamAdapter<T> implements BoundedVectorFileStream<T> {
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
    public int getSize() {
        return stream.getSize();
    }

    @Override
    public String getName() {
        return stream.getName();
    }

    @Override
    public Iterator<T> iterator() {
        return stream.iterator();
    }
}
