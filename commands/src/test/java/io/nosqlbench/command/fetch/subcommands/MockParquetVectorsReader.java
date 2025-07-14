package io.nosqlbench.command.fetch.subcommands;

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

import io.nosqlbench.nbdatatools.api.fileio.BoundedVectorFileStream;
import io.nosqlbench.nbdatatools.api.services.DataType;
import io.nosqlbench.nbdatatools.api.services.Encoding;
import io.nosqlbench.nbdatatools.api.services.FileType;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A mock implementation of BoundedVectorFileStream for testing purposes.
 * This implementation returns a fixed set of vectors regardless of the input file.
 */
@Encoding(FileType.parquet)
@DataType(float[].class)
public class MockParquetVectorsReader implements BoundedVectorFileStream<float[]> {

    private final List<float[]> vectors;
    private Path path;

    public MockParquetVectorsReader() {
        // Create a fixed set of vectors for testing
        vectors = new ArrayList<>();
        vectors.add(new float[]{1.0f, 2.0f, 3.0f});
        vectors.add(new float[]{4.0f, 5.0f, 6.0f});
        vectors.add(new float[]{7.0f, 8.0f, 9.0f});
    }

    @Override
    public void open(Path path) {
        this.path = path;
    }

    @Override
    public Iterator<float[]> iterator() {
        return vectors.iterator();
    }

    @Override
    public int getSize() {
        return vectors.size();
    }

    @Override
    public String getName() {
        return "mock-parquet";
    }
}