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


import io.nosqlbench.nbvectors.api.services.DataType;
import io.nosqlbench.nbvectors.api.services.Encoding;
import io.nosqlbench.nbvectors.api.fileio.VectorRandomAccessReader;
import io.nosqlbench.nbvectors.api.services.FileType;

import java.nio.file.Path;
import java.util.AbstractList;

/**
 * A mock implementation of SizedReader for testing.
 * This class implements SizedReader directly and provides basic functionality.
 */
@DataType(float[].class)
@Encoding(FileType.csv)
public class MockSizedReader extends AbstractList<float[]> implements VectorRandomAccessReader<float[]> {
    
    private final int size;
    private final String name;
    private Path filePath;
    
    public MockSizedReader() {
        this.size = 10;
        this.name = "MockSizedReader";
    }
    
    public MockSizedReader(Path path) {
        this.size = 10;
        this.name = "MockSizedReader";
        this.filePath = path;
    }
    
    public MockSizedReader(int size, String name) {
        this.size = size;
        this.name = name;
    }
    
    public void init(Path path) {
        this.filePath = path;
    }
    
    public Path getFilePath() {
        return filePath;
    }

    @Override
    public float[] get(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
        
        float[] data = new float[3];
        data[0] = index;
        data[1] = index + 0.5f;
        data[2] = index + 1.0f;
        return data;
    }

    @Override
    public int size() {
        return size;
    }
    @Override
    public int getSize() {
        return size;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void open(Path filePath) {
    }
}
