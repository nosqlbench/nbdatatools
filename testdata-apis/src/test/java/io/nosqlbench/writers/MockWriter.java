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
import java.util.ArrayList;
import java.util.List;

/**
 * Mock Writer implementation for testing WriterLookup.
 * This class implements the Writer interface and is annotated with DataType and Encoding
 * to be discoverable by WriterLookup.
 */
@DataType(float[].class)
@Encoding(Encoding.Type.csv)
public class MockWriter implements Writer<float[]> {

    private final List<float[]> writtenData = new ArrayList<>();
    private Path path;
    
    public MockWriter() {
        // Default constructor
    }
    
    public MockWriter(Path path) {
        this.path = path;
    }
    
    public void init(Path path) {
        this.path = path;
    }
    
    @Override
    public void initialize(Path path) {
        this.path = path;
    }
    
    @Override
    public void write(float[] data) {
        writtenData.add(data);
    }
    
    public List<float[]> getWrittenData() {
        return writtenData;
    }
    
    public Path getPath() {
        return path;
    }
    
    @Override
    public String getName() {
        return "MockWriter";
    }

    public void close() {}
}
