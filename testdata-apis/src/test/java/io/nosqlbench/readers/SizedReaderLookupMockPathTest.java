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


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SizedReaderLookupMockPathTest {

    @TempDir
    Path tempDir;

    @Test
    void testFindReaderWithPath() {
        Path testFile = tempDir.resolve("test.csv");
        
        Optional<SizedReader<float[]>> reader = SizedReaderLookup.findReader(Encoding.Type.csv, float[].class, testFile);
        
        assertTrue(reader.isPresent(), "Should find a reader for test.csv with csv encoding");
        SizedReader<float[]> sizedReader = reader.get();
        assertTrue(sizedReader instanceof MockSizedReader, "Should return a MockSizedReader");
        
        // Verify the reader was initialized with the path
        MockSizedReader mockReader = (MockSizedReader) sizedReader;
        assertEquals(testFile, mockReader.getFilePath(), "Reader should be initialized with the correct path");
    }

    @Test
    void testFindReaderWithEncodingNameAndPath() {
        Path testFile = tempDir.resolve("test.csv");
        
        Optional<SizedReader<float[]>> reader = SizedReaderLookup.findReader("csv", float[].class, testFile);
        
        assertTrue(reader.isPresent(), "Should find a reader for test.csv with csv encoding string");
        assertEquals("MockSizedReader", reader.get().getName(), "Should return the mock reader");
    }

    @Test
    void testFindFloatReaderWithPath() {
        Path testFile = tempDir.resolve("test.csv");
        
        Optional<SizedReader<float[]>> reader = SizedReaderLookup.findFloatReader(Encoding.Type.csv, testFile);
        
        assertTrue(reader.isPresent(), "Should find a float reader for test.csv with csv encoding");
        assertEquals("MockSizedReader", reader.get().getName(), "Should return the mock reader");
    }

    @Test
    void testFindFloatReaderWithEncodingNameAndPath() {
        Path testFile = tempDir.resolve("test.csv");
        
        Optional<SizedReader<float[]>> reader = SizedReaderLookup.findFloatReader("csv", testFile);
        
        assertTrue(reader.isPresent(), "Should find a float reader for test.csv with csv encoding string");
        assertEquals("MockSizedReader", reader.get().getName(), "Should return the mock reader");
    }

    @Test
    void testFindReaderWithNonexistentEncodingAndPath() {
        Path testFile = tempDir.resolve("test.unknown");
        
        Optional<SizedReader<float[]>> reader = SizedReaderLookup.findReader("nonexistent", float[].class, testFile);
        
        assertTrue(reader.isEmpty(), "Should not find a reader for nonexistent encoding");
    }
}
