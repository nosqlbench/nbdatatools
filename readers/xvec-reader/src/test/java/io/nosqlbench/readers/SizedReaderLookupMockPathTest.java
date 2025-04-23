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

import static org.junit.jupiter.api.Assertions.*;

public class SizedReaderLookupMockPathTest {

    @TempDir
    Path tempDir;

    @Test
    void testFindReaderWithPath() {
        Path testFile = tempDir.resolve("test.csv");
        
        Optional<SizedStreamer<float[]>> reader = SizedStreamerLookup.findFloatVectorReader("csv");
        assertTrue(reader.isPresent(), "Should find a reader with selector 'csv'");
    }
    
    @Test
    void testFindReaderWithPathNonExistent() {
        Path testFile = tempDir.resolve("nonexistent.csv");
        
        // Test with a nonexistent selector
        Optional<SizedStreamer<float[]>> reader = SizedStreamerLookup.findFloatVectorReader("nonexistent");
        assertTrue(reader.isEmpty(), "Should not find a reader with selector 'nonexistent'");
    }
    
    @Test
    void testFindReaderWithNonMatchingType() {
        Path testFile = tempDir.resolve("test.csv");
        
        // Test with a non-matching data type
        Optional<SizedStreamer<String>> reader = SizedStreamerLookup.findReader("csv", String.class);
        assertTrue(reader.isEmpty(), "Should not find a reader for String.class");
    }
    
    @Test
    void testFindIntVectorReaderWithPath() {
        Path testFile = tempDir.resolve("test.csv");
        
        // Test the convenience method for int vectors
        Optional<SizedStreamer<int[]>> reader = SizedStreamerLookup.findIntVectorReader("csv");
        // This test should match the behavior in SizedReaderLookupTest
        assertTrue(reader.isEmpty(), "Should not find an int[] reader with our mock");
    }
}
