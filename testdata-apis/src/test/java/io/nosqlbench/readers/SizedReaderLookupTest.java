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


import io.nosqlbench.streamers.SizedStreamer;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/// Test for the SizedReaderLookup utility.
/// These tests use a mock implementation of SizedReader included in this test package.
public class SizedReaderLookupTest {

    @Test
    void testFindReaderWithMatchingCriteria() {
        // Should find the mock implementation
        Optional<SizedStreamer<float[]>> reader = SizedStreamerLookup.findFloatVectorReader("csv");
        assertTrue(reader.isPresent(), "Should find a reader with selector 'csv'");
        assertEquals("MockReader", reader.get().getName(), "Should return the mock reader");
    }

    @Test
    void testFindReaderWithNonMatchingSelector() {
        // Should not find any implementation with this selector
        Optional<SizedStreamer<float[]>> reader = SizedStreamerLookup.findFloatVectorReader("nonexistent");
        assertTrue(reader.isEmpty(), "Should not find a reader with selector 'nonexistent'");
    }

    @Test
    void testFindReaderWithNonMatchingDataType() {
        // MockReader is annotated with float[], so this should not match
        Optional<SizedStreamer<String>> reader = SizedStreamerLookup.findReader("csv", String.class);
        assertTrue(reader.isEmpty(), "Should not find a reader for String.class");
    }
    
    @Test
    void testFindIntVectorReader() {
        // Test the convenience method for int vectors (should be empty since our mock is for float[])
        Optional<SizedStreamer<int[]>> reader = SizedStreamerLookup.findIntVectorReader("csv");
        assertTrue(reader.isEmpty(), "Should not find an int[] reader with our mock");
    }
}
