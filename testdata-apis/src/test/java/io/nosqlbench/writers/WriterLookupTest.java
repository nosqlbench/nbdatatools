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

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/// Test for the WriterLookup utility.
/// These tests use a mock implementation of Writer included in this test package.
public class WriterLookupTest {

//    @Test
//    void testFindWriterWithMatchingCriteria() {
//        // Should find the mock implementation
//        Optional<Writer<float[]>> writer = WriterLookup.findFloatWriter("csv");
//        assertTrue(writer.isPresent(), "Should find a writer with encoding 'csv'");
//        assertEquals("MockWriter", writer.get().getName(), "Should return the mock writer");
//    }

    @Test
    void testFindWriterWithNonMatchingDataType() {
        // MockWriter is annotated with float[], so this should not match
        Optional<Writer<String>> writer = WriterLookup.findWriter("csv", String.class);
        assertTrue(writer.isEmpty(), "Should not find a writer for String.class");
    }
    
}
