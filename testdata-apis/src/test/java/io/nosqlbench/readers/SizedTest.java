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


import io.nosqlbench.nbvectors.api.fileio.Sized;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SizedTest {

    @Test
    void testSizedInterface() {
        // Test implementation of Sized interface
        Sized sized = new TestSized(42);
        assertEquals(42, sized.getSize(), "getSize should return the correct size");
    }

    @Test
    void testMockReaderImplementsSized() {
        // Test that the MockReader correctly implements Sized
        Sized mockReader = new MockReader();
        assertEquals(10, mockReader.getSize(), "MockReader getSize should return 10");
    }

    // Simple implementation of Sized for testing
    private static class TestSized implements Sized {
        private final int size;

        public TestSized(int size) {
            this.size = size;
        }

        @Override
        public int getSize() {
            return size;
        }
    }
}
