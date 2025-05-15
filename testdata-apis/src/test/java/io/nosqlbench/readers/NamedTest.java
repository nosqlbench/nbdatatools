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


import io.nosqlbench.nbvectors.api.fileio.Named;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NamedTest {

    @Test
    void testNamedInterface() {
        // Test implementation of Named interface
        Named named = new TestNamed("TestName");
        assertEquals("TestName", named.getName(), "getName should return the correct name");
    }

    @Test
    void testMockReaderImplementsNamed() {
        // Test that the MockReader correctly implements Named
        Named mockReader = new MockReader();
        assertEquals("MockReader", mockReader.getName(), "MockReader getName should return 'MockReader'");
    }

    // Simple implementation of Named for testing
    private static class TestNamed implements Named {
        private final String name;

        public TestNamed(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }
    }
}
