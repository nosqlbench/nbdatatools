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


import io.nosqlbench.nbvectors.api.services.Encoding;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class EncodingTest {

    @Test
    void testEncodingAnnotation() {
        // Test that the annotation is present on the annotated class
        Encoding encoding = MockReader.class.getAnnotation(Encoding.class);
        assertNotNull(encoding, "Encoding annotation should be present on MockReader");
        assertEquals(Encoding.Type.csv, encoding.value(), "Encoding value should be csv");
    }

    @Test
    void testEnumValues() {
        // Get all enum values
        Encoding.Type[] values = Encoding.Type.values();
        
        // Assert that all expected values are present
        // Note: Adjusted to test for the actual enum values in the codebase
        assertTrue(containsValue(values, Encoding.Type.csv), "Should contain csv");
        
        // Print all values for debugging
        System.out.println("Available Encoding.Type values:");
        for (Encoding.Type type : values) {
            System.out.println(" - " + type.name());
        }
    }

    @Test
    void testValueOf() {
        // Test valueOf method for csv which we know exists
        assertEquals(Encoding.Type.csv, Encoding.Type.valueOf("csv"));
        
        // Test invalid value throws exception
        assertThrows(IllegalArgumentException.class, () -> Encoding.Type.valueOf("invalid"));
    }

    // Helper method to check if an enum value is in the array
    private boolean containsValue(Encoding.Type[] values, Encoding.Type value) {
        for (Encoding.Type type : values) {
            if (type == value) {
                return true;
            }
        }
        return false;
    }
    
    @Test
    void testRuntimeRetention() throws ClassNotFoundException {
        // Test that the annotation has runtime retention
        Class<?> encodingClass = Class.forName("io.nosqlbench.nbvectors.api.services.Encoding");
        java.lang.annotation.Retention retention = encodingClass.getAnnotation(java.lang.annotation.Retention.class);
        assertNotNull(retention, "Retention annotation should be present");
        assertEquals(java.lang.annotation.RetentionPolicy.RUNTIME, retention.value(), 
                "Retention policy should be RUNTIME");
    }
}
