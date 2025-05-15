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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class DataTypeTest {

    @Test
    void testDataTypeAnnotation() {
        // Test that the annotation is present on the annotated class
        DataType dataType = MockReader.class.getAnnotation(DataType.class);
        assertNotNull(dataType, "DataType annotation should be present on MockReader");
        assertEquals(float[].class, dataType.value(), "DataType value should be float[]");
    }
    
    @Test
    void testRuntimeRetention() throws ClassNotFoundException {
        // Test that the annotation has runtime retention
        Class<?> dataTypeClass = Class.forName("io.nosqlbench.nbvectors.api.services.DataType");
        java.lang.annotation.Retention retention = dataTypeClass.getAnnotation(java.lang.annotation.Retention.class);
        assertNotNull(retention, "Retention annotation should be present");
        assertEquals(java.lang.annotation.RetentionPolicy.RUNTIME, retention.value(), 
                "Retention policy should be RUNTIME");
    }
    
    @Test
    void testTypeTarget() throws ClassNotFoundException {
        // Test that the annotation targets types
        Class<?> dataTypeClass = Class.forName("io.nosqlbench.nbvectors.api.services.DataType");
        java.lang.annotation.Target target = dataTypeClass.getAnnotation(java.lang.annotation.Target.class);
        assertNotNull(target, "Target annotation should be present");
        assertArrayEquals(new java.lang.annotation.ElementType[]{java.lang.annotation.ElementType.TYPE}, 
                target.value(), "Target should be ElementType.TYPE");
    }
}
