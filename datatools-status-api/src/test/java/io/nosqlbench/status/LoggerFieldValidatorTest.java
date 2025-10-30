/*
 * Copyright (c) nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.nosqlbench.status;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LoggerFieldValidatorTest {

    // Test class with a Logger field (for validation testing)
    static class ClassWithLoggerField {
        private static final Logger logger = LogManager.getLogger(ClassWithLoggerField.class);
    }

    // Test class with no Logger field
    static class ClassWithoutLoggerField {
        private String someField;
    }

    // Test class with field named "logger" but different type
    static class ClassWithLoggerNamedField {
        private String logger;  // Not a Logger type, just named "logger"
    }

    @Test
    void detectsLoggerFieldByType() {
        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> LoggerFieldValidator.validateNoLoggerFieldsExist(ClassWithLoggerField.class)
        );

        assertTrue(exception.getMessage().contains("Logger fields detected"),
            "Should detect Logger field by type");
        assertTrue(exception.getMessage().contains("ClassWithLoggerField"),
            "Should mention the class name");
        assertTrue(exception.getMessage().contains("logger"),
            "Should mention the field name");
    }

    @Test
    void detectsLoggerFieldByName() {
        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> LoggerFieldValidator.validateNoLoggerFieldsExist(ClassWithLoggerNamedField.class)
        );

        assertTrue(exception.getMessage().contains("Logger fields detected"),
            "Should detect field named 'logger'");
        assertTrue(exception.getMessage().contains("field name matches 'logger'"),
            "Should mention it was detected by name");
    }

    @Test
    void allowsClassWithoutLoggerFields() {
        assertDoesNotThrow(
            () -> LoggerFieldValidator.validateNoLoggerFieldsExist(ClassWithoutLoggerField.class),
            "Should allow class without Logger fields"
        );
    }

    @Test
    void allowsNullClass() {
        assertDoesNotThrow(
            () -> LoggerFieldValidator.validateNoLoggerFieldsExist(null),
            "Should allow null class"
        );
    }

    @Test
    void constructorThrowsException() throws Exception {
        var constructor = LoggerFieldValidator.class.getDeclaredConstructor();
        constructor.setAccessible(true);

        try {
            constructor.newInstance();
            fail("Constructor should throw UnsupportedOperationException");
        } catch (java.lang.reflect.InvocationTargetException e) {
            // Reflection wraps the exception, check the cause
            assertTrue(e.getCause() instanceof UnsupportedOperationException,
                "Cause should be UnsupportedOperationException");
            assertEquals("Utility class should not be instantiated", e.getCause().getMessage());
        }
    }
}
