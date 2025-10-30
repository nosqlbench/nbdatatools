/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.nosqlbench.status;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class to validate that no Logger fields have been initialized before
 * logging configuration is established. This is critical for proper logging setup
 * because Logger fields initialized before configuration will get the wrong appenders.
 *
 * <p>This validator is used by {@link StatusSinkMode#initializeEarly()} to ensure
 * that logging configuration happens before any classes with static Logger fields
 * are loaded.</p>
 *
 * @since 4.0.0
 */
public class LoggerFieldValidator {

    /**
     * Validates that the specified class has no Logger fields defined.
     * This checks for:
     * <ul>
     *   <li>Fields named "logger" (case-insensitive)</li>
     *   <li>Fields whose type's simple name contains "Logger"</li>
     * </ul>
     *
     * @param callingClass the class to inspect for Logger fields
     * @throws IllegalStateException if any Logger fields are found
     */
    public static void validateNoLoggerFieldsExist(Class<?> callingClass) {
        if (callingClass == null) {
            return;
        }

        List<String> loggerFields = new ArrayList<>();

        // Check all declared fields in the class
        for (Field field : callingClass.getDeclaredFields()) {
            String fieldName = field.getName();
            String typeName = field.getType().getSimpleName();

            // Check if field name is "logger" (case-insensitive)
            if (fieldName.equalsIgnoreCase("logger")) {
                loggerFields.add(fieldName + " (field name matches 'logger')");
            }
            // Check if type name contains "Logger"
            else if (typeName.contains("Logger")) {
                loggerFields.add(fieldName + " (type: " + typeName + ")");
            }
        }

        if (!loggerFields.isEmpty()) {
            throw new IllegalStateException(
                "Logger fields detected in " + callingClass.getName() + " before StatusSinkMode initialization. " +
                "This will cause logging to be configured with wrong appenders. " +
                "StatusSinkMode.initializeEarly() must be called BEFORE any classes with Logger fields are loaded. " +
                "Found logger fields: " + String.join(", ", loggerFields)
            );
        }
    }

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private LoggerFieldValidator() {
        throw new UnsupportedOperationException("Utility class should not be instantiated");
    }
}
