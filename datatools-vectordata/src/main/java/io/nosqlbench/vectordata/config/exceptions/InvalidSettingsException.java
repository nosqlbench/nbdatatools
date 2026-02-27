package io.nosqlbench.vectordata.config.exceptions;

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

import java.nio.file.Path;

/// Exception thrown when the settings.yaml file is malformed or contains invalid values.
///
/// This exception is thrown when:
/// - The YAML is syntactically invalid
/// - The cache_dir key is missing
/// - The cache_dir value is empty or null
public class InvalidSettingsException extends RuntimeException {

    /// The path to the settings file.
    private final Path settingsPath;
    /// The invalid value, or null if not applicable.
    private final String invalidValue;

    /// Constructs an InvalidSettingsException with a message describing the issue.
    ///
    /// @param message a description of what is invalid
    /// @param settingsPath the path to the settings file
    public InvalidSettingsException(String message, Path settingsPath) {
        super(buildMessage(message, settingsPath, null));
        this.settingsPath = settingsPath;
        this.invalidValue = null;
    }

    /// Constructs an InvalidSettingsException with the invalid value.
    ///
    /// @param message a description of what is invalid
    /// @param settingsPath the path to the settings file
    /// @param invalidValue the value that was found to be invalid
    public InvalidSettingsException(String message, Path settingsPath, String invalidValue) {
        super(buildMessage(message, settingsPath, invalidValue));
        this.settingsPath = settingsPath;
        this.invalidValue = invalidValue;
    }

    /// Constructs an InvalidSettingsException caused by another exception.
    ///
    /// @param message a description of what is invalid
    /// @param settingsPath the path to the settings file
    /// @param cause the underlying exception
    public InvalidSettingsException(String message, Path settingsPath, Throwable cause) {
        super(buildMessage(message, settingsPath, null), cause);
        this.settingsPath = settingsPath;
        this.invalidValue = null;
    }

    private static String buildMessage(String message, Path settingsPath, String invalidValue) {
        StringBuilder sb = new StringBuilder();
        sb.append("\nInvalid settings in ").append(settingsPath).append("\n\n");
        sb.append("Error: ").append(message).append("\n");
        if (invalidValue != null) {
            sb.append("Invalid value: ").append(invalidValue).append("\n");
        }
        sb.append("\nValid cache_dir values:\n");
        sb.append("  - auto:largest-non-root  Use largest non-root drive\n");
        sb.append("  - auto:largest-any       Use largest drive (may be root)\n");
        sb.append("  - default                Use ~/.cache/vectordata\n");
        sb.append("  - /path/to/dir           Use specific absolute path\n");
        return sb.toString();
    }

    /// Returns the path to the invalid settings file.
    ///
    /// @return the path to the settings file
    public Path getSettingsPath() {
        return settingsPath;
    }

    /// Returns the invalid value if one was specified.
    ///
    /// @return the invalid value, or null if not applicable
    public String getInvalidValue() {
        return invalidValue;
    }
}
