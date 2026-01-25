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

/// Exception thrown when the specified cache directory path is invalid.
///
/// This exception is thrown when:
/// - The path is not absolute (for explicit paths)
/// - The path doesn't exist and cannot be created
/// - The path exists but is not writable
/// - The path is a file, not a directory
public class InvalidCacheDirectoryException extends RuntimeException {

    private static final String MESSAGE_TEMPLATE =
        "\n" +
        "Invalid cache directory: %s\n" +
        "\n" +
        "Reason: %s\n" +
        "\n" +
        "The cache directory must be:\n" +
        "  - An absolute path (starting with /)\n" +
        "  - Either existing and writable, or in a location where it can be created\n" +
        "  - A directory, not a file\n" +
        "\n" +
        "Examples of valid paths:\n" +
        "  - /data/vectordata-cache\n" +
        "  - /home/user/.cache/vectordata\n" +
        "  - /mnt/nvme0/vectordata-cache\n";

    private final Path path;
    private final String reason;

    /// Constructs an InvalidCacheDirectoryException with the invalid path and reason.
    ///
    /// @param path the invalid cache directory path
    /// @param reason a description of why the path is invalid
    public InvalidCacheDirectoryException(Path path, String reason) {
        super(String.format(MESSAGE_TEMPLATE, path, reason));
        this.path = path;
        this.reason = reason;
    }

    /// Constructs an InvalidCacheDirectoryException with a string path and reason.
    ///
    /// @param pathString the invalid cache directory path as a string
    /// @param reason a description of why the path is invalid
    public InvalidCacheDirectoryException(String pathString, String reason) {
        super(String.format(MESSAGE_TEMPLATE, pathString, reason));
        this.path = pathString != null ? Path.of(pathString) : null;
        this.reason = reason;
    }

    /// Returns the invalid cache directory path.
    ///
    /// @return the invalid path, or null if the path string could not be parsed
    public Path getPath() {
        return path;
    }

    /// Returns the reason why the path is invalid.
    ///
    /// @return a description of why the path is invalid
    public String getReason() {
        return reason;
    }
}
