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

/// Exception thrown when the vectordata cache directory is not configured.
///
/// This exception is thrown when settings.yaml does not exist or does not
/// contain a valid cache_dir configuration. The exception message provides
/// detailed instructions for configuring the cache directory.
public class CacheDirectoryNotConfiguredException extends RuntimeException {

    private static final String MESSAGE_TEMPLATE =
        "\n" +
        "vectordata cache directory is not configured.\n" +
        "\n" +
        "Configuration file: %s (not found)\n" +
        "\n" +
        "To configure, create this file with contents:\n" +
        "\n" +
        "  cache_dir: auto:largest-non-root\n" +
        "\n" +
        "Available options:\n" +
        "  - auto:largest-non-root  Use largest non-root drive (recommended)\n" +
        "  - auto:largest-any       Use largest drive (may be root)\n" +
        "  - default                Use ~/.cache/vectordata\n" +
        "  - /path/to/dir           Use specific path\n" +
        "\n" +
        "Or run: nbvectors config init\n";

    /// The path to the expected settings file.
    private final Path settingsPath;

    /// Constructs a CacheDirectoryNotConfiguredException with the path to the missing settings file.
    ///
    /// @param settingsPath the path where the settings file was expected
    public CacheDirectoryNotConfiguredException(Path settingsPath) {
        super(String.format(MESSAGE_TEMPLATE, settingsPath));
        this.settingsPath = settingsPath;
    }

    /// Returns the path to the settings file that was not found.
    ///
    /// @return the path to the expected settings file
    public Path getSettingsPath() {
        return settingsPath;
    }
}
