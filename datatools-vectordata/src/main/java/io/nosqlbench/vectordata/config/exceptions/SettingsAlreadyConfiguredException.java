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

/// Exception thrown when attempting to set the cache directory when it is already configured.
///
/// This exception enforces the protection against accidental reconfiguration of the
/// cache directory. Users must explicitly use force=true or set protect_settings: false
/// in settings.yaml to override existing configuration.
public class SettingsAlreadyConfiguredException extends RuntimeException {

    private static final String MESSAGE_TEMPLATE =
        "\n" +
        "Cache directory is already configured.\n" +
        "\n" +
        "Current setting in %s:\n" +
        "  cache_dir: %s\n" +
        "\n" +
        "To change this setting:\n" +
        "  1. Edit %s manually, or\n" +
        "  2. Use setCacheDirectory(value, force=true) to override programmatically, or\n" +
        "  3. Use 'nbvectors config init --cache-dir <new-path> --force' from CLI\n" +
        "\n" +
        "This protection prevents accidental reconfiguration. To disable it,\n" +
        "set 'protect_settings: false' in settings.yaml.\n";

    /// The path to the existing settings file.
    private final Path settingsPath;
    /// The currently configured cache directory.
    private final String currentCacheDir;

    /// Constructs a SettingsAlreadyConfiguredException with the current configuration details.
    ///
    /// @param settingsPath the path to the existing settings file
    /// @param currentCacheDir the currently configured cache directory
    public SettingsAlreadyConfiguredException(Path settingsPath, String currentCacheDir) {
        super(String.format(MESSAGE_TEMPLATE, settingsPath, currentCacheDir, settingsPath));
        this.settingsPath = settingsPath;
        this.currentCacheDir = currentCacheDir;
    }

    /// Returns the path to the existing settings file.
    ///
    /// @return the path to the settings file
    public Path getSettingsPath() {
        return settingsPath;
    }

    /// Returns the currently configured cache directory.
    ///
    /// @return the current cache directory value
    public String getCurrentCacheDir() {
        return currentCacheDir;
    }
}
