package io.nosqlbench.vectordata.config;

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

import io.nosqlbench.vectordata.config.exceptions.CacheDirectoryNotConfiguredException;
import io.nosqlbench.vectordata.config.exceptions.InvalidCacheDirectoryException;
import io.nosqlbench.vectordata.config.exceptions.InvalidSettingsException;
import io.nosqlbench.vectordata.config.exceptions.SettingsAlreadyConfiguredException;
import io.nosqlbench.vectordata.utils.SHARED;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/// Main settings class for vectordata configuration.
///
/// This class manages the vectordata settings stored in ~/.config/vectordata/settings.yaml.
/// It handles:
/// - Loading settings from the standard location or a custom path
/// - Resolving auto:* directives to actual paths and persisting the result
/// - Validating cache directory paths
/// - Protecting against accidental reconfiguration
///
/// Example usage:
/// ```java
/// // Load settings from standard location
/// VectorDataSettings settings = VectorDataSettings.load();
/// Path cacheDir = settings.getCacheDirectory();
///
/// // Set cache directory programmatically
/// VectorDataSettings.setCacheDirectory("auto:largest-non-root");
/// ```
public class VectorDataSettings {

    private static final Logger logger = LogManager.getLogger(VectorDataSettings.class);

    /// Default configuration directory under user home.
    public static final String DEFAULT_CONFIG_DIR = ".config/vectordata";

    /// Settings file name.
    public static final String SETTINGS_FILE = "settings.yaml";

    /// Key for cache directory in settings.
    public static final String KEY_CACHE_DIR = "cache_dir";

    /// Key for protect settings flag.
    public static final String KEY_PROTECT_SETTINGS = "protect_settings";

    private final Path settingsPath;
    private final Path cacheDirectory;
    private final boolean protectSettings;
    private final boolean wasResolved;
    private final String originalCacheDirValue;

    private VectorDataSettings(
        Path settingsPath,
        Path cacheDirectory,
        boolean protectSettings,
        boolean wasResolved,
        String originalCacheDirValue
    ) {
        this.settingsPath = settingsPath;
        this.cacheDirectory = cacheDirectory;
        this.protectSettings = protectSettings;
        this.wasResolved = wasResolved;
        this.originalCacheDirValue = originalCacheDirValue;
    }

    /// Load settings from the standard location (~/.config/vectordata/settings.yaml).
    ///
    /// If cache_dir contains a resolvable value (auto:* or default), it will be
    /// resolved and the settings file will be updated with the resolved absolute path.
    ///
    /// @return the loaded settings
    /// @throws CacheDirectoryNotConfiguredException if settings.yaml does not exist
    /// @throws InvalidSettingsException if settings.yaml is malformed or cache_dir is invalid
    public static VectorDataSettings load() {
        Path settingsPath = getDefaultSettingsPath();
        return load(settingsPath);
    }

    /// Load settings from a custom location.
    ///
    /// Same resolution and persistence behavior as load().
    ///
    /// @param settingsFile the path to the settings file
    /// @return the loaded settings
    /// @throws CacheDirectoryNotConfiguredException if the settings file does not exist
    /// @throws InvalidSettingsException if the file is malformed or cache_dir is invalid
    public static VectorDataSettings load(Path settingsFile) {
        if (!Files.exists(settingsFile)) {
            throw new CacheDirectoryNotConfiguredException(settingsFile);
        }

        Map<String, Object> data = parseYaml(settingsFile);
        return loadFromMap(settingsFile, data);
    }

    /// Get the default path for the settings file.
    ///
    /// @return path to ~/.config/vectordata/settings.yaml
    public static Path getDefaultSettingsPath() {
        String home = System.getProperty("user.home");
        if (home == null || home.isEmpty()) {
            throw new InvalidSettingsException(
                "Cannot determine settings path - user.home system property is not set",
                Path.of("~", DEFAULT_CONFIG_DIR, SETTINGS_FILE)
            );
        }
        return Path.of(home, DEFAULT_CONFIG_DIR, SETTINGS_FILE);
    }

    /// Check if settings are configured at the default location.
    ///
    /// @return true if settings.yaml exists at the default location
    public static boolean isConfigured() {
        return Files.exists(getDefaultSettingsPath());
    }

    /// Check if settings are configured at a specific location.
    ///
    /// @param settingsFile the path to check
    /// @return true if the settings file exists
    public static boolean isConfigured(Path settingsFile) {
        return Files.exists(settingsFile);
    }

    /// Set the cache directory in the default settings file.
    ///
    /// @param cacheDirValue the cache_dir value (path, "default", or "auto:*")
    /// @throws SettingsAlreadyConfiguredException if cache_dir is already set and protect_settings is true
    /// @throws InvalidCacheDirectoryException if the value is invalid
    public static void setCacheDirectory(String cacheDirValue) {
        setCacheDirectory(cacheDirValue, false);
    }

    /// Set the cache directory with optional force override.
    ///
    /// @param cacheDirValue the cache_dir value
    /// @param force if true, overwrite existing configuration
    /// @throws SettingsAlreadyConfiguredException if settings exist, force is false, and protect_settings is true
    /// @throws InvalidCacheDirectoryException if the value is invalid
    public static void setCacheDirectory(String cacheDirValue, boolean force) {
        Path settingsPath = getDefaultSettingsPath();
        setCacheDirectory(settingsPath, cacheDirValue, force);
    }

    /// Set the cache directory in a specific settings file.
    ///
    /// @param settingsFile the path to the settings file
    /// @param cacheDirValue the cache_dir value
    /// @param force if true, overwrite existing configuration
    /// @throws SettingsAlreadyConfiguredException if settings exist, force is false, and protect_settings is true
    /// @throws InvalidCacheDirectoryException if the value is invalid
    public static void setCacheDirectory(Path settingsFile, String cacheDirValue, boolean force) {
        // Check for existing configuration
        if (Files.exists(settingsFile) && !force) {
            Map<String, Object> existing = parseYaml(settingsFile);
            String existingCacheDir = (String) existing.get(KEY_CACHE_DIR);
            boolean protectSettings = Boolean.TRUE.equals(existing.getOrDefault(KEY_PROTECT_SETTINGS, true));

            if (existingCacheDir != null && protectSettings) {
                throw new SettingsAlreadyConfiguredException(settingsFile, existingCacheDir);
            }
        }

        // Resolve if needed
        Path resolvedPath;
        boolean requiresResolution = MountPointResolver.requiresResolution(cacheDirValue);
        if (requiresResolution) {
            resolvedPath = MountPointResolver.resolve(cacheDirValue);
            logger.info("Resolved cache_dir '{}' -> '{}'", cacheDirValue, resolvedPath);
        } else {
            resolvedPath = MountPointResolver.resolve(cacheDirValue);
        }

        // Ensure cache directory exists
        ensureDirectoryExists(resolvedPath);

        // Save settings
        saveSettings(settingsFile, resolvedPath, true);

        if (requiresResolution) {
            logger.info("Settings initialized with cache_dir: {} (resolved from {})", resolvedPath, cacheDirValue);
        } else {
            logger.info("Settings initialized with cache_dir: {}", resolvedPath);
        }
    }

    /// Get the resolved cache directory path.
    ///
    /// After load(), this always returns an absolute path (never auto:* or default).
    ///
    /// @return the cache directory path
    public Path getCacheDirectory() {
        return cacheDirectory;
    }

    /// Check if the settings file was updated during load due to resolution.
    ///
    /// @return true if cache_dir was resolved from auto:* or default
    public boolean wasResolved() {
        return wasResolved;
    }

    /// Get the original value before resolution (for logging/diagnostics).
    ///
    /// @return the original cache_dir value if resolution occurred, empty otherwise
    public Optional<String> getOriginalCacheDirValue() {
        return Optional.ofNullable(originalCacheDirValue);
    }

    /// Get the path to the settings file.
    ///
    /// @return the settings file path
    public Path getSettingsPath() {
        return settingsPath;
    }

    /// Check if settings are protected against accidental changes.
    ///
    /// @return true if protect_settings is enabled
    public boolean isProtectSettings() {
        return protectSettings;
    }

    // --- Private implementation ---

    private static Map<String, Object> parseYaml(Path settingsFile) {
        try {
            String yaml = Files.readString(settingsFile);
            Object parsed = SHARED.yamlLoader.loadFromString(yaml);

            if (parsed == null) {
                throw new InvalidSettingsException("Settings file is empty", settingsFile);
            }

            if (!(parsed instanceof Map)) {
                throw new InvalidSettingsException(
                    "Settings file must be a YAML map (key: value format)",
                    settingsFile
                );
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> data = (Map<String, Object>) parsed;
            return data;

        } catch (IOException e) {
            throw new InvalidSettingsException("Failed to read settings file", settingsFile, e);
        } catch (Exception e) {
            if (e instanceof InvalidSettingsException) {
                throw e;
            }
            throw new InvalidSettingsException("Failed to parse YAML: " + e.getMessage(), settingsFile, e);
        }
    }

    private static VectorDataSettings loadFromMap(Path settingsFile, Map<String, Object> data) {
        // Extract cache_dir
        Object cacheDirObj = data.get(KEY_CACHE_DIR);
        if (cacheDirObj == null) {
            throw new InvalidSettingsException("Missing required key: " + KEY_CACHE_DIR, settingsFile);
        }

        String cacheDirValue = cacheDirObj.toString().trim();
        if (cacheDirValue.isEmpty()) {
            throw new InvalidSettingsException("cache_dir value is empty", settingsFile, cacheDirValue);
        }

        // Extract protect_settings (default true)
        boolean protectSettings = true;
        Object protectObj = data.get(KEY_PROTECT_SETTINGS);
        if (protectObj != null) {
            if (protectObj instanceof Boolean) {
                protectSettings = (Boolean) protectObj;
            } else {
                protectSettings = Boolean.parseBoolean(protectObj.toString());
            }
        }

        // Check if resolution is needed
        boolean needsResolution = MountPointResolver.requiresResolution(cacheDirValue);
        String originalValue = needsResolution ? cacheDirValue : null;

        // Resolve the path
        Path resolvedPath = MountPointResolver.resolve(cacheDirValue);

        // If we resolved, persist the resolved value
        if (needsResolution) {
            logger.info("Resolved cache_dir '{}' -> '{}'", cacheDirValue, resolvedPath);
            ensureDirectoryExists(resolvedPath);
            saveSettings(settingsFile, resolvedPath, protectSettings);
        } else {
            // Ensure directory exists even for explicit paths
            ensureDirectoryExists(resolvedPath);
        }

        return new VectorDataSettings(
            settingsFile,
            resolvedPath,
            protectSettings,
            needsResolution,
            originalValue
        );
    }

    private static void ensureDirectoryExists(Path directory) {
        if (Files.exists(directory)) {
            if (!Files.isDirectory(directory)) {
                throw new InvalidCacheDirectoryException(directory, "Path exists but is not a directory");
            }
            return;
        }

        try {
            // Create with secure permissions (0700)
            Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwx------");
            Files.createDirectories(directory, PosixFilePermissions.asFileAttribute(perms));
            logger.info("Created cache directory: {}", directory);
        } catch (UnsupportedOperationException e) {
            // Non-POSIX filesystem, create without permission attribute
            try {
                Files.createDirectories(directory);
                logger.info("Created cache directory: {}", directory);
            } catch (IOException ioe) {
                throw new InvalidCacheDirectoryException(directory, "Failed to create directory: " + ioe.getMessage());
            }
        } catch (IOException e) {
            throw new InvalidCacheDirectoryException(directory, "Failed to create directory: " + e.getMessage());
        }
    }

    private static void saveSettings(Path settingsFile, Path cacheDirectory, boolean protectSettings) {
        // Ensure parent directory exists
        Path parent = settingsFile.getParent();
        if (parent != null && !Files.exists(parent)) {
            try {
                Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwx------");
                Files.createDirectories(parent, PosixFilePermissions.asFileAttribute(perms));
            } catch (UnsupportedOperationException e) {
                try {
                    Files.createDirectories(parent);
                } catch (IOException ioe) {
                    throw new InvalidSettingsException(
                        "Failed to create settings directory: " + ioe.getMessage(),
                        settingsFile
                    );
                }
            } catch (IOException e) {
                throw new InvalidSettingsException(
                    "Failed to create settings directory: " + e.getMessage(),
                    settingsFile
                );
            }
        }

        // Build settings map
        Map<String, Object> data = new LinkedHashMap<>();
        data.put(KEY_CACHE_DIR, cacheDirectory.toString());
        data.put(KEY_PROTECT_SETTINGS, protectSettings);

        // Dump to YAML
        String yaml = SHARED.yamlDumper.dumpToString(data);

        try {
            // Write with secure permissions (0600)
            Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rw-------");
            Files.writeString(settingsFile, yaml);
            try {
                Files.setPosixFilePermissions(settingsFile, perms);
            } catch (UnsupportedOperationException e) {
                // Non-POSIX filesystem, ignore
            }
            logger.debug("Saved settings to: {}", settingsFile);
        } catch (IOException e) {
            throw new InvalidSettingsException("Failed to write settings file: " + e.getMessage(), settingsFile, e);
        }
    }

    @Override
    public String toString() {
        return "VectorDataSettings{" +
               "settingsPath=" + settingsPath +
               ", cacheDirectory=" + cacheDirectory +
               ", protectSettings=" + protectSettings +
               ", wasResolved=" + wasResolved +
               '}';
    }
}
