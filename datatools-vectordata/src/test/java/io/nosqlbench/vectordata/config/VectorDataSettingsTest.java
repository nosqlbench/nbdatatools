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
import io.nosqlbench.vectordata.config.exceptions.InvalidSettingsException;
import io.nosqlbench.vectordata.config.exceptions.SettingsAlreadyConfiguredException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/// Unit tests for [VectorDataSettings].
class VectorDataSettingsTest {

    @TempDir
    Path tempDir;

    private Path settingsFile;
    private Path cacheDir;

    @BeforeEach
    void setUp() {
        settingsFile = tempDir.resolve("settings.yaml");
        cacheDir = tempDir.resolve("cache");
    }

    @AfterEach
    void tearDown() {
        // Cleanup is handled by @TempDir
    }

    @Test
    void load_withMissingFile_throwsCacheDirectoryNotConfiguredException() {
        assertThrows(CacheDirectoryNotConfiguredException.class,
            () -> VectorDataSettings.load(settingsFile));
    }

    @Test
    void load_withEmptyFile_throwsInvalidSettingsException() throws IOException {
        Files.writeString(settingsFile, "");
        assertThrows(InvalidSettingsException.class,
            () -> VectorDataSettings.load(settingsFile));
    }

    @Test
    void load_withMissingCacheDir_throwsInvalidSettingsException() throws IOException {
        Files.writeString(settingsFile, "other_key: value\n");
        assertThrows(InvalidSettingsException.class,
            () -> VectorDataSettings.load(settingsFile));
    }

    @Test
    void load_withEmptyCacheDir_throwsInvalidSettingsException() throws IOException {
        Files.writeString(settingsFile, "cache_dir: \"\"\n");
        assertThrows(InvalidSettingsException.class,
            () -> VectorDataSettings.load(settingsFile));
    }

    @Test
    void load_withExplicitPath_succeedsAndCreatesDirectory() throws IOException {
        Files.writeString(settingsFile,
            "cache_dir: " + cacheDir.toAbsolutePath() + "\n");

        VectorDataSettings settings = VectorDataSettings.load(settingsFile);

        assertEquals(cacheDir.toAbsolutePath(), settings.getCacheDirectory());
        assertFalse(settings.wasResolved());
        assertTrue(settings.getOriginalCacheDirValue().isEmpty());
        assertTrue(Files.exists(cacheDir), "Cache directory should be created");
    }

    @Test
    void load_withDefaultDirective_resolvesAndPersists() throws IOException {
        Files.writeString(settingsFile, "cache_dir: default\n");

        VectorDataSettings settings = VectorDataSettings.load(settingsFile);

        assertTrue(settings.wasResolved());
        assertTrue(settings.getOriginalCacheDirValue().isPresent());
        assertEquals("default", settings.getOriginalCacheDirValue().get());

        // Verify the file was updated with resolved path
        String updatedContent = Files.readString(settingsFile);
        assertFalse(updatedContent.contains("default"), "File should contain resolved path, not 'default'");
        assertTrue(updatedContent.contains("cache_dir:"));
    }

    @Test
    void load_withProtectSettingsTrue_returnsProtectedSettings() throws IOException {
        Files.writeString(settingsFile,
            "cache_dir: " + cacheDir.toAbsolutePath() + "\n" +
            "protect_settings: true\n");

        VectorDataSettings settings = VectorDataSettings.load(settingsFile);

        assertTrue(settings.isProtectSettings());
    }

    @Test
    void load_withProtectSettingsFalse_returnsUnprotectedSettings() throws IOException {
        Files.writeString(settingsFile,
            "cache_dir: " + cacheDir.toAbsolutePath() + "\n" +
            "protect_settings: false\n");

        VectorDataSettings settings = VectorDataSettings.load(settingsFile);

        assertFalse(settings.isProtectSettings());
    }

    @Test
    void load_withoutProtectSettings_defaultsToTrue() throws IOException {
        Files.writeString(settingsFile,
            "cache_dir: " + cacheDir.toAbsolutePath() + "\n");

        VectorDataSettings settings = VectorDataSettings.load(settingsFile);

        assertTrue(settings.isProtectSettings());
    }

    @Test
    void setCacheDirectory_withNewFile_createsSettings() {
        VectorDataSettings.setCacheDirectory(settingsFile, cacheDir.toAbsolutePath().toString(), false);

        assertTrue(Files.exists(settingsFile));
        assertTrue(Files.exists(cacheDir));

        VectorDataSettings settings = VectorDataSettings.load(settingsFile);
        assertEquals(cacheDir.toAbsolutePath(), settings.getCacheDirectory());
    }

    @Test
    void setCacheDirectory_withExistingProtected_throwsSettingsAlreadyConfiguredException() throws IOException {
        // First, create protected settings
        VectorDataSettings.setCacheDirectory(settingsFile, cacheDir.toAbsolutePath().toString(), false);

        // Then try to overwrite
        Path newCache = tempDir.resolve("new-cache");
        assertThrows(SettingsAlreadyConfiguredException.class,
            () -> VectorDataSettings.setCacheDirectory(settingsFile, newCache.toAbsolutePath().toString(), false));
    }

    @Test
    void setCacheDirectory_withForce_overwritesExisting() throws IOException {
        // First, create settings
        VectorDataSettings.setCacheDirectory(settingsFile, cacheDir.toAbsolutePath().toString(), false);

        // Overwrite with force
        Path newCache = tempDir.resolve("new-cache");
        VectorDataSettings.setCacheDirectory(settingsFile, newCache.toAbsolutePath().toString(), true);

        VectorDataSettings settings = VectorDataSettings.load(settingsFile);
        assertEquals(newCache.toAbsolutePath(), settings.getCacheDirectory());
    }

    @Test
    void setCacheDirectory_withUnprotectedSettings_allowsOverwrite() throws IOException {
        // Create unprotected settings
        Files.writeString(settingsFile,
            "cache_dir: " + cacheDir.toAbsolutePath() + "\n" +
            "protect_settings: false\n");
        Files.createDirectories(cacheDir);

        // Overwrite without force
        Path newCache = tempDir.resolve("new-cache");
        VectorDataSettings.setCacheDirectory(settingsFile, newCache.toAbsolutePath().toString(), false);

        VectorDataSettings settings = VectorDataSettings.load(settingsFile);
        assertEquals(newCache.toAbsolutePath(), settings.getCacheDirectory());
    }

    @Test
    void isConfigured_withMissingFile_returnsFalse() {
        assertFalse(VectorDataSettings.isConfigured(settingsFile));
    }

    @Test
    void isConfigured_withExistingFile_returnsTrue() throws IOException {
        Files.writeString(settingsFile, "cache_dir: /some/path\n");
        assertTrue(VectorDataSettings.isConfigured(settingsFile));
    }

    @Test
    void getSettingsPath_returnsCorrectPath() throws IOException {
        Files.writeString(settingsFile,
            "cache_dir: " + cacheDir.toAbsolutePath() + "\n");

        VectorDataSettings settings = VectorDataSettings.load(settingsFile);

        assertEquals(settingsFile, settings.getSettingsPath());
    }

    @Test
    void load_withInvalidYaml_throwsInvalidSettingsException() throws IOException {
        Files.writeString(settingsFile, "cache_dir: [invalid: yaml: here\n");
        assertThrows(InvalidSettingsException.class,
            () -> VectorDataSettings.load(settingsFile));
    }

    @Test
    void load_withNonMapYaml_throwsInvalidSettingsException() throws IOException {
        Files.writeString(settingsFile, "- item1\n- item2\n");
        assertThrows(InvalidSettingsException.class,
            () -> VectorDataSettings.load(settingsFile));
    }
}
