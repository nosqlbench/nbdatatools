package io.nosqlbench.command.config.subcommands;

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

import io.nosqlbench.vectordata.config.MountPointInfo;
import io.nosqlbench.vectordata.config.VectorDataSettings;
import io.nosqlbench.vectordata.config.exceptions.CacheDirectoryNotConfiguredException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

/// Display current vectordata configuration.
///
/// Shows the current settings from ~/.config/vectordata/settings.yaml,
/// including cache directory path and status.
@CommandLine.Command(name = "show",
    header = "Display current configuration",
    description = "Show the current vectordata configuration settings",
    exitCodeList = {"0: success", "1: error"})
public class CMD_config_show implements Callable<Integer> {

    private static final Logger logger = LogManager.getLogger(CMD_config_show.class);

    @Override
    public Integer call() {
        try {
            Path settingsPath = VectorDataSettings.getDefaultSettingsPath();

            if (!VectorDataSettings.isConfigured()) {
                System.out.println("Configuration: " + settingsPath + " (not found)");
                System.out.println();
                System.out.println("vectordata is not configured.");
                System.out.println();
                System.out.println("To initialize, run: nbvectors config init");
                return 0;
            }

            VectorDataSettings settings = VectorDataSettings.load();

            System.out.println("Configuration: " + settings.getSettingsPath());
            System.out.println();

            // Cache directory info
            Path cacheDir = settings.getCacheDirectory();
            System.out.println("cache_dir: " + cacheDir);

            // Show resolved-from info if applicable
            settings.getOriginalCacheDirValue().ifPresent(original ->
                System.out.println("  Resolved from: " + original)
            );

            // Show directory status
            if (Files.exists(cacheDir)) {
                if (Files.isDirectory(cacheDir)) {
                    System.out.println("  Status: Active");

                    // Get space info
                    try {
                        FileStore store = Files.getFileStore(cacheDir);
                        long usedSpace = calculateDirectorySize(cacheDir);
                        long availableSpace = store.getUsableSpace();

                        System.out.println("  Used space: " + MountPointInfo.formatBytes(usedSpace));
                        System.out.println("  Available space: " + MountPointInfo.formatBytes(availableSpace));
                    } catch (Exception e) {
                        logger.debug("Failed to get space info: {}", e.getMessage());
                    }
                } else {
                    System.out.println("  Status: Error - path exists but is not a directory");
                }
            } else {
                System.out.println("  Status: Newly created (empty)");
            }

            System.out.println();
            System.out.println("protect_settings: " + settings.isProtectSettings());

            return 0;

        } catch (CacheDirectoryNotConfiguredException e) {
            System.out.println("Configuration: " + e.getSettingsPath() + " (not found)");
            System.out.println();
            System.out.println("vectordata is not configured.");
            System.out.println();
            System.out.println("To initialize, run: nbvectors config init");
            return 0;

        } catch (Exception e) {
            System.err.println("Error reading configuration: " + e.getMessage());
            logger.error("Failed to show configuration", e);
            return 1;
        }
    }

    /// Calculate the total size of a directory and its contents.
    ///
    /// @param directory the directory to measure
    /// @return total size in bytes
    private long calculateDirectorySize(Path directory) {
        try {
            return Files.walk(directory)
                .filter(Files::isRegularFile)
                .mapToLong(path -> {
                    try {
                        return Files.size(path);
                    } catch (Exception e) {
                        return 0;
                    }
                })
                .sum();
        } catch (Exception e) {
            return 0;
        }
    }
}
