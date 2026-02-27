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
import io.nosqlbench.vectordata.config.MountPointResolver;
import io.nosqlbench.vectordata.config.VectorDataSettings;
import io.nosqlbench.vectordata.config.exceptions.InvalidCacheDirectoryException;
import io.nosqlbench.vectordata.config.exceptions.NoSuitableMountPointException;
import io.nosqlbench.vectordata.config.exceptions.SettingsAlreadyConfiguredException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.BufferedReader;
import java.io.Console;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;

/// Initialize vectordata configuration with a cache directory.
///
/// This command creates or updates ~/.config/vectordata/settings.yaml
/// with the specified cache directory configuration.
@CommandLine.Command(name = "init",
    header = "Initialize vectordata configuration",
    description = "Create or update settings.yaml with cache directory configuration",
    exitCodeList = {"0: success", "1: error"})
public class CMD_config_init implements Callable<Integer> {

    /// Creates a new CMD_config_init instance.
    public CMD_config_init() {
    }

    private static final Logger logger = LogManager.getLogger(CMD_config_init.class);

    @CommandLine.Option(names = {"--cache-dir", "-c"},
        description = "Cache directory value. Options: auto:largest-non-root, auto:largest-any, default, or an absolute path")
    private String cacheDir;

    @CommandLine.Option(names = {"--force", "-f"},
        description = "Force overwrite of existing configuration")
    private boolean force = false;

    @Override
    public Integer call() {
        try {
            // If no cache-dir specified, run interactive mode
            if (cacheDir == null || cacheDir.trim().isEmpty()) {
                return runInteractive();
            }

            // Non-interactive mode with specified cache-dir
            VectorDataSettings.setCacheDirectory(cacheDir, force);

            Path settingsPath = VectorDataSettings.getDefaultSettingsPath();
            System.out.println("Configuration initialized successfully.");
            System.out.println();
            System.out.println("Settings file: " + settingsPath);

            // Load and display the result
            VectorDataSettings settings = VectorDataSettings.load();
            System.out.println("Cache directory: " + settings.getCacheDirectory());

            if (settings.wasResolved()) {
                settings.getOriginalCacheDirValue().ifPresent(original ->
                    System.out.println("Resolved from: " + original)
                );
            }

            return 0;

        } catch (SettingsAlreadyConfiguredException e) {
            System.err.println("Error: Configuration already exists.");
            System.err.println();
            System.err.println("Current cache_dir: " + e.getCurrentCacheDir());
            System.err.println("Settings file: " + e.getSettingsPath());
            System.err.println();
            System.err.println("To overwrite, use: nbvectors config init --cache-dir <value> --force");
            return 1;

        } catch (NoSuitableMountPointException e) {
            System.err.println("Error: " + e.getReason());
            System.err.println();
            System.err.println("Run 'nbvectors config list-mounts' to see available mount points.");
            System.err.println("Or specify an explicit path: nbvectors config init --cache-dir /path/to/cache");
            return 1;

        } catch (InvalidCacheDirectoryException e) {
            System.err.println("Error: Invalid cache directory.");
            System.err.println(e.getReason());
            return 1;

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            logger.error("Failed to initialize configuration", e);
            return 1;
        }
    }

    private int runInteractive() {
        System.out.println("vectordata Configuration Setup");
        System.out.println("==============================");
        System.out.println();

        // Show available mount points
        System.out.println("Available storage locations:");
        System.out.println();

        List<MountPointInfo> mounts = MountPointResolver.listWritableMountPoints();
        if (mounts.isEmpty()) {
            System.err.println("Warning: No writable mount points detected.");
            System.err.println("You may need to specify an explicit path.");
            System.err.println();
        } else {
            System.out.printf("  %-30s %-12s %-12s %s%n", "Mount Point", "Available", "Total", "Note");
            System.out.printf("  %-30s %-12s %-12s %s%n", "-".repeat(30), "-".repeat(10), "-".repeat(10), "-".repeat(10));
            for (MountPointInfo mount : mounts) {
                String note = mount.isRoot() ? "(root)" : "";
                System.out.printf("  %-30s %-12s %-12s %s%n",
                    mount.mountPoint(),
                    mount.formattedAvailableSpace(),
                    mount.formattedTotalSpace(),
                    note);
            }
            System.out.println();
        }

        // Show options
        System.out.println("Cache directory options:");
        System.out.println("  1. auto:largest-non-root  - Use largest non-root drive (recommended)");
        System.out.println("  2. auto:largest-any       - Use largest drive (may be root)");
        System.out.println("  3. default                - Use ~/.cache/vectordata");
        System.out.println("  4. Enter a custom path");
        System.out.println();

        // Get user input
        String choice = readLine("Select option (1-4) or enter path: ");
        if (choice == null) {
            System.err.println("No input provided. Aborting.");
            return 1;
        }

        choice = choice.trim();
        String selectedValue;

        switch (choice) {
            case "1" -> selectedValue = "auto:largest-non-root";
            case "2" -> selectedValue = "auto:largest-any";
            case "3" -> selectedValue = "default";
            case "4" -> {
                String customPath = readLine("Enter absolute path: ");
                if (customPath == null || customPath.trim().isEmpty()) {
                    System.err.println("No path provided. Aborting.");
                    return 1;
                }
                selectedValue = customPath.trim();
            }
            default -> {
                // Treat as a direct path
                if (choice.startsWith("/") || choice.startsWith("~") ||
                    choice.startsWith("auto:") || choice.equals("default")) {
                    selectedValue = choice;
                } else {
                    System.err.println("Invalid option: " + choice);
                    return 1;
                }
            }
        }

        // Check for existing configuration
        if (VectorDataSettings.isConfigured() && !force) {
            try {
                VectorDataSettings existing = VectorDataSettings.load();
                System.err.println();
                System.err.println("Warning: Configuration already exists!");
                System.err.println("Current cache_dir: " + existing.getCacheDirectory());
                System.err.println();
                String confirm = readLine("Overwrite? (y/N): ");
                if (confirm == null || !confirm.trim().toLowerCase().startsWith("y")) {
                    System.out.println("Aborted.");
                    return 0;
                }
                force = true;
            } catch (Exception e) {
                // Settings file exists but is invalid, allow overwrite
                force = true;
            }
        }

        // Set the configuration
        try {
            VectorDataSettings.setCacheDirectory(selectedValue, force);

            Path settingsPath = VectorDataSettings.getDefaultSettingsPath();
            System.out.println();
            System.out.println("Configuration initialized successfully!");
            System.out.println();
            System.out.println("Settings file: " + settingsPath);

            VectorDataSettings settings = VectorDataSettings.load();
            System.out.println("Cache directory: " + settings.getCacheDirectory());

            if (settings.wasResolved()) {
                settings.getOriginalCacheDirValue().ifPresent(original ->
                    System.out.println("Resolved from: " + original)
                );
            }

            return 0;

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    private String readLine(String prompt) {
        System.out.print(prompt);
        Console console = System.console();
        if (console != null) {
            return console.readLine();
        }
        // Fall back to BufferedReader for non-interactive environments
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            return reader.readLine();
        } catch (Exception e) {
            return null;
        }
    }
}
