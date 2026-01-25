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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.util.List;
import java.util.concurrent.Callable;

/// List available mount points for cache storage.
///
/// Shows all writable mount points on the system that could be used
/// for cache storage, along with their available and total space.
@CommandLine.Command(name = "list-mounts",
    header = "List available mount points for cache storage",
    description = "Show all writable mount points suitable for cache storage",
    exitCodeList = {"0: success"})
public class CMD_config_list_mounts implements Callable<Integer> {

    private static final Logger logger = LogManager.getLogger(CMD_config_list_mounts.class);

    @CommandLine.Option(names = {"--all", "-a"},
        description = "Include all mount points, even those with minimal space")
    private boolean showAll = false;

    @Override
    public Integer call() {
        System.out.println("Available mount points for cache storage:");
        System.out.println();

        List<MountPointInfo> mounts = MountPointResolver.listWritableMountPoints();

        if (mounts.isEmpty()) {
            System.out.println("  No writable mount points found.");
            System.out.println();
            System.out.println("  This might indicate:");
            System.out.println("    - Running in a restricted environment");
            System.out.println("    - No storage devices are mounted");
            System.out.println("    - Permissions issues");
            System.out.println();
            System.out.println("  You can still specify an explicit path:");
            System.out.println("    nbvectors config init --cache-dir /path/to/cache");
            return 0;
        }

        // Filter small mounts unless --all is specified
        long minSpace = showAll ? 0 : 100L * 1024 * 1024; // 100 MB minimum
        List<MountPointInfo> filtered = mounts.stream()
            .filter(m -> m.availableSpace() >= minSpace)
            .toList();

        if (filtered.isEmpty() && !showAll) {
            System.out.println("  No mount points with sufficient space (>100 MB) found.");
            System.out.println("  Use --all to show all mount points.");
            return 0;
        }

        // Print header
        System.out.printf("  %-35s %-12s %-12s %s%n",
            "Mount Point", "Available", "Total", "Writable");
        System.out.printf("  %-35s %-12s %-12s %s%n",
            "-".repeat(35), "-".repeat(10), "-".repeat(10), "-".repeat(10));

        // Print mount points
        for (MountPointInfo mount : filtered) {
            System.out.printf("  %-35s %-12s %-12s %s%n",
                truncatePath(mount.mountPoint().toString(), 35),
                mount.formattedAvailableSpace(),
                mount.formattedTotalSpace(),
                mount.writableLabel());
        }

        // Summary
        System.out.println();
        int hidden = mounts.size() - filtered.size();
        if (hidden > 0) {
            System.out.println("  (" + hidden + " mount point(s) with <100 MB hidden. Use --all to show.)");
            System.out.println();
        }

        // Recommendations
        System.out.println("Usage:");
        System.out.println("  nbvectors config init --cache-dir auto:largest-non-root");
        System.out.println("  nbvectors config init --cache-dir auto:largest-any");
        System.out.println("  nbvectors config init --cache-dir /specific/path");

        return 0;
    }

    /// Truncate a path string to fit within a maximum length.
    ///
    /// @param path the path string
    /// @param maxLength the maximum length
    /// @return the truncated path, with "..." prefix if truncated
    private String truncatePath(String path, int maxLength) {
        if (path.length() <= maxLength) {
            return path;
        }
        return "..." + path.substring(path.length() - maxLength + 3);
    }
}
