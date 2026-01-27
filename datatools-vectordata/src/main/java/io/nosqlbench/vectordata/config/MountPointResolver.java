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

import io.nosqlbench.vectordata.config.exceptions.InvalidCacheDirectoryException;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/// Resolves cache directory values including auto:* directives.
///
/// This class handles the resolution of cache_dir values from settings.yaml,
/// including automatic mount point selection based on available space.
///
/// Supported directives:
/// - `auto:largest-non-root` - Select the largest writable mount point that is not root
/// - `auto:largest-any` - Select the largest writable mount point (including root)
/// - `default` - Resolve to ~/.cache/vectordata
/// - Any absolute path - Validated and returned as-is
public class MountPointResolver {

    /// The subdirectory name to create under mount points for vectordata cache.
    public static final String CACHE_SUBDIR = "vectordata-cache";

    /// Default cache directory under user home.
    public static final String DEFAULT_CACHE_PATH = ".cache/vectordata";

    private MountPointResolver() {
        // Utility class
    }

    /// Checks if a cache_dir value requires resolution.
    ///
    /// @param cacheDirValue the cache_dir value from settings
    /// @return true if the value needs resolution (auto:*, default), false for absolute paths
    public static boolean requiresResolution(String cacheDirValue) {
        if (cacheDirValue == null) {
            return false;
        }
        String trimmed = cacheDirValue.trim().toLowerCase();
        return trimmed.startsWith("auto:") || trimmed.equals("default");
    }

    /// Resolves a cache_dir value to an absolute path.
    ///
    /// For auto:* directives and "default", this performs resolution.
    /// For absolute paths, this validates and returns the path.
    ///
    /// @param cacheDirValue the cache_dir value from settings
    /// @return the resolved absolute path
    /// @throws InvalidCacheDirectoryException if the value/path is invalid
    public static Path resolve(String cacheDirValue) {
        if (cacheDirValue == null || cacheDirValue.trim().isEmpty()) {
            throw new InvalidCacheDirectoryException((Path) null, "cache_dir value is null or empty");
        }

        String trimmed = cacheDirValue.trim();
        String lowerValue = trimmed.toLowerCase();

        if (lowerValue.equals("default")) {
            return resolveDefault();
        } else if (lowerValue.equals("auto:largest-non-root")) {
            return findOptimalCacheDirectory(false);
        } else if (lowerValue.equals("auto:largest-any")) {
            return findOptimalCacheDirectory(true);
        } else if (lowerValue.startsWith("auto:")) {
            throw new InvalidCacheDirectoryException(
                trimmed,
                "Unknown auto directive. Valid options: auto:largest-non-root, auto:largest-any"
            );
        } else {
            // Treat as explicit path
            return validateExplicitPath(trimmed);
        }
    }

    /// Resolves the "default" directive to ~/.cache/vectordata.
    ///
    /// @return the absolute path to the default cache directory
    private static Path resolveDefault() {
        String home = System.getProperty("user.home");
        if (home == null || home.isEmpty()) {
            throw new InvalidCacheDirectoryException(
                "default",
                "Cannot resolve 'default' - user.home system property is not set"
            );
        }
        return Path.of(home, DEFAULT_CACHE_PATH);
    }

    /// Validates an explicit path for use as a cache directory.
    ///
    /// @param pathString the path to validate
    /// @return the validated path
    /// @throws InvalidCacheDirectoryException if the path is invalid
    private static Path validateExplicitPath(String pathString) {
        // Expand tilde
        if (pathString.startsWith("~")) {
            String home = System.getProperty("user.home");
            if (home != null) {
                pathString = home + pathString.substring(1);
            }
        }

        Path path = Path.of(pathString);

        // Must be absolute
        if (!path.isAbsolute()) {
            throw new InvalidCacheDirectoryException(
                path,
                "Cache directory path must be absolute (starting with /)"
            );
        }

        // If exists, must be a directory and writable
        if (Files.exists(path)) {
            if (!Files.isDirectory(path)) {
                throw new InvalidCacheDirectoryException(
                    path,
                    "Path exists but is not a directory"
                );
            }
            if (!Files.isWritable(path)) {
                throw new InvalidCacheDirectoryException(
                    path,
                    "Directory exists but is not writable"
                );
            }
        } else {
            // Check if parent exists and is writable
            Path parent = path.getParent();
            if (parent != null) {
                if (Files.exists(parent)) {
                    if (!Files.isWritable(parent)) {
                        throw new InvalidCacheDirectoryException(
                            path,
                            "Parent directory " + parent + " is not writable"
                        );
                    }
                }
                // If parent doesn't exist, we'll create it later during actual use
            }
        }

        return path;
    }

    /// Finds the optimal cache directory based on available mount points.
    ///
    /// @param includeRoot whether to include the root filesystem in selection
    /// @return the resolved cache directory path
    /// If no suitable mount point is found, falls back to the default cache directory.
    public static Path findOptimalCacheDirectory(boolean includeRoot) {
        return selectCacheDirectory(listWritableMountPoints(), includeRoot);
    }

    static Path selectCacheDirectory(List<MountPointInfo> mounts, boolean includeRoot) {
        if (mounts == null || mounts.isEmpty()) {
            return resolveDefault();
        }

        List<MountPointInfo> candidates = mounts.stream()
            .filter(m -> includeRoot || !m.isRoot())
            .collect(Collectors.toList());

        if (candidates.isEmpty()) {
            return resolveDefault();
        }

        Optional<MountPointInfo> best = candidates.stream()
            .max(Comparator.comparingLong(MountPointInfo::availableSpace));

        if (best.isEmpty()) {
            return resolveDefault();
        }

        return best.get().mountPoint().resolve(CACHE_SUBDIR);
    }

    /// Lists all writable mount points on the system.
    ///
    /// @return a list of mount point information, sorted by available space (descending)
    public static List<MountPointInfo> listWritableMountPoints() {
        List<MountPointInfo> results = new ArrayList<>();

        for (FileStore store : FileSystems.getDefault().getFileStores()) {
            try {
                // Skip read-only stores
                if (store.isReadOnly()) {
                    continue;
                }

                // Get mount point path
                Path mountPoint = getMountPoint(store);
                if (mountPoint == null) {
                    continue;
                }

                // Skip if not writable
                if (!Files.isWritable(mountPoint)) {
                    continue;
                }

                // Skip pseudo-filesystems
                if (isPseudoFilesystem(store, mountPoint)) {
                    continue;
                }

                long totalSpace = store.getTotalSpace();
                long availableSpace = store.getUsableSpace();
                boolean isRoot = mountPoint.equals(Path.of("/"));

                results.add(new MountPointInfo(mountPoint, totalSpace, availableSpace, isRoot));

            } catch (IOException e) {
                // Skip stores that throw errors when querying
                continue;
            }
        }

        // Sort by available space (largest first)
        results.sort(Comparator.comparingLong(MountPointInfo::availableSpace).reversed());

        return results;
    }

    /// Extracts the mount point path from a FileStore.
    ///
    /// @param store the file store
    /// @return the mount point path, or null if it cannot be determined
    private static Path getMountPoint(FileStore store) {
        // FileStore.toString() typically returns "path (type)"
        String storeString = store.toString();
        int parenIndex = storeString.lastIndexOf('(');
        String pathPart;
        if (parenIndex > 0) {
            pathPart = storeString.substring(0, parenIndex).trim();
        } else {
            pathPart = storeString.trim();
        }

        if (pathPart.isEmpty()) {
            return null;
        }

        try {
            Path path = Path.of(pathPart);
            if (Files.exists(path)) {
                return path;
            }
        } catch (Exception e) {
            // Invalid path
        }

        return null;
    }

    /// Checks if a file store represents a pseudo-filesystem that should be skipped.
    ///
    /// @param store the file store
    /// @param mountPoint the mount point path
    /// @return true if this is a pseudo-filesystem
    private static boolean isPseudoFilesystem(FileStore store, Path mountPoint) {
        String type = store.type().toLowerCase();
        String path = mountPoint.toString();

        // Skip common pseudo-filesystems
        if (type.equals("tmpfs") || type.equals("devtmpfs") ||
            type.equals("proc") || type.equals("sysfs") ||
            type.equals("devpts") || type.equals("securityfs") ||
            type.equals("cgroup") || type.equals("cgroup2") ||
            type.equals("pstore") || type.equals("debugfs") ||
            type.equals("tracefs") || type.equals("hugetlbfs") ||
            type.equals("mqueue") || type.equals("fusectl") ||
            type.equals("configfs") || type.equals("binfmt_misc") ||
            type.equals("overlay")) {
            return true;
        }

        // Skip system directories
        if (path.startsWith("/proc") || path.startsWith("/sys") ||
            path.startsWith("/dev") || path.startsWith("/run") ||
            path.equals("/boot") || path.equals("/boot/efi")) {
            return true;
        }

        return false;
    }
}
