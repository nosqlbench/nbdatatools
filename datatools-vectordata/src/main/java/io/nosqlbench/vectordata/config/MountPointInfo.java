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

import java.nio.file.Path;
import java.util.Objects;

/// Information about a mount point suitable for cache storage.
///
/// This class captures key information about a filesystem mount point,
/// including its path, storage capacity, and whether it is the root filesystem.
public final class MountPointInfo {

    private final Path mountPoint;
    private final long totalSpace;
    private final long availableSpace;
    private final boolean isRoot;

    /// Constructs a MountPointInfo with the specified values.
    ///
    /// @param mountPoint the path to the mount point
    /// @param totalSpace total storage capacity in bytes
    /// @param availableSpace available storage capacity in bytes
    /// @param isRoot true if this is the root filesystem (mounted at "/")
    public MountPointInfo(Path mountPoint, long totalSpace, long availableSpace, boolean isRoot) {
        this.mountPoint = mountPoint;
        this.totalSpace = totalSpace;
        this.availableSpace = availableSpace;
        this.isRoot = isRoot;
    }

    /// Returns the path to the mount point.
    ///
    /// @return the mount point path
    public Path mountPoint() {
        return mountPoint;
    }

    /// Returns the total storage capacity in bytes.
    ///
    /// @return total space in bytes
    public long totalSpace() {
        return totalSpace;
    }

    /// Returns the available storage capacity in bytes.
    ///
    /// @return available space in bytes
    public long availableSpace() {
        return availableSpace;
    }

    /// Returns true if this is the root filesystem.
    ///
    /// @return true if mounted at "/"
    public boolean isRoot() {
        return isRoot;
    }

    /// Format a byte count as a human-readable string with appropriate unit.
    ///
    /// @param bytes the number of bytes
    /// @return a formatted string like "1.5 TB", "256 GB", "100 MB", etc.
    public static String formatBytes(long bytes) {
        if (bytes < 0) {
            return "unknown";
        }
        if (bytes >= 1L << 40) { // >= 1 TB
            return String.format("%.1f TB", bytes / (double) (1L << 40));
        } else if (bytes >= 1L << 30) { // >= 1 GB
            return String.format("%.1f GB", bytes / (double) (1L << 30));
        } else if (bytes >= 1L << 20) { // >= 1 MB
            return String.format("%.1f MB", bytes / (double) (1L << 20));
        } else if (bytes >= 1L << 10) { // >= 1 KB
            return String.format("%.1f KB", bytes / (double) (1L << 10));
        } else {
            return bytes + " B";
        }
    }

    /// Returns the total space formatted as a human-readable string.
    ///
    /// @return formatted total space (e.g., "1.5 TB")
    public String formattedTotalSpace() {
        return formatBytes(totalSpace);
    }

    /// Returns the available space formatted as a human-readable string.
    ///
    /// @return formatted available space (e.g., "500 GB")
    public String formattedAvailableSpace() {
        return formatBytes(availableSpace);
    }

    /// Returns a display label indicating if this is the root filesystem.
    ///
    /// @return "Yes (root)" if this is root, "Yes" otherwise
    public String writableLabel() {
        return isRoot ? "Yes (root)" : "Yes";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MountPointInfo that = (MountPointInfo) o;
        return totalSpace == that.totalSpace &&
               availableSpace == that.availableSpace &&
               isRoot == that.isRoot &&
               Objects.equals(mountPoint, that.mountPoint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mountPoint, totalSpace, availableSpace, isRoot);
    }

    @Override
    public String toString() {
        return String.format("MountPointInfo[%s, available=%s, total=%s, isRoot=%s]",
            mountPoint, formattedAvailableSpace(), formattedTotalSpace(), isRoot);
    }
}
