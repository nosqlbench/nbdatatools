package io.nosqlbench.vectordata.downloader;

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


import io.nosqlbench.vectordata.discovery.TestDataGroup;

import java.nio.file.Path;
import java.util.Optional;

/// Represents the result of a dataset download operation.
///
/// This class contains information about the download result, including the path,
/// number of bytes downloaded, status, and any errors that occurred.
///
/// @param path The path where the file was downloaded
/// @param status The status of the download (DOWNLOADED, SKIPPED, or FAILED)
/// @param bytes The number of bytes downloaded
/// @param error Any exception that occurred during the download, or null if successful
public record DownloadResult(
    Path path,
    DownloadStatus status,
    long bytes,
    Exception error
) {
    /// Creates a successful download result.
    ///
    /// @param path The path where the file was downloaded
    /// @param bytes The number of bytes downloaded
    /// @return A new DownloadResult with DOWNLOADED status
    public static DownloadResult downloaded(Path path, long bytes) {
        return new DownloadResult(path, DownloadStatus.DOWNLOADED, bytes, null);
    }

    /// Creates a skipped download result.
    ///
    /// @param path The path that was skipped
    /// @param bytes The number of bytes in the existing file
    /// @return A new DownloadResult with SKIPPED status
    public static DownloadResult skipped(Path path, long bytes) {
        return new DownloadResult(path, DownloadStatus.SKIPPED, bytes, null);
    }

    /// Creates a failed download result.
    ///
    /// @param path The path where the download was attempted
    /// @param error The exception that caused the failure
    /// @return A new DownloadResult with FAILED status
    public static DownloadResult failed(Path path, Exception error) {
        return new DownloadResult(path, DownloadStatus.FAILED, 0, error);
    }

    /// Checks if the download was successful.
    ///
    /// @return true if the download was successful, false otherwise
    public boolean isSuccess() {
        return status == DownloadStatus.DOWNLOADED || status == DownloadStatus.SKIPPED;
    }

    /// Gets the associated data group, if available.
    ///
    /// @return An Optional containing the data group, or empty if not available
    public Optional<TestDataGroup> getDataGroup() {
        if (isSuccess()) {
            return Optional.of(new TestDataGroup(path));
        } else {
            return Optional.empty();
        }

    }
    /// Gets the associated data group, throwing an exception if not available.
    ///
    /// @return The data group
    /// @throws IllegalStateException if the data group is not available
    public TestDataGroup getRequiredDataGroup() {
        return getDataGroup().orElseThrow(() -> new RuntimeException("download of '" + path +
                                                                     "' failed: " + error.getMessage()));
    }
}
