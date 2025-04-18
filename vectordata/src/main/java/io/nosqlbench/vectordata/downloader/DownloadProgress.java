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


import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/// Represents the progress of a dataset download operation.
///
/// This class provides methods to track the progress of a download, check if it's complete,
/// and retrieve the result when finished.
///
/// @param targetPath The path where the file is being downloaded
/// @param totalBytes The total number of bytes to download
/// @param currentBytes The current number of bytes downloaded
/// @param future The CompletableFuture that will complete when the download is done
public record DownloadProgress(
    Path targetPath,
    long totalBytes,
    AtomicLong currentBytes,
    CompletableFuture<DownloadResult> future
) {
    /// Checks if the download is complete.
    ///
    /// @return true if the download is complete, false otherwise
    public boolean isDone() {
        return future.isDone();
    }

    /// Gets the current progress of the download.
    ///
    /// @return A value between 0.0 and 1.0 indicating the download progress
    public double getProgress() {
        return totalBytes > 0 ? (double) currentBytes.get() / totalBytes : 0.0;
    }

    /// Gets the download result, blocking until the download is complete.
    ///
    /// @return The download result
    /// @throws InterruptedException If the current thread is interrupted while waiting
    /// @throws ExecutionException If the download fails
    public DownloadResult get() throws InterruptedException, ExecutionException {
        return future.get();
    }

    /// Gets the download result, blocking until the download is complete or the timeout expires.
    ///
    /// @param timeout The maximum time to wait
    /// @param unit The time unit of the timeout argument
    /// @return The download result
    /// @throws InterruptedException If the current thread is interrupted while waiting
    /// @throws ExecutionException If the download fails
    /// @throws TimeoutException If the wait timed out
    public DownloadResult get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
        return future.get(timeout, unit);
    }

    /// Polls for the download result, blocking for the specified timeout.
    /// @param timeout The maximum time to wait.
    /// @param unit    The time unit of the timeout argument.
    /// @return The DownloadResult if it becomes available within the timeout, or null if the timeout expires.
    /// @throws InterruptedException If the current thread is interrupted while waiting.
    public DownloadResult poll(long timeout, TimeUnit unit) throws InterruptedException {
        try {
            return future.get(timeout, unit);
        } catch (TimeoutException | ExecutionException e) {
            return null;
        }
    }
}
