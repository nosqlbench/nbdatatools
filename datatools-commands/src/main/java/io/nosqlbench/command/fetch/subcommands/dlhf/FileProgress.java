package io.nosqlbench.command.fetch.subcommands.dlhf;

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
import java.util.concurrent.atomic.AtomicLong;

/// Tracks download progress for a single file from Hugging Face datasets.
/// Provides methods to update and display download progress.
public class FileProgress {
    final Path filename;
    final long totalSize;
    final AtomicLong currentBytes = new AtomicLong(0);
    volatile boolean completed = false;
    volatile boolean failed = false;
    volatile String error = null;

    /// Creates a new file progress tracker
    /// @param filename Path to the file being downloaded
    /// @param totalSize Total expected size in bytes
    public FileProgress(Path filename, long totalSize) {
        this.filename = filename;
        this.totalSize = totalSize;
    }

    /// get the filename
    /// @return the filename
    public Path getFilename() {
        return filename;
    }

    /// get the total size
    /// @return the total size
    public long getTotalSize() {
        return totalSize;
    }

    /// get current bytes
    /// @return the current bytes
    public long getCurrentBytes() {
        return currentBytes.get();
    }

    /// get completed status
    /// @return the completed status
    public boolean isCompleted() {
        return completed;
    }

    /// get failed status
    /// @return the failed status
    public boolean isFailed() {
        return failed;
    }

    /// get error message
    /// @return the error message
    public String getError() {
        return error;
    }

    /// Generates an ASCII progress bar showing download status
    /// @param width Total width of the progress bar in characters
    /// @return Formatted string containing filename, progress bar and percentage
    public String getProgressBar(int width) {
        int barWidth = width - filename.toString().length() - 15;
        long progress = currentBytes.get();
        float percentage = (float) progress / totalSize * 100;
        int completedWidth = (int) (barWidth * (progress / (float) totalSize));

        StringBuilder bar = new StringBuilder();
        bar.append(filename).append(" [");
        for (int i = 0; i < barWidth; i++) {
            if (i < completedWidth) {
                bar.append("=");
            } else if (i == completedWidth) {
                bar.append(">");
            } else {
                bar.append(" ");
            }
        }
        bar.append(String.format("] %3.0f%%", percentage));

        if (failed) {
            bar.append(" FAILED");
        } else if (completed) {
            bar.append(" DONE");
        }

        return bar.toString();
    }
}