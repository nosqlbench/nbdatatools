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


import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/// download stats
public class DownloadStats {
    private final AtomicInteger completedFiles = new AtomicInteger(0);
    private final AtomicInteger totalFiles = new AtomicInteger(0);
    private final AtomicLong totalBytes = new AtomicLong(0);
    private final AtomicLong downloadedBytes = new AtomicLong(0);
    private final AtomicInteger skippedFiles = new AtomicInteger(0);
    private final AtomicInteger downloadedFiles = new AtomicInteger(0);
    private final AtomicInteger failedFiles = new AtomicInteger(0);

    /// increment the number of completed files
    public void incrementCompletedFiles() {
        completedFiles.incrementAndGet();
    }

    /// set the total number of files
    /// @param total the total number of files
    public void setTotalFiles(int total) {
        totalFiles.set(total);
    }

    /// add to the total number of bytes
    /// @param bytes the number of bytes to add
    public void addToTotalBytes(long bytes) {
        totalBytes.addAndGet(bytes);
    }

    /// add to the number of downloaded bytes
    /// @param bytes the number of bytes to add
    public void addToDownloadedBytes(long bytes) {
        downloadedBytes.addAndGet(bytes);
    }

    /// get the number of completed files
    /// @return the number of completed files
    public int getCompletedFiles() {
        return completedFiles.get();
    }

    /// get the total number of files
    /// @return the total number of files
    public int getTotalFiles() {
        return totalFiles.get();
    }

    /// get the total number of bytes
    /// @return the total number of bytes
    public long getTotalBytes() {
        return totalBytes.get();
    }

    /// get the number of downloaded bytes
    /// @return the number of downloaded bytes
    public long getDownloadedBytes() {
        return downloadedBytes.get();
    }

    /// increment the number of skipped files
    public void incrementSkippedFiles() {
        skippedFiles.incrementAndGet();
        completedFiles.incrementAndGet();
    }

    /// increment the number of downloaded files
    public void incrementDownloadedFiles() {
        downloadedFiles.incrementAndGet();
        completedFiles.incrementAndGet();
    }

    /// increment the number of failed files
    public void incrementFailedFiles() {
        failedFiles.incrementAndGet();
    }

    /// get the number of skipped files
    /// @return the number of skipped files
    public int getSkippedFiles() {
        return skippedFiles.get();
    }

    /// get the number of downloaded files
    /// @return the number of downloaded files
    public int getDownloadedFiles() {
        return downloadedFiles.get();
    }

    /// get the number of failed files
    /// @return the number of failed files
    public int getFailedFiles() {
        return failedFiles.get();
    }
}