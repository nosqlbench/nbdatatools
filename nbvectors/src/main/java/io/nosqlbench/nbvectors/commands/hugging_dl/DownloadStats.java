package io.nosqlbench.nbvectors.commands.hugging_dl;

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

public class DownloadStats {
    private final AtomicInteger completedFiles = new AtomicInteger(0);
    private final AtomicInteger totalFiles = new AtomicInteger(0);
    private final AtomicLong totalBytes = new AtomicLong(0);
    private final AtomicLong downloadedBytes = new AtomicLong(0);
    private final AtomicInteger skippedFiles = new AtomicInteger(0);
    private final AtomicInteger downloadedFiles = new AtomicInteger(0);
    private final AtomicInteger failedFiles = new AtomicInteger(0);

    public void incrementCompletedFiles() {
        completedFiles.incrementAndGet();
    }

    public void setTotalFiles(int total) {
        totalFiles.set(total);
    }

    public void addToTotalBytes(long bytes) {
        totalBytes.addAndGet(bytes);
    }

    public void addToDownloadedBytes(long bytes) {
        downloadedBytes.addAndGet(bytes);
    }

    public int getCompletedFiles() {
        return completedFiles.get();
    }

    public int getTotalFiles() {
        return totalFiles.get();
    }

    public long getTotalBytes() {
        return totalBytes.get();
    }

    public long getDownloadedBytes() {
        return downloadedBytes.get();
    }

    public void incrementSkippedFiles() {
        skippedFiles.incrementAndGet();
        completedFiles.incrementAndGet();
    }

    public void incrementDownloadedFiles() {
        downloadedFiles.incrementAndGet();
        completedFiles.incrementAndGet();
    }

    public void incrementFailedFiles() {
        failedFiles.incrementAndGet();
    }

    public int getSkippedFiles() {
        return skippedFiles.get();
    }

    public int getDownloadedFiles() {
        return downloadedFiles.get();
    }

    public int getFailedFiles() {
        return failedFiles.get();
    }
}
