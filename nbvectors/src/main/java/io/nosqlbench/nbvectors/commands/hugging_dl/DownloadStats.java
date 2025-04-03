package io.nosqlbench.nbvectors.commands.hugging_dl;

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