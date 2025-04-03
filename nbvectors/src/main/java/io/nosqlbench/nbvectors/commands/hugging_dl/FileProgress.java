package io.nosqlbench.nbvectors.commands.hugging_dl;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

public class FileProgress {
    final Path filename;
    final long totalSize;
    final AtomicLong currentBytes = new AtomicLong(0);
    volatile boolean completed = false;
    volatile boolean failed = false;
    volatile String error = null;

    public FileProgress(Path filename, long totalSize) {
        this.filename = filename;
        this.totalSize = totalSize;
    }

    public Path getFilename() {
        return filename;
    }

    public long getTotalSize() {
        return totalSize;
    }

    public long getCurrentBytes() {
        return currentBytes.get();
    }

    public boolean isCompleted() {
        return completed;
    }

    public boolean isFailed() {
        return failed;
    }

    public String getError() {
        return error;
    }

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