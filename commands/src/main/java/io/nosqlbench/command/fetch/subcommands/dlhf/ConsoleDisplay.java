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


import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.InfoCmp;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/// a console display for the huggingface dl command
public class ConsoleDisplay implements AutoCloseable {
    private final Terminal terminal;
    private final Object terminalLock = new Object();
    private final Queue<LogEntry> logMessages = new ConcurrentLinkedQueue<>();
    private final Queue<String> preDownloadInfo = new ConcurrentLinkedQueue<>();
    private static final int MAX_LOG_LINES = 10;
    private final String dsName;
    private final Path target;
    private String currentStatus = "Initializing...";
    private String currentAction = "";
    private volatile boolean running = true;
    private Thread progressThread;
    private final ConcurrentHashMap<Path, FileProgress> fileProgresses;
    private final DownloadStats stats;
    private long eventCounter = 0;

    private static class LogEntry {
        final long number;
        final String message;

        LogEntry(long number, String message) {
            this.number = number;
            this.message = message;
        }
    }

    /// create a new console display
    /// @param dsName the name of the dataset to download
    /// @param target the target directory to download to
    /// @param stats the download stats
    /// @param fileProgresses the file progresses
    /// @throws IOException if the terminal cannot be created
    public ConsoleDisplay(String dsName, Path target,
        ConcurrentHashMap<Path, FileProgress> fileProgresses, DownloadStats stats) throws IOException {
        this.dsName = dsName;
        this.target = target;
        this.fileProgresses = fileProgresses;
        this.stats = stats;
        this.terminal = TerminalBuilder.builder()
            .system(true)
            .build();
        log("Initializing display for dataset '%s' to path '%s'", dsName, target);
    }

    /// set the current status
    /// @param status the current status
    public void setStatus(String status) {
        this.currentStatus = status;
    }

    /// set the current action
    /// @param action the current action
    public void setAction(String action) {
        this.currentAction = action;
    }

    /// clear the pre-download info
    /// @see #addPreDownloadInfo
    public void clearPreDownloadInfo() {
        preDownloadInfo.clear();
    }

    /// add pre-download info
    /// @param info the info to add
    public void addPreDownloadInfo(String info) {
        preDownloadInfo.add(info);
    }

    /// log a message
    /// @param format the format string
    /// @param args the arguments to the format string
    public void log(String format, Object... args) {
        String message = String.format("[%s] %s",
            LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")),
            String.format(format, args));

        // Keep exactly 3 messages in the queue
        while (logMessages.size() >= 3) {
            logMessages.poll();
        }
        logMessages.add(new LogEntry(++eventCounter, message));
    }

    /// update the display
    void updateDisplay() {
        synchronized (terminalLock) {
            int width = terminal.getWidth();
            int height = terminal.getHeight();

            // Clear screen and move cursor to top
            terminal.writer().print("\033[2J\033[H");

            if (stats.getTotalFiles() == 0) {
                displayPreDownloadInfo(width);
            } else {
                displayDownloadProgress(width, height);
            }

            terminal.writer().flush();
        }
    }

    private void displayPreDownloadInfo(int width) {
        terminal.writer().println("Dataset Download Preparation");
        terminal.writer().println("─".repeat(width));
        terminal.writer().println("Dataset: " + dsName);
        terminal.writer().println("Target: " + target.toAbsolutePath());
        terminal.writer().println("Status: " + currentStatus);
        if (!currentAction.isEmpty()) {
            terminal.writer().println("Action: " + currentAction);
        }
        terminal.writer().println();

        if (!preDownloadInfo.isEmpty()) {
            terminal.writer().println("Dataset Information:");
            terminal.writer().println("─".repeat(width));
            preDownloadInfo.forEach(info -> {
                if (info.startsWith("\n")) {
                    terminal.writer().println();
                    terminal.writer().println(info.substring(1));
                } else {
                    terminal.writer().println(info);
                }
            });
        }

        displayLogMessages(width);
    }

    private void displayDownloadProgress(int width, int height) {
        // Reserve space for header, overall progress, and log messages
        int reservedLines = 8; // Header(1) + Progress(2) + Separator(1) + Log header(1) + Log lines(3)
        int maxFileProgressLines = Math.max(1, height - reservedLines);

        // Overall progress
        float percentage = (float) stats.getCompletedFiles() / stats.getTotalFiles() * 100;
        long downloadedMB = stats.getDownloadedBytes() / (1024 * 1024);
        long totalMB = stats.getTotalBytes() / (1024 * 1024);

        terminal.writer().println(String.format("Dataset: %s", dsName));
        terminal.writer().println(String.format("Overall Progress: %d/%d files (%3.0f%%) - %d/%d MB",
            stats.getCompletedFiles(), stats.getTotalFiles(), percentage, downloadedMB, totalMB));
        terminal.writer().println("─".repeat(width));

        // File progress bars - show only active or recent files
        List<FileProgress> activeFiles = fileProgresses.values().stream()
            .filter(p -> !p.isCompleted() || p.isFailed())
            .limit(maxFileProgressLines)
            .collect(java.util.stream.Collectors.toList());

        if (activeFiles.isEmpty() && !fileProgresses.isEmpty()) {
            // Show last completed file if no active files
            activeFiles = fileProgresses.values().stream()
                .limit(1)
                .collect(java.util.stream.Collectors.toList());
        }

        activeFiles.forEach(progress -> {
            terminal.writer().println(progress.getProgressBar(width));
        });

        displayLogMessages(width);
    }

    private void displayLogMessages(int width) {
        terminal.writer().println("─".repeat(width));
        terminal.writer().println("Recent Events:");

        // Create an array of fixed size for log lines
        LogEntry[] logLines = new LogEntry[3];
        Iterator<LogEntry> logIter = logMessages.iterator();

        // Fill the array with the most recent messages
        for (int i = 0; i < 3 && logIter.hasNext(); i++) {
            logLines[i] = logIter.next();
        }

        // Display each line, using empty string if no message exists
        for (int i = 0; i < 3; i++) {
            if (logLines[i] != null) {
                terminal.writer().printf("#%-4d %s%n", logLines[i].number, logLines[i].message);
            } else {
                terminal.writer().println();
            }
        }

        // Add extra blank line after the events
        terminal.writer().println();
    }

    /// startInclusive the progress thread
    /// @see #close
    public void startProgressThread() {
        progressThread = new Thread(() -> {
            terminal.puts(InfoCmp.Capability.cursor_invisible);
            try {
                while (running) {
                    updateDisplay();
                    Thread.sleep(100);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        progressThread.start();
    }

    /// close the display
    /// @see #startProgressThread
    @Override
    public void close() {
        running = false;
        if (progressThread != null) {
            progressThread.interrupt();
            try {
                progressThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            // Make the cursor visible again before closing
            terminal.puts(InfoCmp.Capability.cursor_visible);
            terminal.writer().flush();
            terminal.close();
        } catch (IOException e) {
            // Log or handle terminal closing error
        }
    }
}