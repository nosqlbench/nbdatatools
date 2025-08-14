package io.nosqlbench.command.merkle.console;

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
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Console display for Merkle tree operations using terminal UI.
 * 
 * Provides real-time progress updates, status information, and log messages
 * with interactive terminal interface including progress bars and event logs.
 */
public class MerkleConsoleDisplay implements AutoCloseable {
    private final Terminal terminal;
    private final Object terminalLock = new Object();
    private final Queue<LogEntry> logMessages = new ConcurrentLinkedQueue<>();
    private final Path sourceFile;
    private String currentStatus = "Initializing...";
    private String currentAction = "";
    private volatile boolean running = true;
    private Thread progressThread;
    private long eventCounter = 0;
    private long bytesProcessed = 0;
    private long totalBytes = 0;
    private int sectionsCompleted = 0;
    private int totalSections = 0;
    private long sessionBlocksProcessed = 0;
    private long sessionTotalBlocks = 0;
    private boolean showSessionProgress = false;

    private static class LogEntry {
        final long number;
        final String message;

        LogEntry(long number, String message) {
            this.number = number;
            this.message = message;
        }
    }

    /**
     * Creates a new console display for the specified source file.
     * 
     * @param sourceFile The file being processed
     * @throws IOException If terminal initialization fails
     */
    public MerkleConsoleDisplay(Path sourceFile) throws IOException {
        this.sourceFile = sourceFile;
        this.terminal = TerminalBuilder.builder()
            .system(true)
            .build();
        log("Initializing Merkle tree computation for '%s'", sourceFile);
    }

    /**
     * Sets the current status message.
     * 
     * @param status The status to display
     */
    public void setStatus(String status) {
        this.currentStatus = status;
    }

    /**
     * Sets the current action being performed.
     * 
     * @param action The action description
     */
    public void setAction(String action) {
        this.currentAction = action;
    }

    /**
     * Updates the progress information for the current file.
     * 
     * @param bytesProcessed Number of bytes processed
     * @param totalBytes Total bytes to process
     * @param sectionsCompleted Number of leaf chunks completed
     * @param totalSections Total number of leaf chunks
     */
    public void updateProgress(long bytesProcessed, long totalBytes, int sectionsCompleted, int totalSections) {
        this.bytesProcessed = bytesProcessed;
        this.totalBytes = totalBytes;
        this.sectionsCompleted = sectionsCompleted;
        this.totalSections = totalSections;
    }

    /**
     * Updates the session-wide progress information.
     * This tracks progress across all files in the current session.
     *
     * @param blocksProcessed The total number of leaf chunks processed across all files
     * @param totalBlocks The total number of leaf chunks across all files
     */
    public void updateSessionProgress(long blocksProcessed, long totalBlocks) {
        this.sessionBlocksProcessed = blocksProcessed;
        this.sessionTotalBlocks = totalBlocks;
        this.showSessionProgress = true;
    }

    /**
     * Logs a formatted message to the display.
     * 
     * @param format The format string
     * @param args Format arguments
     */
    public void log(String format, Object... args) {
        String message = String.format("[%s] %s",
            LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")),
            String.format(format, args));

        while (logMessages.size() >= 6) { // Increased from 3 to 6 to show more log entries
            logMessages.poll();
        }
        logMessages.add(new LogEntry(++eventCounter, message));
    }

    void updateDisplay() {
        synchronized (terminalLock) {
            int width = terminal.getWidth();
            int height = terminal.getHeight();

            // Clear screen and move cursor to top
            terminal.writer().print("\033[2J\033[H");

            displayProgress(width);

            terminal.writer().flush();
        }
    }

    private void displayProgress(int width) {
        terminal.writer().println("Merkle Tree Computation");
        terminal.writer().println("─".repeat(width));
        terminal.writer().println("Source: " + sourceFile.toAbsolutePath());
        terminal.writer().println("Status: " + currentStatus);
        if (!currentAction.isEmpty()) {
            terminal.writer().println("Action: " + currentAction);
        }
        terminal.writer().println();

        // Display progress bar if we have total bytes
        if (totalBytes > 0) {
            float percentage = (float) bytesProcessed / totalBytes * 100;
            String processedSize = formatSize(bytesProcessed);
            String totalSize = formatSize(totalBytes);

            terminal.writer().println(String.format("File Progress: %d/%d leaf chunks (%3.0f%%) - %s/%s",
                sectionsCompleted, totalSections, percentage, processedSize, totalSize));

            // Progress bar for file
            int barWidth = width - 10;
            int completed = (int) ((barWidth * bytesProcessed) / totalBytes);
            terminal.writer().print("[");
            for (int i = 0; i < barWidth; i++) {
                if (i < completed) {
                    terminal.writer().print("=");
                } else if (i == completed) {
                    terminal.writer().print(">");
                } else {
                    terminal.writer().print(" ");
                }
            }
            terminal.writer().println("]");
        }

        // Display session-wide progress if available
        if (showSessionProgress && sessionTotalBlocks > 0) {
            terminal.writer().println();
            float sessionPercentage = (float) sessionBlocksProcessed / sessionTotalBlocks * 100;

            terminal.writer().println(String.format("Session Progress: %d/%d leaf chunks (%3.0f%%)",
                sessionBlocksProcessed, sessionTotalBlocks, sessionPercentage));

            // Progress bar for session
            int barWidth = width - 10;
            int completed = (int) ((barWidth * sessionBlocksProcessed) / sessionTotalBlocks);
            terminal.writer().print("[");
            for (int i = 0; i < barWidth; i++) {
                if (i < completed) {
                    terminal.writer().print("#");  // Use different character for session progress
                } else if (i == completed) {
                    terminal.writer().print(">");
                } else {
                    terminal.writer().print(" ");
                }
            }
            terminal.writer().println("]");
        }

        displayLogMessages(width);
    }

    private void displayLogMessages(int width) {
        terminal.writer().println("─".repeat(width));
        terminal.writer().println("Recent Events:");

        LogEntry[] logLines = new LogEntry[6]; // Increased from 3 to 6 to show more log entries
        Iterator<LogEntry> logIter = logMessages.iterator();

        for (int i = 0; i < 6 && logIter.hasNext(); i++) { // Increased from 3 to 6
            logLines[i] = logIter.next();
        }

        for (int i = 0; i < 6; i++) { // Increased from 3 to 6
            if (logLines[i] != null) {
                terminal.writer().printf("#%-4d %s%n", logLines[i].number, logLines[i].message);
            } else {
                terminal.writer().println();
            }
        }

        terminal.writer().println();
    }

    /// Formats a size in bytes to a human-readable string with appropriate units.
    ///
    /// @param bytes The size in bytes
    /// @return A formatted string with appropriate units
    private String formatSize(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.2f KB", bytes / 1024.0);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.2f MB", bytes / (1024.0 * 1024.0));
        } else {
            return String.format("%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0));
        }
    }

    /**
     * Starts the background thread that updates the display periodically.
     */
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
            // Ignore closing errors
        }
    }
}
