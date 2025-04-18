package io.nosqlbench.nbvectors.commands.merkle;

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

    private static class LogEntry {
        final long number;
        final String message;

        LogEntry(long number, String message) {
            this.number = number;
            this.message = message;
        }
    }

    public MerkleConsoleDisplay(Path sourceFile) throws IOException {
        this.sourceFile = sourceFile;
        this.terminal = TerminalBuilder.builder()
            .system(true)
            .build();
        log("Initializing Merkle tree computation for '%s'", sourceFile);
    }

    public void setStatus(String status) {
        this.currentStatus = status;
    }

    public void setAction(String action) {
        this.currentAction = action;
    }

    public void updateProgress(long bytesProcessed, long totalBytes, int sectionsCompleted, int totalSections) {
        this.bytesProcessed = bytesProcessed;
        this.totalBytes = totalBytes;
        this.sectionsCompleted = sectionsCompleted;
        this.totalSections = totalSections;
    }

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
            long processedGB = bytesProcessed / (1024 * 1024 * 1024);
            long totalGB = totalBytes / (1024 * 1024 * 1024);

            terminal.writer().println(String.format("Overall Progress: %d/%d sections (%3.0f%%) - %d/%d GB",
                sectionsCompleted, totalSections, percentage, processedGB, totalGB));

            // Progress bar
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
