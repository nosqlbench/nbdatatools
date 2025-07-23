package io.nosqlbench.vectordata.events;

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


import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/// A file-based implementation of EventSink.
///
/// This class implements the EventSink interface and logs events to a file.
/// It's useful for persistent logging of download progress and merkle tree operations.
/// The first line of the log file contains the creation time in ISO 8601 format.
/// Subsequent log entries use a timestamp relative to this creation time.
/// This implementation maintains a reorder buffer to sort incoming messages by timestamp.
/// Events older than 500ms are flushed, as well as events at the tail end of the buffer
/// if the buffer would exceed 100 events.
public class LogFileEventSink implements EventSink, Closeable {
    private final Path logFilePath;
    private final BufferedWriter writer;
    private final Instant creationTime;
    private boolean closed = false;
    private static final DateTimeFormatter ISO_8601_FORMATTER = DateTimeFormatter.ISO_INSTANT;
    private static final String FILE_INIT_EVENT = "FILE_INIT";

    // Reordering buffer configuration
    private static final int DEFAULT_BUFFER_SIZE = 100;
    private static final long MAX_EVENT_AGE_MS = 500;

    // Executor for periodic buffer flushing
    private final ScheduledExecutorService scheduler;

    // Buffer for reordering events
    private final PriorityQueue<BufferedLogEvent> eventBuffer;

    /// Represents a log event in the buffer
    private static class BufferedLogEvent {
        private final Instant timestamp;
        private final String formattedMessage;

        public BufferedLogEvent(Instant timestamp, String formattedMessage) {
            this.timestamp = timestamp;
            this.formattedMessage = formattedMessage;
        }

        public Instant getTimestamp() {
            return timestamp;
        }

        public String getFormattedMessage() {
            return formattedMessage;
        }
    }

    /// Construct a LogFileEventSink with the specified log file path.
    ///
    /// @param logFilePath The path where log events will be written
    public LogFileEventSink(Path logFilePath) {
        this.logFilePath = logFilePath;
        this.creationTime = Instant.now();

        // Initialize the event buffer with a comparator that sorts by timestamp
        this.eventBuffer = new PriorityQueue<>(
            Comparator.comparing(BufferedLogEvent::getTimestamp)
        );

        // Initialize the scheduler for periodic buffer flushing
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "LogFileEventSink-Flusher");
            t.setDaemon(true);
            return t;
        });

        try {

            Files.createDirectories(logFilePath.getParent());

            this.writer = new BufferedWriter(new FileWriter(logFilePath.toFile(), true));
            // Write the creation time as the first line in ISO 8601 format with a special event type
            writer.write(String.format("%s I %-10s%n", 
                ISO_8601_FORMATTER.format(creationTime), 
                FILE_INIT_EVENT));
            writer.flush();

            // Schedule periodic buffer flushing (every 100ms)
            scheduler.scheduleAtFixedRate(this::flushBuffer, 100, 100, TimeUnit.MILLISECONDS);

            // Register a shutdown hook to ensure all events are flushed when the JVM shuts down
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    if (!closed) {
                        closeOnShutdown();
                    }
                } catch (Exception e) {
                    System.err.println("Error during shutdown hook execution: " + e.getMessage());
                }
            }, "LogFileEventSink-ShutdownHook"));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create log file: " + logFilePath, e);
        }
    }

    /// Get the path to the log file.
    ///
    /// @return The log file path
    public Path getLogFilePath() {
        return logFilePath;
    }

    @Override
    public void debug(String format, Object... args) {
        writeLog("DEBUG", format, args);
    }

    @Override
    public void info(String format, Object... args) {
        writeLog("INFO", format, args);
    }

    @Override
    public void warn(String format, Object... args) {
        writeLog("WARN", format, args);
    }

    @Override
    public void warn(String message, Throwable t) {
        writeLogWithThrowable("WARN", message, t);
    }

    @Override
    public void error(String format, Object... args) {
        writeLog("ERROR", format, args);
    }

    @Override
    public void error(String message, Throwable t) {
        writeLogWithThrowable("ERROR", message, t);
    }

    @Override
    public void trace(String format, Object... args) {
        writeLog("TRACE", format, args);
    }

    /// Format an event message with named parameters, ensuring the event name is right-justified.
    ///
    /// @param event The EventType enum value
    /// @param params Map of parameter names to values
    /// @return Formatted message string
    @Override
    public String formatEventMessage(EventType event, Map<String, Object> params) {
        StringBuilder sb = new StringBuilder();

        // Left-justify the event name to ensure it always takes the maximum size
        String eventName = event.name();
        sb.append(String.format("%-15s", eventName));

        if (params != null && !params.isEmpty()) {
            // Group parameters that constitute pairs
            Map<String, Object> processedParams = new java.util.LinkedHashMap<>();

            // Track which parameters are part of tuples
            Set<String> processedTupleParams = new java.util.HashSet<>();

            // First pass: identify common tuple patterns generically
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                // Check if this parameter is a tuple start parameter
                if (key.endsWith("Chunk") && key.startsWith("start") && params.containsKey("endChunk")) {
                    // This is a chunk tuple
                    String endKey = "endChunk";
                    Object endValue = params.get(endKey);
                    processedParams.put("chunk(start,end)", String.format("(%s,%s)", value, endValue));
                    processedTupleParams.add(key);
                    processedTupleParams.add(endKey);
                } else if (key.equals("start") && params.containsKey("end")) {
                    // This is a generic start/end tuple
                    String endKey = "end";
                    Object endValue = params.get(endKey);
                    processedParams.put("range(start,end)", String.format("(%s,%s)", value, endValue));
                    processedTupleParams.add(key);
                    processedTupleParams.add(endKey);
                }
            }

            // Second pass: handle regular parameters that aren't part of tuples
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                // Skip parameters that were already processed as part of tuples
                if (processedTupleParams.contains(key)) {
                    continue;
                }

                // Regular parameter
                processedParams.put(key, value);
            }

            // Format parameters
            for (Map.Entry<String, Object> entry : processedParams.entrySet()) {
                sb.append(" ").append(entry.getKey()).append(":").append(entry.getValue());
            }
        }
        return sb.toString();
    }

    /// Write a log entry for an EventType with parameters.
    ///
    /// @param event The EventType enum value
    /// @param params Map of parameter names to values
    private synchronized void writeEventLog(EventType event, Map<String, Object> params) {
        if (closed) return;

        try {
            String timestamp = formatTimestamp();
            String message = formatEventMessage(event, params);
            // Format: timestamp severity event_message
            // The event_message already includes the event name and parameters
            String formattedMessage = String.format("%s %c %s", timestamp, event.getLevelSymbol(), message);

            // Add the event to the buffer instead of writing directly
            eventBuffer.add(new BufferedLogEvent(Instant.now(), formattedMessage));
        } catch (Exception e) {
            System.err.println("Error buffering event log: " + e.getMessage());
        }
    }

    /// Override the default log method to use our custom writeEventLog method.
    ///
    /// @param event The EventType enum value
    /// @param params Map of parameter names to values
    @Override
    public void log(EventType event, Map<String, Object> params) {
        validateRequiredParams(event, params);
        writeEventLog(event, params);
    }

    /// Formats the time elapsed since log file creation.
    ///
    /// For timestamps less than one day, uses the format "HHMMSS.millis" (right justified).
    /// For timestamps more than a day, uses the ISO 8601 format.
    ///
    /// @return A formatted timestamp string
    private String formatTimestamp() {
        Duration elapsed = Duration.between(creationTime, Instant.now());
        long totalMillis = elapsed.toMillis();

        long millisPart = totalMillis % 1000;
        long totalSeconds = totalMillis / 1000;

        long secondsPart = totalSeconds % 60;
        long totalMinutes = totalSeconds / 60;

        long minutesPart = totalMinutes % 60;
        long totalHours = totalMinutes / 60;

        long hoursPart = totalHours % 24;
        long daysPart = totalHours / 24;

        if (daysPart > 0) {
            // More than a day, use ISO 8601 format
            return ISO_8601_FORMATTER.format(Instant.now());
        } else {
            // Less than a day, use HHMMSS.millis format (right justified)
            // Format is right justified and fills the left side with spaces to maintain alignment
            return String.format("%8s", String.format("%02d%02d%02d.%06d", 
                hoursPart, minutesPart, secondsPart, millisPart));
        }
    }

    private synchronized void writeLog(String level, String format, Object... args) {
        if (closed) return;

        try {
            String timestamp = formatTimestamp();
            String message = String.format(format.replace("{}", "%s"), args);
            char levelChar = getLevelChar(level);
            // Format: timestamp severity message
            String formattedMessage = String.format("%s %c %s", timestamp, levelChar, message);

            // Add the event to the buffer instead of writing directly
            eventBuffer.add(new BufferedLogEvent(Instant.now(), formattedMessage));
        } catch (Exception e) {
            System.err.println("Error buffering log: " + e.getMessage());
        }
    }

    private synchronized void writeLogWithThrowable(String level, String message, Throwable t) {
        if (closed) return;

        try {
            String timestamp = formatTimestamp();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            t.printStackTrace(pw);
            char levelChar = getLevelChar(level);
            // Format: timestamp severity message\nstacktrace
            String formattedMessage = String.format("%s %c %s\n%s", timestamp, levelChar, message, sw);

            // Add the event to the buffer instead of writing directly
            eventBuffer.add(new BufferedLogEvent(Instant.now(), formattedMessage));
        } catch (Exception e) {
            System.err.println("Error buffering log with throwable: " + e.getMessage());
        }
    }

    /// Get the single character representation of a log level
    ///
    /// @param level The log level as a string
    /// @return The single character representation of the level
    private char getLevelChar(String level) {
        return switch (level) {
            case "TRACE" -> 'T';
            case "DEBUG" -> 'D';
            case "INFO" -> 'I';
            case "WARN" -> 'W';
            case "ERROR" -> 'E';
            default -> '?';
        };
    }

    /// Flush the event buffer based on age and size criteria.
    /// Events older than MAX_EVENT_AGE_MS are flushed, as well as events at the tail
    /// end of the buffer if the buffer would exceed DEFAULT_BUFFER_SIZE.
    private synchronized void flushBuffer() {
        if (closed) return;

        Instant now = Instant.now();
        Instant cutoffTime = now.minusMillis(MAX_EVENT_AGE_MS);

        try {
            // First, flush events that are older than MAX_EVENT_AGE_MS
            while (!eventBuffer.isEmpty() && eventBuffer.peek().getTimestamp().isBefore(cutoffTime)) {
                BufferedLogEvent event = eventBuffer.poll();
                writer.write(event.getFormattedMessage());
                writer.newLine();
            }

            // Then, flush events at the tail end if the buffer would exceed DEFAULT_BUFFER_SIZE
            while (eventBuffer.size() > DEFAULT_BUFFER_SIZE) {
                BufferedLogEvent event = eventBuffer.poll();
                writer.write(event.getFormattedMessage());
                writer.newLine();
            }

            writer.flush();
        } catch (IOException e) {
            System.err.println("Error flushing buffer: " + e.getMessage());
        }
    }



    @Override
    public void close() throws IOException {
        if (!closed) {
            info("Closing log file");
            closed = true;

            // Flush any remaining events in the buffer
            flushAllEvents();

            // Shutdown the scheduler
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }

            writer.close();
        }
    }

    /// Method called by the shutdown hook to safely close the event sink during JVM shutdown
    ///
    /// This method is similar to close() but handles exceptions internally to ensure
    /// it doesn't throw during shutdown
    void closeOnShutdown() {
        if (!closed) {
            try {
                info("Closing log file during JVM shutdown");
                closed = true;

                // Flush any remaining events in the buffer
                flushAllEvents();

                // Shutdown the scheduler
                scheduler.shutdown();
                try {
                    if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                        scheduler.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    scheduler.shutdownNow();
                }

                writer.close();
            } catch (Exception e) {
                System.err.println("Error closing log file during shutdown: " + e.getMessage());
            }
        }
    }

    /// Flush all events in the buffer immediately
    private synchronized void flushAllEvents() {
        try {
            while (!eventBuffer.isEmpty()) {
                BufferedLogEvent event = eventBuffer.poll();
                writer.write(event.getFormattedMessage());
                writer.newLine();
            }
            writer.flush();
        } catch (IOException e) {
            System.err.println("Error flushing all events: " + e.getMessage());
        }
    }

}
