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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/// An in-memory implementation of EventSink.
///
/// This class implements the EventSink interface and stores events in memory.
/// It has a configurable limit of events (default 10000), and events are reordered
/// according to timestamp lazily when they are accessed. It provides an atomically
/// safe consume and purge method for concurrent reading of events without losing any.
/// If the event limit is reached, extra events are logged to System.err.
public class MemoryEventSink implements EventSink {
    private static final int DEFAULT_EVENT_LIMIT = 10000;
    private static final DateTimeFormatter ISO_8601_FORMATTER = DateTimeFormatter.ISO_INSTANT;

    private final int eventLimit;
    private final Instant creationTime;
    private final CopyOnWriteArrayList<LogEvent> events;
    private final AtomicBoolean needsSorting;
    private final ReentrantLock sortLock;

    /// Represents a log event stored in memory
    public static class LogEvent {
        private final Instant timestamp;
        private final String formattedMessage;
        private final EventType.Level level;
        private final String rawMessage;
        private final Map<String, Object> params;
        private final EventType eventType;

        /// Creates a new LogEvent instance.
        /// @param timestamp The time when the event occurred
        /// @param level The severity level of the event
        /// @param rawMessage The original unformatted message
        /// @param formattedMessage The formatted message with timestamp and level
        /// @param params Additional parameters associated with the event
        /// @param eventType The type of the event
        public LogEvent(Instant timestamp, EventType.Level level, String rawMessage, 
                        String formattedMessage, Map<String, Object> params, EventType eventType) {
            this.timestamp = timestamp;
            this.level = level;
            this.rawMessage = rawMessage;
            this.formattedMessage = formattedMessage;
            this.params = params != null ? Map.copyOf(params) : null;
            this.eventType = eventType;
        }

        /// Gets the timestamp when the event occurred.
        /// @return The event timestamp
        public Instant getTimestamp() {
            return timestamp;
        }

        /// Gets the severity level of the event.
        /// @return The event level
        public EventType.Level getLevel() {
            return level;
        }

        /// Gets the original unformatted message.
        /// @return The raw message
        public String getRawMessage() {
            return rawMessage;
        }

        /// Gets the formatted message with timestamp and level.
        /// @return The formatted message
        public String getFormattedMessage() {
            return formattedMessage;
        }

        /// Gets the additional parameters associated with the event.
        /// @return The event parameters, or null if none
        public Map<String, Object> getParams() {
            return params;
        }

        /// Gets the type of the event.
        /// @return The event type
        public EventType getEventType() {
            return eventType;
        }
    }

    /// Construct a MemoryEventSink with the default event limit (10000).
    public MemoryEventSink() {
        this(DEFAULT_EVENT_LIMIT);
    }

    /// Construct a MemoryEventSink with the specified event limit.
    ///
    /// @param eventLimit The maximum number of events to store in memory
    public MemoryEventSink(int eventLimit) {
        this.eventLimit = eventLimit;
        this.creationTime = Instant.now();
        this.events = new CopyOnWriteArrayList<>();
        this.needsSorting = new AtomicBoolean(false);
        this.sortLock = new ReentrantLock();

        // Add creation time event
        addEvent(
            Instant.now(),
            EventType.Level.INFO,
            "FILE_INIT",
            String.format("%s I %-10s", ISO_8601_FORMATTER.format(creationTime), "FILE_INIT"),
            null,
            null
        );
    }

    @Override
    public void debug(String format, Object... args) {
        writeLog(EventType.Level.DEBUG, format, args);
    }

    @Override
    public void info(String format, Object... args) {
        writeLog(EventType.Level.INFO, format, args);
    }

    @Override
    public void warn(String format, Object... args) {
        writeLog(EventType.Level.WARN, format, args);
    }

    @Override
    public void warn(String message, Throwable t) {
        writeLogWithThrowable(EventType.Level.WARN, message, t);
    }

    @Override
    public void error(String format, Object... args) {
        writeLog(EventType.Level.ERROR, format, args);
    }

    @Override
    public void error(String message, Throwable t) {
        writeLogWithThrowable(EventType.Level.ERROR, message, t);
    }

    @Override
    public void trace(String format, Object... args) {
        writeLog(EventType.Level.TRACE, format, args);
    }

    @Override
    public void log(EventType event, Map<String, Object> params) {
        validateRequiredParams(event, params);
        writeEventLog(event, params);
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

            // Identify tuples based on parameter names
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

            // Handle regular parameters that aren't part of tuples
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
    private void writeEventLog(EventType event, Map<String, Object> params) {
        try {
            String timestamp = formatTimestamp();
            String message = formatEventMessage(event, params);
            // Format: timestamp severity event_message
            String formattedMessage = String.format("%s %c %s", timestamp, event.getLevelSymbol(), message);

            addEvent(Instant.now(), event.getLevel(), message, formattedMessage, params, event);
        } catch (Exception e) {
            System.err.println("Error adding event to memory sink: " + e.getMessage());
        }
    }

    private void writeLog(EventType.Level level, String format, Object... args) {
        try {
            String timestamp = formatTimestamp();
            String message = String.format(format.replace("{}", "%s"), args);
            char levelChar = getLevelChar(level);
            // Format: timestamp severity message
            String formattedMessage = String.format("%s %c %s", timestamp, levelChar, message);

            addEvent(Instant.now(), level, message, formattedMessage, null, null);
        } catch (Exception e) {
            System.err.println("Error adding log to memory sink: " + e.getMessage());
        }
    }

    private void writeLogWithThrowable(EventType.Level level, String message, Throwable t) {
        try {
            String timestamp = formatTimestamp();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            t.printStackTrace(pw);
            char levelChar = getLevelChar(level);
            // Format: timestamp severity message\nstacktrace
            String formattedMessage = String.format("%s %c %s\n%s", timestamp, levelChar, message, sw);

            addEvent(Instant.now(), level, message, formattedMessage, null, null);
        } catch (Exception e) {
            System.err.println("Error adding log with throwable to memory sink: " + e.getMessage());
        }
    }

    /// Add an event to the memory store, handling overflow if necessary
    private void addEvent(Instant timestamp, EventType.Level level, String rawMessage, 
                         String formattedMessage, Map<String, Object> params, EventType eventType) {
        LogEvent event = new LogEvent(timestamp, level, rawMessage, formattedMessage, params, eventType);

        if (events.size() >= eventLimit) {
            // If we've reached the limit, log to System.err and don't add to the list
            System.err.println(formattedMessage);
            return;
        }

        events.add(event);
        needsSorting.set(true);
    }

    /// Formats the time elapsed since creation.
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
            return String.format("%8s", String.format("%02d%02d%02d.%06d", 
                hoursPart, minutesPart, secondsPart, millisPart));
        }
    }

    /// Get the single character representation of a log level
    ///
    /// @param level The log level
    /// @return The single character representation of the level
    private char getLevelChar(EventType.Level level) {
        return switch (level) {
            case TRACE -> 'T';
            case DEBUG -> 'D';
            case INFO -> 'I';
            case WARN -> 'W';
            case ERROR -> 'E';
        };
    }

    /// Get all events, sorted by timestamp.
    ///
    /// @return A list of all events
    public List<LogEvent> getEvents() {
        ensureSorted();
        return new ArrayList<>(events);
    }

    /// Get the number of events currently stored.
    ///
    /// @return The number of events
    public int getEventCount() {
        return events.size();
    }

    /// Get the maximum number of events that can be stored.
    ///
    /// @return The event limit
    public int getEventLimit() {
        return eventLimit;
    }

    /// Consume all events and clear the buffer atomically.
    ///
    /// This method is thread-safe and allows for concurrent reading of events
    /// without losing any.
    ///
    /// @return A list of all events that were in the buffer
    public List<LogEvent> consumeAndPurge() {
        ensureSorted();
        List<LogEvent> result = new ArrayList<>(events);
        events.clear();
        return result;
    }

    /// Clear all events from the buffer.
    public void clear() {
        events.clear();
    }

    /// Get all events of a specific type.
    ///
    /// @param eventType The event type to filter by
    /// @return A list of parameter maps for events of the specified type
    public List<Map<String, Object>> getEventsByType(EventType eventType) {
        ensureSorted();
        return events.stream()
            .filter(e -> e.getEventType() == eventType)
            .map(LogEvent::getParams)
            .toList();
    }

    /// Add a custom event with the specified parameters.
    /// This is useful for testing when you need to manually add events.
    ///
    /// @param eventType The event type
    /// @param params The event parameters
    public void addCustomEvent(EventType eventType, Map<String, Object> params) {
        validateRequiredParams(eventType, params);
        addEvent(Instant.now(), eventType.getLevel(), "", "", params, eventType);
    }

    /// Ensure events are sorted by timestamp.
    ///
    /// This is done lazily, only when events are accessed.
    private void ensureSorted() {
        if (needsSorting.get() && sortLock.tryLock()) {
            try {
                if (needsSorting.get()) {  // Double-check after acquiring lock
                    List<LogEvent> sortedEvents = new ArrayList<>(events);
                    sortedEvents.sort(Comparator.comparing(LogEvent::getTimestamp));

                    events.clear();
                    events.addAll(sortedEvents);

                    needsSorting.set(false);
                }
            } finally {
                sortLock.unlock();
            }
        }
    }
}
