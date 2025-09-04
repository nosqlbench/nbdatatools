package io.nosqlbench.vectordata.status;

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

import io.nosqlbench.vectordata.events.EventType;
import io.nosqlbench.vectordata.events.LogFileEventSink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/// Test class for LogFileEventSink
///
/// This class tests the reordering logic and shutdown hook functionality
/// of the LogFileEventSink class.
class LogFileEventSinkTest {

    /// Test enum implementing EventType for testing purposes
    private enum TestEventType implements EventType {
        TEST_EVENT_1(Level.INFO),
        TEST_EVENT_2(Level.DEBUG),
        TEST_EVENT_3(Level.WARN);

        private final Level level;

        TestEventType(Level level) {
            this.level = level;
        }

        @Override
        public Level getLevel() {
            return level;
        }

        @Override
        public Map<String, Class<?>> getRequiredParams() {
            Map<String, Class<?>> params = new HashMap<>();
            params.put("timestamp", Long.class);
            return params;
        }

        @Override
        public char getLevelSymbol() {
            switch (level) {
                case TRACE: return 'T';
                case DEBUG: return 'D';
                case INFO: return 'I';
                case WARN: return 'W';
                case ERROR: return 'E';
                default: throw new IllegalArgumentException("Unknown level: " + level);
            }
        }
    }

    /// Test that events are written in creation time order
    @Test
    void testEventReordering(@TempDir Path tempDir) throws IOException, InterruptedException {
        // Create a log file
        Path logFile = tempDir.resolve("test.log");

        // Create a LogFileEventSink
        try (LogFileEventSink sink = new LogFileEventSink(logFile)) {
            // Log events with different creation times
            // First event
            Map<String, Object> params1 = new HashMap<>();
            params1.put("timestamp", 1L);
            sink.log(TestEventType.TEST_EVENT_1, params1);

            // Wait a bit to ensure different creation times
            Thread.sleep(50);

            // Second event
            Map<String, Object> params2 = new HashMap<>();
            params2.put("timestamp", 2L);
            sink.log(TestEventType.TEST_EVENT_2, params2);

            // Wait a bit to ensure different creation times
            Thread.sleep(50);

            // Third event
            Map<String, Object> params3 = new HashMap<>();
            params3.put("timestamp", 3L);
            sink.log(TestEventType.TEST_EVENT_3, params3);

            // Wait for the events to be flushed (more than 500ms)
            Thread.sleep(600);
        }

        // Read the log file and verify the events are in creation time order
        List<String> lines = Files.readAllLines(logFile);

        // First line should be the file init event
        assertTrue(lines.get(0).contains("FILE_INIT"), "First line should be the file init event");

        // Extract timestamps from the remaining lines
        List<Long> timestamps = new ArrayList<>();
        Pattern pattern = Pattern.compile("timestamp:(\\d+)");

        for (int i = 1; i < lines.size(); i++) {
            Matcher matcher = pattern.matcher(lines.get(i));
            if (matcher.find()) {
                timestamps.add(Long.parseLong(matcher.group(1)));
            }
        }

        // Verify timestamps are in ascending order (1, 2, 3)
        assertEquals(3, timestamps.size(), "Should have 3 events with timestamps");
        for (int i = 0; i < timestamps.size() - 1; i++) {
            assertEquals(i + 1, timestamps.get(i).longValue(), 
                "Events should be in creation time order");
        }
    }

    /// Test that the shutdown hook flushes all events
    @Test
    void testShutdownHook(@TempDir Path tempDir) throws IOException, InterruptedException {
        // Create a log file
        Path logFile = tempDir.resolve("shutdown_test.log");

        // Create a LogFileEventSink without using try-with-resources
        LogFileEventSink sink = new LogFileEventSink(logFile);

        // Log some events
        for (int i = 0; i < 10; i++) {
            Map<String, Object> params = new HashMap<>();
            params.put("timestamp", (long) i);
            sink.log(TestEventType.TEST_EVENT_1, params);
        }

        // Simulate JVM shutdown by directly calling the closeOnShutdown method
        // This is done via reflection since it's a private method
        try {
            java.lang.reflect.Method closeOnShutdownMethod = 
                LogFileEventSink.class.getDeclaredMethod("closeOnShutdown");
            closeOnShutdownMethod.setAccessible(true);
            closeOnShutdownMethod.invoke(sink);
        } catch (Exception e) {
            fail("Failed to invoke closeOnShutdown method: " + e.getMessage());
        }

        // Read the log file and verify all events were flushed
        List<String> lines = Files.readAllLines(logFile);

        // First line should be the file init event
        assertTrue(lines.get(0).contains("FILE_INIT"), "First line should be the file init event");

        // There should be at least 10 event lines plus the init line and possibly a shutdown message
        assertTrue(lines.size() >= 11, 
            "Expected at least 11 lines in log file, but found " + lines.size());

        // Verify the shutdown message was logged
        boolean foundShutdownMessage = false;
        for (String line : lines) {
            if (line.contains("Closing log file during JVM shutdown")) {
                foundShutdownMessage = true;
                break;
            }
        }
        assertTrue(foundShutdownMessage, "Shutdown message should be logged");
    }

    /// Test that events older than 500ms are flushed
    @Test
    void testOldEventsFlushing(@TempDir Path tempDir) throws IOException, InterruptedException {
        // Create a log file
        Path logFile = tempDir.resolve("old_events_test.log");

        // Create a LogFileEventSink
        try (LogFileEventSink sink = new LogFileEventSink(logFile)) {
            // Log some events
            for (int i = 0; i < 5; i++) {
                Map<String, Object> params = new HashMap<>();
                params.put("timestamp", (long) i);
                sink.log(TestEventType.TEST_EVENT_1, params);
            }

            // Wait for the events to be flushed (more than 500ms)
            Thread.sleep(600);

            // Verify the events were flushed by checking the file size
            long sizeAfterFirstBatch = Files.size(logFile);
            assertTrue(sizeAfterFirstBatch > 0, "File should contain data after first batch");

            // Log more events
            for (int i = 5; i < 10; i++) {
                Map<String, Object> params = new HashMap<>();
                params.put("timestamp", (long) i);
                sink.log(TestEventType.TEST_EVENT_1, params);
            }

            // Wait for the events to be flushed
            Thread.sleep(600);

            // Verify the new events were flushed
            long sizeAfterSecondBatch = Files.size(logFile);
            assertTrue(sizeAfterSecondBatch > sizeAfterFirstBatch, 
                "File should contain more data after second batch");
        }
    }

    /// Test that events are flushed when the buffer exceeds the maximum size
    @Test
    void testBufferSizeLimit(@TempDir Path tempDir) throws IOException, InterruptedException {
        // Create a log file
        Path logFile = tempDir.resolve("buffer_size_test.log");

        // Create a LogFileEventSink
        try (LogFileEventSink sink = new LogFileEventSink(logFile)) {
            // Log more than DEFAULT_BUFFER_SIZE (100) events
            for (int i = 0; i < 150; i++) {
                Map<String, Object> params = new HashMap<>();
                params.put("timestamp", (long) i);
                sink.log(TestEventType.TEST_EVENT_1, params);
            }

            // Wait a short time for the buffer to be processed
            Thread.sleep(200);

            // Verify some events were flushed by checking the file size
            long fileSize = Files.size(logFile);
            assertTrue(fileSize > 0, "File should contain data after exceeding buffer size");
        }
    }
}
