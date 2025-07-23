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
import io.nosqlbench.vectordata.events.MemoryEventSink;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Test class for MemoryEventSink
///
/// This class tests the event storage, lazy sorting, event limit, and
/// consumeAndPurge functionality of the MemoryEventSink class.
class MemoryEventSinkTest {

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
    }

    private final PrintStream originalErr = System.err;
    private ByteArrayOutputStream errContent;

    @BeforeEach
    void setUpStreams() {
        errContent = new ByteArrayOutputStream();
        System.setErr(new PrintStream(errContent));
    }

    @AfterEach
    void restoreStreams() {
        System.setErr(originalErr);
    }

    /// Test that events are stored and can be retrieved
    @Test
    void testEventStorage() {
        // Create a MemoryEventSink
        MemoryEventSink sink = new MemoryEventSink();

        // Log events
        for (int i = 0; i < 5; i++) {
            Map<String, Object> params = new HashMap<>();
            params.put("timestamp", (long) i);
            sink.log(TestEventType.TEST_EVENT_1, params);
        }

        // Verify events were stored
        List<MemoryEventSink.LogEvent> events = sink.getEvents();
        assertEquals(6, events.size(), "Should have 5 events plus the init event");

        // Verify the first event is the init event
        assertTrue(events.get(0).getFormattedMessage().contains("FILE_INIT"), 
            "First event should be the file init event");

        // Verify the remaining events have the correct timestamps
        for (int i = 1; i < events.size(); i++) {
            MemoryEventSink.LogEvent event = events.get(i);
            Map<String, Object> params = event.getParams();
            assertNotNull(params, "Event params should not be null");
            assertEquals((long) (i - 1), params.get("timestamp"), 
                "Event should have the correct timestamp");
        }
    }

    /// Test that events are lazily sorted by timestamp
    @Test
    void testLazySorting() throws InterruptedException {
        // Create a MemoryEventSink
        MemoryEventSink sink = new MemoryEventSink();

        // Log events in reverse order
        for (int i = 10; i > 0; i--) {
            Map<String, Object> params = new HashMap<>();
            params.put("timestamp", (long) i);
            sink.log(TestEventType.TEST_EVENT_1, params);
            
            // Add a small delay to ensure different timestamps
            Thread.sleep(10);
        }

        // Verify events are sorted by timestamp when accessed
        List<MemoryEventSink.LogEvent> events = sink.getEvents();
        
        // Skip the first event (init event)
        for (int i = 1; i < events.size() - 1; i++) {
            assertTrue(
                events.get(i).getTimestamp().isBefore(events.get(i + 1).getTimestamp()) ||
                events.get(i).getTimestamp().equals(events.get(i + 1).getTimestamp()),
                "Events should be sorted by timestamp"
            );
        }
    }

    /// Test that the event limit is enforced
    @Test
    void testEventLimit() {
        // Create a MemoryEventSink with a small limit
        int limit = 10;
        MemoryEventSink sink = new MemoryEventSink(limit);

        // Log more events than the limit
        for (int i = 0; i < limit * 2; i++) {
            Map<String, Object> params = new HashMap<>();
            params.put("timestamp", (long) i);
            sink.log(TestEventType.TEST_EVENT_1, params);
        }

        // Verify only up to the limit events are stored (plus the init event)
        List<MemoryEventSink.LogEvent> events = sink.getEvents();
        assertEquals(limit, events.size(), "Should have at most 'limit' events");

        // Verify excess events were logged to System.err
        String errOutput = errContent.toString();
        assertFalse(errOutput.isEmpty(), "Excess events should be logged to System.err");
    }

    /// Test the consumeAndPurge method
    @Test
    void testConsumeAndPurge() {
        // Create a MemoryEventSink
        MemoryEventSink sink = new MemoryEventSink();

        // Log some events
        for (int i = 0; i < 5; i++) {
            Map<String, Object> params = new HashMap<>();
            params.put("timestamp", (long) i);
            sink.log(TestEventType.TEST_EVENT_1, params);
        }

        // Consume and purge the events
        List<MemoryEventSink.LogEvent> consumedEvents = sink.consumeAndPurge();
        assertEquals(6, consumedEvents.size(), "Should have consumed 5 events plus the init event");

        // Verify the sink is now empty
        List<MemoryEventSink.LogEvent> remainingEvents = sink.getEvents();
        assertTrue(remainingEvents.isEmpty(), "Sink should be empty after consumeAndPurge");
    }

    /// Test concurrent access to the sink
    @Test
    void testConcurrentAccess() throws InterruptedException {
        // Create a MemoryEventSink
        MemoryEventSink sink = new MemoryEventSink();
        
        // Number of threads and events per thread
        int threadCount = 10;
        int eventsPerThread = 100;
        
        // CountDownLatch to synchronize thread start
        CountDownLatch startLatch = new CountDownLatch(1);
        
        // CountDownLatch to wait for all threads to finish
        CountDownLatch finishLatch = new CountDownLatch(threadCount);
        
        // Track the total number of events added
        AtomicInteger totalEventsAdded = new AtomicInteger(0);
        
        // Create and start threads
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    // Wait for the start signal
                    startLatch.await();
                    
                    // Add events
                    for (int i = 0; i < eventsPerThread; i++) {
                        Map<String, Object> params = new HashMap<>();
                        params.put("timestamp", (long) (threadId * eventsPerThread + i));
                        params.put("threadId", threadId);
                        sink.log(TestEventType.TEST_EVENT_1, params);
                        totalEventsAdded.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    finishLatch.countDown();
                }
            });
        }
        
        // Start all threads simultaneously
        startLatch.countDown();
        
        // Wait for all threads to finish
        finishLatch.await(10, TimeUnit.SECONDS);
        executor.shutdown();
        
        // Verify all events were added (or logged to System.err if over the limit)
        int expectedEvents = Math.min(totalEventsAdded.get() + 1, sink.getEventLimit()); // +1 for init event
        assertEquals(expectedEvents, sink.getEventCount(), 
            "Should have the expected number of events");
        
        // Test concurrent consume and purge
        List<MemoryEventSink.LogEvent> consumedEvents = sink.consumeAndPurge();
        assertEquals(expectedEvents, consumedEvents.size(), 
            "Should have consumed the expected number of events");
        assertEquals(0, sink.getEventCount(), "Sink should be empty after consumeAndPurge");
    }

    /// Test the clear method
    @Test
    void testClear() {
        // Create a MemoryEventSink
        MemoryEventSink sink = new MemoryEventSink();

        // Log some events
        for (int i = 0; i < 5; i++) {
            Map<String, Object> params = new HashMap<>();
            params.put("timestamp", (long) i);
            sink.log(TestEventType.TEST_EVENT_1, params);
        }

        // Verify events were stored
        assertEquals(6, sink.getEventCount(), "Should have 5 events plus the init event");

        // Clear the sink
        sink.clear();

        // Verify the sink is now empty
        assertEquals(0, sink.getEventCount(), "Sink should be empty after clear");
    }

    /// Test different log levels
    @Test
    void testLogLevels() {
        // Create a MemoryEventSink
        MemoryEventSink sink = new MemoryEventSink();

        // Log events with different levels
        sink.debug("Debug message");
        sink.info("Info message");
        sink.warn("Warn message");
        sink.error("Error message");
        sink.trace("Trace message");

        // Log events with throwables
        Exception exception = new Exception("Test exception");
        sink.warn("Warn with exception", exception);
        sink.error("Error with exception", exception);

        // Verify all events were stored
        List<MemoryEventSink.LogEvent> events = sink.getEvents();
        assertEquals(8, events.size(), "Should have 7 events plus the init event");

        // Verify the events have the correct levels
        Map<EventType.Level, Integer> levelCounts = new HashMap<>();
        for (MemoryEventSink.LogEvent event : events) {
            EventType.Level level = event.getLevel();
            levelCounts.put(level, levelCounts.getOrDefault(level, 0) + 1);
        }

        assertEquals(1, levelCounts.getOrDefault(EventType.Level.DEBUG, 0), "Should have 1 DEBUG event");
        assertEquals(2, levelCounts.getOrDefault(EventType.Level.INFO, 0), "Should have 2 INFO events (including init)");
        assertEquals(2, levelCounts.getOrDefault(EventType.Level.WARN, 0), "Should have 2 WARN events");
        assertEquals(2, levelCounts.getOrDefault(EventType.Level.ERROR, 0), "Should have 2 ERROR events");
        assertEquals(1, levelCounts.getOrDefault(EventType.Level.TRACE, 0), "Should have 1 TRACE event");
    }
}