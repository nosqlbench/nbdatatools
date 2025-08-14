package io.nosqlbench.nbdatatools.api.transport;

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


import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Example usage and test cases for the PartialData interface.
 */
public class PartialDataExampleTest {

    @Test
    void testPartialDataWithStringTracker() {
        AtomicReference<String> receivedTracker = new AtomicReference<>();
        AtomicInteger receivedDataSize = new AtomicInteger(0);
        
        // Create a PartialData callback with String tracking object
        PartialData<String> callback = (data, tracker) -> {
            receivedTracker.set(tracker);
            receivedDataSize.set(data.remaining());
        };
        
        // Simulate receiving partial data
        ByteBuffer testData = ByteBuffer.wrap("test data".getBytes());
        String trackingId = "request-123";
        
        assertDoesNotThrow(() -> callback.onPartialData(testData, trackingId));
        
        assertEquals("request-123", receivedTracker.get());
        assertEquals(9, receivedDataSize.get()); // "test data" length
    }
    
    @Test
    void testPartialDataWithCustomTracker() {
        // Custom tracking object
        record RequestContext(String requestId, int expectedSize, long startTime) {}
        
        AtomicReference<RequestContext> receivedContext = new AtomicReference<>();
        AtomicInteger totalBytesReceived = new AtomicInteger(0);
        
        PartialData<RequestContext> callback = (data, context) -> {
            receivedContext.set(context);
            totalBytesReceived.addAndGet(data.remaining());
        };
        
        RequestContext context = new RequestContext("req-456", 1024, System.currentTimeMillis());
        ByteBuffer chunk1 = ByteBuffer.wrap("chunk1".getBytes());
        ByteBuffer chunk2 = ByteBuffer.wrap("chunk2".getBytes());
        
        assertDoesNotThrow(() -> {
            callback.onPartialData(chunk1, context);
            callback.onPartialData(chunk2, context);
        });
        
        assertEquals("req-456", receivedContext.get().requestId());
        assertEquals(1024, receivedContext.get().expectedSize());
        assertEquals(12, totalBytesReceived.get()); // "chunk1" + "chunk2"
    }
    
    @Test
    void testPartialDataExceptionHandling() {
        PartialData<String> failingCallback = (data, tracker) -> {
            throw new RuntimeException("Processing failed");
        };
        
        ByteBuffer testData = ByteBuffer.wrap("test".getBytes());
        
        assertThrows(RuntimeException.class, () -> 
            failingCallback.onPartialData(testData, "test-tracker"));
    }
    
    @Test
    void testPartialDataWithNullTracker() {
        AtomicReference<Object> receivedTracker = new AtomicReference<>();
        
        PartialData<Object> callback = (data, tracker) -> {
            receivedTracker.set(tracker);
        };
        
        ByteBuffer testData = ByteBuffer.wrap("test".getBytes());
        
        assertDoesNotThrow(() -> callback.onPartialData(testData, null));
        assertNull(receivedTracker.get());
    }
}
