package io.nosqlbench.nbdatatools.api.concurrent;

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
import org.junit.jupiter.api.BeforeEach;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class ProgressIndicatingFutureTest {
    
    private CompletableFuture<String> underlyingFuture;
    private AtomicReference<Double> totalWork;
    private AtomicReference<Double> currentWork;
    private ProgressIndicatingFuture<String> progressFuture;
    
    @BeforeEach
    void setUp() {
        underlyingFuture = new CompletableFuture<>();
        totalWork = new AtomicReference<>(100.0);
        currentWork = new AtomicReference<>(0.0);
        
        Supplier<Double> totalCallback = totalWork::get;
        Supplier<Double> currentCallback = currentWork::get;
        
        progressFuture = new ProgressIndicatingFuture<>(underlyingFuture, totalCallback, currentCallback);
    }
    
    @Test
    void testCallbackBasedProgress() {
        assertEquals(100.0, progressFuture.getTotalWork());
        assertEquals(0.0, progressFuture.getCurrentWork());
        
        currentWork.set(25.0);
        assertEquals(25.0, progressFuture.getCurrentWork());
        assertEquals(25.0, progressFuture.getProgressPercentage());
        
        totalWork.set(200.0);
        assertEquals(200.0, progressFuture.getTotalWork());
        assertEquals(12.5, progressFuture.getProgressPercentage());
    }
    
    @Test
    void testFutureCompletion() throws Exception {
        assertFalse(progressFuture.isDone());
        
        underlyingFuture.complete("result");
        
        assertTrue(progressFuture.isDone());
        assertEquals("result", progressFuture.get());
    }
    
    @Test
    void testFutureException() {
        Exception testException = new RuntimeException("test error");
        underlyingFuture.completeExceptionally(testException);
        
        assertTrue(progressFuture.isDone());
        assertTrue(progressFuture.isCompletedExceptionally());
    }
    
    @Test
    void testInheritsCompletableFutureBehavior() {
        // Test that it properly extends CompletableFuture
        assertTrue(progressFuture instanceof CompletableFuture);
        assertTrue(progressFuture instanceof ProgressIndicator);
        
        // Test chaining
        CompletableFuture<Integer> chained = progressFuture.thenApply(String::length);
        underlyingFuture.complete("hello");
        
        assertEquals(5, chained.join());
    }
    
    @Test
    void testBytesPerUnitMultiplier() {
        // Test default bytes per unit
        assertEquals(1.0, progressFuture.getBytesPerUnit());
        
        // Test with custom bytes per unit
        CompletableFuture<String> underlyingFuture2 = new CompletableFuture<>();
        ProgressIndicatingFuture<String> progressFutureWithBytes = new ProgressIndicatingFuture<>(
            underlyingFuture2, 
            () -> 10.0, 
            () -> 5.0, 
            1024.0);
        
        assertEquals(1024.0, progressFutureWithBytes.getBytesPerUnit());
        
        // Test progress string with bytes
        String progressString = progressFutureWithBytes.getProgressString();
        assertTrue(progressString.contains("chunks"));
        assertTrue(progressString.contains("KB"));
    }
}