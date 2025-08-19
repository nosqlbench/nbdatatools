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
import java.util.function.BiConsumer;
import java.util.concurrent.CompletableFuture;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ProgressIndicatorTest {

    private TestProgressIndicator progressIndicator;
    
    @BeforeEach
    void setUp() {
        progressIndicator = new TestProgressIndicator();
    }
    
    @Test
    void testGetProgressPercentage() {
        progressIndicator.setProgress(25, 100);
        assertEquals(25.0, progressIndicator.getProgressPercentage(), 0.001);
        
        progressIndicator.setProgress(0, 100);
        assertEquals(0.0, progressIndicator.getProgressPercentage(), 0.001);
        
        progressIndicator.setProgress(100, 100);
        assertEquals(100.0, progressIndicator.getProgressPercentage(), 0.001);
        
        // Edge case: zero total work
        progressIndicator.setProgress(0, 0);
        assertEquals(0.0, progressIndicator.getProgressPercentage(), 0.001);
    }
    
    @Test
    void testGetProgressFraction() {
        progressIndicator.setProgress(25, 100);
        assertEquals(0.25, progressIndicator.getProgressFraction(), 0.001);
        
        progressIndicator.setProgress(50, 200);
        assertEquals(0.25, progressIndicator.getProgressFraction(), 0.001);
        
        // Edge case: zero total work
        progressIndicator.setProgress(0, 0);
        assertEquals(0.0, progressIndicator.getProgressFraction(), 0.001);
    }
    
    @Test
    void testHasProgress() {
        progressIndicator.setProgress(0, 100);
        assertFalse(progressIndicator.hasProgress());
        
        progressIndicator.setProgress(1, 100);
        assertTrue(progressIndicator.hasProgress());
        
        progressIndicator.setProgress(100, 100);
        assertTrue(progressIndicator.hasProgress());
    }
    
    @Test
    void testIsWorkComplete() {
        progressIndicator.setProgress(50, 100);
        assertFalse(progressIndicator.isWorkComplete());
        
        progressIndicator.setProgress(100, 100);
        assertTrue(progressIndicator.isWorkComplete());
        
        progressIndicator.setProgress(101, 100);
        assertTrue(progressIndicator.isWorkComplete());
        
        // Edge case: zero total work
        progressIndicator.setProgress(0, 0);
        assertFalse(progressIndicator.isWorkComplete());
    }
    
    @Test
    void testGetRemainingWork() {
        progressIndicator.setProgress(25, 100);
        assertEquals(75.0, progressIndicator.getRemainingWork(), 0.001);
        
        progressIndicator.setProgress(100, 100);
        assertEquals(0.0, progressIndicator.getRemainingWork(), 0.001);
        
        progressIndicator.setProgress(101, 100);
        assertEquals(0.0, progressIndicator.getRemainingWork(), 0.001);
    }
    
    
    @Test
    void testGetProgressString() {
        progressIndicator.setProgress(45.5, 100.0);
        String progressString = progressIndicator.getProgressString();
        assertTrue(progressString.contains("45.5"));
        assertTrue(progressString.contains("100.0"));
        assertTrue(progressString.contains("45.5%"));
    }
    
    @Test
    void testFormatBytes() {
        assertEquals("512 B", progressIndicator.formatBytes(512));
        assertEquals("1.5 KB", progressIndicator.formatBytes(1536));
        assertEquals("2.0 MB", progressIndicator.formatBytes(2 * 1024 * 1024));
        assertEquals("1.5 GB", progressIndicator.formatBytes(1.5 * 1024 * 1024 * 1024));
        assertEquals("2.0 TB", progressIndicator.formatBytes(2.0 * 1024 * 1024 * 1024 * 1024));
    }
    
    @Test
    void testProgressStringWithBytesPerUnit() {
        TestProgressIndicatorWithBytes bytesIndicator = new TestProgressIndicatorWithBytes();
        bytesIndicator.setProgress(5, 10);
        bytesIndicator.setBytesPerUnit(1024); // 1KB per unit
        
        String progressString = bytesIndicator.getProgressString();
        assertTrue(progressString.contains("chunks"));
        assertTrue(progressString.contains("KB"));
        assertTrue(progressString.contains("5.0/10.0"));
    }
    
    @Test
    void testEnhancedProgressStringWithMbitRate() throws InterruptedException {
        TestProgressIndicatorWithBytes bytesIndicator = new TestProgressIndicatorWithBytes();
        bytesIndicator.setBytesPerUnit(1024 * 1024); // 1MB per unit
        
        // Create progress history manually to simulate rate calculation
        java.util.Deque<ProgressIndicator.ProgressSample> progressHistory = new java.util.ArrayDeque<>();
        long startTime = System.currentTimeMillis();
        
        // Add initial sample
        progressHistory.add(new ProgressIndicator.ProgressSample(0.0, startTime));
        
        // Simulate progress after 1 second
        Thread.sleep(100); // Short sleep for test
        long currentTime = System.currentTimeMillis();
        
        // Simulate downloading 10MB in the time elapsed (10 units * 1MB per unit)
        double timeElapsedMs = currentTime - startTime;
        double simulatedWorkDone = 10.0; // 10 MB worth of work
        progressHistory.add(new ProgressIndicator.ProgressSample(simulatedWorkDone, currentTime));
        
        bytesIndicator.setProgress(simulatedWorkDone, 100.0);
        
        String enhancedString = bytesIndicator.getEnhancedProgressString(progressHistory, startTime);
        
        // Should contain Mbit/s rate since we have bytes per unit > 1
        assertTrue(enhancedString.contains("Mbit/s"), "Enhanced string should contain Mbit/s rate: " + enhancedString);
        assertTrue(enhancedString.contains("elapsed"), "Enhanced string should contain elapsed time: " + enhancedString);
    }
    
    @Test
    void testGetProgressSnapshot() {
        progressIndicator.setProgress(30, 100);
        ProgressIndicator.ProgressSnapshot snapshot = progressIndicator.getProgressSnapshot();
        
        assertEquals(30.0, snapshot.currentWork());
        assertEquals(100.0, snapshot.totalWork());
        assertEquals(30.0, snapshot.getPercentage());
        assertEquals(0.3, snapshot.getFraction());
        assertEquals(70.0, snapshot.getRemainingWork());
        assertFalse(snapshot.isComplete());
        
        progressIndicator.setProgress(100, 100);
        snapshot = progressIndicator.getProgressSnapshot();
        assertTrue(snapshot.isComplete());
    }
    
    @Test
    void testMonitorProgressNonCompletableFuture() throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(outputStream);
        
        // Start monitoring with a short interval
        CompletableFuture<Void> monitoring = progressIndicator.monitorProgress(printStream, 100);
        
        // Simulate progress updates
        Thread.sleep(50);
        progressIndicator.setProgress(25, 100);
        Thread.sleep(150);
        progressIndicator.setProgress(50, 100);
        Thread.sleep(150);
        progressIndicator.setProgress(100, 100); // Complete
        
        // Wait for monitoring to complete
        monitoring.get();
        
        String output = outputStream.toString();
        assertTrue(output.contains("0.0/100.0"));
        assertTrue(output.contains("Complete"));
    }

    // Test implementation of ProgressIndicator for testing
    private static class TestProgressIndicator implements ProgressIndicator<String> {
        private double currentWork = 0;
        private double totalWork = 100;
        
        void setProgress(double current, double total) {
            this.currentWork = current;
            this.totalWork = total;
        }
        
        @Override
        public double getTotalWork() {
            return totalWork;
        }
        
        @Override
        public double getCurrentWork() {
            return currentWork;
        }
    }
    
    // Test implementation with bytes per unit support
    private static class TestProgressIndicatorWithBytes implements ProgressIndicator<String> {
        private double currentWork = 0;
        private double totalWork = 100;
        private double bytesPerUnit = 1.0;
        
        void setProgress(double current, double total) {
            this.currentWork = current;
            this.totalWork = total;
        }
        
        void setBytesPerUnit(double bytesPerUnit) {
            this.bytesPerUnit = bytesPerUnit;
        }
        
        @Override
        public double getTotalWork() {
            return totalWork;
        }
        
        @Override
        public double getCurrentWork() {
            return currentWork;
        }
        
        @Override
        public double getBytesPerUnit() {
            return bytesPerUnit;
        }
    }
}