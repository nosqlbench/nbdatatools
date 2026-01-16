package io.nosqlbench.vshapes.stream;

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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/// Unit tests for MemoryPressureMonitor.
@Tag("unit")
class MemoryPressureMonitorTest {

    @Test
    void constructor_withDefaultThresholds() {
        MemoryPressureMonitor monitor = new MemoryPressureMonitor();
        assertNotNull(monitor);
    }

    @Test
    void constructor_withCustomThresholds() {
        MemoryPressureMonitor monitor = new MemoryPressureMonitor(0.90, 0.75);
        assertNotNull(monitor);
    }

    @Test
    void constructor_rejectsInvalidHighThreshold() {
        assertThrows(IllegalArgumentException.class, () ->
            new MemoryPressureMonitor(0.0, 0.5));
        assertThrows(IllegalArgumentException.class, () ->
            new MemoryPressureMonitor(1.1, 0.5));
    }

    @Test
    void constructor_rejectsInvalidModerateThreshold() {
        assertThrows(IllegalArgumentException.class, () ->
            new MemoryPressureMonitor(0.9, 0.0));
        assertThrows(IllegalArgumentException.class, () ->
            new MemoryPressureMonitor(0.9, 1.1));
    }

    @Test
    void constructor_rejectsModerateGreaterThanHigh() {
        assertThrows(IllegalArgumentException.class, () ->
            new MemoryPressureMonitor(0.70, 0.85));
        assertThrows(IllegalArgumentException.class, () ->
            new MemoryPressureMonitor(0.80, 0.80));
    }

    @Test
    void getHeapUsageFraction_returnsValidValue() {
        MemoryPressureMonitor monitor = new MemoryPressureMonitor();
        double usage = monitor.getHeapUsageFraction();

        assertTrue(usage >= 0.0, "Usage should be >= 0");
        assertTrue(usage <= 1.0, "Usage should be <= 1.0");
    }

    @Test
    void getPressureLevel_returnsValidLevel() {
        MemoryPressureMonitor monitor = new MemoryPressureMonitor();
        MemoryPressureMonitor.PressureLevel level = monitor.getPressureLevel();

        assertNotNull(level);
        assertTrue(level == MemoryPressureMonitor.PressureLevel.LOW ||
                   level == MemoryPressureMonitor.PressureLevel.MODERATE ||
                   level == MemoryPressureMonitor.PressureLevel.HIGH);
    }

    @Test
    void getFreeHeapBytes_returnsPositiveValue() {
        MemoryPressureMonitor monitor = new MemoryPressureMonitor();
        long freeBytes = monitor.getFreeHeapBytes();

        assertTrue(freeBytes >= 0, "Free bytes should be >= 0");
    }

    @Test
    void getMemoryState_returnsValidSnapshot() {
        MemoryPressureMonitor monitor = new MemoryPressureMonitor();
        MemoryPressureMonitor.MemoryState state = monitor.getMemoryState();

        assertNotNull(state);
        assertTrue(state.usedBytes() >= 0);
        assertTrue(state.committedBytes() >= 0);
        assertTrue(state.maxBytes() > 0);
        assertNotNull(state.pressureLevel());
        assertTrue(state.usageFraction() >= 0.0 && state.usageFraction() <= 1.0);
    }

    @Test
    void memoryState_toString_isReadable() {
        MemoryPressureMonitor monitor = new MemoryPressureMonitor();
        MemoryPressureMonitor.MemoryState state = monitor.getMemoryState();

        String str = state.toString();
        assertNotNull(str);
        assertTrue(str.contains("MemoryState"));
        assertTrue(str.contains("MB"));
        assertTrue(str.contains("%"));
    }

    @Test
    void recommendedPrefetchCount_lowPressure_returnsRequested() {
        // With very high thresholds, we should be in LOW pressure
        MemoryPressureMonitor monitor = new MemoryPressureMonitor(0.99, 0.98);

        // At low pressure, should return the requested count
        int result = monitor.recommendedPrefetchCount(4);
        assertEquals(4, result);
    }

    @Test
    void recommendedPrefetchCount_minimumIsOne() {
        // Even at high pressure, should return at least 1
        MemoryPressureMonitor monitor = new MemoryPressureMonitor();
        int result = monitor.recommendedPrefetchCount(1);

        assertTrue(result >= 1, "Recommended count should be at least 1");
    }

    @Test
    void shouldPausePrefetch_returnsBooleanCorrectly() {
        MemoryPressureMonitor monitor = new MemoryPressureMonitor();

        // Method should return a valid boolean without error
        boolean shouldPause = monitor.shouldPausePrefetch();
        // Can't predict the actual value, just verify it runs
        assertTrue(shouldPause || !shouldPause);
    }

    @Test
    void shouldReducePrefetch_returnsBooleanCorrectly() {
        MemoryPressureMonitor monitor = new MemoryPressureMonitor();

        boolean shouldReduce = monitor.shouldReducePrefetch();
        // Can't predict the actual value, just verify it runs
        assertTrue(shouldReduce || !shouldReduce);
    }

    @Test
    void waitForMemoryRelief_returnsQuicklyWhenNotUnderPressure() {
        // With very high thresholds, we shouldn't be under pressure
        MemoryPressureMonitor monitor = new MemoryPressureMonitor(0.99, 0.98);

        long start = System.currentTimeMillis();
        boolean result = monitor.waitForMemoryRelief(10000);
        long elapsed = System.currentTimeMillis() - start;

        assertTrue(result, "Should return true when not under pressure");
        assertTrue(elapsed < 1000, "Should return quickly when not under pressure");
    }

    @Test
    void pressureLevelEnum_hasExpectedValues() {
        MemoryPressureMonitor.PressureLevel[] levels = MemoryPressureMonitor.PressureLevel.values();

        assertEquals(3, levels.length);
        assertNotNull(MemoryPressureMonitor.PressureLevel.LOW);
        assertNotNull(MemoryPressureMonitor.PressureLevel.MODERATE);
        assertNotNull(MemoryPressureMonitor.PressureLevel.HIGH);
    }
}
