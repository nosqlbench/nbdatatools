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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

/**
 * Monitors JVM heap memory pressure to guide adaptive prefetching behavior.
 *
 * <h2>Purpose</h2>
 *
 * <p>This class provides real-time monitoring of JVM heap usage to allow
 * data processing pipelines to adapt their memory consumption. When memory
 * pressure is high, consumers can reduce prefetch buffers or pause prefetching
 * entirely to avoid OutOfMemoryErrors.
 *
 * <h2>Pressure Levels</h2>
 *
 * <ul>
 *   <li><b>LOW</b> - Heap usage below moderate threshold; full prefetching</li>
 *   <li><b>MODERATE</b> - Heap usage between moderate and high thresholds; reduced prefetching</li>
 *   <li><b>HIGH</b> - Heap usage above high threshold; pause prefetching</li>
 * </ul>
 *
 * <h2>Default Thresholds</h2>
 *
 * <ul>
 *   <li>High pressure: 85% of max heap</li>
 *   <li>Moderate pressure: 70% of max heap</li>
 * </ul>
 *
 * @see PrefetchingDataSource
 */
public final class MemoryPressureMonitor {

    /**
     * Memory pressure levels.
     */
    public enum PressureLevel {
        /** Heap usage below moderate threshold - full prefetching allowed */
        LOW,
        /** Heap usage between moderate and high - reduced prefetching */
        MODERATE,
        /** Heap usage above high threshold - pause prefetching */
        HIGH
    }

    /**
     * Snapshot of current memory state.
     *
     * @param usedBytes bytes currently used in heap
     * @param committedBytes bytes committed to the heap
     * @param maxBytes maximum heap size
     * @param usageFraction fraction of max heap used (0.0 to 1.0)
     * @param pressureLevel current pressure level
     */
    public record MemoryState(
        long usedBytes,
        long committedBytes,
        long maxBytes,
        double usageFraction,
        PressureLevel pressureLevel
    ) {
        @Override
        public String toString() {
            return String.format("MemoryState[used=%d MB, committed=%d MB, max=%d MB, %.1f%%, %s]",
                usedBytes / (1024 * 1024),
                committedBytes / (1024 * 1024),
                maxBytes / (1024 * 1024),
                usageFraction * 100,
                pressureLevel);
        }
    }

    /** Default high pressure threshold (85% of max heap) */
    public static final double DEFAULT_HIGH_THRESHOLD = 0.85;

    /** Default moderate pressure threshold (70% of max heap) */
    public static final double DEFAULT_MODERATE_THRESHOLD = 0.70;

    private final MemoryMXBean memoryMXBean;
    private final double highThreshold;
    private final double moderateThreshold;

    /**
     * Creates a monitor with default thresholds.
     */
    public MemoryPressureMonitor() {
        this(DEFAULT_HIGH_THRESHOLD, DEFAULT_MODERATE_THRESHOLD);
    }

    /**
     * Creates a monitor with custom thresholds.
     *
     * @param highThreshold threshold for HIGH pressure (must be in (0, 1])
     * @param moderateThreshold threshold for MODERATE pressure (must be in (0, 1] and less than highThreshold)
     * @throws IllegalArgumentException if thresholds are invalid
     */
    public MemoryPressureMonitor(double highThreshold, double moderateThreshold) {
        if (highThreshold <= 0.0 || highThreshold > 1.0) {
            throw new IllegalArgumentException(
                "highThreshold must be in (0, 1], got: " + highThreshold);
        }
        if (moderateThreshold <= 0.0 || moderateThreshold > 1.0) {
            throw new IllegalArgumentException(
                "moderateThreshold must be in (0, 1], got: " + moderateThreshold);
        }
        if (moderateThreshold >= highThreshold) {
            throw new IllegalArgumentException(
                "moderateThreshold must be less than highThreshold, got moderate=" +
                moderateThreshold + ", high=" + highThreshold);
        }

        this.highThreshold = highThreshold;
        this.moderateThreshold = moderateThreshold;
        this.memoryMXBean = ManagementFactory.getMemoryMXBean();
    }

    /**
     * Returns the current heap usage as a fraction of max heap.
     *
     * @return usage fraction in range [0.0, 1.0]
     */
    public double getHeapUsageFraction() {
        MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();
        long used = heapUsage.getUsed();
        long max = heapUsage.getMax();

        if (max <= 0) {
            // Max not defined, use committed as fallback
            max = heapUsage.getCommitted();
        }

        return max > 0 ? (double) used / max : 0.0;
    }

    /**
     * Returns the current memory pressure level.
     *
     * @return the pressure level based on current heap usage
     */
    public PressureLevel getPressureLevel() {
        double usage = getHeapUsageFraction();

        if (usage >= highThreshold) {
            return PressureLevel.HIGH;
        } else if (usage >= moderateThreshold) {
            return PressureLevel.MODERATE;
        } else {
            return PressureLevel.LOW;
        }
    }

    /**
     * Returns the number of free bytes available in the heap.
     *
     * @return free heap bytes
     */
    public long getFreeHeapBytes() {
        MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();
        long max = heapUsage.getMax();

        if (max <= 0) {
            max = heapUsage.getCommitted();
        }

        return Math.max(0, max - heapUsage.getUsed());
    }

    /**
     * Returns a snapshot of the current memory state.
     *
     * @return the current memory state
     */
    public MemoryState getMemoryState() {
        MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();
        long used = heapUsage.getUsed();
        long committed = heapUsage.getCommitted();
        long max = heapUsage.getMax();

        if (max <= 0) {
            max = committed;
        }

        double usageFraction = max > 0 ? (double) used / max : 0.0;
        PressureLevel level = getPressureLevel();

        return new MemoryState(used, committed, max, usageFraction, level);
    }

    /**
     * Returns the recommended prefetch count based on current memory pressure.
     *
     * <ul>
     *   <li>LOW pressure: returns the requested count</li>
     *   <li>MODERATE pressure: returns half of requested (minimum 1)</li>
     *   <li>HIGH pressure: returns 1 (minimum prefetch)</li>
     * </ul>
     *
     * @param requested the requested prefetch count
     * @return the recommended prefetch count (minimum 1)
     */
    public int recommendedPrefetchCount(int requested) {
        PressureLevel level = getPressureLevel();

        return switch (level) {
            case LOW -> Math.max(1, requested);
            case MODERATE -> Math.max(1, requested / 2);
            case HIGH -> 1;
        };
    }

    /**
     * Returns whether prefetching should be paused due to high memory pressure.
     *
     * @return true if pressure is HIGH
     */
    public boolean shouldPausePrefetch() {
        return getPressureLevel() == PressureLevel.HIGH;
    }

    /**
     * Returns whether prefetching should be reduced due to memory pressure.
     *
     * @return true if pressure is MODERATE or HIGH
     */
    public boolean shouldReducePrefetch() {
        PressureLevel level = getPressureLevel();
        return level == PressureLevel.MODERATE || level == PressureLevel.HIGH;
    }

    /**
     * Waits for memory pressure to drop below the high threshold.
     *
     * <p>This method performs the following:
     * <ol>
     *   <li>If not under high pressure, returns immediately</li>
     *   <li>Requests garbage collection</li>
     *   <li>Waits up to the timeout for pressure to drop</li>
     * </ol>
     *
     * @param timeoutMs maximum time to wait in milliseconds
     * @return true if memory pressure dropped below high threshold, false if timeout
     */
    public boolean waitForMemoryRelief(long timeoutMs) {
        if (!shouldPausePrefetch()) {
            return true;
        }

        // Request GC
        System.gc();

        long deadline = System.currentTimeMillis() + timeoutMs;
        long sleepInterval = Math.min(100, timeoutMs / 10);

        while (System.currentTimeMillis() < deadline) {
            if (!shouldPausePrefetch()) {
                return true;
            }

            try {
                Thread.sleep(sleepInterval);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        return !shouldPausePrefetch();
    }
}
