/*
 * Copyright (c) nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.nosqlbench.command.common;

import picocli.CommandLine;

/**
 * Shared batch processing options for memory-limited parallel operations.
 * All supporting types are inner classes for self-contained encapsulation.
 */
public class BatchProcessingOption {

    /**
     * Immutable batch processing configuration.
     * Specifies threading, memory limits, and batch size for parallel operations.
     *
     * @param threads        number of threads (0 = auto-detect from CPU cores)
     * @param memoryLimitMB  maximum memory for buffering in MB (0 = no limit)
     * @param batchSize      number of items to process per batch
     */
    public record BatchConfig(int threads, long memoryLimitMB, int batchSize) {

        /**
         * Default configuration: auto threads, 1GB memory limit, 10000 batch size.
         */
        public static final BatchConfig DEFAULT = new BatchConfig(0, 1024, 10000);

        /**
         * Compact constructor with validation.
         */
        public BatchConfig {
            if (threads < 0) {
                throw new IllegalArgumentException("Thread count cannot be negative: " + threads);
            }
            if (memoryLimitMB < 0) {
                throw new IllegalArgumentException("Memory limit cannot be negative: " + memoryLimitMB);
            }
            if (batchSize <= 0) {
                throw new IllegalArgumentException("Batch size must be positive: " + batchSize);
            }
        }

        /**
         * Gets the effective thread count, auto-detecting if set to 0.
         */
        public int effectiveThreads() {
            return threads > 0 ? threads : Runtime.getRuntime().availableProcessors();
        }

        /**
         * Gets the memory limit in bytes.
         */
        public long memoryLimitBytes() {
            return memoryLimitMB * 1024 * 1024;
        }

        /**
         * Checks if memory limit is enabled (non-zero).
         */
        public boolean hasMemoryLimit() {
            return memoryLimitMB > 0;
        }

        /**
         * Checks if parallel processing is enabled (threads > 1).
         */
        public boolean isParallel() {
            return effectiveThreads() > 1;
        }

        /**
         * Returns a debuggable string representation.
         */
        @Override
        public String toString() {
            String threadInfo = threads > 0 ? String.valueOf(threads) : "auto";
            String memoryInfo = memoryLimitMB > 0 ? memoryLimitMB + "MB" : "unlimited";
            return "threads=" + threadInfo + ", memory=" + memoryInfo + ", batchSize=" + batchSize;
        }
    }

    @CommandLine.Option(
        names = {"--threads"},
        description = "Number of threads for parallel processing (0 = auto-detect from CPU cores)",
        defaultValue = "0"
    )
    private int threads = 0;

    @CommandLine.Option(
        names = {"--memory-limit"},
        description = "Maximum memory for buffering in MB (0 = no limit)",
        defaultValue = "1024"
    )
    private long memoryLimitMB = 1024;

    @CommandLine.Option(
        names = {"--batch-size"},
        description = "Number of items to process per batch",
        defaultValue = "10000"
    )
    private int batchSize = 10000;

    /**
     * Gets the BatchConfig record constructed from the options.
     * Validation happens when the record is created.
     */
    public BatchConfig getBatchConfig() {
        return new BatchConfig(threads, memoryLimitMB, batchSize);
    }

    /**
     * Gets the effective thread count, auto-detecting if set to 0.
     */
    public int getEffectiveThreadCount() {
        return getBatchConfig().effectiveThreads();
    }

    /**
     * Gets the raw thread count value (may be 0 for auto-detect).
     */
    public int getThreads() {
        return threads;
    }

    /**
     * Gets the memory limit in bytes.
     */
    public long getMemoryLimitBytes() {
        return getBatchConfig().memoryLimitBytes();
    }

    /**
     * Gets the memory limit in megabytes.
     */
    public long getMemoryLimitMB() {
        return memoryLimitMB;
    }

    /**
     * Gets the batch size.
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Checks if memory limit is enabled (non-zero).
     */
    public boolean hasMemoryLimit() {
        return getBatchConfig().hasMemoryLimit();
    }

    /**
     * Checks if parallel processing is enabled (threads > 1).
     */
    public boolean isParallel() {
        return getBatchConfig().isParallel();
    }

    /**
     * Validates the options.
     */
    public void validate() {
        getBatchConfig(); // Will throw if invalid
    }

    /**
     * Returns string representation of the batch configuration.
     */
    @Override
    public String toString() {
        return getBatchConfig().toString();
    }
}
