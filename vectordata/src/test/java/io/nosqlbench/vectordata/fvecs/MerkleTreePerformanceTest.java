package io.nosqlbench.vectordata.fvecs;

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

import io.nosqlbench.vectordata.merklev2.MerkleRefFactory;
import io.nosqlbench.vectordata.merklev2.MerkleRefBuildProgress;
import io.nosqlbench.vectordata.merklev2.MerkleDataImpl;
import io.nosqlbench.vectordata.merklev2.MerkleShape;
import io.nosqlbench.vectordata.status.EventType;
import io.nosqlbench.vectordata.status.MemoryEventSink;
import jdk.jfr.Configuration;
import jdk.jfr.Recording;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Test class for measuring the performance of MerkleTree creation on a large fvecs file.
 * This test depends on LargeFvecsFileGenerationTest to create the large test file first.
 * It creates a mrkl file on this file and records how long each stage takes using the event sink.
 */
@Tag("performance")
@Disabled
public class MerkleTreePerformanceTest {

    // Custom event type for tracking MerkleTree creation stages
    private enum MerkleCreationStage implements EventType {
        START(EventType.Level.INFO),
        FILE_EXISTENCE_CHECK(EventType.Level.INFO),
        FILE_SIZE_CALCULATION(EventType.Level.INFO),
        RANGE_CALCULATION(EventType.Level.INFO),
        DIMENSION_CALCULATION(EventType.Level.INFO),
        PROGRESS_CREATION(EventType.Level.INFO),
        FILE_PREPARATION(EventType.Level.INFO),
        TREE_CREATION_STARTED(EventType.Level.INFO),
        TREE_CREATION_PROGRESS(EventType.Level.INFO),
        TREE_CREATION_COMPLETED(EventType.Level.INFO),
        TREE_SAVE_STARTED(EventType.Level.INFO),
        TREE_SAVE_COMPLETED(EventType.Level.INFO),
        TREE_VERIFICATION_STARTED(EventType.Level.INFO),
        TREE_VERIFICATION_COMPLETED(EventType.Level.INFO),
        COMPLETE(EventType.Level.INFO);

        private final EventType.Level level;
        private final Map<String, Class<?>> requiredParams = Collections.emptyMap();

        MerkleCreationStage(EventType.Level level) {
            this.level = level;
        }

        @Override
        public EventType.Level getLevel() {
            return level;
        }

        @Override
        public Map<String, Class<?>> getRequiredParams() {
            return requiredParams;
        }
    }

    // Constants for MerkleTree creation
    private static final long CHUNK_SIZE = 1024 * 1024; // 1MB chunks for better performance with large files
    private static final MemoryEventSink eventSink = new MemoryEventSink();

    // JFR recording output path
    private static final String JFR_OUTPUT_DIR = System.getProperty("java.io.tmpdir");
    private static final String JFR_FILENAME_PREFIX = "merkle_tree_performance";

    /**
     * Test method that creates a MerkleTree on the large fvecs file and measures performance.
     * This test is skipped if the large fvecs file doesn't exist.
     *
     * @throws IOException If there's an error creating or verifying the MerkleTree
     */
    @Tag("performance")
    @Test
    public void testMerkleTreeCreationPerformance() throws IOException, ExecutionException, InterruptedException {
        // Initialize profiling tools
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

        // Enable CPU time measurement if supported
        if (threadMXBean.isCurrentThreadCpuTimeSupported()) {
            threadMXBean.setThreadCpuTimeEnabled(true);
        }

        // Get initial memory usage
        MemoryUsage heapBefore = memoryMXBean.getHeapMemoryUsage();
        MemoryUsage nonHeapBefore = memoryMXBean.getNonHeapMemoryUsage();

        // Get initial CPU time
        long startCpuTime = threadMXBean.isCurrentThreadCpuTimeSupported() ? 
            threadMXBean.getCurrentThreadCpuTime() : 0;

        // Start JFR recording
        Recording recording = null;
        Path jfrOutputPath = null;
        try {
            // Load the default JFR configuration (profile)
            Configuration config = Configuration.getConfiguration("profile");

            // Create a new recording with the loaded configuration
            recording = new Recording(config);

            // Enable method profiling for detailed method-level analysis
            recording.enable("jdk.ExecutionSample").withPeriod(Duration.ofMillis(10));
            recording.enable("jdk.NativeMethodSample").withPeriod(Duration.ofMillis(10));
            recording.enable("jdk.ObjectAllocationInNewTLAB").with("enabled", "true");
            recording.enable("jdk.ObjectAllocationOutsideTLAB").with("enabled", "true");

            // Start the recording
            recording.start();

            System.out.println("[DEBUG_LOG] ===== JFR: Started recording =====");
        } catch (Exception e) {
            System.out.println("[DEBUG_LOG] ===== JFR: Failed to start recording: " + e.getMessage() + " =====");
            e.printStackTrace();
        }

        System.out.println("[DEBUG_LOG] ===== PROFILING: Starting MerkleTree Creation Performance Test =====");
        System.out.println("[DEBUG_LOG] Initial Heap Memory: " + formatMemorySize(heapBefore.getUsed()) + 
            " / " + formatMemorySize(heapBefore.getCommitted()));
        System.out.println("[DEBUG_LOG] Initial Non-Heap Memory: " + formatMemorySize(nonHeapBefore.getUsed()) + 
            " / " + formatMemorySize(nonHeapBefore.getCommitted()));

        // Ensure the large fvecs file exists by calling the helper method
        Path largeFvecsFilePath = LargeFvecsFileGenerationTest.ensureLargeFvecsFileExists();

        // Skip the test if the large fvecs file still doesn't exist (could happen if there's not enough disk space)
        Assumptions.assumeTrue(Files.exists(largeFvecsFilePath),
            "Skipping test because the large fvecs file couldn't be created at " + largeFvecsFilePath + ". Check available disk space.");

        System.out.println("[DEBUG_LOG] Starting MerkleTree creation performance test");
        System.out.println("[DEBUG_LOG] Large fvecs file: " + largeFvecsFilePath);
        System.out.println("[DEBUG_LOG] File size: " + (Files.size(largeFvecsFilePath) / (1024 * 1024)) + " MB");

        // Record the start time
        Instant startTime = Instant.now();
        logEvent(MerkleCreationStage.START, Map.of(
            "timestamp", startTime,
            "file", largeFvecsFilePath.toString(),
            "fileSize", Files.size(largeFvecsFilePath)
        ));

        // Log file existence check
        logEvent(MerkleCreationStage.FILE_EXISTENCE_CHECK, Map.of(
            "timestamp", Instant.now(),
            "fileExists", Files.exists(largeFvecsFilePath)
        ));

        // Log file size calculation
        long fileSize = Files.size(largeFvecsFilePath);
        logEvent(MerkleCreationStage.FILE_SIZE_CALCULATION, Map.of(
            "timestamp", Instant.now(),
            "fileSize", fileSize
        ));

        // Log range calculation
        logEvent(MerkleCreationStage.RANGE_CALCULATION, Map.of(
            "timestamp", Instant.now(),
            "rangeStart", 0,
            "rangeLength", fileSize
        ));

        // Log dimension calculation
        MerkleShape geometry = MerkleShape.fromContentSize(fileSize);
        logEvent(MerkleCreationStage.DIMENSION_CALCULATION, Map.of(
            "timestamp", Instant.now(),
            "leafCount", geometry.getLeafCount(),
            "nodeCount", geometry.getNodeCount()
        ));

        // Log progress creation
        logEvent(MerkleCreationStage.PROGRESS_CREATION, Map.of(
            "timestamp", Instant.now(),
            "totalChunks", geometry.getNodeCount(),
            "totalBytes", fileSize
        ));

        // Prepare for MerkleTree creation
        logEvent(MerkleCreationStage.FILE_PREPARATION, Map.of(
            "timestamp", Instant.now(),
            "chunkSize", CHUNK_SIZE
        ));

        // Create the MerkleTree
        logEvent(MerkleCreationStage.TREE_CREATION_STARTED, Map.of(
            "timestamp", Instant.now()
        ));

        // Create a progress tracker
        final long[] lastProgressUpdate = {System.currentTimeMillis()};

        // Create the MerkleTree from the file
        MerkleRefBuildProgress progress = MerkleRefFactory.fromData(
            largeFvecsFilePath
        );

        // Add a progress listener
        progress.getFuture().thenAccept(tree -> {
            logEvent(MerkleCreationStage.TREE_CREATION_COMPLETED, Map.of(
                "timestamp", Instant.now(),
                "leafCount", tree.getShape().getLeafCount()
            ));
        });

        // Monitor progress
        while (!progress.getFuture().isDone()) {
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastProgressUpdate[0] > 5000) { // Log every 5 seconds
                int processedChunks = progress.getProcessedChunks();
                int totalChunks = progress.getTotalChunks();
                double progressPercent = (double) processedChunks / totalChunks * 100;

                logEvent(MerkleCreationStage.TREE_CREATION_PROGRESS, Map.of(
                    "timestamp", Instant.now(),
                    "processedChunks", processedChunks,
                    "totalChunks", totalChunks,
                    "progressPercent", progressPercent,
                    "phase", progress.getPhase()
                ));

                lastProgressUpdate[0] = currentTime;
            }

            // Sleep a bit to avoid busy waiting
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // Get the completed MerkleTree
        MerkleDataImpl merkleTree = progress.getFuture().get();

        // Save the MerkleTree to a file
        Path merkleFilePath = largeFvecsFilePath.resolveSibling(largeFvecsFilePath.getFileName() + ".mref");

        logEvent(MerkleCreationStage.TREE_SAVE_STARTED, Map.of(
            "timestamp", Instant.now(),
            "merkleFilePath", merkleFilePath.toString()
        ));

        merkleTree.save(merkleFilePath);

        logEvent(MerkleCreationStage.TREE_SAVE_COMPLETED, Map.of(
            "timestamp", Instant.now(),
            "merkleFileSize", Files.size(merkleFilePath)
        ));

        // Verify the MerkleTree file by loading it
        logEvent(MerkleCreationStage.TREE_VERIFICATION_STARTED, Map.of(
            "timestamp", Instant.now()
        ));

        // Load the tree to verify it, but skip post-write verification since we just saved it
        // This avoids the redundant and expensive verification process when we know the file is valid
        MerkleDataImpl loadedTree = MerkleRefFactory.load(merkleFilePath);

        // Verify the loaded tree has the same number of leaves
        if (loadedTree.getShape().getLeafCount() != merkleTree.getShape().getLeafCount()) {
            throw new AssertionError("Loaded tree has different number of leaves: " + 
                loadedTree.getShape().getLeafCount() + " vs " + merkleTree.getShape().getLeafCount());
        }

        logEvent(MerkleCreationStage.TREE_VERIFICATION_COMPLETED, Map.of(
            "timestamp", Instant.now()
        ));

        // Record the end time
        Instant endTime = Instant.now();

        // Get final memory usage
        MemoryUsage heapAfter = memoryMXBean.getHeapMemoryUsage();
        MemoryUsage nonHeapAfter = memoryMXBean.getNonHeapMemoryUsage();

        // Get final CPU time
        long endCpuTime = threadMXBean.isCurrentThreadCpuTimeSupported() ? 
            threadMXBean.getCurrentThreadCpuTime() : 0;

        // Calculate memory and CPU usage
        long heapUsed = heapAfter.getUsed() - heapBefore.getUsed();
        long nonHeapUsed = nonHeapAfter.getUsed() - nonHeapBefore.getUsed();
        long cpuTimeNanos = endCpuTime - startCpuTime;

        // Log the complete event with profiling data
        Map<String, Object> completeParams = new HashMap<>();
        completeParams.put("timestamp", endTime);
        completeParams.put("totalDurationMs", Duration.between(startTime, endTime).toMillis());
        completeParams.put("heapMemoryUsed", heapUsed);
        completeParams.put("nonHeapMemoryUsed", nonHeapUsed);
        completeParams.put("cpuTimeNanos", cpuTimeNanos);

        logEvent(MerkleCreationStage.COMPLETE, completeParams);

        // Generate and print the performance report
        generatePerformanceReport();

        // Print profiling summary
        System.out.println("\n[DEBUG_LOG] ===== PROFILING: MerkleTree Creation Performance Summary =====");
        System.out.println("[DEBUG_LOG] Total Duration: " + Duration.between(startTime, endTime).toMillis() + " ms");
        System.out.println("[DEBUG_LOG] Heap Memory Used: " + formatMemorySize(heapUsed) + 
            " (from " + formatMemorySize(heapBefore.getUsed()) + " to " + formatMemorySize(heapAfter.getUsed()) + ")");
        System.out.println("[DEBUG_LOG] Non-Heap Memory Used: " + formatMemorySize(nonHeapUsed) + 
            " (from " + formatMemorySize(nonHeapBefore.getUsed()) + " to " + formatMemorySize(nonHeapAfter.getUsed()) + ")");

        if (threadMXBean.isCurrentThreadCpuTimeSupported()) {
            double cpuTimeMs = cpuTimeNanos / 1_000_000.0;
            double cpuUtilization = cpuTimeMs / Duration.between(startTime, endTime).toMillis() * 100.0;
            System.out.println("[DEBUG_LOG] CPU Time: " + String.format("%.2f ms", cpuTimeMs));
            System.out.println("[DEBUG_LOG] CPU Utilization: " + String.format("%.2f%%", cpuUtilization));
        }

        System.out.println("[DEBUG_LOG] ===== PROFILING: End of Summary =====");

        // Stop JFR recording and save to file
        if (recording != null) {
            try {
                // Stop the recording
                recording.stop();

                // Create a unique filename with timestamp
                String timestamp = Instant.now().toString().replace(":", "-");
                jfrOutputPath = Paths.get(JFR_OUTPUT_DIR, JFR_FILENAME_PREFIX + "_" + timestamp + ".jfr");

                // Save the recording to a file
                recording.dump(jfrOutputPath);

                System.out.println("[DEBUG_LOG] ===== JFR: Recording saved to " + jfrOutputPath + " =====");
                System.out.println("[DEBUG_LOG] ===== JFR: To analyze this recording: =====");
                System.out.println("[DEBUG_LOG] 1. Download and install Java Mission Control (JMC) from https://www.oracle.com/java/technologies/jdk-mission-control.html");
                System.out.println("[DEBUG_LOG] 2. Open JMC and select File > Open File...");
                System.out.println("[DEBUG_LOG] 3. Navigate to " + jfrOutputPath + " and open it");
                System.out.println("[DEBUG_LOG] 4. Explore the Method Profiling tab for method-level performance analysis");
                System.out.println("[DEBUG_LOG] 5. Alternatively, use the jfr command-line tool: jfr print --events 'jdk.ExecutionSample' " + jfrOutputPath);

                // Close the recording to free resources
                recording.close();
            } catch (Exception e) {
                System.out.println("[DEBUG_LOG] ===== JFR: Failed to save recording: " + e.getMessage() + " =====");
                e.printStackTrace();
            }
        }

        System.out.println("[DEBUG_LOG] MerkleTree creation performance test completed");
    }

    /**
     * Logs an event with the specified event type and parameters.
     *
     * @param eventType The type of event
     * @param params The event parameters
     */
    private void logEvent(EventType eventType, Map<String, Object> params) {
        eventSink.addCustomEvent(eventType, params);

        // Also log to console for debugging
        System.out.println("[DEBUG_LOG] Event: " + eventType + ", Params: " + params);
    }

    /**
     * Generates and prints a performance report based on the logged events.
     */
    private void generatePerformanceReport() {
        System.out.println("\n[DEBUG_LOG] ===== MerkleTree Creation Performance Report =====");

        // Extract timing information for each stage
        Map<String, Duration> stageDurations = new LinkedHashMap<>();

        // Get events for each stage
        Map<MerkleCreationStage, List<Map<String, Object>>> eventsByStage = new HashMap<>();
        for (MerkleCreationStage stage : MerkleCreationStage.values()) {
            eventsByStage.put(stage, eventSink.getEventsByType(stage));
        }

        // Calculate durations between stages
        calculateStageDuration(eventsByStage, MerkleCreationStage.START, MerkleCreationStage.FILE_EXISTENCE_CHECK, 
            "File Existence Check", stageDurations);

        calculateStageDuration(eventsByStage, MerkleCreationStage.FILE_EXISTENCE_CHECK, MerkleCreationStage.FILE_SIZE_CALCULATION, 
            "File Size Calculation", stageDurations);

        calculateStageDuration(eventsByStage, MerkleCreationStage.FILE_SIZE_CALCULATION, MerkleCreationStage.RANGE_CALCULATION, 
            "Range Calculation", stageDurations);

        calculateStageDuration(eventsByStage, MerkleCreationStage.RANGE_CALCULATION, MerkleCreationStage.DIMENSION_CALCULATION, 
            "Dimension Calculation", stageDurations);

        calculateStageDuration(eventsByStage, MerkleCreationStage.DIMENSION_CALCULATION, MerkleCreationStage.PROGRESS_CREATION, 
            "Progress Creation", stageDurations);

        calculateStageDuration(eventsByStage, MerkleCreationStage.PROGRESS_CREATION, MerkleCreationStage.FILE_PREPARATION, 
            "Final File Preparation", stageDurations);

        calculateStageDuration(eventsByStage, MerkleCreationStage.START, MerkleCreationStage.FILE_PREPARATION, 
            "Total File Preparation", stageDurations);

        calculateStageDuration(eventsByStage, MerkleCreationStage.FILE_PREPARATION, MerkleCreationStage.TREE_CREATION_STARTED, 
            "Tree Creation Setup", stageDurations);

        calculateStageDuration(eventsByStage, MerkleCreationStage.TREE_CREATION_STARTED, MerkleCreationStage.TREE_CREATION_COMPLETED, 
            "Tree Creation", stageDurations);

        calculateStageDuration(eventsByStage, MerkleCreationStage.TREE_CREATION_COMPLETED, MerkleCreationStage.TREE_SAVE_STARTED, 
            "Pre-Save Processing", stageDurations);

        calculateStageDuration(eventsByStage, MerkleCreationStage.TREE_SAVE_STARTED, MerkleCreationStage.TREE_SAVE_COMPLETED, 
            "Tree Saving", stageDurations);

        calculateStageDuration(eventsByStage, MerkleCreationStage.TREE_SAVE_COMPLETED, MerkleCreationStage.TREE_VERIFICATION_STARTED, 
            "Pre-Verification Processing", stageDurations);

        calculateStageDuration(eventsByStage, MerkleCreationStage.TREE_VERIFICATION_STARTED, MerkleCreationStage.TREE_VERIFICATION_COMPLETED, 
            "Tree Verification", stageDurations);

        calculateStageDuration(eventsByStage, MerkleCreationStage.START, MerkleCreationStage.COMPLETE, 
            "Total Duration", stageDurations);

        // Print the report
        System.out.println("[DEBUG_LOG] Stage Durations:");
        stageDurations.forEach((stage, duration) -> {
            System.out.println(String.format("[DEBUG_LOG] %s: %d ms (%.2f seconds)", 
                stage, duration.toMillis(), duration.toMillis() / 1000.0));
        });

        // Print file information
        if (!eventsByStage.get(MerkleCreationStage.START).isEmpty() && 
            !eventsByStage.get(MerkleCreationStage.TREE_SAVE_COMPLETED).isEmpty()) {

            Map<String, Object> startEvent = eventsByStage.get(MerkleCreationStage.START).get(0);
            Map<String, Object> treeSaveCompletedEvent = eventsByStage.get(MerkleCreationStage.TREE_SAVE_COMPLETED).get(0);

            System.out.println("[DEBUG_LOG] File Information:");
            System.out.println(String.format("[DEBUG_LOG] Input File: %s", startEvent.get("file")));
            System.out.println(String.format("[DEBUG_LOG] Input File Size: %.2f MB", 
                ((Number) startEvent.get("fileSize")).longValue() / (1024.0 * 1024.0)));
            System.out.println(String.format("[DEBUG_LOG] Merkle File Size: %.2f MB", 
                ((Number) treeSaveCompletedEvent.get("merkleFileSize")).longValue() / (1024.0 * 1024.0)));
            System.out.println(String.format("[DEBUG_LOG] Chunk Size: %.2f MB", 
                CHUNK_SIZE / (1024.0 * 1024.0)));
        }

        System.out.println("[DEBUG_LOG] ================================================\n");
    }

    /**
     * Formats a memory size in bytes to a human-readable string.
     *
     * @param bytes The memory size in bytes
     * @return A human-readable string representation of the memory size
     */
    private String formatMemorySize(long bytes) {
        final long kilobyte = 1024;
        final long megabyte = kilobyte * 1024;
        final long gigabyte = megabyte * 1024;

        if (bytes >= gigabyte) {
            return String.format("%.2f GB", (double) bytes / gigabyte);
        } else if (bytes >= megabyte) {
            return String.format("%.2f MB", (double) bytes / megabyte);
        } else if (bytes >= kilobyte) {
            return String.format("%.2f KB", (double) bytes / kilobyte);
        } else {
            return bytes + " bytes";
        }
    }

    /**
     * Calculates the duration between two stages and adds it to the stage durations map.
     *
     * @param eventsByStage Map of events by stage
     * @param startStage The starting stage
     * @param endStage The ending stage
     * @param stageName The name of the stage for reporting
     * @param stageDurations The map to add the duration to
     */
    private void calculateStageDuration(
            Map<MerkleCreationStage, List<Map<String, Object>>> eventsByStage,
            MerkleCreationStage startStage,
            MerkleCreationStage endStage,
            String stageName,
            Map<String, Duration> stageDurations) {

        List<Map<String, Object>> startEvents = eventsByStage.get(startStage);
        List<Map<String, Object>> endEvents = eventsByStage.get(endStage);

        if (startEvents != null && !startEvents.isEmpty() && endEvents != null && !endEvents.isEmpty()) {
            Instant startTime = (Instant) startEvents.get(0).get("timestamp");
            Instant endTime = (Instant) endEvents.get(0).get("timestamp");

            Duration duration = Duration.between(startTime, endTime);
            stageDurations.put(stageName, duration);
        }
    }
}
