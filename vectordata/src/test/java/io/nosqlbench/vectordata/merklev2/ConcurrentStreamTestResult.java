package io.nosqlbench.vectordata.merklev2;

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

import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

///
/// Contains the results of testing scheduler behavior with concurrent streams.
/// 
/// This class analyzes how well a scheduler handles multiple concurrent read
/// requests, focusing on duplicate download avoidance, resource efficiency,
/// and overall stream interference patterns.
///
/// The analysis considers:
/// - Overlap detection between concurrent streams
/// - Duplicate download identification
/// - Resource utilization efficiency
/// - Stream isolation effectiveness
///
public class ConcurrentStreamTestResult {
    private final String schedulerName;
    private final TestScenario scenario;
    private final List<SchedulingTestFramework.ReadRequest> streams;
    private final List<SchedulingTestResult> individualResults;
    
    public ConcurrentStreamTestResult(
            String schedulerName,
            TestScenario scenario,
            List<SchedulingTestFramework.ReadRequest> streams,
            List<SchedulingTestResult> individualResults) {
        this.schedulerName = schedulerName;
        this.scenario = scenario;
        this.streams = List.copyOf(streams);
        this.individualResults = List.copyOf(individualResults);
    }
    
    ///
    /// Gets the name of the tested scheduler.
    ///
    /// @return Scheduler class name
    ///
    public String getSchedulerName() {
        return schedulerName;
    }
    
    ///
    /// Gets the test scenario.
    ///
    /// @return The test scenario used
    ///
    public TestScenario getScenario() {
        return scenario;
    }
    
    ///
    /// Gets the concurrent read requests that were tested.
    ///
    /// @return List of read requests
    ///
    public List<SchedulingTestFramework.ReadRequest> getStreams() {
        return streams;
    }
    
    ///
    /// Gets the individual test results for each stream.
    ///
    /// @return List of individual scheduling results
    ///
    public List<SchedulingTestResult> getIndividualResults() {
        return individualResults;
    }
    
    ///
    /// Identifies overlapping streams that access the same chunks.
    ///
    /// @return Map of stream pairs to their overlapping chunks
    ///
    public Map<StreamPair, Set<Integer>> findOverlappingStreams() {
        Map<StreamPair, Set<Integer>> overlaps = new HashMap<>();
        
        for (int i = 0; i < streams.size(); i++) {
            for (int j = i + 1; j < streams.size(); j++) {
                SchedulingTestFramework.ReadRequest stream1 = streams.get(i);
                SchedulingTestFramework.ReadRequest stream2 = streams.get(j);
                
                if (stream1.overlaps(stream2)) {
                    Set<Integer> chunks1 = individualResults.get(i).getRequiredChunks().stream()
                        .collect(Collectors.toSet());
                    Set<Integer> chunks2 = individualResults.get(j).getRequiredChunks().stream()
                        .collect(Collectors.toSet());
                    
                    Set<Integer> commonChunks = new HashSet<>(chunks1);
                    commonChunks.retainAll(chunks2);
                    
                    if (!commonChunks.isEmpty()) {
                        overlaps.put(new StreamPair(i, j), commonChunks);
                    }
                }
            }
        }
        
        return overlaps;
    }
    
    ///
    /// Calculates the total number of unique chunks that would be downloaded.
    ///
    /// @return Number of unique chunks across all streams
    ///
    public int calculateTotalUniqueChunks() {
        return individualResults.stream()
            .flatMap(result -> result.getCoveredChunks().stream())
            .collect(Collectors.toSet())
            .size();
    }
    
    ///
    /// Calculates the total number of chunk downloads if streams were isolated.
    ///
    /// @return Sum of chunk downloads from all individual streams
    ///
    public int calculateIsolatedChunkDownloads() {
        return individualResults.stream()
            .mapToInt(result -> result.getCoveredChunks().size())
            .sum();
    }
    
    ///
    /// Calculates the duplicate download ratio.
    /// 
    /// This measures how much redundant downloading would occur if streams
    /// were processed independently versus optimally coordinated.
    ///
    /// @return Ratio of isolated downloads to unique downloads (1.0 = no duplication)
    ///
    public double calculateDuplicationRatio() {
        int unique = calculateTotalUniqueChunks();
        int isolated = calculateIsolatedChunkDownloads();
        
        if (unique == 0) {
            return isolated == 0 ? 1.0 : Double.POSITIVE_INFINITY;
        }
        
        return (double) isolated / unique;
    }
    
    ///
    /// Calculates the average efficiency across all streams.
    ///
    /// @return Average efficiency of individual stream results
    ///
    public double calculateAverageEfficiency() {
        return individualResults.stream()
            .mapToDouble(SchedulingTestResult::calculateOverallEfficiency)
            .average()
            .orElse(0.0);
    }
    
    ///
    /// Calculates the total bytes that would be downloaded across all streams.
    ///
    /// @return Total download bytes if streams were processed independently
    ///
    public long calculateTotalDownloadBytes() {
        return individualResults.stream()
            .mapToLong(SchedulingTestResult::calculateTotalDownloadBytes)
            .sum();
    }
    
    // Assertion methods for concurrent stream testing
    
    ///
    /// Asserts that the duplication ratio is below the specified threshold.
    /// 
    /// A lower ratio indicates better coordination between concurrent streams.
    ///
    /// @param maxDuplication Maximum acceptable duplication ratio
    /// @return This result for method chaining
    /// @throws AssertionError if duplication exceeds threshold
    ///
    public ConcurrentStreamTestResult assertDuplicationBelow(double maxDuplication) {
        double actual = calculateDuplicationRatio();
        assertTrue(actual <= maxDuplication,
            String.format("Duplication ratio %.3f exceeds threshold %.3f", actual, maxDuplication));
        return this;
    }
    
    ///
    /// Asserts that streams with overlapping chunks show scheduling coordination.
    /// 
    /// This verifies that when streams access the same chunks, the scheduler
    /// attempts to coordinate downloads rather than treating them independently.
    ///
    /// @return This result for method chaining
    /// @throws AssertionError if coordination is not detected
    ///
    public ConcurrentStreamTestResult assertOverlapCoordination() {
        Map<StreamPair, Set<Integer>> overlaps = findOverlappingStreams();
        
        if (!overlaps.isEmpty()) {
            double duplicationRatio = calculateDuplicationRatio();
            assertTrue(duplicationRatio < 2.0,
                "Expected coordination for overlapping streams, but duplication ratio is " + duplicationRatio);
        }
        
        return this;
    }
    
    ///
    /// Asserts that each individual stream meets minimum efficiency requirements.
    ///
    /// @param minEfficiency Minimum efficiency for each stream
    /// @return This result for method chaining
    /// @throws AssertionError if any stream is below threshold
    ///
    public ConcurrentStreamTestResult assertAllStreamsEfficient(double minEfficiency) {
        for (int i = 0; i < individualResults.size(); i++) {
            double efficiency = individualResults.get(i).calculateOverallEfficiency();
            assertTrue(efficiency >= minEfficiency,
                String.format("Stream %d efficiency %.3f is below threshold %.3f", 
                    i, efficiency, minEfficiency));
        }
        return this;
    }
    
    ///
    /// Asserts that all streams achieve complete coverage.
    ///
    /// @return This result for method chaining
    /// @throws AssertionError if any stream has incomplete coverage
    ///
    public ConcurrentStreamTestResult assertAllStreamsCovered() {
        for (int i = 0; i < individualResults.size(); i++) {
            double coverage = individualResults.get(i).calculateCoverage();
            assertEquals(1.0, coverage, 0.001,
                String.format("Stream %d has incomplete coverage: %.3f", i, coverage));
        }
        return this;
    }
    
    ///
    /// Asserts that the specified number of stream overlaps exist.
    ///
    /// @param expectedOverlaps Expected number of overlapping stream pairs
    /// @return This result for method chaining
    /// @throws AssertionError if overlap count doesn't match
    ///
    public ConcurrentStreamTestResult assertOverlapCount(int expectedOverlaps) {
        int actualOverlaps = findOverlappingStreams().size();
        assertEquals(expectedOverlaps, actualOverlaps,
            String.format("Expected %d overlaps, found %d", expectedOverlaps, actualOverlaps));
        return this;
    }
    
    ///
    /// Creates a detailed analysis string of concurrent stream behavior.
    ///
    /// @return Detailed analysis for debugging and reporting
    ///
    public String toDetailedString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ConcurrentStreamTestResult for ").append(schedulerName).append("\n");
        sb.append("Scenario: ").append(scenario.getDescription()).append("\n");
        sb.append("Streams: ").append(streams.size()).append("\n");
        
        // Overall metrics
        sb.append("Overall Metrics:\n");
        sb.append("  Unique Chunks: ").append(calculateTotalUniqueChunks()).append("\n");
        sb.append("  Isolated Downloads: ").append(calculateIsolatedChunkDownloads()).append("\n");
        sb.append("  Duplication Ratio: ").append(String.format("%.3f", calculateDuplicationRatio())).append("\n");
        sb.append("  Average Efficiency: ").append(String.format("%.3f", calculateAverageEfficiency())).append("\n");
        sb.append("  Total Download Bytes: ").append(calculateTotalDownloadBytes()).append("\n");
        
        // Overlapping streams
        Map<StreamPair, Set<Integer>> overlaps = findOverlappingStreams();
        sb.append("Overlapping Streams: ").append(overlaps.size()).append("\n");
        for (Map.Entry<StreamPair, Set<Integer>> entry : overlaps.entrySet()) {
            StreamPair pair = entry.getKey();
            Set<Integer> chunks = entry.getValue();
            sb.append("  Stream ").append(pair.stream1).append(" & ").append(pair.stream2)
              .append(": ").append(chunks.size()).append(" common chunks ").append(chunks).append("\n");
        }
        
        // Individual stream results
        sb.append("Individual Stream Results:\n");
        for (int i = 0; i < individualResults.size(); i++) {
            SchedulingTestFramework.ReadRequest request = streams.get(i);
            SchedulingTestResult result = individualResults.get(i);
            sb.append("  Stream ").append(i).append(": offset=").append(request.offset())
              .append(", length=").append(request.length())
              .append(", efficiency=").append(String.format("%.3f", result.calculateOverallEfficiency()))
              .append(", coverage=").append(String.format("%.3f", result.calculateCoverage()))
              .append("\n");
        }
        
        return sb.toString();
    }
    
    ///
    /// Represents a pair of stream indices for overlap analysis.
    ///
    /// @param stream1 Index of the first stream
    /// @param stream2 Index of the second stream
    ///
    public record StreamPair(int stream1, int stream2) {
        
        public StreamPair {
            if (stream1 < 0 || stream2 < 0) {
                throw new IllegalArgumentException("Stream indices must be non-negative");
            }
            if (stream1 == stream2) {
                throw new IllegalArgumentException("Stream indices must be different");
            }
        }
        
        @Override
        public String toString() {
            return String.format("(%d, %d)", stream1, stream2);
        }
    }
}