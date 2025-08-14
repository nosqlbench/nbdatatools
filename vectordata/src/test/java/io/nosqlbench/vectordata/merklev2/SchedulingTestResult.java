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
import java.util.stream.Collectors;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

///
/// Contains the results of testing a scheduler and provides assertion methods
/// for verifying expected behavior.
/// 
/// This class encapsulates all the information about how a scheduler performed
/// on a specific test scenario, including efficiency metrics, coverage analysis,
/// and detailed decision breakdown. It provides fluent assertion methods that
/// can be chained together for comprehensive verification.
///
/// Example usage:
/// ```java
/// result.assertEfficiencyAbove(0.8)
///       .assertCoverageComplete()
///       .assertMaxOverDownload(20)
///       .assertReasonUsed(SchedulingReason.EFFICIENT_COVERAGE);
/// ```
///
public class SchedulingTestResult {
    private final String schedulerName;
    private final TestScenario scenario;
    private final long readOffset;
    private final int readLength;
    private final List<Integer> requiredChunks;
    private final List<SchedulingDecision> decisions;
    
    public SchedulingTestResult(
            String schedulerName,
            TestScenario scenario,
            long readOffset,
            int readLength,
            List<Integer> requiredChunks,
            List<SchedulingDecision> decisions) {
        this.schedulerName = schedulerName;
        this.scenario = scenario;
        this.readOffset = readOffset;
        this.readLength = readLength;
        this.requiredChunks = List.copyOf(requiredChunks);
        this.decisions = List.copyOf(decisions);
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
    /// Gets the test scenario that was used.
    ///
    /// @return The test scenario
    ///
    public TestScenario getScenario() {
        return scenario;
    }
    
    ///
    /// Gets the scheduling decisions made by the scheduler.
    ///
    /// @return List of scheduling decisions
    ///
    public List<SchedulingDecision> getDecisions() {
        return decisions;
    }
    
    ///
    /// Gets the chunks that were required to be downloaded.
    ///
    /// @return List of required chunk indices
    ///
    public List<Integer> getRequiredChunks() {
        return requiredChunks;
    }
    
    ///
    /// Gets all chunks that will be downloaded by the scheduled nodes.
    ///
    /// @return Set of all chunk indices that will be downloaded
    ///
    public Set<Integer> getCoveredChunks() {
        return decisions.stream()
            .flatMap(decision -> decision.coveredChunks().stream())
            .collect(Collectors.toSet());
    }
    
    ///
    /// Calculates the overall efficiency of the scheduling decisions.
    /// 
    /// Efficiency is the ratio of required chunks to covered chunks.
    /// Perfect efficiency (1.0) means all downloaded chunks are needed.
    ///
    /// @return Efficiency ratio between 0.0 and 1.0
    ///
    public double calculateOverallEfficiency() {
        Set<Integer> covered = getCoveredChunks();
        if (covered.isEmpty()) {
            return requiredChunks.isEmpty() ? 1.0 : 0.0;
        }
        
        long neededCount = covered.stream()
            .mapToLong(chunk -> requiredChunks.contains(chunk) ? 1 : 0)
            .sum();
            
        return (double) neededCount / covered.size();
    }
    
    ///
    /// Calculates the coverage completeness.
    /// 
    /// Coverage is the ratio of required chunks that are covered by decisions
    /// to the total required chunks.
    ///
    /// @return Coverage ratio between 0.0 and 1.0
    ///
    public double calculateCoverage() {
        if (requiredChunks.isEmpty()) {
            return 1.0; // Perfect coverage when nothing is required
        }
        
        Set<Integer> covered = getCoveredChunks();
        long coveredRequired = requiredChunks.stream()
            .mapToLong(chunk -> covered.contains(chunk) ? 1 : 0)
            .sum();
            
        return (double) coveredRequired / requiredChunks.size();
    }
    
    ///
    /// Calculates the total bytes that will be downloaded.
    ///
    /// @return Total download size in bytes
    ///
    public long calculateTotalDownloadBytes() {
        return decisions.stream()
            .mapToLong(SchedulingDecision::estimatedBytes)
            .sum();
    }
    
    ///
    /// Calculates the number of excess chunks downloaded.
    ///
    /// @return Number of chunks downloaded that weren't required
    ///
    public int calculateOverDownloadChunks() {
        Set<Integer> covered = getCoveredChunks();
        return (int) covered.stream()
            .mapToLong(chunk -> requiredChunks.contains(chunk) ? 0 : 1)
            .sum();
    }
    
    // Assertion methods for test verification
    
    ///
    /// Asserts that the overall efficiency is above the specified threshold.
    ///
    /// @param minEfficiency Minimum acceptable efficiency (0.0 to 1.0)
    /// @return This result for method chaining
    /// @throws AssertionError if efficiency is below threshold
    ///
    public SchedulingTestResult assertEfficiencyAbove(double minEfficiency) {
        double actual = calculateOverallEfficiency();
        assertTrue(actual >= minEfficiency,
            String.format("Efficiency %.3f is below threshold %.3f", actual, minEfficiency));
        return this;
    }
    
    ///
    /// Asserts that coverage is complete (all required chunks are covered).
    ///
    /// @return This result for method chaining
    /// @throws AssertionError if coverage is incomplete
    ///
    public SchedulingTestResult assertCoverageComplete() {
        double coverage = calculateCoverage();
        assertEquals(1.0, coverage, 0.001, "Coverage should be complete");
        return this;
    }
    
    ///
    /// Asserts that coverage is at least the specified ratio.
    ///
    /// @param minCoverage Minimum acceptable coverage (0.0 to 1.0)
    /// @return This result for method chaining
    /// @throws AssertionError if coverage is below threshold
    ///
    public SchedulingTestResult assertCoverageAbove(double minCoverage) {
        double actual = calculateCoverage();
        assertTrue(actual >= minCoverage,
            String.format("Coverage %.3f is below threshold %.3f", actual, minCoverage));
        return this;
    }
    
    ///
    /// Asserts that the number of over-downloaded chunks is within limit.
    ///
    /// @param maxOverDownload Maximum allowed over-download chunks
    /// @return This result for method chaining
    /// @throws AssertionError if over-download exceeds limit
    ///
    public SchedulingTestResult assertMaxOverDownload(int maxOverDownload) {
        int actual = calculateOverDownloadChunks();
        assertTrue(actual <= maxOverDownload,
            String.format("Over-download %d chunks exceeds limit %d", actual, maxOverDownload));
        return this;
    }
    
    ///
    /// Asserts that no unnecessary chunks are downloaded.
    ///
    /// @return This result for method chaining
    /// @throws AssertionError if any unnecessary chunks are downloaded
    ///
    public SchedulingTestResult assertNoOverDownload() {
        return assertMaxOverDownload(0);
    }
    
    ///
    /// Asserts that a specific scheduling reason was used.
    ///
    /// @param reason The expected scheduling reason
    /// @return This result for method chaining
    /// @throws AssertionError if the reason was not used
    ///
    public SchedulingTestResult assertReasonUsed(SchedulingReason reason) {
        boolean found = decisions.stream()
            .anyMatch(decision -> decision.reason() == reason);
        assertTrue(found, "Expected scheduling reason " + reason + " was not used");
        return this;
    }
    
    ///
    /// Asserts that a specific scheduling reason was NOT used.
    ///
    /// @param reason The scheduling reason that should not be used
    /// @return This result for method chaining
    /// @throws AssertionError if the reason was used
    ///
    public SchedulingTestResult assertReasonNotUsed(SchedulingReason reason) {
        boolean found = decisions.stream()
            .anyMatch(decision -> decision.reason() == reason);
        assertFalse(found, "Unexpected scheduling reason " + reason + " was used");
        return this;
    }
    
    ///
    /// Asserts that the number of decisions is within the expected range.
    ///
    /// @param minDecisions Minimum number of expected decisions
    /// @param maxDecisions Maximum number of expected decisions
    /// @return This result for method chaining
    /// @throws AssertionError if decision count is outside range
    ///
    public SchedulingTestResult assertDecisionCount(int minDecisions, int maxDecisions) {
        int actual = decisions.size();
        assertTrue(actual >= minDecisions && actual <= maxDecisions,
            String.format("Decision count %d is outside range [%d, %d]", 
                actual, minDecisions, maxDecisions));
        return this;
    }
    
    ///
    /// Asserts that leaf nodes are preferred for small requests.
    /// 
    /// This checks that when only a few chunks are needed, the scheduler
    /// prefers individual leaf nodes over larger internal nodes.
    ///
    /// @return This result for method chaining
    /// @throws AssertionError if leaf nodes are not preferred appropriately
    ///
    public SchedulingTestResult assertLeafNodesPreferred() {
        if (requiredChunks.size() <= 2) {
            long leafDecisions = decisions.stream()
                .mapToLong(decision -> scenario.getShape().isLeafNode(decision.nodeIndex()) ? 1 : 0)
                .sum();
            assertTrue(leafDecisions > 0, "Expected leaf nodes for small requests");
        }
        return this;
    }
    
    ///
    /// Asserts that internal nodes are used for larger requests.
    /// 
    /// This checks that when many chunks are needed, the scheduler
    /// attempts to use internal nodes for efficiency.
    ///
    /// @return This result for method chaining
    /// @throws AssertionError if internal nodes are not used appropriately
    ///
    public SchedulingTestResult assertInternalNodesUsed() {
        if (requiredChunks.size() > 5) {
            long internalDecisions = decisions.stream()
                .mapToLong(decision -> !scenario.getShape().isLeafNode(decision.nodeIndex()) ? 1 : 0)
                .sum();
            assertTrue(internalDecisions > 0, "Expected internal nodes for large requests");
        }
        return this;
    }
    
    ///
    /// Creates a detailed string representation of the test results.
    ///
    /// @return Detailed analysis string for debugging and reporting
    ///
    public String toDetailedString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SchedulingTestResult for ").append(schedulerName).append("\n");
        sb.append("Scenario: ").append(scenario.getDescription()).append("\n");
        sb.append("Read Request: offset=").append(readOffset).append(", length=").append(readLength).append("\n");
        sb.append("Required Chunks: ").append(requiredChunks).append("\n");
        sb.append("Metrics:\n");
        sb.append("  Efficiency: ").append(String.format("%.3f", calculateOverallEfficiency())).append("\n");
        sb.append("  Coverage: ").append(String.format("%.3f", calculateCoverage())).append("\n");
        sb.append("  Download Bytes: ").append(calculateTotalDownloadBytes()).append("\n");
        sb.append("  Over-download: ").append(calculateOverDownloadChunks()).append(" chunks\n");
        sb.append("Decisions (").append(decisions.size()).append("):\n");
        for (int i = 0; i < decisions.size(); i++) {
            sb.append("  ").append(i + 1).append(". ").append(decisions.get(i).toDebugString()).append("\n");
        }
        return sb.toString();
    }
}