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
import java.util.ArrayList;
import java.util.BitSet;

///
/// Test framework for verifying ChunkScheduler behavior and performance.
/// 
/// This framework provides comprehensive tools for testing scheduler implementations
/// by creating controlled test scenarios and providing detailed analysis of
/// scheduling decisions. It enables verification of correctness, efficiency,
/// and behavior under various conditions.
///
/// Key features:
/// - Scenario-based testing with controllable content size and chunk patterns
/// - Mock MerkleState for simulating various validity conditions
/// - Detailed analysis of scheduling decisions with metrics
/// - Support for concurrent stream testing
/// - Performance benchmarking capabilities
///
/// Example usage:
/// ```java
/// SchedulingTestFramework framework = new SchedulingTestFramework();
/// TestScenario scenario = framework.createScenario()
///     .withContentSize(10 * 1024 * 1024) // 10MB
///     .withChunkSize(1024 * 1024)       // 1MB chunks
///     .withInvalidChunks(2, 5, 8)       // Mark specific chunks as invalid
///     .build();
/// 
/// SchedulingTestResult result = framework.testScheduler(
///     new AggressiveChunkScheduler(), 
///     scenario, 
///     1024, 2048  // Test read from offset 1024, length 2048
/// );
/// 
/// result.assertEfficiencyAbove(0.8);
/// result.assertMaxOverDownload(20);
/// ```
///
public class SchedulingTestFramework {
    
    ///
    /// Creates a new test scenario builder.
    /// 
    /// @return A TestScenario.Builder for configuring test conditions
    ///
    public TestScenario.Builder createScenario() {
        return new TestScenario.Builder();
    }
    
    ///
    /// Tests a scheduler with the given scenario and read parameters.
    /// 
    /// This method executes the scheduler's decision-making process and
    /// provides detailed analysis of the results, including efficiency
    /// metrics, coverage analysis, and reasoning validation.
    ///
    /// @param scheduler The scheduler to test
    /// @param scenario The test scenario defining content and state
    /// @param offset The read offset to test
    /// @param length The read length to test
    /// @return Detailed test results with assertion methods
    ///
    public SchedulingTestResult testScheduler(
            ChunkScheduler scheduler,
            TestScenario scenario,
            long offset,
            int length) {
        
        // Execute the scheduler's analysis
        List<SchedulingDecision> decisions = scheduler.analyzeSchedulingDecisions(
            offset, length, scenario.getShape(), scenario.getState()
        );
        
        // Calculate the required chunks for this read
        int startChunk = scenario.getShape().getChunkIndexForPosition(offset);
        int endChunk = scenario.getShape().getChunkIndexForPosition(
            Math.min(offset + length - 1, scenario.getShape().getTotalContentSize() - 1));
        
        List<Integer> requiredChunks = new ArrayList<>();
        for (int chunk = startChunk; chunk <= endChunk; chunk++) {
            if (!scenario.getState().isValid(chunk)) {
                requiredChunks.add(chunk);
            }
        }
        
        return new SchedulingTestResult(
            scheduler.getClass().getSimpleName(),
            scenario,
            offset,
            length,
            requiredChunks,
            decisions
        );
    }
    
    ///
    /// Tests scheduler behavior with concurrent stream interference.
    /// 
    /// This method simulates multiple concurrent read requests to verify
    /// that the scheduler properly handles overlapping requirements and
    /// avoids unnecessary duplicate downloads.
    ///
    /// @param scheduler The scheduler to test
    /// @param scenario The test scenario
    /// @param streams List of concurrent read requests (offset, length pairs)
    /// @return Analysis of how well the scheduler handles concurrent streams
    ///
    public ConcurrentStreamTestResult testConcurrentStreams(
            ChunkScheduler scheduler,
            TestScenario scenario,
            List<ReadRequest> streams) {
        
        List<SchedulingTestResult> individualResults = new ArrayList<>();
        
        // Test each stream individually
        for (ReadRequest request : streams) {
            SchedulingTestResult result = testScheduler(
                scheduler, scenario, request.offset(), request.length()
            );
            individualResults.add(result);
        }
        
        return new ConcurrentStreamTestResult(
            scheduler.getClass().getSimpleName(),
            scenario,
            streams,
            individualResults
        );
    }
    
    ///
    /// Creates a mock MerkleState for testing purposes.
    /// 
    /// @param shape The MerkleShape to use
    /// @param validChunks BitSet indicating which chunks are valid
    /// @return A MockMerkleState for testing
    ///
    public MockMerkleState createMockState(MerkleShape shape, BitSet validChunks) {
        return new MockMerkleState(shape, validChunks);
    }
    
    ///
    /// Creates a mock MerkleState with all chunks invalid.
    /// 
    /// @param shape The MerkleShape to use
    /// @return A MockMerkleState with no valid chunks
    ///
    public MockMerkleState createEmptyState(MerkleShape shape) {
        return new MockMerkleState(shape, new BitSet());
    }
    
    ///
    /// Represents a read request for concurrent stream testing.
    ///
    public static class ReadRequest {
        /// The starting byte offset
        private final long offset;
        /// The number of bytes to read
        private final int length;
        
        ///
        /// Creates a read request.
        ///
        /// @param offset Starting byte offset (must be non-negative)
        /// @param length Number of bytes to read (must be positive)
        /// @throws IllegalArgumentException if parameters are invalid
        ///
        public ReadRequest(long offset, int length) {
            if (offset < 0) {
                throw new IllegalArgumentException("Offset cannot be negative: " + offset);
            }
            if (length <= 0) {
                throw new IllegalArgumentException("Length must be positive: " + length);
            }
            this.offset = offset;
            this.length = length;
        }
        
        public long offset() { return offset; }
        public int length() { return length; }
        
        ///
        /// Gets the ending byte position (exclusive).
        ///
        /// @return The end position of this read request
        ///
        public long getEndPosition() {
            return offset + length;
        }
        
        ///
        /// Checks if this request overlaps with another request.
        ///
        /// @param other The other read request
        /// @return true if the requests overlap
        ///
        public boolean overlaps(ReadRequest other) {
            return offset < other.getEndPosition() && getEndPosition() > other.offset;
        }
    }
}