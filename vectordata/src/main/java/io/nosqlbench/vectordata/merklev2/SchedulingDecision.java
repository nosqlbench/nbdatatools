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

///
/// Records a scheduling decision made by a ChunkScheduler, including
/// full traceability information for testing and debugging.
///
/// This record captures both the what (which nodes were selected) and
/// the why (reasoning behind the selection) of scheduling decisions,
/// enabling comprehensive verification of scheduler behavior.
///
/// @param nodeIndex The merkle node selected for download
/// @param reason The reasoning category for this selection
/// @param priority The calculated priority of this download task
/// @param estimatedBytes The estimated number of bytes this node covers
/// @param requiredChunks List of chunk indices actually needed by the request
/// @param coveredChunks List of chunk indices this node will download
/// @param explanation Human-readable explanation of the decision
///
public record SchedulingDecision(
    int nodeIndex,
    SchedulingReason reason,
    int priority,
    long estimatedBytes,
    List<Integer> requiredChunks,
    List<Integer> coveredChunks,
    String explanation
) {
    
    ///
    /// Calculates the efficiency of this scheduling decision.
    /// 
    /// Efficiency is the ratio of required chunks to covered chunks.
    /// A value of 1.0 means perfect efficiency (all downloaded chunks are needed).
    /// A value of 0.5 means 50% efficiency (half the downloaded chunks are wasted).
    ///
    /// @return Efficiency ratio between 0.0 and 1.0
    ///
    public double calculateEfficiency() {
        if (coveredChunks.isEmpty()) {
            return 0.0;
        }
        
        // Count how many covered chunks are actually required
        long neededChunks = coveredChunks.stream()
            .mapToLong(chunk -> requiredChunks.contains(chunk) ? 1 : 0)
            .sum();
            
        return (double) neededChunks / coveredChunks.size();
    }
    
    ///
    /// Calculates the coverage ratio of this decision.
    /// 
    /// Coverage is the ratio of required chunks covered by this decision
    /// to the total required chunks in the original request.
    ///
    /// @return Coverage ratio between 0.0 and 1.0
    ///
    public double calculateCoverage() {
        if (requiredChunks.isEmpty()) {
            return 0.0;
        }
        
        // Count how many required chunks this decision covers
        long coveredRequired = requiredChunks.stream()
            .mapToLong(chunk -> coveredChunks.contains(chunk) ? 1 : 0)
            .sum();
            
        return (double) coveredRequired / requiredChunks.size();
    }
    
    ///
    /// Creates a debugging string representation of this decision.
    ///
    /// @return Detailed string suitable for logging and debugging
    ///
    public String toDebugString() {
        return String.format(
            "SchedulingDecision{node=%d, reason=%s, priority=%d, " +
            "bytes=%d, efficiency=%.2f, coverage=%.2f, explanation='%s'}",
            nodeIndex, reason, priority, estimatedBytes,
            calculateEfficiency(), calculateCoverage(), explanation
        );
    }
}