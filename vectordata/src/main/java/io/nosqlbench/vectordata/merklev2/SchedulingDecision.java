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
import java.util.Objects;

///
/// Records a scheduling decision made by a ChunkScheduler, including
/// full traceability information for testing and debugging.
///
/// This class captures both the what (which nodes were selected) and
/// the why (reasoning behind the selection) of scheduling decisions,
/// enabling comprehensive verification of scheduler behavior.
///
public class SchedulingDecision {
    private final int nodeIndex;
    private final SchedulingReason reason;
    private final int priority;
    private final long estimatedBytes;
    private final List<Integer> requiredChunks;
    private final List<Integer> coveredChunks;
    private final String explanation;

    public SchedulingDecision(int nodeIndex, SchedulingReason reason, int priority,
                            long estimatedBytes, List<Integer> requiredChunks,
                            List<Integer> coveredChunks, String explanation) {
        this.nodeIndex = nodeIndex;
        this.reason = reason;
        this.priority = priority;
        this.estimatedBytes = estimatedBytes;
        this.requiredChunks = requiredChunks;
        this.coveredChunks = coveredChunks;
        this.explanation = explanation;
    }

    public int nodeIndex() {
        return nodeIndex;
    }

    public SchedulingReason reason() {
        return reason;
    }

    public int priority() {
        return priority;
    }

    public long estimatedBytes() {
        return estimatedBytes;
    }

    public List<Integer> requiredChunks() {
        return requiredChunks;
    }

    public List<Integer> coveredChunks() {
        return coveredChunks;
    }

    public String explanation() {
        return explanation;
    }
    
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchedulingDecision that = (SchedulingDecision) o;
        return nodeIndex == that.nodeIndex && priority == that.priority &&
               estimatedBytes == that.estimatedBytes &&
               Objects.equals(reason, that.reason) &&
               Objects.equals(requiredChunks, that.requiredChunks) &&
               Objects.equals(coveredChunks, that.coveredChunks) &&
               Objects.equals(explanation, that.explanation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeIndex, reason, priority, estimatedBytes, requiredChunks, coveredChunks, explanation);
    }

    @Override
    public String toString() {
        return "SchedulingDecision{" +
               "nodeIndex=" + nodeIndex +
               ", reason=" + reason +
               ", priority=" + priority +
               ", estimatedBytes=" + estimatedBytes +
               ", requiredChunks=" + requiredChunks +
               ", coveredChunks=" + coveredChunks +
               ", explanation='" + explanation + '\'' +
               '}';
    }
}