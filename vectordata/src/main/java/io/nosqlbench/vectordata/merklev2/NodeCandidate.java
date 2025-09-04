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

import java.util.Set;

///
/// Represents a candidate merkle node for scheduling consideration.
/// 
/// This class encapsulates all the information needed to evaluate
/// whether a particular merkle node should be selected for download,
/// including coverage analysis, efficiency metrics, and reasoning.
///
public class NodeCandidate {
    /// The merkle tree node index
    private final int nodeIndex;
    /// Whether this is a leaf node or internal node
    private final boolean isLeafNode;
    /// Set of required chunks this node would cover
    private final Set<Integer> coveredRequiredChunks;
    /// Set of all chunks this node would download (including prefetch)
    private final Set<Integer> coveredAllChunks;
    /// Estimated download size for this node
    private final long estimatedBytes;
    /// Efficiency ratio (required chunks / all chunks)
    private final double efficiency;
    /// Human-readable explanation of this candidate
    private final String explanation;
    
    public NodeCandidate(int nodeIndex, boolean isLeafNode, Set<Integer> coveredRequiredChunks, Set<Integer> coveredAllChunks, long estimatedBytes, double efficiency, String explanation) {
        if (nodeIndex < 0) {
            throw new IllegalArgumentException("Node index cannot be negative: " + nodeIndex);
        }
        if (coveredRequiredChunks == null) {
            throw new IllegalArgumentException("Covered required chunks cannot be null");
        }
        if (coveredAllChunks == null) {
            throw new IllegalArgumentException("Covered all chunks cannot be null");
        }
        if (estimatedBytes < 0) {
            throw new IllegalArgumentException("Estimated bytes cannot be negative: " + estimatedBytes);
        }
        if (efficiency < 0.0 || efficiency > 1.0) {
            throw new IllegalArgumentException("Efficiency must be between 0.0 and 1.0: " + efficiency);
        }
        if (explanation == null || explanation.trim().isEmpty()) {
            throw new IllegalArgumentException("Explanation cannot be null or empty");
        }
        
        this.nodeIndex = nodeIndex;
        this.isLeafNode = isLeafNode;
        // Defensive copying of sets
        this.coveredRequiredChunks = Set.copyOf(coveredRequiredChunks);
        this.coveredAllChunks = Set.copyOf(coveredAllChunks);
        this.estimatedBytes = estimatedBytes;
        this.efficiency = efficiency;
        this.explanation = explanation;
    }
    
    /// @return The merkle tree node index
    public int nodeIndex() {
        return nodeIndex;
    }
    
    /// @return Whether this is a leaf node or internal node
    public boolean isLeafNode() {
        return isLeafNode;
    }
    
    /// @return Set of required chunks this node would cover
    public Set<Integer> coveredRequiredChunks() {
        return coveredRequiredChunks;
    }
    
    /// @return Set of all chunks this node would download (including prefetch)
    public Set<Integer> coveredAllChunks() {
        return coveredAllChunks;
    }
    
    /// @return Estimated download size for this node
    public long estimatedBytes() {
        return estimatedBytes;
    }
    
    /// @return Efficiency ratio (required chunks / all chunks)
    public double efficiency() {
        return efficiency;
    }
    
    /// @return Human-readable explanation of this candidate
    public String explanation() {
        return explanation;
    }
    
    
    ///
    /// Gets the number of required chunks this candidate would cover.
    ///
    /// @return Count of required chunks covered
    ///
    public int getRequiredChunkCount() {
        return coveredRequiredChunks.size();
    }
    
    ///
    /// Gets the total number of chunks this candidate would download.
    ///
    /// @return Count of all chunks that would be downloaded
    ///
    public int getTotalChunkCount() {
        return coveredAllChunks.size();
    }
    
    ///
    /// Gets the number of excess chunks (prefetch) this candidate would download.
    ///
    /// @return Count of chunks downloaded beyond what's required
    ///
    public int getExcessChunkCount() {
        return coveredAllChunks.size() - coveredRequiredChunks.size();
    }
    
    ///
    /// Checks if this candidate covers any of the specified chunks.
    ///
    /// @param chunks The chunks to check against
    /// @return true if this candidate covers any of the specified chunks
    ///
    public boolean coversAnyOf(Set<Integer> chunks) {
        return coveredAllChunks.stream().anyMatch(chunks::contains);
    }
    
    ///
    /// Checks if this candidate covers all of the specified chunks.
    ///
    /// @param chunks The chunks to check against
    /// @return true if this candidate covers all of the specified chunks
    ///
    public boolean coversAllOf(Set<Integer> chunks) {
        return coveredAllChunks.containsAll(chunks);
    }
    
    ///
    /// Creates a debugging string representation.
    ///
    /// @return Detailed string suitable for logging and debugging
    ///
    public String toDebugString() {
        return String.format(
            "NodeCandidate{node=%d, leaf=%b, required=%d, total=%d, efficiency=%.3f, bytes=%d, explanation='%s'}",
            nodeIndex, isLeafNode, getRequiredChunkCount(), getTotalChunkCount(), 
            efficiency, estimatedBytes, explanation
        );
    }
}