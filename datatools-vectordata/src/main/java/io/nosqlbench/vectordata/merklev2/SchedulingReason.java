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

///
/// Enumeration of reasons why a scheduler selected a particular merkle node for download.
/// 
/// This enum provides categorization of scheduling decisions to enable detailed
/// analysis and testing of scheduler behavior. Each reason represents a different
/// strategic consideration that influenced the node selection.
///
public enum SchedulingReason {
    
    ///
    /// Node was selected because it exactly matches the required chunks with no waste.
    /// This represents optimal efficiency where every byte downloaded is needed.
    ///
    EXACT_MATCH("Node exactly covers required chunks with no waste"),
    
    ///
    /// Node was selected because it covers multiple required chunks efficiently.
    /// Some over-downloading may occur, but the efficiency ratio is acceptable.
    ///
    EFFICIENT_COVERAGE("Node efficiently covers multiple required chunks"),
    
    ///
    /// Node represents the minimal download needed to satisfy a chunk requirement.
    /// This typically happens when no internal nodes provide better efficiency.
    ///
    MINIMAL_DOWNLOAD("Smallest possible download to satisfy requirement"),
    
    ///
    /// Node was selected for prefetching to optimize future requests.
    /// This represents speculative downloading based on predicted access patterns.
    ///
    PREFETCH("Node selected for speculative prefetching"),
    
    ///
    /// Node was selected to consolidate multiple small requests into one larger request.
    /// This optimizes network efficiency at the cost of some over-downloading.
    ///
    CONSOLIDATION("Node consolidates multiple small requests"),
    
    ///
    /// Node was selected because it's already in progress from another request.
    /// This avoids duplicate downloads and leverages existing work.
    ///
    REUSE_EXISTING("Node already being downloaded by another request"),
    
    ///
    /// Node was selected based on its priority in the download queue.
    /// This typically applies when scheduler needs to respect ordering constraints.
    ///
    PRIORITY_BASED("Node selected based on priority ordering"),
    
    ///
    /// Node was selected as a fallback when optimal choices aren't available.
    /// This ensures progress is made even when conditions aren't ideal.
    ///
    FALLBACK("Node selected as fallback option"),
    
    ///
    /// Node was selected due to bandwidth optimization considerations.
    /// This prioritizes maximizing network utilization over minimal downloading.
    ///
    BANDWIDTH_OPTIMIZATION("Node selected to optimize bandwidth utilization"),
    
    ///
    /// Node was selected due to latency optimization considerations.
    /// This prioritizes minimizing request count over minimal downloading.
    ///
    LATENCY_OPTIMIZATION("Node selected to minimize request latency"),
    
    ///
    /// Node was selected as fallback due to transport size limitations.
    /// This happens when optimal nodes exceed transport capacity (e.g., 2GB ByteBuffer limit).
    ///
    TRANSPORT_SIZE_FALLBACK("Node selected due to transport size limitations");
    
    private final String description;
    
    SchedulingReason(String description) {
        this.description = description;
    }
    
    ///
    /// Gets a human-readable description of this scheduling reason.
    ///
    /// @return Description of why this reason applies
    ///
    public String getDescription() {
        return description;
    }
    
    ///
    /// Determines if this reason represents an efficiency-focused decision.
    /// 
    /// @return true if this reason prioritizes download efficiency
    ///
    public boolean isEfficiencyFocused() {
        return this == EXACT_MATCH || this == EFFICIENT_COVERAGE || this == MINIMAL_DOWNLOAD;
    }
    
    ///
    /// Determines if this reason represents a performance-focused decision.
    /// 
    /// @return true if this reason prioritizes performance optimization
    ///
    public boolean isPerformanceFocused() {
        return this == BANDWIDTH_OPTIMIZATION || this == LATENCY_OPTIMIZATION || 
               this == CONSOLIDATION || this == REUSE_EXISTING;
    }
    
    ///
    /// Determines if this reason represents a speculative decision.
    /// 
    /// @return true if this reason involves downloading beyond immediate needs
    ///
    public boolean isSpeculative() {
        return this == PREFETCH || this == BANDWIDTH_OPTIMIZATION;
    }
}