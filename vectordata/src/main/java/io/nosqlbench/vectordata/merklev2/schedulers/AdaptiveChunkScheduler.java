package io.nosqlbench.vectordata.merklev2.schedulers;

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

import io.nosqlbench.vectordata.merklev2.ChunkScheduler;
import io.nosqlbench.vectordata.merklev2.MerkleShape;
import io.nosqlbench.vectordata.merklev2.MerkleState;
import io.nosqlbench.vectordata.merklev2.SchedulingTarget;
import io.nosqlbench.vectordata.merklev2.SchedulingDecision;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/// Adaptive chunk scheduler that adjusts strategy based on observed performance.
/// 
/// This scheduler dynamically adapts its behavior based on network performance
/// metrics, switching between aggressive and conservative strategies as
/// conditions change. It learns from download patterns and performance
/// to optimize for the current environment.
/// 
/// Strategy:
/// - Start with moderate chunk sizes
/// - Monitor download success rates and throughput
/// - Adapt chunk size and prefetching based on performance
/// - Switch strategies when performance degrades
/// - Learn from access patterns to improve predictions
/// 
/// Best suited for:
/// - Variable network conditions
/// - Unknown or changing bandwidth/latency
/// - Mixed access patterns
/// - Long-running applications that can benefit from learning
public class AdaptiveChunkScheduler implements ChunkScheduler {
    
    /// Performance tracking - note: in real implementation this would be external state
    private final AtomicLong totalBytesDownloaded = new AtomicLong(0);
    private final AtomicLong totalDownloadTime = new AtomicLong(0);
    private final AtomicInteger successfulDownloads = new AtomicInteger(0);
    private final AtomicInteger failedDownloads = new AtomicInteger(0);
    
    /// Adaptive parameters
    private volatile int currentAggressiveness = 2; // Start moderate (1=conservative, 5=aggressive)
    private volatile double recentThroughput = 0.0;
    private volatile Instant lastAdaptation = Instant.now();
    
    /// Access pattern tracking
    private final List<AccessPattern> recentAccesses = new ArrayList<>();
    private static final int MAX_PATTERN_HISTORY = 20;
    
    /// Delegate schedulers for different strategies
    private final AggressiveChunkScheduler aggressiveScheduler = new AggressiveChunkScheduler();
    private final DefaultChunkScheduler defaultScheduler = new DefaultChunkScheduler();
    private final ConservativeChunkScheduler conservativeScheduler = new ConservativeChunkScheduler();
    
    @Override
    public List<SchedulingDecision> selectOptimalNodes(
            List<Integer> requiredChunks,
            MerkleShape shape,
            MerkleState state) {
        
        if (requiredChunks.isEmpty()) {
            return List.of();
        }
        
        // Adapt strategy based on current performance metrics
        adaptStrategy();
        
        // Select the appropriate delegate scheduler based on current aggressiveness
        ChunkScheduler delegateScheduler = selectDelegateScheduler();
        
        // Get decisions from the selected scheduler
        List<SchedulingDecision> decisions = delegateScheduler.selectOptimalNodes(requiredChunks, shape, state);
        
        // Update performance tracking (simplified)
        updatePerformanceMetrics(decisions);
        
        return decisions;
    }
    
    @Override
    public void scheduleDownloads(
        long offset,
        long length,
        MerkleShape shape, MerkleState state,
        SchedulingTarget schedulingTarget) {
        
        // Record access pattern
        recordAccess(offset, length);
        
        // Adapt strategy if needed
        adaptStrategy();
        
        // Simplified implementation - delegate to default scheduler for now
        // In a real implementation, this would use the performance metrics to adapt behavior
        // Use the same delegate scheduler for consistency
        ChunkScheduler delegateScheduler = selectDelegateScheduler();
        delegateScheduler.scheduleDownloads(offset, length, shape, state, schedulingTarget);
    }
    
    ///
    /// Selects the appropriate delegate scheduler based on current aggressiveness level.
    ///
    /// @return The scheduler to use for the current conditions
    ///
    private ChunkScheduler selectDelegateScheduler() {
        return switch (currentAggressiveness) {
            case 1 -> conservativeScheduler;  // Most conservative
            case 2, 3 -> defaultScheduler;    // Balanced approach
            case 4, 5 -> aggressiveScheduler; // Most aggressive
            default -> defaultScheduler;      // Fallback
        };
    }
    
    ///
    /// Updates performance metrics based on scheduling decisions.
    /// In a real implementation, this would track actual download performance.
    ///
    /// @param decisions The scheduling decisions made
    ///
    private void updatePerformanceMetrics(List<SchedulingDecision> decisions) {
        // Simplified performance tracking
        long totalBytes = decisions.stream().mapToLong(SchedulingDecision::estimatedBytes).sum();
        totalBytesDownloaded.addAndGet(totalBytes);
        
        // Simulate performance feedback - in real implementation this would come from actual downloads
        double efficiency = decisions.stream()
            .mapToDouble(d -> (double) d.requiredChunks().size() / d.coveredChunks().size())
            .average()
            .orElse(1.0);
        
        // Adapt aggressiveness based on efficiency
        if (efficiency > 0.8 && currentAggressiveness < 5) {
            // High efficiency - can be more aggressive
            currentAggressiveness = Math.min(5, currentAggressiveness + 1);
        } else if (efficiency < 0.5 && currentAggressiveness > 1) {
            // Low efficiency - be more conservative
            currentAggressiveness = Math.max(1, currentAggressiveness - 1);
        }
    }
    
    /// Records an access pattern for learning.
    private void recordAccess(long offset, long length) {
        synchronized (recentAccesses) {
            recentAccesses.add(new AccessPattern(offset, length, Instant.now()));
            if (recentAccesses.size() > MAX_PATTERN_HISTORY) {
                recentAccesses.remove(0);
            }
        }
    }
    
    /// Adapts the scheduling strategy based on recent performance.
    private void adaptStrategy() {
        Instant now = Instant.now();
        
        // Only adapt every few seconds to avoid over-adjustment
        if (now.isAfter(lastAdaptation.plusSeconds(5))) {
            // Simplified adaptation logic
            double successRate = (double) successfulDownloads.get() / 
                Math.max(1, successfulDownloads.get() + failedDownloads.get());
            
            if (successRate > 0.9 && currentAggressiveness < 5) {
                // High success rate - can be more aggressive
                currentAggressiveness++;
            } else if (successRate < 0.7 && currentAggressiveness > 1) {
                // Low success rate - be more conservative
                currentAggressiveness--;
            }
            
            lastAdaptation = now;
        }
    }
    
    /// Represents an access pattern for learning.
    private static class AccessPattern {
        final long offset;
        final long length;
        final Instant timestamp;
        
        AccessPattern(long offset, long length, Instant timestamp) {
            this.offset = offset;
            this.length = length;
            this.timestamp = timestamp;
        }
    }
}