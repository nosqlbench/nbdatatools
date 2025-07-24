package io.nosqlbench.vectordata.simulation.eventdriven;

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

import java.util.Random;

/// Network model for computing transfer times and failure rates.
/// 
/// This class encapsulates network characteristics and provides methods
/// to compute download durations based on bandwidth, latency, and
/// reliability constraints. It uses probabilistic models to simulate
/// realistic network behavior.
/// 
/// The model accounts for:
/// - Bandwidth limitations (bytes per second)
/// - Network latency (round-trip time)
/// - Connection setup overhead
/// - Session concurrency limits and queueing delays
/// - Minimum unloaded latency for chunked transfers
/// - Failure rates and retry behavior
/// 
/// Transfer time calculation:
/// transferTime = unloadedLatency + (bytes / effectiveBandwidth) + overhead + queueingDelay
public class NetworkModel {
    
    private final String name;
    private final long bandwidthBps;
    private final double latencySeconds;
    private final int maxConcurrentConnections;
    private final int maxSessionConcurrency;
    private final double unloadedLatencySeconds;
    private final double failureRate;
    private final double connectionOverheadSeconds;
    private final Random random;
    
    /// Creates a network model with specified parameters.
    /// 
    /// @param name Human-readable name for this network model
    /// @param bandwidthBps Available bandwidth in bytes per second
    /// @param latencySeconds Network latency in seconds
    /// @param maxConcurrentConnections Maximum concurrent connections
    /// @param maxSessionConcurrency Maximum concurrent transfers per session/host
    /// @param unloadedLatencySeconds Minimum latency for chunked transfers when unloaded
    /// @param failureRate Probability of request failure (0.0 to 1.0)
    /// @param connectionOverheadSeconds Overhead for establishing connections
    /// @param randomSeed Seed for reproducible random behavior
    public NetworkModel(String name, long bandwidthBps, double latencySeconds,
                       int maxConcurrentConnections, int maxSessionConcurrency,
                       double unloadedLatencySeconds, double failureRate,
                       double connectionOverheadSeconds, long randomSeed) {
        this.name = name;
        this.bandwidthBps = bandwidthBps;
        this.latencySeconds = latencySeconds;
        this.maxConcurrentConnections = maxConcurrentConnections;
        this.maxSessionConcurrency = maxSessionConcurrency;
        this.unloadedLatencySeconds = unloadedLatencySeconds;
        this.failureRate = failureRate;
        this.connectionOverheadSeconds = connectionOverheadSeconds;
        this.random = new Random(randomSeed);
    }
    
    /// Computes the time required to transfer the specified number of bytes.
    /// 
    /// This includes minimum unloaded latency, network latency, bandwidth-limited 
    /// transfer time, connection overhead, and session concurrency queueing delays.
    /// The actual time may vary slightly due to random factors to simulate 
    /// real-world variability.
    /// 
    /// @param bytes Number of bytes to transfer
    /// @param concurrentConnections Current number of active connections
    /// @param sessionConcurrency Current number of concurrent transfers in this session
    /// @return Transfer time in seconds
    public double computeTransferTime(long bytes, int concurrentConnections, int sessionConcurrency) {
        // Apply connection-level congestion factor
        double connectionCongestionFactor = computeConnectionCongestionFactor(concurrentConnections);
        
        // Apply session-level congestion factor
        double sessionCongestionFactor = computeSessionCongestionFactor(sessionConcurrency);
        
        // Combined effective bandwidth reduction
        double totalCongestionFactor = connectionCongestionFactor * sessionCongestionFactor;
        double effectiveBandwidth = bandwidthBps / totalCongestionFactor;
        
        // Compute queueing delay from session concurrency
        double queueingDelay = computeSessionQueueingDelay(sessionConcurrency);
        
        // Base transfer time = unloadedLatency + networkLatency + (bytes / bandwidth) + overhead + queueing
        double transferTime = unloadedLatencySeconds +
                             latencySeconds + 
                             (bytes / effectiveBandwidth) + 
                             connectionOverheadSeconds +
                             queueingDelay;
        
        // Add small random variation (±10%)
        double variation = 1.0 + (random.nextGaussian() * 0.05);
        variation = Math.max(0.5, Math.min(1.5, variation)); // Clamp to reasonable range
        
        return transferTime * variation;
    }
    
    /// Computes the time required to transfer bytes (legacy method for compatibility).
    /// 
    /// @param bytes Number of bytes to transfer
    /// @param concurrentConnections Current number of active connections
    /// @return Transfer time in seconds
    public double computeTransferTime(long bytes, int concurrentConnections) {
        return computeTransferTime(bytes, concurrentConnections, 1);
    }
    
    /// Determines if a transfer should fail based on the failure rate.
    /// 
    /// @return true if the transfer should fail
    public boolean shouldFail() {
        return random.nextDouble() < failureRate;
    }
    
    /// Computes connection-level congestion factor based on concurrent connections.
    /// 
    /// As the number of concurrent connections approaches the maximum,
    /// the effective bandwidth per connection decreases.
    /// 
    /// @param concurrentConnections Current number of connections
    /// @return Connection congestion factor (≥ 1.0)
    private double computeConnectionCongestionFactor(int concurrentConnections) {
        if (concurrentConnections <= 1) {
            return 1.0;
        }
        
        // Linear increase in congestion up to max connections
        double utilizationRatio = (double) concurrentConnections / maxConcurrentConnections;
        return 1.0 + (utilizationRatio * 2.0); // Up to 3x congestion at max capacity
    }
    
    /// Computes session-level congestion factor based on concurrent transfers per session.
    /// 
    /// HTTP/2 multiplexing and session limits can cause additional bandwidth sharing
    /// and head-of-line blocking within a single session.
    /// 
    /// @param sessionConcurrency Current number of concurrent transfers in session
    /// @return Session congestion factor (≥ 1.0)
    private double computeSessionCongestionFactor(int sessionConcurrency) {
        if (sessionConcurrency <= 1) {
            return 1.0;
        }
        
        // More aggressive congestion for session limits due to protocol overhead
        double sessionUtilization = (double) sessionConcurrency / maxSessionConcurrency;
        
        // Exponential congestion increase for session overload
        if (sessionConcurrency > maxSessionConcurrency) {
            double overload = sessionConcurrency - maxSessionConcurrency;
            return 2.0 + (overload * 0.5); // Severe degradation when exceeding session limits
        }
        
        return 1.0 + (sessionUtilization * sessionUtilization * 1.5); // Quadratic increase
    }
    
    /// Computes queueing delay due to session concurrency limits.
    /// 
    /// When more transfers are requested than the session can handle concurrently,
    /// additional transfers must wait in a queue.
    /// 
    /// @param sessionConcurrency Current number of concurrent transfers in session
    /// @return Queueing delay in seconds
    private double computeSessionQueueingDelay(int sessionConcurrency) {
        if (sessionConcurrency <= maxSessionConcurrency) {
            return 0.0; // No queueing needed
        }
        
        // Queue length is the excess beyond session limits
        int queueLength = sessionConcurrency - maxSessionConcurrency;
        
        // Each queued request adds latency equal to estimated service time
        // Assume average chunk size and current session congestion
        double averageServiceTime = unloadedLatencySeconds + latencySeconds + connectionOverheadSeconds;
        double sessionCongestion = computeSessionCongestionFactor(maxSessionConcurrency);
        
        return queueLength * averageServiceTime * sessionCongestion * 0.7; // 70% of full service time
    }
    
    /// Gets the maximum number of concurrent connections.
    /// 
    /// @return Maximum concurrent connections
    public int getMaxConcurrentConnections() {
        return maxConcurrentConnections;
    }
    
    /// Gets the maximum session concurrency.
    /// 
    /// @return Maximum concurrent transfers per session
    public int getMaxSessionConcurrency() {
        return maxSessionConcurrency;
    }
    
    /// Gets the unloaded latency for chunked transfers.
    /// 
    /// @return Minimum unloaded latency in seconds
    public double getUnloadedLatencySeconds() {
        return unloadedLatencySeconds;
    }
    
    /// Gets the network model name.
    /// 
    /// @return Network model name
    public String getName() {
        return name;
    }
    
    /// Gets the bandwidth in bytes per second.
    /// 
    /// @return Bandwidth in bytes per second
    public long getBandwidthBps() {
        return bandwidthBps;
    }
    
    /// Gets the network latency in seconds.
    /// 
    /// @return Latency in seconds
    public double getLatencySeconds() {
        return latencySeconds;
    }
    
    /// Gets the failure rate.
    /// 
    /// @return Failure rate (0.0 to 1.0)
    public double getFailureRate() {
        return failureRate;
    }
    
    @Override
    public String toString() {
        return String.format("%s[%.1f Mbps, %.0fms latency, %d connections, %d session, %.1f%% failures]",
            name, (bandwidthBps * 8.0) / 1_000_000, latencySeconds * 1000,
            maxConcurrentConnections, maxSessionConcurrency, failureRate * 100);
    }
    
    /// Pre-defined network models for common scenarios.
    public static class Presets {
        
        public static final NetworkModel LOCALHOST = new NetworkModel(
            "Localhost", 1_000_000_000L, 0.001, 32, 16, 0.0005, 0.0, 0.0, 42L
        );
        
        public static final NetworkModel FIBER_GIGABIT = new NetworkModel(
            "Fiber Gigabit", 125_000_000L, 0.005, 16, 8, 0.002, 0.0001, 0.002, 42L
        );
        
        public static final NetworkModel BROADBAND_FAST = new NetworkModel(
            "Fast Broadband", 12_500_000L, 0.020, 8, 6, 0.008, 0.001, 0.005, 42L
        );
        
        public static final NetworkModel BROADBAND_STANDARD = new NetworkModel(
            "Standard Broadband", 2_500_000L, 0.050, 4, 4, 0.020, 0.01, 0.010, 42L
        );
        
        public static final NetworkModel MOBILE_LTE = new NetworkModel(
            "Mobile LTE", 625_000L, 0.150, 2, 2, 0.080, 0.05, 0.020, 42L
        );
        
        public static final NetworkModel SATELLITE = new NetworkModel(
            "Satellite", 125_000L, 0.600, 1, 1, 0.300, 0.10, 0.050, 42L
        );
        
        /// Gets all preset network models.
        /// 
        /// @return Array of all preset network models
        public static NetworkModel[] getAllPresets() {
            return new NetworkModel[] {
                LOCALHOST, FIBER_GIGABIT, BROADBAND_FAST,
                BROADBAND_STANDARD, MOBILE_LTE, SATELLITE
            };
        }
    }
}