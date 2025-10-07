package io.nosqlbench.vectordata.simulation.mockdriven;

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

import java.time.Duration;
import java.util.Objects;

/// Represents network condition parameters for simulation testing.
/// 
/// This class encapsulates the various network characteristics that affect
/// download performance, allowing tests to simulate different real-world
/// network scenarios.
/// 
/// Network conditions include:
/// - Bandwidth limitations (bytes per second)
/// - Network latency (round-trip time)
/// - Connection concurrency limits
/// - Reliability (success rate)
/// 
/// Example conditions:
/// ```java
/// // Fast broadband connection
/// NetworkConditions broadband = NetworkConditions.builder()
///     .bandwidth(100_000_000) // 100 Mbps
///     .latency(Duration.ofMillis(20))
///     .maxConcurrentConnections(8)
///     .successRate(0.999)
///     .description("Fast Broadband")
///     .build();
/// 
/// // Mobile/cellular connection
/// NetworkConditions mobile = NetworkConditions.builder()
///     .bandwidth(5_000_000) // 5 Mbps
///     .latency(Duration.ofMillis(150))
///     .maxConcurrentConnections(2)
///     .successRate(0.95)
///     .description("Mobile LTE")
///     .build();
/// ```
public class NetworkConditions {
    
    private final long bandwidthBps;
    private final Duration latency;
    private final int maxConcurrentConnections;
    private final double successRate;
    private final String description;
    
    /// Creates network conditions with specified parameters.
    /// 
    /// @param bandwidthBps Available bandwidth in bytes per second (0 = unlimited)
    /// @param latency Network latency for requests
    /// @param maxConcurrentConnections Maximum concurrent connections allowed
    /// @param successRate Success rate for requests (0.0 to 1.0)
    /// @param description Human-readable description of these conditions
    public NetworkConditions(long bandwidthBps, Duration latency, int maxConcurrentConnections, 
                           double successRate, String description) {
        if (bandwidthBps < 0) {
            throw new IllegalArgumentException("Bandwidth cannot be negative");
        }
        if (latency == null || latency.isNegative()) {
            throw new IllegalArgumentException("Latency must be non-negative");
        }
        if (maxConcurrentConnections <= 0) {
            throw new IllegalArgumentException("Max concurrent connections must be positive");
        }
        if (successRate < 0.0 || successRate > 1.0) {
            throw new IllegalArgumentException("Success rate must be between 0.0 and 1.0");
        }
        
        this.bandwidthBps = bandwidthBps;
        this.latency = latency;
        this.maxConcurrentConnections = maxConcurrentConnections;
        this.successRate = successRate;
        this.description = description != null ? description : "Custom Network Conditions";
    }
    
    /// Gets the bandwidth limit in bytes per second.
    /// 
    /// @return Bandwidth in bytes per second, or 0 for unlimited
    public long getBandwidthBps() {
        return bandwidthBps;
    }
    
    /// Gets the network latency.
    /// 
    /// @return Network latency duration
    public Duration getLatency() {
        return latency;
    }
    
    /// Gets the maximum number of concurrent connections allowed.
    /// 
    /// @return Maximum concurrent connections
    public int getMaxConcurrentConnections() {
        return maxConcurrentConnections;
    }
    
    /// Gets the success rate for requests.
    /// 
    /// @return Success rate as a value between 0.0 and 1.0
    public double getSuccessRate() {
        return successRate;
    }
    
    /// Gets a human-readable description of these network conditions.
    /// 
    /// @return Description string
    public String getDescription() {
        return description;
    }
    
    /// Gets the bandwidth in a human-readable format.
    /// 
    /// @return Formatted bandwidth string (e.g., "100 Mbps", "5.5 MB/s")
    public String getFormattedBandwidth() {
        if (bandwidthBps == 0) {
            return "Unlimited";
        }
        
        if (bandwidthBps >= 1_000_000) {
            double mbps = (bandwidthBps * 8.0) / 1_000_000;
            return String.format("%.1f Mbps", mbps);
        } else if (bandwidthBps >= 1_000) {
            double kbps = (bandwidthBps * 8.0) / 1_000;
            return String.format("%.1f Kbps", kbps);
        } else {
            return bandwidthBps + " B/s";
        }
    }
    
    /// Creates a builder for constructing NetworkConditions.
    /// 
    /// @return A new NetworkConditions builder
    public static Builder builder() {
        return new Builder();
    }
    
    /// Pre-defined network condition scenarios for common testing.
    public static class Scenarios {
        
        /// High-speed fiber connection.
        public static final NetworkConditions FIBER = new NetworkConditions(
            125_000_000, // 1 Gbps
            Duration.ofMillis(5),
            16,
            0.9999,
            "Fiber Gigabit"
        );
        
        /// Fast broadband connection.
        public static final NetworkConditions BROADBAND_FAST = new NetworkConditions(
            12_500_000, // 100 Mbps
            Duration.ofMillis(20),
            8,
            0.999,
            "Fast Broadband"
        );
        
        /// Standard broadband connection.
        public static final NetworkConditions BROADBAND_STANDARD = new NetworkConditions(
            2_500_000, // 20 Mbps
            Duration.ofMillis(50),
            4,
            0.99,
            "Standard Broadband"
        );
        
        /// Mobile LTE connection.
        public static final NetworkConditions MOBILE_LTE = new NetworkConditions(
            625_000, // 5 Mbps
            Duration.ofMillis(150),
            2,
            0.95,
            "Mobile LTE"
        );
        
        /// Slow/satellite connection.
        public static final NetworkConditions SATELLITE = new NetworkConditions(
            125_000, // 1 Mbps
            Duration.ofMillis(600),
            1,
            0.9,
            "Satellite"
        );
        
        /// Unrestricted/localhost connection.
        public static final NetworkConditions LOCALHOST = new NetworkConditions(
            0, // Unlimited
            Duration.ofMillis(1),
            32,
            1.0,
            "Localhost"
        );
    }
    
    /// Builder class for constructing NetworkConditions instances.
    public static class Builder {
        private long bandwidthBps = 0;
        private Duration latency = Duration.ofMillis(50);
        private int maxConcurrentConnections = 4;
        private double successRate = 1.0;
        private String description = null;
        
        /// Sets the bandwidth in bytes per second.
        /// 
        /// @param bandwidthBps Bandwidth in bytes per second
        /// @return This builder
        public Builder bandwidth(long bandwidthBps) {
            this.bandwidthBps = bandwidthBps;
            return this;
        }
        
        /// Sets the bandwidth in Mbps.
        /// 
        /// @param mbps Bandwidth in megabits per second
        /// @return This builder
        public Builder bandwidthMbps(double mbps) {
            this.bandwidthBps = (long) (mbps * 1_000_000 / 8);
            return this;
        }
        
        /// Sets the network latency.
        /// 
        /// @param latency Network latency duration
        /// @return This builder
        public Builder latency(Duration latency) {
            this.latency = latency;
            return this;
        }
        
        /// Sets the network latency in milliseconds.
        /// 
        /// @param latencyMs Latency in milliseconds
        /// @return This builder
        public Builder latencyMs(long latencyMs) {
            this.latency = Duration.ofMillis(latencyMs);
            return this;
        }
        
        /// Sets the maximum concurrent connections.
        /// 
        /// @param maxConnections Maximum concurrent connections
        /// @return This builder
        public Builder maxConcurrentConnections(int maxConnections) {
            this.maxConcurrentConnections = maxConnections;
            return this;
        }
        
        /// Sets the success rate for requests.
        /// 
        /// @param successRate Success rate (0.0 to 1.0)
        /// @return This builder
        public Builder successRate(double successRate) {
            this.successRate = successRate;
            return this;
        }
        
        /// Sets the description for these network conditions.
        /// 
        /// @param description Human-readable description
        /// @return This builder
        public Builder description(String description) {
            this.description = description;
            return this;
        }
        
        /// Builds the NetworkConditions instance.
        /// 
        /// @return A new NetworkConditions instance
        public NetworkConditions build() {
            return new NetworkConditions(bandwidthBps, latency, maxConcurrentConnections, 
                                       successRate, description);
        }
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NetworkConditions that = (NetworkConditions) o;
        return bandwidthBps == that.bandwidthBps &&
               maxConcurrentConnections == that.maxConcurrentConnections &&
               Double.compare(that.successRate, successRate) == 0 &&
               Objects.equals(latency, that.latency) &&
               Objects.equals(description, that.description);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(bandwidthBps, latency, maxConcurrentConnections, successRate, description);
    }
    
    @Override
    public String toString() {
        return String.format("NetworkConditions{%s: %s, %dms latency, %d connections, %.1f%% success}", 
                           description, getFormattedBandwidth(), latency.toMillis(), 
                           maxConcurrentConnections, successRate * 100);
    }
}