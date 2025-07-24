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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/// Factory and utilities for creating various network condition scenarios.
/// 
/// This class provides pre-configured network conditions and utilities for
/// creating custom scenarios for scheduler testing. It includes realistic
/// network profiles based on common connection types and usage patterns.
/// 
/// The simulator can create:
/// - Static network conditions (constant bandwidth/latency)
/// - Dynamic conditions (changing over time)
/// - Degradation scenarios (performance that gets worse)  
/// - Recovery scenarios (performance that improves)
/// - Real-world network profiles
/// 
/// Example usage:
/// ```java
/// // Get common scenarios
/// List<NetworkConditions> scenarios = NetworkConditionSimulator.getCommonScenarios();
/// 
/// // Create custom degradation scenario
/// List<NetworkConditions> degradation = NetworkConditionSimulator.createDegradationScenario(
///     NetworkConditions.Scenarios.BROADBAND_FAST,
///     NetworkConditions.Scenarios.MOBILE_LTE,
///     5 // 5 steps
/// );
/// 
/// // Create bandwidth throttling scenario
/// List<NetworkConditions> throttled = NetworkConditionSimulator.createBandwidthThrottlingScenario(
///     Duration.ofMinutes(1), // Test duration
///     Duration.ofSeconds(10)  // Throttle every 10 seconds
/// );
/// ```
public class NetworkConditionSimulator {
    
    /// Gets a comprehensive list of common network scenarios for testing.
    /// 
    /// This includes scenarios covering the full spectrum from high-speed
    /// fiber connections down to slow satellite connections, providing
    /// good coverage for scheduler performance evaluation.
    /// 
    /// @return List of common network condition scenarios
    public static List<NetworkConditions> getCommonScenarios() {
        return Arrays.asList(
            NetworkConditions.Scenarios.LOCALHOST,
            NetworkConditions.Scenarios.FIBER,
            NetworkConditions.Scenarios.BROADBAND_FAST,
            NetworkConditions.Scenarios.BROADBAND_STANDARD,
            NetworkConditions.Scenarios.MOBILE_LTE,
            NetworkConditions.Scenarios.SATELLITE
        );
    }
    
    /// Creates a network degradation scenario.
    /// 
    /// This simulates a network that starts with good conditions and
    /// gradually degrades to poor conditions. Useful for testing how
    /// schedulers adapt to changing network performance.
    /// 
    /// @param startConditions Initial good network conditions
    /// @param endConditions Final degraded network conditions
    /// @param steps Number of degradation steps
    /// @return List of network conditions showing gradual degradation
    public static List<NetworkConditions> createDegradationScenario(
            NetworkConditions startConditions, 
            NetworkConditions endConditions, 
            int steps) {
        
        if (steps < 2) {
            throw new IllegalArgumentException("Must have at least 2 steps");
        }
        
        List<NetworkConditions> scenario = new ArrayList<>();
        
        for (int i = 0; i < steps; i++) {
            double ratio = (double) i / (steps - 1);
            NetworkConditions conditions = interpolateConditions(startConditions, endConditions, ratio);
            scenario.add(conditions);
        }
        
        return scenario;
    }
    
    /// Creates a network recovery scenario.
    /// 
    /// This simulates a network that starts with poor conditions and
    /// gradually improves to good conditions. Useful for testing scheduler
    /// behavior during network recovery.
    /// 
    /// @param startConditions Initial poor network conditions
    /// @param endConditions Final improved network conditions
    /// @param steps Number of recovery steps
    /// @return List of network conditions showing gradual improvement
    public static List<NetworkConditions> createRecoveryScenario(
            NetworkConditions startConditions,
            NetworkConditions endConditions,
            int steps) {
        
        // Recovery is just degradation in reverse
        List<NetworkConditions> degradation = createDegradationScenario(endConditions, startConditions, steps);
        Collections.reverse(degradation);
        return degradation;
    }
    
    /// Creates a bandwidth throttling scenario.
    /// 
    /// This simulates periodic bandwidth reductions, such as might occur
    /// during network congestion or traffic shaping. Useful for testing
    /// how schedulers handle sudden bandwidth changes.
    /// 
    /// @param totalDuration Total duration of the test scenario
    /// @param throttleInterval How often to change bandwidth
    /// @return List of network conditions with varying bandwidth
    public static List<NetworkConditions> createBandwidthThrottlingScenario(
            Duration totalDuration,
            Duration throttleInterval) {
        
        List<NetworkConditions> scenario = new ArrayList<>();
        
        // Base conditions - fast broadband
        NetworkConditions baseConditions = NetworkConditions.Scenarios.BROADBAND_FAST;
        
        // Throttled conditions - much slower
        NetworkConditions throttledConditions = NetworkConditions.builder()
            .bandwidth(baseConditions.getBandwidthBps() / 10) // 10x slower
            .latency(baseConditions.getLatency().multipliedBy(2)) // 2x latency
            .maxConcurrentConnections(baseConditions.getMaxConcurrentConnections() / 2)
            .successRate(baseConditions.getSuccessRate() * 0.9) // Slightly less reliable
            .description("Throttled " + baseConditions.getDescription())
            .build();
        
        long intervalMs = throttleInterval.toMillis();
        long totalMs = totalDuration.toMillis();
        
        boolean isThrottled = false;
        for (long timeMs = 0; timeMs < totalMs; timeMs += intervalMs) {
            NetworkConditions current = isThrottled ? throttledConditions : baseConditions;
            scenario.add(current);
            isThrottled = !isThrottled; // Alternate between normal and throttled
        }
        
        return scenario;
    }
    
    /// Creates a high-concurrency scenario.
    /// 
    /// This tests scheduler behavior when many concurrent connections
    /// are available, which can reveal different optimization strategies
    /// for parallel downloading.
    /// 
    /// @param baseConditions Base network conditions to enhance
    /// @param maxConcurrency Maximum concurrent connections to allow
    /// @return Enhanced network conditions with high concurrency
    public static NetworkConditions createHighConcurrencyScenario(
            NetworkConditions baseConditions,
            int maxConcurrency) {
        
        return NetworkConditions.builder()
            .bandwidth(baseConditions.getBandwidthBps())
            .latency(baseConditions.getLatency())
            .maxConcurrentConnections(maxConcurrency)
            .successRate(baseConditions.getSuccessRate())
            .description("High Concurrency " + baseConditions.getDescription())
            .build();
    }
    
    /// Creates a low-reliability scenario.
    /// 
    /// This simulates unreliable network conditions with frequent failures,
    /// useful for testing scheduler retry and error handling strategies.
    /// 
    /// @param baseConditions Base network conditions to modify
    /// @param failureRate Failure rate (0.0 to 1.0, where 0.1 = 10% failures)
    /// @return Network conditions with reduced reliability
    public static NetworkConditions createLowReliabilityScenario(
            NetworkConditions baseConditions,
            double failureRate) {
        
        if (failureRate < 0.0 || failureRate > 1.0) {
            throw new IllegalArgumentException("Failure rate must be between 0.0 and 1.0");
        }
        
        double successRate = 1.0 - failureRate;
        
        return NetworkConditions.builder()
            .bandwidth(baseConditions.getBandwidthBps())
            .latency(baseConditions.getLatency())
            .maxConcurrentConnections(baseConditions.getMaxConcurrentConnections())
            .successRate(successRate)
            .description("Low Reliability " + baseConditions.getDescription())
            .build();
    }
    
    /// Creates a high-latency scenario.
    /// 
    /// This simulates network conditions with very high latency, such as
    /// satellite or international connections. Useful for testing how
    /// schedulers optimize for latency vs. bandwidth trade-offs.
    /// 
    /// @param baseConditions Base network conditions to modify
    /// @param latencyMultiplier Factor to multiply latency by
    /// @return Network conditions with increased latency
    public static NetworkConditions createHighLatencyScenario(
            NetworkConditions baseConditions,
            double latencyMultiplier) {
        
        if (latencyMultiplier < 1.0) {
            throw new IllegalArgumentException("Latency multiplier must be >= 1.0");
        }
        
        Duration newLatency = baseConditions.getLatency().multipliedBy((long) latencyMultiplier);
        
        return NetworkConditions.builder()
            .bandwidth(baseConditions.getBandwidthBps())
            .latency(newLatency)
            .maxConcurrentConnections(baseConditions.getMaxConcurrentConnections())
            .successRate(baseConditions.getSuccessRate())
            .description("High Latency " + baseConditions.getDescription())
            .build();
    }
    
    /// Creates a realistic mobile scenario with variable conditions.
    /// 
    /// This simulates the variable nature of mobile connections, including
    /// periods of good connectivity interspersed with poor connectivity
    /// as might occur when moving between cell towers.
    /// 
    /// @param duration Total scenario duration
    /// @param changeInterval How often conditions change
    /// @return List of variable mobile network conditions
    public static List<NetworkConditions> createVariableMobileScenario(
            Duration duration,
            Duration changeInterval) {
        
        List<NetworkConditions> scenario = new ArrayList<>();
        
        // Mobile condition variations
        NetworkConditions[] mobileVariations = {
            NetworkConditions.builder()
                .bandwidthMbps(50.0) // Excellent 5G
                .latencyMs(20)
                .maxConcurrentConnections(4)
                .successRate(0.99)
                .description("Excellent Mobile")
                .build(),
            NetworkConditions.builder()
                .bandwidthMbps(20.0) // Good LTE
                .latencyMs(50)
                .maxConcurrentConnections(3)
                .successRate(0.98)
                .description("Good Mobile")
                .build(),
            NetworkConditions.builder()
                .bandwidthMbps(5.0) // Standard LTE
                .latencyMs(100)
                .maxConcurrentConnections(2)
                .successRate(0.95)
                .description("Standard Mobile")
                .build(),
            NetworkConditions.builder()
                .bandwidthMbps(1.0) // Poor 3G
                .latencyMs(300)
                .maxConcurrentConnections(1)
                .successRate(0.85)
                .description("Poor Mobile")
                .build()
        };
        
        long intervalMs = changeInterval.toMillis();
        long totalMs = duration.toMillis();
        
        java.util.Random random = new java.util.Random(42); // Deterministic for testing
        
        for (long timeMs = 0; timeMs < totalMs; timeMs += intervalMs) {
            NetworkConditions conditions = mobileVariations[random.nextInt(mobileVariations.length)];
            scenario.add(conditions);
        }
        
        return scenario;
    }
    
    /// Interpolates between two network conditions.
    /// 
    /// Creates intermediate network conditions by linearly interpolating
    /// between the parameters of two given conditions.
    /// 
    /// @param start Starting network conditions
    /// @param end Ending network conditions
    /// @param ratio Interpolation ratio (0.0 = start, 1.0 = end)
    /// @return Interpolated network conditions
    private static NetworkConditions interpolateConditions(
            NetworkConditions start, 
            NetworkConditions end, 
            double ratio) {
        
        // Linear interpolation for numeric values
        long bandwidth = (long) (start.getBandwidthBps() + 
                               ratio * (end.getBandwidthBps() - start.getBandwidthBps()));
        
        long latencyMs = (long) (start.getLatency().toMillis() + 
                               ratio * (end.getLatency().toMillis() - start.getLatency().toMillis()));
        
        int connections = (int) (start.getMaxConcurrentConnections() + 
                               ratio * (end.getMaxConcurrentConnections() - start.getMaxConcurrentConnections()));
        
        double successRate = start.getSuccessRate() + 
                           ratio * (end.getSuccessRate() - start.getSuccessRate());
        
        // Ensure reasonable bounds
        connections = Math.max(1, connections);
        successRate = Math.max(0.0, Math.min(1.0, successRate));
        
        String description = String.format("Interpolated (%.1f%% → %s)", 
                                         ratio * 100, end.getDescription());
        
        return NetworkConditions.builder()
            .bandwidth(bandwidth)
            .latencyMs(latencyMs)
            .maxConcurrentConnections(connections)
            .successRate(successRate)
            .description(description)
            .build();
    }
}