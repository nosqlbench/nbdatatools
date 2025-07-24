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

/// Simulation framework for testing and comparing chunk scheduler implementations.
/// 
/// This package provides a comprehensive testing environment for evaluating different
/// chunk download scheduling strategies under various network conditions. It includes:
/// 
/// - Mock transport clients with configurable throughput and latency
/// - Simulated network conditions (bandwidth constraints, connection limits, etc.)
/// - Performance measurement and comparison tools
/// - Multiple scheduler implementations for benchmarking
/// - Test scenarios representing real-world usage patterns
/// 
/// The simulation framework allows developers to:
/// 1. Test new scheduler algorithms before deployment
/// 2. Compare performance under different network conditions
/// 3. Identify optimal scheduling strategies for specific scenarios
/// 4. Validate scheduler behavior under stress conditions
/// 
/// Key components:
/// - {@link MockChunkedTransportClient} - Configurable transport simulation
/// - {@link NetworkConditionSimulator} - Network environment simulation
/// - {@link SchedulerPerformanceTest} - Benchmarking framework
/// - {@link SimulatedMerkleState} - Merkle state tracking for tests
/// - {@link SimulatedMerkleShape} - Simplified merkle tree geometry
/// 
/// Example usage:
/// ```java
/// // Create network conditions
/// NetworkConditions conditions = NetworkConditions.builder()
///     .bandwidth(100_000_000) // 100 Mbps
///     .latency(50) // 50ms
///     .maxConcurrentConnections(4)
///     .build();
/// 
/// // Run scheduler comparison
/// SchedulerPerformanceTest test = new SchedulerPerformanceTest()
///     .withNetworkConditions(conditions)
///     .withFileSize(1_000_000_000) // 1GB
///     .withSchedulers(
///         new DefaultChunkScheduler(),
///         new AggressiveScheduler(),
///         new ConservativeScheduler()
///     );
/// 
/// PerformanceResults results = test.run();
/// ```
package io.nosqlbench.vectordata.simulation.mockdriven;