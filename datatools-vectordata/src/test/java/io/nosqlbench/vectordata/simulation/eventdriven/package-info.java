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

/// Event-driven simulation framework for scheduler performance analysis.
/// 
/// This package provides a discrete event simulation platform that models
/// scheduler behavior using computed time rather than real-time execution.
/// Instead of actually downloading data or waiting for network delays,
/// the simulation computes when events would occur and processes them
/// in sequence.
/// 
/// Key advantages over real-time simulation:
/// - Extremely fast execution (millions of events per second)
/// - Perfect reproducibility
/// - No resource consumption
/// - Ability to simulate years of operation in seconds
/// - Precise control over timing and event ordering
/// 
/// Core concepts:
/// - **Event**: A discrete action that occurs at a specific simulation time
/// - **Event Queue**: Priority queue ordered by simulation time
/// - **Simulation Clock**: Virtual time that advances as events are processed
/// - **Actors**: Components that generate and respond to events
/// 
/// The simulation models:
/// - Network latency and bandwidth constraints
/// - Concurrent connection limits
/// - Chunk download queuing and scheduling
/// - Failure rates and retry behavior
/// - Various access patterns and workloads
/// 
/// Example usage:
/// ```java
/// EventDrivenSimulation sim = new EventDrivenSimulation()
///     .withSimulatedTime(Duration.ofHours(1))
///     .withFileSize(1_000_000_000L) // 1GB
///     .withScheduler(new EventDrivenAggressiveScheduler())
///     .withNetworkModel(NetworkModel.BROADBAND_FAST)
///     .withWorkloadGenerator(new SequentialAccessGenerator());
/// 
/// SimulationResults results = sim.run();
/// results.printStatistics();
/// ```
package io.nosqlbench.vectordata.simulation.eventdriven;