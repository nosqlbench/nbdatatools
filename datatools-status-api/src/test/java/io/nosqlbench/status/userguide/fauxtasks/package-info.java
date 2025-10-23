/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Example task implementations demonstrating various StatusSource patterns.
 *
 * <p>This package contains example task classes used by the Status API User Guide.
 * These are "faux" tasks that simulate real work with Thread.sleep() to demonstrate
 * different patterns for implementing StatusSource and tracking progress.
 *
 * <h2>Task Types:</h2>
 *
 * <h3>Sequential Tasks (volatile long):</h3>
 * <ul>
 *   <li>{@link io.nosqlbench.status.userguide.fauxtasks.DataLoader} -
 *       Basic task with volatile long progress tracking for single-threaded execution</li>
 *   <li>{@link io.nosqlbench.status.userguide.fauxtasks.DataValidator} -
 *       Validation task demonstrating the same pattern</li>
 *   <li>{@link io.nosqlbench.status.userguide.fauxtasks.DataProcessor} -
 *       Processing task demonstrating the same pattern</li>
 * </ul>
 *
 * <h3>Parallel Tasks (AtomicLong):</h3>
 * <ul>
 *   <li>{@link io.nosqlbench.status.userguide.fauxtasks.ParallelDataLoader} -
 *       Task using AtomicLong for thread-safe progress tracking in parallel execution</li>
 * </ul>
 *
 * <h3>Closure-Based Tasks:</h3>
 * <ul>
 *   <li>{@link io.nosqlbench.status.userguide.fauxtasks.ClosureBasedProcessor} -
 *       Parent task that tracks many lightweight closures without requiring each
 *       closure to implement StatusSource</li>
 * </ul>
 *
 * <h2>Key Patterns Demonstrated:</h2>
 * <ul>
 *   <li><strong>Single-threaded progress:</strong> Use volatile long for simple sequential tasks</li>
 *   <li><strong>Multi-threaded progress:</strong> Use AtomicLong for parallel execution</li>
 *   <li><strong>Closure aggregation:</strong> Track many lightweight tasks with one parent StatusSource</li>
 *   <li><strong>State management:</strong> Proper RunState transitions (PENDING -> RUNNING -> SUCCESS/FAILED)</li>
 * </ul>
 *
 * <h2>Usage:</h2>
 * <p>These classes are designed to be used in the Level examples in the parent package.
 * They demonstrate real-world patterns for implementing StatusSource in different scenarios:
 * <ul>
 *   <li>Use volatile long when you have a single thread updating progress</li>
 *   <li>Use AtomicLong when you have multiple threads updating progress concurrently</li>
 *   <li>Use ClosureBasedProcessor pattern when tracking hundreds of lightweight closures</li>
 * </ul>
 *
 * @see io.nosqlbench.status.eventing.StatusSource
 * @see io.nosqlbench.status.StatusTracker
 * @since 4.0.0
 */
package io.nosqlbench.status.userguide.fauxtasks;
