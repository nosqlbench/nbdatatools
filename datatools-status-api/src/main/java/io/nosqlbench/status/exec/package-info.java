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
 * ExecutorService wrappers that automatically track task execution using the StatusContext API.
 *
 * <p>This package provides {@link io.nosqlbench.status.exec.TrackedExecutorService} implementations
 * that transparently wrap standard Java {@link java.util.concurrent.ExecutorService} instances to
 * provide automatic status tracking for submitted tasks.
 *
 * <h2>Key Classes</h2>
 * <ul>
 *   <li>{@link io.nosqlbench.status.exec.TrackedExecutors} - Factory for creating tracked executors</li>
 *   <li>{@link io.nosqlbench.status.exec.TrackedExecutorService} - Main interface extending ExecutorService</li>
 *   <li>{@link io.nosqlbench.status.exec.TaskStatistics} - Thread-safe task execution metrics</li>
 *   <li>{@link io.nosqlbench.status.exec.TrackingMode} - Enum defining tracking strategies</li>
 * </ul>
 *
 * <h2>Quick Start</h2>
 * <pre>{@code
 * ExecutorService executor = Executors.newFixedThreadPool(4);
 * try (StatusContext ctx = new StatusContext("pipeline");
 *      StatusScope scope = ctx.createScope("Processing");
 *      TrackedExecutorService tracked = TrackedExecutors.wrap(executor, scope).build()) {
 *
 *     ctx.addSink(new ConsoleLoggerSink());
 *     tracked.submit(() -> doWork());
 * }
 * }</pre>
 *
 * <h2>Tracking Modes</h2>
 * <p>Two tracking strategies are available:
 * <ul>
 *   <li><strong>Individual:</strong> Each task tracked separately (detailed visibility)</li>
 *   <li><strong>Aggregate:</strong> All tasks tracked as a group (high volume)</li>
 * </ul>
 *
 * @see io.nosqlbench.status.StatusContext
 * @see io.nosqlbench.status.StatusScope
 * @see io.nosqlbench.status.StatusTracker
 * @since 4.0.0
 */
package io.nosqlbench.status.exec;
