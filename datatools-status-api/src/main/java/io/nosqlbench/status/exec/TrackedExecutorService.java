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

package io.nosqlbench.status.exec;

import io.nosqlbench.status.StatusScope;

import java.util.concurrent.ExecutorService;

/**
 * An ExecutorService wrapper that automatically tracks submitted tasks using the StatusContext API.
 * Tasks are tracked with the status monitoring system, providing visibility into execution progress
 * and lifecycle events.
 *
 * <p>This interface extends {@link ExecutorService} and adds tracking capabilities to monitor:
 * <ul>
 *   <li>Task submission, execution, and completion</li>
 *   <li>Progress for tasks implementing {@link io.nosqlbench.status.eventing.StatusSource}</li>
 *   <li>Aggregate statistics (submitted, completed, failed, cancelled counts)</li>
 *   <li>Hierarchical organization via {@link StatusScope}</li>
 * </ul>
 *
 * <p><strong>Two Tracking Modes:</strong></p>
 * <ul>
 *   <li><strong>INDIVIDUAL:</strong> Each task gets its own {@link io.nosqlbench.status.StatusTracker},
 *       providing detailed per-task visibility. Best for ≤50 tasks where individual progress matters.</li>
 *   <li><strong>AGGREGATE:</strong> All tasks share a single parent tracker that counts completion.
 *       Best for 100+ lightweight tasks where individual tracking would be overhead.</li>
 * </ul>
 *
 * <p><strong>StatusSource Detection:</strong></p>
 * <p>The wrapper automatically detects if submitted Callable/Runnable implements
 * {@link io.nosqlbench.status.eventing.StatusSource}. If so, it tracks the task's self-reported
 * progress. Otherwise, it tracks basic lifecycle (pending → running → success/failed).
 *
 * <h2>Usage Examples</h2>
 *
 * <h3>Individual Task Tracking (detailed visibility)</h3>
 * <pre>{@code
 * ExecutorService executor = Executors.newFixedThreadPool(4);
 * try (StatusContext ctx = new StatusContext("pipeline");
 *      StatusScope scope = ctx.createScope("ETL");
 *      TrackedExecutorService tracked = TrackedExecutors.wrap(executor, scope)
 *          .withMode(TrackingMode.INDIVIDUAL)
 *          .build()) {
 *
 *     ctx.addSink(new ConsoleLoggerSink());
 *
 *     // Each task tracked individually
 *     Future<Data> f1 = tracked.submit(new DataLoadTask());
 *     Future<Data> f2 = tracked.submit(new DataTransformTask());
 *
 *     f1.get();
 *     f2.get();
 *
 *     System.out.println("Stats: " + tracked.getStatistics());
 * }
 * }</pre>
 *
 * <h3>Aggregate Tracking (high volume)</h3>
 * <pre>{@code
 * ExecutorService executor = Executors.newFixedThreadPool(20);
 * try (StatusContext ctx = new StatusContext("batch");
 *      StatusScope scope = ctx.createScope("Processing");
 *      TrackedExecutorService tracked = TrackedExecutors.wrap(executor, scope)
 *          .withMode(TrackingMode.AGGREGATE)
 *          .withTaskGroupName("Records")
 *          .build()) {
 *
 *     ctx.addSink(new ConsoleLoggerSink());
 *
 *     List<Future<?>> futures = new ArrayList<>();
 *     for (int i = 0; i < 1000; i++) {
 *         futures.add(tracked.submit(() -> processRecord(i)));
 *     }
 *
 *     for (Future<?> f : futures) {
 *         f.get();
 *     }
 *
 *     // Shows: "Records [1000/1000 completed] 100% ✅"
 * }
 * }</pre>
 *
 * @see TrackedExecutors
 * @see TaskStatistics
 * @see io.nosqlbench.status.StatusTracker
 * @since 4.0.0
 */
public interface TrackedExecutorService extends ExecutorService, AutoCloseable {

    /**
     * Returns the StatusScope used for tracking submitted tasks.
     * All tasks submitted to this executor are tracked under this scope.
     *
     * @return the tracking scope for this executor
     */
    StatusScope getTrackingScope();

    /**
     * Returns current statistics about task execution.
     * Statistics include counts of submitted, completed, failed, and cancelled tasks.
     *
     * @return task execution statistics
     */
    TaskStatistics getStatistics();

    /**
     * Closes this tracked executor and releases all tracking resources.
     * This method closes the underlying executor and cleans up all StatusTracker resources.
     *
     * <p>After calling this method, no new tasks can be submitted and the executor
     * will no longer track task status.
     */
    @Override
    void close();
}
