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

/**
 * Defines the tracking strategy for {@link TrackedExecutorService} implementations.
 * The tracking mode determines how submitted tasks are monitored and reported through
 * the status tracking system.
 *
 * <h2>Mode Comparison</h2>
 * <table border="1">
 *   <caption>Comparison of tracking modes</caption>
 *   <tr>
 *     <th>Mode</th>
 *     <th>Visibility</th>
 *     <th>Overhead</th>
 *     <th>Best For</th>
 *   </tr>
 *   <tr>
 *     <td>INDIVIDUAL</td>
 *     <td>Per-task progress and state</td>
 *     <td>High (N trackers)</td>
 *     <td>≤50 important tasks</td>
 *   </tr>
 *   <tr>
 *     <td>AGGREGATE</td>
 *     <td>Group completion rate</td>
 *     <td>Low (1 tracker)</td>
 *     <td>100+ lightweight tasks</td>
 *   </tr>
 *   <tr>
 *     <td>AUTO</td>
 *     <td>Varies</td>
 *     <td>Varies</td>
 *     <td>Unknown workload</td>
 *   </tr>
 * </table>
 *
 * @see TrackedExecutorService
 * @see TrackedExecutors
 * @since 4.0.0
 */
public enum TrackingMode {
    /**
     * Track each submitted task individually with its own {@link io.nosqlbench.status.StatusTracker}.
     * Provides detailed per-task visibility into progress and lifecycle.
     *
     * <p><strong>Characteristics:</strong>
     * <ul>
     *   <li>Each task gets a separate StatusTracker</li>
     *   <li>Individual progress reporting for StatusSource tasks</li>
     *   <li>Detailed lifecycle tracking (pending/running/success/failed)</li>
     *   <li>Higher memory and CPU overhead</li>
     * </ul>
     *
     * <p><strong>Use When:</strong>
     * <ul>
     *   <li>Task count is moderate (≤50 tasks)</li>
     *   <li>Individual task progress is important</li>
     *   <li>Tasks have meaningful self-reported progress</li>
     *   <li>Debugging or detailed monitoring is needed</li>
     * </ul>
     *
     * <p><strong>Example:</strong>
     * <pre>{@code
     * try (TrackedExecutorService tracked = TrackedExecutors.wrap(executor, scope)
     *         .withMode(TrackingMode.INDIVIDUAL)
     *         .build()) {
     *     tracked.submit(new DataLoadTask());     // Tracked individually
     *     tracked.submit(new DataTransformTask()); // Tracked individually
     * }
     * }</pre>
     */
    INDIVIDUAL,

    /**
     * Track all tasks with a single aggregate counter.
     * All submitted tasks are represented by one parent tracker that reports aggregate metrics.
     *
     * <p><strong>Characteristics:</strong>
     * <ul>
     *   <li>Single StatusTracker for all tasks</li>
     *   <li>Progress = completed/submitted ratio</li>
     *   <li>Lower memory and CPU overhead</li>
     *   <li>No per-task detail</li>
     * </ul>
     *
     * <p><strong>Use When:</strong>
     * <ul>
     *   <li>Task count is high (100+ tasks)</li>
     *   <li>Tasks are lightweight and short-lived</li>
     *   <li>Only aggregate metrics matter</li>
     *   <li>Minimizing overhead is important</li>
     * </ul>
     *
     * <p><strong>Example:</strong>
     * <pre>{@code
     * try (TrackedExecutorService tracked = TrackedExecutors.wrap(executor, scope)
     *         .withMode(TrackingMode.AGGREGATE)
     *         .withTaskGroupName("Records")
     *         .build()) {
     *     for (int i = 0; i < 1000; i++) {
     *         tracked.submit(() -> processRecord(i));
     *     }
     *     // Shows: "Records [1000/1000 completed] 100%"
     * }
     * }</pre>
     */
    AGGREGATE,

    /**
     * Automatically select the tracking mode based on workload characteristics.
     * Currently defaults to AGGREGATE mode for better scalability, but may be enhanced
     * in the future to use heuristics like executor queue size or expected task count.
     *
     * <p><strong>Selection Strategy:</strong>
     * <ul>
     *   <li>Currently: Always uses AGGREGATE mode</li>
     *   <li>Future: May inspect executor or use configurable threshold</li>
     * </ul>
     *
     * <p><strong>Use When:</strong>
     * <ul>
     *   <li>Workload characteristics are unknown</li>
     *   <li>Task count varies dynamically</li>
     *   <li>Default behavior is acceptable</li>
     * </ul>
     *
     * <p><strong>Example:</strong>
     * <pre>{@code
     * try (TrackedExecutorService tracked = TrackedExecutors.wrap(executor, scope)
     *         .withMode(TrackingMode.AUTO)  // or omit - AUTO is default
     *         .build()) {
     *     tracked.submit(task);  // Mode selected automatically
     * }
     * }</pre>
     */
    AUTO
}
