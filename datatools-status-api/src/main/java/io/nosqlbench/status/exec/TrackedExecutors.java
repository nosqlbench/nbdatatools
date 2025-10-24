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

import java.util.Objects;
import java.util.concurrent.ExecutorService;

/**
 * Factory class for creating {@link TrackedExecutorService} instances that automatically
 * track task execution using the StatusContext API.
 *
 * <p>This class provides a fluent builder API for configuring tracked executors with
 * different tracking modes and options.
 *
 * <h2>Tracking Modes</h2>
 * <ul>
 *   <li><strong>INDIVIDUAL:</strong> Each task gets its own StatusTracker. Best for ≤50 tasks
 *       where individual progress and state matter.</li>
 *   <li><strong>AGGREGATE:</strong> All tasks share a single tracker. Best for 100+ lightweight
 *       tasks where only aggregate metrics matter.</li>
 *   <li><strong>AUTO:</strong> Automatically selects based on expected task count (default).</li>
 * </ul>
 *
 * <h2>Usage Examples</h2>
 *
 * <h3>Simple Usage (Auto mode)</h3>
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
 * <h3>Individual Task Tracking</h3>
 * <pre>{@code
 * try (TrackedExecutorService tracked = TrackedExecutors.wrap(executor, scope)
 *         .withMode(TrackingMode.INDIVIDUAL)
 *         .build()) {
 *
 *     Future<Data> f1 = tracked.submit(new DataLoadTask());
 *     Future<Data> f2 = tracked.submit(new DataTransformTask());
 *     // Each task tracked separately with detailed progress
 * }
 * }</pre>
 *
 * <h3>Aggregate Tracking for High Volume</h3>
 * <pre>{@code
 * try (TrackedExecutorService tracked = TrackedExecutors.wrap(executor, scope)
 *         .withMode(TrackingMode.AGGREGATE)
 *         .withTaskGroupName("Records")
 *         .build()) {
 *
 *     for (int i = 0; i < 1000; i++) {
 *         tracked.submit(() -> processRecord(i));
 *     }
 *     // All 1000 tasks tracked as single "Records" group
 * }
 * }</pre>
 *
 * @see TrackedExecutorService
 * @see TrackingMode
 * @since 4.0.0
 */
public final class TrackedExecutors {

    // Private constructor - use static factory methods
    private TrackedExecutors() {
    }

    /**
     * Creates a builder for wrapping an ExecutorService with status tracking.
     * The resulting tracked executor will submit tasks under the specified scope.
     *
     * @param executor the executor service to wrap
     * @param scope the status scope under which tasks will be tracked
     * @return a builder for configuring the tracked executor
     * @throws NullPointerException if executor or scope is null
     */
    public static Builder wrap(ExecutorService executor, StatusScope scope) {
        return new Builder(executor, scope);
    }

    /**
     * Builder for configuring and creating {@link TrackedExecutorService} instances.
     * Provides a fluent API for setting tracking mode and options.
     */
    public static final class Builder {
        private final ExecutorService executor;
        private final StatusScope scope;
        private TrackingMode mode = TrackingMode.AUTO;
        private int aggregateThreshold = 50;
        private String taskGroupName = "Tasks";

        Builder(ExecutorService executor, StatusScope scope) {
            this.executor = Objects.requireNonNull(executor, "executor");
            this.scope = Objects.requireNonNull(scope, "scope");
        }

        /**
         * Sets the tracking mode.
         * Defaults to AUTO if not specified.
         *
         * @param mode the tracking mode to use
         * @return this builder
         * @throws NullPointerException if mode is null
         */
        public Builder withMode(TrackingMode mode) {
            this.mode = Objects.requireNonNull(mode, "mode");
            return this;
        }

        /**
         * Sets the threshold for auto-mode selection.
         * When in AUTO mode, individual tracking is used if expected task count is below
         * this threshold; otherwise aggregate tracking is used.
         * Default is 50 tasks.
         *
         * @param threshold the task count threshold
         * @return this builder
         * @throws IllegalArgumentException if threshold is less than 1
         */
        public Builder withAggregateThreshold(int threshold) {
            if (threshold < 1) {
                throw new IllegalArgumentException("Threshold must be at least 1");
            }
            this.aggregateThreshold = threshold;
            return this;
        }

        /**
         * Sets the display name for the task group when using aggregate tracking.
         * This name appears in status updates and logs.
         * Default is "Tasks".
         *
         * @param name the task group name
         * @return this builder
         * @throws NullPointerException if name is null
         */
        public Builder withTaskGroupName(String name) {
            this.taskGroupName = Objects.requireNonNull(name, "name");
            return this;
        }

        /**
         * Builds the configured TrackedExecutorService.
         *
         * @return a new tracked executor service
         */
        public TrackedExecutorService build() {
            switch (mode) {
                case INDIVIDUAL:
                    return new IndividualTaskTrackedExecutor(executor, scope);
                case AGGREGATE:
                    return new AggregateTrackedExecutor(executor, scope, taskGroupName);
                case AUTO:
                default:
                    // For AUTO mode, default to aggregate as it's more scalable
                    // In future, could inspect executor queue size or other heuristics
                    return new AggregateTrackedExecutor(executor, scope, taskGroupName);
            }
        }
    }
}
