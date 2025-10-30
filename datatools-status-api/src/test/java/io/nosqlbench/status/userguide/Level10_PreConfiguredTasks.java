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

package io.nosqlbench.status.userguide;

import io.nosqlbench.status.StatusContext;
import io.nosqlbench.status.StatusScope;
import io.nosqlbench.status.StatusTracker;
import io.nosqlbench.status.sinks.ConsoleLoggerSink;
import io.nosqlbench.status.userguide.fauxtasks.DataLoader;
import io.nosqlbench.status.userguide.fauxtasks.DataProcessor;
import io.nosqlbench.status.userguide.fauxtasks.DataValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Level 10: Pre-Configured Tasks
 *
 * <p>Building on previous levels: Configure and register all tasks upfront before execution,
 * making them visible in PENDING state. This provides a complete view of the workflow before
 * any work begins.
 *
 * <h2>Key Differences from Previous Levels:</h2>
 * <ul>
 *   <li><strong>NEW:</strong> Tasks are created and tracked BEFORE execution starts</li>
 *   <li><strong>NEW:</strong> All tasks visible in PENDING state initially</li>
 *   <li><strong>NEW:</strong> StatusTrackers stored in a list for later execution</li>
 *   <li><strong>NEW:</strong> Clear separation of configuration phase vs execution phase</li>
 *   <li>Provides complete visibility into planned work upfront</li>
 * </ul>
 *
 * <h2>Implementation Details:</h2>
 * <ul>
 *   <li><strong>Configuration phase:</strong> Create all tasks and trackers upfront</li>
 *   <li><strong>Visibility:</strong> Tasks appear in monitoring immediately as PENDING</li>
 *   <li><strong>Execution phase:</strong> Execute tasks sequentially or in parallel</li>
 *   <li><strong>Resource management:</strong> Trackers auto-close when done</li>
 * </ul>
 *
 * <h2>Benefits:</h2>
 * <ul>
 *   <li><strong>Predictability:</strong> Users see the complete workflow before it starts</li>
 *   <li><strong>Planning:</strong> Total task count and estimated duration visible upfront</li>
 *   <li><strong>Debugging:</strong> Easy to see which tasks are stuck in PENDING</li>
 *   <li><strong>Monitoring:</strong> Full pipeline visibility from the start</li>
 * </ul>
 *
 * <h2>The Pattern:</h2>
 * <pre>{@code
 * // Phase 1: Configuration - Create and track all tasks
 * List<TaskExecution> executions = new ArrayList<>();
 * for (Task task : tasks) {
 *     StatusTracker<Task> tracker = scope.trackTask(task);
 *     executions.add(new TaskExecution(task, tracker));
 * }
 * // At this point, all tasks are visible as PENDING
 *
 * // Phase 2: Execution - Execute tasks when ready
 * for (TaskExecution exec : executions) {
 *     exec.task.execute(); // Task transitions to RUNNING -> SUCCESS
 *     exec.tracker.close(); // Mark complete
 * }
 * }</pre>
 *
 * <h2>When to Use:</h2>
 * <ul>
 *   <li>ETL pipelines where all stages are known upfront</li>
 *   <li>Batch processing with predictable task sequences</li>
 *   <li>Workflows that need progress estimation before starting</li>
 *   <li>Systems where task configuration is separate from execution</li>
 * </ul>
 *
 * <h2>Comparison to Previous Patterns:</h2>
 * <ul>
 *   <li><strong>Levels 1-7:</strong> Track and execute tasks together</li>
 *   <li><strong>Level 8-9:</strong> Track parent task, closures execute immediately</li>
 *   <li><strong>Level 10:</strong> Track all tasks first, execute later</li>
 * </ul>
 */
public class Level10_PreConfiguredTasks {
    private static final Logger logger = LogManager.getLogger(Level10_PreConfiguredTasks.class);

    /**
     * Helper class to pair tasks with their trackers for deferred execution.
     * Encapsulates a task, its StatusTracker, and the execution logic to allow
     * configuration phase (task registration) to be separated from execution phase.
     *
     * @param <T> the type of the task being tracked, must implement StatusSource
     */
    private static class TaskExecution<T> implements AutoCloseable {
        final T task;
        final StatusTracker<T> tracker;
        final Runnable executor;

        /**
         * Creates a TaskExecution wrapper for deferred execution.
         *
         * @param task the task to be executed
         * @param tracker the StatusTracker monitoring this task
         * @param executor the Runnable that performs the actual task execution
         */
        TaskExecution(T task, StatusTracker<T> tracker, Runnable executor) {
            this.task = task;
            this.tracker = tracker;
            this.executor = executor;
        }

        /**
         * Executes the task by running the configured executor.
         * Task state transitions from PENDING to RUNNING to SUCCESS/FAILED during execution.
         */
        void execute() {
            executor.run();
        }

        /**
         * Closes the StatusTracker, marking the task as complete.
         * Should be called after execute() completes to properly clean up tracking resources.
         */
        @Override
        public void close() {
            tracker.close();
        }
    }

    /**
     * Demonstrates the pre-configured tasks pattern where all tasks are registered upfront
     * before any execution begins. This provides complete visibility into the workflow plan.
     *
     * <p>This example shows how to:
     * <ul>
     *   <li>Create all tasks in a configuration phase</li>
     *   <li>Register tasks with StatusTrackers before execution (visible as PENDING)</li>
     *   <li>Store task/tracker pairs for deferred execution</li>
     *   <li>Execute tasks in sequence after configuration is complete</li>
     * </ul>
     *
     * <p>Benefits of this pattern:
     * <ul>
     *   <li>Complete workflow visibility before any work starts</li>
     *   <li>Easy to see total task count and estimate duration</li>
     *   <li>Clear separation between configuration and execution</li>
     *   <li>Useful for ETL pipelines and batch processing with known stages</li>
     * </ul>
     *
     * @param args command line arguments (not used)
     */
    public static void main(String[] args) {
        try (StatusContext ctx10 = new StatusContext("pre-configured-pipeline")) {
            ctx10.addSink(new ConsoleLoggerSink());

            try (StatusScope scope10 = ctx10.createScope("DataPipeline")) {

                // NEW: CONFIGURATION PHASE - Create and register all tasks upfront
                List<TaskExecution<?>> executions = new ArrayList<>();

                // Configure all tasks before executing any of them
                DataLoader loader10 = new DataLoader();
                DataValidator validator10 = new DataValidator();
                DataProcessor processor10 = new DataProcessor();

                // Register tasks with tracking - they appear as PENDING immediately
                executions.add(new TaskExecution<>(
                    loader10,
                    scope10.trackTask(loader10),
                    loader10::load
                ));

                executions.add(new TaskExecution<>(
                    validator10,
                    scope10.trackTask(validator10),
                    validator10::validate
                ));

                executions.add(new TaskExecution<>(
                    processor10,
                    scope10.trackTask(processor10),
                    processor10::process
                ));

                // At this point, all 3 tasks are visible in the monitoring system as PENDING
                logger.info("\nAll tasks configured and visible as PENDING. Starting execution...\n");

                // Optional: Add a small delay to see all tasks in PENDING state
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                // NEW: EXECUTION PHASE - Execute tasks in order
                for (TaskExecution<?> execution : executions) {
                    execution.execute();  // Task executes and transitions PENDING -> RUNNING -> SUCCESS
                    execution.close();    // Close tracker when done
                }

                logger.info("\nAll tasks completed.\n");
            }
        }
    }
}
