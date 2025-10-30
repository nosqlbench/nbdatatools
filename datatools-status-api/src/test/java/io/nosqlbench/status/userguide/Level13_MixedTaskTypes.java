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
import io.nosqlbench.status.exec.TrackedExecutorService;
import io.nosqlbench.status.exec.TrackedExecutors;
import io.nosqlbench.status.exec.TrackingMode;
import io.nosqlbench.status.sinks.ConsoleLoggerSink;
import io.nosqlbench.status.userguide.fauxtasks.ParallelDataLoader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Level 13: Mixed Task Types
 *
 * <p>Building on Level 11 & 12: Automatically handle both StatusSource and regular tasks in the same executor.
 *
 * <h2>Key Differences from Levels 11-12:</h2>
 * <ul>
 *   <li><strong>NEW:</strong> Mix of StatusSource and regular Callable/Runnable tasks</li>
 *   <li><strong>NEW:</strong> Automatic StatusSource detection per task</li>
 *   <li><strong>NEW:</strong> Regular tasks wrapped for basic lifecycle tracking</li>
 *   <li>Demonstrates transparent handling of different task types</li>
 * </ul>
 *
 * <h2>Implementation Details:</h2>
 * <ul>
 *   <li><strong>StatusSource tasks:</strong> Tracked with self-reported progress (0.0 to 1.0)</li>
 *   <li><strong>Regular tasks:</strong> Wrapped to track lifecycle (pending/running/success/failed)</li>
 *   <li><strong>Detection:</strong> instanceof check at submit() time</li>
 *   <li><strong>Wrapper:</strong> SimpleTaskWrapper provides StatusSource interface for regular tasks</li>
 *   <li><strong>Progress:</strong> Regular tasks show 0% while running, 100% when done</li>
 * </ul>
 *
 * <h2>Task Type Handling:</h2>
 * <pre>{@code
 * // StatusSource task (detailed progress):
 * class DataLoader implements StatusSource<DataLoader>, Callable<Data> {
 *     private volatile double progress = 0.0;
 *     private volatile RunState state = RunState.PENDING;
 *
 *     public Data call() {
 *         state = RunState.RUNNING;
 *         for (int i = 0; i < 100; i++) {
 *             processRecord(i);
 *             progress = (i + 1) / 100.0;  // Reports actual progress
 *         }
 *         state = RunState.SUCCESS;
 *         return data;
 *     }
 *
 *     public StatusUpdate<DataLoader> getTaskStatus() {
 *         return new StatusUpdate<>(progress, state, this);
 *     }
 * }
 *
 * // Regular task (lifecycle only):
 * Callable<Data> simpleTask = () -> {
 *     // No progress tracking
 *     return processData();
 * };
 *
 * // Both work seamlessly:
 * Future<Data> f1 = tracked.submit(new DataLoader());      // Shows 0-100% progress
 * Future<Data> f2 = tracked.submit(simpleTask);           // Shows pending/running/success
 * }</pre>
 *
 * <h2>StatusSource Detection Benefits:</h2>
 * <ul>
 *   <li><strong>Flexibility:</strong> Mix task types freely</li>
 *   <li><strong>Gradual migration:</strong> Add StatusSource to critical tasks incrementally</li>
 *   <li><strong>Zero cost:</strong> Regular tasks have minimal tracking overhead</li>
 *   <li><strong>No changes required:</strong> Existing code works without modification</li>
 * </ul>
 *
 * <h2>Progress Visibility Comparison:</h2>
 * <ul>
 *   <li><strong>StatusSource task:</strong> "DataLoader [████████░░] 80% - RUNNING"</li>
 *   <li><strong>Regular task:</strong> "Callable [░░░░░░░░░░] 0% - RUNNING"</li>
 *   <li><strong>Regular task (done):</strong> "Callable [██████████] 100% - SUCCESS"</li>
 * </ul>
 *
 * <h2>Performance Impact:</h2>
 * <ul>
 *   <li><strong>StatusSource tasks:</strong> Same as Level 11 (full progress tracking)</li>
 *   <li><strong>Regular tasks:</strong> Minimal - just lifecycle state tracking</li>
 *   <li><strong>Detection overhead:</strong> Single instanceof check (~5ns)</li>
 *   <li><strong>Overall:</strong> No measurable difference vs unwrapped executor</li>
 * </ul>
 *
 * <h2>When to Implement StatusSource:</h2>
 * <ul>
 *   <li>Task duration >1 second (progress updates are meaningful)</li>
 *   <li>Batch processing with clear progress milestones</li>
 *   <li>Long-running computations (machine learning, data processing)</li>
 *   <li>Critical tasks needing detailed monitoring</li>
 *   <li>Tasks where users ask "is it stuck or just slow?"</li>
 * </ul>
 *
 * <h2>When Regular Tasks Are Fine:</h2>
 * <ul>
 *   <li>Task duration <500ms (too fast for progress to matter)</li>
 *   <li>Fire-and-forget operations</li>
 *   <li>Tasks with no intermediate milestones</li>
 *   <li>Low-priority background work</li>
 * </ul>
 */
public class Level13_MixedTaskTypes {
    private static final Logger logger = LogManager.getLogger(Level13_MixedTaskTypes.class);

    public static void main(String[] args) throws Exception {
        ExecutorService executor13 = Executors.newFixedThreadPool(4);
        try (StatusContext ctx13 = new StatusContext("mixed-task-types")) {
            ctx13.addSink(new ConsoleLoggerSink());

            try (StatusScope scope13 = ctx13.createScope("MixedWorkload");
                 TrackedExecutorService tracked13 = TrackedExecutors.wrap(executor13, scope13)
                     .withMode(TrackingMode.INDIVIDUAL)  // Individual mode to see each task
                     .build()) {

                // Type 1: StatusSource task with detailed progress
                ParallelDataLoader statusSourceTask1 = new ParallelDataLoader();
                ParallelDataLoader statusSourceTask2 = new ParallelDataLoader();

                // Type 2: Regular Callable tasks (no progress detail)
                Callable<String> regularTask1 = () -> {
                    Thread.sleep(100);
                    return "Regular result 1";
                };

                Callable<String> regularTask2 = () -> {
                    Thread.sleep(100);
                    return "Regular result 2";
                };

                // Submit mix of task types - all tracked automatically
                logger.info("Submitting mixed task types...\n");

                Future<?> f1 = tracked13.submit(() -> statusSourceTask1.load());  // StatusSource (detailed)
                Future<String> f2 = tracked13.submit(regularTask1);               // Regular (lifecycle only)
                Future<?> f3 = tracked13.submit(() -> statusSourceTask2.load());  // StatusSource (detailed)
                Future<String> f4 = tracked13.submit(regularTask2);               // Regular (lifecycle only)

                // Wait for all
                f1.get();
                String result2 = f2.get();
                f3.get();
                String result4 = f4.get();

                logger.info("\n\nResults:");
                logger.info("  Regular Task 1: " + result2);
                logger.info("  Regular Task 2: " + result4);
                logger.info("  StatusSource tasks completed with detailed progress");

                logger.info("\nStatistics:");
                logger.info(tracked13.getStatistics());

                logger.info("\nKey Observation:");
                logger.info("  - StatusSource tasks showed incremental progress (0-100%)");
                logger.info("  - Regular tasks showed only lifecycle (pending/running/success)");
                logger.info("  - Both tracked seamlessly without code changes");
            }
        } finally {
            executor13.shutdown();
        }
    }
}
