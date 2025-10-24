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
import io.nosqlbench.status.sinks.ConsoleLoggerSink;
import io.nosqlbench.status.userguide.fauxtasks.ParallelDataLoader;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Level 11: Tracked Executor Basics
 *
 * <p>Building on Level 7: Eliminate manual tracker creation boilerplate with TrackedExecutorService.
 *
 * <h2>Key Differences from Level 7:</h2>
 * <ul>
 *   <li><strong>NEW:</strong> TrackedExecutorService wraps standard ExecutorService</li>
 *   <li><strong>NEW:</strong> No manual StatusTracker creation needed</li>
 *   <li><strong>NEW:</strong> Automatic resource cleanup (implements AutoCloseable)</li>
 *   <li><strong>REMOVED:</strong> Explicit try-with-resources for each tracker</li>
 *   <li><strong>REMOVED:</strong> Manual tracker.close() calls</li>
 *   <li>Tasks are tracked automatically on submission</li>
 * </ul>
 *
 * <h2>Implementation Details:</h2>
 * <ul>
 *   <li><strong>Wrapper pattern:</strong> TrackedExecutors.wrap() decorates ExecutorService</li>
 *   <li><strong>Automatic tracking:</strong> submit() transparently creates StatusTracker</li>
 *   <li><strong>StatusSource detection:</strong> Detects if task implements StatusSource</li>
 *   <li><strong>Lifecycle management:</strong> Trackers auto-closed when tasks complete</li>
 *   <li><strong>Statistics tracking:</strong> Built-in metrics (submitted, completed, failed)</li>
 * </ul>
 *
 * <h2>Boilerplate Reduction:</h2>
 * <pre>{@code
 * // Level 7 (Manual - 7 lines per task):
 * ParallelDataLoader loader = new ParallelDataLoader();
 * Future<?> future = executor.submit(() -> {
 *     try (StatusTracker<ParallelDataLoader> tracker = scope.trackTask(loader)) {
 *         loader.load();
 *     }
 * });
 *
 * // Level 11 (Automatic - 2 lines per task):
 * ParallelDataLoader loader = new ParallelDataLoader();
 * Future<?> future = tracked.submit(() -> loader.load());
 * }</pre>
 *
 * <h2>Performance Overhead:</h2>
 * <ul>
 *   <li><strong>CPU:</strong> Negligible - wrapper adds ~10ns per submit()</li>
 *   <li><strong>Memory:</strong> Minimal - 1 map entry per active task</li>
 *   <li><strong>Thread count:</strong> Same as Level 7 (no additional threads)</li>
 *   <li><strong>Overall impact:</strong> <0.01% for typical tasks (>1ms duration)</li>
 * </ul>
 *
 * <h2>When to Use:</h2>
 * <ul>
 *   <li>Any ExecutorService-based parallel execution</li>
 *   <li>Want automatic task tracking without boilerplate</li>
 *   <li>Need statistics (submitted, completed, failed counts)</li>
 *   <li>Moderate task count (≤50 tasks for detailed tracking)</li>
 * </ul>
 *
 * <h2>Comparison:</h2>
 * <ul>
 *   <li><strong>Level 7:</strong> Manual tracker creation (more control, more code)</li>
 *   <li><strong>Level 11:</strong> Automatic tracking (less code, easier to use)</li>
 *   <li><strong>Trade-off:</strong> Convenience vs explicitness</li>
 * </ul>
 */
public class Level11_TrackedExecutorBasics {

    public static void main(String[] args) throws Exception {
        ExecutorService executor11 = Executors.newFixedThreadPool(3);
        try (StatusContext ctx11 = new StatusContext("tracked-executor-basics")) {
            ctx11.addSink(new ConsoleLoggerSink());

            try (StatusScope scope11 = ctx11.createScope("DataLoading");
                 // NEW: Wrap executor for automatic tracking
                 TrackedExecutorService tracked11 = TrackedExecutors.wrap(executor11, scope11).build()) {

                // NEW: Simple submit - tracking happens automatically
                ParallelDataLoader loader1 = new ParallelDataLoader();
                ParallelDataLoader loader2 = new ParallelDataLoader();
                ParallelDataLoader loader3 = new ParallelDataLoader();

                // Submit tasks - no manual tracker creation needed
                Future<?> f1 = tracked11.submit(() -> loader1.load());
                Future<?> f2 = tracked11.submit(() -> loader2.load());
                Future<?> f3 = tracked11.submit(() -> loader3.load());

                // Wait for completion
                f1.get();
                f2.get();
                f3.get();

                // NEW: Access statistics
                System.out.println("\nExecution Statistics:");
                System.out.println(tracked11.getStatistics());
            } // Tracked executor auto-closes and cleans up all trackers
        } finally {
            executor11.shutdown();
        }
    }
}
