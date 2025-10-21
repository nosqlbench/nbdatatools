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
import io.nosqlbench.status.userguide.fauxtasks.ClosureBasedProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Level 8: Parallel Closures with Progress Counter
 *
 * <p>Building on Level 7: Track many lightweight closure-based tasks using a single
 * parent StatusSource with an internal progress counter.
 *
 * <h2>Key Differences from Level 7:</h2>
 * <ul>
 *   <li><strong>NEW:</strong> Single parent task instead of multiple StatusSource tasks</li>
 *   <li><strong>NEW:</strong> Progress counter tracks many lightweight closures</li>
 *   <li><strong>NEW:</strong> No StatusSource implementation in closures (zero overhead)</li>
 *   <li><strong>NEW:</strong> Demonstrates scalability to hundreds/thousands of subtasks</li>
 *   <li>Parent task aggregates progress from all closures</li>
 * </ul>
 *
 * <h2>Implementation Details:</h2>
 * <ul>
 *   <li><strong>Parent task:</strong> Single ClosureBasedProcessor implements StatusSource</li>
 *   <li><strong>Closure execution:</strong> 100 lightweight Runnable closures submitted to executor</li>
 *   <li><strong>Progress tracking:</strong> Parent's AtomicLong updated by each closure completion</li>
 *   <li><strong>No task state:</strong> Closures are stateless, no StatusSource overhead</li>
 *   <li><strong>Coordination:</strong> Future.get() ensures all closures complete</li>
 * </ul>
 *
 * <h2>Performance Overhead:</h2>
 * <ul>
 *   <li><strong>CPU:</strong> Only parent task polled (~1ms per poll), closures have zero overhead</li>
 *   <li><strong>Memory:</strong> ~1KB for parent task + ~100 bytes per Future (minimal)</li>
 *   <li><strong>Thread count:</strong> 1 monitor thread + N worker threads from ExecutorService</li>
 *   <li><strong>Closure overhead:</strong> ZERO - closures just increment counter</li>
 *   <li><strong>Scalability:</strong> Can track thousands of closures efficiently</li>
 *   <li><strong>Overall impact:</strong> ~0.01% overhead regardless of closure count</li>
 * </ul>
 *
 * <h2>When to Use:</h2>
 * <ul>
 *   <li>Many lightweight tasks (hundreds to thousands)</li>
 *   <li>Tasks are too simple to warrant individual StatusSource implementations</li>
 *   <li>Only aggregate progress matters, not per-task progress</li>
 *   <li>Want minimal overhead for high-throughput scenarios</li>
 *   <li>Batch operations, parallel streams, work-stealing patterns</li>
 * </ul>
 *
 * <h2>Comparison to Level 7:</h2>
 * <ul>
 *   <li><strong>Level 7:</strong> Each task implements StatusSource (good for <10 tasks)</li>
 *   <li><strong>Level 8:</strong> Single parent tracks many closures (good for 100+ tasks)</li>
 *   <li><strong>Rule:</strong> Use Level 7 for distinct tasks, Level 8 for batch operations</li>
 * </ul>
 *
 * <h2>Design Pattern:</h2>
 * <pre>{@code
 * // Parent task with progress counter
 * public class ParentTask implements StatusSource<ParentTask> {
 *     private AtomicLong completedCount = new AtomicLong(0);
 *     private long totalTasks = 100;
 *
 *     @Override
 *     public StatusUpdate<ParentTask> getTaskStatus() {
 *         double progress = (double) completedCount.get() / totalTasks;
 *         return new StatusUpdate<>(progress, state, this);
 *     }
 *
 *     public void execute() {
 *         // Submit lightweight closures
 *         for (int i = 0; i < totalTasks; i++) {
 *             executor.submit(() -> {
 *                 doWork();
 *                 completedCount.incrementAndGet(); // Update parent's counter
 *             });
 *         }
 *     }
 * }
 * }</pre>
 */
public class Level8_ParallelClosures {

    public static void main(String[] args) throws Exception {
        ExecutorService executor8 = Executors.newFixedThreadPool(10);
        try (StatusContext ctx8 = new StatusContext("closure-processing")) {
            ctx8.addSink(new ConsoleLoggerSink());

            try (StatusScope scope8 = ctx8.createScope("BatchProcessor")) {

                // NEW: Single parent task that will spawn many closures
                ClosureBasedProcessor processor8 = new ClosureBasedProcessor(100);
                List<Future<?>> futures8 = new ArrayList<>();

                try (StatusTracker<ClosureBasedProcessor> tracker8 = scope8.trackTask(processor8)) {
                    // NEW: Submit many lightweight closures (not StatusSource implementations)
                    for (int i = 0; i < 100; i++) {
                        final int taskId = i;
                        Future<?> future = executor8.submit(() -> {
                            processor8.processClosure(taskId); // Closure updates parent's counter
                        });
                        futures8.add(future);
                    }

                    // Wait for all closures to complete
                    for (Future<?> f : futures8) {
                        f.get();
                    }

                    // Mark parent task as complete
                    processor8.markComplete();
                }
            }
        } finally {
            executor8.shutdown();
        }
    }
}
