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
import io.nosqlbench.status.exec.TaskStatistics;
import io.nosqlbench.status.exec.TrackedExecutorService;
import io.nosqlbench.status.exec.TrackedExecutors;
import io.nosqlbench.status.exec.TrackingMode;
import io.nosqlbench.status.sinks.ConsoleLoggerSink;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Level 12: Aggregate Tracking for High Volume
 *
 * <p>Building on Level 11: Use AGGREGATE mode to efficiently track hundreds or thousands of lightweight tasks.
 *
 * <h2>Key Differences from Level 11:</h2>
 * <ul>
 *   <li><strong>NEW:</strong> TrackingMode.AGGREGATE for bulk task tracking</li>
 *   <li><strong>NEW:</strong> Single StatusTracker for all tasks (not one per task)</li>
 *   <li><strong>NEW:</strong> Custom task group name ("Record Processing")</li>
 *   <li><strong>CHANGE:</strong> 100 tasks instead of 3 (high volume scenario)</li>
 *   <li>Progress shows aggregate completion ratio</li>
 * </ul>
 *
 * <h2>Implementation Details:</h2>
 * <ul>
 *   <li><strong>Single tracker:</strong> One AggregateTaskTracker for all submitted tasks</li>
 *   <li><strong>Progress calculation:</strong> completed tasks / submitted tasks</li>
 *   <li><strong>State tracking:</strong> PENDING → RUNNING → SUCCESS/FAILED based on aggregate</li>
 *   <li><strong>Lightweight:</strong> No per-task StatusTracker overhead</li>
 *   <li><strong>Display:</strong> Shows as "Task Group [N/M completed]"</li>
 * </ul>
 *
 * <h2>Tracking Mode Comparison:</h2>
 * <pre>{@code
 * // INDIVIDUAL mode (Level 11):
 * - Creates N StatusTracker instances (one per task)
 * - Monitor polls N times per interval
 * - Sinks receive N status updates per poll
 * - Memory: ~200 bytes × N tasks
 * - Best for: ≤50 tasks with meaningful individual progress
 *
 * // AGGREGATE mode (Level 12):
 * - Creates 1 AggregateTaskTracker (shared)
 * - Monitor polls once per interval
 * - Sinks receive 1 status update per poll
 * - Memory: ~200 bytes (constant)
 * - Best for: 100+ lightweight tasks
 * }</pre>
 *
 * <h2>Performance Benefits:</h2>
 * <ul>
 *   <li><strong>Memory:</strong> O(1) vs O(N) - constant memory regardless of task count</li>
 *   <li><strong>CPU:</strong> O(1) vs O(N) - constant polling overhead</li>
 *   <li><strong>Network/IO:</strong> O(1) vs O(N) - fewer sink updates</li>
 *   <li><strong>Scalability:</strong> Handles 10,000+ tasks with same overhead as 10 tasks</li>
 * </ul>
 *
 * <h2>Concrete Example (1000 tasks):</h2>
 * <ul>
 *   <li><strong>INDIVIDUAL mode:</strong> 1000 trackers × 200 bytes = 200 KB memory</li>
 *   <li><strong>AGGREGATE mode:</strong> 1 tracker × 200 bytes = 200 bytes memory</li>
 *   <li><strong>Savings:</strong> 99.9% memory reduction</li>
 *   <li><strong>CPU savings:</strong> 99.9% fewer monitor polls</li>
 * </ul>
 *
 * <h2>When to Use AGGREGATE:</h2>
 * <ul>
 *   <li>100+ lightweight tasks (each <100ms)</li>
 *   <li>Only aggregate metrics needed (not per-task detail)</li>
 *   <li>Batch processing scenarios</li>
 *   <li>High-throughput workloads</li>
 *   <li>Want to minimize monitoring overhead</li>
 * </ul>
 *
 * <h2>When to Use INDIVIDUAL (Level 11):</h2>
 * <ul>
 *   <li>≤50 tasks</li>
 *   <li>Need per-task progress visibility</li>
 *   <li>Tasks implement StatusSource with meaningful progress</li>
 *   <li>Debugging or detailed monitoring required</li>
 * </ul>
 */
public class Level12_AggregateTracking {

    public static void main(String[] args) throws Exception {
        ExecutorService executor12 = Executors.newFixedThreadPool(20);
        try (StatusContext ctx12 = new StatusContext("aggregate-tracking")) {
            ctx12.addSink(new ConsoleLoggerSink());

            try (StatusScope scope12 = ctx12.createScope("BatchProcessing");
                 // NEW: AGGREGATE mode for high-volume scenarios
                 TrackedExecutorService tracked12 = TrackedExecutors.wrap(executor12, scope12)
                     .withMode(TrackingMode.AGGREGATE)
                     .withTaskGroupName("Record Processing")  // NEW: Custom display name
                     .build()) {

                List<Future<?>> futures12 = new ArrayList<>();

                // Submit 100 lightweight tasks
                for (int i = 0; i < 100; i++) {
                    final int recordId = i;
                    Future<?> future = tracked12.submit(() -> {
                        // Simulate lightweight record processing
                        try {
                            Thread.sleep(50);
                            processRecord(recordId);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                    });
                    futures12.add(future);
                }

                // Wait for all
                for (Future<?> f : futures12) {
                    f.get();
                }

                // NEW: Statistics show aggregate metrics
                TaskStatistics stats12 = tracked12.getStatistics();
                System.out.println("\nBatch Processing Results:");
                System.out.println("  Submitted: " + stats12.getSubmitted());
                System.out.println("  Completed: " + stats12.getCompleted());
                System.out.println("  Failed: " + stats12.getFailed());
                System.out.println("  Completion Rate: " + (stats12.getCompletionRate() * 100) + "%");

            } // Single tracker cleanup (not 100 trackers!)
        } finally {
            executor12.shutdown();
        }
    }

    private static void processRecord(int recordId) {
        // Simulate record processing
        int hash = ("record-" + recordId).hashCode();
        // Busy work to simulate processing
        for (int i = 0; i < 1000; i++) {
            hash = hash * 31 + i;
        }
    }
}
