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
import io.nosqlbench.status.eventing.RunState;
import io.nosqlbench.status.eventing.StatusSource;
import io.nosqlbench.status.eventing.StatusUpdate;
import io.nosqlbench.status.sinks.ConsoleLoggerSink;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Level 9: Explicit Progress Counter (Inline Pattern)
 *
 * <p>Building on Level 8: Shows the progress counting mechanism explicitly in the main method
 * instead of hiding it inside a separate class. This makes the pattern more visible and easier
 * to understand.
 *
 * <h2>Key Differences from Level 8:</h2>
 * <ul>
 *   <li><strong>NEW:</strong> Progress counter and StatusSource defined inline as lambdas/anonymous classes</li>
 *   <li><strong>NEW:</strong> All tracking logic visible in main method</li>
 *   <li><strong>NEW:</strong> Shows exactly how closures update the shared counter</li>
 *   <li>Same performance characteristics as Level 8</li>
 *   <li>More explicit about the mechanism - educational focus</li>
 * </ul>
 *
 * <h2>Implementation Details:</h2>
 * <ul>
 *   <li><strong>Shared counter:</strong> AtomicLong declared inline and shared by all closures</li>
 *   <li><strong>StatusSource:</strong> Anonymous implementation shows progress calculation</li>
 *   <li><strong>Closure pattern:</strong> Each closure increments counter directly</li>
 *   <li><strong>No hidden logic:</strong> Everything visible in one place</li>
 * </ul>
 *
 * <h2>The Pattern (Visible):</h2>
 * <pre>{@code
 * // 1. Create shared counter
 * AtomicLong completedCount = new AtomicLong(0);
 * long totalTasks = 100;
 *
 * // 2. Create StatusSource that reads counter
 * StatusSource<?> tracker = new StatusSource<>() {
 *     public StatusUpdate<?> getTaskStatus() {
 *         double progress = (double) completedCount.get() / totalTasks;
 *         return new StatusUpdate<>(progress, state, this);
 *     }
 * };
 *
 * // 3. Closures increment counter
 * executor.submit(() -> {
 *     doWork();
 *     completedCount.incrementAndGet(); // Update shared counter
 * });
 * }</pre>
 *
 * <h2>When to Use This Pattern:</h2>
 * <ul>
 *   <li>Quick prototyping - don't want to create a separate class</li>
 *   <li>One-off batch operations</li>
 *   <li>Educational code showing the mechanism</li>
 *   <li>When the operation is simple and self-contained</li>
 * </ul>
 *
 * <h2>When to Use Level 8 Instead:</h2>
 * <ul>
 *   <li>Reusable operation - create a proper class</li>
 *   <li>Complex logic beyond simple counting</li>
 *   <li>Need to unit test the operation independently</li>
 *   <li>Want to encapsulate implementation details</li>
 * </ul>
 *
 * <h2>Performance:</h2>
 * <ul>
 *   <li>Identical to Level 8 - no overhead difference</li>
 *   <li>Anonymous class vs named class has no runtime cost</li>
 *   <li>Same lock-free atomic operations</li>
 * </ul>
 */
public class Level9_ExplicitProgressCounter {

    public static void main(String[] args) throws Exception {
        ExecutorService executor9 = Executors.newFixedThreadPool(10);
        try (StatusContext ctx9 = new StatusContext("explicit-counter-demo")) {
            ctx9.addSink(new ConsoleLoggerSink());

            try (StatusScope scope9 = ctx9.createScope("BatchWork")) {

                // NEW: Explicitly create shared progress counter - visible at surface level
                final AtomicLong completedCount = new AtomicLong(0);
                final long totalTasks = 100;
                final RunState[] state = {RunState.RUNNING}; // Array to allow mutation from lambda

                // NEW: Inline StatusSource - shows exactly how progress is tracked
                // Note: We create a class that properly implements StatusSource<T extends StatusSource<T>>
                class ProgressTracker implements StatusSource<ProgressTracker> {
                    @Override
                    public StatusUpdate<ProgressTracker> getTaskStatus() {
                        double progress = (double) completedCount.get() / totalTasks;
                        return new StatusUpdate<>(progress, state[0], this);
                    }

                    public String getName() {
                        return "BatchWork[" + totalTasks + " tasks]";
                    }
                }

                ProgressTracker progressTracker = new ProgressTracker();
                List<Future<?>> futures9 = new ArrayList<>();

                try (StatusTracker<ProgressTracker> tracker9 = scope9.trackTask(progressTracker)) {
                    // NEW: Each closure directly increments the shared counter
                    for (int i = 0; i < totalTasks; i++) {
                        final int taskId = i;
                        Future<?> future = executor9.submit(() -> {
                            // Simulate work
                            try {
                                Thread.sleep(10);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                state[0] = RunState.FAILED;
                                return;
                            }

                            // NEW: Increment shared counter - this is what drives progress
                            completedCount.incrementAndGet();
                        });
                        futures9.add(future);
                    }

                    // Wait for all closures to complete
                    for (Future<?> f : futures9) {
                        f.get();
                    }

                    // Mark complete
                    state[0] = RunState.SUCCESS;
                }
            }
        } finally {
            executor9.shutdown();
        }
    }
}
