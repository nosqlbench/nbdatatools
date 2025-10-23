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

package io.nosqlbench.status.userguide.fauxtasks;

import io.nosqlbench.status.eventing.RunState;
import io.nosqlbench.status.eventing.StatusSource;
import io.nosqlbench.status.eventing.StatusUpdate;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Example parent task that tracks progress of many lightweight closures.
 * Demonstrates the pattern of aggregating progress from numerous simple tasks
 * without requiring each closure to implement StatusSource.
 *
 * <p>This is ideal for scenarios with hundreds or thousands of small tasks
 * where individual task tracking would be overkill.
 *
 * <h2>Usage Pattern:</h2>
 * <pre>{@code
 * ClosureBasedProcessor processor = new ClosureBasedProcessor(100);
 * try (StatusTracker<ClosureBasedProcessor> tracker = scope.trackTask(processor)) {
 *     for (int i = 0; i < 100; i++) {
 *         executor.submit(() -> processor.processClosure(i));
 *     }
 *     // Wait for completion
 *     processor.markComplete();
 * }
 * }</pre>
 *
 * @see io.nosqlbench.status.userguide.Level8_ParallelClosures
 */
public class ClosureBasedProcessor implements StatusSource<ClosureBasedProcessor> {
    private final long totalClosures;
    private final AtomicLong closuresCompleted = new AtomicLong(0);
    private volatile RunState state = RunState.PENDING;

    /**
     * Creates a closure-based processor that will track completion of the specified number of closures.
     * The processor starts in RUNNING state and expects closures to call {@link #processClosure(int)}
     * to update progress.
     *
     * @param totalClosures the total number of closures that will be executed
     */
    public ClosureBasedProcessor(long totalClosures) {
        this.totalClosures = totalClosures;
        this.state = RunState.RUNNING;
    }

    /**
     * Returns the current status of this processor including progress based on completed closures.
     * Progress is calculated as the ratio of completed closures to total closures.
     *
     * @return a StatusUpdate containing progress (0.0 to 1.0), current state, and this processor instance
     */
    @Override
    public StatusUpdate<ClosureBasedProcessor> getTaskStatus() {
        double progress = (double) closuresCompleted.get() / totalClosures;
        return new StatusUpdate<>(progress, state, this);
    }

    /**
     * Called by each closure to update the parent's progress counter.
     * This method is thread-safe and can be called concurrently from multiple threads.
     * Simulates lightweight work and atomically increments the completion counter.
     *
     * <p>If the thread is interrupted during processing, the processor state is set to FAILED.
     *
     * @param closureId the ID of the closure being processed (used for demonstration purposes)
     */
    public void processClosure(int closureId) {
        // Simulate lightweight work
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            state = RunState.FAILED;
            return;
        }

        // Atomically increment the completion counter
        closuresCompleted.incrementAndGet();
    }

    /**
     * Marks this processor as complete by transitioning the state to SUCCESS.
     * Should be called after all closures have been submitted and completed.
     * This allows the StatusTracker to report final completion status.
     */
    public void markComplete() {
        state = RunState.SUCCESS;
    }

    /**
     * Returns a descriptive name for this processor including the total number of closures.
     *
     * @return a string representation of this processor in the format "ClosureBasedProcessor[N closures]"
     */
    public String getName() {
        return "ClosureBasedProcessor[" + totalClosures + " closures]";
    }
}
