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
 */
public class ClosureBasedProcessor implements StatusSource<ClosureBasedProcessor> {
    private final long totalClosures;
    private final AtomicLong closuresCompleted = new AtomicLong(0);
    private volatile RunState state = RunState.PENDING;

    public ClosureBasedProcessor(long totalClosures) {
        this.totalClosures = totalClosures;
        this.state = RunState.RUNNING;
    }

    @Override
    public StatusUpdate<ClosureBasedProcessor> getTaskStatus() {
        double progress = (double) closuresCompleted.get() / totalClosures;
        return new StatusUpdate<>(progress, state, this);
    }

    /**
     * Called by each closure to update the parent's progress counter.
     * This method is thread-safe and can be called concurrently.
     *
     * @param closureId the ID of the closure being processed
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
     * Called after all closures have been submitted and completed.
     */
    public void markComplete() {
        state = RunState.SUCCESS;
    }

    public String getName() {
        return "ClosureBasedProcessor[" + totalClosures + " closures]";
    }
}
