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
import java.util.stream.IntStream;

/**
 * Example parallel data loader using AtomicLong for thread-safe progress tracking.
 * Demonstrates loading data in parallel using Java streams while safely tracking progress
 * across multiple threads.
 *
 * <p>This class shows how to:
 * <ul>
 *   <li>Use AtomicLong for thread-safe progress counter updates</li>
 *   <li>Process records in parallel using Java parallel streams</li>
 *   <li>Track aggregate progress without synchronization overhead</li>
 *   <li>Implement StatusSource for integration with status tracking</li>
 * </ul>
 *
 * <h2>Thread Safety:</h2>
 * <p>The AtomicLong counter ensures that concurrent threads can safely increment
 * the progress counter without race conditions or lost updates. The volatile state
 * field ensures visibility of state changes across threads.
 *
 * <h2>Usage Example:</h2>
 * <pre>{@code
 * ParallelDataLoader loader = new ParallelDataLoader();
 * try (StatusTracker<ParallelDataLoader> tracker = scope.trackTask(loader)) {
 *     loader.load(); // Loads 1000 records in parallel
 * }
 * }</pre>
 *
 * @see io.nosqlbench.status.userguide.Level7_ParallelExecution
 */
public class ParallelDataLoader implements StatusSource<ParallelDataLoader> {
    private final long totalRecords = 1000;
    private final AtomicLong recordsLoaded = new AtomicLong(0);
    private volatile RunState state = RunState.PENDING;

    /**
     * Returns the current status of this data loader including progress and state.
     * Progress is calculated as the ratio of loaded records to total records.
     *
     * @return a StatusUpdate containing progress (0.0 to 1.0), current state, and this loader instance
     */
    @Override
    public StatusUpdate<ParallelDataLoader> getTaskStatus() {
        double progress = (double) recordsLoaded.get() / totalRecords;
        return new StatusUpdate<>(progress, state, this);
    }

    /**
     * Loads all records in parallel using Java parallel streams.
     * Transitions from PENDING to RUNNING state, processes all records concurrently,
     * then transitions to SUCCESS state upon completion.
     *
     * <p>The method blocks until all records are loaded. Progress can be monitored
     * by calling {@link #getTaskStatus()} from another thread.
     *
     * <p>Thread safety: Multiple threads within the parallel stream safely update
     * the shared recordsLoaded counter using atomic operations.
     */
    public void load() {
        state = RunState.RUNNING;
        // Multiple threads can safely call recordsLoaded.incrementAndGet()
        IntStream.range(0, (int) totalRecords).parallel().forEach(i -> {
            loadRecord(i);
            recordsLoaded.incrementAndGet(); // Atomic - thread-safe
        });
        state = RunState.SUCCESS;
    }

    /**
     * Simulates loading a single record by sleeping for 1 millisecond.
     * This method is called concurrently from multiple threads in the parallel stream.
     *
     * <p>If the thread is interrupted during loading, the interrupt flag is preserved
     * but processing continues for this record.
     *
     * @param i the index of the record to load
     */
    private void loadRecord(int i) {
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Returns the name of this data loader for display in status tracking output.
     *
     * @return the string "ParallelDataLoader"
     */
    public String getName() {
        return "ParallelDataLoader";
    }
}
