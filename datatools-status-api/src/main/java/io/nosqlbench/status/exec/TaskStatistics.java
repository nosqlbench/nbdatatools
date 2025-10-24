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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe statistics about task execution in a {@link TrackedExecutorService}.
 * Tracks counts of submitted, completed, failed, and cancelled tasks using atomic counters.
 *
 * <p>All accessors are thread-safe and return consistent snapshots of the current state.
 * Statistics are updated atomically by the tracked executor as tasks transition through
 * their lifecycle.
 *
 * <h2>Lifecycle Tracking</h2>
 * <p>Tasks move through the following states:
 * <ul>
 *   <li><strong>Submitted:</strong> Task has been submitted to the executor</li>
 *   <li><strong>Completed:</strong> Task executed successfully and returned a result</li>
 *   <li><strong>Failed:</strong> Task threw an exception during execution</li>
 *   <li><strong>Cancelled:</strong> Task was cancelled before or during execution</li>
 *   <li><strong>Pending:</strong> Submitted but not yet completed/failed/cancelled</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * TrackedExecutorService executor = TrackedExecutors.wrap(delegate, scope).build();
 *
 * // Submit some tasks
 * for (int i = 0; i < 100; i++) {
 *     executor.submit(() -> processItem(i));
 * }
 *
 * // Monitor progress
 * TaskStatistics stats = executor.getStatistics();
 * System.out.println("Progress: " + stats.getCompleted() + "/" + stats.getSubmitted());
 * System.out.println("Completion rate: " + stats.getCompletionRate() * 100 + "%");
 * System.out.println("Pending: " + stats.getPending());
 * }</pre>
 *
 * @since 4.0.0
 */
public final class TaskStatistics {
    private final AtomicLong submitted = new AtomicLong(0);
    private final AtomicLong completed = new AtomicLong(0);
    private final AtomicLong failed = new AtomicLong(0);
    private final AtomicLong cancelled = new AtomicLong(0);

    /**
     * Creates a new TaskStatistics instance with all counters initialized to zero.
     */
    public TaskStatistics() {
    }

    /**
     * Returns the total number of tasks submitted to the executor.
     *
     * @return total submitted task count
     */
    public long getSubmitted() {
        return submitted.get();
    }

    /**
     * Returns the number of tasks that completed successfully.
     *
     * @return completed task count
     */
    public long getCompleted() {
        return completed.get();
    }

    /**
     * Returns the number of tasks that failed with an exception.
     *
     * @return failed task count
     */
    public long getFailed() {
        return failed.get();
    }

    /**
     * Returns the number of tasks that were cancelled.
     *
     * @return cancelled task count
     */
    public long getCancelled() {
        return cancelled.get();
    }

    /**
     * Returns the number of tasks that have been submitted but not yet completed, failed, or cancelled.
     * This is calculated as: submitted - (completed + failed + cancelled).
     *
     * @return pending task count
     */
    public long getPending() {
        return submitted.get() - completed.get() - failed.get() - cancelled.get();
    }

    /**
     * Returns the completion rate as a fraction between 0.0 and 1.0.
     * Completion rate is calculated as: completed / submitted.
     * Returns 0.0 if no tasks have been submitted.
     *
     * @return completion rate (0.0 to 1.0)
     */
    public double getCompletionRate() {
        long total = submitted.get();
        return total > 0 ? (double) completed.get() / total : 0.0;
    }

    /**
     * Returns the failure rate as a fraction between 0.0 and 1.0.
     * Failure rate is calculated as: failed / submitted.
     * Returns 0.0 if no tasks have been submitted.
     *
     * @return failure rate (0.0 to 1.0)
     */
    public double getFailureRate() {
        long total = submitted.get();
        return total > 0 ? (double) failed.get() / total : 0.0;
    }

    /**
     * Returns true if all submitted tasks have completed (successfully, failed, or cancelled).
     *
     * @return true if no tasks are pending
     */
    public boolean isComplete() {
        return getPending() == 0;
    }

    // Package-private methods for updating statistics

    void incrementSubmitted() {
        submitted.incrementAndGet();
    }

    void incrementCompleted() {
        completed.incrementAndGet();
    }

    void incrementFailed() {
        failed.incrementAndGet();
    }

    void incrementCancelled() {
        cancelled.incrementAndGet();
    }

    /**
     * Returns a string representation of these statistics.
     *
     * @return formatted statistics string
     */
    @Override
    public String toString() {
        long total = submitted.get();
        long done = completed.get();
        long fails = failed.get();
        long cancels = cancelled.get();
        long pending = getPending();

        return String.format(
            "TaskStatistics[submitted=%d, completed=%d, failed=%d, cancelled=%d, pending=%d, completion=%.1f%%]",
            total, done, fails, cancels, pending, getCompletionRate() * 100
        );
    }
}
