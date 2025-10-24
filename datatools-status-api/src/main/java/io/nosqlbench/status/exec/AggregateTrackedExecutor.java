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

import io.nosqlbench.status.StatusScope;
import io.nosqlbench.status.StatusTracker;
import io.nosqlbench.status.eventing.RunState;
import io.nosqlbench.status.eventing.StatusSource;
import io.nosqlbench.status.eventing.StatusUpdate;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

/**
 * TrackedExecutorService implementation that tracks all tasks with a single aggregate counter.
 * Instead of creating a StatusTracker for each task, this implementation maintains one parent
 * tracker that aggregates progress across all submitted tasks.
 *
 * <p>This implementation is ideal for high-volume scenarios where:
 * <ul>
 *   <li>The number of tasks is large (100+)</li>
 *   <li>Individual task tracking would be expensive</li>
 *   <li>Aggregate metrics (completion rate, total count) are sufficient</li>
 *   <li>Tasks are lightweight and short-lived</li>
 * </ul>
 *
 * <h2>Tracking Model</h2>
 * <p>All tasks are represented by a single {@link AggregateTaskTracker} that reports:
 * <ul>
 *   <li><strong>Progress:</strong> Ratio of completed tasks to submitted tasks</li>
 *   <li><strong>State:</strong> PENDING → RUNNING → SUCCESS/FAILED based on aggregate completion</li>
 *   <li><strong>Counts:</strong> Submitted, completed, failed, cancelled counts</li>
 * </ul>
 *
 * <h2>Performance Benefits</h2>
 * <p>Compared to {@link IndividualTaskTrackedExecutor}:
 * <ul>
 *   <li>Lower memory overhead (1 tracker vs N trackers)</li>
 *   <li>Less monitor polling overhead (1 poll vs N polls)</li>
 *   <li>Simpler status sink updates (1 update vs N updates per poll)</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * ExecutorService delegate = Executors.newFixedThreadPool(20);
 * try (StatusContext ctx = new StatusContext("batch-processing");
 *      StatusScope scope = ctx.createScope("Processing");
 *      TrackedExecutorService tracked = new AggregateTrackedExecutor(delegate, scope, "Records")) {
 *
 *     ctx.addSink(new ConsoleLoggerSink());
 *
 *     // Submit many lightweight tasks - tracked as a group
 *     List<Future<?>> futures = new ArrayList<>();
 *     for (int i = 0; i < 1000; i++) {
 *         futures.add(tracked.submit(() -> processRecord(i)));
 *     }
 *
 *     // Wait for completion
 *     for (Future<?> f : futures) {
 *         f.get();
 *     }
 *
 *     // Shows: "Records [1000/1000 completed] 100% ✅"
 * }
 * }</pre>
 *
 * <h2>Thread Safety</h2>
 * <p>This class is thread-safe. Multiple threads can submit tasks concurrently.
 * Statistics use atomic counters for thread-safe updates.
 *
 * @since 4.0.0
 */
final class AggregateTrackedExecutor implements TrackedExecutorService {

    private final ExecutorService delegate;
    private final StatusScope trackingScope;
    private final TaskStatistics statistics;
    private final AggregateTaskTracker aggregateTracker;
    private final StatusTracker<AggregateTaskTracker> tracker;
    private volatile boolean closed = false;

    /**
     * Creates a new aggregate tracked executor.
     *
     * @param delegate the underlying executor service to delegate task execution to
     * @param trackingScope the scope under which the aggregate tracker will be tracked
     * @param taskGroupName the display name for the group of tasks
     * @throws NullPointerException if any parameter is null
     */
    AggregateTrackedExecutor(ExecutorService delegate, StatusScope trackingScope, String taskGroupName) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.trackingScope = Objects.requireNonNull(trackingScope, "trackingScope");
        Objects.requireNonNull(taskGroupName, "taskGroupName");

        this.statistics = new TaskStatistics();
        this.aggregateTracker = new AggregateTaskTracker(taskGroupName, statistics);
        this.tracker = trackingScope.trackTask(aggregateTracker);
    }

    @Override
    public StatusScope getTrackingScope() {
        return trackingScope;
    }

    @Override
    public TaskStatistics getStatistics() {
        return statistics;
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        checkNotClosed();
        Objects.requireNonNull(task, "task");
        statistics.incrementSubmitted();

        Callable<T> wrapped = () -> {
            try {
                T result = task.call();
                statistics.incrementCompleted();
                return result;
            } catch (Exception e) {
                statistics.incrementFailed();
                throw e;
            }
        };

        return delegate.submit(wrapped);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        checkNotClosed();
        Objects.requireNonNull(task, "task");
        return submit(Executors.callable(task, result));
    }

    @Override
    public Future<?> submit(Runnable task) {
        checkNotClosed();
        Objects.requireNonNull(task, "task");
        return submit(Executors.callable(task, null));
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        checkNotClosed();
        Objects.requireNonNull(tasks, "tasks");
        List<Future<T>> futures = tasks.stream()
            .map(this::submit)
            .collect(java.util.stream.Collectors.toList());

        // Wait for all to complete (matching ExecutorService contract)
        for (Future<T> f : futures) {
            try {
                f.get();
            } catch (ExecutionException e) {
                // Task failed, but continue waiting for others
            }
        }

        return futures;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
        throws InterruptedException {
        checkNotClosed();
        Objects.requireNonNull(tasks, "tasks");
        Objects.requireNonNull(unit, "unit");

        List<Future<T>> futures = tasks.stream()
            .map(this::submit)
            .collect(java.util.stream.Collectors.toList());

        long deadline = System.nanoTime() + unit.toNanos(timeout);
        for (Future<T> future : futures) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return futures;
            }
            try {
                future.get(remaining, TimeUnit.NANOSECONDS);
            } catch (ExecutionException | TimeoutException e) {
                // Continue to wait for others
            }
        }
        return futures;
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        checkNotClosed();
        return delegate.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
        checkNotClosed();
        return delegate.invokeAny(tasks, timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
        checkNotClosed();
        submit(command);
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return delegate.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.awaitTermination(timeout, unit);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        // Mark the aggregate tracker as complete
        aggregateTracker.markComplete();

        // Close the tracker
        try {
            tracker.close();
        } catch (Exception e) {
            // Log but don't fail
        }

        // Shutdown delegate
        delegate.shutdown();
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("TrackedExecutorService has been closed");
        }
    }

    /**
     * Single tracker that aggregates progress from all submitted tasks.
     * Reports progress as the ratio of completed tasks to submitted tasks.
     */
    private static final class AggregateTaskTracker implements StatusSource<AggregateTaskTracker> {
        private final String name;
        private final TaskStatistics statistics;
        private volatile boolean markedComplete = false;

        AggregateTaskTracker(String name, TaskStatistics statistics) {
            this.name = name;
            this.statistics = statistics;
        }

        @Override
        public StatusUpdate<AggregateTaskTracker> getTaskStatus() {
            long total = statistics.getSubmitted();
            long completed = statistics.getCompleted() + statistics.getFailed() + statistics.getCancelled();

            double progress = total > 0 ? (double) completed / total : 0.0;
            RunState state = determineState(total, completed);

            return new StatusUpdate<>(progress, state, this);
        }

        private RunState determineState(long total, long completed) {
            if (total == 0) {
                return RunState.PENDING;
            }

            if (markedComplete || completed == total) {
                long failed = statistics.getFailed();
                return failed > 0 ? RunState.FAILED : RunState.SUCCESS;
            }

            return RunState.RUNNING;
        }

        void markComplete() {
            this.markedComplete = true;
        }

        @Override
        public String toString() {
            long completed = statistics.getCompleted();
            long failed = statistics.getFailed();
            long total = statistics.getSubmitted();

            if (failed > 0) {
                return String.format("%s [%d/%d completed, %d failed]", name, completed, total, failed);
            } else {
                return String.format("%s [%d/%d completed]", name, completed, total);
            }
        }
    }
}
