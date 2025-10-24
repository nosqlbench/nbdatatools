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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;

/**
 * TrackedExecutorService implementation that tracks each submitted task individually.
 * Each task gets its own {@link StatusTracker}, providing detailed per-task visibility
 * into execution progress and lifecycle.
 *
 * <p>This implementation is ideal for scenarios where:
 * <ul>
 *   <li>The number of tasks is moderate (≤50)</li>
 *   <li>Individual task progress is important</li>
 *   <li>Tasks have meaningful self-reported progress via {@link StatusSource}</li>
 *   <li>Detailed per-task visibility is needed for debugging or monitoring</li>
 * </ul>
 *
 * <h2>StatusSource Detection</h2>
 * <p>This executor automatically detects if submitted Callable/Runnable implements
 * {@link StatusSource}:
 * <ul>
 *   <li><strong>StatusSource tasks:</strong> Tracked with their self-reported progress and state</li>
 *   <li><strong>Regular tasks:</strong> Wrapped to track basic lifecycle (pending/running/success/failed)</li>
 * </ul>
 *
 * <h2>Lifecycle Tracking</h2>
 * <p>Each task transitions through states:
 * <ol>
 *   <li><strong>PENDING:</strong> Task submitted but not yet executing</li>
 *   <li><strong>RUNNING:</strong> Task is actively executing</li>
 *   <li><strong>SUCCESS:</strong> Task completed successfully</li>
 *   <li><strong>FAILED:</strong> Task threw an exception</li>
 *   <li><strong>CANCELLED:</strong> Task was cancelled</li>
 * </ol>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * ExecutorService delegate = Executors.newFixedThreadPool(4);
 * try (StatusContext ctx = new StatusContext("pipeline");
 *      StatusScope scope = ctx.createScope("ETL");
 *      TrackedExecutorService tracked = new IndividualTaskTrackedExecutor(delegate, scope)) {
 *
 *     ctx.addSink(new ConsoleLoggerSink());
 *
 *     // Submit tasks - each tracked individually
 *     Future<Data> f1 = tracked.submit(new DataLoadTask());    // Shows individual progress
 *     Future<Data> f2 = tracked.submit(new DataTransformTask()); // Shows individual progress
 *
 *     f1.get();
 *     f2.get();
 * }
 * }</pre>
 *
 * <h2>Thread Safety</h2>
 * <p>This class is thread-safe. Multiple threads can submit tasks concurrently.
 * Statistics and tracker management use concurrent data structures.
 *
 * @since 4.0.0
 */
final class IndividualTaskTrackedExecutor implements TrackedExecutorService {

    private final ExecutorService delegate;
    private final StatusScope trackingScope;
    private final TaskStatistics statistics;
    private final Map<Future<?>, StatusTracker<?>> trackers;
    private volatile boolean closed = false;

    /**
     * Creates a new individual task tracked executor.
     *
     * @param delegate the underlying executor service to delegate task execution to
     * @param trackingScope the scope under which tasks will be tracked
     * @throws NullPointerException if delegate or trackingScope is null
     */
    IndividualTaskTrackedExecutor(ExecutorService delegate, StatusScope trackingScope) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.trackingScope = Objects.requireNonNull(trackingScope, "trackingScope");
        this.statistics = new TaskStatistics();
        this.trackers = new ConcurrentHashMap<>();
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

        if (task instanceof StatusSource) {
            return submitStatusSourceTask(task);
        } else {
            return submitRegularTask(task);
        }
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
        // Submit each task through our tracking mechanism
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
        // Submit through tracking, but use delegate's timeout mechanism
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

        // Close all trackers
        for (StatusTracker<?> tracker : trackers.values()) {
            try {
                tracker.close();
            } catch (Exception e) {
                // Log but don't fail
            }
        }
        trackers.clear();

        // Shutdown delegate
        delegate.shutdown();
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("TrackedExecutorService has been closed");
        }
    }

    /**
     * Submits a task that implements StatusSource.
     * The task's self-reported status will be tracked.
     */
    @SuppressWarnings("unchecked")
    private <T> Future<T> submitStatusSourceTask(Callable<T> task) {
        // Use helper to properly handle generic types
        return submitStatusSourceTaskHelper((StatusSource) task, task);
    }

    @SuppressWarnings("unchecked")
    private <T, S extends StatusSource<S>> Future<T> submitStatusSourceTaskHelper(S statusTask, Callable<T> task) {
        StatusTracker<S> tracker = trackingScope.trackTask(statusTask);

        Callable<T> wrapped = () -> {
            try {
                T result = task.call();
                statistics.incrementCompleted();
                return result;
            } catch (Exception e) {
                statistics.incrementFailed();
                throw e;
            } finally {
                tracker.close();
                trackers.remove(Thread.currentThread());
            }
        };

        Future<T> future = delegate.submit(wrapped);
        trackers.put(future, tracker);
        return future;
    }

    /**
     * Submits a regular task (non-StatusSource).
     * Wraps it to track basic lifecycle.
     */
    private <T> Future<T> submitRegularTask(Callable<T> task) {
        SimpleTaskWrapper<T> wrapper = new SimpleTaskWrapper<>(task);
        StatusTracker<SimpleTaskWrapper<T>> tracker = trackingScope.trackTask(wrapper);

        Callable<T> wrapped = () -> {
            wrapper.markRunning();
            try {
                T result = task.call();
                wrapper.markSuccess();
                statistics.incrementCompleted();
                return result;
            } catch (Exception e) {
                wrapper.markFailed();
                statistics.incrementFailed();
                throw e;
            } finally {
                tracker.close();
                trackers.remove(Thread.currentThread());
            }
        };

        Future<T> future = delegate.submit(wrapped);
        trackers.put(future, tracker);
        return future;
    }

    /**
     * Wrapper for non-StatusSource tasks that provides basic lifecycle tracking.
     * Tracks only state transitions (pending → running → success/failed).
     */
    private static final class SimpleTaskWrapper<T> implements StatusSource<SimpleTaskWrapper<T>> {
        private final Callable<T> delegate;
        private final String taskName;
        private volatile RunState state = RunState.PENDING;

        SimpleTaskWrapper(Callable<T> delegate) {
            this.delegate = delegate;
            this.taskName = delegate.getClass().getSimpleName();
        }

        @Override
        public StatusUpdate<SimpleTaskWrapper<T>> getTaskStatus() {
            double progress = state == RunState.SUCCESS ? 1.0 : 0.0;
            return new StatusUpdate<>(progress, state, this);
        }

        void markRunning() {
            state = RunState.RUNNING;
        }

        void markSuccess() {
            state = RunState.SUCCESS;
        }

        void markFailed() {
            state = RunState.FAILED;
        }

        @Override
        public String toString() {
            return taskName;
        }
    }
}
