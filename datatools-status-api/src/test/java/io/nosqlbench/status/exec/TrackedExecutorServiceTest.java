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

import io.nosqlbench.status.StatusContext;
import io.nosqlbench.status.StatusScope;
import io.nosqlbench.status.eventing.RunState;
import io.nosqlbench.status.eventing.StatusSource;
import io.nosqlbench.status.eventing.StatusUpdate;
import io.nosqlbench.status.sinks.MetricsStatusSink;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class TrackedExecutorServiceTest {

    @Test
    void individualModeTracksEachTaskSeparately() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (StatusContext ctx = new StatusContext("test");
             StatusScope scope = ctx.createScope("Individual");
             TrackedExecutorService tracked = TrackedExecutors.wrap(executor, scope)
                 .withMode(TrackingMode.INDIVIDUAL)
                 .build()) {

            MetricsStatusSink metrics = new MetricsStatusSink();
            ctx.addSink(metrics);

            // Submit tasks
            Future<?> f1 = tracked.submit(() -> sleepMillis(50));
            Future<?> f2 = tracked.submit(() -> sleepMillis(50));

            f1.get();
            f2.get();

            // Statistics should show 2 completed tasks
            TaskStatistics stats = tracked.getStatistics();
            assertEquals(2, stats.getSubmitted());
            assertEquals(2, stats.getCompleted());
            assertEquals(0, stats.getFailed());
            assertTrue(stats.isComplete());
        }
        executor.shutdown();
    }

    @Test
    void aggregateModeTracksSingleCounter() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try (StatusContext ctx = new StatusContext("test");
             StatusScope scope = ctx.createScope("Aggregate");
             TrackedExecutorService tracked = TrackedExecutors.wrap(executor, scope)
                 .withMode(TrackingMode.AGGREGATE)
                 .withTaskGroupName("TestTasks")
                 .build()) {

            MetricsStatusSink metrics = new MetricsStatusSink();
            ctx.addSink(metrics);

            // Submit many tasks
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                futures.add(tracked.submit(() -> sleepMillis(10)));
            }

            for (Future<?> f : futures) {
                f.get();
            }

            // Statistics should show all completed
            TaskStatistics stats = tracked.getStatistics();
            assertEquals(10, stats.getSubmitted());
            assertEquals(10, stats.getCompleted());
            assertEquals(1.0, stats.getCompletionRate(), 0.01);
        }
        executor.shutdown();
    }

    @Test
    void tracksStatusSourceTasks() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (StatusContext ctx = new StatusContext("test");
             StatusScope scope = ctx.createScope("StatusSource");
             TrackedExecutorService tracked = TrackedExecutors.wrap(executor, scope)
                 .withMode(TrackingMode.INDIVIDUAL)
                 .build()) {

            ProgressiveTask task = new ProgressiveTask(5);
            Future<?> future = tracked.submit(() -> {
                task.execute();
                return null;
            });

            future.get();

            TaskStatistics stats = tracked.getStatistics();
            assertEquals(1, stats.getSubmitted());
            assertEquals(1, stats.getCompleted());
        }
        executor.shutdown();
    }

    @Test
    void tracksFailedTasks() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (StatusContext ctx = new StatusContext("test");
             StatusScope scope = ctx.createScope("Failures");
             TrackedExecutorService tracked = TrackedExecutors.wrap(executor, scope)
                 .withMode(TrackingMode.AGGREGATE)
                 .build()) {

            // Submit failing task
            Future<?> future = tracked.submit(() -> {
                throw new RuntimeException("Intentional failure");
            });

            assertThrows(ExecutionException.class, future::get);

            TaskStatistics stats = tracked.getStatistics();
            assertEquals(1, stats.getSubmitted());
            assertEquals(0, stats.getCompleted());
            assertEquals(1, stats.getFailed());
        }
        executor.shutdown();
    }

    @Test
    void handlesMultipleTaskTypes() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try (StatusContext ctx = new StatusContext("test");
             StatusScope scope = ctx.createScope("Mixed");
             TrackedExecutorService tracked = TrackedExecutors.wrap(executor, scope)
                 .withMode(TrackingMode.INDIVIDUAL)
                 .build()) {

            // Submit Callable
            Future<String> f1 = tracked.submit(() -> "result1");

            // Submit Runnable with result
            Future<String> f2 = tracked.submit(() -> sleepMillis(10), "result2");

            // Submit Runnable
            Future<?> f3 = tracked.submit(() -> sleepMillis(10));

            assertEquals("result1", f1.get());
            assertEquals("result2", f2.get());
            f3.get();

            TaskStatistics stats = tracked.getStatistics();
            assertEquals(3, stats.getSubmitted());
            assertEquals(3, stats.getCompleted());
        }
        executor.shutdown();
    }

    @Test
    void handlesHighVolume() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(20);
        try (StatusContext ctx = new StatusContext("test");
             StatusScope scope = ctx.createScope("HighVolume");
             TrackedExecutorService tracked = TrackedExecutors.wrap(executor, scope)
                 .withMode(TrackingMode.AGGREGATE)
                 .withTaskGroupName("Batch")
                 .build()) {

            int taskCount = 100;
            CountDownLatch latch = new CountDownLatch(taskCount);

            for (int i = 0; i < taskCount; i++) {
                tracked.submit(() -> {
                    sleepMillis(5);
                    latch.countDown();
                });
            }

            assertTrue(latch.await(10, TimeUnit.SECONDS));

            TaskStatistics stats = tracked.getStatistics();
            assertEquals(taskCount, stats.getSubmitted());
            assertEquals(taskCount, stats.getCompleted());
        }
        executor.shutdown();
    }

    @Test
    void statisticsReportCorrectMetrics() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (StatusContext ctx = new StatusContext("test");
             StatusScope scope = ctx.createScope("Stats");
             TrackedExecutorService tracked = TrackedExecutors.wrap(executor, scope)
                 .withMode(TrackingMode.AGGREGATE)
                 .build()) {

            // Submit mix of successful and failing tasks
            Future<?> f1 = tracked.submit(() -> "success");
            Future<?> f2 = tracked.submit(() -> {
                throw new RuntimeException("fail");
            });
            Future<?> f3 = tracked.submit(() -> "success");

            f1.get();
            assertThrows(ExecutionException.class, f2::get);
            f3.get();

            TaskStatistics stats = tracked.getStatistics();
            assertEquals(3, stats.getSubmitted());
            assertEquals(2, stats.getCompleted());
            assertEquals(1, stats.getFailed());
            assertEquals(0, stats.getPending());
            assertEquals(2.0 / 3.0, stats.getCompletionRate(), 0.01);
            assertEquals(1.0 / 3.0, stats.getFailureRate(), 0.01);
        }
        executor.shutdown();
    }

    @Test
    void closesGracefully() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        TrackedExecutorService tracked = TrackedExecutors.wrap(executor, new StatusContext("test").createScope("Close"))
            .withMode(TrackingMode.INDIVIDUAL)
            .build();

        Future<?> future = tracked.submit(() -> sleepMillis(10));
        future.get();

        tracked.close();

        // Should not accept new tasks after close
        assertThrows(IllegalStateException.class, () -> tracked.submit(() -> "fail"));
    }

    @Test
    void invokeAllTracksAllTasks() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try (StatusContext ctx = new StatusContext("test");
             StatusScope scope = ctx.createScope("InvokeAll");
             TrackedExecutorService tracked = TrackedExecutors.wrap(executor, scope)
                 .withMode(TrackingMode.AGGREGATE)
                 .build()) {

            List<Callable<Integer>> tasks = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                final int value = i;
                tasks.add(() -> {
                    sleepMillis(10);
                    return value;
                });
            }

            List<Future<Integer>> futures = tracked.invokeAll(tasks);
            assertEquals(5, futures.size());

            for (Future<Integer> f : futures) {
                assertTrue(f.isDone());
            }

            TaskStatistics stats = tracked.getStatistics();
            assertEquals(5, stats.getSubmitted());
            assertEquals(5, stats.getCompleted());
        }
        executor.shutdown();
    }

    // Helper class for testing StatusSource tasks
    private static class ProgressiveTask implements StatusSource<ProgressiveTask> {
        private final int steps;
        private final AtomicInteger currentStep = new AtomicInteger(0);
        private volatile RunState state = RunState.PENDING;

        ProgressiveTask(int steps) {
            this.steps = steps;
        }

        @Override
        public StatusUpdate<ProgressiveTask> getTaskStatus() {
            double progress = (double) currentStep.get() / steps;
            return new StatusUpdate<>(progress, state, this);
        }

        void execute() {
            state = RunState.RUNNING;
            for (int i = 0; i < steps; i++) {
                sleepMillis(10);
                currentStep.incrementAndGet();
            }
            state = RunState.SUCCESS;
        }
    }

    private static void sleepMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
