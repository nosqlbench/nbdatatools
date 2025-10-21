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
import io.nosqlbench.status.sinks.ConsoleLoggerSink;
import io.nosqlbench.status.sinks.MetricsStatusSink;
import io.nosqlbench.status.eventing.StatusSource;
import io.nosqlbench.status.eventing.StatusUpdate;
import io.nosqlbench.status.userguide.fauxtasks.ClosureBasedProcessor;
import io.nosqlbench.status.userguide.fauxtasks.DataLoader;
import io.nosqlbench.status.userguide.fauxtasks.DataProcessor;
import io.nosqlbench.status.userguide.fauxtasks.DataValidator;
import io.nosqlbench.status.userguide.fauxtasks.ParallelDataLoader;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verification tests for all user guide examples.
 * Ensures that each level example works as described and demonstrates the concepts correctly.
 */
@Tag("examples")
public class UserGuideExamplesTest {

    @Test
    public void testLevel1_AbsoluteMinimum() {
        // Verify Level 1 example works
        DataLoader loader1 = new DataLoader();
        try (StatusTracker<DataLoader> tracker1 = new StatusTracker<>(loader1)) {
            loader1.load();

            // Verify we can access status programmatically
            assertNotNull(tracker1.toString());
            assertTrue(tracker1.toString().contains("DataLoader"));
            assertEquals(RunState.SUCCESS, tracker1.getStatus().runstate);
            assertEquals(1.0, tracker1.getStatus().progress, 0.01);
            assertTrue(tracker1.getElapsedRunningTime() > 0);
        }
    }

    @Test
    public void testLevel2_AddConsoleOutput() {
        // Verify Level 2 example works (same as Level 1 but with sink)
        DataLoader loader2 = new DataLoader();
        try (StatusTracker<DataLoader> tracker2 = new StatusTracker<>(loader2)) {
            // Add sink
            tracker2.getContext().addSink(new ConsoleLoggerSink());

            loader2.load();

            // Verify tracking worked
            assertEquals(RunState.SUCCESS, tracker2.getStatus().runstate);
            assertEquals(1.0, tracker2.getStatus().progress, 0.01);
        }
    }

    @Test
    public void testLevel3_MultipleTasks() {
        // Verify Level 3 example works (shared context)
        try (StatusContext ctx3 = new StatusContext("batch-processing")) {
            ctx3.addSink(new ConsoleLoggerSink());

            DataLoader loader3 = new DataLoader();
            DataValidator validator3 = new DataValidator();

            try (StatusTracker<DataLoader> loaderTracker = ctx3.track(loader3);
                 StatusTracker<DataValidator> validatorTracker = ctx3.track(validator3)) {

                // Execute tasks
                loader3.load();
                validator3.validate();

                // Verify both completed
                assertEquals(RunState.SUCCESS, loaderTracker.getStatus().runstate);
                assertEquals(RunState.SUCCESS, validatorTracker.getStatus().runstate);
                assertEquals(2, ctx3.getActiveTrackerCount());
            }
        }
    }

    @Test
    public void testLevel4_OrganizeWithScopes() throws InterruptedException {
        // Verify Level 4 example works (explicit scopes)
        try (StatusContext ctx4 = new StatusContext("data-pipeline")) {
            ctx4.addSink(new ConsoleLoggerSink());

            try (StatusScope ingestionScope = ctx4.createScope("Ingestion");
                 StatusScope processingScope = ctx4.createScope("Processing")) {

                // Ingestion phase
                DataLoader loader4 = new DataLoader();
                DataValidator validator4 = new DataValidator();

                try (StatusTracker<DataLoader> loaderTracker4 = ingestionScope.trackTask(loader4);
                     StatusTracker<DataValidator> validatorTracker4 = ingestionScope.trackTask(validator4)) {
                    loader4.load();
                    validator4.validate();
                }

                // Wait for ingestion to complete
                while (!ingestionScope.isComplete()) {
                    Thread.sleep(10);
                }

                assertTrue(ingestionScope.isComplete());

                // Processing phase
                DataProcessor processor4 = new DataProcessor();
                try (StatusTracker<DataProcessor> processorTracker4 = processingScope.trackTask(processor4)) {
                    processor4.process();
                    assertEquals(RunState.SUCCESS, processorTracker4.getStatus().runstate);
                }
            }
        }
    }

    @Test
    public void testLevel5_CustomSinks() {
        // Verify Level 5 example works (multiple sinks)
        MetricsStatusSink metrics5 = new MetricsStatusSink();

        try (StatusContext ctx5 = new StatusContext("data-pipeline")) {
            ctx5.addSink(new ConsoleLoggerSink());
            ctx5.addSink(metrics5);

            DataLoader loader5 = new DataLoader();

            try (StatusScope scope5 = ctx5.createScope("Work")) {
                try (StatusTracker<DataLoader> tracker5 = scope5.trackTask(loader5)) {
                    loader5.load();
                }
            }
        }

        // Verify metrics were collected
        assertEquals(1, metrics5.getTotalTasksStarted());
        assertEquals(1, metrics5.getTotalTasksFinished());
        assertTrue(metrics5.getAverageTaskDuration() > 0);
    }

    @Test
    public void testLevel6_ConfigurePollInterval() {
        // Verify Level 6 example works (custom poll interval)
        try (StatusContext ctx6 = new StatusContext(
            "data-pipeline",
            Duration.ofMillis(50))) { // Will be clamped to 100ms minimum

            ctx6.addSink(new ConsoleLoggerSink());

            // Verify minimum poll interval was enforced
            assertTrue(ctx6.getDefaultPollInterval().toMillis() >= 100,
                    "Poll interval should be at least 100ms");

            DataLoader loader6 = new DataLoader();
            try (StatusTracker<DataLoader> tracker6 = ctx6.track(loader6)) {
                loader6.load();
                assertEquals(RunState.SUCCESS, tracker6.getStatus().runstate);
            }
        }
    }

    @Test
    public void testLevel7_ParallelExecution() throws Exception {
        // Verify Level 7 example works (parallel execution)
        ExecutorService executor7 = Executors.newFixedThreadPool(3);
        try (StatusContext ctx7 = new StatusContext("parallel-work")) {
            ctx7.addSink(new ConsoleLoggerSink());

            try (StatusScope scope7 = ctx7.createScope("Parallel")) {

                List<Future<?>> futures7 = new ArrayList<>();

                for (int i = 0; i < 3; i++) {
                    ParallelDataLoader loader7 = new ParallelDataLoader();
                    Future<?> future = executor7.submit(() -> {
                        try (StatusTracker<ParallelDataLoader> tracker7 = scope7.trackTask(loader7)) {
                            loader7.load();
                            assertEquals(RunState.SUCCESS, tracker7.getStatus().runstate);
                        }
                    });
                    futures7.add(future);
                }

                // Wait for all tasks
                for (Future<?> f : futures7) {
                    f.get();
                }

                // Verify scope completion
                while (!scope7.isComplete()) {
                    Thread.sleep(10);
                }
                assertTrue(scope7.isComplete());
            }
        } finally {
            executor7.shutdown();
        }
    }

    @Test
    public void testLevel8_ParallelClosures() throws Exception {
        // Verify Level 8 example works (parallel closures with progress counter)
        ExecutorService executor8 = Executors.newFixedThreadPool(10);
        try (StatusContext ctx8 = new StatusContext("closure-processing")) {
            ctx8.addSink(new ConsoleLoggerSink());

            try (StatusScope scope8 = ctx8.createScope("BatchProcessor")) {

                // Single parent task that will spawn many closures
                ClosureBasedProcessor processor8 = new ClosureBasedProcessor(100);
                List<Future<?>> futures8 = new ArrayList<>();

                try (StatusTracker<ClosureBasedProcessor> tracker8 = scope8.trackTask(processor8)) {
                    // Submit many lightweight closures
                    for (int i = 0; i < 100; i++) {
                        final int taskId = i;
                        Future<?> future = executor8.submit(() -> {
                            processor8.processClosure(taskId);
                        });
                        futures8.add(future);
                    }

                    // Wait for all closures to complete
                    for (Future<?> f : futures8) {
                        f.get();
                    }

                    // Mark parent task as complete
                    processor8.markComplete();

                    // Verify completion
                    assertEquals(RunState.SUCCESS, tracker8.getStatus().runstate);
                    assertEquals(1.0, tracker8.getStatus().progress, 0.01);
                }

                // Verify scope completion
                while (!scope8.isComplete()) {
                    Thread.sleep(10);
                }
                assertTrue(scope8.isComplete());
            }
        } finally {
            executor8.shutdown();
        }
    }

    @Test
    public void testLevel9_ExplicitProgressCounter() throws Exception {
        // Verify Level 9 example works (explicit inline progress counter)
        ExecutorService executor9 = Executors.newFixedThreadPool(10);
        try (StatusContext ctx9 = new StatusContext("explicit-counter-demo")) {
            ctx9.addSink(new ConsoleLoggerSink());

            try (StatusScope scope9 = ctx9.createScope("BatchWork")) {

                // Explicitly create shared progress counter
                final AtomicLong completedCount = new AtomicLong(0);
                final long totalTasks = 100;
                final RunState[] state = {RunState.RUNNING};

                // Inline StatusSource using local class
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
                    // Each closure directly increments the shared counter
                    for (int i = 0; i < totalTasks; i++) {
                        Future<?> future = executor9.submit(() -> {
                            try {
                                Thread.sleep(10);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                state[0] = RunState.FAILED;
                                return;
                            }
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

                    // Verify completion
                    assertEquals(RunState.SUCCESS, tracker9.getStatus().runstate);
                    assertEquals(1.0, tracker9.getStatus().progress, 0.01);
                    assertEquals(100, completedCount.get());
                }

                // Verify scope completion
                while (!scope9.isComplete()) {
                    Thread.sleep(10);
                }
                assertTrue(scope9.isComplete());
            }
        } finally {
            executor9.shutdown();
        }
    }

    @Test
    public void testLevel10_PreConfiguredTasks() {
        // Verify Level 10 example works (pre-configured tasks visible as PENDING)
        try (StatusContext ctx10 = new StatusContext("pre-configured-pipeline")) {
            ctx10.addSink(new ConsoleLoggerSink());

            try (StatusScope scope10 = ctx10.createScope("DataPipeline")) {

                // Helper class to pair tasks with their trackers
                class TaskExecution<T> implements AutoCloseable {
                    final T task;
                    final StatusTracker<T> tracker;
                    final Runnable executor;

                    TaskExecution(T task, StatusTracker<T> tracker, Runnable executor) {
                        this.task = task;
                        this.tracker = tracker;
                        this.executor = executor;
                    }

                    void execute() {
                        executor.run();
                    }

                    @Override
                    public void close() {
                        tracker.close();
                    }
                }

                // Configuration phase - create and register all tasks
                List<TaskExecution<?>> executions = new ArrayList<>();

                DataLoader loader10 = new DataLoader();
                DataValidator validator10 = new DataValidator();
                DataProcessor processor10 = new DataProcessor();

                // Register tasks - they appear as PENDING immediately
                executions.add(new TaskExecution<>(
                    loader10,
                    scope10.trackTask(loader10),
                    loader10::load
                ));

                executions.add(new TaskExecution<>(
                    validator10,
                    scope10.trackTask(validator10),
                    validator10::validate
                ));

                executions.add(new TaskExecution<>(
                    processor10,
                    scope10.trackTask(processor10),
                    processor10::process
                ));

                // Verify all tasks are tracked (visible in the scope)
                assertEquals(3, scope10.getChildTasks().size());

                // Verify all tasks start as PENDING
                for (TaskExecution<?> exec : executions) {
                    assertEquals(RunState.PENDING, exec.tracker.getStatus().runstate);
                }

                // Execution phase - execute tasks in order
                for (TaskExecution<?> execution : executions) {
                    execution.execute();
                    execution.close();
                }

                // Verify all tasks completed successfully
                assertFalse(scope10.getChildTasks().stream()
                    .anyMatch(t -> t.getStatus().runstate != RunState.SUCCESS));
            }
        }
    }

    @Test
    public void testAllLevelExamplesRunSuccessfully() {
        // Meta-test: Verify all individual level tests pass
        // This ensures the user guide examples remain functional
        assertDoesNotThrow(() -> {
            testLevel1_AbsoluteMinimum();
            testLevel2_AddConsoleOutput();
            testLevel3_MultipleTasks();
            testLevel4_OrganizeWithScopes();
            testLevel5_CustomSinks();
            testLevel6_ConfigurePollInterval();
            testLevel7_ParallelExecution();
            testLevel8_ParallelClosures();
            testLevel9_ExplicitProgressCounter();
            testLevel10_PreConfiguredTasks();
        }, "All user guide examples should run without errors");
    }
}
