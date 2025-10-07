/*
 * Copyright (c) nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.nosqlbench.nbdatatools.api.concurrent;

import org.junit.jupiter.api.Test;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class ProgressIndicatorShutdownTest {

    static class TestProgressFuture extends CompletableFuture<String> implements ProgressIndicator<String> {
        private volatile double current = 0;
        private final double total = 100;

        @Override
        public double getTotalWork() {
            return total;
        }

        @Override
        public double getCurrentWork() {
            return current;
        }

        public void setProgress(double value) {
            this.current = value;
        }
    }

    @Test
    public void testNormalCompletionDoesNotShowInterruptMessage() throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(outputStream);

        TestProgressFuture future = new TestProgressFuture();

        // Start monitoring
        CompletableFuture<Void> monitor = future.monitorProgress(printStream, 100);

        // Simulate work and complete normally
        Thread workThread = new Thread(() -> {
            try {
                for (int i = 0; i <= 100; i += 20) {
                    future.setProgress(i);
                    Thread.sleep(50);
                }
                future.complete("Done!");
            } catch (InterruptedException e) {
                future.completeExceptionally(e);
            }
        });

        workThread.start();
        workThread.join();

        // Wait for monitoring to complete
        monitor.get(2, TimeUnit.SECONDS);

        String output = outputStream.toString();

        // Verify that "Interrupted by user" does NOT appear in output
        assertThat(output)
            .doesNotContain("Interrupted by user")
            .doesNotContain("Ctrl-C")
            .contains("Complete");
    }

    @Test
    public void testCancelledFutureShowsCancelledMessage() throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(outputStream);

        TestProgressFuture future = new TestProgressFuture();

        // Start monitoring
        CompletableFuture<Void> monitor = future.monitorProgress(printStream, 100);

        // Cancel the future after a short delay
        Thread cancelThread = new Thread(() -> {
            try {
                Thread.sleep(200);
                future.cancel(true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        cancelThread.start();
        cancelThread.join();

        // Wait for monitoring to complete
        try {
            monitor.get(2, TimeUnit.SECONDS);
        } catch (Exception e) {
            // Expected - the monitor will throw due to cancellation
        }

        String output = outputStream.toString();

        // When cancelled programmatically, the error message is shown
        // but it should NOT show "Interrupted by user (Ctrl-C)"
        assertThat(output)
            .doesNotContain("Interrupted by user")
            .doesNotContain("Ctrl-C")
            .contains("Error monitoring progress");
    }

    @Test
    public void testExceptionalCompletionShowsFailedMessage() throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(outputStream);

        TestProgressFuture future = new TestProgressFuture();

        // Start monitoring
        CompletableFuture<Void> monitor = future.monitorProgress(printStream, 100);

        // Complete exceptionally after a short delay
        Thread failThread = new Thread(() -> {
            try {
                Thread.sleep(200);
                future.completeExceptionally(new RuntimeException("Test failure"));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        failThread.start();
        failThread.join();

        // Wait for monitoring to complete
        try {
            monitor.get(2, TimeUnit.SECONDS);
        } catch (Exception e) {
            // Expected - will throw due to exceptional completion
        }

        String output = outputStream.toString();

        // Should show error message for exceptions but NOT "Interrupted by user"
        assertThat(output)
            .doesNotContain("Interrupted by user")
            .doesNotContain("Ctrl-C")
            .contains("Error monitoring progress");
    }
}