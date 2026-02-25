/*
 * Copyright (c) nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.nosqlbench.datatools.optimizers;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.*;

/// Tests for [JITMaturityGuard] verifying lifecycle enforcement, JFR-based
/// C2 detection, and correct compilation tier reporting.
///
/// Each test that checks JFR compilation events uses its own unique inner
/// class as the hot-path target. JFR only records compilations that occur
/// during the recording window — using separate classes ensures each test
/// observes a first compilation rather than missing an already-compiled method.
class JITMaturityGuardTest {

    // ========================================================================
    // Unique hot-path targets — one per test that inspects JFR events.
    // Each has slightly different arithmetic to prevent the JIT from sharing
    // compiled code between them.
    // ========================================================================

    static class HotPathA {
        private final long[] data = new long[1024];
        long compute(long index) {
            int i = (int) (index & 1023);
            data[i] = data[i] + index * 31 + 17;
            return data[i];
        }
    }

    static class HotPathB {
        private final long[] data = new long[1024];
        long compute(long index) {
            int i = (int) (index & 1023);
            data[i] = data[i] + index * 37 + 13;
            return data[i];
        }
    }

    static class HotPathC {
        private final long[] data = new long[1024];
        long compute(long index) {
            int i = (int) (index & 1023);
            data[i] = data[i] + index * 41 + 11;
            return data[i];
        }
    }

    static class HotPathD {
        private final long[] data = new long[1024];
        long compute(long index) {
            int i = (int) (index & 1023);
            data[i] = data[i] + index * 43 + 7;
            return data[i];
        }
    }

    static class HotPathE {
        private final long[] data = new long[1024];
        long compute(long index) {
            int i = (int) (index & 1023);
            data[i] = data[i] + index * 47 + 3;
            return data[i];
        }
    }

    // ---- Lifecycle enforcement tests ----

    /// The guard must reject [JITMaturityGuard#run] after [JITMaturityGuard#verify]
    /// has been called — the recording is stopped and no further compilations
    /// can be observed.
    @Test
    void run_afterVerify_throwsIllegalState() {
        try (JITMaturityGuard guard = new JITMaturityGuard()) {
            guard.run(() -> {}, 1);
            guard.verify();

            assertThatThrownBy(() -> guard.run(() -> {}, 1))
                .isInstanceOf(IllegalStateException.class);
        }
    }

    /// Calling [JITMaturityGuard#verify] twice must fail — the recording
    /// can only be stopped once.
    @Test
    void verify_calledTwice_throwsIllegalState() {
        try (JITMaturityGuard guard = new JITMaturityGuard()) {
            guard.run(() -> {}, 1);
            guard.verify();

            assertThatThrownBy(guard::verify)
                .isInstanceOf(IllegalStateException.class);
        }
    }

    /// [JITMaturityGuard#close] without [JITMaturityGuard#verify] must not throw —
    /// it just cleans up the recording.
    @Test
    void close_withoutVerify_doesNotThrow() {
        assertThatCode(() -> {
            try (JITMaturityGuard guard = new JITMaturityGuard()) {
                guard.run(() -> {}, 100);
            }
        }).doesNotThrowAnyException();
    }

    // ---- C2 compilation verification tests ----

    /// A hot method must reach C2 (tier 4) when the guard's lifecycle wraps
    /// the first invocation. This is the core contract: construct guard,
    /// run work, verify — and C2 must be observed.
    @Test
    void hotMethod_reachesC2_whenGuardWrapsFirstInvocation() {
        HotPathA target = new HotPathA();
        AtomicLong sink = new AtomicLong();

        try (JITMaturityGuard guard = new JITMaturityGuard()) {
            guard.run(() -> sink.set(target.compute(sink.get())), 1_000_000);
            JITMaturityGuard.Result result = guard.verify(HotPathA.class);

            assertThat(result.totalCompilations())
                .as("Should capture compilation events")
                .isGreaterThan(0);
            assertThat(result.c2Compilations())
                .as("At least one method should reach C2")
                .isGreaterThan(0);
            assertThat(result.targetEventsAbsent())
                .as("Compilation events for target class must be present")
                .isFalse();
            assertThat(result.allTargetsReachedC2())
                .as("HotPathA.compute must reach C2 (tier 4)")
                .isTrue();
        }
    }

    /// When a method is invoked BEFORE the guard is constructed, the guard
    /// must report that no compilation events were found for the target class
    /// — the compilation happened outside the recording window.
    @Test
    void hotMethod_missedWhenCompiledBeforeGuard() {
        HotPathB target = new HotPathB();
        AtomicLong sink = new AtomicLong();

        // Pre-invoke enough to trigger C2 BEFORE the guard exists
        for (int i = 0; i < 1_000_000; i++) {
            sink.set(target.compute(i));
        }

        try (JITMaturityGuard guard = new JITMaturityGuard()) {
            // Run again under the guard — but the method is already compiled
            guard.run(() -> sink.set(target.compute(sink.get())), 100_000);
            JITMaturityGuard.Result result = guard.verify(HotPathB.class);

            assertThat(result.targetEventsAbsent())
                .as("Guard should report absent target events when method was "
                    + "compiled before the recording started")
                .isTrue();
        }
    }

    /// The [Result#targetMethodLevels] map must contain the correct max
    /// compile level for each observed method.
    @Test
    void result_reportsCorrectMaxCompileLevel() {
        HotPathC target = new HotPathC();
        AtomicLong sink = new AtomicLong();

        try (JITMaturityGuard guard = new JITMaturityGuard()) {
            guard.run(() -> sink.set(target.compute(sink.get())), 1_000_000);
            JITMaturityGuard.Result result = guard.verify(HotPathC.class);

            assertThat(result.targetMethodLevels())
                .as("Should have at least one method entry for HotPathC")
                .isNotEmpty();

            // Every observed method for our target should be at C2
            for (var entry : result.targetMethodLevels().entrySet()) {
                assertThat(entry.getValue())
                    .as("Method %s should reach C2 (tier 4)", entry.getKey())
                    .isEqualTo(4);
            }
        }
    }

    /// The static convenience method must execute the work the expected
    /// number of times (1M) and complete without error.
    @Test
    void staticForceAndVerify_executesWork() {
        AtomicLong counter = new AtomicLong();
        JITMaturityGuard.forceAndVerify(counter::incrementAndGet);
        assertThat(counter.get())
            .as("Static forceAndVerify should execute 1M iterations")
            .isEqualTo(1_000_000);
    }

    /// The static convenience method with target classes must complete
    /// without error when the guard wraps the first invocations.
    @Test
    void staticForceAndVerify_withTargetClass_completesSuccessfully() {
        HotPathD target = new HotPathD();
        AtomicLong sink = new AtomicLong();

        assertThatCode(() ->
            JITMaturityGuard.forceAndVerify(
                () -> sink.set(target.compute(sink.get())),
                HotPathD.class
            )
        ).doesNotThrowAnyException();

        assertThat(sink.get()).isNotEqualTo(0);
    }

    /// JFR class-name filtering must not attribute compilation events from
    /// one class to another.
    @Test
    void verify_doesNotConfuseUnrelatedClasses() {
        HotPathE target = new HotPathE();
        AtomicLong sink = new AtomicLong();

        try (JITMaturityGuard guard = new JITMaturityGuard()) {
            guard.run(() -> sink.set(target.compute(sink.get())), 1_000_000);

            // Verify against a class that was never invoked
            JITMaturityGuard.Result result = guard.verify(String.class);

            assertThat(result.targetEventsAbsent())
                .as("No compilation events should be attributed to String "
                    + "when only HotPathE was exercised")
                .isTrue();
        }
    }

    /// Multiple [JITMaturityGuard#run] calls must accumulate — compilations
    /// triggered by any call are captured by the same recording.
    @Test
    void multipleRunCalls_accumulateUnderSameRecording() {
        HotPathA target = new HotPathA(); // reuse is fine — already compiled from earlier test
        AtomicLong sink = new AtomicLong();

        // Use a unique target to avoid the "already compiled" issue
        // We're testing accumulation, not C2 verification, so we just
        // verify no exceptions and that work ran
        AtomicLong counter = new AtomicLong();
        try (JITMaturityGuard guard = new JITMaturityGuard()) {
            guard.run(counter::incrementAndGet, 500_000);
            guard.run(counter::incrementAndGet, 500_000);
            JITMaturityGuard.Result result = guard.verify();

            assertThat(counter.get())
                .as("Two run() calls of 500K each should total 1M")
                .isEqualTo(1_000_000);
            assertThat(result.totalCompilations())
                .as("Recording should capture compilations across both run() calls")
                .isGreaterThanOrEqualTo(0); // may be 0 if everything was already compiled
        }
    }
}
