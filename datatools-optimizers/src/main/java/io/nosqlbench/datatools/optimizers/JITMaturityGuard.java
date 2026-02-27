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

import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedMethod;
import jdk.jfr.consumer.RecordingFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/// Ensures JIT maturation by forcing work and verifying C2 compilation via JFR.
///
/// Enforces a strict lifecycle so that the JFR recording is always active before
/// any target method invocations occur:
///
/// 1. **Construction** — starts a JFR recording capturing `jdk.Compilation` events.
///    The guard MUST be constructed before the target methods are ever invoked,
///    otherwise compilations may occur outside the recording window and go unobserved.
///
/// 2. **[#run(Runnable, int)]** — executes the hot-path work under JFR observation.
///    All compilation events triggered by the work are captured.
///
/// 3. **[#verify(Class...)]** — stops the recording, inspects events, and reports
///    which target methods reached C2 (tier 4). Throws if target classes were
///    specified but no compilation events were found for them.
///
/// 4. **[#close()]** — cleans up JFR resources. Called automatically via
///    try-with-resources.
///
/// Example usage:
/// ```java
/// try (var guard = new JITMaturityGuard()) {
///     guard.run(() -> engine.getRecord(random.nextLong(rowCount)), 1_000_000);
///     guard.verify(engine.getClass());
/// }
/// ```
///
/// A static convenience method [#forceAndVerify(Runnable, Class...)] is provided
/// for simple cases where the full lifecycle is contained in a single call.
public class JITMaturityGuard implements AutoCloseable {

    private static final int DEFAULT_ITERATIONS = 1_000_000;

    private final Recording recording;
    private final List<RecordedEvent> capturedEvents = new ArrayList<>();
    private long totalIterations;
    private boolean stopped;
    private boolean closed;

    /// Creates a new guard and immediately starts a JFR recording for
    /// `jdk.Compilation` events.
    ///
    /// **This must be called before the target methods are ever invoked.**
    /// JFR only captures compilations that occur during the recording window.
    /// If the target methods have already been called (and compiled) before
    /// construction, those compilation events will not be observed.
    public JITMaturityGuard() {
        recording = new Recording();
        recording.enable("jdk.Compilation");
        recording.start();
    }

    /// Executes the work [Runnable] for the specified number of iterations
    /// under JFR observation.
    ///
    /// May be called multiple times to accumulate iterations. All compilations
    /// triggered by any call to `run` are captured by the same recording.
    ///
    /// @param work       the hot-path operation to force-compile
    /// @param iterations number of times to invoke the work
    /// @throws IllegalStateException if [#verify] or [#close] has already been called
    public void run(Runnable work, int iterations) {
        if (stopped) {
            throw new IllegalStateException(
                "Cannot run work after verify() or close() — recording is already stopped");
        }
        for (int i = 0; i < iterations; i++) {
            work.run();
        }
        totalIterations += iterations;
    }

    /// Stops the JFR recording and verifies C2 compilation status for the
    /// specified target classes.
    ///
    /// Inspects all `jdk.Compilation` events captured during the recording
    /// window and reports which methods in the target classes reached C2
    /// (tier 4) and which did not.
    ///
    /// If no target classes are specified, only aggregate compilation
    /// statistics are reported (total compilations and C2 count).
    ///
    /// @param targetClasses classes whose methods must reach C2; if empty,
    ///                      only aggregate stats are reported
    /// @return a [Result] containing the compilation events and verification outcome
    /// @throws IllegalStateException if verify has already been called
    public Result verify(Class<?>... targetClasses) {
        if (stopped) {
            throw new IllegalStateException("verify() has already been called");
        }
        stopped = true;
        recording.stop();

        Path dumpFile = null;
        try {
            dumpFile = Files.createTempFile("jit-warmup-", ".jfr");
            recording.dump(dumpFile);
            recording.close();

            capturedEvents.addAll(RecordingFile.readAllEvents(dumpFile));
        } catch (IOException e) {
            System.err.println("WARNING: JFR recording failed during JIT warmup: " + e.getMessage());
            return new Result(0, 0, Map.of());
        } finally {
            if (dumpFile != null) {
                try { Files.deleteIfExists(dumpFile); } catch (IOException ignored) {}
            }
        }

        long totalCompilations = capturedEvents.size();
        long c2Compilations = capturedEvents.stream()
            .filter(e -> e.getInt("compileLevel") == 4)
            .count();

        System.out.println("JIT Maturation: " + totalIterations + " iterations, "
            + totalCompilations + " compilations, "
            + c2Compilations + " at C2/tier-4");

        Map<String, Integer> targetMethodLevels = Map.of();
        if (targetClasses.length > 0) {
            Set<String> targetClassNames = new LinkedHashSet<>();
            for (Class<?> cls : targetClasses) {
                targetClassNames.add(cls.getName());
            }
            targetMethodLevels = reportC2Status(capturedEvents, targetClassNames);
        }

        return new Result(totalCompilations, c2Compilations, targetMethodLevels);
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        if (!stopped) {
            stopped = true;
            recording.stop();
            recording.close();
        }
    }

    /// Returns the events captured during the recording window.
    ///
    /// Only populated after [#verify] has been called.
    /// @return the captured JFR events
    public List<RecordedEvent> capturedEvents() {
        return Collections.unmodifiableList(capturedEvents);
    }

    /// The outcome of a [#verify] call.
    ///
    /// @param totalCompilations number of `jdk.Compilation` events captured
    /// @param c2Compilations    number of those events at compile level 4 (C2)
    /// @param targetMethodLevels map of `ClassName.methodNameDescriptor` to max compile level
    ///                           observed, for methods belonging to the requested target classes.
    ///                           Empty if no target classes were specified.
    public record Result(long totalCompilations, long c2Compilations,
                         Map<String, Integer> targetMethodLevels) {

        /// Returns `true` if every method in [#targetMethodLevels] reached C2 (tier 4).
        /// Returns `true` vacuously if no target classes were specified.
        /// @return true if all targets reached C2
        public boolean allTargetsReachedC2() {
            return targetMethodLevels.values().stream().allMatch(level -> level == 4);
        }

        /// Returns `true` if target classes were specified but no compilation
        /// events were found for any of them.
        /// @return true if no target events were found
        public boolean targetEventsAbsent() {
            return targetMethodLevels.isEmpty();
        }
    }

    // ---- Static convenience methods ----

    /// Forces JIT maturation using an arbitrary work unit (no C2 verification).
    ///
    /// Equivalent to constructing a guard, running 1M iterations, and verifying
    /// with no target classes.
    ///
    /// @param work the hot-path operation to force-compile
    public static void forceAndVerify(Runnable work) {
        forceAndVerify(work, new Class<?>[0]);
    }

    /// Forces JIT maturation and verifies C2 compilation of target classes.
    ///
    /// Creates a [JITMaturityGuard], runs the work for 1M iterations under
    /// JFR observation, then verifies that methods in the target classes
    /// reached C2 compilation.
    ///
    /// @param work          the hot-path operation to force-compile
    /// @param targetClasses classes whose methods must reach C2; if empty,
    ///                      no class-specific verification is performed
    public static void forceAndVerify(Runnable work, Class<?>... targetClasses) {
        try (JITMaturityGuard guard = new JITMaturityGuard()) {
            guard.run(work, DEFAULT_ITERATIONS);
            guard.verify(targetClasses);
        }
    }

    /// Filters JFR compilation events for the target classes and reports which
    /// methods reached C2 (tier 4) and which did not.
    ///
    /// @param events           all recorded `jdk.Compilation` events
    /// @param targetClassNames fully-qualified class names to filter on
    /// @return map of method signature to max compile level observed
    private static Map<String, Integer> reportC2Status(List<RecordedEvent> events,
                                                       Set<String> targetClassNames) {
        Map<String, Integer> methodMaxLevel = new LinkedHashMap<>();

        for (RecordedEvent event : events) {
            RecordedMethod method = event.getValue("method");
            if (method == null) continue;

            String typeName = method.getType().getName();
            if (!targetClassNames.contains(typeName)) continue;

            String methodKey = typeName + "." + method.getName() + method.getDescriptor();
            int level = event.getInt("compileLevel");
            methodMaxLevel.merge(methodKey, level, Math::max);
        }

        if (methodMaxLevel.isEmpty()) {
            System.out.println("WARNING: No JFR compilation events found for target classes: "
                + targetClassNames + ". The JFR recording was active but no compilations "
                + "were captured for these classes. Ensure the guard is constructed before "
                + "any invocations of the target methods.");
            return methodMaxLevel;
        }

        List<String> c2Methods = new ArrayList<>();
        List<String> nonC2Methods = new ArrayList<>();

        for (Map.Entry<String, Integer> entry : methodMaxLevel.entrySet()) {
            if (entry.getValue() == 4) {
                c2Methods.add(entry.getKey());
            } else {
                nonC2Methods.add(entry.getKey() + " (max tier " + entry.getValue() + ")");
            }
        }

        if (!c2Methods.isEmpty()) {
            System.out.println("C2-compiled methods (" + c2Methods.size() + "):");
            for (String m : c2Methods) {
                System.out.println("  [C2] " + m);
            }
        }

        if (!nonC2Methods.isEmpty()) {
            System.out.println("WARNING: Methods NOT reaching C2 (" + nonC2Methods.size() + "):");
            for (String m : nonC2Methods) {
                System.out.println("  [!C2] " + m);
            }
        }

        return methodMaxLevel;
    }
}
