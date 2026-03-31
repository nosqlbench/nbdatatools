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

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

/// Reusable TPE-driven JMH optimization harness.
///
/// Extracts the generic benchmark-subprocess-round-tripping logic so that any
/// JMH benchmark in this module can be tuned with a [TpeSampler] by supplying
/// only a parameter space and a [CommandCustomizer].
///
/// Example usage:
/// ```java
/// JmhTpeOptimizer optimizer = JmhTpeOptimizer.builder()
///     .title("My Benchmark Tuning")
///     .paramSpace(space)
///     .commandCustomizer((params, jvmArgs, jmhArgs) -> {
///         jmhArgs.add("MyBenchmark");
///         jmhArgs.addAll(List.of("-p", "size=" + params.get("size")));
///     })
///     .build();
/// optimizer.run();
/// ```
public class JmhTpeOptimizer {

    /// Functional interface for benchmark-specific command-line customization.
    ///
    /// The optimizer builds the boilerplate (java command, -Xmx, --add-opens,
    /// -jar, -rf json, -rff, JMH iteration settings) and delegates
    /// benchmark-specific arguments to this customizer.
    @FunctionalInterface
    public interface CommandCustomizer {
        /// Adds benchmark-specific arguments to the JMH subprocess command.
        ///
        /// @param params the parameter configuration for this trial
        /// @param jvmArgs arguments inserted before `-jar` (e.g. `-Dprop=val`)
        /// @param jmhArgs arguments inserted after the jar path (benchmark name, `-p`, `-t`, etc.)
        void customize(Map<String, String> params,
                       List<String> jvmArgs,
                       List<String> jmhArgs);
    }

    private static final Path DEFAULT_BENCHMARKS_JAR = Path.of("datatools-predicated/target/benchmarks.jar");
    private static final String[] ADD_OPENS = {
        "--add-opens", "java.base/java.nio=ALL-UNNAMED",
        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens", "java.base/java.lang=ALL-UNNAMED"
    };

    private final String title;
    private final Path benchmarksJar;
    private final Path resultsFile;
    private final LinkedHashMap<String, String[]> paramSpace;
    private final CommandCustomizer commandCustomizer;
    private final String memory;
    private final int trials;
    private final int forks;
    private final int warmupIterations;
    private final int warmupSeconds;
    private final int measurementIterations;
    private final int measurementSeconds;
    private final Path jsonlFile;

    private JmhTpeOptimizer(Builder builder) {
        this.title = builder.title;
        this.benchmarksJar = builder.benchmarksJar;
        this.resultsFile = builder.resultsFile;
        this.paramSpace = builder.paramSpace;
        this.commandCustomizer = builder.commandCustomizer;
        this.memory = builder.memory;
        this.trials = builder.trials;
        this.forks = builder.forks;
        this.warmupIterations = builder.warmupIterations;
        this.warmupSeconds = builder.warmupSeconds;
        this.measurementIterations = builder.measurementIterations;
        this.measurementSeconds = builder.measurementSeconds;
        this.jsonlFile = builder.jsonlFile;
    }

    /// Creates a new builder for configuring a [JmhTpeOptimizer].
    /// @return a new builder instance
    public static Builder builder() {
        return new Builder();
    }

    /// Runs the TPE optimization loop.
    ///
    /// Validates that the benchmarks jar exists, then iterates: suggests a
    /// configuration via [TpeSampler], launches a JMH subprocess, extracts the
    /// score, and feeds it back. Writes markdown results at the end.
    /// If a JSONL file was configured via [Builder#jsonlFile(Path)], each trial
    /// is logged incrementally so the log survives crashes.
    public void run() {
        if (!Files.exists(benchmarksJar)) {
            System.err.println("ERROR: " + benchmarksJar + " not found. Run: mvn package -pl datatools-predicated -am -DskipTests");
            System.exit(1);
        }

        TpeSampler sampler = new TpeSampler(paramSpace);
        Path jsonFile = Path.of("current_jmh_result.json");

        System.out.println("=== " + title + " (TPE) ===");
        System.out.println("Search space: " + sampler.spaceSize() + " combinations");
        System.out.println("Trials: " + trials + " (first 10 random, then TPE-guided)");
        System.out.println();

        Instant startTime = Instant.now();
        BufferedWriter jsonlWriter = null;

        try {
            if (jsonlFile != null) {
                try {
                    jsonlWriter = Files.newBufferedWriter(jsonlFile,
                        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                    writeJsonlHeader(jsonlWriter, startTime, sampler.spaceSize());
                } catch (IOException e) {
                    System.err.println("WARNING: could not open JSONL log: " + e.getMessage());
                    jsonlWriter = null;
                }
            }

            for (int trial = 1; trial <= trials; trial++) {
                Optional<Map<String, String>> suggestion = sampler.suggest();
                if (suggestion.isEmpty()) {
                    System.out.println("Search space exhausted after " + sampler.trialCount() + " trials.");
                    break;
                }

                Map<String, String> params = suggestion.get();
                String phase = (trial <= 10) ? "random" : "TPE";
                System.out.printf("[%d/%d] (%s) %s%n", trial, trials, phase, formatParams(params));

                Instant trialStart = Instant.now();

                try {
                    double score = runBenchmark(params, jsonFile);
                    sampler.addTrial(params, score);

                    Optional<TpeSampler.Trial> best = sampler.getBestTrial();
                    double bestSoFar = best.map(TpeSampler.Trial::score).orElse(0.0);
                    System.out.printf("  -> %.0f ops/s | best so far: %.0f ops/s%n", score, bestSoFar);

                    if (jsonlWriter != null) {
                        try {
                            long trialDurationMs = Duration.between(trialStart, Instant.now()).toMillis();
                            long elapsedMs = Duration.between(startTime, Instant.now()).toMillis();
                            writeJsonlTrial(jsonlWriter, trial, phase, params, score, bestSoFar,
                                trialDurationMs, elapsedMs);
                        } catch (IOException e) {
                            System.err.println("WARNING: failed to write JSONL trial: " + e.getMessage());
                        }
                    }
                } catch (IOException | InterruptedException e) {
                    System.err.println("  ERROR: " + e.getMessage());
                    sampler.addTrial(params, -1);

                    if (jsonlWriter != null) {
                        try {
                            long trialDurationMs = Duration.between(trialStart, Instant.now()).toMillis();
                            long elapsedMs = Duration.between(startTime, Instant.now()).toMillis();
                            double bestSoFar = sampler.getBestTrial()
                                .map(TpeSampler.Trial::score).orElse(0.0);
                            writeJsonlTrial(jsonlWriter, trial, phase, params, -1, bestSoFar,
                                trialDurationMs, elapsedMs);
                        } catch (IOException ex) {
                            System.err.println("WARNING: failed to write JSONL trial: " + ex.getMessage());
                        }
                    }
                }
            }

            Duration elapsed = Duration.between(startTime, Instant.now());

            if (jsonlWriter != null) {
                try {
                    writeJsonlSummary(jsonlWriter, sampler, elapsed.toMillis());
                } catch (IOException e) {
                    System.err.println("WARNING: failed to write JSONL summary: " + e.getMessage());
                }
            }

            System.out.println();
            System.out.println("=== Optimization complete (" + formatDuration(elapsed) + ") ===");

            try {
                writeResults(sampler, elapsed);
            } catch (IOException e) {
                System.err.println("ERROR writing results: " + e.getMessage());
            }

            List<TpeSampler.Trial> top = sampler.getAllTrialsSorted();
            int showN = Math.min(10, top.size());
            System.out.println();
            System.out.println("Top " + showN + " configurations:");

            List<String> paramNames = new ArrayList<>(paramSpace.keySet());
            StringBuilder headerFmt = new StringBuilder();
            StringBuilder divider = new StringBuilder();
            for (String name : paramNames) {
                int width = Math.max(name.length(), 11);
                headerFmt.append("%-").append(width + 2).append("s ");
                divider.append("-".repeat(width + 2)).append(" ");
            }
            headerFmt.append("%18s%n");
            divider.append("-".repeat(18));

            Object[] headerArgs = new Object[paramNames.size() + 1];
            for (int i = 0; i < paramNames.size(); i++) headerArgs[i] = paramNames.get(i);
            headerArgs[paramNames.size()] = "Throughput";
            System.out.printf(headerFmt.toString(), headerArgs);
            System.out.println(divider);

            for (int i = 0; i < showN; i++) {
                TpeSampler.Trial t = top.get(i);
                Map<String, String> p = t.params();
                Object[] rowArgs = new Object[paramNames.size() + 1];
                for (int j = 0; j < paramNames.size(); j++) {
                    rowArgs[j] = p.get(paramNames.get(j));
                }
                rowArgs[paramNames.size()] = String.format("%.0f ops/s", t.score());

                StringBuilder rowFmt = new StringBuilder();
                for (String name : paramNames) {
                    int width = Math.max(name.length(), 11);
                    rowFmt.append("%-").append(width + 2).append("s ");
                }
                rowFmt.append("%18s%n");
                System.out.printf(rowFmt.toString(), rowArgs);
            }

            System.out.println();
            if (resultsFile != null) {
                System.out.println("Results written to " + resultsFile);
            }
            if (jsonlFile != null) {
                System.out.println("JSONL trial log written to " + jsonlFile);
            }
        } finally {
            if (jsonlWriter != null) {
                try {
                    jsonlWriter.close();
                } catch (IOException e) {
                    System.err.println("ERROR closing JSONL writer: " + e.getMessage());
                }
            }
        }
    }

    private double runBenchmark(Map<String, String> params, Path jsonFile) throws IOException, InterruptedException {
        Files.deleteIfExists(jsonFile);

        String javaCmd = ProcessHandle.current().info().command().orElse("java");

        List<String> jvmArgs = new ArrayList<>();
        List<String> jmhArgs = new ArrayList<>();
        commandCustomizer.customize(params, jvmArgs, jmhArgs);

        List<String> cmd = new ArrayList<>();
        cmd.add(javaCmd);
        cmd.add("-Xmx" + memory);
        Collections.addAll(cmd, ADD_OPENS);
        cmd.addAll(jvmArgs);
        cmd.add("-jar");
        cmd.add(benchmarksJar.toString());
        cmd.addAll(jmhArgs);
        cmd.add("-f"); cmd.add(String.valueOf(forks));
        cmd.add("-wi"); cmd.add(String.valueOf(warmupIterations));
        cmd.add("-i"); cmd.add(String.valueOf(measurementIterations));
        cmd.add("-w"); cmd.add(String.valueOf(warmupSeconds));
        cmd.add("-r"); cmd.add(String.valueOf(measurementSeconds));
        cmd.add("-rf"); cmd.add("json");
        cmd.add("-rff"); cmd.add(jsonFile.toString());

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.inheritIO();
        Process process = pb.start();
        int exitCode = process.waitFor();

        if (exitCode != 0 || !Files.exists(jsonFile)) {
            System.err.println("  WARNING: benchmark trial failed (exit=" + exitCode + ")");
            return -1;
        }

        return JmhJsonScoreExtractor.extractScore(jsonFile);
    }

    private void writeResults(TpeSampler sampler, Duration elapsed) throws IOException {
        if (resultsFile == null) return;

        StringBuilder sb = new StringBuilder();
        String timestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'")
            .format(Instant.now().atOffset(ZoneOffset.UTC));

        sb.append("# ").append(title).append(" Results (TPE Optimizer)\n\n");
        sb.append("- **Date:** ").append(timestamp).append("\n");
        sb.append("- **Algorithm:** TPE (Tree-structured Parzen Estimator)\n");
        sb.append("- **Trials:** ").append(sampler.trialCount()).append(" of ").append(sampler.spaceSize()).append(" possible\n");
        sb.append("- **Elapsed:** ").append(formatDuration(elapsed)).append("\n");
        sb.append("- **Heap:** ").append(memory).append("\n");
        sb.append("- **JMH:** warmup ").append(warmupIterations).append("x").append(warmupSeconds)
            .append("s, measurement ").append(measurementIterations).append("x").append(measurementSeconds)
            .append("s, ").append(forks).append(" fork\n");
        sb.append("\n");

        List<String> paramNames = new ArrayList<>(paramSpace.keySet());

        sb.append("## Best Configuration\n\n");
        sampler.getBestTrial().ifPresent(best -> {
            Map<String, String> p = best.params();
            sb.append("| Parameter | Value |\n");
            sb.append("|-----------|-------|\n");
            for (var entry : p.entrySet()) {
                sb.append("| ").append(entry.getKey()).append(" | ").append(entry.getValue()).append(" |\n");
            }
            sb.append("| **Throughput** | **").append(String.format("%.0f", best.score())).append(" ops/s** |\n");
            sb.append("\n");
        });

        sb.append("## All Trials (ranked by throughput)\n\n");
        sb.append("| Rank |");
        for (String name : paramNames) sb.append(" ").append(name).append(" |");
        sb.append(" Throughput (ops/s) |\n");
        sb.append("|-----:|");
        for (String ignored : paramNames) sb.append("------------|");
        sb.append("-------------------:|\n");

        List<TpeSampler.Trial> sorted = sampler.getAllTrialsSorted();
        for (int i = 0; i < sorted.size(); i++) {
            TpeSampler.Trial t = sorted.get(i);
            Map<String, String> p = t.params();
            sb.append("| ").append(i + 1);
            for (String name : paramNames) sb.append(" | ").append(p.get(name));
            sb.append(" | ").append(String.format("%.0f", t.score())).append(" |\n");
        }
        sb.append("\n");

        sb.append("## Trial Execution Order\n\n");
        sb.append("| Trial | Phase |");
        for (String name : paramNames) sb.append(" ").append(name).append(" |");
        sb.append(" Throughput (ops/s) |\n");
        sb.append("|------:|-------|");
        for (String ignored : paramNames) sb.append("------------|");
        sb.append("-------------------:|\n");

        List<TpeSampler.Trial> chronological = sampler.getTrialsInOrder();
        int nStartup = 10;
        for (int i = 0; i < chronological.size(); i++) {
            TpeSampler.Trial t = chronological.get(i);
            Map<String, String> p = t.params();
            String phase = (i < nStartup) ? "random" : "TPE";
            sb.append("| ").append(i + 1).append(" | ").append(phase);
            for (String name : paramNames) sb.append(" | ").append(p.get(name));
            sb.append(" | ").append(String.format("%.0f", t.score())).append(" |\n");
        }

        Files.writeString(resultsFile, sb.toString());
    }

    /// Formats a parameter map as a space-separated `key=value` string for display.
    static String formatParams(Map<String, String> params) {
        StringBuilder sb = new StringBuilder();
        for (var entry : params.entrySet()) {
            if (!sb.isEmpty()) sb.append(" ");
            sb.append(entry.getKey()).append("=").append(entry.getValue());
        }
        return sb.toString();
    }

    /// Formats a [Duration] as a human-readable string (e.g. "5m 30s" or "45s").
    /// @param d the duration to format
    /// @return the formatted string
    public static String formatDuration(Duration d) {
        long minutes = d.toMinutes();
        long seconds = d.toSecondsPart();
        if (minutes > 0) return minutes + "m " + seconds + "s";
        return seconds + "s";
    }

    /// Writes the JSONL header line containing run configuration metadata.
    private void writeJsonlHeader(BufferedWriter writer, Instant startTime, int spaceSize) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"type\":\"header\"");
        sb.append(",\"title\":\"").append(escapeJson(title)).append('"');
        sb.append(",\"spaceSize\":").append(spaceSize);
        sb.append(",\"totalTrials\":").append(trials);
        sb.append(",\"memory\":\"").append(escapeJson(memory)).append('"');
        sb.append(",\"forks\":").append(forks);
        sb.append(",\"warmupIterations\":").append(warmupIterations);
        sb.append(",\"warmupSeconds\":").append(warmupSeconds);
        sb.append(",\"measurementIterations\":").append(measurementIterations);
        sb.append(",\"measurementSeconds\":").append(measurementSeconds);
        sb.append(",\"startTime\":\"").append(startTime.atOffset(ZoneOffset.UTC)
            .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)).append('"');
        sb.append('}');
        writer.write(sb.toString());
        writer.newLine();
        writer.flush();
    }

    /// Writes a JSONL trial line immediately after a trial completes.
    private void writeJsonlTrial(BufferedWriter writer, int trial, String phase,
                                 Map<String, String> params, double score, double bestSoFar,
                                 long trialDurationMs, long elapsedMs) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"type\":\"trial\"");
        sb.append(",\"trial\":").append(trial);
        sb.append(",\"phase\":\"").append(escapeJson(phase)).append('"');
        sb.append(",\"params\":").append(mapToJson(params));
        sb.append(",\"score\":").append(score);
        sb.append(",\"bestSoFar\":").append(bestSoFar);
        sb.append(",\"trialDurationMs\":").append(trialDurationMs);
        sb.append(",\"elapsedMs\":").append(elapsedMs);
        sb.append(",\"timestamp\":\"").append(Instant.now().atOffset(ZoneOffset.UTC)
            .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)).append('"');
        sb.append('}');
        writer.write(sb.toString());
        writer.newLine();
        writer.flush();
    }

    /// Writes the JSONL summary line after all trials complete.
    private void writeJsonlSummary(BufferedWriter writer, TpeSampler sampler,
                                   long totalElapsedMs) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"type\":\"summary\"");
        sb.append(",\"trialsCompleted\":").append(sampler.trialCount());
        Optional<TpeSampler.Trial> best = sampler.getBestTrial();
        sb.append(",\"bestScore\":").append(best.map(TpeSampler.Trial::score).orElse(0.0));
        sb.append(",\"bestParams\":").append(mapToJson(
            best.map(TpeSampler.Trial::params).orElse(Map.of())));
        sb.append(",\"totalElapsedMs\":").append(totalElapsedMs);
        sb.append(",\"endTime\":\"").append(Instant.now().atOffset(ZoneOffset.UTC)
            .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)).append('"');
        sb.append('}');
        writer.write(sb.toString());
        writer.newLine();
        writer.flush();
    }

    /// Escapes a string for use as a JSON string value.
    ///
    /// Handles backslash, double-quote, and control characters (U+0000 through U+001F).
    /// @param value the string to escape
    /// @return the escaped JSON string
    public static String escapeJson(String value) {
        if (value == null) return "null";
        StringBuilder sb = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.toString();
    }

    /// Serializes a string-to-string map as a JSON object.
    ///
    /// Keys and values are escaped with [#escapeJson(String)].
    /// @param map the map to serialize
    /// @return the JSON object string
    public static String mapToJson(Map<String, String> map) {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        boolean first = true;
        for (var entry : map.entrySet()) {
            if (!first) sb.append(',');
            sb.append('"').append(escapeJson(entry.getKey())).append("\":\"")
                .append(escapeJson(entry.getValue())).append('"');
            first = false;
        }
        sb.append('}');
        return sb.toString();
    }

    /// Builder for configuring a [JmhTpeOptimizer].
    public static class Builder {
        private String title = "JMH Tuning";
        private Path benchmarksJar = DEFAULT_BENCHMARKS_JAR;
        private Path resultsFile;
        private LinkedHashMap<String, String[]> paramSpace;
        private CommandCustomizer commandCustomizer;
        private String memory = "10g";
        private int trials = 40;
        private int forks = 1;
        private int warmupIterations = 1;
        private int warmupSeconds = 3;
        private int measurementIterations = 2;
        private int measurementSeconds = 5;
        private Path jsonlFile;

        private Builder() {}

        /// Sets the title for this optimization run, used in output and reports.
        /// @param title the run title
        /// @return this builder
        public Builder title(String title) {
            this.title = title;
            return this;
        }

        /// Sets the path to the JMH benchmarks jar.
        /// @param benchmarksJar the path to the jar
        /// @return this builder
        public Builder benchmarksJar(Path benchmarksJar) {
            this.benchmarksJar = benchmarksJar;
            return this;
        }

        /// Sets the path for the markdown results file.
        /// @param resultsFile the path for results output
        /// @return this builder
        public Builder resultsFile(Path resultsFile) {
            this.resultsFile = resultsFile;
            return this;
        }

        /// Sets the parameter search space as an ordered map of name to possible values.
        /// @param paramSpace the parameter space
        /// @return this builder
        public Builder paramSpace(LinkedHashMap<String, String[]> paramSpace) {
            this.paramSpace = paramSpace;
            return this;
        }

        /// Sets the benchmark-specific command customizer.
        /// @param commandCustomizer the command customizer
        /// @return this builder
        public Builder commandCustomizer(CommandCustomizer commandCustomizer) {
            this.commandCustomizer = commandCustomizer;
            return this;
        }

        /// Sets the JVM heap size (e.g. "10g"). Defaults to "10g".
        /// @param memory the heap size string
        /// @return this builder
        public Builder memory(String memory) {
            this.memory = memory;
            return this;
        }

        /// Sets the number of TPE trials to run. Defaults to 40.
        /// @param trials the number of trials
        /// @return this builder
        public Builder trials(int trials) {
            this.trials = trials;
            return this;
        }

        /// Sets the number of JMH forks per trial. Defaults to 1.
        /// @param forks the number of forks
        /// @return this builder
        public Builder forks(int forks) {
            this.forks = forks;
            return this;
        }

        /// Sets the number of JMH warmup iterations. Defaults to 1.
        /// @param warmupIterations the number of warmup iterations
        /// @return this builder
        public Builder warmupIterations(int warmupIterations) {
            this.warmupIterations = warmupIterations;
            return this;
        }

        /// Sets the duration of each JMH warmup iteration in seconds. Defaults to 3.
        /// @param warmupSeconds the warmup duration in seconds
        /// @return this builder
        public Builder warmupSeconds(int warmupSeconds) {
            this.warmupSeconds = warmupSeconds;
            return this;
        }

        /// Sets the number of JMH measurement iterations. Defaults to 2.
        /// @param measurementIterations the number of measurement iterations
        /// @return this builder
        public Builder measurementIterations(int measurementIterations) {
            this.measurementIterations = measurementIterations;
            return this;
        }

        /// Sets the duration of each JMH measurement iteration in seconds. Defaults to 5.
        /// @param measurementSeconds the measurement duration in seconds
        /// @return this builder
        public Builder measurementSeconds(int measurementSeconds) {
            this.measurementSeconds = measurementSeconds;
            return this;
        }

        /// Sets the path for an incremental JSONL trial log.
        ///
        /// When set, the optimizer writes one JSON object per line: a header line at
        /// the start, a trial line after each benchmark evaluation (flushed immediately
        /// so the log survives crashes), and a summary line at the end.
        /// If not set, no JSONL log is written.
        ///
        /// @param jsonlFile path to the JSONL output file
        /// @return this builder
        public Builder jsonlFile(Path jsonlFile) {
            this.jsonlFile = jsonlFile;
            return this;
        }

        /// Builds and returns the configured [JmhTpeOptimizer].
        ///
        /// @return the configured optimizer
        /// @throws IllegalStateException if paramSpace or commandCustomizer is not set
        public JmhTpeOptimizer build() {
            if (paramSpace == null) throw new IllegalStateException("paramSpace is required");
            if (commandCustomizer == null) throw new IllegalStateException("commandCustomizer is required");
            return new JmhTpeOptimizer(this);
        }
    }
}
