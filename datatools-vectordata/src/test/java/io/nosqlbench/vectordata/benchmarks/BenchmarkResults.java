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

package io.nosqlbench.vectordata.benchmarks;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.openjdk.jmh.results.RunResult;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

/// Post-processes JMH {@link RunResult} entries to print a normalized summary
/// table and append each result as a JSON line to a rolling history file.
///
/// All results are normalized to a single unit — **records/s** — regardless of
/// whether JMH measured throughput ({@code ops/ms}) or average time
/// ({@code ms/op}). Batch benchmarks that process {@code batchSize} records per
/// JMH operation are scaled accordingly so comparisons across batch sizes,
/// benchmark methods, and measurement modes are all apples-to-apples.
///
/// The JSONL file is opened in append mode so successive runs accumulate a
/// history that can be diffed, plotted, or fed into regression checks.
public final class BenchmarkResults {

    private BenchmarkResults() {
    }

    /// Benchmark methods that touch exactly one record per JMH operation,
    /// regardless of the {@code batchSize} parameter.
    private static final Set<String> SINGLE_RECORD_OPS = Set.of(
        "singleVectorGet", "neighborIndicesGet",
        "singlePredicateGet", "metadataContentGet",
        "resultIndicesGet"
    );

    /// Benchmark methods that measure infrastructure cost (not per-record I/O).
    private static final Set<String> META_OPS = Set.of(
        "metadataLayoutLoad", "predicateContextResolve"
    );

    /// Prints a formatted records/s summary and appends each result as a JSON
    /// line to the rolling results log.
    ///
    /// @param results        the JMH run results
    /// @param runConfig      generation parameters (records, queries, seed, etc.)
    /// @param benchmarkLabel human-readable label for the summary header
    /// @param resultsLogPath path to the rolling JSONL file (appended, never overwritten)
    /// @throws IOException if writing to the JSONL file fails
    public static void summarizeAndLog(
        Collection<RunResult> results,
        Map<String, Object> runConfig,
        String benchmarkLabel,
        String resultsLogPath
    ) throws IOException {
        if (results.isEmpty()) return;

        String bar = "═".repeat(100);
        System.out.println();
        System.out.println(bar);
        System.out.println("  " + benchmarkLabel);
        StringJoiner configLine = new StringJoiner(" | ", "  ", "");
        runConfig.forEach((k, v) -> configLine.add(k + "=" + v));
        System.out.println(configLine);
        System.out.println(bar);
        System.out.println();

        System.out.printf("  %-28s %-18s  %-22s  %s%n",
            "Benchmark", "Params", "Records/s", "Raw Score");
        System.out.println("  " + "─".repeat(90));

        Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().create();
        List<String> jsonlLines = new ArrayList<>();
        String timestamp = Instant.now().toString();

        for (RunResult rr : results) {
            var bp = rr.getParams();
            String fullName = bp.getBenchmark();
            String method = fullName.substring(fullName.lastIndexOf('.') + 1);
            String mode = bp.getMode().shortLabel();

            var primary = rr.getPrimaryResult();
            double score = primary.getScore();
            double error = primary.getScoreError();
            String unit = primary.getScoreUnit();

            Map<String, String> params = new LinkedHashMap<>();
            for (String key : bp.getParamsKeys()) {
                params.put(key, bp.getParam(key));
            }

            // compact params display
            StringJoiner pj = new StringJoiner(" ");
            if (params.containsKey("dimension")) pj.add("d=" + params.get("dimension"));
            if (params.containsKey("backendType")) pj.add(params.get("backendType"));
            if (params.containsKey("batchSize")) pj.add("bs=" + params.get("batchSize"));
            String paramStr = pj.toString();

            int batchSize = params.containsKey("batchSize")
                ? Integer.parseInt(params.get("batchSize")) : 1;

            // Normalize everything to records/s for direct comparison
            double recordsPerSec;

            if (META_OPS.contains(method)) {
                // Infrastructure ops: convert raw score to ops/s
                if ("thrpt".equals(mode)) {
                    recordsPerSec = score * 1000.0; // ops/ms → ops/s
                } else {
                    recordsPerSec = (score > 0) ? 1000.0 / score : 0; // ms/op → ops/s
                }
            } else if (SINGLE_RECORD_OPS.contains(method) || batchSize <= 1) {
                if ("thrpt".equals(mode)) {
                    recordsPerSec = score * 1000.0; // ops/ms → records/s
                } else {
                    recordsPerSec = (score > 0) ? 1000.0 / score : 0; // ms/record → records/s
                }
            } else {
                if ("thrpt".equals(mode)) {
                    recordsPerSec = score * batchSize * 1000.0; // ops/ms * records/op → records/s
                } else {
                    double perRecord = score / batchSize; // ms/record
                    recordsPerSec = (perRecord > 0) ? 1000.0 / perRecord : 0;
                }
            }

            String rateStr = fmtRate(recordsPerSec) + " records/s";
            String rawStr = fmtScore(score) + " \u00b1" + fmtScore(error) + " " + unit;

            System.out.printf("  %-28s %-18s  %-22s  %s%n",
                method, paramStr, rateStr, rawStr);

            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("timestamp", timestamp);
            entry.put("benchmark", method);
            entry.put("params", params);
            entry.put("mode", mode);
            entry.put("score", score);
            entry.put("scoreError", error);
            entry.put("scoreUnit", unit);
            entry.put("recordsPerSec", recordsPerSec);
            entry.put("config", runConfig);
            jsonlLines.add(gson.toJson(entry));
        }

        System.out.println();

        try (FileWriter writer = new FileWriter(resultsLogPath, true)) {
            for (String line : jsonlLines) {
                writer.write(line);
                writer.write('\n');
            }
        }
        System.out.println("  " + jsonlLines.size() + " results appended to " + resultsLogPath);
        System.out.println();
    }

    /// Formats a rate value with SI suffixes for readability.
    ///
    /// @param v the rate in records/s
    /// @return human-readable string like "1.23M" or "456K"
    private static String fmtRate(double v) {
        if (Double.isNaN(v) || Double.isInfinite(v) || v == 0) return String.valueOf(v);
        double abs = Math.abs(v);
        if (abs >= 1_000_000_000) return String.format("%,.2fG", v / 1_000_000_000);
        if (abs >= 1_000_000) return String.format("%,.2fM", v / 1_000_000);
        if (abs >= 1_000) return String.format("%,.1fK", v / 1_000);
        return String.format("%.1f", v);
    }

    /// Formats a score value with appropriate precision.
    ///
    /// @param v the value to format
    /// @return human-readable string
    private static String fmtScore(double v) {
        if (Double.isNaN(v) || Double.isInfinite(v)) return String.valueOf(v);
        double abs = Math.abs(v);
        if (abs == 0) return "0";
        if (abs >= 10_000) return String.format("%,.0f", v);
        if (abs >= 100) return String.format("%,.1f", v);
        if (abs >= 1) return String.format("%.3f", v);
        if (abs >= 0.001) return String.format("%.4f", v);
        if (abs >= 0.000_001) return String.format("%.3e", v);
        return String.format("%.2e", v);
    }
}
