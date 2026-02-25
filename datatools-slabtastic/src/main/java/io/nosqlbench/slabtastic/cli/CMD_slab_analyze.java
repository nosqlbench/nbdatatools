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

package io.nosqlbench.slabtastic.cli;

import io.nosqlbench.slabtastic.SlabPage;
import io.nosqlbench.slabtastic.SlabReader;
import picocli.CommandLine;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

/// Analyzes a slabtastic file and reports rich statistics.
///
/// Reports file size, page count, total record count, ordinal range,
/// detected content type, record size statistics with histogram, page
/// size statistics with histogram, page utilization with histogram, and
/// ordinal monotonicity classification.
///
/// Statistics are computed by sampling. Page statistics use sampled pages;
/// record statistics use sampled records. The default sample count is
/// min(1000, 1% of total). Use `--samples` or `--sample-percent` to
/// override.
@CommandLine.Command(
    name = "analyze",
    header = "Analyze file stats, layout, and content",
    description = "Analyzes a slabtastic file and displays structure, statistics, and content type detection.",
    exitCodeList = {"0: Success", "1: Error"}
)
public class CMD_slab_analyze implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "Path to the slabtastic file")
    private Path file;

    @CommandLine.Option(names = {"-v", "--verbose"}, description = "Show per-page details")
    private boolean verbose;

    @CommandLine.Option(names = {"--samples"},
        description = "Number of items to sample for statistics")
    private Integer samples;

    @CommandLine.Option(names = {"--sample-percent"},
        description = "Percentage of items to sample (0-100)")
    private Double samplePercent;

    @CommandLine.Option(names = {"--namespace", "-n"}, defaultValue = "",
        description = "Namespace to operate on (default: default namespace)")
    private String namespace;

    @Override
    public Integer call() {
        PrintStream out = System.out;
        try (SlabReader reader = new SlabReader(file)) {
            List<SlabReader.PageSummary> pages = reader.pages(namespace);
            long totalRecords = reader.recordCount(namespace);

            out.println("File:          " + file);
            out.printf("File size:     %,d bytes%n", reader.fileSize());
            out.printf("Pages:         %d%n", reader.pageCount(namespace));
            out.printf("Total records: %,d%n", totalRecords);
            if (!namespace.isEmpty()) {
                out.printf("Namespace:     %s%n", namespace);
            }

            if (!pages.isEmpty()) {
                long minOrd = pages.getFirst().startOrdinal();
                SlabReader.PageSummary last = pages.getLast();
                long maxOrd = last.startOrdinal() + last.recordCount() - 1;
                out.printf("Ordinal range: [%d, %d]%n", minOrd, maxOrd);
            }

            // Verbose per-page table
            if (verbose && !pages.isEmpty()) {
                out.println();
                out.printf("%-6s  %-14s  %-12s  %-12s  %-14s%n",
                    "Page", "Start Ordinal", "Records", "Page Size", "File Offset");
                out.println("-".repeat(66));
                for (int i = 0; i < pages.size(); i++) {
                    SlabReader.PageSummary ps = pages.get(i);
                    out.printf("%-6d  %-14d  %-12d  %-12d  %-14d%n",
                        i, ps.startOrdinal(), ps.recordCount(),
                        ps.pageSize(), ps.fileOffset());
                }
            }

            // Determine sample sizes
            int pageSampleCount = computeSampleCount(pages.size());
            int recordSampleCount = computeSampleCount(totalRecords);

            out.println();
            out.printf("Samples: %d pages, %d records%n", pageSampleCount, recordSampleCount);

            // Record size statistics (sampled)
            if (totalRecords > 0) {
                out.println();
                out.println("=== Record Size Statistics ===");
                long[] recordSizes = sampleRecordSizes(reader, pages, totalRecords, recordSampleCount);
                printStatisticsAndHistogram(out, recordSizes, "bytes");
            }

            // Page size statistics
            if (!pages.isEmpty()) {
                out.println();
                out.println("=== Page Size Statistics ===");
                long[] pageSizes = new long[pages.size()];
                for (int i = 0; i < pages.size(); i++) {
                    pageSizes[i] = pages.get(i).pageSize();
                }
                printStatisticsAndHistogram(out, pageSizes, "bytes");
            }

            // Page utilization (sampled)
            if (!pages.isEmpty()) {
                out.println();
                out.println("=== Page Utilization ===");
                double[] utilizations = samplePageUtilization(reader, pages, pageSampleCount);
                printUtilizationStats(out, utilizations);
            }

            // Content type detection (sampled)
            if (totalRecords > 0) {
                out.println();
                String detectedType = detectContentType(reader, pages, totalRecords, Math.min(recordSampleCount, 100));
                out.println("Detected content type: " + detectedType);
            }

            // Ordinal monotonicity (full walk)
            if (!pages.isEmpty()) {
                out.println();
                String monotonicity = classifyMonotonicity(pages);
                out.println("Ordinal monotonicity: " + monotonicity);
            }

            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    /// Computes the number of items to sample based on user options or
    /// the default heuristic of min(1000, 1% of total).
    private int computeSampleCount(long totalCount) {
        if (totalCount == 0) return 0;
        if (samples != null) {
            return (int) Math.min(samples, totalCount);
        }
        if (samplePercent != null) {
            return (int) Math.min(Math.max(1, (long) (totalCount * samplePercent / 100.0)), totalCount);
        }
        return (int) Math.min(1000, Math.max(1, totalCount / 100));
    }

    /// Samples record sizes by randomly picking ordinals from the namespace.
    private long[] sampleRecordSizes(SlabReader reader, List<SlabReader.PageSummary> pages,
                                     long totalRecords, int sampleCount) {
        if (sampleCount <= 0) return new long[0];

        // Build a list of all valid ordinal ranges
        long[] sizes = new long[sampleCount];
        List<long[]> ordinalRanges = new ArrayList<>();
        for (SlabReader.PageSummary ps : pages) {
            ordinalRanges.add(new long[]{ps.startOrdinal(), ps.startOrdinal() + ps.recordCount()});
        }

        // Sample random ordinals
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        for (int s = 0; s < sampleCount; s++) {
            // Pick a random record index across all pages
            long idx = rng.nextLong(totalRecords);
            long cumulative = 0;
            long ordinal = 0;
            for (SlabReader.PageSummary ps : pages) {
                if (idx < cumulative + ps.recordCount()) {
                    ordinal = ps.startOrdinal() + (idx - cumulative);
                    break;
                }
                cumulative += ps.recordCount();
            }

            Optional<ByteBuffer> data = reader.get(namespace, ordinal);
            sizes[s] = data.map(ByteBuffer::remaining).orElse(0);
        }
        return sizes;
    }

    /// Samples page utilization by reading full pages and computing
    /// active bytes vs total page size.
    private double[] samplePageUtilization(SlabReader reader, List<SlabReader.PageSummary> pages,
                                           int sampleCount) {
        if (sampleCount <= 0 || pages.isEmpty()) return new double[0];

        int count = Math.min(sampleCount, pages.size());
        double[] utilizations = new double[count];

        // Select pages to sample
        List<Integer> indices;
        if (count >= pages.size()) {
            indices = new ArrayList<>();
            for (int i = 0; i < pages.size(); i++) indices.add(i);
        } else {
            Set<Integer> selected = new LinkedHashSet<>();
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            while (selected.size() < count) {
                selected.add(rng.nextInt(pages.size()));
            }
            indices = new ArrayList<>(selected);
        }

        for (int s = 0; s < indices.size(); s++) {
            SlabReader.PageSummary ps = pages.get(indices.get(s));
            long activeBytes = 0;
            for (int i = 0; i < ps.recordCount(); i++) {
                long ordinal = ps.startOrdinal() + i;
                Optional<ByteBuffer> data = reader.get(namespace, ordinal);
                activeBytes += data.map(ByteBuffer::remaining).orElse(0);
            }
            utilizations[s] = ps.pageSize() > 0 ? (double) activeBytes / ps.pageSize() : 0.0;
        }
        return utilizations;
    }

    /// Detects the content type by sampling records and inspecting their bytes.
    private String detectContentType(SlabReader reader, List<SlabReader.PageSummary> pages,
                                     long totalRecords, int sampleCount) {
        if (sampleCount <= 0 || totalRecords == 0) return "unknown";

        boolean allJson = true;
        boolean allNewline = true;
        boolean allNull = true;
        boolean hasNullBytes = false;
        boolean hasNewlines = true;
        int checked = 0;

        // Sample the first N records sequentially
        outer:
        for (SlabReader.PageSummary ps : pages) {
            for (int i = 0; i < ps.recordCount() && checked < sampleCount; i++) {
                long ordinal = ps.startOrdinal() + i;
                Optional<ByteBuffer> data = reader.get(namespace, ordinal);
                if (data.isEmpty()) continue;

                ByteBuffer buf = data.get();
                byte[] bytes = new byte[buf.remaining()];
                buf.get(bytes);
                checked++;

                // Check for null bytes
                for (byte b : bytes) {
                    if (b == 0) {
                        hasNullBytes = true;
                        break;
                    }
                }

                // Check if ends with newline
                if (bytes.length == 0 || bytes[bytes.length - 1] != '\n') {
                    allNewline = false;
                }

                // Check if ends with null
                if (bytes.length == 0 || bytes[bytes.length - 1] != 0) {
                    allNull = false;
                }

                // Check JSON parsability
                if (allJson) {
                    String text = new String(bytes, StandardCharsets.UTF_8).trim();
                    if (!text.isEmpty()) {
                        try {
                            com.google.gson.JsonParser.parseString(text);
                        } catch (Exception e) {
                            allJson = false;
                        }
                    }
                }
            }
            if (checked >= sampleCount) break;
        }

        if (hasNullBytes && allNull) return "cstrings";
        if (allJson && allNewline) return "jsonl";
        if (allJson) return "json";
        if (hasNullBytes) return "binary";
        if (allNewline) return "text";
        return "binary";
    }

    /// Classifies ordinal monotonicity by walking all pages.
    ///
    /// @return "strictly monotonic", "monotonic with sparse gaps", or "non-monotonic"
    private String classifyMonotonicity(List<SlabReader.PageSummary> pages) {
        if (pages.size() <= 1) return "strictly monotonic";

        boolean strictlyContiguous = true;
        boolean monotonic = true;
        long prevFileOffset = pages.getFirst().fileOffset();
        long prevEndOrdinal = pages.getFirst().startOrdinal() + pages.getFirst().recordCount();

        for (int i = 1; i < pages.size(); i++) {
            SlabReader.PageSummary ps = pages.get(i);

            // Check file offset ordering (physical monotonicity)
            if (ps.fileOffset() < prevFileOffset) {
                monotonic = false;
            }
            prevFileOffset = ps.fileOffset();

            // Check ordinal contiguity
            if (ps.startOrdinal() != prevEndOrdinal) {
                strictlyContiguous = false;
            }
            prevEndOrdinal = ps.startOrdinal() + ps.recordCount();
        }

        if (!monotonic) return "non-monotonic";
        if (strictlyContiguous) return "strictly monotonic";
        return "monotonic with sparse gaps";
    }

    /// Prints min/avg/max and a text histogram for the given values.
    private void printStatisticsAndHistogram(PrintStream out, long[] values, String unit) {
        if (values.length == 0) return;

        long min = Long.MAX_VALUE, max = Long.MIN_VALUE, sum = 0;
        for (long v : values) {
            min = Math.min(min, v);
            max = Math.max(max, v);
            sum += v;
        }
        double avg = (double) sum / values.length;

        out.printf("  min: %,d %s%n", min, unit);
        out.printf("  avg: %,.1f %s%n", avg, unit);
        out.printf("  max: %,d %s%n", max, unit);

        printHistogram(out, values, min, max);
    }

    /// Prints min/avg/max and a text histogram for utilization ratios.
    private void printUtilizationStats(PrintStream out, double[] values) {
        if (values.length == 0) return;

        double min = Double.MAX_VALUE, max = Double.MIN_VALUE, sum = 0;
        for (double v : values) {
            min = Math.min(min, v);
            max = Math.max(max, v);
            sum += v;
        }
        double avg = sum / values.length;

        out.printf("  min: %.1f%%%n", min * 100);
        out.printf("  avg: %.1f%%%n", avg * 100);
        out.printf("  max: %.1f%%%n", max * 100);

        // Convert to long[] for histogram (in basis points for precision)
        long[] basisPoints = new long[values.length];
        for (int i = 0; i < values.length; i++) {
            basisPoints[i] = (long) (values[i] * 10000);
        }
        long bpMin = (long) (min * 10000);
        long bpMax = (long) (max * 10000);
        if (bpMin != bpMax) {
            printHistogramBasisPoints(out, basisPoints, bpMin, bpMax);
        }
    }

    /// Renders a text histogram with ~10 buckets.
    private void printHistogram(PrintStream out, long[] values, long min, long max) {
        if (min == max) return;

        int bucketCount = Math.min(10, (int) (max - min + 1));
        if (bucketCount <= 1) return;

        long range = max - min + 1;
        long bucketSize = Math.max(1, (range + bucketCount - 1) / bucketCount);
        int[] counts = new int[bucketCount];

        for (long v : values) {
            int bucket = (int) Math.min((v - min) / bucketSize, bucketCount - 1);
            counts[bucket]++;
        }

        int maxCount = 0;
        for (int c : counts) maxCount = Math.max(maxCount, c);

        out.println();
        for (int i = 0; i < bucketCount; i++) {
            long lo = min + i * bucketSize;
            long hi = Math.min(lo + bucketSize - 1, max);
            int barLen = maxCount > 0 ? (int) (30.0 * counts[i] / maxCount) : 0;
            out.printf("  %,8d-%,8d: %s (%d)%n", lo, hi,
                "\u2588".repeat(barLen), counts[i]);
        }
    }

    /// Renders a text histogram for basis-point utilization values.
    private void printHistogramBasisPoints(PrintStream out, long[] values, long min, long max) {
        int bucketCount = Math.min(10, (int) (max - min + 1));
        if (bucketCount <= 1) return;

        long range = max - min + 1;
        long bucketSize = Math.max(1, (range + bucketCount - 1) / bucketCount);
        int[] counts = new int[bucketCount];

        for (long v : values) {
            int bucket = (int) Math.min((v - min) / bucketSize, bucketCount - 1);
            counts[bucket]++;
        }

        int maxCount = 0;
        for (int c : counts) maxCount = Math.max(maxCount, c);

        out.println();
        for (int i = 0; i < bucketCount; i++) {
            long lo = min + i * bucketSize;
            long hi = Math.min(lo + bucketSize - 1, max);
            int barLen = maxCount > 0 ? (int) (30.0 * counts[i] / maxCount) : 0;
            out.printf("  %5.1f%%-%5.1f%%: %s (%d)%n",
                lo / 100.0, hi / 100.0,
                "\u2588".repeat(barLen), counts[i]);
        }
    }
}
