package io.nosqlbench.command.analyze.subcommands;

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

import io.nosqlbench.vshapes.extract.DimensionStatistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.Callable;

/// Show detailed statistics for dimensions in a vector file.
///
/// This command computes and displays statistical properties for one or more
/// dimensions in a vector file, including moments (mean, variance, skewness,
/// kurtosis), range (min, max), and percentiles.
///
/// ## Usage
///
/// ```bash
/// # Single dimension
/// nbvectors analyze stats --input data.fvec --dimension 23
///
/// # All dimensions (summary table)
/// nbvectors analyze stats --input data.fvec --all-dimensions
/// ```
///
/// ## Output
///
/// For a single dimension, shows detailed statistics including percentiles.
/// For all dimensions, shows a summary table with key metrics.
@CommandLine.Command(
    name = "stats",
    header = "Show dimension statistics for a vector file",
    description = "Computes statistical properties (mean, variance, skewness, kurtosis, percentiles) for vector dimensions.",
    exitCodeList = {
        "0: Success",
        "1: Error reading file"
    }
)
public class CMD_analyze_stats implements Callable<Integer> {

    private static final Logger logger = LogManager.getLogger(CMD_analyze_stats.class);

    // ANSI color codes
    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_BOLD = "\u001B[1m";
    private static final String ANSI_DIM = "\u001B[2m";
    private static final String ANSI_BLUE = "\u001B[94m";
    private static final String ANSI_CYAN = "\u001B[96m";

    @CommandLine.Option(
        names = {"--input", "-i"},
        description = "Path to the vector file (.fvec format)",
        required = true
    )
    private Path inputPath;

    @CommandLine.Option(
        names = {"--dimension", "-d"},
        description = "Specific dimension to analyze (0-indexed)"
    )
    private Integer dimension;

    @CommandLine.Option(
        names = {"--all-dimensions", "-a"},
        description = "Show statistics for all dimensions (summary table)"
    )
    private boolean allDimensions = false;

    @CommandLine.Option(
        names = {"--sample", "-s"},
        description = "Maximum number of vectors to sample (default: all)"
    )
    private Integer sampleSize;

    @Override
    public Integer call() {
        try {
            if (!Files.exists(inputPath)) {
                System.err.println("Error: File not found: " + inputPath);
                return 1;
            }

            if (dimension == null && !allDimensions) {
                System.err.println("Error: Specify --dimension or --all-dimensions");
                return 1;
            }

            // Read file header to get dimensions
            int[] fileInfo = readFvecHeader(inputPath);
            int dimensions = fileInfo[0];
            int vectorCount = fileInfo[1];

            int samplesToUse = sampleSize != null ? Math.min(sampleSize, vectorCount) : vectorCount;

            System.out.println();
            System.out.println("═══════════════════════════════════════════════════════════════════════════════");
            System.out.println("                           DIMENSION STATISTICS                                ");
            System.out.println("═══════════════════════════════════════════════════════════════════════════════");
            System.out.println();
            System.out.printf("File: %s%n", inputPath);
            System.out.printf("Dimensions: %d%n", dimensions);
            System.out.printf("Vectors: %,d%s%n", vectorCount,
                sampleSize != null ? String.format(" (sampling %,d)", samplesToUse) : "");
            System.out.println();

            if (allDimensions) {
                printAllDimensionsTable(dimensions, samplesToUse);
            } else {
                if (dimension < 0 || dimension >= dimensions) {
                    System.err.printf("Error: Dimension %d out of range [0, %d)%n", dimension, dimensions);
                    return 1;
                }
                printSingleDimensionStats(dimension, dimensions, samplesToUse);
            }

            return 0;

        } catch (Exception e) {
            logger.error("Error analyzing file", e);
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    private void printAllDimensionsTable(int dimensions, int vectorCount) throws IOException {
        System.out.println("Dimension Statistics Summary:");
        System.out.println("┌───────┬────────────┬────────────┬────────────┬────────────┬────────────┬────────────┐");
        System.out.println("│  Dim  │    Mean    │   StdDev   │    Min     │    Max     │  Skewness  │  Kurtosis  │");
        System.out.println("├───────┼────────────┼────────────┼────────────┼────────────┼────────────┼────────────┤");

        for (int d = 0; d < dimensions; d++) {
            float[] values = readDimensionValues(d, dimensions, vectorCount);
            DimensionStatistics stats = DimensionStatistics.compute(d, values);

            System.out.printf("│ %5d │ %10.6f │ %10.6f │ %10.6f │ %10.6f │ %10.4f │ %10.4f │%n",
                d,
                stats.mean(),
                stats.stdDev(),
                stats.min(),
                stats.max(),
                stats.skewness(),
                stats.kurtosis());

            // Show progress for large files
            if (dimensions > 100 && (d + 1) % 50 == 0) {
                System.err.printf("\r  Processing: %d/%d dimensions...", d + 1, dimensions);
            }
        }

        if (dimensions > 100) {
            System.err.printf("\r  Processing: %d/%d dimensions... done.%n", dimensions, dimensions);
        }

        System.out.println("└───────┴────────────┴────────────┴────────────┴────────────┴────────────┴────────────┘");
    }

    private void printSingleDimensionStats(int dim, int dimensions, int vectorCount) throws IOException {
        float[] values = readDimensionValues(dim, dimensions, vectorCount);
        DimensionStatistics stats = DimensionStatistics.compute(dim, values);

        // Compute percentiles
        float[] sorted = values.clone();
        Arrays.sort(sorted);

        double p1 = percentile(sorted, 1);
        double p5 = percentile(sorted, 5);
        double p25 = percentile(sorted, 25);
        double p50 = percentile(sorted, 50);
        double p75 = percentile(sorted, 75);
        double p95 = percentile(sorted, 95);
        double p99 = percentile(sorted, 99);

        System.out.printf("%sDimension %d Statistics%s%n", ANSI_BOLD, dim, ANSI_RESET);
        System.out.println("─────────────────────────────────────");
        System.out.println();

        System.out.println(ANSI_CYAN + "Central Tendency:" + ANSI_RESET);
        System.out.printf("  Mean:          %12.8f%n", stats.mean());
        System.out.printf("  Median (p50):  %12.8f%n", p50);
        System.out.println();

        System.out.println(ANSI_CYAN + "Dispersion:" + ANSI_RESET);
        System.out.printf("  Std Dev:       %12.8f%n", stats.stdDev());
        System.out.printf("  Variance:      %12.8f%n", stats.variance());
        System.out.printf("  Range:         %12.8f%n", stats.max() - stats.min());
        System.out.println();

        System.out.println(ANSI_CYAN + "Range:" + ANSI_RESET);
        System.out.printf("  Min:           %12.8f%n", stats.min());
        System.out.printf("  Max:           %12.8f%n", stats.max());
        System.out.println();

        System.out.println(ANSI_CYAN + "Shape:" + ANSI_RESET);
        System.out.printf("  Skewness:      %12.4f  %s%n", stats.skewness(), interpretSkewness(stats.skewness()));
        System.out.printf("  Kurtosis:      %12.4f  %s%n", stats.kurtosis(), interpretKurtosis(stats.kurtosis()));
        System.out.println();

        System.out.println(ANSI_CYAN + "Percentiles:" + ANSI_RESET);
        System.out.printf("  p1:            %12.8f%n", p1);
        System.out.printf("  p5:            %12.8f%n", p5);
        System.out.printf("  p25 (Q1):      %12.8f%n", p25);
        System.out.printf("  p50 (Median):  %12.8f%n", p50);
        System.out.printf("  p75 (Q3):      %12.8f%n", p75);
        System.out.printf("  p95:           %12.8f%n", p95);
        System.out.printf("  p99:           %12.8f%n", p99);
        System.out.println();

        // IQR and outlier info
        double iqr = p75 - p25;
        double lowerFence = p25 - 1.5 * iqr;
        double upperFence = p75 + 1.5 * iqr;
        int outlierCount = countOutliers(sorted, lowerFence, upperFence);

        System.out.println(ANSI_CYAN + "Outlier Detection (1.5×IQR):" + ANSI_RESET);
        System.out.printf("  IQR:           %12.8f%n", iqr);
        System.out.printf("  Lower fence:   %12.8f%n", lowerFence);
        System.out.printf("  Upper fence:   %12.8f%n", upperFence);
        System.out.printf("  Outliers:      %d (%.2f%%)%n", outlierCount, 100.0 * outlierCount / sorted.length);
    }

    private String interpretSkewness(double skewness) {
        if (Math.abs(skewness) < 0.5) return ANSI_DIM + "(approximately symmetric)" + ANSI_RESET;
        if (skewness > 0) return ANSI_DIM + "(right-skewed / positive tail)" + ANSI_RESET;
        return ANSI_DIM + "(left-skewed / negative tail)" + ANSI_RESET;
    }

    private String interpretKurtosis(double kurtosis) {
        // Note: This is excess kurtosis (normal = 0) or raw kurtosis (normal = 3)?
        // DimensionStatistics computes raw kurtosis where normal = 3
        if (Math.abs(kurtosis - 3) < 0.5) return ANSI_DIM + "(mesokurtic / normal-like tails)" + ANSI_RESET;
        if (kurtosis > 3) return ANSI_DIM + "(leptokurtic / heavy tails)" + ANSI_RESET;
        return ANSI_DIM + "(platykurtic / light tails)" + ANSI_RESET;
    }

    private double percentile(float[] sorted, double p) {
        if (sorted.length == 0) return 0;
        double index = (p / 100.0) * (sorted.length - 1);
        int lower = (int) Math.floor(index);
        int upper = (int) Math.ceil(index);
        if (lower == upper) return sorted[lower];
        double frac = index - lower;
        return sorted[lower] * (1 - frac) + sorted[upper] * frac;
    }

    private int countOutliers(float[] sorted, double lowerFence, double upperFence) {
        int count = 0;
        for (float v : sorted) {
            if (v < lowerFence || v > upperFence) count++;
        }
        return count;
    }

    private int[] readFvecHeader(Path path) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
            // Read first 4 bytes to get dimensions
            byte[] dimBytes = new byte[4];
            raf.read(dimBytes);
            int dimensions = ByteBuffer.wrap(dimBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();

            // Calculate vector count
            long fileSize = raf.length();
            int bytesPerVector = 4 + dimensions * 4; // 4 for dim header + floats
            int vectorCount = (int) (fileSize / bytesPerVector);

            return new int[]{dimensions, vectorCount};
        }
    }

    private float[] readDimensionValues(int dim, int dimensions, int vectorCount) throws IOException {
        float[] values = new float[vectorCount];
        int bytesPerVector = 4 + dimensions * 4;

        try (RandomAccessFile raf = new RandomAccessFile(inputPath.toFile(), "r");
             FileChannel channel = raf.getChannel()) {

            ByteBuffer buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);

            for (int v = 0; v < vectorCount; v++) {
                // Seek to the dimension value in this vector
                long offset = (long) v * bytesPerVector + 4 + (long) dim * 4;
                channel.position(offset);
                buffer.clear();
                channel.read(buffer);
                buffer.flip();
                values[v] = buffer.getFloat();
            }
        }

        return values;
    }
}
