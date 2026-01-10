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

import io.nosqlbench.vshapes.extract.StatisticalTestSuite;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/// Compare two vector files statistically using two-sample Kolmogorov-Smirnov tests.
///
/// This command loads two vector files and performs a two-sample K-S test for each
/// dimension, determining if the distributions are statistically different.
/// This is useful for verifying that synthetic data matches original data.
///
/// ## Usage
///
/// ```bash
/// nbvectors analyze compare --original original.fvec --synthetic synthetic.fvec
/// ```
///
/// ## Exit Codes
///
/// - 0: All dimensions pass (distributions match)
/// - 1: Some dimensions fail (distributions differ significantly)
/// - 2: Error loading files
@CommandLine.Command(
    name = "compare",
    header = "Compare two vector files statistically",
    description = "Performs two-sample Kolmogorov-Smirnov tests to compare distributions between two vector files.",
    exitCodeList = {
        "0: All dimensions pass (distributions match)",
        "1: Some dimensions fail (significant differences)",
        "2: Error loading files"
    }
)
public class CMD_analyze_compare implements Callable<Integer> {

    private static final Logger logger = LogManager.getLogger(CMD_analyze_compare.class);

    // ANSI color codes
    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_BOLD = "\u001B[1m";
    private static final String ANSI_DIM = "\u001B[2m";
    private static final String ANSI_RED = "\u001B[91m";
    private static final String ANSI_GREEN = "\u001B[92m";
    private static final String ANSI_YELLOW = "\u001B[93m";
    private static final String ANSI_BLUE = "\u001B[94m";

    @CommandLine.Option(
        names = {"--original", "-o"},
        description = "Path to the original/reference vector file",
        required = true
    )
    private Path originalPath;

    @CommandLine.Option(
        names = {"--synthetic", "-s"},
        description = "Path to the synthetic/comparison vector file",
        required = true
    )
    private Path syntheticPath;

    @CommandLine.Option(
        names = {"--alpha", "-a"},
        description = "Significance level for K-S test (default: 0.05)"
    )
    private double alpha = 0.05;

    @CommandLine.Option(
        names = {"--sample"},
        description = "Maximum number of vectors to sample from each file (default: 10000)"
    )
    private int sampleSize = 10000;

    @CommandLine.Option(
        names = {"--verbose", "-v"},
        description = "Show all dimensions (default: show first 10 + failures)"
    )
    private boolean verbose = false;

    @CommandLine.Option(
        names = {"--dimension", "-d"},
        description = "Compare only this specific dimension (default: all)"
    )
    private Integer specificDimension;

    @Override
    public Integer call() {
        try {
            // Validate input files
            if (!Files.exists(originalPath)) {
                System.err.println("Error: Original file not found: " + originalPath);
                return 2;
            }
            if (!Files.exists(syntheticPath)) {
                System.err.println("Error: Synthetic file not found: " + syntheticPath);
                return 2;
            }

            // Read file headers
            int[] origInfo = readFvecHeader(originalPath);
            int[] synthInfo = readFvecHeader(syntheticPath);

            int origDims = origInfo[0];
            int origVectors = origInfo[1];
            int synthDims = synthInfo[0];
            int synthVectors = synthInfo[1];

            if (origDims != synthDims) {
                System.err.printf("Error: Dimension mismatch - original has %d, synthetic has %d%n",
                    origDims, synthDims);
                return 2;
            }

            int dimensions = origDims;
            int origSamples = Math.min(sampleSize, origVectors);
            int synthSamples = Math.min(sampleSize, synthVectors);

            // Print header
            printHeader(origVectors, synthVectors, origSamples, synthSamples, dimensions);

            // Perform K-S tests
            List<DimensionResult> results = new ArrayList<>();
            int passed = 0;
            int failed = 0;

            int startDim = specificDimension != null ? specificDimension : 0;
            int endDim = specificDimension != null ? specificDimension + 1 : dimensions;

            for (int d = startDim; d < endDim; d++) {
                float[] origValues = readDimensionValues(originalPath, d, origDims, origSamples);
                float[] synthValues = readDimensionValues(syntheticPath, d, synthDims, synthSamples);

                StatisticalTestSuite.TestResult ksResult =
                    StatisticalTestSuite.kolmogorovSmirnovTest(origValues, synthValues, alpha);

                double dStatistic = ksResult.statistic();
                // Compute approximate p-value using Kolmogorov distribution approximation
                double pValue = computeApproximatePValue(dStatistic, origValues.length, synthValues.length);

                boolean pass = ksResult.passed();
                if (pass) {
                    passed++;
                } else {
                    failed++;
                }

                results.add(new DimensionResult(d, dStatistic, pValue, pass));

                // Show progress for large dimension counts
                if (dimensions > 50 && (d - startDim + 1) % 50 == 0) {
                    System.err.printf("\r  Testing: %d/%d dimensions...", d - startDim + 1, endDim - startDim);
                }
            }

            if (dimensions > 50) {
                System.err.printf("\r  Testing: %d/%d dimensions... done.%n", endDim - startDim, endDim - startDim);
            }

            // Print results table
            printResultsTable(results, endDim - startDim);

            // Print summary
            double passRate = (passed * 100.0) / results.size();
            printSummary(passed, failed, passRate);

            return failed == 0 ? 0 : 1;

        } catch (Exception e) {
            logger.error("Error comparing files", e);
            System.err.println("Error: " + e.getMessage());
            return 2;
        }
    }

    private void printHeader(int origVectors, int synthVectors, int origSamples, int synthSamples, int dimensions) {
        System.out.println();
        System.out.println("═══════════════════════════════════════════════════════════════════════════════");
        System.out.println("                          DISTRIBUTION COMPARISON                              ");
        System.out.println("═══════════════════════════════════════════════════════════════════════════════");
        System.out.println();
        System.out.println("Datasets:");
        System.out.printf("  Original:  %s (%,d vectors, sampling %,d)%n", originalPath.getFileName(), origVectors, origSamples);
        System.out.printf("  Synthetic: %s (%,d vectors, sampling %,d)%n", syntheticPath.getFileName(), synthVectors, synthSamples);
        System.out.printf("  Dimensions: %d%n", dimensions);
        System.out.printf("  Significance level (α): %.2f%n", alpha);
        System.out.println();
    }

    private void printResultsTable(List<DimensionResult> results, int totalDims) {
        System.out.println("Two-Sample Kolmogorov-Smirnov Tests:");
        System.out.println("┌───────┬───────────┬───────────┬────────────┐");
        System.out.println("│  Dim  │   K-S D   │  p-value  │   Status   │");
        System.out.println("├───────┼───────────┼───────────┼────────────┤");

        int maxToShow = verbose ? totalDims : 20;
        int shown = 0;
        int failedCount = 0;

        // Count failures
        for (DimensionResult r : results) {
            if (!r.passed) failedCount++;
        }

        for (DimensionResult r : results) {
            // In non-verbose mode, show first 10 + all failures
            if (!verbose && shown >= 10 && r.passed) {
                continue;
            }
            if (shown >= maxToShow && r.passed) {
                continue;
            }

            String statusStr;
            String statusColor;
            if (r.passed) {
                statusStr = "✓ PASS";
                statusColor = ANSI_BLUE;
            } else {
                statusStr = "✗ FAIL";
                statusColor = ANSI_RED;
            }

            String dColor = r.dStatistic < 0.02 ? ANSI_BLUE : (r.dStatistic < 0.05 ? ANSI_YELLOW : ANSI_RED);
            String pColor = r.pValue > 0.1 ? ANSI_BLUE : (r.pValue > alpha ? ANSI_YELLOW : ANSI_RED);

            System.out.printf("│ %5d │ %s%9.4f%s │ %s%9.4f%s │ %s%10s%s │%n",
                r.dimension,
                dColor, r.dStatistic, ANSI_RESET,
                pColor, r.pValue, ANSI_RESET,
                statusColor, statusStr, ANSI_RESET);

            shown++;
        }

        int notShown = totalDims - shown;
        if (notShown > 0) {
            System.out.println("├───────┴───────────┴───────────┴────────────┤");
            System.out.printf("│ %s... %d more dimensions not shown%s            │%n",
                ANSI_DIM, notShown, ANSI_RESET);
        }

        System.out.println("└──────────────────────────────────────────────┘");
        System.out.println();
    }

    private void printSummary(int passed, int failed, double passRate) {
        String color = failed == 0 ? ANSI_BLUE : (passRate >= 95 ? ANSI_YELLOW : ANSI_RED);
        String symbol = failed == 0 ? "✓" : "⚠";
        String status = failed == 0 ? "DISTRIBUTIONS MATCH" : "DISTRIBUTIONS DIFFER";

        System.out.printf("Dimensions Passing: %s%d/%d (%.1f%%)%s%n",
            color, passed, passed + failed, passRate, ANSI_RESET);
        System.out.println();

        if (failed == 0) {
            System.out.println(ANSI_BLUE + "╔═══════════════════════════════════════════════════════════════════════════════╗" + ANSI_RESET);
            System.out.println(ANSI_BLUE + "║" + ANSI_BLUE + ANSI_BOLD + "                      ✓ DISTRIBUTIONS MATCH                                   " + ANSI_RESET + ANSI_BLUE + "║" + ANSI_RESET);
            System.out.println(ANSI_BLUE + "║" + ANSI_RESET + "                                                                               " + ANSI_BLUE + "║" + ANSI_RESET);
            System.out.println(ANSI_BLUE + "║" + ANSI_RESET + "  All dimensions pass the Kolmogorov-Smirnov test at α = " + String.format("%.2f", alpha) + "              " + ANSI_BLUE + "║" + ANSI_RESET);
            System.out.println(ANSI_BLUE + "║" + ANSI_RESET + "  The synthetic data statistically matches the original data.                 " + ANSI_BLUE + "║" + ANSI_RESET);
            System.out.println(ANSI_BLUE + "╚═══════════════════════════════════════════════════════════════════════════════╝" + ANSI_RESET);
        } else {
            System.out.println(ANSI_YELLOW + "╔═══════════════════════════════════════════════════════════════════════════════╗" + ANSI_RESET);
            System.out.println(ANSI_YELLOW + "║" + ANSI_YELLOW + ANSI_BOLD + "                      ⚠ DISTRIBUTIONS DIFFER                                   " + ANSI_RESET + ANSI_YELLOW + "║" + ANSI_RESET);
            System.out.println(ANSI_YELLOW + "╠═══════════════════════════════════════════════════════════════════════════════╣" + ANSI_RESET);
            System.out.println(ANSI_YELLOW + "║" + ANSI_RESET + String.format("  Failed dimensions: %d (%.1f%% pass rate)                                     ", failed, passRate) + ANSI_YELLOW + "║" + ANSI_RESET);
            System.out.println(ANSI_YELLOW + "║" + ANSI_RESET + "                                                                               " + ANSI_YELLOW + "║" + ANSI_RESET);
            System.out.println(ANSI_YELLOW + "║" + ANSI_RESET + "  The K-S test detected significant differences in some dimensions.           " + ANSI_YELLOW + "║" + ANSI_RESET);
            System.out.println(ANSI_YELLOW + "║" + ANSI_RESET + "  This may indicate the model doesn't capture all distribution features.      " + ANSI_YELLOW + "║" + ANSI_RESET);
            System.out.println(ANSI_YELLOW + "╚═══════════════════════════════════════════════════════════════════════════════╝" + ANSI_RESET);
        }
    }

    private int[] readFvecHeader(Path path) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
            byte[] dimBytes = new byte[4];
            raf.read(dimBytes);
            int dimensions = ByteBuffer.wrap(dimBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();

            long fileSize = raf.length();
            int bytesPerVector = 4 + dimensions * 4;
            int vectorCount = (int) (fileSize / bytesPerVector);

            return new int[]{dimensions, vectorCount};
        }
    }

    private float[] readDimensionValues(Path path, int dim, int dimensions, int vectorCount) throws IOException {
        float[] values = new float[vectorCount];
        int bytesPerVector = 4 + dimensions * 4;

        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r");
             FileChannel channel = raf.getChannel()) {

            ByteBuffer buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);

            for (int v = 0; v < vectorCount; v++) {
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

    /// Computes an approximate p-value for a two-sample K-S test using the
    /// asymptotic Kolmogorov distribution.
    ///
    /// @param d the K-S D statistic
    /// @param n1 sample size of first distribution
    /// @param n2 sample size of second distribution
    /// @return approximate p-value
    private double computeApproximatePValue(double d, int n1, int n2) {
        // Effective sample size for two-sample test
        double n = (double) n1 * n2 / (n1 + n2);
        // Scaled statistic
        double z = d * Math.sqrt(n);

        // Kolmogorov distribution approximation (first few terms of the series)
        // P(D > d) ≈ 2 * sum_{k=1}^{inf} (-1)^{k-1} * exp(-2 * k^2 * z^2)
        if (z < 0.27) {
            return 1.0; // Very small D, distributions are identical
        }
        if (z > 3.1) {
            return 0.0; // Very large D, distributions are very different
        }

        double sum = 0.0;
        for (int k = 1; k <= 100; k++) {
            double term = Math.exp(-2.0 * k * k * z * z);
            if (k % 2 == 1) {
                sum += term;
            } else {
                sum -= term;
            }
            if (term < 1e-10) break;
        }
        return Math.max(0.0, Math.min(1.0, 2.0 * sum));
    }

    private record DimensionResult(int dimension, double dStatistic, double pValue, boolean passed) {}
}
