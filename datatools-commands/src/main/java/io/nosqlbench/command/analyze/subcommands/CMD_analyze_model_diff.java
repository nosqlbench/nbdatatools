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

import io.nosqlbench.vshapes.model.NormalScalarModel;
import io.nosqlbench.vshapes.model.BetaScalarModel;
import io.nosqlbench.vshapes.model.UniformScalarModel;
import io.nosqlbench.vshapes.model.EmpiricalScalarModel;
import io.nosqlbench.vshapes.model.ScalarModel;
import io.nosqlbench.vshapes.model.VectorSpaceModel;
import io.nosqlbench.vshapes.model.VectorSpaceModelConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/// Compare two VectorSpaceModel JSON files and report differences.
///
/// This command loads two model files and compares them dimension by dimension,
/// reporting type matches and parameter drift. It is useful for verifying that
/// an extraction-generation round-trip preserves model parameters, or for
/// comparing models extracted from different datasets.
///
/// ## Usage
///
/// ```bash
/// nbvectors analyze model-diff --original model1.json --compare model2.json
/// ```
///
/// ## Exit Codes
///
/// - 0: Models match (100% type match, drift within thresholds)
/// - 1: Models differ (type mismatch or drift exceeds thresholds)
/// - 2: Error loading files
@CommandLine.Command(
    name = "model-diff",
    header = "Compare two VectorSpaceModel JSON files",
    description = "Compares two model files dimension by dimension, reporting type matches and parameter drift.",
    exitCodeList = {
        "0: Models match within thresholds",
        "1: Models differ (type mismatch or high drift)",
        "2: Error loading model files"
    }
)
public class CMD_analyze_model_diff implements Callable<Integer> {

    private static final Logger logger = LogManager.getLogger(CMD_analyze_model_diff.class);

    // ANSI color codes
    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_BOLD = "\u001B[1m";
    private static final String ANSI_DIM = "\u001B[2m";
    private static final String ANSI_RED = "\u001B[91m";
    private static final String ANSI_GREEN = "\u001B[92m";
    private static final String ANSI_YELLOW = "\u001B[93m";
    private static final String ANSI_BLUE = "\u001B[94m";
    private static final String ANSI_CYAN = "\u001B[96m";

    @CommandLine.Option(
        names = {"--original", "-o"},
        description = "Path to the original/reference model JSON file",
        required = true
    )
    private Path originalPath;

    @CommandLine.Option(
        names = {"--compare", "-c"},
        description = "Path to the model JSON file to compare against the original",
        required = true
    )
    private Path comparePath;

    @CommandLine.Option(
        names = {"--verbose", "-v"},
        description = "Show all dimensions (default: show first 10 + all problems)"
    )
    private boolean verbose = false;

    @CommandLine.Option(
        names = {"--drift-threshold"},
        description = "Maximum allowed average parameter drift percentage (default: 1.0)"
    )
    private double driftThreshold = 1.0;

    @CommandLine.Option(
        names = {"--max-drift-threshold"},
        description = "Maximum allowed single-dimension drift percentage (default: 2.0)"
    )
    private double maxDriftThreshold = 2.0;

    @CommandLine.Option(
        names = {"--type-match-threshold"},
        description = "Minimum required type match percentage (default: 100.0)"
    )
    private double typeMatchThreshold = 100.0;

    @Override
    public Integer call() {
        try {
            // Validate input files exist
            if (!Files.exists(originalPath)) {
                System.err.println("Error: Original model file not found: " + originalPath);
                return 2;
            }
            if (!Files.exists(comparePath)) {
                System.err.println("Error: Comparison model file not found: " + comparePath);
                return 2;
            }

            // Load models
            System.out.println("Loading models...");
            VectorSpaceModel original = VectorSpaceModelConfig.loadFromFile(originalPath);
            VectorSpaceModel compare = VectorSpaceModelConfig.loadFromFile(comparePath);

            int origDims = original.dimensions();
            int compDims = compare.dimensions();

            if (origDims != compDims) {
                System.err.printf("Error: Dimension mismatch - original has %d dimensions, compare has %d%n",
                    origDims, compDims);
                return 2;
            }

            int dimensions = origDims;

            // Print header
            printHeader(originalPath, comparePath, dimensions);

            // Compare dimension by dimension
            List<DimensionComparison> comparisons = new ArrayList<>();
            int typeMatches = 0;
            double totalDrift = 0;
            double maxDrift = 0;
            int maxDriftDim = 0;
            List<Integer> mismatchedDims = new ArrayList<>();

            for (int d = 0; d < dimensions; d++) {
                ScalarModel origModel = original.scalarModel(d);
                ScalarModel compModel = compare.scalarModel(d);

                boolean typeMatch = origModel.getModelType().equals(compModel.getModelType());
                double drift = calculateParameterDrift(origModel, compModel);

                if (typeMatch) {
                    typeMatches++;
                } else {
                    mismatchedDims.add(d);
                }

                totalDrift += drift;
                if (drift > maxDrift) {
                    maxDrift = drift;
                    maxDriftDim = d;
                }

                comparisons.add(new DimensionComparison(d, origModel, compModel, typeMatch, drift));
            }

            double avgDrift = totalDrift / dimensions;
            double typeMatchPct = (typeMatches * 100.0) / dimensions;

            // Print comparison table
            printComparisonTable(comparisons, dimensions);

            // Print summary
            printSummary(typeMatches, dimensions, typeMatchPct, avgDrift, maxDrift, maxDriftDim);

            // Determine pass/fail
            boolean passed = typeMatchPct >= typeMatchThreshold
                && avgDrift <= driftThreshold
                && maxDrift <= maxDriftThreshold;

            if (passed) {
                printPassedBox(typeMatches, dimensions, typeMatchPct, avgDrift, maxDrift);
                return 0;
            } else {
                printFailedBox(typeMatches, dimensions, typeMatchPct, avgDrift, maxDrift, mismatchedDims);
                return 1;
            }

        } catch (Exception e) {
            logger.error("Error comparing models", e);
            System.err.println("Error: " + e.getMessage());
            return 2;
        }
    }

    private void printHeader(Path original, Path compare, int dimensions) {
        System.out.println();
        System.out.println("═══════════════════════════════════════════════════════════════════════════════");
        System.out.println("                              MODEL COMPARISON                                 ");
        System.out.println("═══════════════════════════════════════════════════════════════════════════════");
        System.out.println();
        System.out.println("Models:");
        System.out.printf("  Original:   %s%n", original);
        System.out.printf("  Comparison: %s%n", compare);
        System.out.printf("  Dimensions: %d%n", dimensions);
        System.out.println();
    }

    private void printComparisonTable(List<DimensionComparison> comparisons, int dimensions) {
        System.out.println("Dimension-by-Dimension Comparison:");
        System.out.println("┌──────┬────────────┬────────────────────────────────┬────────────────────────────────┬────────┬────────┐");
        System.out.println("│ Dim  │ Type       │ Original Model                 │ Comparison Model               │ Drift  │ Status │");
        System.out.println("├──────┼────────────┼────────────────────────────────┼────────────────────────────────┼────────┼────────┤");

        int maxToShow = verbose ? dimensions : Math.min(20, dimensions);
        int shown = 0;

        for (DimensionComparison c : comparisons) {
            boolean isProblematic = !c.typeMatch || c.drift > 1.0;

            // In non-verbose mode, show first few + all problems
            if (!verbose && shown >= 10 && !isProblematic) {
                continue;
            }
            if (shown >= maxToShow && !isProblematic) {
                continue;
            }

            // Determine row color based on status
            String statusSymbol;
            if (c.typeMatch && c.drift < 1.0) {
                statusSymbol = ANSI_BLUE + "✓" + ANSI_RESET;
            } else if (c.drift < 2.0) {
                statusSymbol = ANSI_YELLOW + "⚠" + ANSI_RESET;
            } else {
                statusSymbol = ANSI_RED + "✗" + ANSI_RESET;
            }

            // Format type column
            String typeCol;
            String typeColor;
            if (c.typeMatch) {
                typeCol = c.original.getModelType();
                typeColor = ANSI_DIM;
            } else {
                typeCol = c.original.getModelType() + "→" + c.comparison.getModelType();
                typeColor = ANSI_RED + ANSI_BOLD;
            }

            String originalParams = formatModelParams(c.original);
            String comparisonParams = formatModelParams(c.comparison);

            // Color the params based on drift
            String originalColor = ANSI_DIM;
            String comparisonColor;
            if (c.typeMatch && c.drift < 0.5) {
                comparisonColor = ANSI_BLUE;
            } else if (c.drift < 2.0) {
                comparisonColor = ANSI_YELLOW;
            } else {
                comparisonColor = ANSI_RED;
            }

            // Format drift
            String driftStr;
            String driftColor;
            if (!c.typeMatch) {
                driftStr = "TYPE";
                driftColor = ANSI_RED + ANSI_BOLD;
            } else if (c.drift < 1.0) {
                driftStr = String.format("%.2f%%", c.drift);
                driftColor = ANSI_BLUE;
            } else if (c.drift < 2.0) {
                driftStr = String.format("%.2f%%", c.drift);
                driftColor = ANSI_YELLOW;
            } else {
                driftStr = String.format("%.2f%%", c.drift);
                driftColor = ANSI_RED;
            }

            System.out.printf("│ %4d │ %s%-10s%s │ %s%-30s%s │ %s%-30s%s │ %s%6s%s │   %s    │%n",
                c.dimension,
                typeColor, truncateString(typeCol, 10), ANSI_RESET,
                originalColor, truncateString(originalParams, 30), ANSI_RESET,
                comparisonColor, truncateString(comparisonParams, 30), ANSI_RESET,
                driftColor, driftStr, ANSI_RESET,
                statusSymbol);

            shown++;
        }

        int notShown = dimensions - shown;
        if (notShown > 0) {
            System.out.println("├──────┴────────────┴────────────────────────────────┴────────────────────────────────┴────────┴────────┤");
            System.out.printf("│ %s... %d more dimensions not shown (use --verbose to see all)%s                                           │%n",
                ANSI_DIM, notShown, ANSI_RESET);
        }

        System.out.println("└──────────────────────────────────────────────────────────────────────────────────────────────────────────┘");
        System.out.println();
    }

    private void printSummary(int typeMatches, int dimensions, double typeMatchPct,
                              double avgDrift, double maxDrift, int maxDriftDim) {
        System.out.println("───────────────────────────────────────────────────────────────────────────────");
        System.out.println("                              SUMMARY                                          ");
        System.out.println("───────────────────────────────────────────────────────────────────────────────");

        String typeColor = typeMatchPct >= typeMatchThreshold ? ANSI_BLUE : ANSI_YELLOW;
        String typeSymbol = typeMatchPct >= typeMatchThreshold ? "✓" : "⚠";
        System.out.printf("  %s%s%s Type matches:      %d/%d (%.1f%%)%n",
            typeColor, typeSymbol, ANSI_RESET, typeMatches, dimensions, typeMatchPct);

        String avgColor = avgDrift <= driftThreshold ? ANSI_BLUE : ANSI_YELLOW;
        String avgSymbol = avgDrift <= driftThreshold ? "✓" : "⚠";
        System.out.printf("  %s%s%s Parameter drift:   %s%.2f%%%s avg (threshold: %.1f%%)%n",
            avgColor, avgSymbol, ANSI_RESET, avgColor, avgDrift, ANSI_RESET, driftThreshold);

        String maxColor = maxDrift <= maxDriftThreshold ? ANSI_BLUE : ANSI_YELLOW;
        String maxSymbol = maxDrift <= maxDriftThreshold ? "✓" : "⚠";
        System.out.printf("  %s%s%s Max drift:         %s%.2f%%%s (dim %d, threshold: %.1f%%)%n",
            maxColor, maxSymbol, ANSI_RESET, maxColor, maxDrift, ANSI_RESET, maxDriftDim, maxDriftThreshold);

        System.out.println();
    }

    private void printPassedBox(int typeMatches, int dimensions, double typeMatchPct,
                                double avgDrift, double maxDrift) {
        System.out.println(ANSI_BLUE + "╔═══════════════════════════════════════════════════════════════════════════════╗" + ANSI_RESET);
        System.out.println(ANSI_BLUE + "║" + ANSI_BLUE + ANSI_BOLD + "                         ✓ MODELS MATCH                                       " + ANSI_RESET + ANSI_BLUE + "║" + ANSI_RESET);
        System.out.println(ANSI_BLUE + "║" + ANSI_RESET + "                                                                               " + ANSI_BLUE + "║" + ANSI_RESET);
        System.out.println(ANSI_BLUE + "║" + ANSI_RESET + String.format("  Type matches:      %d/%d (%.1f%%)                                           ", typeMatches, dimensions, typeMatchPct) + ANSI_BLUE + "║" + ANSI_RESET);
        System.out.println(ANSI_BLUE + "║" + ANSI_RESET + String.format("  Average drift:     %.2f%%                                                    ", avgDrift) + ANSI_BLUE + "║" + ANSI_RESET);
        System.out.println(ANSI_BLUE + "║" + ANSI_RESET + String.format("  Max drift:         %.2f%%                                                    ", maxDrift) + ANSI_BLUE + "║" + ANSI_RESET);
        System.out.println(ANSI_BLUE + "║" + ANSI_RESET + "                                                                               " + ANSI_BLUE + "║" + ANSI_RESET);
        System.out.println(ANSI_BLUE + "║" + ANSI_RESET + "  The models are equivalent within the specified thresholds.                  " + ANSI_BLUE + "║" + ANSI_RESET);
        System.out.println(ANSI_BLUE + "╚═══════════════════════════════════════════════════════════════════════════════╝" + ANSI_RESET);
    }

    private void printFailedBox(int typeMatches, int dimensions, double typeMatchPct,
                                double avgDrift, double maxDrift, List<Integer> mismatchedDims) {
        System.out.println(ANSI_YELLOW + "╔═══════════════════════════════════════════════════════════════════════════════╗" + ANSI_RESET);
        System.out.println(ANSI_YELLOW + "║" + ANSI_YELLOW + ANSI_BOLD + "                         ⚠ MODELS DIFFER                                      " + ANSI_RESET + ANSI_YELLOW + "║" + ANSI_RESET);
        System.out.println(ANSI_YELLOW + "╠═══════════════════════════════════════════════════════════════════════════════╣" + ANSI_RESET);
        System.out.println(ANSI_YELLOW + "║" + ANSI_RESET + String.format("  Type matches:      %d/%d (%.1f%%)                                           ", typeMatches, dimensions, typeMatchPct) + ANSI_YELLOW + "║" + ANSI_RESET);
        System.out.println(ANSI_YELLOW + "║" + ANSI_RESET + String.format("  Average drift:     %.2f%% (threshold: %.1f%%)                                ", avgDrift, driftThreshold) + ANSI_YELLOW + "║" + ANSI_RESET);
        System.out.println(ANSI_YELLOW + "║" + ANSI_RESET + String.format("  Max drift:         %.2f%% (threshold: %.1f%%)                                ", maxDrift, maxDriftThreshold) + ANSI_YELLOW + "║" + ANSI_RESET);

        if (!mismatchedDims.isEmpty()) {
            String dims = mismatchedDims.size() <= 8
                ? mismatchedDims.toString()
                : mismatchedDims.subList(0, 8) + "...";
            System.out.println(ANSI_YELLOW + "║" + ANSI_RESET + "                                                                               " + ANSI_YELLOW + "║" + ANSI_RESET);
            System.out.println(ANSI_YELLOW + "║" + ANSI_RESET + "  Mismatched dimensions: " + ANSI_RED + dims + ANSI_RESET + "                                        " + ANSI_YELLOW + "║" + ANSI_RESET);
        }

        System.out.println(ANSI_YELLOW + "╚═══════════════════════════════════════════════════════════════════════════════╝" + ANSI_RESET);
    }

    private String formatModelParams(ScalarModel model) {
        if (model instanceof NormalScalarModel normal) {
            if (normal.isTruncated()) {
                return String.format("μ=%.3f, σ=%.3f [%.2f,%.2f]",
                    normal.getMean(), normal.getStdDev(), normal.lower(), normal.upper());
            }
            return String.format("μ=%.4f, σ=%.4f", normal.getMean(), normal.getStdDev());
        }

        if (model instanceof BetaScalarModel beta) {
            return String.format("α=%.3f, β=%.3f [%.2f,%.2f]",
                beta.getAlpha(), beta.getBeta(), beta.getLower(), beta.getUpper());
        }

        if (model instanceof UniformScalarModel uniform) {
            return String.format("[%.4f, %.4f]", uniform.getLower(), uniform.getUpper());
        }

        if (model instanceof EmpiricalScalarModel) {
            return "empirical (histogram)";
        }

        return model.getModelType();
    }

    private double calculateParameterDrift(ScalarModel orig, ScalarModel compare) {
        if (orig instanceof NormalScalarModel origNormal && compare instanceof NormalScalarModel compNormal) {
            double meanDrift = relativeDrift(origNormal.getMean(), compNormal.getMean(), origNormal.getStdDev());
            double stdDevDrift = relativeDrift(origNormal.getStdDev(), compNormal.getStdDev(), origNormal.getStdDev());
            return (meanDrift + stdDevDrift) * 50;
        }

        if (orig instanceof BetaScalarModel origBeta && compare instanceof BetaScalarModel compBeta) {
            double alphaDrift = relativeDrift(origBeta.getAlpha(), compBeta.getAlpha(), origBeta.getAlpha());
            double betaDrift = relativeDrift(origBeta.getBeta(), compBeta.getBeta(), origBeta.getBeta());
            return (alphaDrift + betaDrift) * 50;
        }

        if (orig instanceof UniformScalarModel origUniform && compare instanceof UniformScalarModel compUniform) {
            double range = origUniform.getUpper() - origUniform.getLower();
            double lowerDrift = range > 0 ? Math.abs(origUniform.getLower() - compUniform.getLower()) / range : 0;
            double upperDrift = range > 0 ? Math.abs(origUniform.getUpper() - compUniform.getUpper()) / range : 0;
            return (lowerDrift + upperDrift) * 50;
        }

        // For empirical or mismatched types
        return 0.5;
    }

    private double relativeDrift(double orig, double compare, double scale) {
        if (scale == 0 || Math.abs(scale) < 1e-10) {
            return Math.abs(orig - compare) < 1e-10 ? 0 : 1.0;
        }
        return Math.abs(orig - compare) / Math.abs(scale);
    }

    private String truncateString(String str, int maxLen) {
        if (str.length() <= maxLen) {
            return str;
        }
        return str.substring(0, maxLen - 2) + "..";
    }

    private record DimensionComparison(
        int dimension,
        ScalarModel original,
        ScalarModel comparison,
        boolean typeMatch,
        double drift
    ) {}
}
