package io.nosqlbench.vshapes.extract;

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

import io.nosqlbench.vshapes.extract.ComponentModelFitter.FitResult;

import java.util.*;

/**
 * Collects and reports fit quality metrics for all Pearson distribution types
 * across all dimensions of a vector dataset.
 *
 * <h2>Purpose</h2>
 *
 * <p>This class runs all available model fitters against each dimension and
 * collects the goodness-of-fit scores, enabling comparison of how well different
 * distribution types match each dimension's data.
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * DimensionFitReport report = DimensionFitReport.compute(transposedData);
 * System.out.println(report.formatTable());
 * }</pre>
 */
public final class DimensionFitReport {

    private final int numDimensions;
    private final List<String> modelTypes;
    private final double[][] fitScores;  // [dimension][modelType]
    private final int[] bestFitIndexPerDimension;
    private final String[] sparklines;  // Unicode sparkline histograms per dimension (may be null)

    private DimensionFitReport(int numDimensions, List<String> modelTypes,
                                double[][] fitScores, int[] bestFitIndexPerDimension,
                                String[] sparklines) {
        this.numDimensions = numDimensions;
        this.modelTypes = modelTypes;
        this.fitScores = fitScores;
        this.bestFitIndexPerDimension = bestFitIndexPerDimension;
        this.sparklines = sparklines;
    }

    /**
     * Creates a fit report from pre-computed all-fits data.
     *
     * <p>This is the preferred method when fit data was already computed during
     * model extraction (e.g., via {@code extractor.withAllFitsCollection()}).
     * It avoids redundant recomputation of all fits.
     *
     * @param allFitsData the pre-computed fit data from extraction
     * @return the fit report
     * @throws IllegalArgumentException if allFitsData is null
     */
    public static DimensionFitReport fromAllFitsData(ModelExtractor.AllFitsData allFitsData) {
        if (allFitsData == null) {
            throw new IllegalArgumentException("allFitsData cannot be null");
        }
        return new DimensionFitReport(
            allFitsData.numDimensions(),
            allFitsData.modelTypes(),
            allFitsData.fitScores(),
            allFitsData.bestFitIndices(),
            allFitsData.sparklines()
        );
    }

    /**
     * Computes fit report for all dimensions using the full Pearson selector.
     *
     * <p><strong>Note:</strong> If fit data was already computed during extraction,
     * prefer using {@link #fromAllFitsData(ModelExtractor.AllFitsData)} to avoid
     * redundant recomputation.
     *
     * @param transposedData data transposed to [dimension][vector] format
     * @return the computed fit report
     */
    public static DimensionFitReport compute(float[][] transposedData) {
        return compute(transposedData, BestFitSelector.fullPearsonSelector());
    }

    /**
     * Computes fit report for all dimensions using a specific selector.
     *
     * @param transposedData data transposed to [dimension][vector] format
     * @param selector the selector containing fitters to evaluate
     * @return the computed fit report
     */
    public static DimensionFitReport compute(float[][] transposedData, BestFitSelector selector) {
        int numDimensions = transposedData.length;
        List<ComponentModelFitter> fitters = selector.getFitters();
        List<String> modelTypes = fitters.stream()
            .map(ComponentModelFitter::getModelType)
            .toList();

        double[][] fitScores = new double[numDimensions][modelTypes.size()];
        int[] bestFitIndexPerDimension = new int[numDimensions];
        String[] sparklines = new String[numDimensions];

        for (int d = 0; d < numDimensions; d++) {
            float[] dimData = transposedData[d];
            DimensionStatistics stats = DimensionStatistics.compute(d, dimData);

            // Generate sparkline histogram
            sparklines[d] = Sparkline.generate(dimData, Sparkline.DEFAULT_WIDTH);

            double bestScore = Double.MAX_VALUE;
            int bestIndex = -1;

            for (int f = 0; f < fitters.size(); f++) {
                try {
                    FitResult result = fitters.get(f).fit(stats, dimData);
                    fitScores[d][f] = result.goodnessOfFit();

                    if (result.goodnessOfFit() < bestScore) {
                        bestScore = result.goodnessOfFit();
                        bestIndex = f;
                    }
                } catch (Exception e) {
                    fitScores[d][f] = Double.NaN;
                }
            }

            bestFitIndexPerDimension[d] = bestIndex;
        }

        return new DimensionFitReport(numDimensions, modelTypes, fitScores, bestFitIndexPerDimension, sparklines);
    }

    /**
     * Returns the number of dimensions.
     */
    public int numDimensions() {
        return numDimensions;
    }

    /**
     * Returns the list of model types evaluated.
     */
    public List<String> modelTypes() {
        return modelTypes;
    }

    /**
     * Gets the fit score for a specific dimension and model type.
     *
     * @param dimension the dimension index
     * @param modelTypeIndex the model type index
     * @return the goodness-of-fit score (lower is better)
     */
    public double getFitScore(int dimension, int modelTypeIndex) {
        return fitScores[dimension][modelTypeIndex];
    }

    /**
     * Gets the best fit model type for a dimension.
     *
     * @param dimension the dimension index
     * @return the model type with the best (lowest) fit score
     */
    public String getBestFit(int dimension) {
        int idx = bestFitIndexPerDimension[dimension];
        return (idx >= 0 && idx < modelTypes.size()) ? modelTypes.get(idx) : "unknown";
    }

    /**
     * Gets the index of the best fit model for a dimension.
     *
     * @param dimension the dimension index
     * @return the index into modelTypes() of the best fit, or -1 if none
     */
    public int getBestFitIndex(int dimension) {
        return bestFitIndexPerDimension[dimension];
    }

    /**
     * Formats the fit report as a table.
     *
     * <p>Format:
     * <pre>
     * Dim | normal | beta   | gamma  | student_t | uniform | Best
     * ----+--------+--------+--------+-----------+---------+------
     *   0 |  0.123 |  0.456*|  0.789 |     0.234 |   0.567 | beta
     *   1 |  0.234 |  0.123*|  0.890 |     0.345 |   0.678 | beta
     * </pre>
     *
     * @return formatted table string
     */
    public String formatTable() {
        return formatTable(numDimensions);
    }

    /**
     * Formats the fit report as a table, limiting to maxDimensions rows.
     *
     * @param maxDimensions maximum number of dimensions to display
     * @return formatted table string
     */
    public String formatTable(int maxDimensions) {
        int dimsToShow = Math.min(numDimensions, maxDimensions);

        boolean hasSparklines = sparklines != null && sparklines.length > 0;
        FitTableFormatter formatter = new FitTableFormatter(modelTypes, hasSparklines);
        for (int d = 0; d < dimsToShow; d++) {
            String sparkline = hasSparklines && d < sparklines.length ? sparklines[d] : null;
            formatter.addRow(d, fitScores[d], bestFitIndexPerDimension[d], sparkline);
        }

        StringBuilder sb = new StringBuilder();
        sb.append(formatter.format());

        if (dimsToShow < numDimensions) {
            sb.append("... (").append(numDimensions - dimsToShow).append(" more dimensions)\n");
        }

        sb.append("\n");
        sb.append(formatSummary());

        return sb.toString();
    }

    /**
     * Formats a summary of best fits by type.
     */
    public String formatSummary() {
        Map<String, Integer> countsByType = new LinkedHashMap<>();
        for (String type : modelTypes) {
            countsByType.put(type, 0);
        }

        for (int d = 0; d < numDimensions; d++) {
            String best = getBestFit(d);
            countsByType.merge(best, 1, Integer::sum);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Best Fit Distribution Summary:\n");
        for (Map.Entry<String, Integer> entry : countsByType.entrySet()) {
            if (entry.getValue() > 0) {
                double pct = 100.0 * entry.getValue() / numDimensions;
                sb.append(String.format("  %-14s: %4d dimensions (%5.1f%%)\n",
                    entry.getKey(), entry.getValue(), pct));
            }
        }

        return sb.toString();
    }
}
