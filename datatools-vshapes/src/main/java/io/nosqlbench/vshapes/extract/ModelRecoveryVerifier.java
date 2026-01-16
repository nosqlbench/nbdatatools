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

import io.nosqlbench.vshapes.model.*;

import java.util.*;

/**
 * Verifies model recovery by comparing source and fitted models.
 *
 * <h2>Purpose</h2>
 *
 * <p>This class enables rigorous round-trip testing of model fitting by:
 * <ol>
 *   <li>Comparing source and fitted models in canonical form</li>
 *   <li>Accounting for mode merging in composite models</li>
 *   <li>Computing parameter recovery accuracy</li>
 *   <li>Tracking matched, merged, and unrecovered components</li>
 * </ol>
 *
 * <h2>Mode Merging</h2>
 *
 * <p>When source composite models have closely-spaced modes, the fitting
 * process may merge them into fewer fitted modes. This verifier tracks
 * such merging and computes effective recovery statistics.
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * CompositeScalarModel source = ...;  // Known ground truth
 * CompositeScalarModel fitted = ...;  // Result of fitting
 *
 * ModelRecoveryVerifier verifier = new ModelRecoveryVerifier();
 * RecoveryResult result = verifier.verify(source, fitted);
 *
 * System.out.println("Recovery rate: " + result.getRecoveryRate());
 * System.out.println("Weight error: " + result.getMaxWeightError());
 * }</pre>
 *
 * @see CompositeScalarModel#toCanonicalForm()
 * @see ScalarModelComparator
 */
public final class ModelRecoveryVerifier {

    /** Default location tolerance for matching modes (units of stdDev). */
    private static final double DEFAULT_LOCATION_TOLERANCE = 1.5;

    /** Default relative weight tolerance for matched modes. */
    private static final double DEFAULT_WEIGHT_TOLERANCE = 0.25;

    private final double locationTolerance;
    private final double weightTolerance;

    /**
     * Creates a verifier with default tolerances.
     */
    public ModelRecoveryVerifier() {
        this(DEFAULT_LOCATION_TOLERANCE, DEFAULT_WEIGHT_TOLERANCE);
    }

    /**
     * Creates a verifier with specified tolerances.
     *
     * @param locationTolerance maximum distance (in stdDev units) for mode matching
     * @param weightTolerance maximum relative weight difference for matched modes
     */
    public ModelRecoveryVerifier(double locationTolerance, double weightTolerance) {
        this.locationTolerance = locationTolerance;
        this.weightTolerance = weightTolerance;
    }

    /**
     * Verifies recovery of a composite model.
     *
     * <p>Both models are converted to canonical form (sorted by location)
     * before comparison. The verification tracks:
     * <ul>
     *   <li>Matched components (source mode recovered in fitted)</li>
     *   <li>Merged components (multiple source modes → single fitted mode)</li>
     *   <li>Unrecovered components (source mode not found in fitted)</li>
     *   <li>Spurious components (fitted mode not in source)</li>
     * </ul>
     *
     * @param source the source (ground truth) model
     * @param fitted the fitted (recovered) model
     * @return verification result with detailed statistics
     */
    public RecoveryResult verify(CompositeScalarModel source, CompositeScalarModel fitted) {
        // Convert both to canonical form
        CompositeScalarModel canonicalSource = (CompositeScalarModel) source.toCanonicalForm();
        CompositeScalarModel canonicalFitted = (CompositeScalarModel) fitted.toCanonicalForm();

        ScalarModel[] sourceComponents = canonicalSource.getScalarModels();
        double[] sourceWeights = normalizeWeights(canonicalSource.getWeights());

        ScalarModel[] fittedComponents = canonicalFitted.getScalarModels();
        double[] fittedWeights = normalizeWeights(canonicalFitted.getWeights());

        // Track assignments: sourceIdx -> List<fittedIdx>
        Map<Integer, List<Integer>> sourceToFitted = new HashMap<>();
        Map<Integer, List<Integer>> fittedToSource = new HashMap<>();
        Set<Integer> unmatchedSource = new HashSet<>();
        Set<Integer> unmatchedFitted = new HashSet<>();

        // Initialize tracking
        for (int i = 0; i < sourceComponents.length; i++) {
            sourceToFitted.put(i, new ArrayList<>());
            unmatchedSource.add(i);
        }
        for (int i = 0; i < fittedComponents.length; i++) {
            fittedToSource.put(i, new ArrayList<>());
            unmatchedFitted.add(i);
        }

        // Match components by location
        for (int s = 0; s < sourceComponents.length; s++) {
            double sourceLoc = ScalarModelComparator.getCharacteristicLocation(sourceComponents[s]);
            double sourceWidth = getCharacteristicWidth(sourceComponents[s]);

            for (int f = 0; f < fittedComponents.length; f++) {
                double fittedLoc = ScalarModelComparator.getCharacteristicLocation(fittedComponents[f]);
                double fittedWidth = getCharacteristicWidth(fittedComponents[f]);

                // Use average width for tolerance calculation
                double avgWidth = (sourceWidth + fittedWidth) / 2;
                double tolerance = avgWidth * locationTolerance;

                if (Math.abs(sourceLoc - fittedLoc) <= tolerance) {
                    sourceToFitted.get(s).add(f);
                    fittedToSource.get(f).add(s);
                    unmatchedSource.remove(s);
                    unmatchedFitted.remove(f);
                }
            }
        }

        // Compute statistics
        int exactMatches = 0;  // 1:1 source:fitted
        int mergedModes = 0;   // N:1 source:fitted
        List<Double> weightErrors = new ArrayList<>();
        List<ComponentMatch> matches = new ArrayList<>();

        for (int s = 0; s < sourceComponents.length; s++) {
            List<Integer> fittedIndices = sourceToFitted.get(s);
            if (fittedIndices.isEmpty()) {
                continue;
            }

            // Check if this is a 1:1 match or part of a merge
            if (fittedIndices.size() == 1) {
                int f = fittedIndices.get(0);
                List<Integer> sourcesForThis = fittedToSource.get(f);
                if (sourcesForThis.size() == 1) {
                    // Exact 1:1 match
                    exactMatches++;
                    double weightError = Math.abs(sourceWeights[s] - fittedWeights[f]);
                    weightErrors.add(weightError);
                    matches.add(new ComponentMatch(s, f, weightError,
                        sourceComponents[s].getModelType().equals(fittedComponents[f].getModelType())));
                } else {
                    // This source is part of a merge (multiple sources → single fitted)
                    mergedModes++;
                }
            }
        }

        // Count spurious fitted modes (no matching source)
        int spuriousModes = unmatchedFitted.size();

        // Compute aggregate statistics
        double avgWeightError = weightErrors.isEmpty() ? 0 :
            weightErrors.stream().mapToDouble(d -> d).average().orElse(0);
        double maxWeightError = weightErrors.isEmpty() ? 0 :
            weightErrors.stream().mapToDouble(d -> d).max().orElse(0);

        // Recovery rate: exact matches / source components
        double recoveryRate = (double) exactMatches / sourceComponents.length;

        // Effective recovery: (exact + partial credit for merges) / source
        double effectiveRecovery = (exactMatches + 0.5 * mergedModes) / sourceComponents.length;

        return new RecoveryResult(
            sourceComponents.length,
            fittedComponents.length,
            exactMatches,
            mergedModes,
            unmatchedSource.size(),
            spuriousModes,
            recoveryRate,
            effectiveRecovery,
            avgWeightError,
            maxWeightError,
            matches
        );
    }

    /**
     * Verifies recovery of a simple (non-composite) scalar model.
     *
     * @param source the source model
     * @param fitted the fitted model
     * @return verification result
     */
    public SimpleRecoveryResult verifySimple(ScalarModel source, ScalarModel fitted) {
        boolean typeMatch = source.getModelType().equals(fitted.getModelType());

        double sourceLoc = ScalarModelComparator.getCharacteristicLocation(source);
        double fittedLoc = ScalarModelComparator.getCharacteristicLocation(fitted);
        double locationError = Math.abs(sourceLoc - fittedLoc);

        double sourceWidth = getCharacteristicWidth(source);
        double fittedWidth = getCharacteristicWidth(fitted);
        double widthError = Math.abs(sourceWidth - fittedWidth) / Math.max(sourceWidth, 0.001);

        return new SimpleRecoveryResult(typeMatch, locationError, widthError);
    }

    private double[] normalizeWeights(double[] weights) {
        double sum = 0;
        for (double w : weights) sum += w;
        if (sum == 0) return weights.clone();

        double[] normalized = new double[weights.length];
        for (int i = 0; i < weights.length; i++) {
            normalized[i] = weights[i] / sum;
        }
        return normalized;
    }

    private double getCharacteristicWidth(ScalarModel model) {
        if (model instanceof NormalScalarModel normal) {
            return normal.getStdDev();
        } else if (model instanceof UniformScalarModel uniform) {
            return (uniform.getUpper() - uniform.getLower()) / Math.sqrt(12);  // StdDev of uniform
        } else if (model instanceof BetaScalarModel beta) {
            // Approximate stdDev of Beta distribution
            double a = beta.getAlpha();
            double b = beta.getBeta();
            double var01 = a * b / ((a + b) * (a + b) * (a + b + 1));
            double range = beta.getUpper() - beta.getLower();
            return range * Math.sqrt(var01);
        } else if (model instanceof GammaScalarModel gamma) {
            return gamma.getScale() * Math.sqrt(gamma.getShape());
        } else if (model instanceof StudentTScalarModel studentT) {
            return studentT.getScale();
        } else if (model instanceof CompositeScalarModel composite) {
            // Weighted average of component widths
            ScalarModel[] components = composite.getScalarModels();
            double[] weights = composite.getWeights();
            double totalWeight = 0;
            double weightedSum = 0;
            for (int i = 0; i < components.length; i++) {
                double w = weights[i];
                totalWeight += w;
                weightedSum += w * getCharacteristicWidth(components[i]);
            }
            return totalWeight > 0 ? weightedSum / totalWeight : 0.1;
        }
        return 0.1;  // Default fallback
    }

    /**
     * Result of composite model recovery verification.
     *
     * @param sourceComponents number of components in the source model
     * @param fittedComponents number of components in the fitted model
     * @param exactMatches number of 1:1 matched components
     * @param mergedModes number of source modes that were merged into fewer fitted modes
     * @param unrecoveredModes number of source modes not found in fitted model
     * @param spuriousModes number of fitted modes not matching any source mode
     * @param recoveryRate ratio of exact matches to source components
     * @param effectiveRecoveryRate recovery rate including partial credit for merges
     * @param avgWeightError average absolute weight difference for matched components
     * @param maxWeightError maximum absolute weight difference for matched components
     * @param componentMatches detailed list of matched component pairs
     */
    public record RecoveryResult(
        int sourceComponents,
        int fittedComponents,
        int exactMatches,
        int mergedModes,
        int unrecoveredModes,
        int spuriousModes,
        double recoveryRate,
        double effectiveRecoveryRate,
        double avgWeightError,
        double maxWeightError,
        List<ComponentMatch> componentMatches
    ) {
        /**
         * Returns a formatted summary of the recovery result.
         */
        public String formatSummary() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("Model Recovery: %d/%d components (%.1f%%)%n",
                exactMatches, sourceComponents, recoveryRate * 100));
            sb.append(String.format("  Exact matches: %d%n", exactMatches));
            sb.append(String.format("  Merged modes: %d%n", mergedModes));
            sb.append(String.format("  Unrecovered: %d%n", unrecoveredModes));
            sb.append(String.format("  Spurious: %d%n", spuriousModes));
            sb.append(String.format("  Weight error: avg=%.4f, max=%.4f%n", avgWeightError, maxWeightError));
            sb.append(String.format("  Effective recovery: %.1f%%%n", effectiveRecoveryRate * 100));
            return sb.toString();
        }

        /**
         * Returns true if recovery meets the specified thresholds.
         */
        public boolean meetsThresholds(double minRecoveryRate, double maxWeightErrorThreshold) {
            return recoveryRate >= minRecoveryRate && maxWeightError <= maxWeightErrorThreshold;
        }
    }

    /**
     * A single matched component pair.
     *
     * @param sourceIndex index of the component in the source model
     * @param fittedIndex index of the component in the fitted model
     * @param weightError absolute difference between source and fitted weights
     * @param typeMatches true if the model types match exactly
     */
    public record ComponentMatch(
        int sourceIndex,
        int fittedIndex,
        double weightError,
        boolean typeMatches
    ) {}

    /**
     * Result of simple (non-composite) model recovery verification.
     *
     * @param typeMatches true if the model types match exactly
     * @param locationError absolute difference between source and fitted locations
     * @param relativeWidthError relative difference in characteristic width
     */
    public record SimpleRecoveryResult(
        boolean typeMatches,
        double locationError,
        double relativeWidthError
    ) {
        /**
         * Returns true if recovery is acceptable.
         */
        public boolean isAcceptable(double maxLocationError, double maxRelativeWidthError) {
            return typeMatches &&
                   locationError <= maxLocationError &&
                   relativeWidthError <= maxRelativeWidthError;
        }
    }
}
