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

import java.util.*;
import java.util.function.IntFunction;
import java.util.function.IntUnaryOperator;

/// Tree-structured Parzen Estimator (TPE) sampler for categorical parameter spaces.
///
/// Faithfully implements the core Optuna TPE algorithm in Java for use with
/// JMH benchmark optimization. Key features matching Optuna's defaults:
///
/// - **Adaptive gamma:** `γ(n) = min(⌈0.1 × n⌉, 25)` determines the number of
///   "good" trials, scaling with the total observation count rather than using
///   a fixed fraction.
/// - **Trial weighting:** Recent trials receive higher weight via a linear ramp,
///   matching Optuna's `default_weights` — uniform for fewer than 25 trials,
///   then a linear ramp from `1/n` to `1.0` for the oldest `n − 25` trials with
///   full weight for the most recent 25.
/// - **Kernel-based categorical posteriors:** Each observation becomes a kernel
///   with a peaked categorical distribution (observed category gets +1), mixed
///   with a uniform prior kernel weighted by [#priorWeight]. The mixture uses
///   trial weights to emphasize recent observations.
/// - **Configurable prior weight:** Controls the strength of the uniform prior
///   relative to observations (default 1.0, matching Optuna).
///
/// The algorithm proceeds by:
/// 1. Collecting observed (params, score) trials
/// 2. Splitting trials into "below" (top γ(n)) and "above" by objective value
/// 3. Building weighted kernel density estimates l(x) and g(x) for each group
/// 4. Suggesting the next trial by sampling candidates from l(x) and selecting
///    the one that maximizes l(x)/g(x)
///
/// For the first [#nStartupTrials] evaluations, random sampling is used to build
/// an initial population before TPE modeling begins.
///
/// @see <a href="https://arxiv.org/abs/1907.10902">Optuna paper (Akiba et al., 2019)</a>
/// @see <a href="https://papers.nips.cc/paper/2011/hash/86e8f7ab32cfd12577bc2619bc635690-Abstract.html">
///     Algorithms for Hyper-Parameter Optimization (Bergstra et al., 2011)</a>
public class TpeSampler {

    private final LinkedHashMap<String, String[]> paramSpace;
    private final List<Trial> history = new ArrayList<>();
    private final IntUnaryOperator gammaFn;
    private final IntFunction<double[]> weightsFn;
    private final double priorWeight;
    private final int nStartupTrials;
    private final int nCandidates;
    private final Random rng;

    /// A completed trial: parameter configuration and observed objective value.
    public record Trial(Map<String, String> params, double score) {}

    /// Creates a TPE sampler with Optuna-equivalent default settings.
    ///
    /// Uses adaptive gamma `min(⌈0.1n⌉, 25)`, recency-weighted trial weights,
    /// prior weight 1.0, 10 startup trials, and 24 candidates per suggestion.
    ///
    /// @param paramSpace ordered map of parameter name to possible values
    public TpeSampler(LinkedHashMap<String, String[]> paramSpace) {
        this(paramSpace, TpeSampler::defaultGamma, TpeSampler::defaultWeights,
            1.0, 10, 24, 42);
    }

    /// Creates a TPE sampler with custom settings.
    ///
    /// @param paramSpace ordered map of parameter name to possible values
    /// @param gammaFn function mapping total trial count to number of "below" (good) trials
    /// @param weightsFn function mapping group size to per-trial mixture weights
    /// @param priorWeight strength of the uniform prior kernel (default 1.0)
    /// @param nStartupTrials number of random trials before TPE kicks in
    /// @param nCandidates number of candidates sampled from l(x) per suggestion
    /// @param seed random seed for reproducibility
    public TpeSampler(LinkedHashMap<String, String[]> paramSpace,
                      IntUnaryOperator gammaFn, IntFunction<double[]> weightsFn,
                      double priorWeight, int nStartupTrials, int nCandidates, long seed) {
        this.paramSpace = paramSpace;
        this.gammaFn = gammaFn;
        this.weightsFn = weightsFn;
        this.priorWeight = priorWeight;
        this.nStartupTrials = nStartupTrials;
        this.nCandidates = nCandidates;
        this.rng = new Random(seed);
    }

    /// Optuna's default gamma: `min(⌈0.1 × n⌉, 25)`.
    ///
    /// Determines how many of the top-performing trials form the "below" (good) group.
    /// Scales with observation count rather than using a fixed fraction, capped at 25.
    static int defaultGamma(int n) {
        return Math.min((int) Math.ceil(0.1 * n), 25);
    }

    /// Optuna's default trial weighting function.
    ///
    /// For fewer than 25 trials, returns uniform weights (all 1.0).
    /// For 25 or more, the oldest `n − 25` trials receive linearly increasing
    /// weights from `1/n` to `1.0`, while the most recent 25 receive full weight.
    /// This causes the model to favor recent observations over stale ones.
    static double[] defaultWeights(int n) {
        if (n == 0) return new double[0];
        if (n < 25) {
            double[] w = new double[n];
            Arrays.fill(w, 1.0);
            return w;
        }
        double[] w = new double[n];
        int rampLen = n - 25;
        if (rampLen == 1) {
            w[0] = 1.0 / n;
        } else {
            double start = 1.0 / n;
            double step = (1.0 - start) / (rampLen - 1);
            for (int i = 0; i < rampLen; i++) {
                w[i] = start + i * step;
            }
        }
        for (int i = rampLen; i < n; i++) {
            w[i] = 1.0;
        }
        return w;
    }

    /// Records a completed trial.
    public void addTrial(Map<String, String> params, double score) {
        history.add(new Trial(Map.copyOf(params), score));
    }

    /// Suggests the next parameter configuration to evaluate.
    ///
    /// During the startup phase, returns random samples. After enough trials
    /// have been collected, uses TPE to suggest promising configurations.
    /// Automatically avoids suggesting previously-evaluated configurations.
    ///
    /// @return suggested parameter configuration, or empty if the entire space is exhausted
    public Optional<Map<String, String>> suggest() {
        int maxRetries = 100;
        for (int attempt = 0; attempt < maxRetries; attempt++) {
            Map<String, String> candidate = (history.size() < nStartupTrials)
                ? randomSample()
                : tpeSample();
            if (!isDuplicate(candidate)) {
                return Optional.of(candidate);
            }
        }
        return Optional.empty();
    }

    /// Returns the trial with the highest observed score.
    public Optional<Trial> getBestTrial() {
        return history.stream().max(Comparator.comparingDouble(Trial::score));
    }

    /// Returns all completed trials, sorted by score descending.
    public List<Trial> getAllTrialsSorted() {
        return history.stream()
            .sorted(Comparator.comparingDouble(Trial::score).reversed())
            .toList();
    }

    /// Returns all completed trials in the order they were evaluated.
    public List<Trial> getTrialsInOrder() {
        return List.copyOf(history);
    }

    /// Returns the number of completed trials.
    public int trialCount() {
        return history.size();
    }

    /// Returns the total number of possible configurations in the parameter space.
    public int spaceSize() {
        int size = 1;
        for (String[] values : paramSpace.values()) {
            size *= values.length;
        }
        return size;
    }

    private Map<String, String> randomSample() {
        Map<String, String> sample = new LinkedHashMap<>();
        for (var entry : paramSpace.entrySet()) {
            String[] values = entry.getValue();
            sample.put(entry.getKey(), values[rng.nextInt(values.length)]);
        }
        return sample;
    }

    private Map<String, String> tpeSample() {
        if (history.isEmpty()) {
            return randomSample();
        }

        List<Trial> sorted = new ArrayList<>(history);
        sorted.sort(Comparator.comparingDouble(Trial::score).reversed());

        int n = sorted.size();
        int nBelow = gammaFn.applyAsInt(n);
        nBelow = Math.max(1, Math.min(nBelow, n - 1));

        List<Trial> below = sorted.subList(0, nBelow);
        List<Trial> above = sorted.subList(nBelow, n);

        double[] wBelow = weightsFn.apply(nBelow);
        double[] wAbove = weightsFn.apply(n - nBelow);

        double[] belowMixWeights = buildMixtureWeights(wBelow);
        double[] aboveMixWeights = buildMixtureWeights(wAbove);

        // Pre-build kernels for each parameter (invariant across candidates)
        List<String> paramNames = new ArrayList<>(paramSpace.keySet());
        double[][][] belowKernels = new double[paramNames.size()][][];
        double[][][] aboveKernels = new double[paramNames.size()][][];
        for (int p = 0; p < paramNames.size(); p++) {
            String param = paramNames.get(p);
            String[] values = paramSpace.get(param);
            belowKernels[p] = buildKernels(below, param, values);
            aboveKernels[p] = buildKernels(above, param, values);
        }

        Map<String, String> bestCandidate = null;
        double bestLogRatio = Double.NEGATIVE_INFINITY;

        for (int c = 0; c < nCandidates; c++) {
            Map<String, String> candidate = new LinkedHashMap<>();
            double logRatio = 0.0;

            for (int p = 0; p < paramNames.size(); p++) {
                String param = paramNames.get(p);
                String[] values = paramSpace.get(param);

                int idx = sampleFromMixture(belowKernels[p], belowMixWeights);
                candidate.put(param, values[idx]);

                double logL = mixtureLogPdf(belowKernels[p], belowMixWeights, idx);
                double logG = mixtureLogPdf(aboveKernels[p], aboveMixWeights, idx);
                logRatio += logL - logG;
            }

            // Prefer non-duplicate candidates; when the EI landscape is peaked,
            // all candidates may converge to the same already-tried config.
            // Skipping duplicates here prevents suggest() from exhausting retries.
            if (!isDuplicate(candidate) && logRatio > bestLogRatio) {
                bestLogRatio = logRatio;
                bestCandidate = candidate;
            }
        }

        // If all EI candidates were duplicates, fall back to random exploration
        return (bestCandidate != null) ? bestCandidate : randomSample();
    }

    /// Builds a kernel matrix for a categorical parameter, matching Optuna's
    /// `_calculate_categorical_distributions`.
    ///
    /// Creates `n_trials + 1` kernels (one per observation plus a uniform prior).
    /// Each kernel row is initialized with `prior_weight / n_kernels` across all
    /// categories, then the observed category for each trial kernel gets +1.
    /// All rows are then normalized to sum to 1.
    ///
    /// @return `double[nKernels][nCategories]` — row-normalized kernel matrix
    private double[][] buildKernels(List<Trial> trials, String param, String[] values) {
        int nObs = trials.size();
        int nKernels = nObs + 1; // +1 for prior kernel
        int nCategories = values.length;
        double[][] kernels = new double[nKernels][nCategories];

        double fill = priorWeight / nKernels;
        for (double[] row : kernels) {
            Arrays.fill(row, fill);
        }

        for (int i = 0; i < nObs; i++) {
            String val = trials.get(i).params().get(param);
            for (int j = 0; j < nCategories; j++) {
                if (values[j].equals(val)) {
                    kernels[i][j] += 1.0;
                    break;
                }
            }
        }

        // Row-normalize
        for (double[] row : kernels) {
            double sum = 0;
            for (double v : row) sum += v;
            if (sum > 0) {
                for (int j = 0; j < row.length; j++) row[j] /= sum;
            }
        }

        return kernels;
    }

    /// Builds normalized mixture weights from trial weights plus a prior component.
    ///
    /// The returned array has `trialWeights.length + 1` entries: the trial weights
    /// followed by [#priorWeight], all normalized to sum to 1.
    private double[] buildMixtureWeights(double[] trialWeights) {
        double[] mix = new double[trialWeights.length + 1];
        System.arraycopy(trialWeights, 0, mix, 0, trialWeights.length);
        mix[trialWeights.length] = priorWeight;

        double sum = 0;
        for (double w : mix) sum += w;
        for (int i = 0; i < mix.length; i++) mix[i] /= sum;
        return mix;
    }

    /// Samples a category index from a weighted mixture of categorical kernels.
    ///
    /// First selects a kernel proportional to the mixture weights, then samples
    /// a category from that kernel's categorical distribution.
    private int sampleFromMixture(double[][] kernels, double[] mixtureWeights) {
        // Select kernel
        int k = sampleCategorical(mixtureWeights);
        // Sample category from selected kernel
        return sampleCategorical(kernels[k]);
    }

    /// Computes the log-probability of a category under a weighted mixture of kernels.
    ///
    /// `log P(x) = log Σ_k (w_k × kernel_k[x])`
    private double mixtureLogPdf(double[][] kernels, double[] mixtureWeights, int categoryIdx) {
        double pdf = 0;
        for (int k = 0; k < kernels.length; k++) {
            pdf += mixtureWeights[k] * kernels[k][categoryIdx];
        }
        return Math.log(Math.max(pdf, 1e-300));
    }

    private int sampleCategorical(double[] probs) {
        double r = rng.nextDouble();
        double cumulative = 0;
        for (int i = 0; i < probs.length; i++) {
            cumulative += probs[i];
            if (r <= cumulative) return i;
        }
        return probs.length - 1;
    }

    private boolean isDuplicate(Map<String, String> candidate) {
        for (Trial t : history) {
            if (t.params().equals(candidate)) return true;
        }
        return false;
    }
}
