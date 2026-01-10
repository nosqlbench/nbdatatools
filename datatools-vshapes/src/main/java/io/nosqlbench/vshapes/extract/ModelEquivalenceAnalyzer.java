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
 * Analyzes Pearson distribution models to determine if higher-order moment
 * parameters provide meaningful improvement over simpler models.
 *
 * <h2>Purpose</h2>
 *
 * <p>When fitting distributions using the full Pearson system, higher-order models
 * (e.g., Pearson Type IV with 4 parameters) may not always be necessary. This analyzer
 * quantifies how much the additional moment parameters (skewness, kurtosis) contribute
 * to the distribution shape, helping users decide whether to use a simpler model.
 *
 * <h2>Pearson Hierarchy</h2>
 *
 * <p>The Pearson system forms a hierarchy based on moment constraints:
 *
 * <pre>
 * ┌─────────────────────────────────────────────────────────────────────────────┐
 * │                        PEARSON MODEL HIERARCHY                              │
 * │                     (ordered by moment complexity)                          │
 * └─────────────────────────────────────────────────────────────────────────────┘
 *
 *   Simplest                                                          Most Complex
 *   (2 params)              (3 params)               (4 params)
 *   ┌─────────┐            ┌──────────┐            ┌──────────────┐
 *   │ Uniform │            │  Gamma   │            │  Pearson IV  │
 *   │ (min,   │            │ (shape,  │            │ (m, ν, a, λ) │
 *   │  max)   │            │  scale,  │            │              │
 *   │         │            │  loc)    │            │ Skewness +   │
 *   │ β₁=0    │            │          │            │ Kurtosis     │
 *   │ β₂=1.8  │            │ Skewness │            │              │
 *   └─────────┘            └──────────┘            └──────────────┘
 *        ↑                      ↑                        ↑
 *   ┌─────────┐            ┌──────────┐            ┌──────────────┐
 *   │ Normal  │            │ Student-t│            │    Beta      │
 *   │ (μ, σ)  │            │  (ν,μ,σ) │            │  (α,β,a,b)   │
 *   │         │            │          │            │              │
 *   │ β₁=0    │            │ β₁=0     │            │ Bounded      │
 *   │ β₂=3    │            │ β₂>3     │            │ Support      │
 *   └─────────┘            └──────────┘            └──────────────┘
 * </pre>
 *
 * <h2>Divergence Metrics</h2>
 *
 * <p>Three complementary metrics quantify model equivalence:
 *
 * <ul>
 *   <li><b>Max CDF Difference</b>: Maximum |F₁(x) - F₂(x)|. Intuitive, bounded [0,1].
 *       Values &lt; 0.01 indicate practically equivalent distributions.</li>
 *   <li><b>Mean Absolute CDF Difference</b>: Average |F₁(x) - F₂(x)| over the support.
 *       Less sensitive to tail behavior than max difference.</li>
 *   <li><b>Moment Deviation</b>: Relative difference in standardized moments.
 *       Directly measures if skewness/kurtosis parameters matter.</li>
 * </ul>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Analyze a Pearson IV model
 * PearsonIVScalarModel p4 = new PearsonIVScalarModel(3.0, 0.1, 1.0, 0.0);
 * ModelEquivalenceAnalyzer analyzer = new ModelEquivalenceAnalyzer();
 *
 * // Get full analysis
 * EquivalenceReport report = analyzer.analyze(p4);
 * System.out.println(report);
 *
 * // Check if normal is sufficient
 * if (report.getRecommendedSimplification() != null) {
 *     System.out.println("Can simplify to: " + report.getRecommendedSimplification());
 * }
 * }</pre>
 *
 * @see EquivalenceReport
 * @see PearsonType
 */
public class ModelEquivalenceAnalyzer {

    /** Default number of points for CDF comparison. */
    private static final int DEFAULT_CDF_POINTS = 1000;

    /** Default threshold for considering models equivalent. */
    private static final double DEFAULT_EQUIVALENCE_THRESHOLD = 0.02;

    private final int cdfPoints;
    private final double equivalenceThreshold;

    /**
     * Constructs an analyzer with default settings.
     */
    public ModelEquivalenceAnalyzer() {
        this(DEFAULT_CDF_POINTS, DEFAULT_EQUIVALENCE_THRESHOLD);
    }

    /**
     * Constructs an analyzer with custom settings.
     *
     * @param cdfPoints number of points for CDF comparison
     * @param equivalenceThreshold max CDF difference threshold for equivalence
     */
    public ModelEquivalenceAnalyzer(int cdfPoints, double equivalenceThreshold) {
        this.cdfPoints = cdfPoints;
        this.equivalenceThreshold = equivalenceThreshold;
    }

    /**
     * Analyzes a scalar model and compares it against simpler alternatives.
     *
     * @param model the model to analyze
     * @return an equivalence report with comparisons and recommendations
     */
    public EquivalenceReport analyze(ScalarModel model) {
        List<ModelComparison> comparisons = new ArrayList<>();

        // Extract moments from the source model
        MomentProfile sourceProfile = extractMoments(model);

        // Generate candidate simpler models
        List<ScalarModel> candidates = generateCandidates(model, sourceProfile);

        // Compare against each candidate
        for (ScalarModel candidate : candidates) {
            MomentProfile candidateProfile = extractMoments(candidate);
            ModelComparison comparison = compare(model, sourceProfile, candidate, candidateProfile);
            comparisons.add(comparison);
        }

        // Sort by divergence (best matches first)
        comparisons.sort(Comparator.comparingDouble(ModelComparison::getMaxCdfDifference));

        // Find recommended simplification
        ScalarModel recommended = null;
        for (ModelComparison comparison : comparisons) {
            if (comparison.equivalent() && isSimpler(comparison.candidate(), model)) {
                recommended = comparison.candidate();
                break;
            }
        }

        return new EquivalenceReport(model, sourceProfile, comparisons,
                                      recommended, equivalenceThreshold);
    }

    /**
     * Compares a VectorSpaceModel against simpler per-dimension alternatives.
     *
     * @param vectorModel the vector model to analyze
     * @return a map of dimension index to equivalence report
     */
    public Map<Integer, EquivalenceReport> analyzeVector(VectorSpaceModel vectorModel) {
        Map<Integer, EquivalenceReport> reports = new LinkedHashMap<>();
        for (int d = 0; d < vectorModel.dimensions(); d++) {
            ScalarModel scalar = vectorModel.scalarModel(d);
            reports.put(d, analyze(scalar));
        }
        return reports;
    }

    /**
     * Provides a summary analysis of a VectorSpaceModel.
     *
     * @param vectorModel the vector model to analyze
     * @return summary statistics about model simplification opportunities
     */
    public VectorSimplificationSummary summarizeVector(VectorSpaceModel vectorModel) {
        Map<Integer, EquivalenceReport> reports = analyzeVector(vectorModel);

        int canSimplifyCount = 0;
        int normalEquivalentCount = 0;
        int uniformEquivalentCount = 0;
        double avgMaxCdfDiff = 0.0;

        Map<String, Integer> recommendedTypeCounts = new LinkedHashMap<>();

        for (EquivalenceReport report : reports.values()) {
            if (report.getRecommendedSimplification() != null) {
                canSimplifyCount++;
                String type = report.getRecommendedSimplification().getModelType();
                recommendedTypeCounts.merge(type, 1, Integer::sum);

                if (type.equals(NormalScalarModel.MODEL_TYPE)) {
                    normalEquivalentCount++;
                } else if (type.equals(UniformScalarModel.MODEL_TYPE)) {
                    uniformEquivalentCount++;
                }
            }

            // Average the best match's CDF difference
            if (!report.getComparisons().isEmpty()) {
                avgMaxCdfDiff += report.getComparisons().get(0).getMaxCdfDifference();
            }
        }

        avgMaxCdfDiff /= reports.size();

        return new VectorSimplificationSummary(
            vectorModel.dimensions(),
            canSimplifyCount,
            normalEquivalentCount,
            uniformEquivalentCount,
            avgMaxCdfDiff,
            recommendedTypeCounts
        );
    }

    /**
     * Extracts standardized moments from a scalar model.
     */
    private MomentProfile extractMoments(ScalarModel model) {
        double mean = 0, stdDev = 1, skewness = 0, kurtosis = 3;
        double lower = Double.NEGATIVE_INFINITY;
        double upper = Double.POSITIVE_INFINITY;

        if (model instanceof NormalScalarModel normal) {
            mean = normal.getMean();
            stdDev = normal.getStdDev();
            skewness = 0;
            kurtosis = 3;
            lower = normal.lower();
            upper = normal.upper();
        } else if (model instanceof GammaScalarModel gamma) {
            mean = gamma.getMean();
            stdDev = gamma.getStdDev();
            skewness = gamma.getSkewness();
            kurtosis = gamma.getKurtosis();
            lower = gamma.getLower();
        } else if (model instanceof StudentTScalarModel t) {
            mean = t.getMean();
            stdDev = t.hasFiniteVariance() ? t.getStdDev() : Double.NaN;
            skewness = t.getSkewness();
            kurtosis = t.getKurtosis();
        } else if (model instanceof PearsonIVScalarModel p4) {
            mean = p4.getMean();
            // Approximate stdDev from scale parameter
            stdDev = p4.getA();
            skewness = p4.isSymmetric() ? 0 : estimateSkewness(p4);
            kurtosis = estimateKurtosis(p4);
        } else if (model instanceof BetaScalarModel beta) {
            mean = beta.getMean();
            stdDev = beta.getStdDev();
            skewness = beta.getSkewness();
            kurtosis = beta.getKurtosis();
            lower = beta.getLower();
            upper = beta.getUpper();
        } else if (model instanceof UniformScalarModel uniform) {
            mean = uniform.getMean();
            stdDev = uniform.getStdDev();
            skewness = 0;
            kurtosis = 1.8;  // Uniform distribution kurtosis
            lower = uniform.getLower();
            upper = uniform.getUpper();
        } else if (model instanceof InverseGammaScalarModel ig) {
            mean = ig.getMean();
            stdDev = ig.hasFiniteVariance() ? ig.getStdDev() : Double.NaN;
            skewness = ig.getSkewness();  // Returns NaN if undefined
            double excessKurt = ig.getExcessKurtosis();
            kurtosis = Double.isNaN(excessKurt) ? Double.NaN : 3 + excessKurt;
        } else if (model instanceof BetaPrimeScalarModel bp) {
            mean = bp.getMean();
            stdDev = bp.hasFiniteVariance() ? bp.getStdDev() : Double.NaN;
            skewness = bp.getSkewness();  // Returns NaN if undefined
            // BetaPrimeScalarModel doesn't define getExcessKurtosis(), use approximation
            kurtosis = Double.NaN;  // Would need full calculation
        }

        return new MomentProfile(mean, stdDev, skewness, kurtosis, lower, upper);
    }

    /**
     * Generates candidate simpler models based on the source model's moments.
     */
    private List<ScalarModel> generateCandidates(ScalarModel source, MomentProfile profile) {
        List<ScalarModel> candidates = new ArrayList<>();

        double mean = profile.mean();
        double stdDev = Double.isNaN(profile.stdDev()) ? 1.0 : profile.stdDev();

        // Always try normal (2 parameters: mean, stdDev)
        if (Double.isFinite(profile.lower()) && Double.isFinite(profile.upper())) {
            // Truncated normal for bounded support
            candidates.add(new NormalScalarModel(mean, stdDev, profile.lower(), profile.upper()));
        } else {
            candidates.add(new NormalScalarModel(mean, stdDev));
        }

        // Try uniform if bounded support
        if (Double.isFinite(profile.lower()) && Double.isFinite(profile.upper())) {
            candidates.add(new UniformScalarModel(profile.lower(), profile.upper()));
        }

        // Try Student-t if we have excess kurtosis (symmetric, heavy tails)
        if (Math.abs(profile.skewness()) < 0.1 && profile.kurtosis() > 3.5) {
            double excessKurt = profile.kurtosis() - 3;
            if (excessKurt > 0) {
                double df = 4 + 6.0 / excessKurt;
                if (df > 2) {
                    candidates.add(new StudentTScalarModel(df, mean, stdDev));
                }
            }
        }

        // Try Gamma if positive skewness and semi-bounded
        if (profile.skewness() > 0.1 && Double.isFinite(profile.lower()) && mean > 0) {
            double variance = stdDev * stdDev;
            double shape = (mean * mean) / variance;
            double scale = variance / mean;
            if (shape > 0 && scale > 0) {
                candidates.add(new GammaScalarModel(shape, scale, profile.lower()));
            }
        }

        // If source is already normal, we shouldn't return it as a candidate
        // (Filter out same-type candidates)
        candidates.removeIf(c -> c.getClass().equals(source.getClass()));

        return candidates;
    }

    /**
     * Compares two models using CDF-based divergence metrics.
     */
    private ModelComparison compare(ScalarModel source, MomentProfile sourceProfile,
                                     ScalarModel candidate, MomentProfile candidateProfile) {
        // Determine comparison range
        double lower = determineComparisonLower(sourceProfile, candidateProfile);
        double upper = determineComparisonUpper(sourceProfile, candidateProfile);

        double maxDiff = 0;
        double sumDiff = 0;
        double step = (upper - lower) / cdfPoints;

        for (int i = 0; i <= cdfPoints; i++) {
            double x = lower + i * step;
            double cdf1 = evaluateCdf(source, x);
            double cdf2 = evaluateCdf(candidate, x);
            double diff = Math.abs(cdf1 - cdf2);
            maxDiff = Math.max(maxDiff, diff);
            sumDiff += diff;
        }

        double meanDiff = sumDiff / (cdfPoints + 1);

        // Compute moment deviations
        double skewnessDeviation = Math.abs(sourceProfile.skewness() - candidateProfile.skewness());
        double kurtosisDeviation = Math.abs(sourceProfile.kurtosis() - candidateProfile.kurtosis());

        boolean equivalent = maxDiff <= equivalenceThreshold;

        return new ModelComparison(
            candidate,
            candidateProfile,
            maxDiff,
            meanDiff,
            skewnessDeviation,
            kurtosisDeviation,
            equivalent
        );
    }

    /**
     * Evaluates the CDF of a model at a given point.
     */
    private double evaluateCdf(ScalarModel model, double x) {
        if (model instanceof NormalScalarModel normal) {
            return normal.cdf(x);
        } else if (model instanceof UniformScalarModel uniform) {
            return uniform.cdf(x);
        } else if (model instanceof StudentTScalarModel t) {
            return studentTCdf(x, t);
        } else if (model instanceof GammaScalarModel gamma) {
            return gammaCdf(x, gamma);
        } else if (model instanceof BetaScalarModel beta) {
            return betaCdf(x, beta);
        } else if (model instanceof PearsonIVScalarModel p4) {
            return pearsonIVCdf(x, p4);
        }
        // Fallback: approximate from moments using normal approximation
        MomentProfile profile = extractMoments(model);
        return NormalCDF.cdf(x, profile.mean(), profile.stdDev());
    }

    private double determineComparisonLower(MomentProfile p1, MomentProfile p2) {
        double lower = Math.max(p1.lower(), p2.lower());
        if (!Double.isFinite(lower)) {
            // Use mean - 5*stdDev as practical lower bound
            double m1 = p1.mean(), m2 = p2.mean();
            double s1 = Double.isNaN(p1.stdDev()) ? 1 : p1.stdDev();
            double s2 = Double.isNaN(p2.stdDev()) ? 1 : p2.stdDev();
            lower = Math.min(m1 - 5*s1, m2 - 5*s2);
        }
        return lower;
    }

    private double determineComparisonUpper(MomentProfile p1, MomentProfile p2) {
        double upper = Math.min(p1.upper(), p2.upper());
        if (!Double.isFinite(upper)) {
            double m1 = p1.mean(), m2 = p2.mean();
            double s1 = Double.isNaN(p1.stdDev()) ? 1 : p1.stdDev();
            double s2 = Double.isNaN(p2.stdDev()) ? 1 : p2.stdDev();
            upper = Math.max(m1 + 5*s1, m2 + 5*s2);
        }
        return upper;
    }

    /**
     * Checks if candidate is simpler than source based on parameter count/type.
     */
    private boolean isSimpler(ScalarModel candidate, ScalarModel source) {
        int candidateComplexity = modelComplexity(candidate);
        int sourceComplexity = modelComplexity(source);
        return candidateComplexity < sourceComplexity;
    }

    private int modelComplexity(ScalarModel model) {
        return switch (model.getModelType()) {
            case UniformScalarModel.MODEL_TYPE -> 1;
            case NormalScalarModel.MODEL_TYPE -> 2;
            case StudentTScalarModel.MODEL_TYPE -> 3;
            case GammaScalarModel.MODEL_TYPE -> 3;
            case BetaScalarModel.MODEL_TYPE -> 4;
            case PearsonIVScalarModel.MODEL_TYPE -> 4;
            case InverseGammaScalarModel.MODEL_TYPE -> 3;
            case BetaPrimeScalarModel.MODEL_TYPE -> 3;
            default -> 5;  // Empirical or unknown
        };
    }

    // CDF approximations for various distributions

    private double studentTCdf(double x, StudentTScalarModel t) {
        double z = (x - t.getLocation()) / t.getScale();
        double nu = t.getDegreesOfFreedom();
        // Use approximation via regularized incomplete beta function
        double t2 = z * z;
        double p = 0.5 * regularizedIncompleteBeta(nu / 2, 0.5, nu / (nu + t2));
        return z >= 0 ? 1 - p : p;
    }

    private double gammaCdf(double x, GammaScalarModel gamma) {
        double shifted = x - gamma.getLocation();
        if (shifted <= 0) return 0;
        // Use regularized incomplete gamma function
        return regularizedGammaP(gamma.getShape(), shifted / gamma.getScale());
    }

    private double betaCdf(double x, BetaScalarModel beta) {
        double normalized = (x - beta.getLower()) / (beta.getUpper() - beta.getLower());
        if (normalized <= 0) return 0;
        if (normalized >= 1) return 1;
        return regularizedIncompleteBeta(beta.getAlpha(), beta.getBeta(), normalized);
    }

    private double pearsonIVCdf(double x, PearsonIVScalarModel p4) {
        // Approximate using numerical integration or normal approximation
        double z = (x - p4.getLambda()) / p4.getA();
        // For now, use arctan-based approximation
        double theta = Math.atan(z);
        double base = (theta + Math.PI/2) / Math.PI;
        // Adjust for skewness
        if (!p4.isSymmetric()) {
            base += p4.getNu() * theta / (2 * Math.PI * p4.getM());
        }
        return Math.max(0, Math.min(1, base));
    }

    private double estimateSkewness(PearsonIVScalarModel p4) {
        // Approximate skewness from nu parameter
        double m = p4.getM();
        double nu = p4.getNu();
        if (m > 1.5) {
            return -nu / Math.sqrt(m);
        }
        return 0;
    }

    private double estimateKurtosis(PearsonIVScalarModel p4) {
        // Approximate kurtosis from m parameter
        double m = p4.getM();
        if (m > 2) {
            return 3 + 6 / (2 * m - 4);
        }
        return 6;  // Heavy tails
    }

    // Mathematical helper functions

    private double regularizedGammaP(double a, double x) {
        // Approximation using series expansion for small x, continued fraction for large x
        if (x < a + 1) {
            return gammaSeriesP(a, x);
        } else {
            return 1 - gammaContinuedFractionQ(a, x);
        }
    }

    private double gammaSeriesP(double a, double x) {
        double sum = 1.0 / a;
        double term = sum;
        for (int n = 1; n < 100; n++) {
            term *= x / (a + n);
            sum += term;
            if (Math.abs(term) < 1e-10 * Math.abs(sum)) break;
        }
        return sum * Math.exp(-x + a * Math.log(x) - logGamma(a));
    }

    private double gammaContinuedFractionQ(double a, double x) {
        double b = x + 1 - a;
        double c = 1.0 / 1e-30;
        double d = 1.0 / b;
        double h = d;
        for (int i = 1; i <= 100; i++) {
            double an = -i * (i - a);
            b += 2;
            d = an * d + b;
            if (Math.abs(d) < 1e-30) d = 1e-30;
            c = b + an / c;
            if (Math.abs(c) < 1e-30) c = 1e-30;
            d = 1.0 / d;
            double del = d * c;
            h *= del;
            if (Math.abs(del - 1) < 1e-10) break;
        }
        return Math.exp(-x + a * Math.log(x) - logGamma(a)) * h;
    }

    private double regularizedIncompleteBeta(double a, double b, double x) {
        if (x == 0 || x == 1) return x;
        // Use continued fraction representation
        if (x > (a + 1) / (a + b + 2)) {
            return 1 - regularizedIncompleteBeta(b, a, 1 - x);
        }
        double front = Math.exp(logGamma(a + b) - logGamma(a) - logGamma(b) +
                                a * Math.log(x) + b * Math.log(1 - x)) / a;
        return front * betaContinuedFraction(a, b, x);
    }

    private double betaContinuedFraction(double a, double b, double x) {
        double qab = a + b;
        double qap = a + 1;
        double qam = a - 1;
        double c = 1;
        double d = 1 - qab * x / qap;
        if (Math.abs(d) < 1e-30) d = 1e-30;
        d = 1 / d;
        double h = d;
        for (int m = 1; m <= 100; m++) {
            int m2 = 2 * m;
            double aa = m * (b - m) * x / ((qam + m2) * (a + m2));
            d = 1 + aa * d;
            if (Math.abs(d) < 1e-30) d = 1e-30;
            c = 1 + aa / c;
            if (Math.abs(c) < 1e-30) c = 1e-30;
            d = 1 / d;
            h *= d * c;
            aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2));
            d = 1 + aa * d;
            if (Math.abs(d) < 1e-30) d = 1e-30;
            c = 1 + aa / c;
            if (Math.abs(c) < 1e-30) c = 1e-30;
            d = 1 / d;
            double del = d * c;
            h *= del;
            if (Math.abs(del - 1) < 1e-10) break;
        }
        return h;
    }

    private double logGamma(double x) {
        // Lanczos approximation
        double[] c = {76.18009172947146, -86.50532032941677, 24.01409824083091,
                      -1.231739572450155, 0.1208650973866179e-2, -0.5395239384953e-5};
        double y = x;
        double tmp = x + 5.5;
        tmp -= (x + 0.5) * Math.log(tmp);
        double ser = 1.000000000190015;
        for (int j = 0; j < 6; j++) {
            ser += c[j] / ++y;
        }
        return -tmp + Math.log(2.5066282746310005 * ser / x);
    }

    /**
     * Moment profile for a scalar model.
     *
     * @param mean the mean of the distribution
     * @param stdDev the standard deviation
     * @param skewness the skewness (third standardized moment)
     * @param kurtosis the kurtosis (fourth standardized moment)
     * @param lower the lower bound of support
     * @param upper the upper bound of support
     */
    public record MomentProfile(
        double mean,
        double stdDev,
        double skewness,
        double kurtosis,
        double lower,
        double upper
    ) {
        public boolean isSymmetric() {
            return Math.abs(skewness) < 0.1;
        }

        public boolean isMesokurtic() {
            return Math.abs(kurtosis - 3) < 0.5;
        }

        public boolean isBounded() {
            return Double.isFinite(lower) && Double.isFinite(upper);
        }

        public boolean isSemiBounded() {
            return Double.isFinite(lower) ^ Double.isFinite(upper);
        }
    }

    /**
     * Comparison result between source model and a candidate simplification.
     *
     * @param candidate the candidate simplified model
     * @param candidateProfile moment profile of the candidate
     * @param maxCdfDifference maximum CDF difference between models
     * @param meanCdfDifference mean CDF difference between models
     * @param skewnessDeviation deviation in skewness from source
     * @param kurtosisDeviation deviation in kurtosis from source
     * @param equivalent whether the candidate is equivalent within threshold
     */
    public record ModelComparison(
        ScalarModel candidate,
        MomentProfile candidateProfile,
        double maxCdfDifference,
        double meanCdfDifference,
        double skewnessDeviation,
        double kurtosisDeviation,
        boolean equivalent
    ) {
        public ScalarModel getCandidate() { return candidate; }
        public double getMaxCdfDifference() { return maxCdfDifference; }
    }

    /**
     * Full equivalence analysis report for a scalar model.
     */
    public static class EquivalenceReport {
        private final ScalarModel source;
        private final MomentProfile sourceProfile;
        private final List<ModelComparison> comparisons;
        private final ScalarModel recommendedSimplification;
        private final double threshold;

        EquivalenceReport(ScalarModel source, MomentProfile sourceProfile,
                          List<ModelComparison> comparisons,
                          ScalarModel recommendedSimplification, double threshold) {
            this.source = source;
            this.sourceProfile = sourceProfile;
            this.comparisons = comparisons;
            this.recommendedSimplification = recommendedSimplification;
            this.threshold = threshold;
        }

        public ScalarModel getSource() { return source; }
        public MomentProfile getSourceProfile() { return sourceProfile; }
        public List<ModelComparison> getComparisons() { return comparisons; }
        public ScalarModel getRecommendedSimplification() { return recommendedSimplification; }
        public double getThreshold() { return threshold; }

        public boolean canSimplify() {
            return recommendedSimplification != null;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("EquivalenceReport for ").append(source.getModelType()).append("\n");
            sb.append("  Source moments: mean=").append(String.format("%.4f", sourceProfile.mean()));
            sb.append(", stdDev=").append(String.format("%.4f", sourceProfile.stdDev()));
            sb.append(", skewness=").append(String.format("%.4f", sourceProfile.skewness()));
            sb.append(", kurtosis=").append(String.format("%.4f", sourceProfile.kurtosis()));
            sb.append("\n");

            sb.append("  Comparisons (threshold=").append(threshold).append("):\n");
            for (ModelComparison comp : comparisons) {
                sb.append("    ").append(comp.candidate.getModelType());
                sb.append(": maxCdfDiff=").append(String.format("%.4f", comp.maxCdfDifference));
                sb.append(", skewDev=").append(String.format("%.4f", comp.skewnessDeviation));
                sb.append(", kurtDev=").append(String.format("%.4f", comp.kurtosisDeviation));
                if (comp.equivalent) sb.append(" [EQUIVALENT]");
                sb.append("\n");
            }

            if (recommendedSimplification != null) {
                sb.append("  RECOMMENDATION: Simplify to ").append(recommendedSimplification.getModelType());
            } else {
                sb.append("  RECOMMENDATION: Keep current model (no simpler equivalent found)");
            }

            return sb.toString();
        }
    }

    /**
     * Summary of simplification opportunities for a vector model.
     *
     * @param totalDimensions total number of dimensions analyzed
     * @param canSimplifyCount number of dimensions that can be simplified
     * @param normalEquivalentCount dimensions equivalent to normal distribution
     * @param uniformEquivalentCount dimensions equivalent to uniform distribution
     * @param averageMaxCdfDifference average max CDF difference across simplifiable dims
     * @param recommendedTypeCounts count of each recommended simplified type
     */
    public record VectorSimplificationSummary(
        int totalDimensions,
        int canSimplifyCount,
        int normalEquivalentCount,
        int uniformEquivalentCount,
        double averageMaxCdfDifference,
        Map<String, Integer> recommendedTypeCounts
    ) {
        public double simplificationRate() {
            return (double) canSimplifyCount / totalDimensions;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("VectorSimplificationSummary:\n");
            sb.append("  Total dimensions: ").append(totalDimensions).append("\n");
            sb.append("  Can simplify: ").append(canSimplifyCount);
            sb.append(" (").append(String.format("%.1f%%", simplificationRate() * 100)).append(")\n");
            sb.append("  Normal-equivalent: ").append(normalEquivalentCount).append("\n");
            sb.append("  Uniform-equivalent: ").append(uniformEquivalentCount).append("\n");
            sb.append("  Average max CDF diff: ").append(String.format("%.4f", averageMaxCdfDifference)).append("\n");
            if (!recommendedTypeCounts.isEmpty()) {
                sb.append("  Recommended types: ").append(recommendedTypeCounts);
            }
            return sb.toString();
        }
    }
}
