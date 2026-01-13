package io.nosqlbench.vshapes.model;

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

import java.util.Comparator;

/**
 * Provides deterministic ordering of {@link ScalarModel} instances.
 *
 * <h2>Ordering Rules</h2>
 *
 * <p>Models are ordered by:
 * <ol>
 *   <li>Model type name (alphabetically)</li>
 *   <li>Parameter values in type-specific order</li>
 * </ol>
 *
 * <h2>Parameter Ordering by Type</h2>
 *
 * <table>
 *   <caption>Parameter comparison order for each model type</caption>
 *   <tr><th>Type</th><th>Parameter Order</th></tr>
 *   <tr><td>beta</td><td>alpha, beta, lower, upper</td></tr>
 *   <tr><td>beta_prime</td><td>alpha, beta</td></tr>
 *   <tr><td>composite</td><td>(recursively sorted components)</td></tr>
 *   <tr><td>empirical</td><td>min bin edge</td></tr>
 *   <tr><td>gamma</td><td>shape, scale</td></tr>
 *   <tr><td>inverse_gamma</td><td>alpha, beta</td></tr>
 *   <tr><td>normal</td><td>mean, stdDev, lower, upper</td></tr>
 *   <tr><td>pearson_iv</td><td>m, nu, a, lambda</td></tr>
 *   <tr><td>student_t</td><td>df, loc, scale</td></tr>
 *   <tr><td>uniform</td><td>lower, upper</td></tr>
 * </table>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Sort a list of models
 * List<ScalarModel> models = ...;
 * models.sort(ScalarModelComparator.INSTANCE);
 *
 * // Compare two models
 * int cmp = ScalarModelComparator.INSTANCE.compare(model1, model2);
 * }</pre>
 *
 * @see ScalarModel
 * @see CompositeScalarModel#toCanonicalForm()
 */
public final class ScalarModelComparator implements Comparator<ScalarModel> {

    /** Singleton instance for convenience. */
    public static final ScalarModelComparator INSTANCE = new ScalarModelComparator();

    private static final double EPSILON = 1e-10;

    private ScalarModelComparator() {
        // Singleton
    }

    @Override
    public int compare(ScalarModel m1, ScalarModel m2) {
        if (m1 == m2) return 0;
        if (m1 == null) return -1;
        if (m2 == null) return 1;

        // First compare by model type name
        int typeCompare = m1.getModelType().compareTo(m2.getModelType());
        if (typeCompare != 0) {
            return typeCompare;
        }

        // Same type - compare by parameters
        return compareParameters(m1, m2);
    }

    private int compareParameters(ScalarModel m1, ScalarModel m2) {
        String type = m1.getModelType();

        return switch (type) {
            case NormalScalarModel.MODEL_TYPE -> compareNormal(
                (NormalScalarModel) m1, (NormalScalarModel) m2);
            case UniformScalarModel.MODEL_TYPE -> compareUniform(
                (UniformScalarModel) m1, (UniformScalarModel) m2);
            case BetaScalarModel.MODEL_TYPE -> compareBeta(
                (BetaScalarModel) m1, (BetaScalarModel) m2);
            case GammaScalarModel.MODEL_TYPE -> compareGamma(
                (GammaScalarModel) m1, (GammaScalarModel) m2);
            case StudentTScalarModel.MODEL_TYPE -> compareStudentT(
                (StudentTScalarModel) m1, (StudentTScalarModel) m2);
            case InverseGammaScalarModel.MODEL_TYPE -> compareInverseGamma(
                (InverseGammaScalarModel) m1, (InverseGammaScalarModel) m2);
            case BetaPrimeScalarModel.MODEL_TYPE -> compareBetaPrime(
                (BetaPrimeScalarModel) m1, (BetaPrimeScalarModel) m2);
            case PearsonIVScalarModel.MODEL_TYPE -> comparePearsonIV(
                (PearsonIVScalarModel) m1, (PearsonIVScalarModel) m2);
            case CompositeScalarModel.MODEL_TYPE -> compareComposite(
                (CompositeScalarModel) m1, (CompositeScalarModel) m2);
            case EmpiricalScalarModel.MODEL_TYPE -> compareEmpirical(
                (EmpiricalScalarModel) m1, (EmpiricalScalarModel) m2);
            default -> 0;  // Unknown type - treat as equal
        };
    }

    private int compareNormal(NormalScalarModel m1, NormalScalarModel m2) {
        int cmp = compareDouble(m1.getMean(), m2.getMean());
        if (cmp != 0) return cmp;
        cmp = compareDouble(m1.getStdDev(), m2.getStdDev());
        if (cmp != 0) return cmp;
        cmp = compareDouble(m1.lower(), m2.lower());
        if (cmp != 0) return cmp;
        return compareDouble(m1.upper(), m2.upper());
    }

    private int compareUniform(UniformScalarModel m1, UniformScalarModel m2) {
        int cmp = compareDouble(m1.getLower(), m2.getLower());
        if (cmp != 0) return cmp;
        return compareDouble(m1.getUpper(), m2.getUpper());
    }

    private int compareBeta(BetaScalarModel m1, BetaScalarModel m2) {
        int cmp = compareDouble(m1.getAlpha(), m2.getAlpha());
        if (cmp != 0) return cmp;
        cmp = compareDouble(m1.getBeta(), m2.getBeta());
        if (cmp != 0) return cmp;
        cmp = compareDouble(m1.getLower(), m2.getLower());
        if (cmp != 0) return cmp;
        return compareDouble(m1.getUpper(), m2.getUpper());
    }

    private int compareGamma(GammaScalarModel m1, GammaScalarModel m2) {
        int cmp = compareDouble(m1.getShape(), m2.getShape());
        if (cmp != 0) return cmp;
        return compareDouble(m1.getScale(), m2.getScale());
    }

    private int compareStudentT(StudentTScalarModel m1, StudentTScalarModel m2) {
        int cmp = compareDouble(m1.getDegreesOfFreedom(), m2.getDegreesOfFreedom());
        if (cmp != 0) return cmp;
        cmp = compareDouble(m1.getLocation(), m2.getLocation());
        if (cmp != 0) return cmp;
        return compareDouble(m1.getScale(), m2.getScale());
    }

    private int compareInverseGamma(InverseGammaScalarModel m1, InverseGammaScalarModel m2) {
        int cmp = compareDouble(m1.getShape(), m2.getShape());
        if (cmp != 0) return cmp;
        return compareDouble(m1.getScale(), m2.getScale());
    }

    private int compareBetaPrime(BetaPrimeScalarModel m1, BetaPrimeScalarModel m2) {
        int cmp = compareDouble(m1.getAlpha(), m2.getAlpha());
        if (cmp != 0) return cmp;
        return compareDouble(m1.getBeta(), m2.getBeta());
    }

    private int comparePearsonIV(PearsonIVScalarModel m1, PearsonIVScalarModel m2) {
        int cmp = compareDouble(m1.getM(), m2.getM());
        if (cmp != 0) return cmp;
        cmp = compareDouble(m1.getNu(), m2.getNu());
        if (cmp != 0) return cmp;
        cmp = compareDouble(m1.getA(), m2.getA());
        if (cmp != 0) return cmp;
        return compareDouble(m1.getLambda(), m2.getLambda());
    }

    private int compareComposite(CompositeScalarModel m1, CompositeScalarModel m2) {
        // First compare by component count
        int cmp = Integer.compare(m1.getComponentCount(), m2.getComponentCount());
        if (cmp != 0) return cmp;

        // Compare weights (normalized)
        double[] w1 = normalizeWeights(m1.getWeights());
        double[] w2 = normalizeWeights(m2.getWeights());
        for (int i = 0; i < w1.length; i++) {
            cmp = compareDouble(w1[i], w2[i]);
            if (cmp != 0) return cmp;
        }

        // Compare components (recursively)
        ScalarModel[] c1 = m1.getScalarModels();
        ScalarModel[] c2 = m2.getScalarModels();
        for (int i = 0; i < c1.length; i++) {
            cmp = compare(c1[i], c2[i]);
            if (cmp != 0) return cmp;
        }

        return 0;
    }

    private int compareEmpirical(EmpiricalScalarModel m1, EmpiricalScalarModel m2) {
        // Compare by min bin edge as primary key
        double[] edges1 = m1.getBinEdges();
        double[] edges2 = m2.getBinEdges();

        if (edges1.length == 0 && edges2.length == 0) return 0;
        if (edges1.length == 0) return -1;
        if (edges2.length == 0) return 1;

        return compareDouble(edges1[0], edges2[0]);
    }

    private int compareDouble(double d1, double d2) {
        // Handle special values
        if (Double.isNaN(d1) && Double.isNaN(d2)) return 0;
        if (Double.isNaN(d1)) return 1;  // NaN sorts last
        if (Double.isNaN(d2)) return -1;

        // Treat very small differences as equal
        double diff = d1 - d2;
        if (Math.abs(diff) < EPSILON) return 0;

        return Double.compare(d1, d2);
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

    /**
     * Computes a characteristic value for ordering a model by its "location".
     *
     * <p>This is useful for sorting composite components by where they
     * are centered in the distribution space.
     *
     * @param model the model
     * @return a characteristic location value (typically mean or mode)
     */
    public static double getCharacteristicLocation(ScalarModel model) {
        if (model instanceof NormalScalarModel normal) {
            return normal.getMean();
        } else if (model instanceof UniformScalarModel uniform) {
            return (uniform.getLower() + uniform.getUpper()) / 2.0;
        } else if (model instanceof BetaScalarModel beta) {
            // Mode of Beta distribution (when alpha, beta > 1)
            double alpha = beta.getAlpha();
            double b = beta.getBeta();
            if (alpha > 1 && b > 1) {
                double mode01 = (alpha - 1) / (alpha + b - 2);
                return beta.getLower() + mode01 * (beta.getUpper() - beta.getLower());
            }
            // Mean as fallback
            double mean01 = alpha / (alpha + b);
            return beta.getLower() + mean01 * (beta.getUpper() - beta.getLower());
        } else if (model instanceof GammaScalarModel gamma) {
            return gamma.getShape() * gamma.getScale();  // Mean
        } else if (model instanceof StudentTScalarModel studentT) {
            return studentT.getLocation();
        } else if (model instanceof InverseGammaScalarModel invGamma) {
            if (invGamma.getShape() > 1) {
                return invGamma.getScale() / (invGamma.getShape() - 1);  // Mean
            }
            return invGamma.getScale() / invGamma.getShape();  // Approximate
        } else if (model instanceof BetaPrimeScalarModel betaPrime) {
            return betaPrime.getAlpha() / (betaPrime.getBeta() - 1);  // Mean when beta > 1
        } else if (model instanceof PearsonIVScalarModel pearsonIV) {
            return pearsonIV.getLambda();  // Location parameter
        } else if (model instanceof CompositeScalarModel composite) {
            // Weighted average of component locations
            ScalarModel[] components = composite.getScalarModels();
            double[] weights = composite.getWeights();
            double totalWeight = 0;
            double weightedSum = 0;
            for (int i = 0; i < components.length; i++) {
                double w = weights[i];
                totalWeight += w;
                weightedSum += w * getCharacteristicLocation(components[i]);
            }
            return totalWeight > 0 ? weightedSum / totalWeight : 0;
        } else if (model instanceof EmpiricalScalarModel empirical) {
            double[] edges = empirical.getBinEdges();
            if (edges.length >= 2) {
                return (edges[0] + edges[edges.length - 1]) / 2.0;
            }
            return 0;
        }
        return 0;
    }

    /**
     * Comparator that orders models by their characteristic location.
     *
     * <p>This is useful for sorting composite components by position
     * in the distribution space, independent of model type.
     */
    public static final Comparator<ScalarModel> BY_LOCATION =
        Comparator.comparingDouble(ScalarModelComparator::getCharacteristicLocation);
}
