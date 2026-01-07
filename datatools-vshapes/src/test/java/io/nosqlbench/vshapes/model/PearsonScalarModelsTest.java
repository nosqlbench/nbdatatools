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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Pearson distribution ScalarModel implementations.
 */
@Tag("unit")
public class PearsonScalarModelsTest {

    // ===== BetaScalarModel Tests (Type I/II) =====

    @Test
    void betaScalarModelBasicConstruction() {
        BetaScalarModel beta = new BetaScalarModel(2.0, 5.0);
        assertEquals("beta", beta.getModelType());
        assertEquals(2.0, beta.getAlpha());
        assertEquals(5.0, beta.getBeta());
        assertEquals(0.0, beta.getLower());
        assertEquals(1.0, beta.getUpper());
    }

    @Test
    void betaScalarModelCustomBounds() {
        BetaScalarModel beta = new BetaScalarModel(2.0, 5.0, -1.0, 1.0);
        assertEquals(-1.0, beta.getLower());
        assertEquals(1.0, beta.getUpper());
        assertEquals(2.0, beta.getRange());
    }

    @Test
    void betaScalarModelMoments() {
        BetaScalarModel beta = new BetaScalarModel(2.0, 5.0);
        // For Beta(2,5) on [0,1]: mean = 2/7 ≈ 0.286
        assertEquals(2.0 / 7.0, beta.getMean(), 0.001);
        assertTrue(beta.getVariance() > 0);
        assertTrue(beta.getStdDev() > 0);
    }

    @Test
    void betaScalarModelSymmetry() {
        BetaScalarModel symmetric = new BetaScalarModel(3.0, 3.0);
        assertTrue(symmetric.isSymmetric());
        assertEquals(PearsonType.TYPE_II_SYMMETRIC_BETA, symmetric.getPearsonType());

        BetaScalarModel asymmetric = new BetaScalarModel(2.0, 5.0);
        assertFalse(asymmetric.isSymmetric());
        assertEquals(PearsonType.TYPE_I_BETA, asymmetric.getPearsonType());
    }

    @Test
    void betaScalarModelFactoryMethods() {
        BetaScalarModel uniform = BetaScalarModel.uniform();
        assertEquals(1.0, uniform.getAlpha());
        assertEquals(1.0, uniform.getBeta());

        BetaScalarModel symmetric = BetaScalarModel.symmetric(3.0);
        assertTrue(symmetric.isSymmetric());
    }

    @Test
    void betaScalarModelInvalidParameters() {
        assertThrows(IllegalArgumentException.class, () -> new BetaScalarModel(0, 1));
        assertThrows(IllegalArgumentException.class, () -> new BetaScalarModel(1, 0));
        assertThrows(IllegalArgumentException.class, () -> new BetaScalarModel(1, 1, 1, 0));
    }

    // ===== GammaScalarModel Tests (Type III) =====

    @Test
    void gammaScalarModelBasicConstruction() {
        GammaScalarModel gamma = new GammaScalarModel(2.0, 3.0);
        assertEquals("gamma", gamma.getModelType());
        assertEquals(2.0, gamma.getShape());
        assertEquals(3.0, gamma.getScale());
        assertEquals(0.0, gamma.getLocation());
    }

    @Test
    void gammaScalarModelWithLocation() {
        GammaScalarModel gamma = new GammaScalarModel(2.0, 3.0, 5.0);
        assertEquals(5.0, gamma.getLocation());
        assertEquals(5.0, gamma.getLower());
    }

    @Test
    void gammaScalarModelMoments() {
        GammaScalarModel gamma = new GammaScalarModel(4.0, 2.0);
        // Mean = k * θ = 4 * 2 = 8
        assertEquals(8.0, gamma.getMean(), 0.001);
        // Variance = k * θ² = 4 * 4 = 16
        assertEquals(16.0, gamma.getVariance(), 0.001);
        // Skewness = 2/√k = 2/2 = 1
        assertEquals(1.0, gamma.getSkewness(), 0.001);
    }

    @Test
    void gammaScalarModelExponential() {
        GammaScalarModel exponential = GammaScalarModel.exponential(5.0);
        assertTrue(exponential.isExponential());
        assertEquals(1.0, exponential.getShape());
        assertEquals(5.0, exponential.getScale());
    }

    @Test
    void gammaScalarModelChiSquared() {
        GammaScalarModel chi2 = GammaScalarModel.chiSquared(6);
        assertEquals(3.0, chi2.getShape());
        assertEquals(2.0, chi2.getScale());
    }

    @Test
    void gammaScalarModelFromMeanVariance() {
        GammaScalarModel gamma = GammaScalarModel.fromMeanVariance(8.0, 16.0);
        assertEquals(8.0, gamma.getMean(), 0.001);
        assertEquals(16.0, gamma.getVariance(), 0.001);
    }

    @Test
    void gammaScalarModelInvalidParameters() {
        assertThrows(IllegalArgumentException.class, () -> new GammaScalarModel(0, 1));
        assertThrows(IllegalArgumentException.class, () -> new GammaScalarModel(1, 0));
    }

    // ===== StudentTScalarModel Tests (Type VII) =====

    @Test
    void studentTScalarModelBasicConstruction() {
        StudentTScalarModel t = new StudentTScalarModel(5.0);
        assertEquals("student-t", t.getModelType());
        assertEquals(5.0, t.getDegreesOfFreedom());
        assertEquals(0.0, t.getLocation());
        assertEquals(1.0, t.getScale());
    }

    @Test
    void studentTScalarModelLocationScale() {
        StudentTScalarModel t = new StudentTScalarModel(10.0, 5.0, 2.0);
        assertEquals(5.0, t.getLocation());
        assertEquals(2.0, t.getScale());
        assertEquals(5.0, t.getMean(), 0.001);
    }

    @Test
    void studentTScalarModelMoments() {
        StudentTScalarModel t = new StudentTScalarModel(10.0);
        assertEquals(0.0, t.getMean(), 0.001);
        // Variance = ν/(ν-2) = 10/8 = 1.25
        assertEquals(1.25, t.getVariance(), 0.001);
        assertEquals(0.0, t.getSkewness(), 0.001);
    }

    @Test
    void studentTScalarModelCauchy() {
        StudentTScalarModel cauchy = StudentTScalarModel.cauchy();
        assertTrue(cauchy.isCauchy());
        assertEquals(1.0, cauchy.getDegreesOfFreedom());
        assertFalse(cauchy.hasMean());
        assertTrue(Double.isNaN(cauchy.getMean()));
    }

    @Test
    void studentTScalarModelFiniteMoments() {
        StudentTScalarModel t2 = new StudentTScalarModel(2.0);
        assertTrue(t2.hasMean());
        assertFalse(t2.hasFiniteVariance());

        StudentTScalarModel t5 = new StudentTScalarModel(5.0);
        assertTrue(t5.hasMean());
        assertTrue(t5.hasFiniteVariance());
        assertTrue(t5.hasFiniteKurtosis());
    }

    @Test
    void studentTScalarModelWithExcessKurtosis() {
        StudentTScalarModel t = StudentTScalarModel.withExcessKurtosis(1.0);
        // ν = 4 + 6/1 = 10
        assertEquals(10.0, t.getDegreesOfFreedom(), 0.001);
    }

    @Test
    void studentTScalarModelInvalidParameters() {
        assertThrows(IllegalArgumentException.class, () -> new StudentTScalarModel(0));
        assertThrows(IllegalArgumentException.class, () -> new StudentTScalarModel(5, 0, 0));
    }

    // ===== InverseGammaScalarModel Tests (Type V) =====

    @Test
    void inverseGammaScalarModelBasicConstruction() {
        InverseGammaScalarModel ig = new InverseGammaScalarModel(3.0, 2.0);
        assertEquals("inverse-gamma", ig.getModelType());
        assertEquals(3.0, ig.getShape());
        assertEquals(2.0, ig.getScale());
    }

    @Test
    void inverseGammaScalarModelMoments() {
        InverseGammaScalarModel ig = new InverseGammaScalarModel(4.0, 6.0);
        // Mean = β/(α-1) = 6/3 = 2
        assertEquals(2.0, ig.getMean(), 0.001);
        assertTrue(ig.hasFiniteMean());
        assertTrue(ig.hasFiniteVariance());
    }

    @Test
    void inverseGammaScalarModelMode() {
        InverseGammaScalarModel ig = new InverseGammaScalarModel(3.0, 2.0);
        // Mode = β/(α+1) = 2/4 = 0.5
        assertEquals(0.5, ig.getMode(), 0.001);
    }

    @Test
    void inverseGammaScalarModelInverseChiSquared() {
        InverseGammaScalarModel invChi2 = InverseGammaScalarModel.inverseChiSquared(6);
        assertEquals(3.0, invChi2.getShape());
        assertEquals(0.5, invChi2.getScale());
    }

    @Test
    void inverseGammaScalarModelInvalidParameters() {
        assertThrows(IllegalArgumentException.class, () -> new InverseGammaScalarModel(0, 1));
        assertThrows(IllegalArgumentException.class, () -> new InverseGammaScalarModel(1, 0));
    }

    // ===== BetaPrimeScalarModel Tests (Type VI) =====

    @Test
    void betaPrimeScalarModelBasicConstruction() {
        BetaPrimeScalarModel bp = new BetaPrimeScalarModel(2.0, 4.0);
        assertEquals("beta-prime", bp.getModelType());
        assertEquals(2.0, bp.getAlpha());
        assertEquals(4.0, bp.getBeta());
        assertEquals(1.0, bp.getScale());
    }

    @Test
    void betaPrimeScalarModelMoments() {
        BetaPrimeScalarModel bp = new BetaPrimeScalarModel(2.0, 5.0);
        // Mean = α/(β-1) = 2/4 = 0.5
        assertEquals(0.5, bp.getMean(), 0.001);
        assertTrue(bp.hasFiniteMean());
        assertTrue(bp.hasFiniteVariance());
    }

    @Test
    void betaPrimeScalarModelFDistribution() {
        BetaPrimeScalarModel f = BetaPrimeScalarModel.fDistribution(4, 6);
        assertEquals(2.0, f.getAlpha());
        assertEquals(3.0, f.getBeta());
        assertEquals(1.5, f.getScale());
    }

    @Test
    void betaPrimeScalarModelInvalidParameters() {
        assertThrows(IllegalArgumentException.class, () -> new BetaPrimeScalarModel(0, 1));
        assertThrows(IllegalArgumentException.class, () -> new BetaPrimeScalarModel(1, 0));
        assertThrows(IllegalArgumentException.class, () -> new BetaPrimeScalarModel(1, 1, 0));
    }

    // ===== PearsonIVScalarModel Tests (Type IV) =====

    @Test
    void pearsonIVScalarModelBasicConstruction() {
        PearsonIVScalarModel p4 = new PearsonIVScalarModel(2.0, 1.0, 1.0, 0.0);
        assertEquals("pearson-iv", p4.getModelType());
        assertEquals(2.0, p4.getM());
        assertEquals(1.0, p4.getNu());
        assertEquals(1.0, p4.getA());
        assertEquals(0.0, p4.getLambda());
    }

    @Test
    void pearsonIVScalarModelSymmetric() {
        PearsonIVScalarModel p4 = PearsonIVScalarModel.symmetric(2.0, 1.0, 0.0);
        assertTrue(p4.isSymmetric());
        assertEquals(0.0, p4.getNu());
    }

    @Test
    void pearsonIVScalarModelStandard() {
        PearsonIVScalarModel p4 = PearsonIVScalarModel.standard(2.0, 1.0);
        assertEquals(1.0, p4.getA());
        assertEquals(0.0, p4.getLambda());
    }

    @Test
    void pearsonIVScalarModelMean() {
        PearsonIVScalarModel p4 = new PearsonIVScalarModel(3.0, 1.0, 1.0, 0.0);
        assertTrue(p4.hasMean());
        // Mean = λ + a*ν/(2m-2) = 0 + 1*1/(4) = 0.25
        assertEquals(0.25, p4.getMean(), 0.001);
    }

    @Test
    void pearsonIVScalarModelInvalidParameters() {
        assertThrows(IllegalArgumentException.class, () -> new PearsonIVScalarModel(0.4, 1, 1, 0));
        assertThrows(IllegalArgumentException.class, () -> new PearsonIVScalarModel(2, 1, 0, 0));
    }

    // ===== UniformScalar Factory Tests =====

    @Test
    void uniformScalarArrayFactories() {
        BetaScalarModel[] betaArray = BetaScalarModel.uniformScalar(2.0, 5.0, 4);
        assertEquals(4, betaArray.length);
        for (BetaScalarModel m : betaArray) {
            assertEquals(2.0, m.getAlpha());
        }

        GammaScalarModel[] gammaArray = GammaScalarModel.uniformScalar(2.0, 3.0, 3);
        assertEquals(3, gammaArray.length);

        StudentTScalarModel[] tArray = StudentTScalarModel.uniformScalar(5.0, 2);
        assertEquals(2, tArray.length);

        InverseGammaScalarModel[] igArray = InverseGammaScalarModel.uniformScalar(3.0, 2.0, 5);
        assertEquals(5, igArray.length);

        BetaPrimeScalarModel[] bpArray = BetaPrimeScalarModel.uniformScalar(2.0, 4.0, 3);
        assertEquals(3, bpArray.length);

        PearsonIVScalarModel[] p4Array = PearsonIVScalarModel.uniformScalar(2.0, 1.0, 1.0, 0.0, 4);
        assertEquals(4, p4Array.length);
    }

    // ===== Equals and HashCode Tests =====

    @Test
    void scalarModelsEqualsAndHashCode() {
        BetaScalarModel b1 = new BetaScalarModel(2.0, 5.0);
        BetaScalarModel b2 = new BetaScalarModel(2.0, 5.0);
        BetaScalarModel b3 = new BetaScalarModel(2.0, 6.0);

        assertEquals(b1, b2);
        assertEquals(b1.hashCode(), b2.hashCode());
        assertNotEquals(b1, b3);

        GammaScalarModel g1 = new GammaScalarModel(2.0, 3.0);
        GammaScalarModel g2 = new GammaScalarModel(2.0, 3.0);
        assertEquals(g1, g2);

        StudentTScalarModel t1 = new StudentTScalarModel(5.0);
        StudentTScalarModel t2 = new StudentTScalarModel(5.0);
        assertEquals(t1, t2);
    }

    // ===== ToString Tests =====

    @Test
    void scalarModelsToString() {
        assertNotNull(new BetaScalarModel(2.0, 5.0).toString());
        assertNotNull(new GammaScalarModel(2.0, 3.0).toString());
        assertNotNull(new StudentTScalarModel(5.0).toString());
        assertNotNull(new InverseGammaScalarModel(3.0, 2.0).toString());
        assertNotNull(new BetaPrimeScalarModel(2.0, 4.0).toString());
        assertNotNull(new PearsonIVScalarModel(2.0, 1.0, 1.0, 0.0).toString());
    }
}
