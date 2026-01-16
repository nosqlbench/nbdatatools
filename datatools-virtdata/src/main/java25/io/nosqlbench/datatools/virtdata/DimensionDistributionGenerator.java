package io.nosqlbench.datatools.virtdata;

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

import io.nosqlbench.datatools.virtdata.sampling.ComponentSampler;
import io.nosqlbench.datatools.virtdata.sampling.ComponentSamplerFactory;
import io.nosqlbench.vshapes.model.NormalScalarModel;
import io.nosqlbench.vshapes.model.VectorSpaceModel;
import jdk.incubator.vector.*;

/**
 * Panama Vector API optimized dimension distribution generator.
 *
 * <p>This Java 25+ implementation uses SIMD operations for batch vector generation,
 * providing significant speedups on CPUs with AVX2 or AVX-512 support.
 *
 * <p>Key optimizations:
 * <ul>
 *   <li>SIMD polynomial approximation for inverse normal CDF</li>
 *   <li>Vectorized log/sqrt using pure arithmetic (no scalar fallback)</li>
 *   <li>FMA (fused multiply-add) throughout for performance and precision</li>
 * </ul>
 *
 * <p>SIMD species selection is centralized via {@link LocalSpecies}.
 */
@GeneratorName("dimension-distribution")
@ModelType(VectorSpaceModel.class)
public class DimensionDistributionGenerator implements VectorGenerator<VectorSpaceModel> {

    private static final VectorSpecies<Double> SPECIES = LocalSpecies.doubleSpecies();
    private static final int LANES = SPECIES.length();

    // Abramowitz-Stegun 26.2.23 coefficients for inverse normal CDF
    private static final double C0 = 2.515517;
    private static final double C1 = 0.802853;
    private static final double C2 = 0.010328;
    private static final double D1 = 1.432788;
    private static final double D2 = 0.189269;
    private static final double D3 = 0.001308;

    // Log polynomial coefficients (Remez minimax approximation for log(1+x) on [0,1])
    // log(1+x) ≈ x * (L0 + x*(L1 + x*(L2 + x*(L3 + x*L4))))
    private static final double L0 = 0.9999999999999998;
    private static final double L1 = -0.4999999999532199;
    private static final double L2 = 0.33333332916609;
    private static final double L3 = -0.24999845065233;
    private static final double L4 = 0.20012028055456;
    private static final double LN2 = 0.6931471805599453;

    private VectorSpaceModel model;
    private int dimensions;
    private long uniqueVectors;
    private ComponentSampler[] samplers;
    private boolean allNormal;
    private double[] means;
    private double[] stdDevs;

    /** Tracks whether this generator has been initialized. */
    private boolean initialized = false;

    /**
     * Default constructor for ServiceLoader discovery.
     * Must call {@link #initialize(VectorSpaceModel)} before use.
     */
    public DimensionDistributionGenerator() {
    }

    /**
     * Constructs a Panama-optimized dimension distribution generator.
     * @param model the vector space model defining N, M, and per-component distributions
     */
    public DimensionDistributionGenerator(VectorSpaceModel model) {
        initialize(model);
    }

    @Override
    public String getGeneratorType() {
        return "dimension-distribution";
    }

    @Override
    public String getDescription() {
        return "Panama Vector API optimized generator using per-dimension distribution sampling with SIMD";
    }

    @Override
    public void initialize(VectorSpaceModel model) {
        if (this.initialized) {
            throw new IllegalStateException("Generator already initialized");
        }
        this.model = model;
        this.dimensions = model.dimensions();
        this.uniqueVectors = model.uniqueVectors();
        this.allNormal = model.isAllNormal();
        this.samplers = ComponentSamplerFactory.forModels(model.scalarModels());

        // Extract means/stdDevs for SIMD optimization (only used if allNormal)
        this.means = new double[dimensions];
        this.stdDevs = new double[dimensions];

        if (allNormal) {
            NormalScalarModel[] normals = model.normalScalarModels();
            for (int d = 0; d < dimensions; d++) {
                means[d] = normals[d].getMean();
                stdDevs[d] = normals[d].getStdDev();
            }
        }
        this.initialized = true;
    }

    @Override
    public boolean isInitialized() {
        return initialized;
    }

    @Override
    public float[] apply(long ordinal) {
        float[] result = new float[dimensions];
        generateInto(ordinal, result, 0);
        return result;
    }

    /**
     * Generates a vector into an existing array using SIMD when all components are normal.
     */
    @Override
    public void generateInto(long ordinal, float[] target, int offset) {
        long normalizedOrdinal = normalizeOrdinal(ordinal);

        if (allNormal) {
            // Use SIMD path for Gaussian components
            int d = 0;
            int upperBound = dimensions - (dimensions % LANES);

            // SIMD loop
            for (; d < upperBound; d += LANES) {
                double[] unitValues = new double[LANES];
                for (int lane = 0; lane < LANES; lane++) {
                    unitValues[lane] = StratifiedSampler.unitIntervalValue(normalizedOrdinal, d + lane, uniqueVectors);
                }

                DoubleVector uVec = DoubleVector.fromArray(SPECIES, unitValues, 0);
                DoubleVector meanVec = DoubleVector.fromArray(SPECIES, means, d);
                DoubleVector stdDevVec = DoubleVector.fromArray(SPECIES, stdDevs, d);

                DoubleVector result_vec = simdInverseNormalCDF(uVec, meanVec, stdDevVec);

                for (int lane = 0; lane < LANES; lane++) {
                    target[offset + d + lane] = (float) result_vec.lane(lane);
                }
            }

            // Scalar tail for normal
            for (; d < dimensions; d++) {
                double u = StratifiedSampler.unitIntervalValue(normalizedOrdinal, d, uniqueVectors);
                target[offset + d] = (float) samplers[d].sample(u);
            }
        } else {
            // Scalar path for heterogeneous component models
            for (int d = 0; d < dimensions; d++) {
                double u = StratifiedSampler.unitIntervalValue(normalizedOrdinal, d, uniqueVectors);
                target[offset + d] = (float) samplers[d].sample(u);
            }
        }
    }

    /**
     * Generates a vector as double precision.
     */
    @Override
    public double[] applyAsDouble(long ordinal) {
        double[] result = new double[dimensions];
        generateIntoDouble(ordinal, result, 0);
        return result;
    }

    /**
     * Generates a vector into an existing double array using SIMD when all components are normal.
     */
    @Override
    public void generateIntoDouble(long ordinal, double[] target, int offset) {
        long normalizedOrdinal = normalizeOrdinal(ordinal);

        if (allNormal) {
            int d = 0;
            int upperBound = dimensions - (dimensions % LANES);

            for (; d < upperBound; d += LANES) {
                double[] unitValues = new double[LANES];
                for (int lane = 0; lane < LANES; lane++) {
                    unitValues[lane] = StratifiedSampler.unitIntervalValue(normalizedOrdinal, d + lane, uniqueVectors);
                }

                DoubleVector uVec = DoubleVector.fromArray(SPECIES, unitValues, 0);
                DoubleVector meanVec = DoubleVector.fromArray(SPECIES, means, d);
                DoubleVector stdDevVec = DoubleVector.fromArray(SPECIES, stdDevs, d);

                DoubleVector result_vec = simdInverseNormalCDF(uVec, meanVec, stdDevVec);
                result_vec.intoArray(target, offset + d);
            }

            for (; d < dimensions; d++) {
                double u = StratifiedSampler.unitIntervalValue(normalizedOrdinal, d, uniqueVectors);
                target[offset + d] = samplers[d].sample(u);
            }
        } else {
            for (int d = 0; d < dimensions; d++) {
                double u = StratifiedSampler.unitIntervalValue(normalizedOrdinal, d, uniqueVectors);
                target[offset + d] = samplers[d].sample(u);
            }
        }
    }

    /**
     * Generates a batch of vectors.
     */
    @Override
    public float[][] generateBatch(long startOrdinal, int count) {
        float[][] result = new float[count][dimensions];
        float[] flat = generateFlatBatch(startOrdinal, count);
        for (int i = 0; i < count; i++) {
            System.arraycopy(flat, i * dimensions, result[i], 0, dimensions);
        }
        return result;
    }

    /**
     * Generates a flat batch of vectors using SIMD optimization when all components are normal.
     */
    @Override
    public float[] generateFlatBatch(long startOrdinal, int count) {
        float[] result = new float[count * dimensions];

        for (int v = 0; v < count; v++) {
            long ordinal = startOrdinal + v;
            long normalizedOrdinal = normalizeOrdinal(ordinal);
            int baseOffset = v * dimensions;

            if (allNormal) {
                int d = 0;
                int upperBound = dimensions - (dimensions % LANES);

                for (; d < upperBound; d += LANES) {
                    double[] unitValues = new double[LANES];
                    for (int lane = 0; lane < LANES; lane++) {
                        unitValues[lane] = StratifiedSampler.unitIntervalValue(normalizedOrdinal, d + lane, uniqueVectors);
                    }

                    DoubleVector uVec = DoubleVector.fromArray(SPECIES, unitValues, 0);
                    DoubleVector meanVec = DoubleVector.fromArray(SPECIES, means, d);
                    DoubleVector stdDevVec = DoubleVector.fromArray(SPECIES, stdDevs, d);

                    DoubleVector transformed = simdInverseNormalCDF(uVec, meanVec, stdDevVec);

                    for (int lane = 0; lane < LANES; lane++) {
                        result[baseOffset + d + lane] = (float) transformed.lane(lane);
                    }
                }

                for (; d < dimensions; d++) {
                    double u = StratifiedSampler.unitIntervalValue(normalizedOrdinal, d, uniqueVectors);
                    result[baseOffset + d] = (float) samplers[d].sample(u);
                }
            } else {
                for (int d = 0; d < dimensions; d++) {
                    double u = StratifiedSampler.unitIntervalValue(normalizedOrdinal, d, uniqueVectors);
                    result[baseOffset + d] = (float) samplers[d].sample(u);
                }
            }
        }

        return result;
    }

    /**
     * SIMD inverse normal CDF using pure arithmetic operations.
     * All operations (log, sqrt) are implemented as vectorized polynomial approximations
     * that use only FMA/add/mul/div - no scalar fallbacks.
     */
    private DoubleVector simdInverseNormalCDF(DoubleVector u, DoubleVector mean, DoubleVector stdDev) {
        var half = DoubleVector.broadcast(SPECIES, 0.5);
        var one = DoubleVector.broadcast(SPECIES, 1.0);
        var two = DoubleVector.broadcast(SPECIES, 2.0);

        // Determine tail: lower if u < 0.5
        var isLowerTail = u.lt(half);

        // p = u for lower tail, p = 1-u for upper tail (always use smaller value for precision)
        var p = one.sub(u).blend(u, isLowerTail);

        // Compute t = sqrt(-2 * ln(p)) using vectorized approximations
        var logP = simdLog(p);
        var minusTwoLogP = logP.mul(two).neg();
        var t = simdSqrt(minusTwoLogP);

        // Rational polynomial approximation (Abramowitz-Stegun 26.2.23)
        var c0 = DoubleVector.broadcast(SPECIES, C0);
        var c1 = DoubleVector.broadcast(SPECIES, C1);
        var c2 = DoubleVector.broadcast(SPECIES, C2);
        var d1 = DoubleVector.broadcast(SPECIES, D1);
        var d2 = DoubleVector.broadcast(SPECIES, D2);
        var d3 = DoubleVector.broadcast(SPECIES, D3);

        // Horner's method for numerator: c0 + t*(c1 + t*c2)
        var num = c2.fma(t, c1).fma(t, c0);

        // Horner's method for denominator: 1 + t*(d1 + t*(d2 + t*d3))
        var den = d3.fma(t, d2).fma(t, d1).fma(t, one);

        var z = t.sub(num.div(den));

        // Apply sign
        var negZ = z.neg();
        var signedZ = z.blend(negZ, isLowerTail);

        // Scale: mean + stdDev * z
        return stdDev.fma(signedZ, mean);
    }

    /**
     * Vectorized natural logarithm using polynomial approximation.
     * Uses range reduction: log(x) = log(m * 2^e) = log(m) + e*ln(2)
     * where m is in [1, 2), approximated with a minimax polynomial.
     */
    private DoubleVector simdLog(DoubleVector x) {
        // Extract exponent and mantissa using bit manipulation
        // For IEEE-754 double: bits = sign(1) | exponent(11) | mantissa(52)
        var bits = x.reinterpretAsLongs();

        // Exponent bias for double is 1023
        var expBias = LongVector.broadcast(bits.species(), 1023L);
        var mantissaMask = LongVector.broadcast(bits.species(), 0x000FFFFFFFFFFFFFL);
        var expMask = LongVector.broadcast(bits.species(), 0x7FF0000000000000L);

        // Extract biased exponent, unbias it
        var biasedExp = bits.and(expMask).lanewise(VectorOperators.LSHR, 52);
        var exp = biasedExp.sub(expBias);

        // Extract mantissa and normalize to [1, 2)
        var mantissaBits = bits.and(mantissaMask).or(LongVector.broadcast(bits.species(), 0x3FF0000000000000L));
        var m = mantissaBits.reinterpretAsDoubles();

        // Now m is in [1, 2), compute log(m) using polynomial
        // log(m) = log(1 + (m-1)) where (m-1) is in [0, 1)
        var one = DoubleVector.broadcast(SPECIES, 1.0);
        var y = m.sub(one);  // y in [0, 1)

        // Polynomial approximation for log(1+y)
        // log(1+y) ≈ y * (L0 + y*(L1 + y*(L2 + y*(L3 + y*L4))))
        var l0 = DoubleVector.broadcast(SPECIES, L0);
        var l1 = DoubleVector.broadcast(SPECIES, L1);
        var l2 = DoubleVector.broadcast(SPECIES, L2);
        var l3 = DoubleVector.broadcast(SPECIES, L3);
        var l4 = DoubleVector.broadcast(SPECIES, L4);
        var ln2 = DoubleVector.broadcast(SPECIES, LN2);

        // Horner's method
        var poly = l4.fma(y, l3).fma(y, l2).fma(y, l1).fma(y, l0);
        var logM = y.mul(poly);

        // log(x) = log(m) + e * ln(2)
        var expDouble = (DoubleVector) exp.convert(VectorOperators.L2D, 0);
        return expDouble.fma(ln2, logM);
    }

    /**
     * Vectorized square root using Newton-Raphson iteration.
     * Uses fast inverse square root as starting point.
     */
    private DoubleVector simdSqrt(DoubleVector x) {
        // Newton-Raphson: y_{n+1} = 0.5 * (y_n + x/y_n)
        // Start with a reasonable initial guess using bit manipulation

        var half = DoubleVector.broadcast(SPECIES, 0.5);
        var threeHalves = DoubleVector.broadcast(SPECIES, 1.5);

        // Fast inverse sqrt approximation (Quake-style but for double)
        // Initial guess: take bits, subtract from magic constant, shift
        var bits = x.reinterpretAsLongs();
        // Magic constant for double: 0x5FE6EB50C7B537A9 (Lomont's constant)
        var magic = LongVector.broadcast(bits.species(), 0x5FE6EB50C7B537A9L);
        var guess_bits = magic.sub(bits.lanewise(VectorOperators.LSHR, 1));
        var invSqrt = guess_bits.reinterpretAsDoubles();

        // Two Newton-Raphson iterations for inverse sqrt: y = y * (1.5 - 0.5*x*y*y)
        var halfX = x.mul(half);
        invSqrt = invSqrt.mul(threeHalves.sub(halfX.mul(invSqrt).mul(invSqrt)));
        invSqrt = invSqrt.mul(threeHalves.sub(halfX.mul(invSqrt).mul(invSqrt)));

        // sqrt(x) = x * invSqrt(x)
        return x.mul(invSqrt);
    }

    @Override
    public VectorSpaceModel model() {
        return model;
    }

    @Override
    public int dimensions() {
        return dimensions;
    }

    @Override
    public long uniqueVectors() {
        return uniqueVectors;
    }

    private long normalizeOrdinal(long ordinal) {
        long normalized = ordinal % uniqueVectors;
        return normalized < 0 ? normalized + uniqueVectors : normalized;
    }
}
