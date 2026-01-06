package io.nosqlbench.vshapes;

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

import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorSpecies;

/**
 * Panama Vector API species selection based on {@link ComputeMode}.
 *
 * <p>This Java 25+ helper class provides the optimal {@link VectorSpecies} for
 * the detected compute mode. It centralizes species selection logic that was
 * previously duplicated across multiple SIMD implementations.
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Get optimal species for float operations
 * VectorSpecies<Float> floatSpecies = ComputeModeSpecies.floatSpecies();
 * int lanes = floatSpecies.length();  // 16 for AVX-512F, 8 for AVX2, 4 for SSE
 *
 * // Get optimal species for double operations
 * VectorSpecies<Double> doubleSpecies = ComputeModeSpecies.doubleSpecies();
 *
 * // Use in SIMD loops
 * for (int i = 0; i < upperBound; i += lanes) {
 *     var v = FloatVector.fromArray(floatSpecies, array, i);
 *     // ...
 * }
 * }</pre>
 *
 * <h2>Species Selection</h2>
 *
 * <table border="1">
 * <caption>Species by compute mode</caption>
 * <tr><th>Mode</th><th>Float Species</th><th>Double Species</th><th>Float Lanes</th><th>Double Lanes</th></tr>
 * <tr><td>PANAMA_AVX512F</td><td>SPECIES_512</td><td>SPECIES_512</td><td>16</td><td>8</td></tr>
 * <tr><td>PANAMA_AVX2</td><td>SPECIES_256</td><td>SPECIES_256</td><td>8</td><td>4</td></tr>
 * <tr><td>PANAMA_SSE</td><td>SPECIES_128</td><td>SPECIES_128</td><td>4</td><td>2</td></tr>
 * <tr><td>SCALAR</td><td>SPECIES_64</td><td>SPECIES_64</td><td>2</td><td>1</td></tr>
 * </table>
 *
 * @see ComputeMode
 */
public final class ComputeModeSpecies {

    // Cached species for performance (determined once at class load)
    private static final VectorSpecies<Float> FLOAT_SPECIES;
    private static final VectorSpecies<Double> DOUBLE_SPECIES;
    private static final int FLOAT_LANES;
    private static final int DOUBLE_LANES;

    static {
        ComputeMode.Mode mode = ComputeMode.getEffectiveMode();
        FLOAT_SPECIES = selectFloatSpecies(mode);
        DOUBLE_SPECIES = selectDoubleSpecies(mode);
        FLOAT_LANES = FLOAT_SPECIES.length();
        DOUBLE_LANES = DOUBLE_SPECIES.length();
    }

    private ComputeModeSpecies() {
        // Utility class
    }

    /**
     * Returns the optimal {@link VectorSpecies} for float operations.
     *
     * <p>The species is determined by {@link ComputeMode#getEffectiveMode()}
     * and cached for the lifetime of the JVM.
     *
     * @return the float vector species matching the effective compute mode
     */
    public static VectorSpecies<Float> floatSpecies() {
        return FLOAT_SPECIES;
    }

    /**
     * Returns the optimal {@link VectorSpecies} for double operations.
     *
     * <p>The species is determined by {@link ComputeMode#getEffectiveMode()}
     * and cached for the lifetime of the JVM.
     *
     * @return the double vector species matching the effective compute mode
     */
    public static VectorSpecies<Double> doubleSpecies() {
        return DOUBLE_SPECIES;
    }

    /**
     * Returns the number of float lanes in the optimal species.
     *
     * @return float lanes (16 for AVX-512F, 8 for AVX2, 4 for SSE, 2 for scalar)
     */
    public static int floatLanes() {
        return FLOAT_LANES;
    }

    /**
     * Returns the number of double lanes in the optimal species.
     *
     * @return double lanes (8 for AVX-512F, 4 for AVX2, 2 for SSE, 1 for scalar)
     */
    public static int doubleLanes() {
        return DOUBLE_LANES;
    }

    /**
     * Returns the vector bit size for the optimal species.
     *
     * @return bits (512, 256, 128, or 64)
     */
    public static int vectorBitSize() {
        return FLOAT_SPECIES.vectorBitSize();
    }

    /**
     * Returns a description of the selected species for logging/diagnostics.
     *
     * @return description like "FloatVector.SPECIES_512 (16 lanes)"
     */
    public static String getDescription() {
        return String.format("FloatVector species: %d-bit (%d lanes), DoubleVector species: %d-bit (%d lanes)",
            FLOAT_SPECIES.vectorBitSize(), FLOAT_LANES,
            DOUBLE_SPECIES.vectorBitSize(), DOUBLE_LANES);
    }

    private static VectorSpecies<Float> selectFloatSpecies(ComputeMode.Mode mode) {
        return switch (mode) {
            case PANAMA_AVX512F -> FloatVector.SPECIES_512;
            case PANAMA_AVX2 -> FloatVector.SPECIES_256;
            case PANAMA_SSE -> FloatVector.SPECIES_128;
            case SCALAR -> FloatVector.SPECIES_64;
        };
    }

    private static VectorSpecies<Double> selectDoubleSpecies(ComputeMode.Mode mode) {
        return switch (mode) {
            case PANAMA_AVX512F -> DoubleVector.SPECIES_512;
            case PANAMA_AVX2 -> DoubleVector.SPECIES_256;
            case PANAMA_SSE -> DoubleVector.SPECIES_128;
            case SCALAR -> DoubleVector.SPECIES_64;
        };
    }
}
