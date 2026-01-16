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

import io.nosqlbench.vshapes.ComputeMode;
import jdk.incubator.vector.*;

/**
 * Local VectorSpecies selection helper for this module.
 *
 * <p>Uses {@link ComputeMode} to determine the optimal species based on
 * CPU capabilities and selects the appropriate Panama Vector API species.
 *
 * <p>Note: This class is local to this module to avoid multi-release JAR
 * cross-module dependency issues. Each module that needs SIMD operations
 * should have its own local species helper.
 */
final class LocalSpecies {

    private static final VectorSpecies<Float> FLOAT_SPECIES;
    private static final VectorSpecies<Double> DOUBLE_SPECIES;

    static {
        ComputeMode.Mode mode = ComputeMode.getEffectiveMode();
        FLOAT_SPECIES = selectFloatSpecies(mode);
        DOUBLE_SPECIES = selectDoubleSpecies(mode);
    }

    private LocalSpecies() {
        // Utility class
    }

    /**
     * Returns the optimal VectorSpecies for float operations.
     */
    public static VectorSpecies<Float> floatSpecies() {
        return FLOAT_SPECIES;
    }

    /**
     * Returns the optimal VectorSpecies for double operations.
     */
    public static VectorSpecies<Double> doubleSpecies() {
        return DOUBLE_SPECIES;
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
