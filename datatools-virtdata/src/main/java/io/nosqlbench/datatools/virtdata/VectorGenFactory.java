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

import io.nosqlbench.vshapes.model.VectorSpaceModel;

import java.util.function.LongFunction;

/**
 * Factory for creating DimensionDistributionGenerator instances with explicit implementation selection.
 *
 * <p>By default, the JVM's multi-release JAR mechanism automatically selects the
 * optimal implementation based on runtime version. This factory allows explicit
 * selection for testing, benchmarking, or when the automatic selection is not desired.
 */
public final class VectorGenFactory {

    /**
     * Implementation mode for vector generation.
     */
    public enum Mode {
        /**
         * Automatically select based on runtime (default multi-release JAR behavior).
         */
        AUTO,

        /**
         * Force the base Java 11 scalar implementation.
         */
        SCALAR,

        /**
         * Force the Panama Vector API implementation (requires Java 25+ runtime).
         */
        PANAMA
    }

    private VectorGenFactory() {
        // Utility class
    }

    /**
     * Creates a DimensionDistributionGenerator with automatic implementation selection.
     * @param model the vector space model
     * @return a DimensionDistributionGenerator instance
     */
    public static DimensionDistributionGenerator create(VectorSpaceModel model) {
        return new DimensionDistributionGenerator(model);
    }

    /**
     * Creates a generator with explicit implementation selection.
     *
     * @param model the vector space model
     * @param mode the implementation mode
     * @return a generator instance (scalar or auto based on mode)
     * @throws UnsupportedOperationException if PANAMA mode is requested but not available
     */
    public static LongFunction<float[]> create(VectorSpaceModel model, Mode mode) {
        switch (mode) {
            case AUTO:
                return new DimensionDistributionGenerator(model);
            case SCALAR:
                return new ScalarDimensionDistributionGenerator(model);
            case PANAMA:
                if (!isPanamaAvailable()) {
                    throw new UnsupportedOperationException(
                        "Panama Vector API not available. Requires Java 25+ runtime.");
                }
                return new DimensionDistributionGenerator(model);
            default:
                throw new IllegalArgumentException("Unknown mode: " + mode);
        }
    }

    /**
     * Checks if the Panama Vector API is available in the current runtime.
     * @return true if Panama is available
     */
    public static boolean isPanamaAvailable() {
        try {
            Class.forName("jdk.incubator.vector.FloatVector");
            return Runtime.version().feature() >= 25;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    /**
     * Returns the name of the implementation that would be used in AUTO mode.
     * @return "Panama" or "Scalar"
     */
    public static String getAutoImplementationName() {
        return isPanamaAvailable() ? "Panama" : "Scalar";
    }
}
