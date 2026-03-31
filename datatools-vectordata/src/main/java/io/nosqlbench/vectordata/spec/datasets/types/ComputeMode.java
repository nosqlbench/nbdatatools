package io.nosqlbench.vectordata.spec.datasets.types;

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

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Set;

/// Runtime compute mode detection for SIMD acceleration.
///
/// Automatically detects the optimal compute mode based on:
/// - Java runtime version (Panama Vector API requires Java 25+)
/// - CPU instruction set support (AVX-512F, AVX2, AVX, SSE)
/// - Actual Panama Vector API availability
///
/// ## Compute Modes
///
/// | Mode | Description | Requirements |
/// |------|-------------|--------------|
/// | **PANAMA_AVX512F** | Full SIMD with 512-bit vectors | Java 25+, AVX-512F CPU |
/// | **PANAMA_AVX2** | SIMD with 256-bit vectors | Java 25+, AVX2 CPU |
/// | **PANAMA_SSE** | SIMD with 128-bit vectors | Java 25+, SSE2 CPU |
/// | **SCALAR** | Pure Java scalar operations | Any Java 11+ |
public final class ComputeMode {

    /// Available compute modes in order of preference (most efficient first).
    public enum Mode {
        /// Panama Vector API with AVX-512F Foundation (512-bit SIMD).
        PANAMA_AVX512F("Panama AVX-512F", "512-bit SIMD vectorization (AVX-512 Foundation)", 16),

        /// Panama Vector API with AVX2 (256-bit SIMD).
        PANAMA_AVX2("Panama AVX2", "256-bit SIMD vectorization", 8),

        /// Panama Vector API with SSE (128-bit SIMD).
        PANAMA_SSE("Panama SSE", "128-bit SIMD vectorization", 4),

        /// Pure Java scalar operations (no SIMD).
        SCALAR("Scalar", "Pure Java scalar operations", 1);

        private final String name;
        private final String description;
        private final int floatLanes;

        Mode(String name, String description, int floatLanes) {
            this.name = name;
            this.description = description;
            this.floatLanes = floatLanes;
        }

        /// Returns the display name of this mode.
        /// @return the display name
        public String displayName() { return name; }

        /// Returns a description of this mode.
        /// @return the description
        public String description() { return description; }

        /// Returns the number of float values processed per SIMD lane.
        /// @return the float lane count
        public int floatLanes() { return floatLanes; }

        /// Returns whether this mode uses Panama Vector API.
        /// @return true if this mode uses Panama
        public boolean usesPanama() { return this != SCALAR; }
    }

    private static final Set<String> cpuFlags = new HashSet<>();
    private static final boolean cpuFlagsInitialized;
    private static final boolean hasAVX512F;
    private static final boolean hasAVX2;
    private static final boolean hasAVX;
    private static final boolean panamaAvailable;
    private static final int javaVersion;
    private static final int preferredVectorBits;
    private static final Mode effectiveMode;

    static {
        javaVersion = Runtime.version().feature();

        boolean initialized = false;
        try {
            try (BufferedReader reader = new BufferedReader(new FileReader("/proc/cpuinfo"))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.startsWith("flags") || line.startsWith("Features")) {
                        String[] parts = line.split(":");
                        if (parts.length > 1) {
                            for (String flag : parts[1].trim().split("\\s+")) {
                                cpuFlags.add(flag.toLowerCase());
                            }
                        }
                    }
                }
            }
            initialized = true;
        } catch (Exception e) {
            // Not on Linux or can't read cpuinfo
        }
        cpuFlagsInitialized = initialized;

        hasAVX512F = cpuFlags.contains("avx512f");
        hasAVX2 = cpuFlags.contains("avx2");
        hasAVX = cpuFlags.contains("avx");

        boolean panama = false;
        int vectorBits = 0;
        try {
            Class<?> floatVectorClass = Class.forName("jdk.incubator.vector.FloatVector");
            Class<?> vectorSpeciesClass = Class.forName("jdk.incubator.vector.VectorSpecies");
            if (javaVersion >= 25) {
                Object speciesPreferred = floatVectorClass.getField("SPECIES_PREFERRED").get(null);
                vectorBits = (int) vectorSpeciesClass.getMethod("vectorBitSize").invoke(speciesPreferred);
                panama = true;
            }
        } catch (Exception e) {
            // Panama not available
        }
        panamaAvailable = panama;
        preferredVectorBits = vectorBits;

        if (!panamaAvailable) {
            effectiveMode = Mode.SCALAR;
        } else if (preferredVectorBits >= 512 && hasAVX512F) {
            effectiveMode = Mode.PANAMA_AVX512F;
        } else if (preferredVectorBits >= 256 && (hasAVX2 || hasAVX)) {
            effectiveMode = Mode.PANAMA_AVX2;
        } else if (preferredVectorBits >= 128) {
            effectiveMode = Mode.PANAMA_SSE;
        } else {
            effectiveMode = Mode.SCALAR;
        }
    }

    private ComputeMode() {}

    /// Returns the automatically detected optimal compute mode.
    /// @return the most efficient available compute mode
    public static Mode getEffectiveMode() { return effectiveMode; }

    /// Returns whether the Panama Vector API is available.
    /// @return true if Panama Vector API can be used
    public static boolean isPanamaAvailable() { return panamaAvailable; }
}
