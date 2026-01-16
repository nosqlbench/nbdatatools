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

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Set;

/// Runtime compute mode detection for vector space analysis.
///
/// ## Purpose
///
/// This class automatically detects the optimal compute mode based on:
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
///
/// ## AVX-512 Variants
///
/// AVX-512F (Foundation) is the base AVX-512 instruction set required for 512-bit
/// SIMD operations. This is distinct from other AVX-512 extensions:
/// - **AVX-512F** - Foundation (required for this mode)
/// - **AVX-512BW** - Byte and Word operations
/// - **AVX-512DQ** - Doubleword and Quadword operations
/// - **AVX-512VL** - Vector Length extensions
///
/// This implementation requires only AVX-512F, which is present on all AVX-512
/// capable CPUs (Skylake-X, Ice Lake, Zen 4, etc.).
///
/// ## Usage
///
/// ```java
/// // Get the automatically selected mode
/// ComputeMode.Mode mode = ComputeMode.getEffectiveMode();
/// System.out.println("Using: " + mode.description());
///
/// // Check specific capabilities
/// if (ComputeMode.supportsAVX512F()) {
///     System.out.println("AVX-512F acceleration available");
/// }
///
/// // Get detailed capability report
/// System.out.println(ComputeMode.getCapabilityReport());
/// ```
///
/// ## Performance Expectations
///
/// | Mode | Relative Performance | Use Case |
/// |------|---------------------|----------|
/// | PANAMA_AVX512F | ~8-16x scalar | Large vector datasets |
/// | PANAMA_AVX2 | ~4-8x scalar | Most modern CPUs |
/// | PANAMA_SSE | ~2-4x scalar | Older CPUs |
/// | SCALAR | 1x (baseline) | Fallback/compatibility |
public final class ComputeMode {

    /// Available compute modes in order of preference (most efficient first).
    public enum Mode {
        /// Panama Vector API with AVX-512F Foundation (512-bit SIMD, 16 floats/8 doubles per lane).
        ///
        /// Requires AVX-512F (Foundation) instruction set, which is the base requirement
        /// for 512-bit SIMD operations. Present on Skylake-X, Ice Lake, Zen 4, and later CPUs.
        PANAMA_AVX512F("Panama AVX-512F", "512-bit SIMD vectorization (AVX-512 Foundation)", 16),

        /// Panama Vector API with AVX2 (256-bit SIMD, 8 floats/4 doubles per lane).
        ///
        /// Requires AVX2 instruction set. Present on Haswell (2013) and later CPUs.
        PANAMA_AVX2("Panama AVX2", "256-bit SIMD vectorization", 8),

        /// Panama Vector API with SSE (128-bit SIMD, 4 floats/2 doubles per lane).
        ///
        /// Fallback for CPUs without AVX2 support. Uses SSE2/SSE4 instructions.
        PANAMA_SSE("Panama SSE", "128-bit SIMD vectorization", 4),

        /// Pure Java scalar operations (no SIMD).
        ///
        /// Used when Panama Vector API is not available (Java < 25) or
        /// when no suitable SIMD instructions are detected.
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
        public String displayName() {
            return name;
        }

        /// Returns a description of this mode.
        public String description() {
            return description;
        }

        /// Returns the number of float values processed per SIMD lane.
        public int floatLanes() {
            return floatLanes;
        }

        /// Returns whether this mode uses Panama Vector API.
        public boolean usesPanama() {
            return this != SCALAR;
        }

        /// Returns whether this mode uses AVX-512F.
        public boolean usesAVX512F() {
            return this == PANAMA_AVX512F;
        }

        /// @deprecated Use {@link #usesAVX512F()} instead.
        @Deprecated
        public boolean usesAVX512() {
            return usesAVX512F();
        }
    }

    // CPU capability flags (detected once at class load)
    private static final Set<String> cpuFlags = new HashSet<>();
    private static final boolean cpuFlagsInitialized;
    private static final boolean hasAVX512F;  // AVX-512 Foundation
    private static final boolean hasAVX2;
    private static final boolean hasAVX;

    // Panama availability (detected once at class load)
    private static final boolean panamaAvailable;
    private static final int javaVersion;
    private static final int preferredVectorBits;

    // Error message if Panama should be available but isn't enabled
    private static final String panamaNotEnabledError;

    // The effective mode for this JVM instance
    private static final Mode effectiveMode;

    static {
        // Detect Java version
        javaVersion = Runtime.version().feature();

        // Load CPU flags
        boolean initialized = false;
        try {
            loadCPUFlags();
            initialized = true;
        } catch (Exception e) {
            // Not on Linux or can't read cpuinfo - not fatal
        }
        cpuFlagsInitialized = initialized;

        // Check CPU capabilities
        // AVX-512F (Foundation) is the base instruction set for 512-bit SIMD
        hasAVX512F = cpuFlags.contains("avx512f");
        hasAVX2 = cpuFlags.contains("avx2");
        hasAVX = cpuFlags.contains("avx");

        // Check Panama availability
        boolean panama = false;
        int vectorBits = 0;
        String panamaError = null;
        try {
            Class<?> floatVectorClass = Class.forName("jdk.incubator.vector.FloatVector");
            // Also load VectorSpecies interface for proper method resolution
            Class<?> vectorSpeciesClass = Class.forName("jdk.incubator.vector.VectorSpecies");
            if (javaVersion >= 25) {
                // Get preferred species to determine actual vector width
                // SPECIES_PREFERRED is a static final field, not a method
                Object speciesPreferred = floatVectorClass.getField("SPECIES_PREFERRED").get(null);
                // Call vectorBitSize() on the public VectorSpecies interface, not the internal implementation
                vectorBits = (int) vectorSpeciesClass.getMethod("vectorBitSize").invoke(speciesPreferred);
                panama = true;
            }
        } catch (Exception e) {
            // Panama not available - check if it SHOULD be available
            if (javaVersion >= 25 && cpuFlagsInitialized && (hasAVX || hasAVX2 || hasAVX512F)) {
                // Java supports Panama and CPU has AVX, but Panama module not enabled
                String bestAvx = hasAVX512F ? "AVX-512F" : (hasAVX2 ? "AVX2" : "AVX");
                panamaError = String.format(
                    "Panama Vector API not enabled but should be available!%n%n" +
                    "Your system supports SIMD acceleration:%n" +
                    "  • Java version: %d (Panama supported)%n" +
                    "  • CPU SIMD capability: %s%n%n" +
                    "To enable Panama Vector API, add this JVM option:%n" +
                    "  --add-modules jdk.incubator.vector%n%n" +
                    "Example:%n" +
                    "  java --add-modules jdk.incubator.vector -jar your-app.jar%n%n" +
                    "Or set JAVA_TOOL_OPTIONS environment variable:%n" +
                    "  export JAVA_TOOL_OPTIONS=\"--add-modules jdk.incubator.vector\"%n%n" +
                    "Without Panama, vector operations will use scalar fallback (~%dx slower).",
                    javaVersion, bestAvx, hasAVX512F ? 16 : (hasAVX2 ? 8 : 4)
                );
            }
        }
        panamaAvailable = panama;
        preferredVectorBits = vectorBits;
        panamaNotEnabledError = panamaError;

        // Determine effective mode
        effectiveMode = determineEffectiveMode();
    }

    private ComputeMode() {
        // Utility class
    }

    /// Load CPU flags from /proc/cpuinfo (Linux) or system properties.
    private static void loadCPUFlags() throws Exception {
        // Try Linux /proc/cpuinfo first
        try (BufferedReader reader = new BufferedReader(new FileReader("/proc/cpuinfo"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("flags") || line.startsWith("Features")) {
                    String[] parts = line.split(":");
                    if (parts.length > 1) {
                        String[] flags = parts[1].trim().split("\\s+");
                        for (String flag : flags) {
                            cpuFlags.add(flag.toLowerCase());
                        }
                    }
                }
            }
        }
    }

    /// Determines the most efficient compute mode based on runtime capabilities.
    private static Mode determineEffectiveMode() {
        if (!panamaAvailable) {
            return Mode.SCALAR;
        }

        // Panama is available - determine vector width
        // AVX-512F (Foundation) is required for 512-bit operations
        if (preferredVectorBits >= 512 && hasAVX512F) {
            return Mode.PANAMA_AVX512F;
        } else if (preferredVectorBits >= 256 && (hasAVX2 || hasAVX)) {
            return Mode.PANAMA_AVX2;
        } else if (preferredVectorBits >= 128) {
            return Mode.PANAMA_SSE;
        } else {
            // Panama available but no useful vector width detected
            return Mode.SCALAR;
        }
    }

    /// Returns the automatically detected optimal compute mode for this runtime.
    ///
    /// The mode is determined once at class load time based on:
    /// - Java version (Panama requires Java 25+)
    /// - CPU instruction set capabilities
    /// - JVM's preferred vector species
    ///
    /// @return the most efficient available compute mode
    public static Mode getEffectiveMode() {
        return effectiveMode;
    }

    /// Returns whether the Panama Vector API is available in this runtime.
    ///
    /// @return true if Panama Vector API can be used
    public static boolean isPanamaAvailable() {
        return panamaAvailable;
    }

    /// Validates that Panama is enabled when it should be available.
    ///
    /// This method checks if:
    /// 1. CPU supports AVX instructions (detected from /proc/cpuinfo)
    /// 2. Java version is 25+ (supports Panama Vector API)
    /// 3. But Panama is not actually enabled
    ///
    /// If all conditions are met, this indicates the user forgot to add the
    /// required JVM option to enable the incubator module.
    ///
    /// This validation can be bypassed by setting the system property:
    /// {@code -Dcompute.panama.validation.skip=true}
    ///
    /// @throws IllegalStateException if Panama should be available but isn't enabled
    public static void validatePanamaEnabled() {
        // Allow bypassing validation via system property (for testing or explicit scalar mode)
        if (Boolean.getBoolean("compute.panama.validation.skip")) {
            return;
        }
        if (panamaNotEnabledError != null) {
            throw new IllegalStateException(panamaNotEnabledError);
        }
    }

    /// Returns whether Panama should be enabled but isn't.
    ///
    /// Use this for warning messages instead of throwing an error.
    ///
    /// @return true if Panama could be enabled but the JVM option is missing
    public static boolean isPanamaMisconfigured() {
        return panamaNotEnabledError != null;
    }

    /// Returns the error message explaining how to enable Panama, or null if not applicable.
    ///
    /// @return error message or null if Panama is properly configured (either enabled or not needed)
    public static String getPanamaMisconfigurationMessage() {
        return panamaNotEnabledError;
    }

    /// Returns whether AVX-512F (Foundation) instructions are available on this CPU.
    ///
    /// AVX-512F is the base instruction set for 512-bit SIMD operations,
    /// required for all AVX-512 capable CPUs.
    ///
    /// @return true if AVX-512F is supported
    public static boolean supportsAVX512F() {
        return hasAVX512F;
    }

    /// @deprecated Use {@link #supportsAVX512F()} instead for clarity.
    @Deprecated
    public static boolean supportsAVX512() {
        return supportsAVX512F();
    }

    /// Returns whether AVX2 instructions are available on this CPU.
    ///
    /// @return true if AVX2 is supported
    public static boolean supportsAVX2() {
        return hasAVX2;
    }

    /// Returns whether AVX instructions are available on this CPU.
    ///
    /// @return true if AVX is supported
    public static boolean supportsAVX() {
        return hasAVX;
    }

    /// Returns the Java runtime version.
    ///
    /// @return the Java feature version (e.g., 11, 17, 21, 25)
    public static int getJavaVersion() {
        return javaVersion;
    }

    /// Returns the preferred vector bit width reported by the JVM.
    ///
    /// @return vector bit width (512, 256, 128, or 0 if unavailable)
    public static int getPreferredVectorBits() {
        return preferredVectorBits;
    }

    /// Returns whether CPU capability detection succeeded.
    ///
    /// @return true if /proc/cpuinfo was successfully read
    public static boolean isCPUDetectionAvailable() {
        return cpuFlagsInitialized;
    }

    /// Returns the best SIMD capability string for display.
    ///
    /// @return human-readable SIMD capability description
    public static String getBestSIMDCapability() {
        if (hasAVX512F) {
            return "AVX-512F (512-bit SIMD)";
        } else if (hasAVX2) {
            return "AVX2 (256-bit SIMD)";
        } else if (hasAVX) {
            return "AVX (256-bit SIMD)";
        } else {
            return "SSE or lower";
        }
    }

    /// Generates a detailed capability report for diagnostics.
    ///
    /// @return multi-line string describing all detected capabilities
    public static String getCapabilityReport() {
        StringBuilder sb = new StringBuilder();
        sb.append("═══════════════════════════════════════════════════════════\n");
        sb.append("              COMPUTE MODE CAPABILITY REPORT               \n");
        sb.append("═══════════════════════════════════════════════════════════\n");
        sb.append("\n");

        // Runtime info
        sb.append("Runtime Environment:\n");
        sb.append(String.format("  Java Version:        %d (%s)\n", javaVersion, System.getProperty("java.version")));
        sb.append(String.format("  OS:                  %s %s\n", System.getProperty("os.name"), System.getProperty("os.arch")));
        sb.append("\n");

        // CPU capabilities
        sb.append("CPU Capabilities:\n");
        sb.append(String.format("  Detection Available: %s\n", cpuFlagsInitialized ? "Yes" : "No (non-Linux or restricted)"));
        if (cpuFlagsInitialized) {
            sb.append(String.format("  AVX-512F:            %s\n", hasAVX512F ? "✓ Supported" : "✗ Not available"));
            sb.append(String.format("  AVX2:                %s\n", hasAVX2 ? "✓ Supported" : "✗ Not available"));
            sb.append(String.format("  AVX:                 %s\n", hasAVX ? "✓ Supported" : "✗ Not available"));
            sb.append(String.format("  Best SIMD:           %s\n", getBestSIMDCapability()));
        }
        sb.append("\n");

        // Panama status
        sb.append("Panama Vector API:\n");
        sb.append(String.format("  Available:           %s\n", panamaAvailable ? "✓ Yes" : "✗ No (requires Java 25+)"));
        if (panamaAvailable) {
            sb.append(String.format("  Preferred Width:     %d bits\n", preferredVectorBits));
            sb.append(String.format("  Float Lanes:         %d per vector\n", preferredVectorBits / 32));
            sb.append(String.format("  Double Lanes:        %d per vector\n", preferredVectorBits / 64));
        }
        sb.append("\n");

        // Effective mode
        sb.append("Selected Mode:\n");
        sb.append(String.format("  Mode:                %s\n", effectiveMode.displayName()));
        sb.append(String.format("  Description:         %s\n", effectiveMode.description()));
        sb.append(String.format("  Float Throughput:    %dx scalar\n", effectiveMode.floatLanes()));
        sb.append("\n");

        // Performance expectations
        sb.append("Performance Expectations:\n");
        switch (effectiveMode) {
            case PANAMA_AVX512F:
                sb.append("  ▸ Maximum SIMD acceleration (8-16x for suitable workloads)\n");
                sb.append("  ▸ Best for large vector datasets (>10K vectors)\n");
                sb.append("  ▸ Optimal for batch distance computations\n");
                break;
            case PANAMA_AVX2:
                sb.append("  ▸ Good SIMD acceleration (4-8x for suitable workloads)\n");
                sb.append("  ▸ Effective for medium to large datasets\n");
                sb.append("  ▸ Common on most modern CPUs (2013+)\n");
                break;
            case PANAMA_SSE:
                sb.append("  ▸ Basic SIMD acceleration (2-4x for suitable workloads)\n");
                sb.append("  ▸ Fallback for older hardware\n");
                break;
            case SCALAR:
                sb.append("  ▸ No SIMD acceleration\n");
                sb.append("  ▸ Upgrade to Java 25+ for Panama Vector API support\n");
                break;
        }

        sb.append("═══════════════════════════════════════════════════════════\n");
        return sb.toString();
    }

    /// Returns a compact one-line summary of the compute mode.
    ///
    /// @return short description like "Panama AVX-512 (512-bit SIMD)"
    public static String getModeSummary() {
        if (effectiveMode == Mode.SCALAR) {
            return String.format("Scalar (Java %d)", javaVersion);
        } else {
            return String.format("%s (%d-bit vectors)", effectiveMode.displayName(), preferredVectorBits);
        }
    }

    @Override
    public String toString() {
        return getModeSummary();
    }
}
