package io.nosqlbench.command.compute;

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
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Detects CPU instruction set capabilities for SIMD optimizations.
 * Reads /proc/cpuinfo on Linux to determine AVX-512, AVX2, etc support.
 */
public class ISACapabilityDetector {

    private static final Set<String> cpuFlags = new HashSet<>();
    private static boolean initialized = false;

    static {
        try {
            loadCPUFlags();
            initialized = true;
        } catch (Exception e) {
            // Not on Linux or can't read cpuinfo - not fatal
            initialized = false;
        }
    }

    /**
     * Load CPU flags from /proc/cpuinfo (Linux only).
     */
    private static void loadCPUFlags() throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader("/proc/cpuinfo"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("flags") || line.startsWith("Features")) {
                    // Extract flags after the colon
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

    /**
     * Check if AVX-512 is supported by the CPU.
     */
    public static boolean supportsAVX512() {
        if (!initialized) return false;
        // AVX-512 Foundation (F) is the base
        return cpuFlags.contains("avx512f");
    }

    /**
     * Check if AVX2 is supported by the CPU.
     */
    public static boolean supportsAVX2() {
        if (!initialized) return false;
        return cpuFlags.contains("avx2");
    }

    /**
     * Check if AVX is supported by the CPU.
     */
    public static boolean supportsAVX() {
        if (!initialized) return false;
        return cpuFlags.contains("avx");
    }

    /**
     * Get the best SIMD capability supported by this CPU.
     */
    public static String getBestSIMDCapability() {
        if (supportsAVX512()) {
            return "AVX-512 (512-bit SIMD)";
        } else if (supportsAVX2()) {
            return "AVX2 (256-bit SIMD)";
        } else if (supportsAVX()) {
            return "AVX (256-bit SIMD)";
        } else {
            return "SSE or lower";
        }
    }

    /**
     * Check if Panama optimizations would benefit on this CPU.
     * Returns true if AVX2 or better is available.
     */
    public static boolean shouldUsePanama() {
        return supportsAVX2() || supportsAVX512();
    }

    /**
     * Get all detected CPU flags for debugging.
     */
    public static Set<String> getAllFlags() {
        return new HashSet<>(cpuFlags);
    }

    /**
     * Check if detection succeeded.
     */
    public static boolean isAvailable() {
        return initialized;
    }
}
