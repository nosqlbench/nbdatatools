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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the ComputeMode detection utility.
 */
public class ComputeModeTest {

    @Test
    void effectiveModeIsNotNull() {
        ComputeMode.Mode mode = ComputeMode.getEffectiveMode();
        assertNotNull(mode, "Effective mode should never be null");
    }

    @Test
    void javaVersionIsPositive() {
        int version = ComputeMode.getJavaVersion();
        assertTrue(version >= 11, "Java version should be at least 11");
    }

    @Test
    void modeHasValidProperties() {
        for (ComputeMode.Mode mode : ComputeMode.Mode.values()) {
            assertNotNull(mode.displayName(), "Mode display name should not be null");
            assertNotNull(mode.description(), "Mode description should not be null");
            assertTrue(mode.floatLanes() >= 1, "Float lanes should be at least 1");
        }
    }

    @Test
    void scalarModeDoesNotUsePanama() {
        assertFalse(ComputeMode.Mode.SCALAR.usesPanama());
        assertFalse(ComputeMode.Mode.SCALAR.usesAVX512F());
    }

    @Test
    void panamaModeUsesPanama() {
        assertTrue(ComputeMode.Mode.PANAMA_AVX512F.usesPanama());
        assertTrue(ComputeMode.Mode.PANAMA_AVX2.usesPanama());
        assertTrue(ComputeMode.Mode.PANAMA_SSE.usesPanama());
    }

    @Test
    void avx512FModeUsesAVX512F() {
        assertTrue(ComputeMode.Mode.PANAMA_AVX512F.usesAVX512F());
        assertFalse(ComputeMode.Mode.PANAMA_AVX2.usesAVX512F());
        assertFalse(ComputeMode.Mode.PANAMA_SSE.usesAVX512F());
        assertFalse(ComputeMode.Mode.SCALAR.usesAVX512F());
    }

    @Test
    void floatLanesAreCorrect() {
        assertEquals(16, ComputeMode.Mode.PANAMA_AVX512F.floatLanes());
        assertEquals(8, ComputeMode.Mode.PANAMA_AVX2.floatLanes());
        assertEquals(4, ComputeMode.Mode.PANAMA_SSE.floatLanes());
        assertEquals(1, ComputeMode.Mode.SCALAR.floatLanes());
    }

    @Test
    void capabilityReportIsNotEmpty() {
        String report = ComputeMode.getCapabilityReport();
        assertNotNull(report);
        assertFalse(report.isEmpty());
        assertTrue(report.contains("COMPUTE MODE CAPABILITY REPORT"));
        assertTrue(report.contains("Java Version"));
        assertTrue(report.contains("Selected Mode"));
    }

    @Test
    void modeSummaryIsNotEmpty() {
        String summary = ComputeMode.getModeSummary();
        assertNotNull(summary);
        assertFalse(summary.isEmpty());
    }

    @Test
    void bestSIMDCapabilityIsNotEmpty() {
        String simd = ComputeMode.getBestSIMDCapability();
        assertNotNull(simd);
        assertFalse(simd.isEmpty());
    }

    @Test
    void panamaAvailabilityConsistentWithJavaVersion() {
        // If Java < 25, Panama should not be available
        if (ComputeMode.getJavaVersion() < 25) {
            assertFalse(ComputeMode.isPanamaAvailable(),
                "Panama should not be available on Java < 25");
            assertEquals(ComputeMode.Mode.SCALAR, ComputeMode.getEffectiveMode(),
                "Should use SCALAR mode on Java < 25");
        }
    }

    @Test
    void preferredVectorBitsConsistentWithPanamaAvailability() {
        if (ComputeMode.isPanamaAvailable()) {
            assertTrue(ComputeMode.getPreferredVectorBits() > 0,
                "Vector bits should be positive when Panama is available");
        } else {
            assertEquals(0, ComputeMode.getPreferredVectorBits(),
                "Vector bits should be 0 when Panama is not available");
        }
    }

    @Test
    void effectiveModeMatchesPanamaAndCPUCapabilities() {
        ComputeMode.Mode mode = ComputeMode.getEffectiveMode();

        if (!ComputeMode.isPanamaAvailable()) {
            assertEquals(ComputeMode.Mode.SCALAR, mode,
                "Without Panama, mode should be SCALAR");
        } else {
            // Panama is available
            assertNotEquals(ComputeMode.Mode.SCALAR, mode,
                "With Panama available, mode should not be SCALAR");

            // Mode should be consistent with CPU capabilities
            if (mode == ComputeMode.Mode.PANAMA_AVX512F) {
                assertTrue(ComputeMode.supportsAVX512F(),
                    "PANAMA_AVX512F mode requires AVX-512F support");
            }
        }
    }

    @Test
    void vectorSpaceAnalyzerReportsComputeMode() {
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer();

        ComputeMode.Mode mode = analyzer.getComputeMode();
        assertNotNull(mode);

        String summary = analyzer.getComputeModeSummary();
        assertNotNull(summary);
        assertFalse(summary.isEmpty());
    }
}
