package io.nosqlbench.vectordata.access;

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

import io.nosqlbench.vectordata.layoutv2.DSProfile;
import io.nosqlbench.vectordata.layoutv2.DSProfileGroup;
import io.nosqlbench.vectordata.layoutv2.DSSizedExpander;
import io.nosqlbench.vectordata.utils.SHARED;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/// Tests for [DSSizedExpander] — verifies that sized profile specifications
/// are correctly expanded into concrete profile entries through the YAML
/// parsing pipeline.
class SizedExpansionTest {

    /// Parses a YAML profiles section and returns the resulting profile group,
    /// excluding the "default" profile from the returned list of sized profiles.
    ///
    /// @param yamlBody The YAML string to parse
    /// @return The parsed DSProfileGroup
    private DSProfileGroup parseProfiles(String yamlBody) {
        @SuppressWarnings("unchecked")
        Map<String, Object> config = (Map<String, Object>) SHARED.yamlLoader.loadFromString(yamlBody);
        @SuppressWarnings("unchecked")
        Map<String, Object> profilesData = (Map<String, Object>) config.get("profiles");
        return DSProfileGroup.fromData(profilesData);
    }

    /// Returns the sized (non-default) profiles sorted by base_count ascending.
    ///
    /// @param group The profile group to extract sized profiles from
    /// @return A list of profiles sorted by base_count
    private List<DSProfile> sizedProfiles(DSProfileGroup group) {
        List<DSProfile> result = new ArrayList<>();
        for (Map.Entry<String, DSProfile> entry : group.entrySet()) {
            if (!"default".equals(entry.getKey()) && entry.getValue().getBaseCount() != null) {
                result.add(entry.getValue());
            }
        }
        result.sort((a, b) -> Long.compare(a.getBaseCount(), b.getBaseCount()));
        return result;
    }

    /// Simple list `[10m, 20m, 50m]` should produce 3 profiles with correct base_count.
    @Test
    void testSizedSimpleList() {
        String yaml = """
            profiles:
              default:
                maxk: 100
                base_vectors: base.fvec
              sized: [10m, 20m, 50m]
            """;
        DSProfileGroup group = parseProfiles(yaml);
        List<DSProfile> sized = sizedProfiles(group);

        assertThat(sized).hasSize(3);
        assertThat(sized.get(0).getBaseCount()).isEqualTo(10_000_000L);
        assertThat(sized.get(1).getBaseCount()).isEqualTo(20_000_000L);
        assertThat(sized.get(2).getBaseCount()).isEqualTo(50_000_000L);
    }

    /// Range `[100m..400m/100m]` should produce profiles at 100m, 200m, 300m, 400m.
    @Test
    void testSizedRangeExpansion() {
        String yaml = """
            profiles:
              default:
                maxk: 100
                base_vectors: base.fvec
              sized: [100m..400m/100m]
            """;
        DSProfileGroup group = parseProfiles(yaml);
        List<DSProfile> sized = sizedProfiles(group);

        assertThat(sized).hasSize(4);
        assertThat(sized.get(0).getBaseCount()).isEqualTo(100_000_000L);
        assertThat(sized.get(1).getBaseCount()).isEqualTo(200_000_000L);
        assertThat(sized.get(2).getBaseCount()).isEqualTo(300_000_000L);
        assertThat(sized.get(3).getBaseCount()).isEqualTo(400_000_000L);
    }

    /// Mixed simple values and ranges: `[10m, 20m, 100m..300m/100m]` should produce
    /// 5 profiles total.
    @Test
    void testSizedMixedSimpleAndRange() {
        String yaml = """
            profiles:
              default:
                maxk: 100
                base_vectors: base.fvec
              sized: [10m, 20m, 100m..300m/100m]
            """;
        DSProfileGroup group = parseProfiles(yaml);
        List<DSProfile> sized = sizedProfiles(group);

        assertThat(sized).hasSize(5);
        assertThat(sized.get(0).getBaseCount()).isEqualTo(10_000_000L);
        assertThat(sized.get(1).getBaseCount()).isEqualTo(20_000_000L);
        assertThat(sized.get(2).getBaseCount()).isEqualTo(100_000_000L);
        assertThat(sized.get(3).getBaseCount()).isEqualTo(200_000_000L);
        assertThat(sized.get(4).getBaseCount()).isEqualTo(300_000_000L);
    }

    /// Profiles from mixed-order inputs should be sorted smallest to largest.
    @Test
    void testSizedSortedByCount() {
        String yaml = """
            profiles:
              default:
                maxk: 100
                base_vectors: base.fvec
              sized: [50m, 10m, 30m, 20m]
            """;
        DSProfileGroup group = parseProfiles(yaml);
        List<DSProfile> sized = sizedProfiles(group);

        assertThat(sized).hasSize(4);
        // Verify strictly ascending order
        for (int i = 1; i < sized.size(); i++) {
            assertThat(sized.get(i).getBaseCount())
                .as("profile %d should be larger than profile %d", i, i - 1)
                .isGreaterThan(sized.get(i - 1).getBaseCount());
        }
        assertThat(sized.get(0).getBaseCount()).isEqualTo(10_000_000L);
        assertThat(sized.get(3).getBaseCount()).isEqualTo(50_000_000L);
    }

    /// Fibonacci expansion: `fib:1m..400m` should produce fibonacci multiples
    /// of 1m within the range [1m, 400m].
    /// Fibonacci sequence: 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377.
    /// After dedup (TreeMap in expand), unique values: 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377.
    @Test
    void testSizedFibonacci() {
        String yaml = """
            profiles:
              default:
                maxk: 100
                base_vectors: base.fvec
              sized: ["fib:1m..400m"]
            """;
        DSProfileGroup group = parseProfiles(yaml);
        List<DSProfile> sized = sizedProfiles(group);

        long[] expectedCounts = {
            1_000_000, 2_000_000, 3_000_000, 5_000_000, 8_000_000,
            13_000_000, 21_000_000, 34_000_000, 55_000_000, 89_000_000,
            144_000_000, 233_000_000, 377_000_000
        };
        assertThat(sized).hasSize(expectedCounts.length);
        for (int i = 0; i < expectedCounts.length; i++) {
            assertThat(sized.get(i).getBaseCount())
                .as("fibonacci profile %d", i)
                .isEqualTo(expectedCounts[i]);
        }
    }

    /// Geometric doubling: `mul:1m..100m/2` should produce a doubling series
    /// 1m, 2m, 4m, 8m, 16m, 32m, 64m.
    @Test
    void testSizedGeometricDoubling() {
        String yaml = """
            profiles:
              default:
                maxk: 100
                base_vectors: base.fvec
              sized: ["mul:1m..100m/2"]
            """;
        DSProfileGroup group = parseProfiles(yaml);
        List<DSProfile> sized = sizedProfiles(group);

        assertThat(sized).hasSize(7);
        assertThat(sized.get(0).getBaseCount()).isEqualTo(1_000_000L);
        assertThat(sized.get(1).getBaseCount()).isEqualTo(2_000_000L);
        assertThat(sized.get(2).getBaseCount()).isEqualTo(4_000_000L);
        assertThat(sized.get(3).getBaseCount()).isEqualTo(8_000_000L);
        assertThat(sized.get(4).getBaseCount()).isEqualTo(16_000_000L);
        assertThat(sized.get(5).getBaseCount()).isEqualTo(32_000_000L);
        assertThat(sized.get(6).getBaseCount()).isEqualTo(64_000_000L);
    }

    /// Geometric with fractional factor: `mul:10m..100m/1.5` should compound by 1.5.
    /// Series: 10m, floor(10m*1.5)=15m, floor(15m*1.5)=22500k, floor(22500k*1.5)=33750k,
    /// floor(33750k*1.5)=50625k, floor(50625k*1.5)=75937500.
    @Test
    void testSizedGeometricFractional() {
        String yaml = """
            profiles:
              default:
                maxk: 100
                base_vectors: base.fvec
              sized: ["mul:10m..100m/1.5"]
            """;
        DSProfileGroup group = parseProfiles(yaml);
        List<DSProfile> sized = sizedProfiles(group);

        assertThat(sized).hasSize(6);
        assertThat(sized.get(0).getBaseCount()).isEqualTo(10_000_000L);
        assertThat(sized.get(1).getBaseCount()).isEqualTo(15_000_000L);
        assertThat(sized.get(2).getBaseCount()).isEqualTo(22_500_000L);
        assertThat(sized.get(3).getBaseCount()).isEqualTo(33_750_000L);
        assertThat(sized.get(4).getBaseCount()).isEqualTo(50_625_000L);
        assertThat(sized.get(5).getBaseCount()).isEqualTo(75_937_500L);
    }

    /// Count-division form: `0m..400m/10` (bare number = count) should produce
    /// 10 equal divisions: 40m, 80m, 120m, ..., 400m.
    @Test
    void testSizedCountMode() {
        String yaml = """
            profiles:
              default:
                maxk: 100
                base_vectors: base.fvec
              sized: ["0m..400m/10"]
            """;
        DSProfileGroup group = parseProfiles(yaml);
        List<DSProfile> sized = sizedProfiles(group);

        assertThat(sized).hasSize(10);
        for (int i = 0; i < 10; i++) {
            long expected = (i + 1) * 40_000_000L;
            assertThat(sized.get(i).getBaseCount())
                .as("count division %d", i)
                .isEqualTo(expected);
        }
    }

    /// When end is not exactly reachable by step, the last value that does not
    /// exceed end is included: `10m..25m/10m` produces [10m, 20m] only (30m > 25m).
    @Test
    void testSizedRangeNotAligned() {
        String yaml = """
            profiles:
              default:
                maxk: 100
                base_vectors: base.fvec
              sized: [10m..25m/10m]
            """;
        DSProfileGroup group = parseProfiles(yaml);
        List<DSProfile> sized = sizedProfiles(group);

        assertThat(sized).hasSize(2);
        assertThat(sized.get(0).getBaseCount()).isEqualTo(10_000_000L);
        assertThat(sized.get(1).getBaseCount()).isEqualTo(20_000_000L);
    }
}
