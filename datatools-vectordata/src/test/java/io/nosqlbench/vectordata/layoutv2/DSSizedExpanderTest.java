package io.nosqlbench.vectordata.layoutv2;

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


import io.nosqlbench.vectordata.utils.SHARED;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Tests for [DSSizedExpander] — verifies that sized profile specifications
/// are correctly expanded into concrete profile entries.
class DSSizedExpanderTest {

    @Test
    void testFormatCount() {
        assertEquals("10m", DSSizedExpander.formatCount(10_000_000));
        assertEquals("100m", DSSizedExpander.formatCount(100_000_000));
        assertEquals("1b", DSSizedExpander.formatCount(1_000_000_000));
        assertEquals("1k", DSSizedExpander.formatCount(1_000));
        assertEquals("500", DSSizedExpander.formatCount(500));
        assertEquals("1t", DSSizedExpander.formatCount(1_000_000_000_000L));
    }

    @Test
    void testParseSimpleValue() {
        List<DSSizedExpander.SizedEntry> entries = DSSizedExpander.parseRangeSpec("10m");
        assertEquals(1, entries.size());
        assertEquals("10m", entries.get(0).name());
        assertEquals(10_000_000, entries.get(0).baseCount());
    }

    @Test
    void testParseLinearRangeWithStep() {
        List<DSSizedExpander.SizedEntry> entries = DSSizedExpander.parseRangeSpec("100m..400m/100m");
        assertEquals(4, entries.size());
        assertEquals("100m", entries.get(0).name());
        assertEquals(100_000_000, entries.get(0).baseCount());
        assertEquals("200m", entries.get(1).name());
        assertEquals(200_000_000, entries.get(1).baseCount());
        assertEquals("300m", entries.get(2).name());
        assertEquals(300_000_000, entries.get(2).baseCount());
        assertEquals("400m", entries.get(3).name());
        assertEquals(400_000_000, entries.get(3).baseCount());
    }

    @Test
    void testParseLinearRangeWithStepFromZero() {
        List<DSSizedExpander.SizedEntry> entries = DSSizedExpander.parseRangeSpec("0m..400m/10m");
        // Should produce 10m, 20m, 30m, ..., 400m (40 entries, skipping 0)
        assertEquals(40, entries.size());
        assertEquals("10m", entries.get(0).name());
        assertEquals(10_000_000, entries.get(0).baseCount());
        assertEquals("400m", entries.get(entries.size() - 1).name());
        assertEquals(400_000_000, entries.get(entries.size() - 1).baseCount());
    }

    @Test
    void testParseLinearRangeWithCount() {
        List<DSSizedExpander.SizedEntry> entries = DSSizedExpander.parseRangeSpec("0m..400m/10");
        // 10 equal divisions of 400m range = 40m, 80m, 120m, ..., 400m
        assertEquals(10, entries.size());
        assertEquals("40m", entries.get(0).name());
        assertEquals(40_000_000, entries.get(0).baseCount());
        assertEquals("400m", entries.get(entries.size() - 1).name());
        assertEquals(400_000_000, entries.get(entries.size() - 1).baseCount());
    }

    @Test
    void testParseFibonacci() {
        List<DSSizedExpander.SizedEntry> entries = DSSizedExpander.parseRangeSpec("fib:1m..10m");
        // Fibonacci: 1m*1=1m, 1m*1=1m, 1m*2=2m, 1m*3=3m, 1m*5=5m, 1m*8=8m
        // After dedup via TreeMap: 1m, 2m, 3m, 5m, 8m
        assertTrue(entries.size() >= 4);
        assertEquals("1m", entries.get(0).name());
    }

    @Test
    void testParseGeometric() {
        List<DSSizedExpander.SizedEntry> entries = DSSizedExpander.parseRangeSpec("mul:1m..100m/2");
        // Geometric: 1m, 2m, 4m, 8m, 16m, 32m, 64m
        assertEquals(7, entries.size());
        assertEquals("1m", entries.get(0).name());
        assertEquals("2m", entries.get(1).name());
        assertEquals("4m", entries.get(2).name());
        assertEquals("64m", entries.get(6).name());
    }

    @Test
    void testExpandWithObjectForm() {
        // Simulates the img-search dataset.yaml sized section
        DSProfile defaultProfile = new DSProfile();
        defaultProfile.setMaxk(100);
        defaultProfile.setName("default");
        DSSource defaultBase = new DSSource("profiles/base/base_vectors.mvec");
        defaultProfile.put("base_vectors", new DSView("base_vectors", defaultBase, DSWindow.ALL));
        DSSource defaultQuery = new DSSource("profiles/base/query_vectors.mvec");
        defaultProfile.put("query_vectors", new DSView("query_vectors", defaultQuery, DSWindow.ALL));

        DSProfileGroup group = new DSProfileGroup();
        group.addProfile("default", defaultProfile);

        Map<String, Object> sizedData = Map.of(
            "ranges", List.of("10m..30m/10m"),
            "facets", Map.of(
                "base_vectors", "profiles/base/base_vectors.mvec:${range}",
                "query_vectors", "profiles/base/query_vectors.mvec",
                "neighbor_indices", "profiles/${profile}/neighbor_indices.ivec"
            )
        );

        DSSizedExpander.expand(sizedData, defaultProfile, group);

        // Should have default + 10m, 20m, 30m
        assertEquals(4, group.size());

        // Check 10m profile
        DSProfile profile10m = group.get("10m");
        assertNotNull(profile10m);
        assertEquals("10m", profile10m.getName());
        assertEquals(10_000_000, profile10m.getBaseCount());
        assertEquals(100, profile10m.getMaxk());

        // Check base_vectors has window [0..10000000)
        DSView baseView = profile10m.get("base_vectors");
        assertNotNull(baseView);
        assertEquals("profiles/base/base_vectors.mvec", baseView.getSource().getPath());
        assertNotNull(baseView.getSource().getWindow());
        assertTrue(baseView.getSource().getWindow().size() > 0);
        assertEquals(0, baseView.getSource().getWindow().get(0).getMinIncl());
        assertEquals(10_000_000, baseView.getSource().getWindow().get(0).getMaxExcl());

        // Check neighbor_indices has profile name substituted
        DSView indicesView = profile10m.get("neighbor_indices");
        assertNotNull(indicesView);
        assertEquals("profiles/10m/neighbor_indices.ivec", indicesView.getSource().getPath());

        // Check 30m profile
        DSProfile profile30m = group.get("30m");
        assertNotNull(profile30m);
        assertEquals(30_000_000, profile30m.getBaseCount());
    }

    @Test
    void testExpandWithSimpleListForm() {
        DSProfile defaultProfile = new DSProfile();
        defaultProfile.setMaxk(100);
        defaultProfile.setName("default");
        DSSource defaultBase = new DSSource("base.fvec");
        defaultProfile.put("base_vectors", new DSView("base_vectors", defaultBase, DSWindow.ALL));

        DSProfileGroup group = new DSProfileGroup();
        group.addProfile("default", defaultProfile);

        List<String> sizedData = List.of("10m", "20m");

        DSSizedExpander.expand(sizedData, defaultProfile, group);

        // Should have default + 10m, 20m
        assertEquals(3, group.size());

        DSProfile profile10m = group.get("10m");
        assertNotNull(profile10m);
        assertEquals(10_000_000, profile10m.getBaseCount());
        // Should inherit base_vectors from default
        assertNotNull(profile10m.get("base_vectors"));
        assertEquals(100, profile10m.getMaxk());
    }

    @Test
    void testFullYamlParsing() {
        // Parse a complete dataset.yaml with sized profiles
        String yaml = """
            profiles:
              default:
                maxk: 100
                base_vectors: profiles/base/base_vectors.mvec
                query_vectors: profiles/base/query_vectors.mvec
              sized:
                ranges: ["10m..30m/10m"]
                facets:
                  base_vectors: "profiles/base/base_vectors.mvec:${range}"
                  query_vectors: profiles/base/query_vectors.mvec
                  neighbor_indices: "profiles/${profile}/neighbor_indices.ivec"
            """;

        @SuppressWarnings("unchecked")
        Map<String, Object> config = (Map<String, Object>) SHARED.yamlLoader.loadFromString(yaml);
        @SuppressWarnings("unchecked")
        Map<String, Object> profilesData = (Map<String, Object>) config.get("profiles");

        DSProfileGroup group = DSProfileGroup.fromData(profilesData);

        // Should have default + 10m, 20m, 30m = 4 profiles
        assertEquals(4, group.size());
        assertNotNull(group.get("default"));
        assertNotNull(group.get("10m"));
        assertNotNull(group.get("20m"));
        assertNotNull(group.get("30m"));

        // Verify 20m profile details
        DSProfile profile20m = group.get("20m");
        assertEquals(20_000_000, profile20m.getBaseCount());
        assertEquals(100, profile20m.getMaxk());

        DSView baseView = profile20m.get("base_vectors");
        assertEquals("profiles/base/base_vectors.mvec", baseView.getSource().getPath());
        assertEquals(1, baseView.getSource().getWindow().size());
        assertEquals(0, baseView.getSource().getWindow().get(0).getMinIncl());
        assertEquals(20_000_000, baseView.getSource().getWindow().get(0).getMaxExcl());

        DSView indicesView = profile20m.get("neighbor_indices");
        assertEquals("profiles/20m/neighbor_indices.ivec", indicesView.getSource().getPath());
    }

    @Test
    void testBaseCountInExplicitProfile() {
        String yaml = """
            profiles:
              default:
                maxk: 100
                base_vectors: base.fvec
              1m:
                base_count: 1M
                base_vectors: "base.fvec[0..1M]"
            """;

        @SuppressWarnings("unchecked")
        Map<String, Object> config = (Map<String, Object>) SHARED.yamlLoader.loadFromString(yaml);
        @SuppressWarnings("unchecked")
        Map<String, Object> profilesData = (Map<String, Object>) config.get("profiles");

        DSProfileGroup group = DSProfileGroup.fromData(profilesData);

        DSProfile profile1m = group.get("1m");
        assertNotNull(profile1m);
        assertEquals(1_000_000, profile1m.getBaseCount());
        assertEquals(100, profile1m.getMaxk());

        // base_count should NOT be on default
        assertNull(group.get("default").getBaseCount());
    }

    /// Mirrors the vectordata-rs img-search dataset.yaml format exactly:
    /// zero-start range, facets with `${range}` windowing, `${profile}` paths,
    /// and plain shared files.
    @Test
    void testVectordataRsImgSearchFormat() {
        String yaml = """
            profiles:
              default:
                maxk: 100
                base_vectors: profiles/base/base_vectors.mvec
                query_vectors: profiles/base/query_vectors.mvec
                metadata_content: profiles/base/metadata_content.slab
                metadata_predicates: predicates.slab
                neighbor_indices: profiles/default/neighbor_indices.ivec
                neighbor_distances: profiles/default/neighbor_distances.fvec
                metadata_indices: profiles/default/metadata_indices.slab
                filtered_neighbor_indices: profiles/default/filtered_neighbor_indices.ivec
                filtered_neighbor_distances: profiles/default/filtered_neighbor_distances.fvec
              sized:
                ranges: ["0m..400m/10m"]
                facets:
                  base_vectors: "profiles/base/base_vectors.mvec:${range}"
                  query_vectors: profiles/base/query_vectors.mvec
                  metadata_predicates: "predicates.slab"
                  metadata_content: "profiles/base/metadata_content.slab:${range}"
                  neighbor_indices: "profiles/${profile}/neighbor_indices.ivec"
                  neighbor_distances: "profiles/${profile}/neighbor_distances.fvec"
                  metadata_indices: "profiles/${profile}/metadata_indices.slab"
                  filtered_neighbor_indices: "profiles/${profile}/filtered_neighbor_indices.ivec"
                  filtered_neighbor_distances: "profiles/${profile}/filtered_neighbor_distances.fvec"
            """;

        @SuppressWarnings("unchecked")
        var config = (Map<String, Object>) SHARED.yamlLoader.loadFromString(yaml);
        @SuppressWarnings("unchecked")
        var profilesData = (Map<String, Object>) config.get("profiles");

        DSProfileGroup group = DSProfileGroup.fromData(profilesData);

        // 0m..400m/10m skips 0 → 10m, 20m, ..., 400m = 40 profiles + default = 41
        assertEquals(41, group.size());
        assertNotNull(group.get("default"));
        assertNotNull(group.get("10m"));
        assertNotNull(group.get("200m"));
        assertNotNull(group.get("400m"));

        // Verify the first sized profile (10m)
        DSProfile profile10m = group.get("10m");
        assertEquals(10_000_000, profile10m.getBaseCount());
        assertEquals(100, profile10m.getMaxk());

        // base_vectors: windowed via ${range} → [0..10000000)
        DSView baseView = profile10m.get("base_vectors");
        assertNotNull(baseView);
        assertEquals("profiles/base/base_vectors.mvec", baseView.getSource().getPath());
        assertNotNull(baseView.getSource().getWindow());
        assertEquals(1, baseView.getSource().getWindow().size());
        assertEquals(0, baseView.getSource().getWindow().get(0).getMinIncl());
        assertEquals(10_000_000, baseView.getSource().getWindow().get(0).getMaxExcl());

        // metadata_content: also windowed via ${range}
        DSView metaView = profile10m.get("metadata_content");
        assertNotNull(metaView);
        assertEquals("profiles/base/metadata_content.slab", metaView.getSource().getPath());
        assertEquals(0, metaView.getSource().getWindow().get(0).getMinIncl());
        assertEquals(10_000_000, metaView.getSource().getWindow().get(0).getMaxExcl());

        // query_vectors: no substitution, shared across all profiles
        DSView queryView = profile10m.get("query_vectors");
        assertNotNull(queryView);
        assertEquals("profiles/base/query_vectors.mvec", queryView.getSource().getPath());

        // metadata_predicates: plain shared file, no windowing
        DSView predView = profile10m.get("metadata_predicates");
        assertNotNull(predView);
        assertEquals("predicates.slab", predView.getSource().getPath());

        // neighbor_indices: ${profile} substituted → profiles/10m/...
        DSView indicesView = profile10m.get("neighbor_indices");
        assertNotNull(indicesView);
        assertEquals("profiles/10m/neighbor_indices.ivec", indicesView.getSource().getPath());

        // neighbor_distances: ${profile} substituted
        DSView distView = profile10m.get("neighbor_distances");
        assertNotNull(distView);
        assertEquals("profiles/10m/neighbor_distances.fvec", distView.getSource().getPath());

        // filtered_neighbor_indices: ${profile} substituted
        DSView filtIndView = profile10m.get("filtered_neighbor_indices");
        assertNotNull(filtIndView);
        assertEquals("profiles/10m/filtered_neighbor_indices.ivec", filtIndView.getSource().getPath());

        // filtered_neighbor_distances: ${profile} substituted
        DSView filtDistView = profile10m.get("filtered_neighbor_distances");
        assertNotNull(filtDistView);
        assertEquals("profiles/10m/filtered_neighbor_distances.fvec", filtDistView.getSource().getPath());

        // metadata_indices: ${profile} substituted
        DSView metaIndView = profile10m.get("metadata_indices");
        assertNotNull(metaIndView);
        assertEquals("profiles/10m/metadata_indices.slab", metaIndView.getSource().getPath());

        // Verify a middle profile (200m) for correct windowing
        DSProfile profile200m = group.get("200m");
        assertEquals(200_000_000, profile200m.getBaseCount());
        DSView base200m = profile200m.get("base_vectors");
        assertEquals(0, base200m.getSource().getWindow().get(0).getMinIncl());
        assertEquals(200_000_000, base200m.getSource().getWindow().get(0).getMaxExcl());
        assertEquals("profiles/200m/neighbor_indices.ivec",
            profile200m.get("neighbor_indices").getSource().getPath());

        // Verify the last profile (400m)
        DSProfile profile400m = group.get("400m");
        assertEquals(400_000_000, profile400m.getBaseCount());
        DSView base400m = profile400m.get("base_vectors");
        assertEquals(0, base400m.getSource().getWindow().get(0).getMinIncl());
        assertEquals(400_000_000, base400m.getSource().getWindow().get(0).getMaxExcl());

        // default profile should NOT have base_count set
        assertNull(group.get("default").getBaseCount());
    }

    /// Tests the count-division form `"0m..400m/10"` (bare number = divide into
    /// 10 equal parts) through the full YAML parsing pipeline with facet templates.
    /// This produces profiles at 40m, 80m, 120m, ..., 400m (10 profiles, no zero).
    @Test
    void testVectordataRsCountDivisionFormat() {
        String yaml = """
            profiles:
              default:
                maxk: 100
                base_vectors: profiles/base/base_vectors.mvec
                query_vectors: profiles/base/query_vectors.mvec
              sized:
                ranges: ["0m..400m/10"]
                facets:
                  base_vectors: "profiles/base/base_vectors.mvec:${range}"
                  query_vectors: profiles/base/query_vectors.mvec
                  neighbor_indices: "profiles/${profile}/neighbor_indices.ivec"
                  neighbor_distances: "profiles/${profile}/neighbor_distances.fvec"
            """;

        @SuppressWarnings("unchecked")
        var config = (Map<String, Object>) SHARED.yamlLoader.loadFromString(yaml);
        @SuppressWarnings("unchecked")
        var profilesData = (Map<String, Object>) config.get("profiles");

        DSProfileGroup group = DSProfileGroup.fromData(profilesData);

        // 400m / 10 = 40m step → 40m, 80m, 120m, ..., 400m = 10 profiles + default = 11
        assertEquals(11, group.size());
        assertNotNull(group.get("default"));

        // First profile is 40m (not 0)
        DSProfile profile40m = group.get("40m");
        assertNotNull(profile40m, "first division should be 40m");
        assertEquals(40_000_000, profile40m.getBaseCount());
        assertEquals(100, profile40m.getMaxk());

        // base_vectors windowed to [0..40000000)
        DSView baseView = profile40m.get("base_vectors");
        assertNotNull(baseView);
        assertEquals("profiles/base/base_vectors.mvec", baseView.getSource().getPath());
        assertEquals(1, baseView.getSource().getWindow().size());
        assertEquals(0, baseView.getSource().getWindow().get(0).getMinIncl());
        assertEquals(40_000_000, baseView.getSource().getWindow().get(0).getMaxExcl());

        // neighbor_indices: ${profile} → profiles/40m/...
        assertEquals("profiles/40m/neighbor_indices.ivec",
            profile40m.get("neighbor_indices").getSource().getPath());

        // Verify all 10 expected profiles exist at 40m intervals
        for (int i = 1; i <= 10; i++) {
            long expected = i * 40_000_000L;
            String name = DSSizedExpander.formatCount(expected);
            DSProfile p = group.get(name);
            assertNotNull(p, "expected profile " + name);
            assertEquals(expected, p.getBaseCount());
        }

        // Last profile is 400m
        DSProfile profile400m = group.get("400m");
        assertNotNull(profile400m);
        assertEquals(400_000_000, profile400m.getBaseCount());
        DSView base400m = profile400m.get("base_vectors");
        assertEquals(0, base400m.getSource().getWindow().get(0).getMinIncl());
        assertEquals(400_000_000, base400m.getSource().getWindow().get(0).getMaxExcl());

        // No zero profile should exist — default has null base_count, all sized > 0
        assertNull(group.get("default").getBaseCount());
        assertNull(group.get("0"), "zero profile should not exist");
        assertNull(group.get("0m"), "zero profile should not exist");
    }

    /// Per §5.3 form 2: when end is not exactly reachable, the last value that
    /// does not exceed end is included. `"10m..25m/10m"` → `10m, 20m` (30m > 25m).
    @Test
    void testParseLinearRangePartialStep() {
        List<DSSizedExpander.SizedEntry> entries = DSSizedExpander.parseRangeSpec("10m..25m/10m");
        assertEquals(2, entries.size());
        assertEquals("10m", entries.get(0).name());
        assertEquals(10_000_000, entries.get(0).baseCount());
        assertEquals("20m", entries.get(1).name());
        assertEquals(20_000_000, entries.get(1).baseCount());
    }

    /// Per §5.3 form 3: count division with nonzero start.
    /// `"100m..400m/3"` → 3 divisions of the 300m range → `200m, 300m, 400m`.
    @Test
    void testParseLinearCountNonzeroStart() {
        List<DSSizedExpander.SizedEntry> entries = DSSizedExpander.parseRangeSpec("100m..400m/3");
        assertEquals(3, entries.size());
        assertEquals("200m", entries.get(0).name());
        assertEquals(200_000_000, entries.get(0).baseCount());
        assertEquals("300m", entries.get(1).name());
        assertEquals(300_000_000, entries.get(1).baseCount());
        assertEquals("400m", entries.get(2).name());
        assertEquals(400_000_000, entries.get(2).baseCount());
    }

    /// Per §5.3 "critical distinction": the presence or absence of a unit suffix
    /// on the divisor determines step vs count interpretation.
    /// `100m..400m/10m` (step) → 31 profiles; `100m..400m/10` (count) → 10 profiles.
    @Test
    void testStepVsCountCriticalDistinction() {
        // Step form: 100m, 110m, 120m, ..., 400m = 31 entries
        List<DSSizedExpander.SizedEntry> stepEntries = DSSizedExpander.parseRangeSpec("100m..400m/10m");
        assertEquals(31, stepEntries.size());
        assertEquals("100m", stepEntries.get(0).name());
        assertEquals("110m", stepEntries.get(1).name());
        assertEquals("400m", stepEntries.get(30).name());

        // Count form: 10 divisions of 300m range → 130m, 160m, ..., 400m = 10 entries
        List<DSSizedExpander.SizedEntry> countEntries = DSSizedExpander.parseRangeSpec("100m..400m/10");
        assertEquals(10, countEntries.size());
        assertEquals("130m", countEntries.get(0).name());
        assertEquals(130_000_000, countEntries.get(0).baseCount());
        assertEquals("400m", countEntries.get(9).name());
        assertEquals(400_000_000, countEntries.get(9).baseCount());
    }

    /// Per §5.3 form 4: Fibonacci with exact values from the spec.
    /// `"fib:1m..400m"` with base unit 1m, fibonacci sequence 1, 1, 2, 3, 5, 8, 13,
    /// 21, 34, 55, 89, 144, 233, 377. The raw parse includes duplicate 1m (fib
    /// produces two 1s); dedup happens in the TreeMap during `expand()`.
    @Test
    void testParseFibonacciExactValues() {
        List<DSSizedExpander.SizedEntry> entries = DSSizedExpander.parseRangeSpec("fib:1m..400m");
        // Raw fibonacci: 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377 = 14 entries
        // (duplicate 1m from the two fib(1)=fib(2)=1)
        long[] expected = {
            1_000_000, 1_000_000, 2_000_000, 3_000_000, 5_000_000, 8_000_000,
            13_000_000, 21_000_000, 34_000_000, 55_000_000, 89_000_000,
            144_000_000, 233_000_000, 377_000_000
        };
        assertEquals(expected.length, entries.size());
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], entries.get(i).baseCount(),
                "entry " + i + " expected " + expected[i]);
        }
    }

    /// Per §5.3 form 5: geometric with fractional factor.
    /// `"mul:10m..100m/1.5"` → each value is `floor(prev × 1.5)`:
    /// 10m, 15m, 22500k, 33750k, 50625k, 75937500.
    @Test
    void testParseGeometricFractionalFactor() {
        List<DSSizedExpander.SizedEntry> entries = DSSizedExpander.parseRangeSpec("mul:10m..100m/1.5");
        assertEquals(6, entries.size());
        assertEquals(10_000_000, entries.get(0).baseCount());
        assertEquals("10m", entries.get(0).name());
        assertEquals(15_000_000, entries.get(1).baseCount());
        assertEquals("15m", entries.get(1).name());
        assertEquals(22_500_000, entries.get(2).baseCount());
        assertEquals("22500k", entries.get(2).name());
        assertEquals(33_750_000, entries.get(3).baseCount());
        assertEquals("33750k", entries.get(3).name());
        assertEquals(50_625_000, entries.get(4).baseCount());
        assertEquals("50625k", entries.get(4).name());
        assertEquals(75_937_500, entries.get(5).baseCount());
        assertEquals("75937500", entries.get(5).name());
    }

    /// Per §5.3 complete example: sized profiles coexist with explicitly declared
    /// profiles. Explicit profiles are not affected by sized expansion.
    @Test
    void testSizedCoexistsWithExplicitProfiles() {
        String yaml = """
            profiles:
              default:
                maxk: 100
                base_vectors: base_vectors.mvec
                query_vectors: query_vectors.mvec
                neighbor_indices: neighbor_indices.ivec
              sized: [10m, 20m, 100m..300m/100m]
              custom-queries:
                query_vectors: alt_queries.mvec
                neighbor_indices: alt_indices.ivec
            """;

        @SuppressWarnings("unchecked")
        var config = (Map<String, Object>) SHARED.yamlLoader.loadFromString(yaml);
        @SuppressWarnings("unchecked")
        var profilesData = (Map<String, Object>) config.get("profiles");

        DSProfileGroup group = DSProfileGroup.fromData(profilesData);

        // 7 profiles: default, 10m, 20m, 100m, 200m, 300m, custom-queries
        assertEquals(7, group.size());
        assertNotNull(group.get("default"));
        assertNotNull(group.get("10m"));
        assertNotNull(group.get("20m"));
        assertNotNull(group.get("100m"));
        assertNotNull(group.get("200m"));
        assertNotNull(group.get("300m"));
        assertNotNull(group.get("custom-queries"));

        // Sized profiles have base_count and inherit from default
        assertEquals(10_000_000, group.get("10m").getBaseCount());
        assertEquals(100, group.get("10m").getMaxk());
        assertNotNull(group.get("10m").get("base_vectors"));
        assertNotNull(group.get("10m").get("query_vectors"));
        assertNotNull(group.get("10m").get("neighbor_indices"));

        // custom-queries is unaffected by sized expansion — inherits from default
        DSProfile custom = group.get("custom-queries");
        assertNull(custom.getBaseCount());
        assertEquals(100, custom.getMaxk());
        // Overridden views
        assertEquals("alt_queries.mvec", custom.get("query_vectors").getSource().getPath());
        assertEquals("alt_indices.ivec", custom.get("neighbor_indices").getSource().getPath());
        // Inherited view
        assertEquals("base_vectors.mvec", custom.get("base_vectors").getSource().getPath());
    }
}
