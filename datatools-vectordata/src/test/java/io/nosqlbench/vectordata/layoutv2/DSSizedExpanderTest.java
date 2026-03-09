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
        DSSource defaultBase = new DSSource("profiles/base/base_vectors.hvec");
        defaultProfile.put("base_vectors", new DSView("base_vectors", defaultBase, DSWindow.ALL));
        DSSource defaultQuery = new DSSource("profiles/base/query_vectors.hvec");
        defaultProfile.put("query_vectors", new DSView("query_vectors", defaultQuery, DSWindow.ALL));

        DSProfileGroup group = new DSProfileGroup();
        group.addProfile("default", defaultProfile);

        Map<String, Object> sizedData = Map.of(
            "ranges", List.of("10m..30m/10m"),
            "facets", Map.of(
                "base_vectors", "profiles/base/base_vectors.hvec:${range}",
                "query_vectors", "profiles/base/query_vectors.hvec",
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
        assertEquals("profiles/base/base_vectors.hvec", baseView.getSource().getPath());
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
                base_vectors: profiles/base/base_vectors.hvec
                query_vectors: profiles/base/query_vectors.hvec
              sized:
                ranges: ["10m..30m/10m"]
                facets:
                  base_vectors: "profiles/base/base_vectors.hvec:${range}"
                  query_vectors: profiles/base/query_vectors.hvec
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
        assertEquals("profiles/base/base_vectors.hvec", baseView.getSource().getPath());
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
}
