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

import io.nosqlbench.vectordata.layoutv2.DSInterval;
import io.nosqlbench.vectordata.layoutv2.DSProfile;
import io.nosqlbench.vectordata.layoutv2.DSProfileGroup;
import io.nosqlbench.vectordata.layoutv2.DSSource;
import io.nosqlbench.vectordata.layoutv2.DSView;
import io.nosqlbench.vectordata.layoutv2.DSWindow;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/// Comprehensive tests for dataset.yaml config parsing: intervals, windows,
/// sources, views, profiles, and profile groups.
class ConfigParsingTest {

    // ── Interval Parsing ──────────────────────────────────────────────

    /// Bare range `"0..1000"` is treated as inclusive on both ends,
    /// stored as [0, 1001) internally.
    @Test
    void testParseIntervalRange() {
        var interval = DSInterval.fromData("0..1000");
        assertThat(interval.getMinIncl()).isEqualTo(0L);
        assertThat(interval.getMaxExcl()).isEqualTo(1001L);
    }

    /// Bare range with SI suffix: `"0..1M"` -> [0, 1_000_001).
    @Test
    void testParseIntervalWithSuffix() {
        var interval = DSInterval.fromData("0..1M");
        assertThat(interval.getMinIncl()).isEqualTo(0L);
        assertThat(interval.getMaxExcl()).isEqualTo(1_000_001L);
    }

    /// Bracketed half-open `"[0..1000)"` -> [0, 1000).
    @Test
    void testParseIntervalBracketed() {
        var interval = DSInterval.fromData("[0..1000)");
        assertThat(interval.getMinIncl()).isEqualTo(0L);
        assertThat(interval.getMaxExcl()).isEqualTo(1000L);
    }

    /// Single number `"1M"` implies [0, 1_000_000).
    @Test
    void testParseIntervalSingleNumber() {
        var interval = DSInterval.fromData("1M");
        assertThat(interval.getMinIncl()).isEqualTo(0L);
        assertThat(interval.getMaxExcl()).isEqualTo(1_000_000L);
    }

    /// Underscores in numbers are stripped: `"0..1_000"` -> [0, 1001).
    @Test
    void testParseIntervalWithUnderscores() {
        var interval = DSInterval.fromData("0..1_000");
        assertThat(interval.getMinIncl()).isEqualTo(0L);
        assertThat(interval.getMaxExcl()).isEqualTo(1001L);
    }

    /// Inclusive-end bracket `"[0..100]"` -> [0, 101).
    @Test
    void testParseIntervalInclusiveEnd() {
        var interval = DSInterval.fromData("[0..100]");
        assertThat(interval.getMinIncl()).isEqualTo(0L);
        assertThat(interval.getMaxExcl()).isEqualTo(101L);
    }

    /// Exclusive-start parenthesis `"(5..10)"` -> [6, 10).
    @Test
    void testParseIntervalExclusiveStart() {
        var interval = DSInterval.fromData("(5..10)");
        assertThat(interval.getMinIncl()).isEqualTo(6L);
        assertThat(interval.getMaxExcl()).isEqualTo(10L);
    }

    /// K suffix: `"0..5k"` -> [0, 5001).
    @Test
    void testParseIntervalKSuffix() {
        var interval = DSInterval.fromData("0..5k");
        assertThat(interval.getMinIncl()).isEqualTo(0L);
        assertThat(interval.getMaxExcl()).isEqualTo(5001L);
    }

    /// Lowercase m suffix: `"0..10m"` -> [0, 10_000_001).
    @Test
    void testParseIntervalMSuffix() {
        var interval = DSInterval.fromData("0..10m");
        assertThat(interval.getMinIncl()).isEqualTo(0L);
        assertThat(interval.getMaxExcl()).isEqualTo(10_000_001L);
    }

    /// B (billion) suffix: `"0..1B"` -> [0, 1_000_000_001).
    @Test
    void testParseIntervalBGSuffix() {
        var interval = DSInterval.fromData("0..1B");
        assertThat(interval.getMinIncl()).isEqualTo(0L);
        assertThat(interval.getMaxExcl()).isEqualTo(1_000_000_001L);
    }

    /// T (tera) suffix: `"0..1T"` -> [0, 1_000_000_000_001).
    @Test
    void testParseIntervalTSuffix() {
        var interval = DSInterval.fromData("0..1T");
        assertThat(interval.getMinIncl()).isEqualTo(0L);
        assertThat(interval.getMaxExcl()).isEqualTo(1_000_000_000_001L);
    }

    // ── Window Parsing ────────────────────────────────────────────────

    /// Single range string produces one interval.
    @Test
    void testParseWindowSingle() {
        var window = DSWindow.fromData("0..1000");
        assertThat(window).hasSize(1);
        assertThat(window.get(0).getMinIncl()).isEqualTo(0L);
        assertThat(window.get(0).getMaxExcl()).isEqualTo(1001L);
    }

    /// Bracketed comma-separated list `"[0..1K, 2K..3K]"` -> 2 intervals.
    @Test
    void testParseWindowBracketedList() {
        var window = DSWindow.fromData("[0..1K, 2K..3K]");
        assertThat(window).hasSize(2);
        assertThat(window.get(0).getMinIncl()).isEqualTo(0L);
        assertThat(window.get(0).getMaxExcl()).isEqualTo(1001L);
        assertThat(window.get(1).getMinIncl()).isEqualTo(2000L);
        assertThat(window.get(1).getMaxExcl()).isEqualTo(3001L);
    }

    /// A raw YAML integer is treated as [0, N).
    @Test
    void testParseWindowYamlNumber() {
        var window = DSWindow.fromData(1000000);
        assertThat(window).hasSize(1);
        assertThat(window.get(0).getMinIncl()).isEqualTo(0L);
        assertThat(window.get(0).getMaxExcl()).isEqualTo(1_000_000L);
    }

    /// A single-number string `"1M"` is treated as [0, 1_000_000).
    @Test
    void testParseWindowYamlString() {
        var window = DSWindow.fromData("1M");
        assertThat(window).hasSize(1);
        assertThat(window.get(0).getMinIncl()).isEqualTo(0L);
        assertThat(window.get(0).getMaxExcl()).isEqualTo(1_000_000L);
    }

    // ── Source Parsing ────────────────────────────────────────────────

    /// Bare string `"file.fvec"` -> path only, window is ALL.
    @Test
    void testSourceBareString() {
        var source = DSSource.fromData("file.fvec");
        assertThat(source.getPath()).isEqualTo("file.fvec");
        assertThat(source.getWindow()).isSameAs(DSWindow.ALL);
    }

    /// Bracket-window: `"file.fvec[0..1M]"` -> path and window parsed.
    @Test
    void testSourceBracketWindow() {
        var source = DSSource.fromData("file.fvec[0..1M]");
        assertThat(source.getPath()).isEqualTo("file.fvec");
        assertThat(source.getWindow()).hasSize(1);
        assertThat(source.getWindow().get(0).getMinIncl()).isEqualTo(0L);
        assertThat(source.getWindow().get(0).getMaxExcl()).isEqualTo(1_000_001L);
    }

    /// Paren-window: `"file.fvec(0..1000)"` -> path and window parsed.
    @Test
    void testSourceParenWindow() {
        var source = DSSource.fromData("file.fvec(0..1000)");
        assertThat(source.getPath()).isEqualTo("file.fvec");
        assertThat(source.getWindow()).hasSize(1);
        assertThat(source.getWindow().get(0).getMinIncl()).isEqualTo(1L);
        assertThat(source.getWindow().get(0).getMaxExcl()).isEqualTo(1000L);
    }

    /// Namespace: `"file.slab:content"` -> path and namespace parsed.
    @Test
    void testSourceNamespace() {
        var source = DSSource.fromData("file.slab:content");
        assertThat(source.getPath()).isEqualTo("file.slab");
        assertThat(source.getNamespace()).isEqualTo("content");
    }

    /// Namespace with window: `"file.slab:ns:[0..1K]"` -> all three parsed.
    /// `[0..1K]` has closed brackets on both ends, so end becomes 1001.
    @Test
    void testSourceNamespaceAndWindow() {
        var source = DSSource.fromData("file.slab:ns:[0..1K]");
        assertThat(source.getPath()).isEqualTo("file.slab");
        assertThat(source.getNamespace()).isEqualTo("ns");
        assertThat(source.getWindow()).hasSize(1);
        assertThat(source.getWindow().get(0).getMinIncl()).isEqualTo(0L);
        assertThat(source.getWindow().get(0).getMaxExcl()).isEqualTo(1001L);
    }

    /// Map with "source" key: `{source: "f.fvec"}` -> path parsed.
    @Test
    void testSourceYamlMapWithSource() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("source", "f.fvec");
        var source = DSSource.fromData(map);
        assertThat(source.getPath()).isEqualTo("f.fvec");
    }

    /// Map with "source" and "window" keys.
    @Test
    void testSourceYamlMapWithWindow() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("source", "f.fvec");
        map.put("window", "0..1000");
        var source = DSSource.fromData(map);
        assertThat(source.getPath()).isEqualTo("f.fvec");
        assertThat(source.getWindow()).hasSize(1);
        assertThat(source.getWindow().get(0).getMinIncl()).isEqualTo(0L);
        assertThat(source.getWindow().get(0).getMaxExcl()).isEqualTo(1001L);
    }

    // ── View Parsing ──────────────────────────────────────────────────

    /// DSView.fromData requires a Map; bare string source goes through
    /// the source key.
    @Test
    void testViewFromBareString() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("source", "file.fvec");
        var view = DSView.fromData(map);
        assertThat(view.getSource()).isNotNull();
        assertThat(view.getSource().getPath()).isEqualTo("file.fvec");
    }

    /// Map with source and window.
    @Test
    void testViewFromMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("source", "file.fvec");
        map.put("window", "0..1000");
        var view = DSView.fromData(map);
        assertThat(view.getSource().getPath()).isEqualTo("file.fvec");
        assertThat(view.getWindow()).hasSize(1);
        assertThat(view.getWindow().get(0).getMinIncl()).isEqualTo(0L);
        assertThat(view.getWindow().get(0).getMaxExcl()).isEqualTo(1001L);
    }

    /// View sugar with window embedded in source string is handled at the
    /// profile level. Here we test through DSProfile.fromData which
    /// recognises CharSequence view values.
    @Test
    void testViewSugarWithWindowString() {
        Map<String, Object> profileMap = new LinkedHashMap<>();
        profileMap.put("base", "file.fvec[0..1M]");
        var profile = DSProfile.fromData(profileMap);
        var view = profile.get("base_vectors");
        assertThat(view).isNotNull();
        assertThat(view.getSource().getPath()).isEqualTo("file.fvec");
        assertThat(view.getWindow()).hasSize(1);
        assertThat(view.getWindow().get(0).getMinIncl()).isEqualTo(0L);
        assertThat(view.getWindow().get(0).getMaxExcl()).isEqualTo(1_000_001L);
    }

    // ── Profile Parsing ───────────────────────────────────────────────

    /// Basic profile with maxk and two string views.
    @Test
    void testProfileBasic() {
        Map<String, Object> profileMap = new LinkedHashMap<>();
        profileMap.put("maxk", 100);
        profileMap.put("base", "base.fvec");
        profileMap.put("query", "query.fvec");
        var profile = DSProfile.fromData(profileMap);
        assertThat(profile).hasSize(2);
        assertThat(profile.getMaxk()).isEqualTo(100);
        assertThat(profile.containsKey("base_vectors")).isTrue();
        assertThat(profile.containsKey("query_vectors")).isTrue();
    }

    /// Alias resolution: common short names map to canonical TestDataKind names.
    @Test
    void testProfileAliasResolution() {
        Map<String, Object> profileMap = new LinkedHashMap<>();
        profileMap.put("base", "base.fvec");
        profileMap.put("query", "query.fvec");
        profileMap.put("indices", "indices.ivec");
        profileMap.put("distances", "distances.fvec");
        var profile = DSProfile.fromData(profileMap);
        assertThat(profile.containsKey("base_vectors")).isTrue();
        assertThat(profile.containsKey("query_vectors")).isTrue();
        assertThat(profile.containsKey("neighbor_indices")).isTrue();
        assertThat(profile.containsKey("neighbor_distances")).isTrue();
    }

    /// Non-standard keys are preserved as-is (no alias match).
    @Test
    void testProfileCustomFacetsPreserved() {
        Map<String, Object> profileMap = new LinkedHashMap<>();
        profileMap.put("base", "base.fvec");
        profileMap.put("my_custom_facet", "custom.dat");
        var profile = DSProfile.fromData(profileMap);
        assertThat(profile.containsKey("base_vectors")).isTrue();
        assertThat(profile.containsKey("my_custom_facet")).isTrue();
    }

    // ── Profile Group ─────────────────────────────────────────────────

    /// Default profile views are inherited by child profiles.
    @Test
    void testProfileGroupInheritance() {
        Map<String, Object> defaultMap = new LinkedHashMap<>();
        defaultMap.put("maxk", 100);
        defaultMap.put("base", "base.fvec");
        defaultMap.put("query", "query.fvec");

        Map<String, Object> childMap = new LinkedHashMap<>();
        childMap.put("indices", "indices.ivec");

        Map<String, Object> groupMap = new LinkedHashMap<>();
        groupMap.put("default", defaultMap);
        groupMap.put("child", childMap);

        var group = DSProfileGroup.fromData(groupMap);
        var child = group.get("child");
        assertThat(child).isNotNull();
        // Child inherits base and query from default
        assertThat(child.containsKey("base_vectors")).isTrue();
        assertThat(child.containsKey("query_vectors")).isTrue();
        // Child has its own indices
        assertThat(child.containsKey("neighbor_indices")).isTrue();
        // Child inherits maxk
        assertThat(child.getMaxk()).isEqualTo(100);
    }

    /// Child can override maxk from default.
    @Test
    void testProfileGroupChildMaxkOverride() {
        Map<String, Object> defaultMap = new LinkedHashMap<>();
        defaultMap.put("maxk", 100);
        defaultMap.put("base", "base.fvec");

        Map<String, Object> childMap = new LinkedHashMap<>();
        childMap.put("maxk", 50);

        Map<String, Object> groupMap = new LinkedHashMap<>();
        groupMap.put("default", defaultMap);
        groupMap.put("child", childMap);

        var group = DSProfileGroup.fromData(groupMap);
        assertThat(group.get("default").getMaxk()).isEqualTo(100);
        assertThat(group.get("child").getMaxk()).isEqualTo(50);
    }

    /// Custom (non-standard) facets are inherited from the default profile.
    @Test
    void testProfileGroupCustomFacetsInherited() {
        Map<String, Object> defaultMap = new LinkedHashMap<>();
        defaultMap.put("base", "base.fvec");
        defaultMap.put("my_extra", "extra.dat");

        Map<String, Object> childMap = new LinkedHashMap<>();
        childMap.put("query", "query.fvec");

        Map<String, Object> groupMap = new LinkedHashMap<>();
        groupMap.put("default", defaultMap);
        groupMap.put("child", childMap);

        var group = DSProfileGroup.fromData(groupMap);
        var child = group.get("child");
        assertThat(child.containsKey("my_extra")).isTrue();
        assertThat(child.containsKey("query_vectors")).isTrue();
    }
}
