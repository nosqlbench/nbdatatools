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

import io.nosqlbench.vectordata.discovery.TestDataGroup;
import io.nosqlbench.vectordata.discovery.vector.TestDataView;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/// Tests for typed dataset attributes and variables access.
///
/// Mirrors the attribute and variable access patterns from the vectordata-rs
/// reference implementation's DatasetAttributes and DatasetConfig types.
class AttributesAndVariablesTest {

    @TempDir
    Path tempDir;

    /// Verifies typed boolean attribute accessors for dataset quality flags.
    @Test
    void testBooleanAttributes() throws Exception {
        TestVectorFileHelper.createFvec(tempDir, "base.fvec", 4, 10);
        TestVectorFileHelper.createDatasetYaml(tempDir,
            "attributes:\n" +
            "  distance_function: L2\n" +
            "  is_normalized: true\n" +
            "  is_zero_vector_free: true\n" +
            "  is_duplicate_vector_free: false\n" +
            "profiles:\n  default:\n    base_vectors: base.fvec\n");

        TestDataGroup group = new TestDataGroup(tempDir);
        TestDataView view = group.profile("default");

        assertThat(view.isNormalized()).isPresent().hasValue(true);
        assertThat(view.isZeroVectorFree()).isPresent().hasValue(true);
        assertThat(view.isDuplicateVectorFree()).isPresent().hasValue(false);
    }

    /// Verifies boolean attributes return empty when not specified.
    @Test
    void testBooleanAttributesAbsent() throws Exception {
        TestVectorFileHelper.createFvec(tempDir, "base.fvec", 4, 10);
        TestVectorFileHelper.createDatasetYaml(tempDir,
            "attributes:\n  distance_function: L2\n" +
            "profiles:\n  default:\n    base_vectors: base.fvec\n");

        TestDataGroup group = new TestDataGroup(tempDir);
        TestDataView view = group.profile("default");

        assertThat(view.isNormalized()).isEmpty();
        assertThat(view.isZeroVectorFree()).isEmpty();
        assertThat(view.isDuplicateVectorFree()).isEmpty();
    }

    /// Verifies generic attribute access for model, license, vendor, and custom attributes.
    @Test
    void testGenericAttributeAccess() throws Exception {
        TestVectorFileHelper.createFvec(tempDir, "base.fvec", 4, 10);
        TestVectorFileHelper.createDatasetYaml(tempDir,
            "attributes:\n" +
            "  distance_function: COSINE\n" +
            "  model: \"CLIP ViT-B/32\"\n" +
            "  license: Apache-2.0\n" +
            "  vendor: OpenAI\n" +
            "  dimension: 4\n" +
            "profiles:\n  default:\n    base_vectors: base.fvec\n");

        TestDataGroup group = new TestDataGroup(tempDir);

        assertThat(group.getAttribute("model")).isEqualTo("CLIP ViT-B/32");
        assertThat(group.getAttribute("license")).isEqualTo("Apache-2.0");
        assertThat(group.getAttribute("vendor")).isEqualTo("OpenAI");
        assertThat(group.getAttribute("dimension")).isEqualTo(4);
        assertThat(group.getAttribute("nonexistent")).isNull();
    }

    /// Verifies variables loaded from inline variables section in dataset.yaml.
    @Test
    void testInlineVariables() throws Exception {
        TestVectorFileHelper.createFvec(tempDir, "base.fvec", 4, 10);
        TestVectorFileHelper.createDatasetYaml(tempDir,
            "attributes:\n  distance_function: L2\n" +
            "variables:\n" +
            "  base_count: \"1000000\"\n" +
            "  query_count: \"10000\"\n" +
            "  normalized: \"true\"\n" +
            "profiles:\n  default:\n    base_vectors: base.fvec\n");

        TestDataGroup group = new TestDataGroup(tempDir);

        assertThat(group.getVariable("base_count")).isEqualTo("1000000");
        assertThat(group.getVariable("query_count")).isEqualTo("10000");
        assertThat(group.getVariable("nonexistent")).isNull();
    }

    /// Verifies typed variable accessors (long and boolean).
    @Test
    void testTypedVariableAccessors() throws Exception {
        TestVectorFileHelper.createFvec(tempDir, "base.fvec", 4, 10);
        TestVectorFileHelper.createDatasetYaml(tempDir,
            "attributes:\n  distance_function: L2\n" +
            "variables:\n" +
            "  base_count: \"1000000\"\n" +
            "  normalized: \"true\"\n" +
            "profiles:\n  default:\n    base_vectors: base.fvec\n");

        TestDataGroup group = new TestDataGroup(tempDir);

        OptionalLong count = group.getVariableAsLong("base_count");
        assertThat(count).isPresent();
        assertThat(count.getAsLong()).isEqualTo(1_000_000L);

        Optional<Boolean> normalized = group.getVariableAsBool("normalized");
        assertThat(normalized).isPresent().hasValue(true);

        assertThat(group.getVariableAsLong("nonexistent")).isEmpty();
        assertThat(group.getVariableAsBool("nonexistent")).isEmpty();
    }

    /// Verifies variables loaded from variables.yaml file.
    @Test
    void testVariablesFromFile() throws Exception {
        TestVectorFileHelper.createFvec(tempDir, "base.fvec", 4, 10);
        TestVectorFileHelper.createDatasetYaml(tempDir,
            "attributes:\n  distance_function: L2\n" +
            "profiles:\n  default:\n    base_vectors: base.fvec\n");

        // Create variables.yaml alongside dataset.yaml
        Files.writeString(tempDir.resolve("variables.yaml"),
            "base_count: \"5000000\"\n" +
            "seed: \"42\"\n");

        TestDataGroup group = new TestDataGroup(tempDir);

        assertThat(group.getVariable("base_count")).isEqualTo("5000000");
        assertThat(group.getVariable("seed")).isEqualTo("42");
        assertThat(group.getVariableAsLong("seed")).isPresent();
        assertThat(group.getVariableAsLong("seed").getAsLong()).isEqualTo(42L);
    }

    /// Verifies variables.yaml overlays inline variables (file takes precedence).
    @Test
    void testVariablesFileOverlaysInline() throws Exception {
        TestVectorFileHelper.createFvec(tempDir, "base.fvec", 4, 10);
        TestVectorFileHelper.createDatasetYaml(tempDir,
            "attributes:\n  distance_function: L2\n" +
            "variables:\n" +
            "  base_count: \"1000000\"\n" +
            "  seed: \"1\"\n" +
            "profiles:\n  default:\n    base_vectors: base.fvec\n");

        // variables.yaml overrides base_count, adds new var
        Files.writeString(tempDir.resolve("variables.yaml"),
            "base_count: \"5000000\"\n" +
            "threads: \"8\"\n");

        TestDataGroup group = new TestDataGroup(tempDir);

        // base_count overridden by file
        assertThat(group.getVariable("base_count")).isEqualTo("5000000");
        // seed from inline (not overridden)
        assertThat(group.getVariable("seed")).isEqualTo("1");
        // threads from file only
        assertThat(group.getVariable("threads")).isEqualTo("8");
    }

    /// Verifies getVariables() returns all variables as unmodifiable map.
    @Test
    void testGetAllVariables() throws Exception {
        TestVectorFileHelper.createFvec(tempDir, "base.fvec", 4, 10);
        TestVectorFileHelper.createDatasetYaml(tempDir,
            "attributes:\n  distance_function: L2\n" +
            "variables:\n" +
            "  a: \"1\"\n" +
            "  b: \"2\"\n" +
            "profiles:\n  default:\n    base_vectors: base.fvec\n");

        TestDataGroup group = new TestDataGroup(tempDir);
        Map<String, String> vars = group.getVariables();

        assertThat(vars).hasSize(2);
        assertThat(vars).containsEntry("a", "1");
        assertThat(vars).containsEntry("b", "2");
    }

    /// Verifies empty variables when neither section nor file exists.
    @Test
    void testNoVariables() throws Exception {
        TestVectorFileHelper.createFvec(tempDir, "base.fvec", 4, 10);
        TestVectorFileHelper.createDatasetYaml(tempDir,
            "attributes:\n  distance_function: L2\n" +
            "profiles:\n  default:\n    base_vectors: base.fvec\n");

        TestDataGroup group = new TestDataGroup(tempDir);

        assertThat(group.getVariables()).isEmpty();
        assertThat(group.getVariable("anything")).isNull();
    }
}
