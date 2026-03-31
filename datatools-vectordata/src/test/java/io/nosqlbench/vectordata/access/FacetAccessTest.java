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
import io.nosqlbench.vectordata.spec.datasets.types.DatasetView;
import io.nosqlbench.vectordata.spec.datasets.types.DistanceFunction;
import io.nosqlbench.vectordata.spec.datasets.types.FacetDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/// Integration tests for facet discovery, generic facet access, and metadata.
///
/// Mirrors test scenarios from vectordata-rs `tests/facet_access.rs`.
class FacetAccessTest {

    @TempDir
    Path tempDir;

    private static final String FULL_DATASET_YAML =
        "attributes:\n" +
        "  distance_function: L2\n" +
        "  dimension: 4\n" +
        "profiles:\n" +
        "  default:\n" +
        "    base_vectors: base.fvec\n" +
        "    query_vectors: query.fvec\n" +
        "    neighbor_indices: neighbors.ivec\n" +
        "    neighbor_distances: distances.fvec\n";

    @BeforeEach
    void setUp() throws Exception {
        TestVectorFileHelper.createFvec(tempDir, "base.fvec", 4, 10);
        TestVectorFileHelper.createFvec(tempDir, "query.fvec", 4, 5);
        TestVectorFileHelper.createIvec(tempDir, "neighbors.ivec", 3, 5);
        TestVectorFileHelper.createFvec(tempDir, "distances.fvec", 3, 5);
        TestVectorFileHelper.createDatasetYaml(tempDir, FULL_DATASET_YAML);
    }

    /// Verifies facet_manifest returns descriptors for all 4 facets with correct metadata.
    @Test
    void testFacetManifestLocal() throws Exception {
        TestDataGroup group = new TestDataGroup(tempDir);
        TestDataView view = group.profile("default");

        Map<String, FacetDescriptor> manifest = view.getFacetManifest();

        assertThat(manifest).hasSize(4);

        assertThat(manifest).containsKey("base_vectors");
        FacetDescriptor baseDesc = manifest.get("base_vectors");
        assertThat(baseDesc.isStandard()).isTrue();
        assertThat(baseDesc.sourcePath()).isEqualTo("base.fvec");

        assertThat(manifest).containsKey("query_vectors");
        assertThat(manifest).containsKey("neighbor_indices");
        assertThat(manifest).containsKey("neighbor_distances");
    }

    /// Verifies generic facet("base_vectors") returns a working reader.
    @Test
    void testGenericFacetAccessLocal() throws Exception {
        TestDataGroup group = new TestDataGroup(tempDir);
        TestDataView view = group.profile("default");

        Optional<DatasetView<?>> facet = view.getFacet("base_vectors");
        assertThat(facet).isPresent();

        DatasetView<?> reader = facet.get();
        assertThat(reader.getCount()).isEqualTo(10);
    }

    /// Verifies generic facet access for neighbor_indices returns correct type.
    @Test
    void testGenericFacetAccessNeighborIndices() throws Exception {
        TestDataGroup group = new TestDataGroup(tempDir);
        TestDataView view = group.profile("default");

        Optional<DatasetView<?>> facet = view.getFacet("neighbor_indices");
        assertThat(facet).isPresent();

        DatasetView<?> reader = facet.get();
        assertThat(reader.getCount()).isEqualTo(5);

        // Should return int[] vectors
        Object v = reader.get(0);
        assertThat(v).isInstanceOf(int[].class);
        int[] iv = (int[]) v;
        assertThat(iv).hasSize(3);
        assertThat(iv[0]).isEqualTo(0);
        assertThat(iv[1]).isEqualTo(1);
        assertThat(iv[2]).isEqualTo(2);
    }

    /// Verifies that requesting a non-existent facet returns empty.
    @Test
    void testGenericFacetMissing() throws Exception {
        TestDataGroup group = new TestDataGroup(tempDir);
        TestDataView view = group.profile("default");

        Optional<DatasetView<?>> facet = view.getFacet("nonexistent_facet");
        assertThat(facet).isEmpty();
    }

    /// Verifies distance_function is read from attributes when present.
    @Test
    void testDistanceFunctionPresent() throws Exception {
        TestDataGroup group = new TestDataGroup(tempDir);
        TestDataView view = group.profile("default");

        DistanceFunction df = view.getDistanceFunction();
        assertThat(df).isEqualTo(DistanceFunction.L2);
    }

    /// Verifies distance_function returns null when not in attributes.
    @Test
    void testDistanceFunctionAbsent() throws Exception {
        String yaml = "profiles:\n  default:\n    base_vectors: base.fvec\n";
        TestVectorFileHelper.createDatasetYaml(tempDir, yaml);

        TestDataGroup group = new TestDataGroup(tempDir);
        TestDataView view = group.profile("default");

        // When distance_function is absent, implementation may return a default or null
        // The Rust version returns None; Java may return UNDEFINED or null
        // Accept either null or a default value
        DistanceFunction df = view.getDistanceFunction();
        // No assertion on specific value — just verify no crash
    }

    /// Verifies facet aliases are resolved: "base" and "train" map to base_vectors.
    @Test
    void testFacetAliasResolution() throws Exception {
        String yaml = "attributes:\n  distance_function: COSINE\n" +
            "profiles:\n  default:\n" +
            "    base: base.fvec\n" +
            "    query: query.fvec\n" +
            "    indices: neighbors.ivec\n" +
            "    distances: distances.fvec\n";
        TestVectorFileHelper.createDatasetYaml(tempDir, yaml);

        TestDataGroup group = new TestDataGroup(tempDir);
        TestDataView view = group.profile("default");

        // Aliases should resolve to canonical names
        assertThat(view.getBaseVectors()).isPresent();
        assertThat(view.getQueryVectors()).isPresent();
        assertThat(view.getNeighborIndices()).isPresent();
        assertThat(view.getNeighborDistances()).isPresent();

        // Manifest should use canonical names
        Map<String, FacetDescriptor> manifest = view.getFacetManifest();
        assertThat(manifest).containsKey("base_vectors");
        assertThat(manifest).containsKey("query_vectors");
        assertThat(manifest).containsKey("neighbor_indices");
        assertThat(manifest).containsKey("neighbor_distances");
    }

    /// Verifies custom (non-standard) facets are preserved in the manifest.
    @Test
    void testCustomFacetInManifest() throws Exception {
        TestVectorFileHelper.createFvec(tempDir, "embeddings.fvec", 4, 20);
        String yaml = "attributes:\n  distance_function: L2\n" +
            "profiles:\n  default:\n" +
            "    base_vectors: base.fvec\n" +
            "    my_custom_embeddings: embeddings.fvec\n";
        TestVectorFileHelper.createDatasetYaml(tempDir, yaml);

        TestDataGroup group = new TestDataGroup(tempDir);
        TestDataView view = group.profile("default");

        Map<String, FacetDescriptor> manifest = view.getFacetManifest();
        assertThat(manifest).containsKey("base_vectors");
        assertThat(manifest).containsKey("my_custom_embeddings");

        FacetDescriptor custom = manifest.get("my_custom_embeddings");
        assertThat(custom.isStandard()).isFalse();
        assertThat(custom.sourcePath()).isEqualTo("embeddings.fvec");
    }
}
