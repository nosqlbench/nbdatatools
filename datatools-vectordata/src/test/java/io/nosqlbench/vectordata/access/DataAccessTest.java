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
import io.nosqlbench.vectordata.layoutv2.DSWindow;
import io.nosqlbench.vectordata.layoutv2.DSInterval;
import io.nosqlbench.vectordata.spec.datasets.impl.xvec.BaseVectorsXvecImpl;
import io.nosqlbench.vectordata.spec.datasets.types.BaseVectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/// Integration tests for local vector data access.
///
/// Mirrors test scenarios from vectordata-rs `tests/data_access.rs`.
/// Tests cover local file reading, windowed access, and basic dataset loading.
class DataAccessTest {

    @TempDir
    Path tempDir;

    /// Verifies local fvec file access: dimension, count, and vector values.
    ///
    /// Pattern: `vector[i][j] = i * dim + j`
    @Test
    void testLocalVectorViewFvec() throws Exception {
        int dim = 8;
        int count = 1000;
        TestVectorFileHelper.createFvec(tempDir, "base.fvec", dim, count);
        TestVectorFileHelper.createDatasetYaml(tempDir,
            "attributes:\n  distance_function: L2\nprofiles:\n  default:\n    base_vectors: base.fvec\n");

        TestDataGroup group = new TestDataGroup(tempDir);
        TestDataView view = group.profile("default");
        Optional<BaseVectors> baseOpt = view.getBaseVectors();

        assertThat(baseOpt).isPresent();
        BaseVectors base = baseOpt.get();

        assertThat(base.getVectorDimensions()).isEqualTo(dim);
        assertThat(base.getCount()).isEqualTo(count);

        // First vector: [0, 1, 2, 3, 4, 5, 6, 7]
        float[] v0 = base.get(0);
        assertThat(v0).hasSize(dim);
        for (int j = 0; j < dim; j++) {
            assertThat(v0[j]).isEqualTo((float) j);
        }

        // Last vector: [999*8, 999*8+1, ..., 999*8+7]
        float[] vLast = base.get(999);
        assertThat(vLast[0]).isEqualTo(999f * 8f);
    }

    /// Verifies local mvec (f16) file access with f32 conversion.
    ///
    /// Values stored as half-precision should be approximately correct
    /// when read back as f32.
    @Test
    void testLocalVectorViewMvec() throws Exception {
        int dim = 4;
        int count = 100;
        TestVectorFileHelper.createMvec(tempDir, "base.mvec", dim, count);
        TestVectorFileHelper.createDatasetYaml(tempDir,
            "attributes:\n  distance_function: L2\nprofiles:\n  default:\n    base_vectors: base.mvec\n");

        TestDataGroup group = new TestDataGroup(tempDir);
        TestDataView view = group.profile("default");
        Optional<BaseVectors> baseOpt = view.getBaseVectors();

        assertThat(baseOpt).isPresent();
        BaseVectors base = baseOpt.get();

        assertThat(base.getVectorDimensions()).isEqualTo(dim);
        assertThat(base.getCount()).isEqualTo(count);

        // First vector: approximately [0.0, 1.0, 2.0, 3.0]
        float[] v0 = base.get(0);
        assertThat(v0).hasSize(dim);
        assertThat((double) v0[0]).isCloseTo(0.0, org.assertj.core.data.Offset.offset(0.01));
        assertThat((double) v0[1]).isCloseTo(1.0, org.assertj.core.data.Offset.offset(0.01));
        assertThat((double) v0[2]).isCloseTo(2.0, org.assertj.core.data.Offset.offset(0.01));
        assertThat((double) v0[3]).isCloseTo(3.0, org.assertj.core.data.Offset.offset(0.01));
    }

    /// Verifies windowed access: only a subset of vectors is visible.
    ///
    /// Window [100, 300) means view.get(0) reads file vector 100,
    /// view.get(199) reads file vector 299, and view.get(200) is out of bounds.
    @Test
    void testLocalVectorViewWindowed() throws Exception {
        int dim = 4;
        int count = 1000;
        TestVectorFileHelper.createFvec(tempDir, "base.fvec", dim, count);

        // Window: vectors 100..300 (200 vectors)
        String yaml = "attributes:\n  distance_function: L2\n" +
            "profiles:\n  default:\n    base_vectors: \"base.fvec[100..300)\"\n";
        TestVectorFileHelper.createDatasetYaml(tempDir, yaml);

        TestDataGroup group = new TestDataGroup(tempDir);
        TestDataView view = group.profile("default");
        Optional<BaseVectors> baseOpt = view.getBaseVectors();

        assertThat(baseOpt).isPresent();
        BaseVectors base = baseOpt.get();

        assertThat(base.getCount()).isEqualTo(200);
        assertThat(base.getVectorDimensions()).isEqualTo(dim);

        // get(0) should return file vector 100: [400, 401, 402, 403]
        float[] v0 = base.get(0);
        assertThat(v0[0]).isEqualTo(400f);
        assertThat(v0[1]).isEqualTo(401f);
        assertThat(v0[2]).isEqualTo(402f);
        assertThat(v0[3]).isEqualTo(403f);

        // get(199) should return file vector 299: [1196, 1197, 1198, 1199]
        float[] vLast = base.get(199);
        assertThat(vLast[0]).isEqualTo(1196f);
        assertThat(vLast[3]).isEqualTo(1199f);
    }

    /// Verifies dataset loading from a local directory via TestDataGroup.
    @Test
    void testDatasetLoaderLocalFile() throws Exception {
        int dim = 4;
        int count = 50;
        TestVectorFileHelper.createFvec(tempDir, "base.fvec", dim, count);
        TestVectorFileHelper.createIvec(tempDir, "neighbors.ivec", 10, count);
        String yaml = "attributes:\n  distance_function: L2\n" +
            "profiles:\n  default:\n    base_vectors: base.fvec\n    neighbor_indices: neighbors.ivec\n";
        TestVectorFileHelper.createDatasetYaml(tempDir, yaml);

        TestDataGroup group = new TestDataGroup(tempDir);

        // Profile access
        TestDataView view = group.profile("default");
        assertThat(view).isNotNull();

        // Base vectors
        Optional<BaseVectors> base = view.getBaseVectors();
        assertThat(base).isPresent();
        assertThat(base.get().getCount()).isEqualTo(count);
        assertThat(base.get().getVectorDimensions()).isEqualTo(dim);

        // Neighbor indices
        var indices = view.getNeighborIndices();
        assertThat(indices).isPresent();
        assertThat(indices.get().getCount()).isEqualTo(count);
        assertThat(indices.get().getMaxK()).isEqualTo(10);
    }

    /// Verifies multi-facet dataset with query vectors and distances.
    @Test
    void testDatasetMultiFacet() throws Exception {
        int dim = 4;
        TestVectorFileHelper.createFvec(tempDir, "base.fvec", dim, 10);
        TestVectorFileHelper.createFvec(tempDir, "query.fvec", dim, 5);
        TestVectorFileHelper.createIvec(tempDir, "neighbors.ivec", 3, 5);
        TestVectorFileHelper.createFvec(tempDir, "distances.fvec", 3, 5);
        String yaml = "attributes:\n  distance_function: L2\n  dimension: 4\n" +
            "profiles:\n  default:\n" +
            "    base_vectors: base.fvec\n" +
            "    query_vectors: query.fvec\n" +
            "    neighbor_indices: neighbors.ivec\n" +
            "    neighbor_distances: distances.fvec\n";
        TestVectorFileHelper.createDatasetYaml(tempDir, yaml);

        TestDataGroup group = new TestDataGroup(tempDir);
        TestDataView view = group.profile("default");

        assertThat(view.getBaseVectors()).isPresent();
        assertThat(view.getBaseVectors().get().getCount()).isEqualTo(10);

        assertThat(view.getQueryVectors()).isPresent();
        assertThat(view.getQueryVectors().get().getCount()).isEqualTo(5);

        assertThat(view.getNeighborIndices()).isPresent();
        assertThat(view.getNeighborIndices().get().getCount()).isEqualTo(5);
        assertThat(view.getNeighborIndices().get().getMaxK()).isEqualTo(3);

        assertThat(view.getNeighborDistances()).isPresent();
        assertThat(view.getNeighborDistances().get().getCount()).isEqualTo(5);
    }

    /// Verifies profile inheritance — child profiles inherit views from default.
    @Test
    void testProfileInheritance() throws Exception {
        int dim = 4;
        TestVectorFileHelper.createFvec(tempDir, "base.fvec", dim, 100);
        TestVectorFileHelper.createFvec(tempDir, "query.fvec", dim, 10);
        TestVectorFileHelper.createFvec(tempDir, "small_query.fvec", dim, 5);
        String yaml = "attributes:\n  distance_function: COSINE\n" +
            "profiles:\n" +
            "  default:\n" +
            "    base_vectors: base.fvec\n" +
            "    query_vectors: query.fvec\n" +
            "  small:\n" +
            "    query_vectors: small_query.fvec\n";
        TestVectorFileHelper.createDatasetYaml(tempDir, yaml);

        TestDataGroup group = new TestDataGroup(tempDir);

        // Default profile has both
        TestDataView defaultView = group.profile("default");
        assertThat(defaultView.getBaseVectors()).isPresent();
        assertThat(defaultView.getQueryVectors().get().getCount()).isEqualTo(10);

        // Small profile inherits base_vectors from default, overrides query_vectors
        TestDataView smallView = group.profile("small");
        assertThat(smallView.getBaseVectors()).isPresent();
        assertThat(smallView.getBaseVectors().get().getCount()).isEqualTo(100);
        assertThat(smallView.getQueryVectors()).isPresent();
        assertThat(smallView.getQueryVectors().get().getCount()).isEqualTo(5);
    }
}
