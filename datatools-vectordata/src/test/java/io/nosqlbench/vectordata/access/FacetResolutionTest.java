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

import io.nosqlbench.vectordata.spec.datasets.types.TestDataKind;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/// Tests for facet alias resolution and standard kind checks via
/// [TestDataKind#fromOptionalString(String)].
class FacetResolutionTest {

    /// "base" and "train" should both resolve to [TestDataKind#base_vectors].
    @Test
    void testAliasBase() {
        assertThat(TestDataKind.fromOptionalString("base"))
            .isPresent()
            .contains(TestDataKind.base_vectors);
        assertThat(TestDataKind.fromOptionalString("train"))
            .isPresent()
            .contains(TestDataKind.base_vectors);
    }

    /// "query", "queries", and "test" should all resolve to [TestDataKind#query_vectors].
    @Test
    void testAliasQuery() {
        assertThat(TestDataKind.fromOptionalString("query"))
            .isPresent()
            .contains(TestDataKind.query_vectors);
        assertThat(TestDataKind.fromOptionalString("queries"))
            .isPresent()
            .contains(TestDataKind.query_vectors);
        assertThat(TestDataKind.fromOptionalString("test"))
            .isPresent()
            .contains(TestDataKind.query_vectors);
    }

    /// "indices", "neighbors", "ground_truth", and "gt" should all resolve to
    /// [TestDataKind#neighbor_indices].
    @Test
    void testAliasIndices() {
        assertThat(TestDataKind.fromOptionalString("indices"))
            .isPresent()
            .contains(TestDataKind.neighbor_indices);
        assertThat(TestDataKind.fromOptionalString("neighbors"))
            .isPresent()
            .contains(TestDataKind.neighbor_indices);
        assertThat(TestDataKind.fromOptionalString("ground_truth"))
            .isPresent()
            .contains(TestDataKind.neighbor_indices);
        assertThat(TestDataKind.fromOptionalString("gt"))
            .isPresent()
            .contains(TestDataKind.neighbor_indices);
    }

    /// "distances" should resolve to [TestDataKind#neighbor_distances].
    @Test
    void testAliasDistances() {
        assertThat(TestDataKind.fromOptionalString("distances"))
            .isPresent()
            .contains(TestDataKind.neighbor_distances);
    }

    /// Unrecognized names should return an empty Optional.
    @Test
    void testAliasUnknown() {
        assertThat(TestDataKind.fromOptionalString("unknown"))
            .isEmpty();
        assertThat(TestDataKind.fromOptionalString("my_custom_facet"))
            .isEmpty();
    }

    /// Canonical enum names should resolve to themselves.
    @Test
    void testCanonicalNameResolves() {
        assertThat(TestDataKind.fromOptionalString("base_vectors"))
            .isPresent()
            .contains(TestDataKind.base_vectors);
        assertThat(TestDataKind.fromOptionalString("query_vectors"))
            .isPresent()
            .contains(TestDataKind.query_vectors);
        assertThat(TestDataKind.fromOptionalString("neighbor_indices"))
            .isPresent()
            .contains(TestDataKind.neighbor_indices);
        assertThat(TestDataKind.fromOptionalString("neighbor_distances"))
            .isPresent()
            .contains(TestDataKind.neighbor_distances);
    }
}
