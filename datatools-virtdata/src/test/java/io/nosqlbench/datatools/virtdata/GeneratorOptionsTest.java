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

package io.nosqlbench.datatools.virtdata;

import io.nosqlbench.datatools.virtdata.sampling.LerpSampler;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class GeneratorOptionsTest {

    @Test
    void testDefaults() {
        GeneratorOptions options = GeneratorOptions.defaults();

        assertEquals(VectorGenFactory.Mode.AUTO, options.mode());
        assertFalse(options.useLerp());
        assertEquals(LerpSampler.DEFAULT_TABLE_SIZE, options.lerpTableSize());
        assertFalse(options.normalizeL2());
    }

    @Test
    void testBuilder() {
        GeneratorOptions options = GeneratorOptions.builder()
            .mode(VectorGenFactory.Mode.SCALAR)
            .useLerp(true)
            .lerpTableSize(2048)
            .normalizeL2(true)
            .build();

        assertEquals(VectorGenFactory.Mode.SCALAR, options.mode());
        assertTrue(options.useLerp());
        assertEquals(2048, options.lerpTableSize());
        assertTrue(options.normalizeL2());
    }

    @Test
    void testBuilderValidation() {
        // Table size must be >= 16
        assertThrows(IllegalArgumentException.class, () ->
            GeneratorOptions.builder().lerpTableSize(15).build());

        assertThrows(IllegalArgumentException.class, () ->
            GeneratorOptions.builder().lerpTableSize(0).build());

        // Valid table sizes
        assertDoesNotThrow(() ->
            GeneratorOptions.builder().lerpTableSize(16).build());
        assertDoesNotThrow(() ->
            GeneratorOptions.builder().lerpTableSize(4096).build());
    }

    @Test
    void testToBuilder() {
        GeneratorOptions original = GeneratorOptions.builder()
            .useLerp(true)
            .normalizeL2(true)
            .build();

        // Create modified copy
        GeneratorOptions modified = original.toBuilder()
            .lerpTableSize(2048)
            .build();

        // Original unchanged
        assertTrue(original.useLerp());
        assertEquals(LerpSampler.DEFAULT_TABLE_SIZE, original.lerpTableSize());

        // Modified has new value
        assertTrue(modified.useLerp());
        assertEquals(2048, modified.lerpTableSize());
        assertTrue(modified.normalizeL2());
    }

    @Test
    void testToString() {
        GeneratorOptions options = GeneratorOptions.builder()
            .mode(VectorGenFactory.Mode.PANAMA)
            .useLerp(true)
            .build();

        String str = options.toString();
        assertTrue(str.contains("PANAMA"));
        assertTrue(str.contains("useLerp=true"));
    }
}
