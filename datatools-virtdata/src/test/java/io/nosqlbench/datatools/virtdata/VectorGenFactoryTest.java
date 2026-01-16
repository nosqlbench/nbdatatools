package io.nosqlbench.datatools.virtdata;

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

import io.nosqlbench.vshapes.model.VectorSpaceModel;
import org.junit.jupiter.api.Test;

import java.util.function.LongFunction;

import static org.junit.jupiter.api.Assertions.*;

class VectorGenFactoryTest {

    @Test
    void testAutoMode() {
        VectorSpaceModel model = new VectorSpaceModel(1000, 64);
        LongFunction<float[]> gen = VectorGenFactory.create(model, VectorGenFactory.Mode.AUTO);

        float[] vector = gen.apply(42);
        assertNotNull(vector);
        assertEquals(64, vector.length);
    }

    @Test
    void testScalarMode() {
        VectorSpaceModel model = new VectorSpaceModel(1000, 64);
        LongFunction<float[]> gen = VectorGenFactory.create(model, VectorGenFactory.Mode.SCALAR);

        assertInstanceOf(ScalarDimensionDistributionGenerator.class, gen);

        float[] vector = gen.apply(42);
        assertNotNull(vector);
        assertEquals(64, vector.length);
    }

    @Test
    void testScalarMatchesAuto() {
        // Scalar and Auto should produce identical results
        VectorSpaceModel model = new VectorSpaceModel(1000, 32);
        LongFunction<float[]> auto = VectorGenFactory.create(model, VectorGenFactory.Mode.AUTO);
        LongFunction<float[]> scalar = VectorGenFactory.create(model, VectorGenFactory.Mode.SCALAR);

        for (long ordinal = 0; ordinal < 100; ordinal++) {
            float[] autoVec = auto.apply(ordinal);
            float[] scalarVec = scalar.apply(ordinal);

            assertArrayEquals(autoVec, scalarVec,
                "Scalar and Auto should produce identical vectors for ordinal " + ordinal);
        }
    }

    @Test
    void testPanamaAvailability() {
        // Just verify the check doesn't throw
        boolean available = VectorGenFactory.isPanamaAvailable();
        System.out.println("Panama available: " + available);
        System.out.println("Auto implementation: " + VectorGenFactory.getAutoImplementationName());
    }

    @Test
    void testPanamaMode() {
        VectorSpaceModel model = new VectorSpaceModel(1000, 64);

        if (VectorGenFactory.isPanamaAvailable()) {
            LongFunction<float[]> gen = VectorGenFactory.create(model, VectorGenFactory.Mode.PANAMA);
            float[] vector = gen.apply(42);
            assertEquals(64, vector.length);
        } else {
            assertThrows(UnsupportedOperationException.class,
                () -> VectorGenFactory.create(model, VectorGenFactory.Mode.PANAMA));
        }
    }

    @Test
    void testSimpleCreate() {
        VectorSpaceModel model = new VectorSpaceModel(1000, 128);
        DimensionDistributionGenerator gen = VectorGenFactory.create(model);

        assertNotNull(gen);
        float[] vector = gen.apply(0);
        assertEquals(128, vector.length);
    }
}
