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

import io.nosqlbench.vshapes.model.ScalarModel;
import io.nosqlbench.vshapes.model.NormalScalarModel;
import io.nosqlbench.vshapes.model.VectorSpaceModel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class VectorSpaceModelTest {

    @Test
    void testBasicConstruction() {
        VectorSpaceModel model = new VectorSpaceModel(1000, 128);
        assertEquals(1000, model.uniqueVectors());
        assertEquals(128, model.dimensions());
    }

    @Test
    void testCustomNormalConstruction() {
        VectorSpaceModel model = new VectorSpaceModel(500, 64, 2.0, 0.5);
        assertEquals(500, model.uniqueVectors());
        assertEquals(64, model.dimensions());

        // All components should have the same parameters
        NormalScalarModel[] normals = model.normalScalarModels();
        for (int d = 0; d < 64; d++) {
            assertEquals(2.0, normals[d].getMean());
            assertEquals(0.5, normals[d].getStdDev());
        }
    }

    @Test
    void testComponentSpecificModels() {
        NormalScalarModel[] models = new NormalScalarModel[3];
        models[0] = new NormalScalarModel(0.0, 1.0);
        models[1] = new NormalScalarModel(5.0, 2.0);
        models[2] = new NormalScalarModel(-1.0, 0.5);

        VectorSpaceModel vsm = new VectorSpaceModel(1000, models);
        assertEquals(1000, vsm.uniqueVectors());
        assertEquals(3, vsm.dimensions());

        // Access through typed normalScalarModels() method
        NormalScalarModel[] retrieved = vsm.normalScalarModels();
        assertEquals(0.0, retrieved[0].getMean());
        assertEquals(5.0, retrieved[1].getMean());
        assertEquals(-1.0, retrieved[2].getMean());
    }

    @Test
    void testInvalidParameters() {
        assertThrows(IllegalArgumentException.class, () -> new VectorSpaceModel(0, 128));
        assertThrows(IllegalArgumentException.class, () -> new VectorSpaceModel(-1, 128));
        assertThrows(NullPointerException.class, () -> new VectorSpaceModel(1000, (ScalarModel[]) null));
        assertThrows(IllegalArgumentException.class, () -> new VectorSpaceModel(1000, new NormalScalarModel[0]));
    }

    @Test
    void testScalarModelBounds() {
        VectorSpaceModel model = new VectorSpaceModel(1000, 128);
        assertThrows(IndexOutOfBoundsException.class, () -> model.scalarModel(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> model.scalarModel(128));
    }

    @Test
    void testScalarModelsCopy() {
        NormalScalarModel[] original = NormalScalarModel.uniformScalar(0.0, 1.0, 4);
        VectorSpaceModel model = new VectorSpaceModel(100, original);

        ScalarModel[] retrieved = model.scalarModels();
        assertNotSame(original, retrieved);
        assertEquals(original.length, retrieved.length);
    }
}
