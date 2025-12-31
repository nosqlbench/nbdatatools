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
    void testCustomGaussianConstruction() {
        VectorSpaceModel model = new VectorSpaceModel(500, 64, 2.0, 0.5);
        assertEquals(500, model.uniqueVectors());
        assertEquals(64, model.dimensions());

        // All components should have the same parameters
        for (int d = 0; d < 64; d++) {
            GaussianComponentModel cm = model.componentModel(d);
            assertEquals(2.0, cm.mean());
            assertEquals(0.5, cm.stdDev());
        }
    }

    @Test
    void testComponentSpecificModels() {
        GaussianComponentModel[] models = new GaussianComponentModel[3];
        models[0] = new GaussianComponentModel(0.0, 1.0);
        models[1] = new GaussianComponentModel(5.0, 2.0);
        models[2] = new GaussianComponentModel(-1.0, 0.5);

        VectorSpaceModel vsm = new VectorSpaceModel(1000, models);
        assertEquals(1000, vsm.uniqueVectors());
        assertEquals(3, vsm.dimensions());

        assertEquals(0.0, vsm.componentModel(0).mean());
        assertEquals(5.0, vsm.componentModel(1).mean());
        assertEquals(-1.0, vsm.componentModel(2).mean());
    }

    @Test
    void testInvalidParameters() {
        assertThrows(IllegalArgumentException.class, () -> new VectorSpaceModel(0, 128));
        assertThrows(IllegalArgumentException.class, () -> new VectorSpaceModel(-1, 128));
        assertThrows(NullPointerException.class, () -> new VectorSpaceModel(1000, null));
        assertThrows(IllegalArgumentException.class, () -> new VectorSpaceModel(1000, new GaussianComponentModel[0]));
    }

    @Test
    void testComponentModelBounds() {
        VectorSpaceModel model = new VectorSpaceModel(1000, 128);
        assertThrows(IndexOutOfBoundsException.class, () -> model.componentModel(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> model.componentModel(128));
    }

    @Test
    void testComponentModelsCopy() {
        GaussianComponentModel[] original = GaussianComponentModel.uniform(0.0, 1.0, 4);
        VectorSpaceModel model = new VectorSpaceModel(100, original);

        GaussianComponentModel[] retrieved = model.componentModels();
        assertNotSame(original, retrieved);
        assertEquals(original.length, retrieved.length);
    }
}
